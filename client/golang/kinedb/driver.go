package kinedb

// Package KineDB provides a database/sql driver for Itenebris's KineDB.
//
// The driver should be used via the database/sql package:
//
//  import "database/sql"
//  import _ "github.com/kinedb/kinedb-go-client/kinedb"
//
//  dsn := "http://user@localhost:8080?schema=test"
//  db, err := sql.Open("kinedb", dsn)
//

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/google/uuid"

	client "github.com/KineDB/kinedb-client-go/client/api"
	"github.com/KineDB/kinedb-client-go/client/api/proto"
	"github.com/KineDB/kinedb-client-go/common/model"
	commonProto "github.com/KineDB/kinedb-client-go/common/proto"
	"github.com/KineDB/kinedb-client-go/common/types"

	// "synapsedb.io/pkg/synapse/api/client"
	"time"
)

func init() {
	sql.Register("kinedb", &sqldriver{})
}

var (
	// DefaultQueryTimeout is the default timeout for queries executed without a context.
	DefaultQueryTimeout = 60 * time.Second

	// DefaultCancelQueryTimeout is the timeout for the request to cancel queries in kinedb.
	DefaultCancelQueryTimeout = 30 * time.Second

	// ErrOperationNotSupported indicates that a database operation is not supported.
	ErrOperationNotSupported = errors.New("kinedb: operation not supported")

	// ErrQueryCancelled indicates that a query has been cancelled.
	ErrQueryCancelled = errors.New("kinedb: query cancelled")
)

const (
	preparedStatementHeader        = "X-KineDB-Prepared-Statement"
	preparedStatementName          = "_kinedb_go"
	kinedbUserHeader               = "X-KineDB-User"
	kinedbSchemaHeader             = "X-KineDB-Schema"
	kinedbSessionHeader            = "X-KineDB-Session"
	kinedbTransactionHeader        = "X-KineDB-Transaction-Id"
	kinedbStartedTransactionHeader = "X-KineDB-Started-Transaction-Id"
	kinedbClearTransactionHeader   = "X-KineDB-Clear-Transaction-Id"
	kinedbClientTagsHeader         = "X-KineDB-Client-Tags"
	kinedbClientInfoHeader         = "X-KineDB-Client-Info"
)

type sqldriver struct{}

func (d *sqldriver) Open(name string) (driver.Conn, error) {
	return newConn(name)
}

var _ driver.Driver = &sqldriver{}

// Config is a configuration that can be encoded to a DSN string.
type Config struct {
	KinedbDSN         string            // dsn of the KineDB server, e.g. [user[:password]@][net[(addr)]]/dbname[?param1=value1&paramN=valueN]
	Schema            string            // Schema (optional)
	SessionProperties map[string]string // Session properties (optional)
}

// Conn is a kinedb connection.
type Conn struct {
	baseURL           string
	addr              string
	userName          string
	password          string
	discoveryAddr     string
	defaultDatabase   string
	engine            string
	fetchSize         int32
	enableStreamQuery bool
}

var (
	_ driver.Conn               = &Conn{}
	_ driver.ConnPrepareContext = &Conn{}
	_ driver.ConnBeginTx        = &Conn{}
)

func newConn(dsn string) (*Conn, error) {
	kinedbDsn, err := ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("kinedb: malformed dsn: %v", err)
	}
	c := &Conn{
		baseURL:       kinedbDsn.Net + "/" + kinedbDsn.DBName,
		discoveryAddr: kinedbDsn.Net,
	}
	c.addr = kinedbDsn.Addr
	if c.addr == "" {
		c.addr = kinedbDsn.Net
	}
	c.userName = kinedbDsn.User
	c.password = kinedbDsn.Passwd
	c.defaultDatabase = kinedbDsn.DBName
	c.engine = kinedbDsn.Engine
	c.fetchSize = kinedbDsn.FetchSize
	c.enableStreamQuery = kinedbDsn.enableStreamQuery

	DiscoveryAddr(c.discoveryAddr)
	return c, nil
}

// Begin implements the driver.Conn interface.
func (c *Conn) Begin() (driver.Tx, error) {
	return nil, ErrOperationNotSupported
}

func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	args := []string{}
	if opts.ReadOnly {
		args = append(args, "READ ONLY")
	}

	level := sql.IsolationLevel(opts.Isolation)
	if level != sql.LevelDefault {
		err := verifyIsolationLevel(level)
		if err != nil {
			return nil, err
		}
		args = append(args, fmt.Sprintf("ISOLATION LEVEL %s", level.String()))
	}

	query := fmt.Sprintf("START TRANSACTION %s", strings.Join(args, ", "))
	stmt := &driverStmt{conn: c, query: query}
	_, err := stmt.QueryContext(ctx, []driver.NamedValue{})
	if err != nil {
		return nil, err
	}

	return &driverTx{conn: c}, nil
}

// Prepare implements the driver.Conn interface.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return nil, driver.ErrSkip
}

// PrepareContext implements the driver.ConnPrepareContext interface.
func (c *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return &driverStmt{conn: c, query: query}, nil
}

// Close implements the driver.Conn interface.
func (c *Conn) Close() error {
	return nil
}

// ErrQueryFailed indicates that a query to kinedb failed.
type ErrQueryFailed struct {
	StatusCode int
	Reason     error
}

// Error implements the error interface.
func (e *ErrQueryFailed) Error() string {
	return fmt.Sprintf("kinedb: query failed (%d %s): %q",
		e.StatusCode, http.StatusText(e.StatusCode), e.Reason)
}

func newErrQueryFailedFromResponse(resp *http.Response) *ErrQueryFailed {
	const maxBytes = 8 * 1024
	defer resp.Body.Close()
	qf := &ErrQueryFailed{StatusCode: resp.StatusCode}
	b, err := ioutil.ReadAll(io.LimitReader(resp.Body, maxBytes))
	if err != nil {
		qf.Reason = err
		return qf
	}
	reason := string(b)
	if resp.ContentLength > maxBytes {
		reason += "..."
	}
	qf.Reason = errors.New(reason)
	return qf
}

type driverStmt struct {
	conn  *Conn
	query string
	user  string
}

var (
	_ driver.Stmt             = &driverStmt{}
	_ driver.StmtQueryContext = &driverStmt{}
)

func (st *driverStmt) Close() error {
	return nil
}

func (st *driverStmt) NumInput() int {
	return -1
}

func (st *driverStmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, ErrOperationNotSupported
}

type stmtResponse struct {
	ID      string    `json:"id"`
	InfoURI string    `json:"infoUri"`
	NextURI string    `json:"nextUri"`
	Stats   stmtStats `json:"stats"`
	Error   stmtError `json:"error"`
}

type stmtStats struct {
	State           string    `json:"state"`
	Scheduled       bool      `json:"scheduled"`
	Nodes           int       `json:"nodes"`
	TotalSplits     int       `json:"totalSplits"`
	QueuesSplits    int       `json:"queuedSplits"`
	RunningSplits   int       `json:"runningSplits"`
	CompletedSplits int       `json:"completedSplits"`
	UserTimeMillis  int       `json:"userTimeMillis"`
	CPUTimeMillis   int       `json:"cpuTimeMillis"`
	WallTimeMillis  int       `json:"wallTimeMillis"`
	ProcessedRows   int       `json:"processedRows"`
	ProcessedBytes  int       `json:"processedBytes"`
	RootStage       stmtStage `json:"rootStage"`
}

type stmtError struct {
	Message       string               `json:"message"`
	ErrorName     string               `json:"errorName"`
	ErrorCode     int                  `json:"errorCode"`
	ErrorLocation stmtErrorLocation    `json:"errorLocation"`
	FailureInfo   stmtErrorFailureInfo `json:"failureInfo"`
	// Other fields omitted
}

type stmtErrorLocation struct {
	LineNumber   int `json:"lineNumber"`
	ColumnNumber int `json:"columnNumber"`
}

type stmtErrorFailureInfo struct {
	Type string `json:"type"`
	// Other fields omitted
}

func (e stmtError) Error() string {
	return e.FailureInfo.Type + ": " + e.Message
}

type stmtStage struct {
	StageID         string      `json:"stageId"`
	State           string      `json:"state"`
	Done            bool        `json:"done"`
	Nodes           int         `json:"nodes"`
	TotalSplits     int         `json:"totalSplits"`
	QueuedSplits    int         `json:"queuedSplits"`
	RunningSplits   int         `json:"runningSplits"`
	CompletedSplits int         `json:"completedSplits"`
	UserTimeMillis  int         `json:"userTimeMillis"`
	CPUTimeMillis   int         `json:"cpuTimeMillis"`
	WallTimeMillis  int         `json:"wallTimeMillis"`
	ProcessedRows   int         `json:"processedRows"`
	ProcessedBytes  int         `json:"processedBytes"`
	SubStages       []stmtStage `json:"subStages"`
}

// EOF indicates the server has returned io.EOF for the given QueryID.
type EOF struct {
	QueryID string
}

// Error implements the error interface.
func (e *EOF) Error() string {
	return e.QueryID
}

func (st *driverStmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, driver.ErrSkip
}

func (st *driverStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	query := st.query
	request := model.ExecuteSQLRequest{Sql: query,
		DefaultDatabase: st.conn.defaultDatabase,
		Engine:          st.conn.engine,
		FetchSize:       st.conn.fetchSize,
	}

	//
	if st.conn.enableStreamQuery {
		streamClient := client.StreamExecuteSQLWithAddr(ctx, st.conn.addr, request)
		rows := &streamDriverRows{
			ctx:          ctx,
			stmt:         st,
			streamClient: streamClient,
			id:           uuid.New().String(),
		}
		// invoke one time
		if err := rows.streamFetch(false); err != nil {
			return rows, err
		}
		// reset rowindex
		rows.rowindex = 0
		return rows, nil
	}

	resultData := client.ExecuteSQLWithAddr(ctx, st.conn.addr, request)

	rows := &driverRows{
		ctx:           ctx,
		stmt:          st,
		resultRawData: resultData,
		// TODO a query id must return from kinedb
		id: uuid.New().String(),
	}
	completedChannel := make(chan struct{})
	defer close(completedChannel)
	go func() {
		select {
		case <-ctx.Done():
			err := rows.Close()
			if err != nil {
				return
			}
		case <-completedChannel:
			return
		}
	}()

	if err := rows.fetch(false); err != nil {
		return nil, err
	}
	return rows, nil
}

func (st *driverStmt) StreamQueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	query := st.query
	streamClient := client.StreamExecuteSQL(ctx, model.ExecuteSQLRequest{Sql: query})

	rows := &streamDriverRows{
		ctx:          ctx,
		stmt:         st,
		streamClient: streamClient,
		// TODO a query id must return from kinedb
		id:      uuid.New().String(),
		hasNext: false,
	}
	completedChannel := make(chan struct{})
	defer close(completedChannel)
	go func() {
		select {
		case <-ctx.Done():
			err := rows.Close()
			if err != nil {
				return
			}
		case <-completedChannel:
			return
		}
	}()

	return rows, nil
}

type driverRows struct {
	ctx  context.Context
	stmt *driverStmt

	id            string
	resultRawData *commonProto.Results
	err           error
	rowindex      int
	columns       []string
	coltype       []*typeConverter
}

type streamDriverRows struct {
	ctx  context.Context
	stmt *driverStmt

	streamClient  proto.SynapseService_StreamExecuteClient
	id            string
	resultRawData *commonProto.Results
	err           error
	rowindex      int
	columns       []string
	coltype       []*typeConverter
	hasNext       bool
}

var _ driver.Rows = &driverRows{}

var _ driver.Rows = &streamDriverRows{}

func (qr *driverRows) Close() error {
	return qr.err
}

func (qr *streamDriverRows) Close() error {
	if qr.streamClient != nil {
		//err := qr.streamClient.CloseSend().Error()
		//return errors.New(err)
	}
	return nil
}

func (qr *streamDriverRows) Columns() []string {
	if qr.err != nil {
		return []string{}
	}
	return qr.columns
}

func (qr *driverRows) Columns() []string {
	if qr.err != nil {
		return []string{}
	}
	if qr.columns == nil {
		if err := qr.fetch(false); err != nil {
			qr.err = err
			return []string{}
		}
	}
	return qr.columns
}

var coltypeLengthSuffix = regexp.MustCompile(`\(\d+\)$`)

func (qr *driverRows) ColumnTypeDatabaseTypeName(index int) string {
	name := qr.coltype[index].typeName
	if m := coltypeLengthSuffix.FindStringSubmatch(name); m != nil {
		name = name[0 : len(name)-len(m[0])]
	}
	return name
}

func (qr *driverRows) Next(dest []driver.Value) error {
	if qr.err != nil {
		return qr.err
	}
	if qr.columns == nil || qr.rowindex >= len(qr.resultRawData.Rows) {
		return &EOF{QueryID: qr.id}
	}
	if len(qr.coltype) == 0 {
		qr.err = sql.ErrNoRows
		return qr.err
	}
	for i, v := range qr.coltype {
		vv, err := v.ConvertValue(qr.resultRawData.Rows[qr.rowindex].Columns[i])
		if err != nil {
			qr.err = err
			return err
		}
		dest[i] = vv
	}
	qr.rowindex++
	return nil
}

func (qr *streamDriverRows) Next(dest []driver.Value) error {
	if qr.err != nil {
		return qr.err
	}

	if qr.resultRawData == nil || len(qr.resultRawData.Rows) == 0 {
		// fetch next batch page rows
		if err := qr.streamFetch(false); err != nil {
			return err
		}
		// reset rowindex
		qr.rowindex = 0
	}
	// check if no more rows from database
	//reach the last row
	if !qr.hasNext && qr.rowindex == len(qr.resultRawData.Rows) {
		return &EOF{QueryID: qr.id}
	}

	if qr.columns == nil || qr.rowindex >= len(qr.resultRawData.Rows) {
		return &EOF{QueryID: qr.id}
	}
	if len(qr.coltype) == 0 {
		qr.err = sql.ErrNoRows
		return qr.err
	}
	// if len(dest) == 0 {
	// 	dest = make([]driver.Value, len(qr.coltype))
	// }
	for i, v := range qr.coltype {
		vv, err := v.ConvertValue(qr.resultRawData.Rows[qr.rowindex].Columns[i])
		if err != nil {
			qr.err = err
			return err
		}
		dest[i] = vv
	}
	qr.rowindex++
	return nil
}

// TODO convert to synapse kinedb response
type queryResponse struct {
	ID               string        `json:"id"`
	InfoURI          string        `json:"infoUri"`
	PartialCancelURI string        `json:"partialCancelUri"`
	NextURI          string        `json:"nextUri"`
	Columns          []queryColumn `json:"columns"`
	Data             []queryData   `json:"data"`
	Stats            stmtStats     `json:"stats"`
	Error            stmtError     `json:"error"`
}

type queryColumn struct {
	Name          string        `json:"name"`
	Type          string        `json:"type"`
	TypeSignature typeSignature `json:"typeSignature"`
}

type queryData []interface{}

type typeSignature struct {
	RawType          string        `json:"rawType"`
	TypeArguments    []interface{} `json:"typeArguments"`
	LiteralArguments []interface{} `json:"literalArguments"`
}

type infoResponse struct {
	QueryID string `json:"queryId"`
	State   string `json:"state"`
}

func handleResponseError(status int, respErr stmtError) error {
	switch respErr.ErrorName {
	case "":
		return nil
	case "USER_CANCELLED":
		return ErrQueryCancelled
	default:
		return &ErrQueryFailed{
			StatusCode: status,
			Reason:     &respErr,
		}
	}
}

// init result data, and cache
func (qr *driverRows) fetch(allowEOF bool) error {
	if qr.columns == nil && len(qr.resultRawData.Rows) > 0 {
		qr.initColumns()
	}
	return nil
}

func (qr *streamDriverRows) streamFetch(allowEOF bool) error {
	// fetch one batch (page) rows
	res, err := qr.streamClient.Recv()
	if err == io.EOF {
		qr.hasNext = false
		return nil
	}
	if err != nil {
		qr.hasNext = false
		return err
	}
	if len(res.Rows) == 0 {
		qr.hasNext = false
		return nil
	}
	qr.resultRawData = res
	if qr.columns == nil && len(qr.resultRawData.Rows) > 0 {
		qr.columns, qr.coltype = initColumns(qr.resultRawData)
	}
	qr.hasNext = true
	return nil
}

func (qr *driverRows) initColumns() {
	firstRow := qr.resultRawData.Rows[0]
	qr.columns = make([]string, len(firstRow.Columns))
	qr.coltype = make([]*typeConverter, len(firstRow.Columns))

	for i, col := range firstRow.Columns {
		qr.columns[i] = col.Name
		qr.coltype[i] = newTypeConverter(col.Type)
	}
}

func initColumns(resultRawData *commonProto.Results) (columns []string, coltype []*typeConverter) {
	firstRow := resultRawData.Rows[0]
	columns = make([]string, len(firstRow.Columns))
	coltype = make([]*typeConverter, len(firstRow.Columns))

	for i, col := range firstRow.Columns {
		columns[i] = col.Name
		coltype[i] = newTypeConverter(col.Type)
	}
	return
}

type typeConverter struct {
	typeName   string
	parsedType []string // e.g. array, array, varchar, for [][]string
}

func newTypeConverter(typeName string) *typeConverter {
	return &typeConverter{
		typeName:   typeName,
		parsedType: parseType(typeName),
	}
}

// parses kinedb types, e.g. array(varchar(10)) to "array", "varchar"
// TODO: Use queryColumn.TypeSignature instead.
func parseType(name string) []string {
	parts := strings.Split(name, "(")
	if len(parts) == 1 {
		return parts
	}
	last := len(parts) - 1
	parts[last] = strings.TrimRight(parts[last], ")")
	if len(parts[last]) > 0 {
		if _, err := strconv.Atoi(parts[last]); err == nil {
			parts = parts[:last]
		}
	}
	return parts
}

// ConvertValue implements the driver.ValueConverter interface.
func (c *typeConverter) ConvertValue(v interface{}) (driver.Value, error) {
	switch strings.ToLower(c.parsedType[0]) {
	case "boolean":
		vv, err := scanNullBool(v)
		if !vv.Valid {
			return nil, err
		}
		return vv.Bool, err
	case "json", "char", "varchar", "varbinary", "interval year to month", "interval day to second", "decimal", "ipaddress", "unknown":
		vv, err := scanNullString(v)
		if !vv.Valid {
			return nil, err
		}
		return vv.String, err
	case "tinyint", "smallint", "integer", "bigint":
		vv, err := scanNullInt64(v)
		if !vv.Valid {
			return nil, err
		}
		return vv.Int64, err
	case "real", "double":
		vv, err := scanNullFloat64(v)
		if !vv.Valid {
			return nil, err
		}
		return vv.Float64, err
	case "date", "time", "time with time zone", "timestamp", "timestamp with time zone":
		vv, err := scanNullTime(v)
		if !vv.Valid {
			return nil, err
		}
		return vv.Time, err
	case "map":
		if err := validateMap(v); err != nil {
			return nil, err
		}
		return v, nil
	case "array":
		if err := validateSlice(v); err != nil {
			return nil, err
		}
		return v, nil
	default:
		return nil, fmt.Errorf("type not supported: %q", c.typeName)
	}
}

func validateMap(v interface{}) error {
	if v == nil {
		return nil
	}
	if _, ok := v.(map[string]interface{}); !ok {
		return fmt.Errorf("cannot convert %v (%T) to map", v, v)
	}
	return nil
}

func validateSlice(v interface{}) error {
	if v == nil {
		return nil
	}
	if _, ok := v.([]interface{}); !ok {
		return fmt.Errorf("cannot convert %v (%T) to slice", v, v)
	}
	return nil
}

func scanNullBool(v interface{}) (sql.NullBool, error) {
	if v == nil {
		return sql.NullBool{}, nil
	}

	colRef := v.(*commonProto.ColumnValueRef)
	colStandardType, convertTypeErr := types.ByteToStandardType(colRef.Type, colRef.Value)
	if convertTypeErr != nil {
		return sql.NullBool{}, convertTypeErr
	}

	// check if it can be cast to boolean
	canCast := types.CanCoerce(colStandardType.GetStandardType(), types.BOOLEAN)
	if !canCast {
		return sql.NullBool{},
			fmt.Errorf("cannot convert %v (%T) to boolean", v, v)
	}

	// cast to boolean
	targetType := colStandardType.CastTo(types.BOOLEAN)
	finalType := targetType.(*types.BooleanType)

	// return nullable boolean
	vv := finalType.Value
	return sql.NullBool{Valid: true, Bool: vv}, nil
}

// NullSliceBool represents a slice of bool that may be null.
type NullSliceBool struct {
	SliceBool []sql.NullBool
	Valid     bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSliceBool) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("kinedb: cannot convert %v (%T) to []bool", value, value)
	}
	slice := make([]sql.NullBool, len(vs))
	for i := range vs {
		v, err := scanNullBool(vs[i])
		if err != nil {
			return err
		}
		slice[i] = v
	}
	s.SliceBool = slice
	s.Valid = true
	return nil
}

// NullSlice2Bool represents a two-dimensional slice of bool that may be null.
type NullSlice2Bool struct {
	Slice2Bool [][]sql.NullBool
	Valid      bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSlice2Bool) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("kinedb: cannot convert %v (%T) to [][]bool", value, value)
	}
	slice := make([][]sql.NullBool, len(vs))
	for i := range vs {
		var ss NullSliceBool
		if err := ss.Scan(vs[i]); err != nil {
			return err
		}
		slice[i] = ss.SliceBool
	}
	s.Slice2Bool = slice
	s.Valid = true
	return nil
}

// NullSlice3Bool implements a three-dimensional slice of bool that may be null.
type NullSlice3Bool struct {
	Slice3Bool [][][]sql.NullBool
	Valid      bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSlice3Bool) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("kinedb: cannot convert %v (%T) to [][][]bool", value, value)
	}
	slice := make([][][]sql.NullBool, len(vs))
	for i := range vs {
		var ss NullSlice2Bool
		if err := ss.Scan(vs[i]); err != nil {
			return err
		}
		slice[i] = ss.Slice2Bool
	}
	s.Slice3Bool = slice
	s.Valid = true
	return nil
}

func scanNullString(v interface{}) (sql.NullString, error) {
	if v == nil {
		return sql.NullString{}, nil
	}

	colRef := v.(*commonProto.ColumnValueRef)
	colStandardType, convertTypeErr := types.ByteToStandardType(colRef.Type, colRef.Value)
	if convertTypeErr != nil {
		return sql.NullString{}, convertTypeErr
	}

	// check if it can be cast to varchar
	canCast := types.CanCoerce(colStandardType.GetStandardType(), types.VARCHAR)
	if !canCast {
		return sql.NullString{},
			fmt.Errorf("cannot convert %v (%T) to varchar", v, v)
	}

	// cast to varchar
	targetType := colStandardType.CastTo(types.VARCHAR)
	finalType := targetType.(*types.VarcharType)

	// return nullable varchar
	vv := finalType.Value
	return sql.NullString{Valid: true, String: vv}, nil
}

// NullSliceString represents a slice of string that may be null.
type NullSliceString struct {
	SliceString []sql.NullString
	Valid       bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSliceString) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("kinedb: cannot convert %v (%T) to []string", value, value)
	}
	slice := make([]sql.NullString, len(vs))
	for i := range vs {
		v, err := scanNullString(vs[i])
		if err != nil {
			return err
		}
		slice[i] = v
	}
	s.SliceString = slice
	s.Valid = true
	return nil
}

// NullSlice2String represents a two-dimensional slice of string that may be null.
type NullSlice2String struct {
	Slice2String [][]sql.NullString
	Valid        bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSlice2String) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("kinedb: cannot convert %v (%T) to [][]string", value, value)
	}
	slice := make([][]sql.NullString, len(vs))
	for i := range vs {
		var ss NullSliceString
		if err := ss.Scan(vs[i]); err != nil {
			return err
		}
		slice[i] = ss.SliceString
	}
	s.Slice2String = slice
	s.Valid = true
	return nil
}

// NullSlice3String implements a three-dimensional slice of string that may be null.
type NullSlice3String struct {
	Slice3String [][][]sql.NullString
	Valid        bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSlice3String) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("kinedb: cannot convert %v (%T) to [][][]string", value, value)
	}
	slice := make([][][]sql.NullString, len(vs))
	for i := range vs {
		var ss NullSlice2String
		if err := ss.Scan(vs[i]); err != nil {
			return err
		}
		slice[i] = ss.Slice2String
	}
	s.Slice3String = slice
	s.Valid = true
	return nil
}

func scanNullInt64(v interface{}) (sql.NullInt64, error) {
	if v == nil {
		return sql.NullInt64{}, nil
	}
	colRef := v.(*commonProto.ColumnValueRef)
	colStandardType, convertTypeErr := types.ByteToStandardType(colRef.Type, colRef.Value)
	if convertTypeErr != nil {
		return sql.NullInt64{}, convertTypeErr
	}

	// check if it can be cast to bigint
	canCast := types.CanCoerce(colStandardType.GetStandardType(), types.BIGINT)
	if !canCast {
		return sql.NullInt64{},
			fmt.Errorf("cannot convert %v (%T) to int64", v, v)
	}

	// cast to bigint
	targetType := colStandardType.CastTo(types.BIGINT)
	finalType := targetType.(*types.BigintType)

	// return nullable int64
	vv := finalType.Value
	return sql.NullInt64{Valid: true, Int64: vv}, nil
}

// NullSliceInt64 represents a slice of int64 that may be null.
type NullSliceInt64 struct {
	SliceInt64 []sql.NullInt64
	Valid      bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSliceInt64) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("kinedb: cannot convert %v (%T) to []int64", value, value)
	}
	slice := make([]sql.NullInt64, len(vs))
	for i := range vs {
		v, err := scanNullInt64(vs[i])
		if err != nil {
			return err
		}
		slice[i] = v
	}
	s.SliceInt64 = slice
	s.Valid = true
	return nil
}

// NullSlice2Int64 represents a two-dimensional slice of int64 that may be null.
type NullSlice2Int64 struct {
	Slice2Int64 [][]sql.NullInt64
	Valid       bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSlice2Int64) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("kinedb: cannot convert %v (%T) to [][]int64", value, value)
	}
	slice := make([][]sql.NullInt64, len(vs))
	for i := range vs {
		var ss NullSliceInt64
		if err := ss.Scan(vs[i]); err != nil {
			return err
		}
		slice[i] = ss.SliceInt64
	}
	s.Slice2Int64 = slice
	s.Valid = true
	return nil
}

// NullSlice3Int64 implements a three-dimensional slice of int64 that may be null.
type NullSlice3Int64 struct {
	Slice3Int64 [][][]sql.NullInt64
	Valid       bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSlice3Int64) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("kinedb: cannot convert %v (%T) to [][][]int64", value, value)
	}
	slice := make([][][]sql.NullInt64, len(vs))
	for i := range vs {
		var ss NullSlice2Int64
		if err := ss.Scan(vs[i]); err != nil {
			return err
		}
		slice[i] = ss.Slice2Int64
	}
	s.Slice3Int64 = slice
	s.Valid = true
	return nil
}

func scanNullFloat64(v interface{}) (sql.NullFloat64, error) {
	if v == nil {
		return sql.NullFloat64{}, nil
	}

	colRef := v.(*commonProto.ColumnValueRef)
	colStandardType, convertTypeErr := types.ByteToStandardType(colRef.Type, colRef.Value)
	if convertTypeErr != nil {
		return sql.NullFloat64{}, convertTypeErr
	}

	// check if it can be cast to float64
	canCast := types.CanCoerce(colStandardType.GetStandardType(), types.DOUBLE)
	if !canCast {
		return sql.NullFloat64{},
			fmt.Errorf("cannot convert %v (%T) to float64", v, v)
	}

	// cast to float64
	targetType := colStandardType.CastTo(types.DOUBLE)
	finalType := targetType.(*types.DoubleType)

	// return nullable float64
	vv := finalType.Value
	return sql.NullFloat64{Valid: true, Float64: vv}, nil
}

// NullSliceFloat64 represents a slice of float64 that may be null.
type NullSliceFloat64 struct {
	SliceFloat64 []sql.NullFloat64
	Valid        bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSliceFloat64) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("kinedb: cannot convert %v (%T) to []float64", value, value)
	}
	slice := make([]sql.NullFloat64, len(vs))
	for i := range vs {
		v, err := scanNullFloat64(vs[i])
		if err != nil {
			return err
		}
		slice[i] = v
	}
	s.SliceFloat64 = slice
	s.Valid = true
	return nil
}

// NullSlice2Float64 represents a two-dimensional slice of float64 that may be null.
type NullSlice2Float64 struct {
	Slice2Float64 [][]sql.NullFloat64
	Valid         bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSlice2Float64) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("kinedb: cannot convert %v (%T) to [][]float64", value, value)
	}
	slice := make([][]sql.NullFloat64, len(vs))
	for i := range vs {
		var ss NullSliceFloat64
		if err := ss.Scan(vs[i]); err != nil {
			return err
		}
		slice[i] = ss.SliceFloat64
	}
	s.Slice2Float64 = slice
	s.Valid = true
	return nil
}

// NullSlice3Float64 represents a three-dimensional slice of float64 that may be null.
type NullSlice3Float64 struct {
	Slice3Float64 [][][]sql.NullFloat64
	Valid         bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSlice3Float64) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("kinedb: cannot convert %v (%T) to [][][]float64", value, value)
	}
	slice := make([][][]sql.NullFloat64, len(vs))
	for i := range vs {
		var ss NullSlice2Float64
		if err := ss.Scan(vs[i]); err != nil {
			return err
		}
		slice[i] = ss.Slice2Float64
	}
	s.Slice3Float64 = slice
	s.Valid = true
	return nil
}

var timeLayouts = []string{
	"2006-01-02",
	"15:04:05.000",
	"2006-01-02 15:04:05.000",
}

func scanNullTime(v interface{}) (NullTime, error) {
	if v == nil {
		return NullTime{}, nil
	}

	colRef := v.(*commonProto.ColumnValueRef)
	colStandardType, convertTypeErr := types.ByteToStandardType(colRef.Type, colRef.Value)
	if convertTypeErr != nil {
		return NullTime{}, convertTypeErr
	}

	// check if it can be cast to timestamp
	canCast := types.CanCoerce(colStandardType.GetStandardType(), types.TIMESTAMP)
	if !canCast {
		return NullTime{},
			fmt.Errorf("cannot convert %v (%T) to time", v, v)
	}

	// cast to timestamp
	targetType := colStandardType.CastTo(types.TIMESTAMP)
	finalType := targetType.(*types.TimestampType)

	// return nullable timestamp
	vv := finalType.Value
	return parseNullTimeWithTimestamp(vv)
}

func parseNullTimeWithTimestamp(timestampMil int64) (NullTime, error) {
	seconds := timestampMil / 1000
	nanos := (timestampMil % 1000) * 1000000
	t := time.Unix(seconds, nanos)
	return NullTime{Valid: true, Time: t}, nil
}

func parseNullTime(v string) (NullTime, error) {
	var t time.Time
	var err error
	for _, layout := range timeLayouts {
		t, err = time.ParseInLocation(layout, v, time.Local)
		if err == nil {
			return NullTime{Valid: true, Time: t}, nil
		}
	}
	return NullTime{}, err
}

func parseNullTimeWithLocation(v string) (NullTime, error) {
	idx := strings.LastIndex(v, " ")
	if idx == -1 {
		return NullTime{}, fmt.Errorf("cannot convert %v (%T) to time+zone", v, v)
	}
	stamp, location := v[:idx], v[idx+1:]
	loc, err := time.LoadLocation(location)
	if err != nil {
		return NullTime{}, fmt.Errorf("cannot load timezone %q: %v", location, err)
	}
	var t time.Time
	for _, layout := range timeLayouts {
		t, err = time.ParseInLocation(layout, stamp, loc)
		if err == nil {
			return NullTime{Valid: true, Time: t}, nil
		}
	}
	return NullTime{}, err
}

// NullTime represents a time.Time value that can be null.
// The NullTime supports kinedb's Date, Time and Timestamp data types,
// with or without time zone.
type NullTime struct {
	Time  time.Time
	Valid bool
}

// Scan implements the sql.Scanner interface.
func (s *NullTime) Scan(value interface{}) error {
	switch value.(type) {
	case time.Time:
		s.Time, s.Valid = value.(time.Time)
	case NullTime:
		*s = value.(NullTime)
	}
	return nil
}

// NullSliceTime represents a slice of time.Time that may be null.
type NullSliceTime struct {
	SliceTime []NullTime
	Valid     bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSliceTime) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("kinedb: cannot convert %v (%T) to []time.Time", value, value)
	}
	slice := make([]NullTime, len(vs))
	for i := range vs {
		v, err := scanNullTime(vs[i])
		if err != nil {
			return err
		}
		slice[i] = v
	}
	s.SliceTime = slice
	s.Valid = true
	return nil
}

// NullSlice2Time represents a two-dimensional slice of time.Time that may be null.
type NullSlice2Time struct {
	Slice2Time [][]NullTime
	Valid      bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSlice2Time) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("kinedb: cannot convert %v (%T) to [][]time.Time", value, value)
	}
	slice := make([][]NullTime, len(vs))
	for i := range vs {
		var ss NullSliceTime
		if err := ss.Scan(vs[i]); err != nil {
			return err
		}
		slice[i] = ss.SliceTime
	}
	s.Slice2Time = slice
	s.Valid = true
	return nil
}

// NullSlice3Time represents a three-dimensional slice of time.Time that may be null.
type NullSlice3Time struct {
	Slice3Time [][][]NullTime
	Valid      bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSlice3Time) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("kinedb: cannot convert %v (%T) to [][][]time.Time", value, value)
	}
	slice := make([][][]NullTime, len(vs))
	for i := range vs {
		var ss NullSlice2Time
		if err := ss.Scan(vs[i]); err != nil {
			return err
		}
		slice[i] = ss.Slice2Time
	}
	s.Slice3Time = slice
	s.Valid = true
	return nil
}

// NullMap represents a map type that may be null.
type NullMap struct {
	Map   map[string]interface{}
	Valid bool
}

// Scan implements the sql.Scanner interface.
func (m *NullMap) Scan(v interface{}) error {
	if v == nil {
		return nil
	}
	m.Map, m.Valid = v.(map[string]interface{})
	return nil
}

// NullSliceMap represents a slice of NullMap that may be null.
type NullSliceMap struct {
	SliceMap []NullMap
	Valid    bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSliceMap) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("kinedb: cannot convert %v (%T) to []NullMap", value, value)
	}
	slice := make([]NullMap, len(vs))
	for i := range vs {
		if err := validateMap(vs[i]); err != nil {
			return fmt.Errorf("cannot convert %v (%T) to []NullMap", value, value)
		}
		m := NullMap{}
		m.Scan(vs[i])
		slice[i] = m
	}
	s.SliceMap = slice
	s.Valid = true
	return nil
}

// NullSlice2Map represents a two-dimensional slice of NullMap that may be null.
type NullSlice2Map struct {
	Slice2Map [][]NullMap
	Valid     bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSlice2Map) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("kinedb: cannot convert %v (%T) to [][]NullMap", value, value)
	}
	slice := make([][]NullMap, len(vs))
	for i := range vs {
		var ss NullSliceMap
		if err := ss.Scan(vs[i]); err != nil {
			return err
		}
		slice[i] = ss.SliceMap
	}
	s.Slice2Map = slice
	s.Valid = true
	return nil
}

// NullSlice3Map represents a three-dimensional slice of NullMap that may be null.
type NullSlice3Map struct {
	Slice3Map [][][]NullMap
	Valid     bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSlice3Map) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("kinedb: cannot convert %v (%T) to [][][]NullMap", value, value)
	}
	slice := make([][][]NullMap, len(vs))
	for i := range vs {
		var ss NullSlice2Map
		if err := ss.Scan(vs[i]); err != nil {
			return err
		}
		slice[i] = ss.Slice2Map
	}
	s.Slice3Map = slice
	s.Valid = true
	return nil
}
