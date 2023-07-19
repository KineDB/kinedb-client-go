package examples

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
)

func TestQuery(t *testing.T) {
	db, openErr := sql.Open("kinedb", "root:123456@127.0.0.1:10301/mysql100?engine=presto&fetchSize=1000")
	if openErr != nil {
		t.Logf("queryTest init conn error [%+v]", openErr)
	}

	ctx, _ := context.WithCancel(context.Background())
	queryRows, err := db.QueryContext(ctx, "select * from mysql100.user1")
	if err != nil {
		t.Logf("queryTest init queryRows error [%+v]", err)
	}
	var rows [][]any
	for queryRows.Next() {
		columnTypes, _ := queryRows.ColumnTypes()
		data := make([]any, len(columnTypes))
		queryRows.Scan(data...)
		rows = append(rows, data)
	}
	t.Logf("queryTest result data = [%+v]", rows)
}

func TestStreamQuery(t *testing.T) {
	db, openErr := sql.Open("kinedb", "root:123456@127.0.0.1:10301/mysql100?engine=presto&fetchSize=1000&enableStreamQuery=true")
	if openErr != nil {
		t.Logf("stream query init conn error [%+v]", openErr)
	}

	ctx, _ := context.WithCancel(context.Background())
	queryRows, err := db.QueryContext(ctx, "select * from mysql100.user1")
	if err != nil {
		t.Logf("stream query init queryRows error [%+v]", err)
	}
	var index = 0
	//var rows [][]any
	for queryRows.Next() {
		columnTypes, _ := queryRows.ColumnTypes()
		data := make([]any, len(columnTypes))
		for index := range columnTypes {
			data[index] = new(any)
		}
		queryRows.Scan(data...)
		//rows = append(rows, data)
		fmt.Printf("stream query result index %d\n", index)
		fmt.Printf("stream query result data [%+v]\n", data)
		index++
	}
	//t.Logf("queryTest result data = [%+v]", rows)
}
