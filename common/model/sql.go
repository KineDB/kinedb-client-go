package model

import "github.com/KineDB/kinedb-client-go/common/utils"

type ExecuteSQLRequest struct {
	Sql    string `json:"sql"`
	Gql    string `json:"gql"`
	Prompt string `json:"prompt"`
	Engine string `json:"engine"`
}

type ExecuteSQLResult struct {
	Rows     []*utils.OrderedMap `json:"rows"`
	SqlType  string              `json:"sqlType"`
	PageNum  int                 `json:"pageNum"`
	PageSize int                 `json:"pageSize"`
	Total    int32               `json:"total"`
}

type ExecuteSQLResultV2 struct {
	RowNames  *[]string   `json:"rowNames"`
	RowValues *([]*[]any) `json:"rowValues"`
	SqlType   string      `json:"sqlType"`
	PageNum   int         `json:"pageNum"`
	PageSize  int         `json:"pageSize"`
	Total     int32       `json:"total"`
}
