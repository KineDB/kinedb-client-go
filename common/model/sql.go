package model

import "github.com/industry-tenebris/kinedb-goclient/common/utils"

type ExecuteSQLRequest struct {
	Sql                   string `json:"sql"`
	Gql                   string `json:"gql"`
	EnableDistributeQuery bool   `json:"enableDistributeQuery"`
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
