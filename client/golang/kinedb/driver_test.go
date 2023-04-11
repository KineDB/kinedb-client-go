package kinedb

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	client "github.com/industry-tenebris/kinedb-goclient/client/api"
	"github.com/industry-tenebris/kinedb-goclient/common/model"
)

func TestSimpleClient(t *testing.T) {
	// init addr discovery
	DiscoveryAddr("127.0.0.1:2379")

	// query
	sql := "select id, code, name from mysql100.gql_test01"
	ctx := context.Background()
	res := client.ExecuteSQL(ctx, model.ExecuteSQLRequest{Sql: sql})

	fmt.Printf("res: %+v", res)
}

func TestInsertClient(t *testing.T) {
	// init addr discovery
	DiscoveryAddr("127.0.0.1:2379")

	// query
	sql := "insert into mysql100.gql_test01( id, code, name) values (3, \"a\", \"test1\"),(2, \"ab\", \"test2\") "
	ctx := context.Background()
	res := client.ExecuteSQL(ctx, model.ExecuteSQLRequest{Sql: sql})

	fmt.Printf("res: %+v", res)
}

func TestSelectClient(t *testing.T) {
	// init addr discovery
	DiscoveryAddr("127.0.0.1:2379")

	// query
	sql := "select id, code, name from mysql100.gql_test01 where id = 3"
	ctx := context.Background()
	res := client.ExecuteSQL(ctx, model.ExecuteSQLRequest{Sql: sql})

	fmt.Printf("res: %+v", res)
}

func TestUpdateClient(t *testing.T) {
	// init addr discovery
	DiscoveryAddr("127.0.0.1:2379")

	// query
	sql := "update mysql100.gql_test01 set name=\"test02\" where id = 2"
	ctx := context.Background()
	res := client.ExecuteSQL(ctx, model.ExecuteSQLRequest{Sql: sql})

	fmt.Printf("res: %+v", res)
}

func TestDeleteClient(t *testing.T) {
	// init addr discovery
	DiscoveryAddr("127.0.0.1:2379")

	// query
	sql := "delete from mysql100.gql_test01 where id = 2"
	ctx := context.Background()
	res := client.ExecuteSQL(ctx, model.ExecuteSQLRequest{Sql: sql})

	fmt.Printf("res: %+v", res)
}

func TestKinedbDriver(t *testing.T) {
	// [user[:password]@][net[(addr)]]/dbname[?param1=value1&paramN=valueN]

	dsn := "admin:123456@127.0.0.1:2379/mysql100"
	db, openErr := sql.Open("kinedb", dsn)
	if openErr != nil {
		fmt.Printf("open database connection for kinedb error: %s \n", openErr.Error())
		panic(openErr)
	}

	// person(id int,name string,age int)
	sql := "select * from mysql100.gql_test01 "
	rows, queryErr := db.Query(sql)
	if queryErr != nil {
		fmt.Printf("exec %s failed: %s \n", sql, queryErr.Error())
		panic(queryErr)
	}

	defer rows.Close()

	fmt.Printf("query result: %+v \n", rows)

	for rows.Next() {
		var col1 int
		var col2 string
		var col3 string
		if err := rows.Scan(&col1, &col2, &col3); err != nil {
			panic(err)
		}
		fmt.Printf("id=%d, code=%s, name=%s \n", col1, col2, col3)
	}
	if err := rows.Err(); err != nil {
		if _, isEof := err.(*EOF); !isEof {
			panic(err)
		}
	}
	fmt.Printf("finish")
}
