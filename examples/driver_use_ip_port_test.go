package examples

import (
	"context"
	"fmt"
	"testing"

	client "github.com/KineDB/kinedb-client-go/client/api"
	"github.com/KineDB/kinedb-client-go/client/golang/kinedb"
	"github.com/KineDB/kinedb-client-go/common/model"
)

// This code shows how to create an ETCD cluster

// Accessing kinedb in a simple way

func TestSimpleClient(t *testing.T) {
	// init addr discovery
	kinedb.DiscoveryAddr("127.0.0.1:2379")

	// query
	sql := "select id, code, name from mysql100.gql_test01"
	ctx := context.Background()
	res := client.ExecuteSQL(ctx, model.ExecuteSQLRequest{Sql: sql, Engine: "native"})

	fmt.Printf("res: %+v", res)
}

func TestSimpleInsertClient(t *testing.T) {
	// init addr discovery
	kinedb.DiscoveryAddr("127.0.0.1:2379")

	// delete first
	sql1 := "delete from mysql100.gql_test01 where id in (1,2,3) "
	ctx1 := context.Background()
	res1 := client.ExecuteSQL(ctx1, model.ExecuteSQLRequest{Sql: sql1, Engine: "native"})
	fmt.Printf("res1: %+v", res1)

	// insert again
	sql := "insert into mysql100.gql_test01( id, code, name) values (3, \"a\", \"test1\"),(2, \"ab\", \"test2\") "
	ctx := context.Background()
	res := client.ExecuteSQL(ctx, model.ExecuteSQLRequest{Sql: sql, Engine: "native"})

	fmt.Printf("res: %+v", res)
}

func TestSimpleSelectClient(t *testing.T) {
	// init addr discovery
	kinedb.DiscoveryAddr("127.0.0.1:2379")

	// query
	sql := "select id, code, name from mysql100.gql_test01 where id = 3"
	ctx := context.Background()
	res := client.ExecuteSQL(ctx, model.ExecuteSQLRequest{Sql: sql, Engine: "native"})

	fmt.Printf("res: %+v", res)
}

func TestSimpleUpdateClient(t *testing.T) {
	// init addr discovery
	kinedb.DiscoveryAddr("127.0.0.1:2379")

	// query
	sql := "update mysql100.gql_test01 set name=\"test02\" where id = 2"
	ctx := context.Background()
	res := client.ExecuteSQL(ctx, model.ExecuteSQLRequest{Sql: sql, Engine: "native"})

	fmt.Printf("res: %+v", res)
}

func TestSimpleDeleteClient(t *testing.T) {
	// init addr discovery
	kinedb.DiscoveryAddr("127.0.0.1:2379")

	// query
	sql := "delete from mysql100.gql_test01 where id = 2"
	ctx := context.Background()
	res := client.ExecuteSQL(ctx, model.ExecuteSQLRequest{Sql: sql, Engine: "native"})

	fmt.Printf("res: %+v", res)
}
