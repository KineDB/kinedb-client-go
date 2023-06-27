KineDB's Go client follows the implementation and use of the database/sql package in Go.
# features
1. Connect to the kinedb server via gRPC
2. Call the client API to directly perform CRUD operations, which is simple and easy to operate.
# require
1. Go 1.9 or newer
2. KineDB 3.20.0 or newer
# install
You need a working environment that has $GOPATH Go installed and set up.
Download and install the Presto database/SQL driver:
```sh
go get github.com/KineDB/kinedb-client-go
```
Make sure Git is installed and in $PATH.

# driver client using ip and port
[examples](examples/driver_use_ip_port.go)

# simple client when you are in kinedb cluster
[examples](examples/simple_etcd_cluster.go)

# simple example code
```sh
package main

import (
	"context"
	"fmt"

	client "github.com/KineDB/kinedb-client-go/client/api"
	"github.com/KineDB/kinedb-client-go/client/golang/kinedb"
	"github.com/KineDB/kinedb-client-go/common/model"
)

func main() {
	//init addr discovery
	kinedb.DiscoveryAddr("127.0.0.1:2379")

	//insert
	sql := "insert into mysql1.test( id, code, name) values (3, \"a\", \"test1\"),(2, \"ab\", \"test2\") "
	ctx := context.Background()
	res := client.ExecuteSQL(ctx, model.ExecuteSQLRequest{Sql: sql})
	fmt.Printf("res: %+v", res)

	// select
	sql = "select id, code, name from mysql1.test"
	ctx = context.Background()
	res = client.ExecuteSQL(ctx, model.ExecuteSQLRequest{Sql: sql})
	fmt.Printf("res: %+v", res)

	// update
	sql = "update mysql1.test set name=\"test02\" where id = 2"
	ctx = context.Background()
	res = client.ExecuteSQL(ctx, model.ExecuteSQLRequest{Sql: sql})
	fmt.Printf("res: %+v", res)

	// delete
	sql = "delete from mysql100.gql_test01 where id = 2"
	ctx = context.Background()
	res = client.ExecuteSQL(ctx, model.ExecuteSQLRequest{Sql: sql})
	fmt.Printf("res: %+v", res)
}
```

