# kinedb-goclient
This a go client for kinedb, user can import this package to connect kinedb. kinedb-goclient support CRUD base operation.
Guiding:
Setp1: go get this client to you project

go get github.com/industry-tenebris/kinedb-goclient

Setp2: connect to kinedb server, for example:

func main() {
	//init addr discovery
	kinedb.DiscoveryAddr("127.0.0.1:2379")

	// query
	sql := "select id, code, name from mysql100.gql_test01"
	ctx := context.Background()
	res := client.ExecuteSQL(ctx, model.ExecuteSQLRequest{Sql: sql})

	fmt.Printf("res: %+v", res)
}

