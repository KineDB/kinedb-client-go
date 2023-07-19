package examples

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/KineDB/kinedb-client-go/client/golang/kinedb"
)

// This code shows how to use IP and port methods

// Remote connection to kinedb
func TestKinedbDriver(t *testing.T) {
	// [user[:password]@][net[(addr)]]/dbname[?param1=value1&paramN=valueN]

	dsn := "admin:123456@127.0.0.1:10301/mysql100"
	db, openErr := sql.Open("kinedb", dsn)
	if openErr != nil {
		fmt.Printf("open database connection for kinedb error: %s \n", openErr.Error())
		panic(openErr)
	}

	// person(id int,name string,age int)
	sql := "select * from mysql100.user1 "
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
		if _, isEof := err.(*kinedb.EOF); !isEof {
			panic(err)
		}
	}
	fmt.Printf("finish")
}
