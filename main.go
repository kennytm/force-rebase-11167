/* Original prompt:

Create program in golang to run some SQL with specification:

1. Takes following input from command line:
    - host
    - port
    - username
    - password
    - a list of schema names, call them `db1`, `db2`, ...
2. Using `github.com/go-sql-driver/mysql`, connect to `username:password@host:port/`
3. for each schema name `dbi`,
    - run `SHOW TABLES FROM dbi` to collect table names inside `dbi`.
4 for each table name `dbi`.`t`,
    - run `SELECT max(_tidb_rowid) FROM dbi.t`
    - if it produces error code 1054 (Unknown column), skip
    - if the result is NULL, skip
    - otherwise, store the result into hash map `rowIDs`, associated with `dbi.t`, i.e. `rowIDs[{dbi, t}] = N`
5. for each entry {dbi, t}, N in the hash map `rowIDs`,
    - run `ALTER TABLE dbi.t AUTO_INCREMENT (N+1)`

*/
/* (manual review applied post-generation) */

package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/go-sql-driver/mysql" // MySQL Driver
)

// tableIdentifier uniquely identifies a table across schemas.
type tableIdentifier struct {
	Schema string
	Table  string
}

// Constant for the specific MySQL error code we want to ignore.
var unknownColumnError = &mysql.MySQLError{Number: 1054}

func main() {
	// 1. Define and parse command-line flags
	host := flag.String("host", "127.0.0.1", "Database host")
	port := flag.String("port", "4000", "Database port")
	user := flag.String("user", "root", "Database username")
	password := flag.String("password", "", "Database password")
	dryRun := flag.Bool("dry-run", false, "Perform a dry run without executing ALTER TABLE commands")
	schemaList := flag.String("schemas", "", "Comma-separated list of schema names")

	flag.Parse()

	schemas := strings.Split(*schemaList, ",")
	log.Printf("# Target Schemas: %v\n", schemas)

	// 2. Connect to the database
	// DSN (Data Source Name) format: username:password@protocol(address)/
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/", *user, *password, *host, *port)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("! Error opening database connection: %v\n", err)
	}
	defer db.Close()

	log.Println("# Database connection successful.")

	maxRowIDs := make(map[tableIdentifier]int64)

	// 3. Iterate through schemas and tables to find max row IDs
	for _, schema := range schemas {
		log.Printf("# Processing schema: %s\n", schema)

		tables, err := getTablesInSchema(db, schema)
		if err != nil {
			log.Printf("! Error getting tables for schema %s: %v. Skipping schema.\n", schema, err)
			continue
		}

		// 4. For each table, get max _tidb_rowid
		for _, table := range tables {
			maxID, err := getMaxRowID(db, schema, table)
			if maxID == 0 {
				if err != nil {
					log.Printf("!    Skipping table %s.%s: %v.\n", schema, table, err)
				}
				continue
			}

			// Store the valid result
			tableID := tableIdentifier{Schema: schema, Table: table}
			maxRowIDs[tableID] = maxID
		}
	}

	log.Println("# Finished collecting max row IDs.")

	log.Println("# Starting AUTO_INCREMENT updates...")
	for tableID, maxID := range maxRowIDs {
		newAutoIncrement := maxID + 1
		// Important: Use backticks for schema and table names to handle special characters/keywords
		alterQuery := fmt.Sprintf("ALTER TABLE `%s`.`%s` AUTO_INCREMENT = %d", tableID.Schema, tableID.Table, newAutoIncrement)
		log.Printf(">>> %s;", alterQuery)

		if !*dryRun {
			_, err := db.Exec(alterQuery)
			if err != nil {
				log.Printf("!    Error updating AUTO_INCREMENT for %s.%s: %v\n", tableID.Schema, tableID.Table, err)
			}
		}
	}

	log.Println("# AUTO_INCREMENT update process finished.")
}

// getTablesInSchema retrieves a list of table names within a given schema.
func getTablesInSchema(db *sql.DB, schemaName string) ([]string, error) {
	query := fmt.Sprintf("SHOW TABLES FROM `%s`", schemaName)
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("querying tables for schema '%s': %w", schemaName, err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("scanning table name for schema '%s': %w", schemaName, err)
		}
		tables = append(tables, tableName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating table rows for schema '%s': %w", schemaName, err)
	}

	return tables, nil
}

// getMaxRowID queries the maximum _tidb_rowid for a specific table.
func getMaxRowID(db *sql.DB, schemaName, tableName string) (int64, error) {
	query := fmt.Sprintf("SELECT coalesce(max(_tidb_rowid), 0) FROM `%s`.`%s`", schemaName, tableName)
	var maxID int64
	err := db.QueryRow(query).Scan(&maxID)

	if unknownColumnError.Is(err) {
		return 0, nil // Ignore the unknown column error
	}
	return maxID, err
}
