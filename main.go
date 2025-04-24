package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/go-sql-driver/mysql" // MySQL Driver
)

// tableInfo is the fully-qualified table name + the calculated auto_increment value
type tableInfo struct {
	Schema  string
	Table   string
	AutoInc int64
}

// Constant for the specific MySQL error code we want to ignore.
var unknownColumnError = &mysql.MySQLError{Number: 1054}

const (
	modeCompare = iota
	modeRebase
)

func main() {
	// 1. Define and parse command-line flags
	host := flag.String("host", "127.0.0.1", "Database host")
	port := flag.String("port", "4000", "Database port")
	user := flag.String("user", "root", "Database username")
	password := flag.String("password", "", "Database password")
	modeString := flag.String("mode", "", "Mode of operation (compare | rebase)")
	schemaList := flag.String("schemas", "", "Comma-separated list of schema names")

	flag.Parse()

	var mode int
	switch *modeString {
	case "compare":
		mode = modeCompare
		fmt.Println("Schema,Table,Expected,Current,Status")
	case "rebase":
		mode = modeRebase
	default:
		flag.Usage()
		log.Fatalf("! Invalid mode specified. Use 'compare' or 'rebase'.\n")
	}

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

	var tableInfos []tableInfo

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
			tableInfos = append(tableInfos, tableInfo{Schema: schema, Table: table, AutoInc: maxID + 1})
		}
	}

	log.Println("# Finished collecting max row IDs.")

	log.Println("# Starting execution...")
	for _, t := range tableInfos {
		switch mode {
		case modeRebase:
			err = rebaseAutoIncrement(db, &t)
		case modeCompare:
			err = compareAutoIncrement(db, &t)
		}
		if err != nil {
			log.Printf("!    Error executing for %s.%s: %v\n", t.Schema, t.Table, err)
		}
	}

	log.Println("# Execution finished.")
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

func rebaseAutoIncrement(db *sql.DB, t *tableInfo) error {
	query := fmt.Sprintf("ALTER TABLE `%s`.`%s` AUTO_INCREMENT = %d", t.Schema, t.Table, t.AutoInc)
	log.Printf(">>> %s;", query)
	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("rebasing AUTO_INCREMENT for %s.%s: %w", t.Schema, t.Table, err)
	}
	return nil
}

func compareAutoIncrement(db *sql.DB, t *tableInfo) error {
	query := fmt.Sprintf("SHOW TABLE `%s`.`%s` NEXT_ROW_ID", t.Schema, t.Table)
	// perform the query and iterate the resultset, compare if the column `ID_TYPE` has value "_TIDB_ROWID". if yes, read the value in the `NEXT_GLOBAL_ROW_ID` column.
	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("comparing NEXT_ROW_ID for %s.%s: %w", t.Schema, t.Table, err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("getting columns for next row id query '%s.%s': %w", t.Schema, t.Table, err)
	}

	// Find indices of required columns
	idTypeIndex := -1
	nextIDIndex := -1
	for i, colName := range cols {
		// Case-insensitive comparison just to be safe, although SHOW output is usually consistent
		switch strings.ToUpper(colName) {
		case "ID_TYPE":
			idTypeIndex = i
		case "NEXT_GLOBAL_ROW_ID":
			nextIDIndex = i
		}
	}
	if idTypeIndex == -1 || nextIDIndex == -1 {
		return fmt.Errorf("required columns 'ID_TYPE' or 'NEXT_GLOBAL_ROW_ID' not found in output of SHOW TABLE NEXT_ROW_ID for '%s.%s'", t.Schema, t.Table)
	}

	// Create slices for scanning row data
	scanArgs := make([]interface{}, len(cols))
	for i := range scanArgs {
		switch i {
		case idTypeIndex:
			scanArgs[i] = new(string)
		case nextIDIndex:
			scanArgs[i] = new(int64)
		default:
			scanArgs[i] = new(sql.RawBytes)
		}
	}

	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return fmt.Errorf("scanning row for schema '%s' table '%s': %w", t.Schema, t.Table, err)
		}

		if *(scanArgs[idTypeIndex].(*string)) != "_TIDB_ROWID" {
			continue
		}
		nextGlobalRowID := *(scanArgs[nextIDIndex].(*int64))
		var status string
		if nextGlobalRowID >= t.AutoInc {
			status = "ok"
		} else {
			status = "ERROR"
		}
		fmt.Printf("%s,%s,%d,%d,%s\n", t.Schema, t.Table, t.AutoInc, nextGlobalRowID, status)
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("iterating next row id results for '%s.%s': %w", t.Schema, t.Table, err)
	}

	return nil
}
