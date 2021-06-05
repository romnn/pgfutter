package main

import (
	"database/sql"

	"github.com/lib/pq"
)

type Import struct {
	txn  *sql.Tx
	stmt *sql.Stmt
}

func NewCSVImport(db *sql.DB, schema string, tableName string, columns []string, drop bool) (*Import, error) {
	if drop {
		dropTableStatement, err := dropTable(db, schema, tableName)
		if err != nil {
			return nil, err
		}

		_, err = dropTableStatement.Exec()
		if err != nil {
			return nil, err
		}
	}

	createTableStatement, err := createTable(db, schema, tableName, columns)
	if err != nil {
		return nil, err
	}

	_, err = createTableStatement.Exec()
	if err != nil {
		return nil, err
	}

	return newImport(db, schema, tableName, columns)
}

func NewJSONImport(db *sql.DB, schema string, tableName string, column string, dataType string) (*Import, error) {

	table, err := createJSONTable(db, schema, tableName, column, dataType)
	if err != nil {
		return nil, err
	}

	_, err = table.Exec()
	if err != nil {
		return nil, err
	}

	return newImport(db, schema, tableName, []string{column})
}

func newImport(db *sql.DB, schema string, tableName string, columns []string) (*Import, error) {

	txn, err := db.Begin()
	if err != nil {
		return nil, err
	}

	stmt, err := txn.Prepare(pq.CopyInSchema(schema, tableName, columns...))
	if err != nil {
		return nil, err
	}

	return &Import{txn, stmt}, nil
}

func (i *Import) AddRow(nullDelimiter string, columns ...interface{}) error {
	for index := range columns {
		column := columns[index]

		if column == nullDelimiter {
			columns[index] = nil
		}
	}

	_, err := i.stmt.Exec(columns...)
	return err
}

func (i *Import) Commit() error {

	_, err := i.stmt.Exec()
	if err != nil {
		return err
	}

	// Statement might already be closed
	// therefore ignore errors
	_ = i.stmt.Close()

	return i.txn.Commit()

}
