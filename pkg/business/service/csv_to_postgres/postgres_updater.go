package csv_to_postgres

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/lib/pq"
	"log"
	"strings"
)

type PostgresUpdater struct {
	DB        *sql.DB
	Schema    string
	TableName string
	Columns   []string
}

func NewPostgresUpdater(db *sql.DB, schema, tableName string, columns []string) *PostgresUpdater {
	return &PostgresUpdater{
		DB:        db,
		Schema:    schema,
		TableName: tableName,
		Columns:   columns,
	}
}
func (u *PostgresUpdater) SetNewColumnNaming(columns []string) *PostgresUpdater {
	if len(columns) == 0 {
		return u
	}
	u.Columns = columns
	return u
}

func (u *PostgresUpdater) SetNewSchema(schema string) *PostgresUpdater {
	if schema == "" {
		return u
	}
	u.Schema = schema
	return u
}

func (u *PostgresUpdater) SetNewTableName(tableName string) *PostgresUpdater {
	if tableName == "" {
		return u
	}
	u.TableName = tableName
	return u
}

// UpdateData принимает подготовленные CSV данные и выполняет обновление через транзакцию.
func (u *PostgresUpdater) UpdateData(csvData [][]interface{}, ctx context.Context, config UpdateConfig) error {
	tx, err := u.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	tempTableName := "temp_" + u.TableName
	createTempTableQuery := fmt.Sprintf(`
        CREATE TEMP TABLE %s AS
        SELECT * FROM %s.%s WHERE 1=0
    `, tempTableName, u.Schema, u.TableName)
	if _, err := tx.ExecContext(ctx, createTempTableQuery); err != nil {
		return fmt.Errorf("create temp table error: %w", err)
	}

	if err := u.validateWithinTx(tx, tempTableName, config); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	log.Printf("Temp table %s создан", tempTableName)

	stmt, err := tx.PrepareContext(ctx, pq.CopyIn(tempTableName, u.Columns...))
	if err != nil {
		return fmt.Errorf("prepare copyin error: %w", err)
	}

	for i, row := range csvData[1:] {
		if _, err := stmt.ExecContext(ctx, row...); err != nil {
			log.Printf("Error in row %d: %v", i, err)
			return fmt.Errorf("exec copyin error at row %d: %w", i, err)
		}
	}
	if _, err = stmt.ExecContext(ctx); err != nil {
		return fmt.Errorf("final exec copyin error: %w", err)
	}
	if err = stmt.Close(); err != nil {
		return fmt.Errorf("close stmt error: %w", err)
	}

	insertQuery := u.buildInsertQuery(tempTableName, config)
	log.Printf("Выполнение запроса: %s", insertQuery)

	if _, err = tx.ExecContext(ctx, insertQuery); err != nil {
		return fmt.Errorf("insert execution error: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit error: %w", err)
	}

	return nil
}

func (u *PostgresUpdater) prefixedColumns(prefix string) []string {
	cols := make([]string, len(u.Columns))
	for i, col := range u.Columns {
		cols[i] = prefix + col
	}
	return cols
}

func (u *PostgresUpdater) buildInsertQuery(tempTableName string, config UpdateConfig) string {
	var joins []string
	var whereConditions []string

	for _, fk := range config.ForeignKeys {
		alias := fmt.Sprintf("fk%d", len(joins)+1)
		joins = append(joins, fmt.Sprintf(
			"INNER JOIN %s.%s AS %s ON temp.%s = %s.%s",
			u.Schema,
			fk.ReferenceTable,
			alias,
			fk.Columns[0],
			alias,
			fk.ReferenceColumn,
		))
	}

	query := fmt.Sprintf(`
        INSERT INTO %s.%s (%s)
        SELECT %s 
        FROM %s AS temp
        %s  -- JOIN для проверки FK
        LEFT JOIN %s.%s AS main ON temp.%s = main.%s
        WHERE main.%s IS NULL
        %s  -- условия для FK
        ON CONFLICT (%s) DO %s`,
		u.Schema, u.TableName,
		strings.Join(u.Columns, ","),
		strings.Join(u.prefixedColumns("temp."), ","),
		tempTableName,
		strings.Join(joins, "\n"),
		u.Schema, u.TableName,
		u.Columns[0], u.Columns[0],
		u.Columns[0],
		strings.Join(whereConditions, " AND "),
		strings.Join(config.ConflictColumns, ", "),
		config.OnConflictAction,
	)

	return query
}

func (u *PostgresUpdater) validateWithinTx(tx *sql.Tx, tempTableName string, config UpdateConfig) error {
	var checks []string

	for _, fk := range config.ForeignKeys {
		checks = append(checks, fmt.Sprintf(`
            SELECT temp.%s
            FROM %s AS temp
            LEFT JOIN %s.%s AS ref 
                ON temp.%s = ref.%s
            WHERE ref.%s IS NULL`,
			fk.Columns[0],
			tempTableName,
			u.Schema, fk.ReferenceTable,
			fk.Columns[0], fk.ReferenceColumn,
			fk.ReferenceColumn,
		))
	}

	if len(checks) == 0 {
		return nil
	}

	checkQuery := strings.Join(checks, "\nUNION ALL\n")

	var invalidCount int
	err := tx.QueryRow(fmt.Sprintf(`
        SELECT COUNT(*) FROM (%s) AS invalid`,
		checkQuery,
	)).Scan(&invalidCount)

	if err != nil {
		return err
	}

	if invalidCount > 0 {
		return fmt.Errorf("found %d invalid foreign key references", invalidCount)
	}

	return nil
}
