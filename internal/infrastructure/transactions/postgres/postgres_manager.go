package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"gomarketplace_api/internal/core/ports/core_logger"
	"sync"
	"time"

	"github.com/google/uuid"
	"gomarketplace_api/internal/infrastructure/transactions"
)

// контекстный ключ для хранения транзакции
type txKey struct{}

// PostgresTransaction реализация интерфейса Transaction для PostgreSQL
type PostgresTransaction struct {
	tx         *sql.Tx
	id         string
	tenantID   string
	isolation  transactions.IsolationLevel
	readOnly   bool
	active     bool
	createTime time.Time
	mutex      sync.RWMutex
}

// NewPostgresTransaction создает новую транзакцию PostgreSQL
func NewPostgresTransaction(
	tx *sql.Tx,
	tenantID string,
	isolation transactions.IsolationLevel,
	readOnly bool,
) *PostgresTransaction {
	return &PostgresTransaction{
		tx:         tx,
		id:         uuid.New().String(),
		tenantID:   tenantID,
		isolation:  isolation,
		readOnly:   readOnly,
		active:     true,
		createTime: time.Now(),
		mutex:      sync.RWMutex{},
	}
}

// Commit фиксирует транзакцию
func (t *PostgresTransaction) Commit() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if !t.active {
		return fmt.Errorf("transaction is not active")
	}

	err := t.tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	t.active = false
	return nil
}

// Rollback откатывает транзакцию
func (t *PostgresTransaction) Rollback() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if !t.active {
		return nil // Транзакция уже неактивна, ничего не делаем
	}

	err := t.tx.Rollback()
	if err != nil {
		return fmt.Errorf("failed to rollback transaction: %w", err)
	}

	t.active = false
	return nil
}

// IsActive проверяет, активна ли транзакция
func (t *PostgresTransaction) IsActive() bool {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.active
}

// GetID возвращает уникальный идентификатор транзакции
func (t *PostgresTransaction) GetID() string {
	return t.id
}

// GetTenantID возвращает идентификатор арендатора для транзакции
func (t *PostgresTransaction) GetTenantID() string {
	return t.tenantID
}

// GetIsolationLevel возвращает уровень изоляции транзакции
func (t *PostgresTransaction) GetIsolationLevel() transactions.IsolationLevel {
	return t.isolation
}

// IsReadOnly проверяет, является ли транзакция транзакцией только для чтения
func (t *PostgresTransaction) IsReadOnly() bool {
	return t.readOnly
}

// GetSQLTransaction возвращает базовую SQL транзакцию
func (t *PostgresTransaction) GetSQLTransaction() *sql.Tx {
	return t.tx
}

// PostgresTransactionManager реализация TransactionManager для PostgreSQL
type PostgresTransactionManager struct {
	db     *sql.DB
	logger core_logger.LoggerPort
}

// NewPostgresTransactionManager создает новый менеджер транзакций PostgreSQL
func NewPostgresTransactionManager(db *sql.DB, logger core_logger.LoggerPort) *PostgresTransactionManager {
	return &PostgresTransactionManager{
		db:     db,
		logger: logger,
	}
}

// Execute выполняет операцию в транзакции с настройками по умолчанию
func (m *PostgresTransactionManager) Execute(
	ctx context.Context,
	operation transactions.TransactionOperation,
) (interface{}, error) {
	return m.ExecuteWithOptions(ctx, operation, transactions.DefaultTransactionOptions())
}

// ExecuteWithOptions выполняет операцию в транзакции с указанными настройками
func (m *PostgresTransactionManager) ExecuteWithOptions(
	ctx context.Context,
	operation transactions.TransactionOperation,
	options transactions.TransactionOptions,
) (interface{}, error) {
	// Обрабатываем разные случаи поведения распространения
	if tx, exists := m.GetTransaction(ctx); exists {
		switch options.PropagationBehavior {
		case transactions.PropagationRequired, transactions.PropagationSupports:
			// Используем существующую транзакцию
			return operation(ctx)
		case transactions.PropagationRequiresNew:
			// Создаем новую транзакцию, игнорируя существующую
			return m.createAndExecuteTransaction(ctx, operation, options)
		case transactions.PropagationNested:
			// В PostgreSQL нет настоящих вложенных транзакций,
			// но мы можем использовать savepoint
			return m.executeWithSavepoint(ctx, tx, operation)
		case transactions.PropagationNever:
			// Ошибка, если транзакция существует
			return nil, fmt.Errorf("transaction exists but propagation behavior is NEVER")
		case transactions.PropagationNotSupported:
			// Выполняем вне транзакции
			return operation(ctx)
		case transactions.PropagationMandatory:
			// Транзакция существует, хорошо
			return operation(ctx)
		default:
			return nil, fmt.Errorf("unknown propagation behavior: %d", options.PropagationBehavior)
		}
	} else {
		// Транзакция не существует
		switch options.PropagationBehavior {
		case transactions.PropagationRequired, transactions.PropagationRequiresNew:
			// Создаем новую транзакцию
			return m.createAndExecuteTransaction(ctx, operation, options)
		case transactions.PropagationSupports, transactions.PropagationNotSupported, transactions.PropagationNever:
			// Выполняем без транзакции
			return operation(ctx)
		case transactions.PropagationNested, transactions.PropagationMandatory:
			// Ошибка, если транзакция не существует
			return nil, fmt.Errorf("no existing transaction found for propagation behavior %d", options.PropagationBehavior)
		default:
			return nil, fmt.Errorf("unknown propagation behavior: %d", options.PropagationBehavior)
		}
	}
}

// ExecuteWithTenant выполняет операцию в транзакции для указанного арендатора
func (m *PostgresTransactionManager) ExecuteWithTenant(
	ctx context.Context,
	operation transactions.TransactionOperation,
	tenantID string,
) (interface{}, error) {
	options := transactions.DefaultTransactionOptionsWithTenant(tenantID)
	return m.ExecuteWithOptions(ctx, operation, options)
}

// GetTransaction возвращает текущую транзакцию из контекста, если она существует
func (m *PostgresTransactionManager) GetTransaction(ctx context.Context) (transactions.Transaction, bool) {
	tx, ok := ctx.Value(txKey{}).(transactions.Transaction)
	return tx, ok
}

// HasActiveTransaction проверяет, есть ли активная транзакция в контексте
func (m *PostgresTransactionManager) HasActiveTransaction(ctx context.Context) bool {
	tx, ok := m.GetTransaction(ctx)
	return ok && tx.IsActive()
}

// WithTransaction добавляет транзакцию в контекст
func (m *PostgresTransactionManager) WithTransaction(ctx context.Context, tx transactions.Transaction) context.Context {
	return context.WithValue(ctx, txKey{}, tx)
}

// Вспомогательные методы

// createAndExecuteTransaction создает новую транзакцию и выполняет операцию
func (m *PostgresTransactionManager) createAndExecuteTransaction(
	ctx context.Context,
	operation transactions.TransactionOperation,
	options transactions.TransactionOptions,
) (interface{}, error) {
	// Устанавливаем уровень изоляции
	isolationLevel := sql.LevelDefault
	switch options.IsolationLevel {
	case transactions.LevelReadUncommitted:
		isolationLevel = sql.LevelReadUncommitted
	case transactions.LevelReadCommitted:
		isolationLevel = sql.LevelReadCommitted
	case transactions.LevelRepeatableRead:
		isolationLevel = sql.LevelRepeatableRead
	case transactions.LevelSerializable:
		isolationLevel = sql.LevelSerializable
	}

	// Создаем SQL транзакцию с указанным уровнем изоляции и режимом read-only
	txOptions := &sql.TxOptions{
		Isolation: isolationLevel,
		ReadOnly:  options.ReadOnly,
	}

	// Создаем контекст с таймаутом, если указан
	var txCtx context.Context
	var cancel context.CancelFunc
	if options.Timeout > 0 {
		txCtx, cancel = context.WithTimeout(ctx, time.Duration(options.Timeout)*time.Second)
		defer cancel()
	} else {
		txCtx = ctx
	}

	// Начинаем транзакцию
	sqlTx, err := m.db.BeginTx(txCtx, txOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Создаем объект нашей транзакции
	tx := NewPostgresTransaction(
		sqlTx,
		options.TenantID,
		options.IsolationLevel,
		options.ReadOnly,
	)

	// Добавляем транзакцию в контекст
	txCtx = m.WithTransaction(txCtx, tx)

	// Если арендатор указан, устанавливаем схему для данного арендатора
	if options.TenantID != "" {
		_, err := sqlTx.ExecContext(txCtx, fmt.Sprintf("SET LOCAL search_path TO tenant_%s, public", options.TenantID))
		if err != nil {
			_ = tx.Rollback()
			return nil, fmt.Errorf("failed to set tenant schema: %w", err)
		}
	}

	// Выполняем операцию
	result, err := operation(txCtx)

	// В зависимости от результата, фиксируем или откатываем транзакцию
	if err != nil {
		m.logger.Error("Transaction failed, rolling back", "error", err, "txID", tx.GetID())
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			m.logger.Error("Failed to rollback transaction", "rollbackError", rollbackErr, "txID", tx.GetID())
		}
		return nil, err
	}

	// Фиксируем транзакцию
	if err := tx.Commit(); err != nil {
		m.logger.Error("Failed to commit transaction", "error", err, "txID", tx.GetID())
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return result, nil
}

// executeWithSavepoint выполняет операцию с использованием точки сохранения
func (m *PostgresTransactionManager) executeWithSavepoint(
	ctx context.Context,
	tx transactions.Transaction,
	operation transactions.TransactionOperation,
) (interface{}, error) {
	pgTx, ok := tx.(*PostgresTransaction)
	if !ok {
		return nil, fmt.Errorf("transaction is not a PostgresTransaction")
	}

	sqlTx := pgTx.GetSQLTransaction()

	// Создаем уникальное имя для точки сохранения
	savepointName := fmt.Sprintf("sp_%s", uuid.New().String())

	// Создаем точку сохранения
	_, err := sqlTx.ExecContext(ctx, fmt.Sprintf("SAVEPOINT %s", savepointName))
	if err != nil {
		return nil, fmt.Errorf("failed to create savepoint: %w", err)
	}

	// Выполняем операцию
	result, err := operation(ctx)

	// В зависимости от результата, фиксируем или откатываем к точке сохранения
	if err != nil {
		_, rollbackErr := sqlTx.ExecContext(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", savepointName))
		if rollbackErr != nil {
			m.logger.Error("Failed to rollback to savepoint",
				"rollbackError", rollbackErr,
				"savepointName", savepointName,
				"txID", tx.GetID())
		}
		return nil, err
	}

	// Освобождаем точку сохранения
	_, err = sqlTx.ExecContext(ctx, fmt.Sprintf("RELEASE SAVEPOINT %s", savepointName))
	if err != nil {
		m.logger.Warn("Failed to release savepoint",
			"error", err,
			"savepointName", savepointName,
			"txID", tx.GetID())
	}

	return result, nil
}
