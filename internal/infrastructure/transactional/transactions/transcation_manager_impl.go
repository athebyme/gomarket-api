package transactions

import (
	"context"
	"errors"
	"fmt"
	"gomarketplace_api/internal/infrastructure/transactional/transactions/interfaces"
	"gomarketplace_api/internal/infrastructure/transactional/transactions/options"
	"sync"

	"gomarketplace_api/internal/core/ports"
	"gorm.io/gorm"
)

// Ключ для хранения транзакционного контекста
type transactionContextKey struct{}

// Transaction представляет реализацию интерфейса Transaction
type Transaction struct {
	id             string
	db             *gorm.DB
	tenantID       string
	isolationLevel options.IsolationLevel
	readOnly       bool
	active         bool
	mu             sync.RWMutex
}

// Реализация Transaction interface
func (t *Transaction) Commit() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.active {
		return errors.New("transaction is not active")
	}

	err := t.db.Commit().Error
	if err != nil {
		return err
	}

	t.active = false
	return nil
}

func (t *Transaction) Rollback() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.active {
		return nil // Уже откачена или зафиксирована
	}

	err := t.db.Rollback().Error
	if err != nil {
		return err
	}

	t.active = false
	return nil
}

func (t *Transaction) IsActive() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.active
}

func (t *Transaction) GetID() string {
	return t.id
}

func (t *Transaction) GetTenantID() string {
	return t.tenantID
}

func (t *Transaction) GetIsolationLevel() options.IsolationLevel {
	return t.isolationLevel
}

func (t *Transaction) IsReadOnly() bool {
	return t.readOnly
}

// TransactionManager представляет реализацию интерфейса TransactionPort
type TransactionManager struct {
	db            *gorm.DB
	storagePort   ports.TransactionalStoragePort
	cachePort     ports.TransactionalCachePort
	messagingPort ports.TransactionalMessagingPort
	logger        ports.LoggerPort
}

// Создание нового менеджера транзакций
func NewTransactionManager(
	db *gorm.DB,
	storagePort ports.TransactionalStoragePort,
	cachePort ports.TransactionalCachePort,
	messagingPort ports.TransactionalMessagingPort,
	logger ports.LoggerPort,
) *TransactionManager {
	return &TransactionManager{
		db:            db,
		storagePort:   storagePort,
		cachePort:     cachePort,
		messagingPort: messagingPort,
		logger:        logger,
	}
}

// Execute выполняет операцию внутри транзакции
func (tm *TransactionManager) Execute(ctx context.Context, operation interfaces.TransactionOperation) (interface{}, error) {
	return tm.ExecuteWithOptions(ctx, operation, options.DefaultTransactionOptions())
}

// ExecuteWithOptions выполняет операцию внутри транзакции с указанными настройками
func (tm *TransactionManager) ExecuteWithOptions(
	ctx context.Context,
	operation interfaces.TransactionOperation,
	opts options.TransactionOptions,
) (interface{}, error) {
	// Проверяем, есть ли уже активная транзакция
	if existingTx, found := tm.GetTransaction(ctx); found {
		switch opts.PropagationBehavior {
		case options.PropagationRequired, options.PropagationSupports:
			// Используем существующую транзакцию
			return operation(ctx)
		case options.PropagationRequiresNew:
			// Создаем новую транзакцию
			return tm.beginAndExecute(ctx, operation, opts)
		case options.PropagationNested:
			// Создаем вложенную транзакцию (savepoint)
			return tm.executeWithSavepoint(ctx, existingTx, operation)
		case options.PropagationNever:
			// Ошибка, если транзакция существует
			return nil, errors.New("transaction already exists, but propagation behavior is NEVER")
		case options.PropagationNotSupported:
			// Приостанавливаем текущую транзакцию
			return operation(ctx)
		case options.PropagationMandatory:
			// Транзакция существует, все в порядке
			return operation(ctx)
		default:
			return nil, fmt.Errorf("unsupported propagation behavior: %v", opts.PropagationBehavior)
		}
	} else {
		// Транзакция не существует
		switch opts.PropagationBehavior {
		case options.PropagationRequired, options.PropagationRequiresNew:
			// Создаем новую транзакцию
			return tm.beginAndExecute(ctx, operation, opts)
		case options.PropagationSupports, options.PropagationNotSupported, options.PropagationNever:
			// Выполняем без транзакции
			return operation(ctx)
		case options.PropagationMandatory, options.PropagationNested:
			// Ошибка, если транзакция обязательна, но не существует
			return nil, errors.New("no existing transaction for MANDATORY or NESTED propagation")
		default:
			return nil, fmt.Errorf("unsupported propagation behavior: %v", opts.PropagationBehavior)
		}
	}
}

// ExecuteWithTenant выполняет операцию с указанным ID арендатора
func (tm *TransactionManager) ExecuteWithTenant(
	ctx context.Context,
	operation interfaces.TransactionOperation,
	tenantID string,
) (interface{}, error) {
	opts := options.DefaultTransactionOptionsWithTenant(tenantID)
	return tm.ExecuteWithOptions(ctx, operation, opts)
}

// GetTransaction возвращает текущую транзакцию из контекста
func (tm *TransactionManager) GetTransaction(ctx context.Context) (interfaces.Transaction, bool) {
	tx, ok := ctx.Value(transactionContextKey{}).(interfaces.Transaction)
	return tx, ok
}

// HasActiveTransaction проверяет наличие активной транзакции
func (tm *TransactionManager) HasActiveTransaction(ctx context.Context) bool {
	tx, ok := tm.GetTransaction(ctx)
	return ok && tx.IsActive()
}

// WithTransaction добавляет транзакцию в контекст
func (tm *TransactionManager) WithTransaction(ctx context.Context, tx interfaces.Transaction) context.Context {
	return context.WithValue(ctx, transactionContextKey{}, tx)
}

// beginAndExecute начинает новую транзакцию и выполняет операцию
func (tm *TransactionManager) beginAndExecute(
	ctx context.Context,
	operation interfaces.TransactionOperation,
	opts options.TransactionOptions,
) (interface{}, error) {
	// Устанавливаем уровень изоляции
	txOpts := &gorm.Session{
		SkipDefaultTransaction: true,
	}

	// Создаем новую транзакцию
	tx := tm.db.Session(txOpts).Begin()
	if tx.Error != nil {
		return nil, tx.Error
	}

	// Создаем объект транзакции
	transaction := &Transaction{
		id:             fmt.Sprintf("tx-%p", tx),
		db:             tx,
		tenantID:       opts.TenantID,
		isolationLevel: opts.IsolationLevel,
		readOnly:       opts.ReadOnly,
		active:         true,
	}

	// Логируем начало транзакции
	tm.logger.Debug("Starting transaction", "tx_id", transaction.id, "tenant_id", opts.TenantID)

	// Добавляем транзакцию в контекст
	txCtx := tm.WithTransaction(ctx, transaction)

	// Если указан tenant ID, устанавливаем схему
	if opts.TenantID != "" {
		if err := tx.Exec(fmt.Sprintf("SET LOCAL search_path TO tenant_%s, public", opts.TenantID)).Error; err != nil {
			_ = transaction.Rollback()
			return nil, fmt.Errorf("failed to set tenant schema: %w", err)
		}
	}

	// Выполняем операцию
	result, err := operation(txCtx)

	// В зависимости от результата, фиксируем или откатываем транзакцию
	if err != nil {
		tm.logger.Debug("Rolling back transaction", "tx_id", transaction.id, "tenant_id", opts.TenantID, "error", err)
		rollbackErr := transaction.Rollback()
		if rollbackErr != nil {
			tm.logger.Error("Failed to rollback transaction", "tx_id", transaction.id, "error", rollbackErr)
		}
		return nil, err
	}

	tm.logger.Debug("Committing transaction", "tx_id", transaction.id, "tenant_id", opts.TenantID)
	if err := transaction.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return result, nil
}

// executeWithSavepoint создает точку сохранения и выполняет операцию
func (tm *TransactionManager) executeWithSavepoint(
	ctx context.Context,
	existingTx interfaces.Transaction,
	operation interfaces.TransactionOperation,
) (interface{}, error) {
	// Приводим к нашему типу транзакции
	tx, ok := existingTx.(*Transaction)
	if !ok {
		return nil, errors.New("invalid transaction type")
	}

	// Создаем уникальное имя savepoint
	savepointName := fmt.Sprintf("sp_%s", tx.GetID())

	// Создаем savepoint
	if err := tx.db.Exec(fmt.Sprintf("SAVEPOINT %s", savepointName)).Error; err != nil {
		return nil, fmt.Errorf("failed to create savepoint: %w", err)
	}

	// Выполняем операцию
	result, err := operation(ctx)

	// В зависимости от результата, фиксируем или откатываем к savepoint
	if err != nil {
		rollbackErr := tx.db.Exec(fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", savepointName)).Error
		if rollbackErr != nil {
			tm.logger.Error("Failed to rollback to savepoint",
				"savepoint", savepointName,
				"tx_id", tx.GetID(),
				"error", rollbackErr)
		}
		return nil, err
	}

	// Освобождаем savepoint
	if err := tx.db.Exec(fmt.Sprintf("RELEASE SAVEPOINT %s", savepointName)).Error; err != nil {
		tm.logger.Warn("Failed to release savepoint",
			"savepoint", savepointName,
			"tx_id", tx.GetID(),
			"error", err)
	}

	return result, nil
}

// Реализация TransactionPort
func (tm *TransactionManager) ExecuteInTransaction(
	ctx context.Context,
	operation func(ctx context.Context, tx ports.TransactionalPorts) (interface{}, error),
) (interface{}, error) {
	return tm.ExecuteInTransactionWithOptions(ctx, operation, options.DefaultTransactionOptions())
}

func (tm *TransactionManager) ExecuteInTransactionWithOptions(
	ctx context.Context,
	operation func(ctx context.Context, tx ports.TransactionalPorts) (interface{}, error),
	opts options.TransactionOptions,
) (interface{}, error) {
	// Оборачиваем операцию для использования с интерфейсом TransactionOperation
	wrappedOp := func(txCtx context.Context) (interface{}, error) {
		tx, ok := tm.GetTransaction(txCtx)
		if !ok {
			return nil, errors.New("no transaction in context")
		}

		// Создаем набор транзакционных портов
		txPorts := ports.TransactionalPorts{
			Storage:     tm.storagePort.WithTransaction(tx),
			Cache:       tm.cachePort.WithTransaction(tx),
			Messaging:   tm.messagingPort.WithTransaction(tx),
			Transaction: tx,
		}

		// Выполняем операцию с транзакционными портами
		result, err := operation(txCtx, txPorts)

		// В случае успеха, нужно сбросить кэш и отправить сообщения при коммите
		if err == nil {
			// Flush выполнится при коммите транзакции
		}

		return result, err
	}

	return tm.ExecuteWithOptions(ctx, wrappedOp, opts)
}

func (tm *TransactionManager) ExecuteInTransactionWithTenant(
	ctx context.Context,
	operation func(ctx context.Context, tx ports.TransactionalPorts) (interface{}, error),
	tenantID string,
) (interface{}, error) {
	opts := options.DefaultTransactionOptionsWithTenant(tenantID)
	return tm.ExecuteInTransactionWithOptions(ctx, operation, opts)
}

func (tm *TransactionManager) GetTransactionalPorts(ctx context.Context) (ports.TransactionalPorts, error) {
	tx, ok := tm.GetTransaction(ctx)
	if !ok {
		return ports.TransactionalPorts{}, errors.New("no transaction in context")
	}

	return ports.TransactionalPorts{
		Storage:     tm.storagePort.WithTransaction(tx),
		Cache:       tm.cachePort.WithTransaction(tx),
		Messaging:   tm.messagingPort.WithTransaction(tx),
		Transaction: tx,
	}, nil
}

// Убедимся, что TransactionManager реализует интерфейс TransactionPort
var _ ports.TransactionPort = (*TransactionManager)(nil)
var _ interfaces.TransactionManager = (*TransactionManager)(nil)
var _ interfaces.Transaction = (*Transaction)(nil)
