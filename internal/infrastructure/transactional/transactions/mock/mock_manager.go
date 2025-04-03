package mock

import (
	"context"
	"fmt"
	"gomarketplace_api/internal/infrastructure/transactional/transactions/interfaces"
	"gomarketplace_api/internal/infrastructure/transactional/transactions/options"
	"sync"

	"github.com/google/uuid"
	"gomarketplace_api/internal/core/ports"
)

// MockTransaction реализует интерфейс Transaction для тестирования
type MockTransaction struct {
	id         string
	tenantID   string
	isolation  options.IsolationLevel
	readOnly   bool
	active     bool
	committed  bool
	rolledBack bool
	mutex      sync.RWMutex
}

// NewMockTransaction создает новую мок-транзакцию
func NewMockTransaction(tenantID string, isolation options.IsolationLevel, readOnly bool) *MockTransaction {
	return &MockTransaction{
		id:         uuid.New().String(),
		tenantID:   tenantID,
		isolation:  isolation,
		readOnly:   readOnly,
		active:     true,
		committed:  false,
		rolledBack: false,
		mutex:      sync.RWMutex{},
	}
}

func (t *MockTransaction) Commit() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if !t.active {
		return nil
	}

	t.active = false
	t.committed = true
	return nil
}

func (t *MockTransaction) Rollback() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if !t.active {
		return nil
	}

	t.active = false
	t.rolledBack = true
	return nil
}

func (t *MockTransaction) IsActive() bool {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.active
}

func (t *MockTransaction) GetID() string {
	return t.id
}

func (t *MockTransaction) GetTenantID() string {
	return t.tenantID
}

func (t *MockTransaction) GetIsolationLevel() options.IsolationLevel {
	return t.isolation
}

func (t *MockTransaction) IsReadOnly() bool {
	return t.readOnly
}

func (t *MockTransaction) WasCommitted() bool {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.committed
}

func (t *MockTransaction) WasRolledBack() bool {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.rolledBack
}

// MockTransactionManager реализует интерфейс TransactionPort для тестирования
type MockTransactionManager struct {
	transactions       map[string]*MockTransaction
	operationResults   map[string]interface{}
	operationErrors    map[string]error
	executedOperations []string
	mockStorage        ports.StoragePort
	mockCache          ports.CachePort
	mockMessaging      ports.MessagingPort
	mutex              sync.RWMutex
}

// NewMockTransactionManager создает новый мок-менеджер транзакций
func NewMockTransactionManager(
	mockStorage ports.StoragePort,
	mockCache ports.CachePort,
	mockMessaging ports.MessagingPort,
) *MockTransactionManager {
	return &MockTransactionManager{
		transactions:       make(map[string]*MockTransaction),
		operationResults:   make(map[string]interface{}),
		operationErrors:    make(map[string]error),
		executedOperations: make([]string, 100),
		mockStorage:        mockStorage,
		mockCache:          mockCache,
		mockMessaging:      mockMessaging,
		mutex:              sync.RWMutex{},
	}
}

// SetupOperationResult настраивает результат для конкретной операции
func (m *MockTransactionManager) SetupOperationResult(operationName string, result interface{}, err error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.operationResults[operationName] = result
	m.operationErrors[operationName] = err
}

// GetExecutedOperations возвращает список выполненных операций
func (m *MockTransactionManager) GetExecutedOperations() []string {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	return append([]string{}, m.executedOperations...)
}

// GetTransactionByID возвращает транзакцию по идентификатору
func (m *MockTransactionManager) GetTransactionByID(id string) (*MockTransaction, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	tx, exists := m.transactions[id]
	return tx, exists
}

// Реализация TransactionManager

func (m *MockTransactionManager) Execute(ctx context.Context, operation interfaces.TransactionOperation) (interface{}, error) {
	return m.ExecuteWithOptions(ctx, operation, options.DefaultTransactionOptions())
}

func (m *MockTransactionManager) ExecuteWithOptions(
	ctx context.Context,
	operation interfaces.TransactionOperation,
	options options.TransactionOptions,
) (interface{}, error) {
	m.mutex.Lock()

	// Создаем новую транзакцию
	tx := NewMockTransaction(options.TenantID, options.IsolationLevel, options.ReadOnly)
	m.transactions[tx.GetID()] = tx

	// Добавляем транзакцию в контекст
	ctx = context.WithValue(ctx, txKey{}, tx)

	// Записываем выполнение операции
	operationName := getOperationName(operation)
	m.executedOperations = append(m.executedOperations, operationName)

	// Проверяем, есть ли заранее настроенный результат
	result, hasResult := m.operationResults[operationName]
	err, hasError := m.operationErrors[operationName]

	m.mutex.Unlock()

	// Если у нас есть заранее настроенный результат - используем его
	if hasResult || hasError {
		if hasError && err != nil {
			_ = tx.Rollback()
			return nil, err
		}

		_ = tx.Commit()
		return result, nil
	}

	// Иначе выполняем операцию
	result, err = operation(ctx)

	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}

	_ = tx.Commit()
	return result, nil
}

func (m *MockTransactionManager) ExecuteWithTenant(
	ctx context.Context,
	operation interfaces.TransactionOperation,
	tenantID string,
) (interface{}, error) {
	options := options.DefaultTransactionOptionsWithTenant(tenantID)
	return m.ExecuteWithOptions(ctx, operation, options)
}

func (m *MockTransactionManager) GetTransaction(ctx context.Context) (interfaces.Transaction, bool) {
	tx, ok := ctx.Value(txKey{}).(interfaces.Transaction)
	return tx, ok
}

func (m *MockTransactionManager) HasActiveTransaction(ctx context.Context) bool {
	tx, ok := m.GetTransaction(ctx)
	return ok && tx.IsActive()
}

func (m *MockTransactionManager) WithTransaction(ctx context.Context, tx interfaces.Transaction) context.Context {
	return context.WithValue(ctx, txKey{}, tx)
}

// Реализация TransactionPort

func (m *MockTransactionManager) ExecuteInTransaction(
	ctx context.Context,
	operation func(ctx context.Context, tx ports.TransactionalPorts) (interface{}, error),
) (interface{}, error) {
	return m.ExecuteInTransactionWithOptions(ctx, operation, options.DefaultTransactionOptions())
}

func (m *MockTransactionManager) ExecuteInTransactionWithOptions(
	ctx context.Context,
	operation func(ctx context.Context, tx ports.TransactionalPorts) (interface{}, error),
	options options.TransactionOptions,
) (interface{}, error) {
	return m.ExecuteWithOptions(ctx, func(ctx context.Context) (interface{}, error) {
		tx, _ := m.GetTransaction(ctx)

		txPorts := ports.TransactionalPorts{
			Storage:     m.mockStorage,
			Cache:       m.mockCache,
			Messaging:   m.mockMessaging,
			Transaction: tx,
		}

		return operation(ctx, txPorts)
	}, options)
}

func (m *MockTransactionManager) ExecuteInTransactionWithTenant(
	ctx context.Context,
	operation func(ctx context.Context, tx ports.TransactionalPorts) (interface{}, error),
	tenantID string,
) (interface{}, error) {
	options := options.DefaultTransactionOptionsWithTenant(tenantID)
	return m.ExecuteInTransactionWithOptions(ctx, operation, options)
}

func (m *MockTransactionManager) GetTransactionalPorts(ctx context.Context) (ports.TransactionalPorts, error) {
	tx, ok := m.GetTransaction(ctx)
	if !ok {
		return ports.TransactionalPorts{}, fmt.Errorf("no active transaction in context")
	}

	return ports.TransactionalPorts{
		Storage:     m.mockStorage,
		Cache:       m.mockCache,
		Messaging:   m.mockMessaging,
		Transaction: tx,
	}, nil
}

// Вспомогательные функции и типы

type txKey struct{}

func getOperationName(operation interface{}) string {
	// В реальном коде здесь можно использовать reflection для получения имени функции
	// Или еще лучше - использовать какой-то более надежный идентификатор операции
	return fmt.Sprintf("operation-%p", operation)
}
