package interfaces

import "gomarketplace_api/internal/infrastructure/transactions/options"

// Transaction определяет интерфейс для работы с конкретной транзакцией
type Transaction interface {
	// Commit фиксирует транзакцию
	Commit() error

	// Rollback откатывает транзакцию
	Rollback() error

	// IsActive проверяет, активна ли транзакция
	IsActive() bool

	// GetID возвращает уникальный идентификатор транзакции
	GetID() string

	// GetTenantID возвращает идентификатор арендатора для транзакции
	GetTenantID() string

	// GetIsolationLevel возвращает уровень изоляции транзакции
	GetIsolationLevel() options.IsolationLevel

	// IsReadOnly проверяет, является ли транзакция транзакцией только для чтения
	IsReadOnly() bool
}
