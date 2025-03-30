package interfaces

import (
	"context"
	"gomarketplace_api/internal/infrastructure/transactions/options"
)

// TransactionOperation определяет функцию, которая будет выполнена в транзакции
type TransactionOperation func(ctx context.Context) (interface{}, error)

// TransactionManager определяет интерфейс для управления транзакциями
type TransactionManager interface {
	// Execute выполняет операцию в транзакции с настройками по умолчанию
	// Если операция возвращает ошибку, транзакция откатывается,
	// иначе транзакция фиксируется
	Execute(ctx context.Context, operation TransactionOperation) (interface{}, error)

	// ExecuteWithOptions выполняет операцию в транзакции с указанными настройками
	ExecuteWithOptions(ctx context.Context, operation TransactionOperation, options options.TransactionOptions) (interface{}, error)

	// ExecuteWithTenant выполняет операцию в транзакции для указанного арендатора
	ExecuteWithTenant(ctx context.Context, operation TransactionOperation, tenantID string) (interface{}, error)

	// GetTransaction возвращает текущую транзакцию из контекста, если она существует
	GetTransaction(ctx context.Context) (Transaction, bool)

	// HasActiveTransaction проверяет, есть ли активная транзакция в контексте
	HasActiveTransaction(ctx context.Context) bool

	// WithTransaction добавляет транзакцию в контекст
	WithTransaction(ctx context.Context, tx Transaction) context.Context
}
