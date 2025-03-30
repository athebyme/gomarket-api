package ports

import (
	"context"
	"gomarketplace_api/internal/infrastructure/transactions/interfaces"
	"gomarketplace_api/internal/infrastructure/transactions/options"
)

// TransactionPort расширяет обычный TransactionManager, добавляя поддержку для
// распределенных транзакций и интеграцию с другими портами системы
type TransactionPort interface {
	// Расширяет стандартный TransactionManager
	interfaces.TransactionManager

	// ExecuteInTransaction выполняет операцию внутри транзакции, предоставляя
	// согласованные транзакционные порты для всех необходимых подсистем
	ExecuteInTransaction(
		ctx context.Context,
		operation func(ctx context.Context, tx TransactionalPorts) (interface{}, error),
	) (interface{}, error)

	// ExecuteInTransactionWithOptions аналогично ExecuteInTransaction, но с указанием опций транзакции
	ExecuteInTransactionWithOptions(
		ctx context.Context,
		operation func(ctx context.Context, tx TransactionalPorts) (interface{}, error),
		options options.TransactionOptions,
	) (interface{}, error)

	// ExecuteInTransactionWithTenant аналогично ExecuteInTransaction, но с указанием ID арендатора
	ExecuteInTransactionWithTenant(
		ctx context.Context,
		operation func(ctx context.Context, tx TransactionalPorts) (interface{}, error),
		tenantID string,
	) (interface{}, error)

	// GetTransactionalPorts возвращает транзакционные порты для текущей активной транзакции
	GetTransactionalPorts(ctx context.Context) (TransactionalPorts, error)
}

// TransactionalPorts предоставляет доступ к транзакционным версиям всех портов
// Гарантирует, что все операции будут выполнены в рамках одной транзакции
type TransactionalPorts struct {
	// Транзакционное хранилище
	Storage StoragePort

	// Транзакционный кэш (для согласованного кэширования внутри транзакции)
	Cache CachePort

	// Транзакционный брокер сообщений (сообщения отправляются только при фиксации транзакции)
	Messaging MessagingPort

	// Текущая транзакция
	Transaction interfaces.Transaction
}

// TransactionalStoragePort расширяет StoragePort, добавляя методы для работы с транзакциями
type TransactionalStoragePort interface {
	StoragePort

	// WithTransaction возвращает хранилище, связанное с указанной транзакцией
	WithTransaction(tx interfaces.Transaction) StoragePort
}

// TransactionalCachePort расширяет CachePort, добавляя методы для работы с транзакциями
type TransactionalCachePort interface {
	CachePort

	// WithTransaction возвращает кэш, связанный с указанной транзакцией
	WithTransaction(tx interfaces.Transaction) CachePort

	// FlushTransactionCache сбрасывает кэш для указанной транзакции в основной кэш при фиксации
	FlushTransactionCache(tx interfaces.Transaction) error
}

// TransactionalMessagingPort расширяет MessagingPort, добавляя методы для работы с транзакциями
type TransactionalMessagingPort interface {
	MessagingPort

	// WithTransaction возвращает брокер сообщений, связанный с указанной транзакцией
	WithTransaction(tx interfaces.Transaction) MessagingPort

	// CommitMessages отправляет все сообщения, накопленные в рамках транзакции
	CommitMessages(tx interfaces.Transaction) error

	// DiscardMessages отменяет все сообщения, накопленные в рамках транзакции
	DiscardMessages(tx interfaces.Transaction) error
}
