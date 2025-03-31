package transactions

import (
	"context"
	"sync"
	"time"

	"gomarketplace_api/internal/core/ports"
	"gomarketplace_api/internal/infrastructure/transactions/interfaces"
	"gorm.io/gorm"
)

// TransactionalStorageAdapter адаптирует StoragePort для работы с транзакциями
type TransactionalStorageAdapter struct {
	baseStorage ports.StoragePort
}

func NewTransactionalStorageAdapter(storage ports.StoragePort) *TransactionalStorageAdapter {
	return &TransactionalStorageAdapter{
		baseStorage: storage,
	}
}

func (a *TransactionalStorageAdapter) WithTransaction(tx interfaces.Transaction) ports.StoragePort {
	// Приводим транзакцию к нашему типу
	transaction, ok := tx.(*Transaction)
	if !ok {
		// Если не удалось привести, возвращаем базовый порт
		return a.baseStorage
	}

	// Возвращаем транзакционную обертку с GORM-транзакцией
	return &TransactionalGormStorage{
		baseStorage: a.baseStorage,
		tx:          transaction.db,
	}
}

// Реализуем все методы StoragePort
type TransactionalGormStorage struct {
	baseStorage ports.StoragePort
	tx          *gorm.DB
}

// Перенаправляем все вызовы на транзакционное соединение

func (s *TransactionalGormStorage) SaveProduct(ctx context.Context, product *ports.Product, tenantID string) error {
	// Реализация с использованием транзакции
	// TODO: Реализовать
	return nil
}

func (s *TransactionalGormStorage) GetProduct(ctx context.Context, productID string, tenantID string) (*ports.Product, error) {
	// TODO: Реализовать
	return nil, nil
}

// ... реализуем все остальные методы StoragePort ...

// TransactionalCacheAdapter адаптирует CachePort для работы с транзакциями
type TransactionalCacheAdapter struct {
	baseCache ports.CachePort
	mutex     sync.RWMutex
	txCache   map[string]map[string][]byte // map[tx_id]map[cache_key]cache_value
}

func NewTransactionalCacheAdapter(cache ports.CachePort) *TransactionalCacheAdapter {
	return &TransactionalCacheAdapter{
		baseCache: cache,
		txCache:   make(map[string]map[string][]byte),
	}
}

func (a *TransactionalCacheAdapter) WithTransaction(tx interfaces.Transaction) ports.CachePort {
	// Создаем кэш для транзакции, если его еще нет
	a.mutex.Lock()
	defer a.mutex.Unlock()

	txID := tx.GetID()
	if _, exists := a.txCache[txID]; !exists {
		a.txCache[txID] = make(map[string][]byte)
	}

	return &TransactionalCache{
		baseCache: a.baseCache,
		adapter:   a,
		txID:      txID,
	}
}

func (a *TransactionalCacheAdapter) FlushTransactionCache(tx interfaces.Transaction) error {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	txID := tx.GetID()
	txCache, exists := a.txCache[txID]
	if !exists {
		return nil
	}

	// Копируем данные из транзакционного кэша в основной
	ctx := context.Background()
	for key, value := range txCache {
		if err := a.baseCache.Set(ctx, key, value, 0); err != nil {
			return err
		}
	}

	// Удаляем транзакционный кэш
	delete(a.txCache, txID)
	return nil
}

// Реализуем кэш для транзакций
type TransactionalCache struct {
	baseCache ports.CachePort
	adapter   *TransactionalCacheAdapter
	txID      string
}

func (c *TransactionalCache) Get(ctx context.Context, key string) ([]byte, error) {
	// Сначала проверяем транзакционный кэш
	c.adapter.mutex.RLock()
	txCache, exists := c.adapter.txCache[c.txID]
	if exists {
		value, exists := txCache[key]
		c.adapter.mutex.RUnlock()
		if exists {
			return value, nil
		}
	} else {
		c.adapter.mutex.RUnlock()
	}

	// Если в транзакционном кэше нет, идем в основной
	return c.baseCache.Get(ctx, key)
}

func (c *TransactionalCache) Set(ctx context.Context, key string, value []byte, expiration time.Duration) error {
	// Сохраняем в транзакционный кэш
	c.adapter.mutex.Lock()
	txCache, exists := c.adapter.txCache[c.txID]
	if !exists {
		txCache = make(map[string][]byte)
		c.adapter.txCache[c.txID] = txCache
	}
	txCache[key] = value
	c.adapter.mutex.Unlock()

	return nil
}

// ... реализуем все остальные методы CachePort ...

// TransactionalMessagingAdapter адаптирует MessagingPort для работы с транзакциями
type TransactionalMessagingAdapter struct {
	baseMessaging ports.MessagingPort
	mutex         sync.RWMutex
	txMessages    map[string][]PendingMessage // map[tx_id][]messages
}

type PendingMessage struct {
	Topic    string
	Key      string
	Value    []byte
	Headers  map[string]string
	TenantID string
}

func NewTransactionalMessagingAdapter(messaging ports.MessagingPort) *TransactionalMessagingAdapter {
	return &TransactionalMessagingAdapter{
		baseMessaging: messaging,
		txMessages:    make(map[string][]PendingMessage),
	}
}

func (a *TransactionalMessagingAdapter) WithTransaction(tx interfaces.Transaction) ports.MessagingPort {
	// Создаем буфер сообщений для транзакции, если его еще нет
	a.mutex.Lock()
	defer a.mutex.Unlock()

	txID := tx.GetID()
	if _, exists := a.txMessages[txID]; !exists {
		a.txMessages[txID] = make([]PendingMessage, 0)
	}

	return &TransactionalMessaging{
		baseMessaging: a.baseMessaging,
		adapter:       a,
		txID:          txID,
	}
}

func (a *TransactionalMessagingAdapter) CommitMessages(tx interfaces.Transaction) error {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	txID := tx.GetID()
	txMessages, exists := a.txMessages[txID]
	if !exists {
		return nil
	}

	// Отправляем все сообщения из буфера
	ctx := context.Background()
	for _, msg := range txMessages {
		var err error

		if msg.Key != "" {
			// Если есть ключ, используем его
			err = a.baseMessaging.PublishWithKey(ctx, msg.Topic, msg.Key, msg.Value)
		} else if len(msg.Headers) > 0 {
			// Если есть заголовки, используем их
			err = a.baseMessaging.PublishWithHeaders(ctx, msg.Topic, msg.Value, msg.Headers)
		} else if msg.TenantID != "" {
			// Если есть ID арендатора, используем его
			err = a.baseMessaging.PublishForTenant(ctx, msg.Topic, msg.Value, msg.TenantID)
		} else {
			// Иначе простая публикация
			err = a.baseMessaging.Publish(ctx, msg.Topic, msg.Value)
		}

		if err != nil {
			return err
		}
	}

	// Очищаем буфер сообщений
	delete(a.txMessages, txID)
	return nil
}

func (a *TransactionalMessagingAdapter) DiscardMessages(tx interfaces.Transaction) error {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	// Просто удаляем буфер сообщений
	delete(a.txMessages, tx.GetID())
	return nil
}

// Реализуем брокер сообщений для транзакций
type TransactionalMessaging struct {
	baseMessaging ports.MessagingPort
	adapter       *TransactionalMessagingAdapter
	txID          string
}

func (m *TransactionalMessaging) Publish(ctx context.Context, topic string, message []byte) error {
	// Добавляем сообщение в буфер
	m.adapter.mutex.Lock()
	defer m.adapter.mutex.Unlock()

	txMessages, exists := m.adapter.txMessages[m.txID]
	if !exists {
		txMessages = make([]PendingMessage, 0)
		m.adapter.txMessages[m.txID] = txMessages
	}

	m.adapter.txMessages[m.txID] = append(txMessages, PendingMessage{
		Topic: topic,
		Value: message,
	})

	return nil
}

func (m *TransactionalMessaging) PublishWithKey(ctx context.Context, topic string, key string, message []byte) error {
	// Добавляем сообщение с ключом в буфер
	m.adapter.mutex.Lock()
	defer m.adapter.mutex.Unlock()

	txMessages, exists := m.adapter.txMessages[m.txID]
	if !exists {
		txMessages = make([]PendingMessage, 0)
		m.adapter.txMessages[m.txID] = txMessages
	}

	m.adapter.txMessages[m.txID] = append(txMessages, PendingMessage{
		Topic: topic,
		Key:   key,
		Value: message,
	})

	return nil
}

// ... реализуем все остальные методы MessagingPort ...

// Убедимся, что все адаптеры реализуют соответствующие интерфейсы
var _ ports.TransactionalStoragePort = (*TransactionalStorageAdapter)(nil)
var _ ports.TransactionalCachePort = (*TransactionalCacheAdapter)(nil)
var _ ports.TransactionalMessagingPort = (*TransactionalMessagingAdapter)(nil)
