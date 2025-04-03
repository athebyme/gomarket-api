package transactions

import (
	"context"
	"gomarketplace_api/internal/infrastructure/transactional/transactions/interfaces"
	"sync"
	"time"

	"gomarketplace_api/internal/core/ports"
)

// PendingMessage представляет сообщение, ожидающее отправки в рамках транзакции
type PendingMessage struct {
	Topic     string
	Key       string
	Value     []byte
	Headers   map[string]string
	TenantID  string
	CreatedAt time.Time
}

// TransactionalMessagingAdapter адаптер для брокера сообщений с поддержкой транзакций
type TransactionalMessagingAdapter struct {
	baseMessaging ports.MessagingPort
	mutex         sync.RWMutex
	pendingMsgs   map[string][]PendingMessage // map[tx_id][]messages
	logger        ports.LoggerPort
}

// NewTransactionalMessagingAdapter создает новый экземпляр TransactionalMessagingAdapter
func NewTransactionalMessagingAdapter(messaging ports.MessagingPort) ports.TransactionalMessagingPort {
	return &TransactionalMessagingAdapter{
		baseMessaging: messaging,
		pendingMsgs:   make(map[string][]PendingMessage),
	}
}

// WithTransaction возвращает брокер сообщений для указанной транзакции
func (a *TransactionalMessagingAdapter) WithTransaction(tx interfaces.Transaction) ports.MessagingPort {
	// Создаем буфер для сообщений транзакции, если его еще нет
	a.mutex.Lock()
	txID := tx.GetID()
	if _, exists := a.pendingMsgs[txID]; !exists {
		a.pendingMsgs[txID] = make([]PendingMessage, 0)
	}
	a.mutex.Unlock()

	return &TransactionalMessaging{
		baseMessaging: a.baseMessaging,
		adapter:       a,
		txID:          txID,
		tenantID:      tx.GetTenantID(),
	}
}

// CommitMessages отправляет все сообщения, накопленные в рамках транзакции
func (a *TransactionalMessagingAdapter) CommitMessages(tx interfaces.Transaction) error {
	txID := tx.GetID()

	a.mutex.Lock()
	messages, exists := a.pendingMsgs[txID]
	delete(a.pendingMsgs, txID)
	a.mutex.Unlock()

	if !exists || len(messages) == 0 {
		return nil
	}

	ctx := context.Background()
	var lastErr error

	// Отправляем все накопленные сообщения
	for _, msg := range messages {
		var err error
		if msg.Headers != nil && len(msg.Headers) > 0 {
			err = a.baseMessaging.PublishWithHeaders(ctx, msg.Topic, msg.Value, msg.Headers)
		} else if msg.Key != "" {
			err = a.baseMessaging.PublishWithKey(ctx, msg.Topic, msg.Key, msg.Value)
		} else if msg.TenantID != "" {
			err = a.baseMessaging.PublishForTenant(ctx, msg.Topic, msg.Value, msg.TenantID)
		} else {
			err = a.baseMessaging.Publish(ctx, msg.Topic, msg.Value)
		}

		if err != nil {
			if a.logger != nil {
				a.logger.Error("Ошибка при отправке сообщения после фиксации транзакции",
					"txID", txID,
					"topic", msg.Topic,
					"error", err)
			}
			lastErr = err
		}
	}

	return lastErr
}

// DiscardMessages отменяет все сообщения, накопленные в рамках транзакции
func (a *TransactionalMessagingAdapter) DiscardMessages(tx interfaces.Transaction) error {
	txID := tx.GetID()

	a.mutex.Lock()
	delete(a.pendingMsgs, txID)
	a.mutex.Unlock()

	if a.logger != nil {
		a.logger.Debug("Отменены сообщения транзакции", "txID", txID)
	}

	return nil
}

// SetLogger устанавливает логгер для адаптера
func (a *TransactionalMessagingAdapter) SetLogger(logger ports.LoggerPort) {
	a.logger = logger
}

// GetPendingMessageCount возвращает количество сообщений, ожидающих отправки
func (a *TransactionalMessagingAdapter) GetPendingMessageCount(txID string) int {
	a.mutex.RLock()
	defer a.mutex.RUnlock()

	if messages, exists := a.pendingMsgs[txID]; exists {
		return len(messages)
	}
	return 0
}

// GetStats возвращает статистику по транзакционным сообщениям
func (a *TransactionalMessagingAdapter) GetStats() map[string]interface{} {
	a.mutex.RLock()
	defer a.mutex.RUnlock()

	stats := make(map[string]interface{})
	stats["tx_count"] = len(a.pendingMsgs)

	txStats := make(map[string]interface{})
	for txID, messages := range a.pendingMsgs {
		txStat := make(map[string]interface{})
		txStat["message_count"] = len(messages)

		if len(messages) > 0 {
			oldestMsg := messages[0].CreatedAt
			txStat["oldest_message"] = oldestMsg.String()
			txStat["age"] = time.Since(oldestMsg).String()
		}

		txStats[txID] = txStat
	}
	stats["transactions"] = txStats

	return stats
}

// TransactionalMessaging реализует интерфейс MessagingPort для транзакций
type TransactionalMessaging struct {
	baseMessaging ports.MessagingPort
	adapter       *TransactionalMessagingAdapter
	txID          string
	tenantID      string
}

// Publish буферизирует сообщение для отправки при фиксации транзакции
func (m *TransactionalMessaging) Publish(ctx context.Context, topic string, message []byte) error {
	m.adapter.mutex.Lock()
	defer m.adapter.mutex.Unlock()

	m.adapter.pendingMsgs[m.txID] = append(m.adapter.pendingMsgs[m.txID], PendingMessage{
		Topic:     topic,
		Value:     message,
		CreatedAt: time.Now(),
	})

	return nil
}

// PublishWithKey буферизирует сообщение с ключом для отправки при фиксации транзакции
func (m *TransactionalMessaging) PublishWithKey(ctx context.Context, topic string, key string, message []byte) error {
	m.adapter.mutex.Lock()
	defer m.adapter.mutex.Unlock()

	m.adapter.pendingMsgs[m.txID] = append(m.adapter.pendingMsgs[m.txID], PendingMessage{
		Topic:     topic,
		Key:       key,
		Value:     message,
		CreatedAt: time.Now(),
	})

	return nil
}

// PublishWithHeaders буферизирует сообщение с заголовками для отправки при фиксации транзакции
func (m *TransactionalMessaging) PublishWithHeaders(ctx context.Context, topic string, message []byte, headers map[string]string) error {
	m.adapter.mutex.Lock()
	defer m.adapter.mutex.Unlock()

	m.adapter.pendingMsgs[m.txID] = append(m.adapter.pendingMsgs[m.txID], PendingMessage{
		Topic:     topic,
		Value:     message,
		Headers:   headers,
		CreatedAt: time.Now(),
	})

	return nil
}

// PublishForTenant буферизирует сообщение для арендатора для отправки при фиксации транзакции
func (m *TransactionalMessaging) PublishForTenant(ctx context.Context, topic string, message []byte, tenantID string) error {
	m.adapter.mutex.Lock()
	defer m.adapter.mutex.Unlock()

	m.adapter.pendingMsgs[m.txID] = append(m.adapter.pendingMsgs[m.txID], PendingMessage{
		Topic:     topic,
		Value:     message,
		TenantID:  tenantID,
		CreatedAt: time.Now(),
	})

	return nil
}

// PublishBatch буферизирует несколько сообщений для отправки при фиксации транзакции
func (m *TransactionalMessaging) PublishBatch(ctx context.Context, topic string, messages [][]byte) error {
	m.adapter.mutex.Lock()
	defer m.adapter.mutex.Unlock()

	now := time.Now()
	for _, message := range messages {
		m.adapter.pendingMsgs[m.txID] = append(m.adapter.pendingMsgs[m.txID], PendingMessage{
			Topic:     topic,
			Value:     message,
			CreatedAt: now,
		})
	}

	return nil
}

// PublishBatchWithKeys буферизирует несколько сообщений с ключами для отправки при фиксации транзакции
func (m *TransactionalMessaging) PublishBatchWithKeys(ctx context.Context, topic string, keyedMessages map[string][]byte) error {
	m.adapter.mutex.Lock()
	defer m.adapter.mutex.Unlock()

	now := time.Now()
	for key, message := range keyedMessages {
		m.adapter.pendingMsgs[m.txID] = append(m.adapter.pendingMsgs[m.txID], PendingMessage{
			Topic:     topic,
			Key:       key,
			Value:     message,
			CreatedAt: now,
		})
	}

	return nil
}

// Методы для подписки и получения сообщений делегируются базовому MessagingPort,
// так как они не влияют на транзакционность отправки

// Subscribe делегирует вызов базовому MessagingPort
func (m *TransactionalMessaging) Subscribe(ctx context.Context, topic string, handler ports.MessageHandler) (func() error, error) {
	return m.baseMessaging.Subscribe(ctx, topic, handler)
}

// SubscribeWithConfig делегирует вызов базовому MessagingPort
func (m *TransactionalMessaging) SubscribeWithConfig(ctx context.Context, topic string, handler ports.MessageHandler, config *ports.ConsumerConfig) (func() error, error) {
	return m.baseMessaging.SubscribeWithConfig(ctx, topic, handler, config)
}

// SubscribeForTenant делегирует вызов базовому MessagingPort
func (m *TransactionalMessaging) SubscribeForTenant(ctx context.Context, topic string, handler ports.MessageHandler, tenantID string) (func() error, error) {
	return m.baseMessaging.SubscribeForTenant(ctx, topic, handler, tenantID)
}

// SubscribeGroup делегирует вызов базовому MessagingPort
func (m *TransactionalMessaging) SubscribeGroup(ctx context.Context, topic, groupID string, handler ports.MessageHandler) (func() error, error) {
	return m.baseMessaging.SubscribeGroup(ctx, topic, groupID, handler)
}

// Commit делегирует вызов базовому MessagingPort
func (m *TransactionalMessaging) Commit(ctx context.Context, msg *ports.Message) error {
	return m.baseMessaging.Commit(ctx, msg)
}

// CommitBatch делегирует вызов базовому MessagingPort
func (m *TransactionalMessaging) CommitBatch(ctx context.Context, msgs []*ports.Message) error {
	return m.baseMessaging.CommitBatch(ctx, msgs)
}

// Nack делегирует вызов базовому MessagingPort
func (m *TransactionalMessaging) Nack(ctx context.Context, msg *ports.Message) error {
	return m.baseMessaging.Nack(ctx, msg)
}

// CreateTopic делегирует вызов базовому MessagingPort
func (m *TransactionalMessaging) CreateTopic(ctx context.Context, topic string, partitions int, replicationFactor int) error {
	return m.baseMessaging.CreateTopic(ctx, topic, partitions, replicationFactor)
}

// DeleteTopic делегирует вызов базовому MessagingPort
func (m *TransactionalMessaging) DeleteTopic(ctx context.Context, topic string) error {
	return m.baseMessaging.DeleteTopic(ctx, topic)
}

// ListTopics делегирует вызов базовому MessagingPort
func (m *TransactionalMessaging) ListTopics(ctx context.Context) ([]string, error) {
	return m.baseMessaging.ListTopics(ctx)
}

// Close делегирует вызов базовому MessagingPort
func (m *TransactionalMessaging) Close() error {
	return m.baseMessaging.Close()
}
