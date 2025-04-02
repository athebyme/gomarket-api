package kafka

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"gomarketplace_api/internal/core/ports"
)

// KafkaMessagingPort реализует интерфейс MessagingPort с использованием Kafka
type KafkaMessagingPort struct {
	brokers          []string
	producers        map[string]*kafka.Writer
	producersMu      sync.RWMutex
	consumerGroups   map[string]*kafka.Reader
	consumerGroupsMu sync.RWMutex

	// Карта для хранения функций отмены подписки
	unsubscribeFuncs map[string]func() error
	unsubscribeMu    sync.RWMutex
}

// NewKafkaMessagingPort создает экземпляр KafkaMessagingPort
func NewKafkaMessagingPort(brokers []string) *KafkaMessagingPort {
	return &KafkaMessagingPort{
		brokers:          brokers,
		producers:        make(map[string]*kafka.Writer),
		consumerGroups:   make(map[string]*kafka.Reader),
		unsubscribeFuncs: make(map[string]func() error),
	}
}

// getProducer получает или создает producer для указанной темы
func (k *KafkaMessagingPort) getProducer(topic string) *kafka.Writer {
	k.producersMu.RLock()
	producer, exists := k.producers[topic]
	k.producersMu.RUnlock()

	if !exists {
		k.producersMu.Lock()
		defer k.producersMu.Unlock()

		// Повторная проверка после получения блокировки на запись
		producer, exists = k.producers[topic]
		if !exists {
			producer = &kafka.Writer{
				Addr:         kafka.TCP(k.brokers...),
				Topic:        topic,
				Balancer:     &kafka.LeastBytes{},
				RequiredAcks: kafka.RequireAll,
				Async:        false,
			}
			k.producers[topic] = producer
		}
	}

	return producer
}

// Publish публикует сообщение в указанную тему
func (k *KafkaMessagingPort) Publish(ctx context.Context, topic string, message []byte) error {
	producer := k.getProducer(topic)

	return producer.WriteMessages(ctx, kafka.Message{
		Value: message,
		Time:  time.Now(),
	})
}

// PublishWithKey публикует сообщение с указанным ключом
func (k *KafkaMessagingPort) PublishWithKey(ctx context.Context, topic string, key string, message []byte) error {
	producer := k.getProducer(topic)

	return producer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: message,
		Time:  time.Now(),
	})
}

// PublishWithHeaders публикует сообщение с дополнительными заголовками
func (k *KafkaMessagingPort) PublishWithHeaders(ctx context.Context, topic string, message []byte, headers map[string]string) error {
	producer := k.getProducer(topic)

	// Конвертируем заголовки в формат Kafka
	kafkaHeaders := []kafka.Header{}
	for key, value := range headers {
		kafkaHeaders = append(kafkaHeaders, kafka.Header{
			Key:   key,
			Value: []byte(value),
		})
	}

	return producer.WriteMessages(ctx, kafka.Message{
		Value:   message,
		Time:    time.Now(),
		Headers: kafkaHeaders,
	})
}

// PublishForTenant публикует сообщение с учетом ID арендатора
func (k *KafkaMessagingPort) PublishForTenant(ctx context.Context, topic string, message []byte, tenantID string) error {
	// Добавляем ID арендатора в заголовок
	headers := map[string]string{
		"tenant_id": tenantID,
	}

	return k.PublishWithHeaders(ctx, topic, message, headers)
}

// PublishBatch публикует несколько сообщений одним запросом
func (k *KafkaMessagingPort) PublishBatch(ctx context.Context, topic string, messages [][]byte) error {
	producer := k.getProducer(topic)

	kafkaMessages := make([]kafka.Message, len(messages))
	for i, msg := range messages {
		kafkaMessages[i] = kafka.Message{
			Value: msg,
			Time:  time.Now(),
		}
	}

	return producer.WriteMessages(ctx, kafkaMessages...)
}

// PublishBatchWithKeys публикует несколько сообщений с ключами одним запросом
func (k *KafkaMessagingPort) PublishBatchWithKeys(ctx context.Context, topic string, keyedMessages map[string][]byte) error {
	producer := k.getProducer(topic)

	kafkaMessages := make([]kafka.Message, 0, len(keyedMessages))
	for key, msg := range keyedMessages {
		kafkaMessages = append(kafkaMessages, kafka.Message{
			Key:   []byte(key),
			Value: msg,
			Time:  time.Now(),
		})
	}

	return producer.WriteMessages(ctx, kafkaMessages...)
}

// Создаем уникальный идентификатор для подписки
func subscriptionID(topic, groupID string) string {
	return fmt.Sprintf("%s-%s", topic, groupID)
}

// Функция для обработки ошибок в консьюмере
func (k *KafkaMessagingPort) handleConsumerErrors(reader *kafka.Reader, handler ports.MessageHandler, subscriptionID string) {
	for {
		message, err := reader.ReadMessage(context.Background())
		if err != nil {
			// Проверяем, была ли отменена подписка
			k.unsubscribeMu.RLock()
			_, exists := k.unsubscribeFuncs[subscriptionID]
			k.unsubscribeMu.RUnlock()

			if !exists {
				// Подписка была отменена, выходим из цикла
				return
			}

			// Логируем ошибку и продолжаем
			fmt.Printf("Error reading message: %v\n", err)
			continue
		}

		// Создаем объект Message
		msg := &ports.Message{
			ID:          fmt.Sprintf("%d-%d-%d", message.Partition, message.Offset, message.Time.UnixNano()),
			Topic:       message.Topic,
			Key:         string(message.Key),
			Value:       message.Value,
			Headers:     make(map[string]string),
			Metadata:    make(map[string]interface{}),
			PublishedAt: message.Time,
		}

		// Заполняем заголовки
		for _, header := range message.Headers {
			msg.Headers[header.Key] = string(header.Value)
		}

		// Получаем ID арендатора из заголовков
		tenantID, ok := msg.Headers["tenant_id"]
		if ok {
			msg.TenantID = tenantID
		}

		// Вызываем обработчик
		err = handler(context.Background(), msg)
		if err != nil {
			// Логируем ошибку и продолжаем
			fmt.Printf("Error handling message: %v\n", err)
		}
	}
}

// Subscribe подписывается на указанную тему
func (k *KafkaMessagingPort) Subscribe(ctx context.Context, topic string, handler ports.MessageHandler) (func() error, error) {
	// Используем пустую группу потребителей, каждый подписчик получит все сообщения
	groupID := fmt.Sprintf("default-group-%d", time.Now().UnixNano())
	return k.SubscribeGroup(ctx, topic, groupID, handler)
}

// SubscribeWithConfig подписывается на указанную тему с дополнительными настройками
func (k *KafkaMessagingPort) SubscribeWithConfig(ctx context.Context, topic string, handler ports.MessageHandler, config *ports.ConsumerConfig) (func() error, error) {
	groupID := config.GroupID
	if groupID == "" {
		groupID = fmt.Sprintf("default-group-%d", time.Now().UnixNano())
	}

	k.consumerGroupsMu.Lock()
	defer k.consumerGroupsMu.Unlock()

	subID := subscriptionID(topic, groupID)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        k.brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		CommitInterval: time.Second,
		MaxWait:        time.Second,
	})

	k.consumerGroups[subID] = reader

	// Запускаем обработку сообщений в отдельной горутине
	go k.handleConsumerErrors(reader, handler, subID)

	// Создаем функцию для отмены подписки
	unsubscribeFunc := func() error {
		k.unsubscribeMu.Lock()
		defer k.unsubscribeMu.Unlock()

		delete(k.unsubscribeFuncs, subID)

		k.consumerGroupsMu.Lock()
		defer k.consumerGroupsMu.Unlock()

		r, exists := k.consumerGroups[subID]
		if !exists {
			return nil
		}

		err := r.Close()
		delete(k.consumerGroups, subID)
		return err
	}

	// Сохраняем функцию отмены подписки
	k.unsubscribeMu.Lock()
	k.unsubscribeFuncs[subID] = unsubscribeFunc
	k.unsubscribeMu.Unlock()

	return unsubscribeFunc, nil
}

// SubscribeForTenant подписывается на сообщения для конкретного арендатора
func (k *KafkaMessagingPort) SubscribeForTenant(ctx context.Context, topic string, handler ports.MessageHandler, tenantID string) (func() error, error) {
	// Создаем обертку для обработчика, которая фильтрует сообщения по ID арендатора
	tenantHandler := func(ctx context.Context, msg *ports.Message) error {
		if msg.TenantID == tenantID {
			return handler(ctx, msg)
		}
		return nil
	}

	return k.Subscribe(ctx, topic, tenantHandler)
}

// SubscribeGroup подписывается на сообщения в составе группы потребителей
func (k *KafkaMessagingPort) SubscribeGroup(ctx context.Context, topic, groupID string, handler ports.MessageHandler) (func() error, error) {
	config := &ports.ConsumerConfig{
		GroupID: groupID,
	}

	return k.SubscribeWithConfig(ctx, topic, handler, config)
}

// Commit подтверждает обработку сообщения (для Kafka это не требуется при автокоммите)
func (k *KafkaMessagingPort) Commit(ctx context.Context, msg *ports.Message) error {
	// Kafka Reader автоматически выполняет коммит по умолчанию
	return nil
}

// CommitBatch подтверждает обработку нескольких сообщений (для Kafka это не требуется при автокоммите)
func (k *KafkaMessagingPort) CommitBatch(ctx context.Context, msgs []*ports.Message) error {
	// Kafka Reader автоматически выполняет коммит по умолчанию
	return nil
}

// Nack отклоняет сообщение (может привести к повторной обработке)
func (k *KafkaMessagingPort) Nack(ctx context.Context, msg *ports.Message) error {
	// В Kafka нет прямого эквивалента для Nack, можно реализовать через deadletter queue
	// В данной реализации просто логируем
	fmt.Printf("Message nacked: %s\n", msg.ID)
	return nil
}

// CreateTopic создает новую тему
func (k *KafkaMessagingPort) CreateTopic(ctx context.Context, topic string, partitions int, replicationFactor int) error {
	conn, err := kafka.DialContext(ctx, "tcp", k.brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()

	return conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: replicationFactor,
	})
}

// DeleteTopic удаляет тему
func (k *KafkaMessagingPort) DeleteTopic(ctx context.Context, topic string) error {
	conn, err := kafka.DialContext(ctx, "tcp", k.brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()

	return conn.DeleteTopics(topic)
}

// ListTopics возвращает список всех тем
func (k *KafkaMessagingPort) ListTopics(ctx context.Context) ([]string, error) {
	conn, err := kafka.DialContext(ctx, "tcp", k.brokers[0])
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return nil, err
	}

	// Создаем map для дедупликации тем
	topics := make(map[string]struct{})
	for _, p := range partitions {
		topics[p.Topic] = struct{}{}
	}

	// Преобразуем map в slice
	result := make([]string, 0, len(topics))
	for topic := range topics {
		result = append(result, topic)
	}

	return result, nil
}

// Close закрывает все соединения
func (k *KafkaMessagingPort) Close() error {
	var errs []string

	// Закрываем все producers
	k.producersMu.Lock()
	for topic, producer := range k.producers {
		if err := producer.Close(); err != nil {
			errs = append(errs, fmt.Sprintf("failed to close producer for topic %s: %v", topic, err))
		}
	}
	k.producers = make(map[string]*kafka.Writer)
	k.producersMu.Unlock()

	// Закрываем все consumer groups
	k.consumerGroupsMu.Lock()
	for id, reader := range k.consumerGroups {
		if err := reader.Close(); err != nil {
			errs = append(errs, fmt.Sprintf("failed to close consumer group %s: %v", id, err))
		}
	}
	k.consumerGroups = make(map[string]*kafka.Reader)
	k.consumerGroupsMu.Unlock()

	// Очищаем функции отмены подписки
	k.unsubscribeMu.Lock()
	k.unsubscribeFuncs = make(map[string]func() error)
	k.unsubscribeMu.Unlock()

	if len(errs) > 0 {
		return fmt.Errorf("errors while closing Kafka connections: %s", strings.Join(errs, "; "))
	}

	return nil
}

// Удостоверение, что KafkaMessagingPort реализует интерфейс MessagingPort
var _ ports.MessagingPort = (*KafkaMessagingPort)(nil)
