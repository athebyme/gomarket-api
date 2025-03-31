package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"gomarketplace_api/internal/core/ports"
)

// RedisCachePort реализует интерфейс CachePort с использованием Redis
type RedisCachePort struct {
	client *redis.Client
}

// NewRedisCachePort создает экземпляр RedisCachePort
func NewRedisCachePort(client *redis.Client) *RedisCachePort {
	return &RedisCachePort{
		client: client,
	}
}

// buildKey создает ключ с учетом арендатора
func buildKey(key, tenantID string) string {
	if tenantID != "" {
		return fmt.Sprintf("tenant:%s:%s", tenantID, key)
	}
	return key
}

// Get получает значение из кэша по ключу
func (c *RedisCachePort) Get(ctx context.Context, key string) ([]byte, error) {
	result, err := c.client.Get(ctx, key).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, nil // Ключ не найден
		}
		return nil, err
	}
	return result, nil
}

// GetWithTenant получает значение из кэша по ключу с учетом ID арендатора
func (c *RedisCachePort) GetWithTenant(ctx context.Context, key string, tenantID string) ([]byte, error) {
	return c.Get(ctx, buildKey(key, tenantID))
}

// Set сохраняет значение в кэше с указанным сроком действия
func (c *RedisCachePort) Set(ctx context.Context, key string, value []byte, expiration time.Duration) error {
	return c.client.Set(ctx, key, value, expiration).Err()
}

// SetWithTenant сохраняет значение в кэше с учетом ID арендатора
func (c *RedisCachePort) SetWithTenant(ctx context.Context, key string, value []byte, tenantID string, expiration time.Duration) error {
	return c.Set(ctx, buildKey(key, tenantID), value, expiration)
}

// Delete удаляет значение из кэша по ключу
func (c *RedisCachePort) Delete(ctx context.Context, key string) error {
	return c.client.Del(ctx, key).Err()
}

// DeleteWithTenant удаляет значение из кэша по ключу с учетом ID арендатора
func (c *RedisCachePort) DeleteWithTenant(ctx context.Context, key string, tenantID string) error {
	return c.Delete(ctx, buildKey(key, tenantID))
}

// DeleteByPattern удаляет все значения, соответствующие шаблону
func (c *RedisCachePort) DeleteByPattern(ctx context.Context, pattern string) error {
	keys, err := c.client.Keys(ctx, pattern).Result()
	if err != nil {
		return err
	}

	if len(keys) > 0 {
		return c.client.Del(ctx, keys...).Err()
	}

	return nil
}

// DeleteByPatternWithTenant удаляет все значения, соответствующие шаблону с учетом ID арендатора
func (c *RedisCachePort) DeleteByPatternWithTenant(ctx context.Context, pattern string, tenantID string) error {
	return c.DeleteByPattern(ctx, buildKey(pattern, tenantID))
}

// GetMulti получает несколько значений за один запрос
func (c *RedisCachePort) GetMulti(ctx context.Context, keys []string) (map[string][]byte, error) {
	pipe := c.client.Pipeline()

	// Подготавливаем команды для выполнения
	cmds := make(map[string]*redis.StringCmd, len(keys))
	for _, key := range keys {
		cmds[key] = pipe.Get(ctx, key)
	}

	// Выполняем команды
	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return nil, err
	}

	// Собираем результаты
	result := make(map[string][]byte)
	for key, cmd := range cmds {
		val, err := cmd.Bytes()
		if err == nil {
			result[key] = val
		}
	}

	return result, nil
}

// GetMultiWithTenant получает несколько значений за один запрос с учетом ID арендатора
func (c *RedisCachePort) GetMultiWithTenant(ctx context.Context, keys []string, tenantID string) (map[string][]byte, error) {
	// Преобразуем ключи с учетом арендатора
	tenantKeys := make([]string, len(keys))
	for i, key := range keys {
		tenantKeys[i] = buildKey(key, tenantID)
	}

	// Получаем значения
	tenantValues, err := c.GetMulti(ctx, tenantKeys)
	if err != nil {
		return nil, err
	}

	// Преобразуем обратно ключи без префикса арендатора
	result := make(map[string][]byte, len(tenantValues))
	prefix := fmt.Sprintf("tenant:%s:", tenantID)
	for key, value := range tenantValues {
		originalKey := key
		if len(key) > len(prefix) && key[:len(prefix)] == prefix {
			originalKey = key[len(prefix):]
		}
		result[originalKey] = value
	}

	return result, nil
}

// SetMulti сохраняет несколько значений за один запрос
func (c *RedisCachePort) SetMulti(ctx context.Context, items map[string][]byte, expiration time.Duration) error {
	pipe := c.client.Pipeline()

	// Подготавливаем команды для выполнения
	for key, value := range items {
		pipe.Set(ctx, key, value, expiration)
	}

	// Выполняем команды
	_, err := pipe.Exec(ctx)
	return err
}

// SetMultiWithTenant сохраняет несколько значений за один запрос с учетом ID арендатора
func (c *RedisCachePort) SetMultiWithTenant(ctx context.Context, items map[string][]byte, tenantID string, expiration time.Duration) error {
	// Преобразуем ключи с учетом арендатора
	tenantItems := make(map[string][]byte, len(items))
	for key, value := range items {
		tenantItems[buildKey(key, tenantID)] = value
	}

	return c.SetMulti(ctx, tenantItems, expiration)
}

// Increment увеличивает числовое значение ключа на указанную величину
func (c *RedisCachePort) Increment(ctx context.Context, key string, delta int64) (int64, error) {
	return c.client.IncrBy(ctx, key, delta).Result()
}

// IncrementWithTenant увеличивает числовое значение ключа с учетом ID арендатора
func (c *RedisCachePort) IncrementWithTenant(ctx context.Context, key string, tenantID string, delta int64) (int64, error) {
	return c.Increment(ctx, buildKey(key, tenantID), delta)
}

// Lock пытается получить блокировку с указанным ключом
func (c *RedisCachePort) Lock(ctx context.Context, key string, expiration time.Duration) (bool, error) {
	lockKey := fmt.Sprintf("lock:%s", key)
	return c.client.SetNX(ctx, lockKey, "1", expiration).Result()
}

// LockWithTenant пытается получить блокировку с учетом ID арендатора
func (c *RedisCachePort) LockWithTenant(ctx context.Context, key string, tenantID string, expiration time.Duration) (bool, error) {
	return c.Lock(ctx, buildKey(key, tenantID), expiration)
}

// Unlock освобождает блокировку
func (c *RedisCachePort) Unlock(ctx context.Context, key string) error {
	lockKey := fmt.Sprintf("lock:%s", key)
	return c.client.Del(ctx, lockKey).Err()
}

// UnlockWithTenant освобождает блокировку с учетом ID арендатора
func (c *RedisCachePort) UnlockWithTenant(ctx context.Context, key string, tenantID string) error {
	return c.Unlock(ctx, buildKey(key, tenantID))
}

// Close закрывает соединение с системой кэширования
func (c *RedisCachePort) Close() error {
	return c.client.Close()
}

// Убедимся, что RedisCachePort реализует интерфейс CachePort
var _ ports.CachePort = (*RedisCachePort)(nil)
