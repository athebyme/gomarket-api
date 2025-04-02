package transactions

import (
	"context"
	"errors"
	"fmt"
	"gomarketplace_api/internal/infrastructure/transactions/interfaces"
	"strconv"
	"sync"
	"time"

	"gomarketplace_api/internal/core/ports"
)

// CacheEntry представляет запись в кэше с дополнительными метаданными
type CacheEntry struct {
	Value      []byte    `json:"value"`
	Expiration time.Time `json:"expiration,omitempty"`
	Deleted    bool      `json:"deleted"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// ImprovedTransactionalCacheAdapter расширяет базовый TransactionalCacheAdapter
type ImprovedTransactionalCacheAdapter struct {
	baseCache         ports.CachePort
	mutex             sync.RWMutex
	txCache           map[string]map[string]*CacheEntry // map[tx_id]map[cache_key]*CacheEntry
	txCacheCreateTime map[string]time.Time              // map[tx_id]create_time
	txCacheTenantIDs  map[string]string                 // map[tx_id]tenant_id
	cleanupInterval   time.Duration
	logger            ports.LoggerPort
}

// NewImprovedTransactionalCacheAdapter создает новый экземпляр ImprovedTransactionalCacheAdapter
func NewImprovedTransactionalCacheAdapter(
	baseCache ports.CachePort,
	logger ports.LoggerPort,
	cleanupInterval time.Duration,
) *ImprovedTransactionalCacheAdapter {
	adapter := &ImprovedTransactionalCacheAdapter{
		baseCache:         baseCache,
		txCache:           make(map[string]map[string]*CacheEntry),
		txCacheCreateTime: make(map[string]time.Time),
		txCacheTenantIDs:  make(map[string]string),
		cleanupInterval:   cleanupInterval,
		logger:            logger,
	}

	// Запускаем периодическую очистку неиспользуемых кэшей
	go adapter.periodicCleanup()

	return adapter
}

// periodicCleanup периодически очищает неиспользуемые кэши
func (a *ImprovedTransactionalCacheAdapter) periodicCleanup() {
	ticker := time.NewTicker(a.cleanupInterval)
	defer ticker.Stop()

	for range ticker.C {
		a.CleanupStaleTxCaches(a.cleanupInterval * 3)
	}
}

// WithTransaction возвращает кэш для указанной транзакции
func (a *ImprovedTransactionalCacheAdapter) WithTransaction(tx interfaces.Transaction) ports.CachePort {
	// Создаем кэш для транзакции, если его еще нет
	a.mutex.Lock()
	defer a.mutex.Unlock()

	txID := tx.GetID()
	if _, exists := a.txCache[txID]; !exists {
		a.txCache[txID] = make(map[string]*CacheEntry)
		a.txCacheCreateTime[txID] = time.Now()
		a.txCacheTenantIDs[txID] = tx.GetTenantID()
	}

	return &ImprovedTransactionalCache{
		baseCache: a.baseCache,
		adapter:   a,
		txID:      txID,
		tenantID:  tx.GetTenantID(),
	}
}

// FlushTransactionCache сбрасывает кэш транзакции в основной кэш
func (a *ImprovedTransactionalCacheAdapter) FlushTransactionCache(tx interfaces.Transaction) error {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	txID := tx.GetID()
	txCache, exists := a.txCache[txID]
	if !exists {
		return nil
	}

	// Копируем данные из транзакционного кэша в основной
	ctx := context.Background()
	now := time.Now()
	tenantID := a.txCacheTenantIDs[txID]

	for key, entry := range txCache {
		// Пропускаем удаленные записи
		if entry.Deleted {
			if err := a.baseCache.DeleteWithTenant(ctx, key, tenantID); err != nil {
				a.logger.Warn("Не удалось удалить ключ из основного кэша",
					"key", key,
					"txID", txID,
					"error", err)
			}
			continue
		}

		// Проверяем срок действия
		var expiration time.Duration
		if !entry.Expiration.IsZero() {
			// Если время истечения в будущем, вычисляем оставшееся время
			if entry.Expiration.After(now) {
				expiration = entry.Expiration.Sub(now)
			} else {
				// Если время истечения в прошлом, пропускаем запись
				continue
			}
		}

		// Сохраняем в основной кэш
		if err := a.baseCache.SetWithTenant(ctx, key, entry.Value, tenantID, expiration); err != nil {
			a.logger.Warn("Не удалось сохранить ключ в основной кэш",
				"key", key,
				"txID", txID,
				"error", err)
		}
	}

	// Удаляем транзакционный кэш
	delete(a.txCache, txID)
	delete(a.txCacheCreateTime, txID)
	delete(a.txCacheTenantIDs, txID)

	return nil
}

// RollbackTransactionCache удаляет кэш транзакции без сброса в основной кэш
func (a *ImprovedTransactionalCacheAdapter) RollbackTransactionCache(tx interfaces.Transaction) error {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	txID := tx.GetID()
	delete(a.txCache, txID)
	delete(a.txCacheCreateTime, txID)
	delete(a.txCacheTenantIDs, txID)

	return nil
}

// CleanupStaleTxCaches очищает неиспользуемые кэши старше указанного возраста
func (a *ImprovedTransactionalCacheAdapter) CleanupStaleTxCaches(maxAge time.Duration) int {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	count := 0
	now := time.Now()

	for txID, createTime := range a.txCacheCreateTime {
		if now.Sub(createTime) > maxAge {
			delete(a.txCache, txID)
			delete(a.txCacheCreateTime, txID)
			delete(a.txCacheTenantIDs, txID)
			count++

			a.logger.Warn("Очищен устаревший транзакционный кэш",
				"txID", txID,
				"age", now.Sub(createTime).String())
		}
	}

	return count
}

// GetStats возвращает статистику по транзакционным кэшам
func (a *ImprovedTransactionalCacheAdapter) GetStats() map[string]interface{} {
	a.mutex.RLock()
	defer a.mutex.RUnlock()

	stats := make(map[string]interface{})
	stats["tx_count"] = len(a.txCache)

	txStats := make(map[string]interface{})
	for txID, cache := range a.txCache {
		txStat := make(map[string]interface{})
		txStat["entry_count"] = len(cache)
		txStat["created_at"] = a.txCacheCreateTime[txID]
		txStat["tenant_id"] = a.txCacheTenantIDs[txID]
		txStat["age"] = time.Since(a.txCacheCreateTime[txID]).String()

		txStats[txID] = txStat
	}
	stats["transactions"] = txStats

	return stats
}

// ImprovedTransactionalCache реализует транзакционный кэш с расширенными возможностями
type ImprovedTransactionalCache struct {
	baseCache ports.CachePort
	adapter   *ImprovedTransactionalCacheAdapter
	txID      string
	tenantID  string
}

// Get получает значение из кэша по ключу
func (c *ImprovedTransactionalCache) Get(ctx context.Context, key string) ([]byte, error) {
	return c.GetWithTenant(ctx, key, c.tenantID)
}

// GetWithTenant получает значение из кэша по ключу с учетом ID арендатора
func (c *ImprovedTransactionalCache) GetWithTenant(ctx context.Context, key string, tenantID string) ([]byte, error) {
	// Сначала проверяем транзакционный кэш
	c.adapter.mutex.RLock()
	txCache, exists := c.adapter.txCache[c.txID]
	if exists {
		entry, exists := txCache[key]
		c.adapter.mutex.RUnlock()
		if exists {
			// Проверяем, помечена ли запись как удаленная
			if entry.Deleted {
				return nil, ports.ErrCacheMiss
			}

			// Проверяем срок действия
			if !entry.Expiration.IsZero() && entry.Expiration.Before(time.Now()) {
				return nil, ports.ErrCacheMiss
			}

			return entry.Value, nil
		}
	} else {
		c.adapter.mutex.RUnlock()
	}

	// Если в транзакционном кэше нет, идем в основной
	return c.baseCache.GetWithTenant(ctx, key, tenantID)
}

// Set сохраняет значение в кэше с указанным сроком действия
func (c *ImprovedTransactionalCache) Set(ctx context.Context, key string, value []byte, expiration time.Duration) error {
	return c.SetWithTenant(ctx, key, value, c.tenantID, expiration)
}

// SetWithTenant сохраняет значение в кэше с учетом ID арендатора
func (c *ImprovedTransactionalCache) SetWithTenant(ctx context.Context, key string, value []byte, tenantID string, expiration time.Duration) error {
	// Сохраняем в транзакционный кэш
	c.adapter.mutex.Lock()
	defer c.adapter.mutex.Unlock()

	txCache, exists := c.adapter.txCache[c.txID]
	if !exists {
		txCache = make(map[string]*CacheEntry)
		c.adapter.txCache[c.txID] = txCache
		c.adapter.txCacheCreateTime[c.txID] = time.Now()
		c.adapter.txCacheTenantIDs[c.txID] = tenantID
	}

	// Создаем запись в кэше
	now := time.Now()
	var expirationTime time.Time
	if expiration > 0 {
		expirationTime = now.Add(expiration)
	}

	txCache[key] = &CacheEntry{
		Value:      value,
		Expiration: expirationTime,
		Deleted:    false,
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	return nil
}

// Delete удаляет значение из кэша по ключу
func (c *ImprovedTransactionalCache) Delete(ctx context.Context, key string) error {
	return c.DeleteWithTenant(ctx, key, c.tenantID)
}

// DeleteWithTenant удаляет значение из кэша по ключу с учетом ID арендатора
func (c *ImprovedTransactionalCache) DeleteWithTenant(ctx context.Context, key string, tenantID string) error {
	// Помечаем как удаленное в транзакционном кэше
	c.adapter.mutex.Lock()
	defer c.adapter.mutex.Unlock()

	txCache, exists := c.adapter.txCache[c.txID]
	if !exists {
		txCache = make(map[string]*CacheEntry)
		c.adapter.txCache[c.txID] = txCache
		c.adapter.txCacheCreateTime[c.txID] = time.Now()
		c.adapter.txCacheTenantIDs[c.txID] = tenantID
	}

	// Создаем запись помеченную как удаленную
	now := time.Now()
	txCache[key] = &CacheEntry{
		Deleted:   true,
		CreatedAt: now,
		UpdatedAt: now,
	}

	return nil
}

// DeleteByPattern удаляет все значения, соответствующие шаблону
func (c *ImprovedTransactionalCache) DeleteByPattern(ctx context.Context, pattern string) error {
	return c.DeleteByPatternWithTenant(ctx, pattern, c.tenantID)
}

// DeleteByPatternWithTenant удаляет все значения, соответствующие шаблону с учетом ID арендатора
func (c *ImprovedTransactionalCache) DeleteByPatternWithTenant(ctx context.Context, pattern string, tenantID string) error {
	// Это сложная операция, которую мы не можем полностью реализовать в транзакционном кэше
	// Но мы можем попытаться удалить все ключи, которые соответствуют шаблону
	// из транзакционного кэша

	c.adapter.mutex.Lock()
	txCache, exists := c.adapter.txCache[c.txID]
	if exists {
		// Ищем все ключи, которые соответствуют шаблону
		now := time.Now()
		for key := range txCache {
			// Здесь должна быть проверка на соответствие шаблону
			// Для простоты используем префикс
			if matchesPattern(key, pattern) {
				txCache[key] = &CacheEntry{
					Deleted:   true,
					CreatedAt: now,
					UpdatedAt: now,
				}
			}
		}
	}
	c.adapter.mutex.Unlock()

	// Делегируем удаление по шаблону базовому кэшу при коммите транзакции
	// В реальной реализации нужно сохранить информацию о шаблоне
	// и выполнить удаление при коммите

	return nil
}

// GetMulti получает несколько значений за один запрос
func (c *ImprovedTransactionalCache) GetMulti(ctx context.Context, keys []string) (map[string][]byte, error) {
	return c.GetMultiWithTenant(ctx, keys, c.tenantID)
}

// GetMultiWithTenant получает несколько значений за один запрос с учетом ID арендатора
func (c *ImprovedTransactionalCache) GetMultiWithTenant(ctx context.Context, keys []string, tenantID string) (map[string][]byte, error) {
	result := make(map[string][]byte)

	// Сначала проверяем транзакционный кэш
	c.adapter.mutex.RLock()
	txCache, txExists := c.adapter.txCache[c.txID]
	if txExists {
		// Собираем ключи, которые есть в транзакционном кэше
		missingKeys := make([]string, 0, len(keys))
		now := time.Now()

		for _, key := range keys {
			if entry, exists := txCache[key]; exists {
				// Проверяем, помечена ли запись как удаленная
				if entry.Deleted {
					continue
				}

				// Проверяем срок действия
				if !entry.Expiration.IsZero() && entry.Expiration.Before(now) {
					continue
				}

				result[key] = entry.Value
			} else {
				missingKeys = append(missingKeys, key)
			}
		}
		c.adapter.mutex.RUnlock()

		// Если все ключи были найдены в транзакционном кэше, возвращаем результат
		if len(missingKeys) == 0 {
			return result, nil
		}

		// Иначе, получаем недостающие ключи из базового кэша
		baseResult, err := c.baseCache.GetMultiWithTenant(ctx, missingKeys, tenantID)
		if err != nil {
			return result, err
		}

		// Объединяем результаты
		for k, v := range baseResult {
			result[k] = v
		}
	} else {
		c.adapter.mutex.RUnlock()
		// Если транзакционного кэша нет, получаем все из базового кэша
		return c.baseCache.GetMultiWithTenant(ctx, keys, tenantID)
	}

	return result, nil
}

// SetMulti сохраняет несколько значений за один запрос
func (c *ImprovedTransactionalCache) SetMulti(ctx context.Context, items map[string][]byte, expiration time.Duration) error {
	return c.SetMultiWithTenant(ctx, items, c.tenantID, expiration)
}

// SetMultiWithTenant сохраняет несколько значений за один запрос с учетом ID арендатора
func (c *ImprovedTransactionalCache) SetMultiWithTenant(ctx context.Context, items map[string][]byte, tenantID string, expiration time.Duration) error {
	// Сохраняем все элементы в транзакционный кэш
	c.adapter.mutex.Lock()
	defer c.adapter.mutex.Unlock()

	txCache, exists := c.adapter.txCache[c.txID]
	if !exists {
		txCache = make(map[string]*CacheEntry)
		c.adapter.txCache[c.txID] = txCache
		c.adapter.txCacheCreateTime[c.txID] = time.Now()
		c.adapter.txCacheTenantIDs[c.txID] = tenantID
	}

	// Создаем записи в кэше
	now := time.Now()
	var expirationTime time.Time
	if expiration > 0 {
		expirationTime = now.Add(expiration)
	}

	for key, value := range items {
		txCache[key] = &CacheEntry{
			Value:      value,
			Expiration: expirationTime,
			Deleted:    false,
			CreatedAt:  now,
			UpdatedAt:  now,
		}
	}

	return nil
}

// Increment увеличивает числовое значение ключа на указанную величину
func (c *ImprovedTransactionalCache) Increment(ctx context.Context, key string, delta int64) (int64, error) {
	return c.IncrementWithTenant(ctx, key, c.tenantID, delta)
}

// IncrementWithTenant увеличивает числовое значение ключа с учетом ID арендатора
func (c *ImprovedTransactionalCache) IncrementWithTenant(ctx context.Context, key string, tenantID string, delta int64) (int64, error) {
	// Получаем текущее значение
	value, err := c.GetWithTenant(ctx, key, tenantID)
	if err != nil && !errors.Is(err, ports.ErrCacheMiss) {
		return 0, err
	}

	var current int64
	if err == nil {
		// Преобразуем текущее значение в число
		current, err = strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("значение не является числом: %w", err)
		}
	}

	// Увеличиваем
	newValue := current + delta

	// Сохраняем в транзакционном кэше
	c.adapter.mutex.Lock()
	txCache, exists := c.adapter.txCache[c.txID]
	if !exists {
		txCache = make(map[string]*CacheEntry)
		c.adapter.txCache[c.txID] = txCache
		c.adapter.txCacheCreateTime[c.txID] = time.Now()
		c.adapter.txCacheTenantIDs[c.txID] = tenantID
	}

	// Создаем запись в кэше
	now := time.Now()
	txCache[key] = &CacheEntry{
		Value:     []byte(strconv.FormatInt(newValue, 10)),
		Deleted:   false,
		CreatedAt: now,
		UpdatedAt: now,
	}
	c.adapter.mutex.Unlock()

	return newValue, nil
}

// Lock пытается получить блокировку с указанным ключом
func (c *ImprovedTransactionalCache) Lock(ctx context.Context, key string, expiration time.Duration) (bool, error) {
	return c.LockWithTenant(ctx, key, c.tenantID, expiration)
}

// LockWithTenant пытается получить блокировку с учетом ID арендатора
func (c *ImprovedTransactionalCache) LockWithTenant(ctx context.Context, key string, tenantID string, expiration time.Duration) (bool, error) {
	// Проверяем, есть ли блокировка в транзакционном кэше
	lockKey := "lock:" + key

	c.adapter.mutex.RLock()
	txCache, txExists := c.adapter.txCache[c.txID]
	if txExists {
		if entry, exists := txCache[lockKey]; exists && !entry.Deleted {
			c.adapter.mutex.RUnlock()
			// Уже заблокировано в рамках этой транзакции
			return true, nil
		}
	}
	c.adapter.mutex.RUnlock()

	// Пытаемся получить блокировку в основном кэше
	// Используем паттерн SET NX (установить, если не существует)
	success, err := c.baseCache.LockWithTenant(ctx, key, tenantID, expiration)
	if err != nil || !success {
		return success, err
	}

	// Если получили блокировку, сохраняем информацию в транзакционном кэше
	c.adapter.mutex.Lock()
	defer c.adapter.mutex.Unlock()

	if !txExists {
		txCache = make(map[string]*CacheEntry)
		c.adapter.txCache[c.txID] = txCache
		c.adapter.txCacheCreateTime[c.txID] = time.Now()
		c.adapter.txCacheTenantIDs[c.txID] = tenantID
	}

	// Создаем запись для блокировки
	now := time.Now()
	var expirationTime time.Time
	if expiration > 0 {
		expirationTime = now.Add(expiration)
	}

	txCache[lockKey] = &CacheEntry{
		Value:      []byte("1"),
		Expiration: expirationTime,
		Deleted:    false,
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	return true, nil
}

// Unlock освобождает блокировку
func (c *ImprovedTransactionalCache) Unlock(ctx context.Context, key string) error {
	return c.UnlockWithTenant(ctx, key, c.tenantID)
}

// UnlockWithTenant освобождает блокировку с учетом ID арендатора
func (c *ImprovedTransactionalCache) UnlockWithTenant(ctx context.Context, key string, tenantID string) error {
	// Помечаем как удаленное в транзакционном кэше
	lockKey := "lock:" + key

	c.adapter.mutex.Lock()
	txCache, exists := c.adapter.txCache[c.txID]
	if !exists {
		txCache = make(map[string]*CacheEntry)
		c.adapter.txCache[c.txID] = txCache
		c.adapter.txCacheCreateTime[c.txID] = time.Now()
		c.adapter.txCacheTenantIDs[c.txID] = tenantID
	}

	// Создаем запись помеченную как удаленную
	now := time.Now()
	txCache[lockKey] = &CacheEntry{
		Deleted:   true,
		CreatedAt: now,
		UpdatedAt: now,
	}
	c.adapter.mutex.Unlock()

	// Освобождаем блокировку в основном кэше
	return c.baseCache.UnlockWithTenant(ctx, key, tenantID)
}

// Close закрывает соединение с системой кэширования
func (c *ImprovedTransactionalCache) Close() error {
	// Ничего не делаем, так как закрытие будет выполнено на уровне базового кэша
	return nil
}

// matchesPattern проверяет, соответствует ли ключ шаблону
// Для простоты реализации используется проверка на префикс
func matchesPattern(key, pattern string) bool {
	// Если шаблон заканчивается на *, это префикс
	if pattern[len(pattern)-1] == '*' {
		prefix := pattern[:len(pattern)-1]
		return len(key) >= len(prefix) && key[:len(prefix)] == prefix
	}

	// Иначе точное совпадение
	return key == pattern
}
