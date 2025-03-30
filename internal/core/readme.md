# Core - Ядро системы интеграции маркетплейсов

## Обзор

Core — это универсальное ядро системы интеграции различных поставщиков и маркетплейсов. Ядро построено на принципах гексагональной архитектуры (ports & adapters) и предоставляет набор интерфейсов и моделей для обеспечения работы всей системы.

### Ключевые особенности

- 🔄 **Универсальность** — работа с любыми поставщиками и маркетплейсами через единый интерфейс
- 🔌 **Расширяемость** — простое добавление новых поставщиков и маркетплейсов
- 🏢 **Многоарендность** — поддержка нескольких арендаторов с изоляцией данных
- 🔒 **Надежные транзакции** — атомарные операции между разными системами
- 📦 **Контейнеризация** — готовность к развертыванию в Kubernetes
- 📈 **Масштабируемость** — кластерная архитектура для высоких нагрузок
- 📊 **Мониторинг** — встроенная телеметрия и отслеживание метрик

## Текущее состояние

На данный момент ядро системы:

1. **Находится в разработке** — основные интерфейсы и модели данных определены
2. **Интегрированы** следующие поставщики:
    - Wholesaler — основной поставщик
    - an_msc — дополнительный поставщик
3. **Интегрированы** следующие маркетплейсы:
    - Wildberries — полная интеграция

## Структура проекта

```
/internal
  /core                # Ядро системы
    /models            # Модели данных
      - product.go     # Модель продукта
      - supplier.go    # Модель поставщика
      - marketplace.go # Модель маркетплейса
    /ports             # Интерфейсы портов
      - supplier_port.go     # Интерфейс для поставщиков
      - marketplace_port.go  # Интерфейс для маркетплейсов
      - storage_port.go      # Интерфейс для хранилища данных
      - cache_port.go        # Интерфейс для кэша
      - messaging_port.go    # Интерфейс для обмена сообщениями
      - logger_port.go       # Интерфейс для логирования
      - transaction_port.go  # Интерфейс для транзакций
    /services          # Бизнес-логика
      - product_service.go   # Сервис для работы с продуктами
      - sync_service.go      # Сервис синхронизации
      - price_service.go     # Сервис для работы с ценами
```

## Ключевые компоненты

### Модели данных

Ядро определяет следующие основные модели данных:

- **Product** — универсальная модель продукта, агрегирующая информацию от разных поставщиков
- **ProductInventory** — информация о запасах продукта
- **ProductPrice** — информация о ценах продукта
- **ProductMedia** — медиа-файлы продукта (изображения, видео)
- **Supplier** — информация о поставщике
- **MarketplaceProduct** — связь между продуктами и маркетплейсами

### Порты (интерфейсы)

Ядро определяет следующие ключевые порты:

- **SupplierPort** — интерфейс для интеграции поставщиков
- **MarketplacePort** — интерфейс для интеграции маркетплейсов
- **StoragePort** — интерфейс для постоянного хранения данных
- **CachePort** — интерфейс для кэширования
- **MessagingPort** — интерфейс для обмена сообщениями
- **LoggerPort** — интерфейс для логирования
- **TransactionPort** — интерфейс для управления транзакциями

### Сервисы

Ядро содержит следующие сервисы:

- **ProductService** — управление продуктами
- **SyncService** — синхронизация данных между поставщиками и маркетплейсами
- **PriceService** — управление ценами

## Планы развития

### Краткосрочные цели (1-3 месяца)

1. **Доработка транзакционного менеджера**
    - Реализация распределенных транзакций
    - Интеграция с Redis для управления состоянием транзакций

2. **Настройка очередей Kafka**
    - Реализация паттерна Outbox
    - Создание топиков для разных типов событий
    - Настройка потребителей для асинхронной обработки

3. **Контейнеризация с Kubernetes**
    - Создание Dockerfile для всех компонентов
    - Разработка манифестов Kubernetes
    - Настройка Helm-чартов для развертывания

4. **Улучшение многоарендности**
    - Переработка моделей данных для поддержки tenantID
    - Обновление интерфейсов портов
    - Реализация изоляции данных на уровне БД

### Среднесрочные цели (3-6 месяцев)

1. **Интеграция новых маркетплейсов**
    - Ozon
    - Яндекс.Маркет
    - AliExpress

2. **Расширение функциональности**
    - Автоматизированное управление ценами
    - Система мониторинга остатков
    - Анализ конкурентов

3. **Улучшение инфраструктуры**
    - Внедрение распределенного трейсинга (Jaeger)
    - Настройка мониторинга (Prometheus + Grafana)
    - Внедрение ELK-стека для агрегации логов

### Долгосрочные цели (6+ месяцев)

1. **Глубокая аналитика**
    - Интеграция с BigQuery/ClickHouse
    - Построение витрин данных
    - ML-модели для прогнозирования спроса

2. **API Gateway**
    - Разработка универсального API
    - Управление доступом и авторизацией
    - Тарификация и квоты

3. **Расширение географии**
    - Интеграция с международными маркетплейсами
    - Поддержка мультивалютности
    - Адаптация к локальным особенностям рынков

## Использование

### Синхронизация продуктов

```go
// Пример синхронизации продуктов с поставщиком
func SyncProductsExample(ctx context.Context) {
    // Получаем сервис продуктов
    productService := container.GetProductService()
    
    // Синхронизируем продукты от поставщика с ID 1
    count, err := productService.SyncProductsFromSupplier(ctx, 1, "tenant-1")
    if err != nil {
        log.Fatalf("Ошибка синхронизации: %v", err)
    }
    
    log.Printf("Синхронизировано %d продуктов", count)
}
```

### Работа с транзакциями

```go
// Пример использования транзакций
func UpdateProductWithTransaction(ctx context.Context, productID string) {
    // Получаем менеджер транзакций
    txPort := container.GetTransactionPort()
    
    // Выполняем операцию в транзакции
    result, err := txPort.ExecuteInTransactionWithTenant(ctx, func(ctx context.Context, tx ports.TransactionalPorts) (interface{}, error) {
        // Получаем продукт
        product, err := tx.Storage.GetProduct(ctx, productID, "tenant-1")
        if err != nil {
            return nil, err
        }
        
        // Обновляем продукт
        product.UpdatedAt = time.Now()
        
        // Сохраняем изменения
        if err := tx.Storage.SaveProduct(ctx, product, "tenant-1"); err != nil {
            return nil, err
        }
        
        // Обновляем кэш
        productJSON, _ := json.Marshal(product)
        if err := tx.Cache.SetWithTenant(ctx, "product:"+productID, productJSON, "tenant-1", time.Hour); err != nil {
            // Логируем ошибку, но не прерываем транзакцию
            log.Printf("Ошибка обновления кэша: %v", err)
        }
        
        return product, nil
    }, "tenant-1")
    
    if err != nil {
        log.Fatalf("Ошибка обновления продукта: %v", err)
    }
    
    log.Printf("Продукт успешно обновлен: %v", result)
}
```

## Добавление нового поставщика

1. Создайте новый адаптер в `internal/suppliers/new_supplier/`:

```go
package new_supplier

import (
    "context"
    "gomarketplace_api/internal/core/models"
    "gomarketplace_api/internal/core/ports"
)

type NewSupplierAdapter struct {
    // ...
}

func NewNewSupplierAdapter(/* параметры */) (*NewSupplierAdapter, error) {
    // ...
}

// Реализация интерфейса SupplierPort
func (a *NewSupplierAdapter) SyncProducts(ctx context.Context) ([]*models.Product, error) {
    // Реализация
}

// Остальные методы интерфейса
```

2. Зарегистрируйте адаптер в контейнере зависимостей.

## Добавление нового маркетплейса

1. Создайте новый адаптер в `internal/marketplaces/new_marketplace/`:

```go
package new_marketplace

import (
    "context"
    "gomarketplace_api/internal/core/models"
    "gomarketplace_api/internal/core/ports"
)

type NewMarketplaceAdapter struct {
    // ...
}

func NewNewMarketplaceAdapter(/* параметры */) (*NewMarketplaceAdapter, error) {
    // ...
}

// Реализация интерфейса MarketplacePort
func (a *NewMarketplaceAdapter) SyncProducts(ctx context.Context, products []*models.Product) error {
    // Реализация
}

// Остальные методы интерфейса
```

2. Зарегистрируйте адаптер в контейнере зависимостей.

## Вклад в проект

Если вы хотите внести свой вклад в проект:

1. Создайте fork репозитория
2. Создайте ветку для вашей функциональности (`git checkout -b feature/amazing-feature`)
3. Зафиксируйте изменения (`git commit -m 'Add some amazing feature'`)
4. Отправьте изменения в ваш fork (`git push origin feature/amazing-feature`)
5. Откройте Pull Request

## Лицензия

Этот проект лицензирован под [MIT License](LICENSE).