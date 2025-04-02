package services

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"gomarketplace_api/internal/core/models"
	"gomarketplace_api/internal/core/ports"
)

// ProductService определяет интерфейс сервиса для работы с продуктами
type ProdService interface {
	// GetProduct получает продукт по ID с учетом арендатора
	GetProduct(ctx context.Context, productID string, tenantID string) (*models.Product, error)

	// CreateProduct создает новый продукт
	CreateProduct(ctx context.Context, product *models.Product, tenantID string) (*models.Product, error)

	// UpdateProduct обновляет существующий продукт
	UpdateProduct(ctx context.Context, product *models.Product, tenantID string) (*models.Product, error)

	// DeleteProduct удаляет продукт
	DeleteProduct(ctx context.Context, productID string, tenantID string) error

	// ListProducts возвращает список продуктов с поддержкой пагинации и фильтрации
	ListProducts(ctx context.Context, filters map[string]interface{}, page, pageSize int, tenantID string) ([]*models.Product, int, error)

	// SyncProductsFromSupplier синхронизирует продукты от поставщика
	SyncProductsFromSupplier(ctx context.Context, supplierID int, tenantID string) (int, error)

	// SyncProductToMarketplace синхронизирует продукт с маркетплейсом
	SyncProductToMarketplace(ctx context.Context, productID string, marketplaceID int, tenantID string) error
}

// ExtendedProductService расширяет базовый интерфейс ProductService
type ExtendedProductService interface {
	ProdService

	// Методы с улучшенной фильтрацией и пагинацией

	// ListProductsWithFilter возвращает список продуктов с учетом структурированного фильтра и пагинации
	ListProductsWithFilter(ctx context.Context, filter *models.ProductFilter, pagination *models.Pagination, tenantID string) (*models.PagedResult, error)

	// Методы для массовых операций

	// BatchCreateProducts создает несколько продуктов за одну операцию
	BatchCreateProducts(ctx context.Context, products []*models.Product, tenantID string) (int, error)

	// BatchUpdateProducts обновляет несколько продуктов за одну операцию
	BatchUpdateProducts(ctx context.Context, products []*models.Product, tenantID string) (int, error)

	// BatchDeleteProducts удаляет несколько продуктов за одну операцию
	BatchDeleteProducts(ctx context.Context, productIDs []string, tenantID string) (int, error)

	// Методы для работы с историей изменений

	// GetProductHistory возвращает историю изменений продукта
	GetProductHistory(ctx context.Context, productID string, pagination *models.Pagination, tenantID string) (*models.PagedResult, error)

	// Методы для работы с категориями

	// GetProductCategories возвращает все категории продуктов
	GetProductCategories(ctx context.Context, tenantID string) ([]*models.ProductCategory, error)

	// GetProductsByCategory возвращает продукты из указанной категории
	GetProductsByCategory(ctx context.Context, categoryID string, pagination *models.Pagination, tenantID string) (*models.PagedResult, error)

	// Методы для полнотекстового поиска

	// SearchProducts выполняет полнотекстовый поиск по продуктам
	SearchProducts(ctx context.Context, query string, pagination *models.Pagination, tenantID string) (*models.PagedResult, error)

	// Методы для работы с связанными продуктами

	// GetRelatedProducts возвращает связанные продукты
	GetRelatedProducts(ctx context.Context, productID string, limit int, tenantID string) ([]*models.Product, error)

	// SetRelatedProducts устанавливает связи между продуктами
	SetRelatedProducts(ctx context.Context, productID string, relatedIDs []string, tenantID string) error
}

// ProductService управляет продуктами с использованием транзакционного менеджера
type ProductService struct {
	suppliers    map[int]ports.SupplierPort
	marketplaces map[int]ports.MarketplacePort
	txPort       ports.TransactionPort
	logger       ports.LoggerPort
}

// NewProductService создает новый экземпляр ProductService
func NewProductService(
	suppliers map[int]ports.SupplierPort,
	marketplaces map[int]ports.MarketplacePort,
	txPort ports.TransactionPort,
	logger ports.LoggerPort,
) *ProductService {
	return &ProductService{
		suppliers:    suppliers,
		marketplaces: marketplaces,
		txPort:       txPort,
		logger:       logger,
	}
}

// GetProduct получает продукт по ID с учетом арендатора
func (s *ProductService) GetProduct(ctx context.Context, productID string, tenantID string) (*models.Product, error) {
	result, err := s.txPort.ExecuteInTransactionWithTenant(ctx, func(ctx context.Context, tx ports.TransactionalPorts) (interface{}, error) {
		// Попытка получить из кэша
		cacheKey := fmt.Sprintf("product:%s", productID)
		cachedData, err := tx.Cache.GetWithTenant(ctx, cacheKey, tenantID)
		if err == nil && len(cachedData) > 0 {
			var product models.Product
			if err := json.Unmarshal(cachedData, &product); err == nil {
				return &product, nil
			}
		}

		// Если в кэше нет - получаем из хранилища
		product, err := tx.Storage.GetProduct(ctx, productID, tenantID)
		if err != nil {
			return nil, fmt.Errorf("ошибка получения продукта из хранилища: %w", err)
		}
		if product == nil {
			return nil, nil // Продукт не найден
		}

		// Сохраняем в кэш для будущих запросов
		productJSON, _ := json.Marshal(product)
		if err := tx.Cache.SetWithTenant(ctx, cacheKey, productJSON, tenantID, time.Hour); err != nil {
			s.logger.Warn("Не удалось сохранить продукт в кэш",
				"productId", productID,
				"tenantId", tenantID,
				"error", err)
		}

		return product, nil
	}, tenantID)

	if err != nil {
		return nil, err
	}

	if result == nil {
		return nil, nil // Продукт не найден
	}

	return result.(*models.Product), nil
}

// CreateProduct создает новый продукт с использованием транзакции
func (s *ProductService) CreateProduct(ctx context.Context, product *models.Product, tenantID string) (*models.Product, error) {
	result, err := s.txPort.ExecuteInTransactionWithTenant(ctx, func(ctx context.Context, tx ports.TransactionalPorts) (interface{}, error) {
		// Проверяем, существует ли уже продукт с таким ID
		existingProduct, err := tx.Storage.GetProduct(ctx, product.ID, tenantID)
		if err != nil {
			return nil, fmt.Errorf("ошибка проверки существующего продукта: %w", err)
		}
		if existingProduct != nil {
			return nil, fmt.Errorf("продукт с ID %s уже существует", product.ID)
		}

		// Устанавливаем время создания и обновления
		product.CreatedAt = time.Now()
		product.UpdatedAt = time.Now()

		// Сохраняем продукт
		if err := tx.Storage.SaveProduct(ctx, product, tenantID); err != nil {
			return nil, fmt.Errorf("ошибка сохранения продукта: %w", err)
		}

		// Сохраняем в кэш
		productJSON, _ := json.Marshal(product)
		cacheKey := fmt.Sprintf("product:%s", product.ID)
		if err := tx.Cache.SetWithTenant(ctx, cacheKey, productJSON, tenantID, time.Hour); err != nil {
			s.logger.Warn("Не удалось сохранить продукт в кэш",
				"productId", product.ID,
				"tenantId", tenantID,
				"error", err)
		}

		// Публикуем событие создания продукта
		event := map[string]interface{}{
			"event_type": "product_created",
			"product_id": product.ID,
			"tenant_id":  tenantID,
			"timestamp":  time.Now(),
		}

		eventJSON, _ := json.Marshal(event)
		if err := tx.Messaging.PublishForTenant(ctx, "product-events", eventJSON, tenantID); err != nil {
			s.logger.Warn("Не удалось опубликовать событие создания продукта",
				"productId", product.ID,
				"tenantId", tenantID,
				"error", err)
		}

		return product, nil
	}, tenantID)

	if err != nil {
		return nil, err
	}

	return result.(*models.Product), nil
}

// UpdateProduct обновляет существующий продукт с использованием транзакции
func (s *ProductService) UpdateProduct(ctx context.Context, product *models.Product, tenantID string) (*models.Product, error) {
	result, err := s.txPort.ExecuteInTransactionWithTenant(ctx, func(ctx context.Context, tx ports.TransactionalPorts) (interface{}, error) {
		// Проверяем существование продукта
		existingProduct, err := tx.Storage.GetProduct(ctx, product.ID, tenantID)
		if err != nil {
			return nil, fmt.Errorf("ошибка получения существующего продукта: %w", err)
		}
		if existingProduct == nil {
			return nil, fmt.Errorf("продукт с ID %s не найден", product.ID)
		}

		// Сохраняем оригинальное время создания
		product.CreatedAt = existingProduct.CreatedAt
		// Обновляем время обновления
		product.UpdatedAt = time.Now()

		// Сохраняем продукт
		if err := tx.Storage.SaveProduct(ctx, product, tenantID); err != nil {
			return nil, fmt.Errorf("ошибка обновления продукта: %w", err)
		}

		// Обновляем кэш
		productJSON, _ := json.Marshal(product)
		cacheKey := fmt.Sprintf("product:%s", product.ID)
		if err := tx.Cache.SetWithTenant(ctx, cacheKey, productJSON, tenantID, time.Hour); err != nil {
			s.logger.Warn("Не удалось обновить продукт в кэше",
				"productId", product.ID,
				"tenantId", tenantID,
				"error", err)
		}

		// Публикуем событие обновления продукта
		event := map[string]interface{}{
			"event_type": "product_updated",
			"product_id": product.ID,
			"tenant_id":  tenantID,
			"timestamp":  time.Now(),
		}

		eventJSON, _ := json.Marshal(event)
		if err := tx.Messaging.PublishForTenant(ctx, "product-events", eventJSON, tenantID); err != nil {
			s.logger.Warn("Не удалось опубликовать событие обновления продукта",
				"productId", product.ID,
				"tenantId", tenantID,
				"error", err)
		}

		return product, nil
	}, tenantID)

	if err != nil {
		return nil, err
	}

	return result.(*models.Product), nil
}

// DeleteProduct удаляет продукт с использованием транзакции
func (s *ProductService) DeleteProduct(ctx context.Context, productID string, tenantID string) error {
	_, err := s.txPort.ExecuteInTransactionWithTenant(ctx, func(ctx context.Context, tx ports.TransactionalPorts) (interface{}, error) {
		// Проверяем существование продукта
		existingProduct, err := tx.Storage.GetProduct(ctx, productID, tenantID)
		if err != nil {
			return nil, fmt.Errorf("ошибка получения существующего продукта: %w", err)
		}
		if existingProduct == nil {
			return nil, fmt.Errorf("продукт с ID %s не найден", productID)
		}

		// Удаляем продукт
		if err := tx.Storage.DeleteProduct(ctx, productID, tenantID); err != nil {
			return nil, fmt.Errorf("ошибка удаления продукта: %w", err)
		}

		// Удаляем из кэша
		cacheKey := fmt.Sprintf("product:%s", productID)
		if err := tx.Cache.DeleteWithTenant(ctx, cacheKey, tenantID); err != nil {
			s.logger.Warn("Не удалось удалить продукт из кэша",
				"productId", productID,
				"tenantId", tenantID,
				"error", err)
		}

		// Публикуем событие удаления продукта
		event := map[string]interface{}{
			"event_type": "product_deleted",
			"product_id": productID,
			"tenant_id":  tenantID,
			"timestamp":  time.Now(),
		}

		eventJSON, _ := json.Marshal(event)
		if err := tx.Messaging.PublishForTenant(ctx, "product-events", eventJSON, tenantID); err != nil {
			s.logger.Warn("Не удалось опубликовать событие удаления продукта",
				"productId", productID,
				"tenantId", tenantID,
				"error", err)
		}

		return nil, nil
	}, tenantID)

	return err
}

// ListProducts возвращает список продуктов с поддержкой пагинации и фильтрации
func (s *ProductService) ListProducts(ctx context.Context, filters map[string]interface{}, page, pageSize int, tenantID string) ([]*models.Product, int, error) {
	result, err := s.txPort.ExecuteInTransactionWithTenant(ctx, func(ctx context.Context, tx ports.TransactionalPorts) (interface{}, error) {
		products, totalCount, err := tx.Storage.ListProducts(ctx, tenantID, filters, page, pageSize)
		if err != nil {
			return nil, fmt.Errorf("ошибка получения списка продуктов: %w", err)
		}

		return map[string]interface{}{
			"products":   products,
			"totalCount": totalCount,
		}, nil
	}, tenantID)

	if err != nil {
		return nil, 0, err
	}

	resultMap := result.(map[string]interface{})
	products := resultMap["products"].([]*models.Product)
	totalCount := resultMap["totalCount"].(int)

	return products, totalCount, nil
}

// SyncProductsFromSupplier синхронизирует продукты от поставщика с использованием транзакции
func (s *ProductService) SyncProductsFromSupplier(ctx context.Context, supplierID int, tenantID string) (int, error) {
	supplier, exists := s.suppliers[supplierID]
	if !exists {
		return 0, fmt.Errorf("поставщик с ID %d не найден", supplierID)
	}

	// Получаем продукты от поставщика
	products, err := supplier.SyncProducts(ctx)
	if err != nil {
		return 0, fmt.Errorf("ошибка получения продуктов от поставщика: %w", err)
	}

	s.logger.Info("Получены продукты от поставщика",
		"supplierId", supplierID,
		"supplierName", supplier.GetSupplierName(),
		"productCount", len(products))

	// Используем транзакцию для синхронизации продуктов
	result, err := s.txPort.ExecuteInTransactionWithTenant(ctx, func(ctx context.Context, tx ports.TransactionalPorts) (interface{}, error) {
		syncedCount := 0

		// Обрабатываем продукты группами по 100 штук для экономии памяти
		batchSize := 100
		for i := 0; i < len(products); i += batchSize {
			end := i + batchSize
			if end > len(products) {
				end = len(products)
			}

			batch := products[i:end]

			for _, product := range batch {
				if err := tx.Storage.SaveProduct(ctx, product, tenantID); err != nil {
					s.logger.Error("Ошибка сохранения продукта",
						"productId", product.ID,
						"supplierId", product.SupplierID,
						"tenantId", tenantID,
						"error", err)
					continue
				}

				syncedCount++

				// Опционально кэшируем продукт
				productJSON, _ := json.Marshal(product)
				cacheKey := fmt.Sprintf("product:%s", product.ID)
				if err := tx.Cache.SetWithTenant(ctx, cacheKey, productJSON, tenantID, time.Hour); err != nil {
					s.logger.Warn("Не удалось сохранить продукт в кэш",
						"productId", product.ID,
						"tenantId", tenantID,
						"error", err)
				}
			}
		}

		// Публикуем событие синхронизации
		event := map[string]interface{}{
			"event_type":   "products_synced",
			"supplier_id":  supplierID,
			"tenant_id":    tenantID,
			"synced_count": syncedCount,
			"total_count":  len(products),
			"timestamp":    time.Now(),
		}

		eventJSON, _ := json.Marshal(event)
		if err := tx.Messaging.PublishForTenant(ctx, "sync-events", eventJSON, tenantID); err != nil {
			s.logger.Warn("Не удалось опубликовать событие синхронизации",
				"supplierId", supplierID,
				"tenantId", tenantID,
				"error", err)
		}

		return syncedCount, nil
	}, tenantID)

	if err != nil {
		return 0, err
	}

	return result.(int), nil
}

// SyncProductToMarketplace синхронизирует продукт с маркетплейсом с использованием транзакции
func (s *ProductService) SyncProductToMarketplace(ctx context.Context, productID string, marketplaceID int, tenantID string) error {
	marketplace, exists := s.marketplaces[marketplaceID]
	if !exists {
		return fmt.Errorf("маркетплейс с ID %d не найден", marketplaceID)
	}

	_, err := s.txPort.ExecuteInTransactionWithTenant(ctx, func(ctx context.Context, tx ports.TransactionalPorts) (interface{}, error) {
		// Получаем продукт
		product, err := tx.Storage.GetProduct(ctx, productID, tenantID)
		if err != nil {
			return nil, fmt.Errorf("ошибка получения продукта: %w", err)
		}
		if product == nil {
			return nil, fmt.Errorf("продукт с ID %s не найден", productID)
		}

		// Синхронизируем продукт с маркетплейсом
		if err := marketplace.SyncProducts(ctx, []*models.Product{product}); err != nil {
			return nil, fmt.Errorf("ошибка синхронизации продукта с маркетплейсом: %w", err)
		}

		// Сохраняем связь продукта с маркетплейсом
		marketplaceProduct := &models.MarketplaceProduct{
			ID:            fmt.Sprintf("mp-%d-%s", marketplaceID, productID),
			MarketplaceID: marketplaceID,
			CoreProductID: productID,
			Status:        "synced",
		}

		if err := tx.Storage.SaveMarketplaceProduct(ctx, marketplaceProduct, tenantID); err != nil {
			return nil, fmt.Errorf("ошибка сохранения связи продукта с маркетплейсом: %w", err)
		}

		// Публикуем событие синхронизации с маркетплейсом
		event := map[string]interface{}{
			"event_type":     "product_synced_to_marketplace",
			"product_id":     productID,
			"marketplace_id": marketplaceID,
			"tenant_id":      tenantID,
			"timestamp":      time.Now(),
		}

		eventJSON, _ := json.Marshal(event)
		if err := tx.Messaging.PublishForTenant(ctx, "marketplace-events", eventJSON, tenantID); err != nil {
			s.logger.Warn("Не удалось опубликовать событие синхронизации с маркетплейсом",
				"productId", productID,
				"marketplaceId", marketplaceID,
				"tenantId", tenantID,
				"error", err)
		}

		return nil, nil
	}, tenantID)

	return err
}

// ImprovedProductService реализует расширенный интерфейс ProductService
type ImprovedProductService struct {
	*ProductService
	extStorage ports.ExtendedStoragePort
}

// NewImprovedProductService создает новый экземпляр ImprovedProductService
func NewImprovedProductService(
	base *ProductService,
	extStorage ports.ExtendedStoragePort,
) *ImprovedProductService {
	return &ImprovedProductService{
		ProductService: base,
		extStorage:     extStorage,
	}
}

// ListProductsWithFilter возвращает список продуктов с учетом структурированного фильтра и пагинации
func (s *ImprovedProductService) ListProductsWithFilter(
	ctx context.Context,
	filter *models.ProductFilter,
	pagination *models.Pagination,
	tenantID string,
) (*models.PagedResult, error) {
	result, err := s.txPort.ExecuteInTransactionWithTenant(ctx, func(ctx context.Context, tx ports.TransactionalPorts) (interface{}, error) {
		// Приводим хранилище к ExtendedStoragePort
		extStorage, ok := tx.Storage.(ports.ExtendedStoragePort)
		if !ok {
			// Если не удалось привести, используем базовый метод с преобразованием моделей
			s.logger.Warn("Storage не поддерживает ExtendedStoragePort, используем базовый метод")

			// Преобразуем фильтр
			baseFilters := filter.ToMap()

			// Вызываем базовый метод
			products, totalCount, err := tx.Storage.ListProducts(ctx, tenantID, baseFilters, pagination.Page, pagination.PageSize)
			if err != nil {
				return nil, fmt.Errorf("ошибка получения списка продуктов: %w", err)
			}

			// Создаем результат
			pagination.SetTotal(int64(totalCount))
			return models.NewPagedResult(products, pagination), nil
		}

		// Используем расширенный метод
		return extStorage.ListProductsWithFilter(ctx, tenantID, filter, pagination)
	}, tenantID)

	if err != nil {
		return nil, err
	}

	return result.(*models.PagedResult), nil
}

// BatchCreateProducts создает несколько продуктов за одну операцию
func (s *ImprovedProductService) BatchCreateProducts(
	ctx context.Context,
	products []*models.Product,
	tenantID string,
) (int, error) {
	result, err := s.txPort.ExecuteInTransactionWithTenant(ctx, func(ctx context.Context, tx ports.TransactionalPorts) (interface{}, error) {
		// Приводим хранилище к ExtendedStoragePort
		extStorage, ok := tx.Storage.(ports.ExtendedStoragePort)
		if !ok {
			// Если не удалось привести, создаем продукты по одному
			createdCount := 0
			for _, product := range products {
				if err := tx.Storage.SaveProduct(ctx, product, tenantID); err != nil {
					s.logger.Error("Ошибка создания продукта",
						"productId", product.ID,
						"error", err)
					continue
				}
				createdCount++
			}
			return createdCount, nil
		}

		// Устанавливаем время создания и обновления для всех продуктов
		now := time.Now()
		for _, product := range products {
			product.CreatedAt = now
			product.UpdatedAt = now
		}

		// Используем пакетный метод
		if err := extStorage.BatchSaveProducts(ctx, products, tenantID); err != nil {
			return 0, fmt.Errorf("ошибка массового создания продуктов: %w", err)
		}

		// Публикуем событие массового создания продуктов
		event := map[string]interface{}{
			"event_type":    "products_batch_created",
			"tenant_id":     tenantID,
			"product_count": len(products),
			"timestamp":     time.Now(),
		}

		eventJSON, _ := json.Marshal(event)
		if err := tx.Messaging.PublishForTenant(ctx, "product-events", eventJSON, tenantID); err != nil {
			s.logger.Warn("Не удалось опубликовать событие массового создания продуктов",
				"tenantId", tenantID,
				"error", err)
		}

		return len(products), nil
	}, tenantID)

	if err != nil {
		return 0, err
	}

	return result.(int), nil
}

// BatchUpdateProducts обновляет несколько продуктов за одну операцию
func (s *ImprovedProductService) BatchUpdateProducts(
	ctx context.Context,
	products []*models.Product,
	tenantID string,
) (int, error) {
	result, err := s.txPort.ExecuteInTransactionWithTenant(ctx, func(ctx context.Context, tx ports.TransactionalPorts) (interface{}, error) {
		// Приводим хранилище к ExtendedStoragePort
		extStorage, ok := tx.Storage.(ports.ExtendedStoragePort)
		if !ok {
			// Если не удалось привести, обновляем продукты по одному
			updatedCount := 0
			for _, product := range products {
				// Получаем существующий продукт для проверки и сохранения времени создания
				existingProduct, err := tx.Storage.GetProduct(ctx, product.ID, tenantID)
				if err != nil {
					s.logger.Error("Ошибка получения существующего продукта",
						"productId", product.ID,
						"error", err)
					continue
				}
				if existingProduct == nil {
					s.logger.Error("Продукт для обновления не найден",
						"productId", product.ID)
					continue
				}

				// Сохраняем время создания
				product.CreatedAt = existingProduct.CreatedAt
				// Обновляем время обновления
				product.UpdatedAt = time.Now()

				if err := tx.Storage.SaveProduct(ctx, product, tenantID); err != nil {
					s.logger.Error("Ошибка обновления продукта",
						"productId", product.ID,
						"error", err)
					continue
				}
				updatedCount++
			}
			return updatedCount, nil
		}

		// Обновляем время обновления для всех продуктов
		now := time.Now()
		for _, product := range products {
			product.UpdatedAt = now
		}

		// Используем пакетный метод
		if err := extStorage.BatchSaveProducts(ctx, products, tenantID); err != nil {
			return 0, fmt.Errorf("ошибка массового обновления продуктов: %w", err)
		}

		// Публикуем событие массового обновления продуктов
		event := map[string]interface{}{
			"event_type":    "products_batch_updated",
			"tenant_id":     tenantID,
			"product_count": len(products),
			"timestamp":     time.Now(),
		}

		eventJSON, _ := json.Marshal(event)
		if err := tx.Messaging.PublishForTenant(ctx, "product-events", eventJSON, tenantID); err != nil {
			s.logger.Warn("Не удалось опубликовать событие массового обновления продуктов",
				"tenantId", tenantID,
				"error", err)
		}

		return len(products), nil
	}, tenantID)

	if err != nil {
		return 0, err
	}

	return result.(int), nil
}

// BatchDeleteProducts удаляет несколько продуктов за одну операцию
func (s *ImprovedProductService) BatchDeleteProducts(
	ctx context.Context,
	productIDs []string,
	tenantID string,
) (int, error) {
	result, err := s.txPort.ExecuteInTransactionWithTenant(ctx, func(ctx context.Context, tx ports.TransactionalPorts) (interface{}, error) {
		// Приводим хранилище к ExtendedStoragePort
		extStorage, ok := tx.Storage.(ports.ExtendedStoragePort)
		if !ok {
			// Если не удалось привести, удаляем продукты по одному
			deletedCount := 0
			for _, productID := range productIDs {
				if err := tx.Storage.DeleteProduct(ctx, productID, tenantID); err != nil {
					s.logger.Error("Ошибка удаления продукта",
						"productId", productID,
						"error", err)
					continue
				}

				// Удаляем из кэша
				cacheKey := fmt.Sprintf("product:%s", productID)
				if err := tx.Cache.DeleteWithTenant(ctx, cacheKey, tenantID); err != nil {
					s.logger.Warn("Не удалось удалить продукт из кэша",
						"productId", productID,
						"tenantId", tenantID,
						"error", err)
				}

				deletedCount++
			}
			return deletedCount, nil
		}

		// Используем пакетный метод
		if err := extStorage.BatchDeleteProducts(ctx, productIDs, tenantID); err != nil {
			return 0, fmt.Errorf("ошибка массового удаления продуктов: %w", err)
		}

		// Удаляем продукты из кэша
		for _, productID := range productIDs {
			cacheKey := fmt.Sprintf("product:%s", productID)
			if err := tx.Cache.DeleteWithTenant(ctx, cacheKey, tenantID); err != nil {
				s.logger.Warn("Не удалось удалить продукт из кэша",
					"productId", productID,
					"tenantId", tenantID,
					"error", err)
			}
		}

		// Публикуем событие массового удаления продуктов
		event := map[string]interface{}{
			"event_type":    "products_batch_deleted",
			"tenant_id":     tenantID,
			"product_count": len(productIDs),
			"timestamp":     time.Now(),
		}

		eventJSON, _ := json.Marshal(event)
		if err := tx.Messaging.PublishForTenant(ctx, "product-events", eventJSON, tenantID); err != nil {
			s.logger.Warn("Не удалось опубликовать событие массового удаления продуктов",
				"tenantId", tenantID,
				"error", err)
		}

		return len(productIDs), nil
	}, tenantID)

	if err != nil {
		return 0, err
	}

	return result.(int), nil
}

// GetProductHistory возвращает историю изменений продукта
func (s *ImprovedProductService) GetProductHistory(
	ctx context.Context,
	productID string,
	pagination *models.Pagination,
	tenantID string,
) (*models.PagedResult, error) {
	result, err := s.txPort.ExecuteInTransactionWithTenant(ctx, func(ctx context.Context, tx ports.TransactionalPorts) (interface{}, error) {
		// Приводим хранилище к ExtendedStoragePort
		extStorage, ok := tx.Storage.(ports.ExtendedStoragePort)
		if !ok {
			return nil, fmt.Errorf("хранилище не поддерживает историю изменений")
		}

		return extStorage.GetProductHistory(ctx, productID, tenantID, pagination)
	}, tenantID)

	if err != nil {
		return nil, err
	}

	return result.(*models.PagedResult), nil
}

// GetProductCategories возвращает все категории продуктов
func (s *ImprovedProductService) GetProductCategories(
	ctx context.Context,
	tenantID string,
) ([]*models.ProductCategory, error) {
	result, err := s.txPort.ExecuteInTransactionWithTenant(ctx, func(ctx context.Context, tx ports.TransactionalPorts) (interface{}, error) {
		// Приводим хранилище к ExtendedStoragePort
		extStorage, ok := tx.Storage.(ports.ExtendedStoragePort)
		if !ok {
			return nil, fmt.Errorf("хранилище не поддерживает категории продуктов")
		}

		return extStorage.GetProductCategories(ctx, tenantID)
	}, tenantID)

	if err != nil {
		return nil, err
	}

	return result.([]*models.ProductCategory), nil
}

// GetProductsByCategory возвращает продукты из указанной категории
func (s *ImprovedProductService) GetProductsByCategory(
	ctx context.Context,
	categoryID string,
	pagination *models.Pagination,
	tenantID string,
) (*models.PagedResult, error) {
	result, err := s.txPort.ExecuteInTransactionWithTenant(ctx, func(ctx context.Context, tx ports.TransactionalPorts) (interface{}, error) {
		// Приводим хранилище к ExtendedStoragePort
		extStorage, ok := tx.Storage.(ports.ExtendedStoragePort)
		if !ok {
			return nil, fmt.Errorf("хранилище не поддерживает получение продуктов по категории")
		}

		return extStorage.GetProductsByCategory(ctx, categoryID, tenantID, pagination)
	}, tenantID)

	if err != nil {
		return nil, err
	}

	return result.(*models.PagedResult), nil
}

// SearchProducts выполняет полнотекстовый поиск по продуктам
func (s *ImprovedProductService) SearchProducts(
	ctx context.Context,
	query string,
	pagination *models.Pagination,
	tenantID string,
) (*models.PagedResult, error) {
	result, err := s.txPort.ExecuteInTransactionWithTenant(ctx, func(ctx context.Context, tx ports.TransactionalPorts) (interface{}, error) {
		// Приводим хранилище к ExtendedStoragePort
		extStorage, ok := tx.Storage.(ports.ExtendedStoragePort)
		if !ok {
			return nil, fmt.Errorf("хранилище не поддерживает полнотекстовый поиск")
		}

		return extStorage.SearchProducts(ctx, query, tenantID, pagination)
	}, tenantID)

	if err != nil {
		return nil, err
	}

	return result.(*models.PagedResult), nil
}

// GetRelatedProducts возвращает связанные продукты
func (s *ImprovedProductService) GetRelatedProducts(
	ctx context.Context,
	productID string,
	limit int,
	tenantID string,
) ([]*models.Product, error) {
	result, err := s.txPort.ExecuteInTransactionWithTenant(ctx, func(ctx context.Context, tx ports.TransactionalPorts) (interface{}, error) {
		// Приводим хранилище к ExtendedStoragePort
		extStorage, ok := tx.Storage.(ports.ExtendedStoragePort)
		if !ok {
			return nil, fmt.Errorf("хранилище не поддерживает связанные продукты")
		}

		return extStorage.GetRelatedProducts(ctx, productID, tenantID, limit)
	}, tenantID)

	if err != nil {
		return nil, err
	}

	return result.([]*models.Product), nil
}

// SetRelatedProducts устанавливает связи между продуктами
func (s *ImprovedProductService) SetRelatedProducts(
	ctx context.Context,
	productID string,
	relatedIDs []string,
	tenantID string,
) error {
	_, err := s.txPort.ExecuteInTransactionWithTenant(ctx, func(ctx context.Context, tx ports.TransactionalPorts) (interface{}, error) {
		// Приводим хранилище к ExtendedStoragePort
		extStorage, ok := tx.Storage.(ports.ExtendedStoragePort)
		if !ok {
			return nil, fmt.Errorf("хранилище не поддерживает связанные продукты")
		}

		return nil, extStorage.SaveRelatedProducts(ctx, productID, relatedIDs, tenantID)
	}, tenantID)

	return err
}
