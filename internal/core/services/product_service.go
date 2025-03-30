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
