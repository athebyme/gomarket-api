package ports

import (
	"context"
	"gomarketplace_api/internal/core/models"
)

// StoragePort определяет интерфейс для работы с постоянным хранилищем данных
// Реализация может использовать любую базу данных (PostgreSQL, MySQL, MongoDB и т.д.)
type StoragePort interface {
	// Методы для работы с продуктами

	// SaveProduct сохраняет продукт в хранилище
	// Если продукт с таким ID уже существует, он будет обновлен
	SaveProduct(ctx context.Context, product *models.Product, tenantID string) error

	// GetProduct получает продукт по ID
	// Возвращает nil, nil если продукт не найден
	GetProduct(ctx context.Context, productID string, tenantID string) (*models.Product, error)

	// ListProducts возвращает список продуктов с поддержкой пагинации и фильтрации
	ListProducts(ctx context.Context, tenantID string, filters map[string]interface{}, page, pageSize int) ([]*models.Product, int, error)

	// DeleteProduct удаляет продукт из хранилища
	DeleteProduct(ctx context.Context, productID string, tenantID string) error

	// Методы для работы с запасами (инвентарем)

	// SaveInventory сохраняет данные об инвентаре продукта
	SaveInventory(ctx context.Context, inventory *models.ProductInventory, tenantID string) error

	// GetInventory получает данные об инвентаре продукта
	GetInventory(ctx context.Context, productID string, tenantID string) (*models.ProductInventory, error)

	// BatchGetInventory получает данные об инвентаре для нескольких продуктов
	BatchGetInventory(ctx context.Context, productIDs []string, tenantID string) (map[string]*models.ProductInventory, error)

	// Методы для работы с ценами

	// SavePrice сохраняет данные о цене продукта
	SavePrice(ctx context.Context, price *models.ProductPrice, tenantID string) error

	// GetPrice получает данные о цене продукта
	GetPrice(ctx context.Context, productID string, tenantID string) (*models.ProductPrice, error)

	// BatchGetPrices получает данные о ценах для нескольких продуктов
	BatchGetPrices(ctx context.Context, productIDs []string, tenantID string) (map[string]*models.ProductPrice, error)

	// Методы для работы с медиафайлами

	// SaveMedia сохраняет медиафайл продукта
	SaveMedia(ctx context.Context, media *models.ProductMedia, tenantID string) error

	// GetMediaItems получает все медиафайлы продукта
	GetMediaItems(ctx context.Context, productID string, tenantID string) ([]*models.ProductMedia, error)

	// DeleteMedia удаляет медиафайл продукта
	DeleteMedia(ctx context.Context, mediaID string, tenantID string) error

	// Методы для работы с маркетплейсами

	// SaveMarketplaceProduct сохраняет данные о продукте на маркетплейсе
	SaveMarketplaceProduct(ctx context.Context, product *models.MarketplaceProduct, tenantID string) error

	// GetMarketplaceProduct получает данные о продукте на маркетплейсе
	GetMarketplaceProduct(ctx context.Context, productID string, marketplaceID int, tenantID string) (*models.MarketplaceProduct, error)

	// ListMarketplaceProducts возвращает все продукты на конкретном маркетплейсе
	ListMarketplaceProducts(ctx context.Context, marketplaceID int, tenantID string) ([]*models.MarketplaceProduct, error)

	// Методы для работы с поставщиками

	// SaveSupplier сохраняет данные о поставщике
	SaveSupplier(ctx context.Context, supplier *models.Supplier, tenantID string) error

	// GetSupplier получает данные о поставщике
	GetSupplier(ctx context.Context, supplierID int, tenantID string) (*models.Supplier, error)

	// ListSuppliers возвращает список всех поставщиков
	ListSuppliers(ctx context.Context, tenantID string) ([]*models.Supplier, error)

	// Методы для транзакций (опционально)

	// BeginTx начинает новую транзакцию
	BeginTx(ctx context.Context) (context.Context, error)

	// CommitTx фиксирует транзакцию
	CommitTx(ctx context.Context) error

	// RollbackTx откатывает транзакцию
	RollbackTx(ctx context.Context) error

	// Метод закрытия соединения

	// Close закрывает соединение с хранилищем
	Close() error
}

// ExtendedStoragePort расширяет базовый StoragePort дополнительными методами
type ExtendedStoragePort interface {
	StoragePort

	// Методы с улучшенной фильтрацией и пагинацией

	// ListProductsWithFilter возвращает список продуктов с учетом структурированного фильтра и пагинации
	ListProductsWithFilter(ctx context.Context, tenantID string, filter *models.ProductFilter, pagination *models.Pagination) (*models.PagedResult, error)

	// CountProducts возвращает количество продуктов с учетом фильтра
	CountProducts(ctx context.Context, tenantID string, filter *models.ProductFilter) (int64, error)

	// Методы для работы с историей изменений

	// SaveProductHistory сохраняет историю изменений продукта
	SaveProductHistory(ctx context.Context, productID string, changeType string, before, after *models.Product, tenantID string) error

	// GetProductHistory возвращает историю изменений продукта
	GetProductHistory(ctx context.Context, productID string, tenantID string, pagination *models.Pagination) (*models.PagedResult, error)

	// Методы для работы с групповыми операциями

	// BatchSaveProducts сохраняет несколько продуктов за одну операцию
	BatchSaveProducts(ctx context.Context, products []*models.Product, tenantID string) error

	// BatchDeleteProducts удаляет несколько продуктов за одну операцию
	BatchDeleteProducts(ctx context.Context, productIDs []string, tenantID string) error

	// Методы для работы с категориями продуктов

	// GetProductCategories возвращает все категории продуктов
	GetProductCategories(ctx context.Context, tenantID string) ([]*models.ProductCategory, error)

	// GetProductsByCategory возвращает продукты из указанной категории
	GetProductsByCategory(ctx context.Context, categoryID string, tenantID string, pagination *models.Pagination) (*models.PagedResult, error)

	// Методы для поиска

	// SearchProducts выполняет полнотекстовый поиск по продуктам
	SearchProducts(ctx context.Context, query string, tenantID string, pagination *models.Pagination) (*models.PagedResult, error)

	// Методы для работы с связанными продуктами

	// GetRelatedProducts возвращает связанные продукты
	GetRelatedProducts(ctx context.Context, productID string, tenantID string, limit int) ([]*models.Product, error)

	// SaveRelatedProducts сохраняет связи между продуктами
	SaveRelatedProducts(ctx context.Context, productID string, relatedIDs []string, tenantID string) error
}
