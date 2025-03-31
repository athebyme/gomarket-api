package repositories

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gomarketplace_api/internal/core/models"
	"gomarketplace_api/internal/core/ports"
	"gorm.io/gorm"
)

// GormStoragePort реализует StoragePort с использованием GORM
type GormStoragePort struct {
	db *gorm.DB
}

// NewGormStoragePort создает экземпляр GormStoragePort
func NewGormStoragePort(db *gorm.DB) *GormStoragePort {
	return &GormStoragePort{db: db}
}

// SaveProduct сохраняет продукт в хранилище
func (r *GormStoragePort) SaveProduct(ctx context.Context, product *models.Product, tenantID string) error {
	tx := r.db.WithContext(ctx)

	// Добавляем схему арендатора при необходимости
	if tenantID != "" {
		tx = tx.Table(fmt.Sprintf("tenant_%s.products", tenantID))
	}

	// Если ID не указан, создаем новый продукт
	if product.ID == "" {
		return errors.New("product ID must be specified")
	}

	// Проверяем, существует ли продукт
	var count int64
	tx.Model(&models.Product{}).Where("id = ?", product.ID).Count(&count)

	// Обновляем время
	now := time.Now()
	if count == 0 {
		product.CreatedAt = now
	}
	product.UpdatedAt = now

	// Используем Upsert для создания или обновления
	result := tx.Save(product)
	return result.Error
}

// GetProduct получает продукт по ID
func (r *GormStoragePort) GetProduct(ctx context.Context, productID string, tenantID string) (*models.Product, error) {
	tx := r.db.WithContext(ctx)

	// Добавляем схему арендатора при необходимости
	if tenantID != "" {
		tx = tx.Table(fmt.Sprintf("tenant_%s.products", tenantID))
	}

	var product models.Product
	result := tx.Where("id = ?", productID).First(&product)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, result.Error
	}

	return &product, nil
}

// ListProducts возвращает список продуктов с поддержкой пагинации и фильтрации
func (r *GormStoragePort) ListProducts(ctx context.Context, tenantID string, filters map[string]interface{}, page, pageSize int) ([]*models.Product, int, error) {
	tx := r.db.WithContext(ctx)

	// Добавляем схему арендатора при необходимости
	if tenantID != "" {
		tx = tx.Table(fmt.Sprintf("tenant_%s.products", tenantID))
	}

	// Применяем фильтры
	for key, value := range filters {
		tx = tx.Where(key, value)
	}

	// Подсчитываем общее количество записей
	var total int64
	tx.Model(&models.Product{}).Count(&total)

	// Применяем пагинацию
	offset := (page - 1) * pageSize
	if offset < 0 {
		offset = 0
	}

	var products []*models.Product
	result := tx.Offset(offset).Limit(pageSize).Find(&products)
	if result.Error != nil {
		return nil, 0, result.Error
	}

	return products, int(total), nil
}

// DeleteProduct удаляет продукт из хранилища
func (r *GormStoragePort) DeleteProduct(ctx context.Context, productID string, tenantID string) error {
	tx := r.db.WithContext(ctx)

	// Добавляем схему арендатора при необходимости
	if tenantID != "" {
		tx = tx.Table(fmt.Sprintf("tenant_%s.products", tenantID))
	}

	result := tx.Where("id = ?", productID).Delete(&models.Product{})
	return result.Error
}

// SaveInventory сохраняет данные об инвентаре продукта
func (r *GormStoragePort) SaveInventory(ctx context.Context, inventory *models.ProductInventory, tenantID string) error {
	tx := r.db.WithContext(ctx)

	// Добавляем схему арендатора при необходимости
	if tenantID != "" {
		tx = tx.Table(fmt.Sprintf("tenant_%s.product_inventories", tenantID))
	}

	// Обновляем время
	inventory.UpdatedAt = time.Now()

	// Используем Upsert
	result := tx.Where("product_id = ?", inventory.ProductID).
		Assign(map[string]interface{}{
			"quantity":   inventory.Quantity,
			"updated_at": inventory.UpdatedAt,
		}).
		FirstOrCreate(inventory)

	return result.Error
}

// GetInventory получает данные об инвентаре продукта
func (r *GormStoragePort) GetInventory(ctx context.Context, productID string, tenantID string) (*models.ProductInventory, error) {
	tx := r.db.WithContext(ctx)

	// Добавляем схему арендатора при необходимости
	if tenantID != "" {
		tx = tx.Table(fmt.Sprintf("tenant_%s.product_inventories", tenantID))
	}

	var inventory models.ProductInventory
	result := tx.Where("product_id = ?", productID).First(&inventory)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, result.Error
	}

	return &inventory, nil
}

// BatchGetInventory получает данные об инвентаре для нескольких продуктов
func (r *GormStoragePort) BatchGetInventory(ctx context.Context, productIDs []string, tenantID string) (map[string]*models.ProductInventory, error) {
	tx := r.db.WithContext(ctx)

	// Добавляем схему арендатора при необходимости
	if tenantID != "" {
		tx = tx.Table(fmt.Sprintf("tenant_%s.product_inventories", tenantID))
	}

	var inventories []*models.ProductInventory
	result := tx.Where("product_id IN ?", productIDs).Find(&inventories)
	if result.Error != nil {
		return nil, result.Error
	}

	// Преобразуем список в карту
	inventoryMap := make(map[string]*models.ProductInventory)
	for _, inv := range inventories {
		inventoryMap[inv.ProductID] = inv
	}

	return inventoryMap, nil
}

// SavePrice сохраняет данные о цене продукта
func (r *GormStoragePort) SavePrice(ctx context.Context, price *models.ProductPrice, tenantID string) error {
	tx := r.db.WithContext(ctx)

	// Добавляем схему арендатора при необходимости
	if tenantID != "" {
		tx = tx.Table(fmt.Sprintf("tenant_%s.product_prices", tenantID))
	}

	// Обновляем время
	price.UpdatedAt = time.Now()

	// Используем Upsert
	result := tx.Where("product_id = ?", price.ProductID).
		Assign(map[string]interface{}{
			"base_price":    price.BasePrice,
			"special_price": price.SpecialPrice,
			"currency":      price.Currency,
			"start_date":    price.StartDate,
			"end_date":      price.EndDate,
			"updated_at":    price.UpdatedAt,
		}).
		FirstOrCreate(price)

	return result.Error
}

// GetPrice получает данные о цене продукта
func (r *GormStoragePort) GetPrice(ctx context.Context, productID string, tenantID string) (*models.ProductPrice, error) {
	tx := r.db.WithContext(ctx)

	// Добавляем схему арендатора при необходимости
	if tenantID != "" {
		tx = tx.Table(fmt.Sprintf("tenant_%s.product_prices", tenantID))
	}

	var price models.ProductPrice
	result := tx.Where("product_id = ?", productID).First(&price)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, result.Error
	}

	return &price, nil
}

// BatchGetPrices получает данные о ценах для нескольких продуктов
func (r *GormStoragePort) BatchGetPrices(ctx context.Context, productIDs []string, tenantID string) (map[string]*models.ProductPrice, error) {
	tx := r.db.WithContext(ctx)

	// Добавляем схему арендатора при необходимости
	if tenantID != "" {
		tx = tx.Table(fmt.Sprintf("tenant_%s.product_prices", tenantID))
	}

	var prices []*models.ProductPrice
	result := tx.Where("product_id IN ?", productIDs).Find(&prices)
	if result.Error != nil {
		return nil, result.Error
	}

	// Преобразуем список в карту
	priceMap := make(map[string]*models.ProductPrice)
	for _, p := range prices {
		priceMap[p.ProductID] = p
	}

	return priceMap, nil
}

// SaveMedia сохраняет медиафайл продукта
func (r *GormStoragePort) SaveMedia(ctx context.Context, media *models.ProductMedia, tenantID string) error {
	tx := r.db.WithContext(ctx)

	// Добавляем схему арендатора при необходимости
	if tenantID != "" {
		tx = tx.Table(fmt.Sprintf("tenant_%s.product_media", tenantID))
	}

	// Если ID не указан, создаем новый медиафайл
	if media.ID == "" {
		return errors.New("media ID must be specified")
	}

	// Обновляем время создания
	if media.CreatedAt.IsZero() {
		media.CreatedAt = time.Now()
	}

	// Используем Save для создания или обновления
	result := tx.Save(media)
	return result.Error
}

// GetMediaItems получает все медиафайлы продукта
func (r *GormStoragePort) GetMediaItems(ctx context.Context, productID string, tenantID string) ([]*models.ProductMedia, error) {
	tx := r.db.WithContext(ctx)

	// Добавляем схему арендатора при необходимости
	if tenantID != "" {
		tx = tx.Table(fmt.Sprintf("tenant_%s.product_media", tenantID))
	}

	var mediaItems []*models.ProductMedia
	result := tx.Where("product_id = ?", productID).
		Order("position").
		Find(&mediaItems)

	if result.Error != nil {
		return nil, result.Error
	}

	return mediaItems, nil
}

// DeleteMedia удаляет медиафайл продукта
func (r *GormStoragePort) DeleteMedia(ctx context.Context, mediaID string, tenantID string) error {
	tx := r.db.WithContext(ctx)

	// Добавляем схему арендатора при необходимости
	if tenantID != "" {
		tx = tx.Table(fmt.Sprintf("tenant_%s.product_media", tenantID))
	}

	result := tx.Where("id = ?", mediaID).Delete(&models.ProductMedia{})
	return result.Error
}

// SaveMarketplaceProduct сохраняет данные о продукте на маркетплейсе
func (r *GormStoragePort) SaveMarketplaceProduct(ctx context.Context, product *models.MarketplaceProduct, tenantID string) error {
	tx := r.db.WithContext(ctx)

	// Добавляем схему арендатора при необходимости
	if tenantID != "" {
		tx = tx.Table(fmt.Sprintf("tenant_%s.marketplace_products", tenantID))
	}

	// Если ID не указан, создаем новый продукт
	if product.ID == "" {
		return errors.New("marketplace product ID must be specified")
	}

	// Используем Save для создания или обновления
	result := tx.Save(product)
	return result.Error
}

// GetMarketplaceProduct получает данные о продукте на маркетплейсе
func (r *GormStoragePort) GetMarketplaceProduct(ctx context.Context, productID string, marketplaceID int, tenantID string) (*models.MarketplaceProduct, error) {
	tx := r.db.WithContext(ctx)

	// Добавляем схему арендатора при необходимости
	if tenantID != "" {
		tx = tx.Table(fmt.Sprintf("tenant_%s.marketplace_products", tenantID))
	}

	var product models.MarketplaceProduct
	result := tx.Where("core_product_id = ? AND marketplace_id = ?", productID, marketplaceID).First(&product)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, result.Error
	}

	return &product, nil
}

// ListMarketplaceProducts возвращает все продукты на конкретном маркетплейсе
func (r *GormStoragePort) ListMarketplaceProducts(ctx context.Context, marketplaceID int, tenantID string) ([]*models.MarketplaceProduct, error) {
	tx := r.db.WithContext(ctx)

	// Добавляем схему арендатора при необходимости
	if tenantID != "" {
		tx = tx.Table(fmt.Sprintf("tenant_%s.marketplace_products", tenantID))
	}

	var products []*models.MarketplaceProduct
	result := tx.Where("marketplace_id = ?", marketplaceID).Find(&products)
	if result.Error != nil {
		return nil, result.Error
	}

	return products, nil
}

// SaveSupplier сохраняет данные о поставщике
func (r *GormStoragePort) SaveSupplier(ctx context.Context, supplier *models.Supplier, tenantID string) error {
	tx := r.db.WithContext(ctx)

	// Добавляем схему арендатора при необходимости
	if tenantID != "" {
		tx = tx.Table(fmt.Sprintf("tenant_%s.suppliers", tenantID))
	}

	// Обновляем время
	now := time.Now()

	// Для новых поставщиков
	if supplier.ID == 0 {
		supplier.CreatedAt = now
	}
	supplier.UpdatedAt = now

	// Используем Save для создания или обновления
	result := tx.Save(supplier)
	return result.Error
}

// GetSupplier получает данные о поставщике
func (r *GormStoragePort) GetSupplier(ctx context.Context, supplierID int, tenantID string) (*models.Supplier, error) {
	tx := r.db.WithContext(ctx)

	// Добавляем схему арендатора при необходимости
	if tenantID != "" {
		tx = tx.Table(fmt.Sprintf("tenant_%s.suppliers", tenantID))
	}

	var supplier models.Supplier
	result := tx.Where("id = ?", supplierID).First(&supplier)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, result.Error
	}

	return &supplier, nil
}

// ListSuppliers возвращает список всех поставщиков
func (r *GormStoragePort) ListSuppliers(ctx context.Context, tenantID string) ([]*models.Supplier, error) {
	tx := r.db.WithContext(ctx)

	// Добавляем схему арендатора при необходимости
	if tenantID != "" {
		tx = tx.Table(fmt.Sprintf("tenant_%s.suppliers", tenantID))
	}

	var suppliers []*models.Supplier
	result := tx.Find(&suppliers)
	if result.Error != nil {
		return nil, result.Error
	}

	return suppliers, nil
}

// Методы для транзакций
func (r *GormStoragePort) BeginTx(ctx context.Context) (context.Context, error) {
	tx := r.db.WithContext(ctx).Begin()
	if tx.Error != nil {
		return nil, tx.Error
	}

	// Сохраняем транзакцию в контексте
	return context.WithValue(ctx, ports.TransactionKey{}, tx), nil
}

func (r *GormStoragePort) CommitTx(ctx context.Context) error {
	tx, ok := ctx.Value(ports.TransactionKey{}).(*gorm.DB)
	if !ok || tx == nil {
		return errors.New("no transaction found in context")
	}

	return tx.Commit().Error
}

func (r *GormStoragePort) RollbackTx(ctx context.Context) error {
	tx, ok := ctx.Value(ports.TransactionKey{}).(*gorm.DB)
	if !ok || tx == nil {
		return errors.New("no transaction found in context")
	}

	return tx.Rollback().Error
}

func (r *GormStoragePort) Close() error {
	// GORM не имеет метода Close
	// Для закрытия соединения нужен доступ к sql.DB
	sqlDB, err := r.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}

var _ ports.StoragePort = (*GormStoragePort)(nil)
