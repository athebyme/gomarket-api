package transactions

import (
	"context"
	"gomarketplace_api/internal/core/models"
	"gomarketplace_api/internal/core/ports"
	"gomarketplace_api/internal/infrastructure/transactional/transactions/interfaces"
)

// TransactionalStorageAdapter адаптер для хранилища с поддержкой транзакций
type TransactionalStorageAdapter struct {
	baseStorage ports.StoragePort
}

// NewTransactionalStorageAdapter создает адаптер хранилища с поддержкой транзакций
func NewTransactionalStorageAdapter(storage ports.StoragePort) ports.TransactionalStoragePort {
	return &TransactionalStorageAdapter{
		baseStorage: storage,
	}
}

// WithTransaction возвращает хранилище, связанное с указанной транзакцией
func (a *TransactionalStorageAdapter) WithTransaction(tx interfaces.Transaction) ports.StoragePort {
	return &TransactionalStorage{
		baseStorage: a.baseStorage,
		tx:          tx,
	}
}

// TransactionalStorage реализует StoragePort в контексте транзакции
type TransactionalStorage struct {
	baseStorage ports.StoragePort
	tx          interfaces.Transaction
}

// SaveProduct сохраняет продукт в хранилище
func (s *TransactionalStorage) SaveProduct(ctx context.Context, product *models.Product, tenantID string) error {
	// Добавляем информацию о транзакции в контекст
	ctx = context.WithValue(ctx, ports.TransactionKey{}, s.tx)
	return s.baseStorage.SaveProduct(ctx, product, tenantID)
}

// GetProduct получает продукт по ID
func (s *TransactionalStorage) GetProduct(ctx context.Context, productID string, tenantID string) (*models.Product, error) {
	ctx = context.WithValue(ctx, ports.TransactionKey{}, s.tx)
	return s.baseStorage.GetProduct(ctx, productID, tenantID)
}

// ListProducts возвращает список продуктов
func (s *TransactionalStorage) ListProducts(ctx context.Context, tenantID string, filters map[string]interface{}, page, pageSize int) ([]*models.Product, int, error) {
	ctx = context.WithValue(ctx, ports.TransactionKey{}, s.tx)
	return s.baseStorage.ListProducts(ctx, tenantID, filters, page, pageSize)
}

// DeleteProduct удаляет продукт из хранилища
func (s *TransactionalStorage) DeleteProduct(ctx context.Context, productID string, tenantID string) error {
	ctx = context.WithValue(ctx, ports.TransactionKey{}, s.tx)
	return s.baseStorage.DeleteProduct(ctx, productID, tenantID)
}

// SaveInventory сохраняет данные об инвентаре продукта
func (s *TransactionalStorage) SaveInventory(ctx context.Context, inventory *models.ProductInventory, tenantID string) error {
	ctx = context.WithValue(ctx, ports.TransactionKey{}, s.tx)
	return s.baseStorage.SaveInventory(ctx, inventory, tenantID)
}

// GetInventory получает данные об инвентаре продукта
func (s *TransactionalStorage) GetInventory(ctx context.Context, productID string, tenantID string) (*models.ProductInventory, error) {
	ctx = context.WithValue(ctx, ports.TransactionKey{}, s.tx)
	return s.baseStorage.GetInventory(ctx, productID, tenantID)
}

// BatchGetInventory получает данные об инвентаре для нескольких продуктов
func (s *TransactionalStorage) BatchGetInventory(ctx context.Context, productIDs []string, tenantID string) (map[string]*models.ProductInventory, error) {
	ctx = context.WithValue(ctx, ports.TransactionKey{}, s.tx)
	return s.baseStorage.BatchGetInventory(ctx, productIDs, tenantID)
}

// SavePrice сохраняет данные о цене продукта
func (s *TransactionalStorage) SavePrice(ctx context.Context, price *models.ProductPrice, tenantID string) error {
	ctx = context.WithValue(ctx, ports.TransactionKey{}, s.tx)
	return s.baseStorage.SavePrice(ctx, price, tenantID)
}

// GetPrice получает данные о цене продукта
func (s *TransactionalStorage) GetPrice(ctx context.Context, productID string, tenantID string) (*models.ProductPrice, error) {
	ctx = context.WithValue(ctx, ports.TransactionKey{}, s.tx)
	return s.baseStorage.GetPrice(ctx, productID, tenantID)
}

// BatchGetPrices получает данные о ценах для нескольких продуктов
func (s *TransactionalStorage) BatchGetPrices(ctx context.Context, productIDs []string, tenantID string) (map[string]*models.ProductPrice, error) {
	ctx = context.WithValue(ctx, ports.TransactionKey{}, s.tx)
	return s.baseStorage.BatchGetPrices(ctx, productIDs, tenantID)
}

// SaveMedia сохраняет медиафайл продукта
func (s *TransactionalStorage) SaveMedia(ctx context.Context, media *models.ProductMedia, tenantID string) error {
	ctx = context.WithValue(ctx, ports.TransactionKey{}, s.tx)
	return s.baseStorage.SaveMedia(ctx, media, tenantID)
}

// GetMediaItems получает все медиафайлы продукта
func (s *TransactionalStorage) GetMediaItems(ctx context.Context, productID string, tenantID string) ([]*models.ProductMedia, error) {
	ctx = context.WithValue(ctx, ports.TransactionKey{}, s.tx)
	return s.baseStorage.GetMediaItems(ctx, productID, tenantID)
}

// DeleteMedia удаляет медиафайл продукта
func (s *TransactionalStorage) DeleteMedia(ctx context.Context, mediaID string, tenantID string) error {
	ctx = context.WithValue(ctx, ports.TransactionKey{}, s.tx)
	return s.baseStorage.DeleteMedia(ctx, mediaID, tenantID)
}

// SaveMarketplaceProduct сохраняет данные о продукте на маркетплейсе
func (s *TransactionalStorage) SaveMarketplaceProduct(ctx context.Context, product *models.MarketplaceProduct, tenantID string) error {
	ctx = context.WithValue(ctx, ports.TransactionKey{}, s.tx)
	return s.baseStorage.SaveMarketplaceProduct(ctx, product, tenantID)
}

// GetMarketplaceProduct получает данные о продукте на маркетплейсе
func (s *TransactionalStorage) GetMarketplaceProduct(ctx context.Context, productID string, marketplaceID int, tenantID string) (*models.MarketplaceProduct, error) {
	ctx = context.WithValue(ctx, ports.TransactionKey{}, s.tx)
	return s.baseStorage.GetMarketplaceProduct(ctx, productID, marketplaceID, tenantID)
}

// ListMarketplaceProducts возвращает все продукты на конкретном маркетплейсе
func (s *TransactionalStorage) ListMarketplaceProducts(ctx context.Context, marketplaceID int, tenantID string) ([]*models.MarketplaceProduct, error) {
	ctx = context.WithValue(ctx, ports.TransactionKey{}, s.tx)
	return s.baseStorage.ListMarketplaceProducts(ctx, marketplaceID, tenantID)
}

// SaveSupplier сохраняет данные о поставщике
func (s *TransactionalStorage) SaveSupplier(ctx context.Context, supplier *models.Supplier, tenantID string) error {
	ctx = context.WithValue(ctx, ports.TransactionKey{}, s.tx)
	return s.baseStorage.SaveSupplier(ctx, supplier, tenantID)
}

// GetSupplier получает данные о поставщике
func (s *TransactionalStorage) GetSupplier(ctx context.Context, supplierID int, tenantID string) (*models.Supplier, error) {
	ctx = context.WithValue(ctx, ports.TransactionKey{}, s.tx)
	return s.baseStorage.GetSupplier(ctx, supplierID, tenantID)
}

// ListSuppliers возвращает список всех поставщиков
func (s *TransactionalStorage) ListSuppliers(ctx context.Context, tenantID string) ([]*models.Supplier, error) {
	ctx = context.WithValue(ctx, ports.TransactionKey{}, s.tx)
	return s.baseStorage.ListSuppliers(ctx, tenantID)
}

// BeginTx начинает новую транзакцию
func (s *TransactionalStorage) BeginTx(ctx context.Context) (context.Context, error) {
	// Используем уже существующую транзакцию, а не создаем новую
	return context.WithValue(ctx, ports.TransactionKey{}, s.tx), nil
}

// CommitTx фиксирует транзакцию
func (s *TransactionalStorage) CommitTx(ctx context.Context) error {
	// Фиксация будет выполнена менеджером транзакций
	return nil
}

// RollbackTx откатывает транзакцию
func (s *TransactionalStorage) RollbackTx(ctx context.Context) error {
	// Откат будет выполнен менеджером транзакций
	return nil
}

// Close закрывает соединение с хранилищем
func (s *TransactionalStorage) Close() error {
	// Не закрываем базовое хранилище, это ответственность владельца
	return nil
}
