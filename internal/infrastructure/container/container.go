package container

import (
	"gomarketplace_api/internal/core/ports"
	"gomarketplace_api/internal/core/services"
	"sync"
	"time"
)

var (
	suppliersMutex sync.RWMutex
	suppliers      map[int]ports.SupplierPort
	marketplaces   map[int]ports.MarketplacePort

	productService *services.ProductService
	syncService    *services.SyncService

	// Порты инфраструктуры
	storagePort     ports.StoragePort
	cachePort       ports.CachePort
	messagingPort   ports.MessagingPort
	loggerPort      ports.LoggerPort
	transactionPort ports.TransactionPort
)

func init() {
	suppliers = make(map[int]ports.SupplierPort)
	marketplaces = make(map[int]ports.MarketplacePort)
}

// RegisterSupplier регистрирует поставщика в контейнере
func RegisterSupplier(supplier ports.SupplierPort) {
	suppliersMutex.Lock()
	defer suppliersMutex.Unlock()
	suppliers[supplier.GetSupplierID()] = supplier
}

// RegisterMarketplace регистрирует маркетплейс в контейнере
func RegisterMarketplace(marketplace ports.MarketplacePort) {
	marketplaces[marketplace.GetMarketplaceID()] = marketplace
}

// GetProductService возвращает сервис продуктов
func GetProductService() *services.ProductService {
	if productService == nil {
		productService = services.NewProductService(
			suppliers,
			marketplaces,
			GetTransactionPort(),
			GetLoggerPort(),
		)
	}
	return productService
}

// GetSyncService возвращает сервис синхронизации
func GetSyncService() *services.SyncService {
	if syncService == nil {
		syncService = services.NewSyncService(
			GetProductService(),
			GetTransactionPort(),
			GetLoggerPort(),
			1*time.Hour, // Интервал синхронизации
			"default",   // TenantID
		)
	}
	return syncService
}

// GetTransactionPort возвращает порт транзакций
func GetTransactionPort() ports.TransactionPort {
	return transactionPort
}

// GetLoggerPort возвращает порт логирования
func GetLoggerPort() ports.LoggerPort {
	return loggerPort
}

// GetStoragePort возвращает порт хранилища
func GetStoragePort() ports.StoragePort {
	return storagePort
}

// GetCachePort возвращает порт кэша
func GetCachePort() ports.CachePort {
	return cachePort
}

// GetMessagingPort возвращает порт сообщений
func GetMessagingPort() ports.MessagingPort {
	return messagingPort
}

// Устанавливающие методы для инфраструктурных портов

func SetTransactionPort(port ports.TransactionPort) {
	transactionPort = port
}

func SetLoggerPort(port ports.LoggerPort) {
	loggerPort = port
}

func SetStoragePort(port ports.StoragePort) {
	storagePort = port
}

func SetCachePort(port ports.CachePort) {
	cachePort = port
}

func SetMessagingPort(port ports.MessagingPort) {
	messagingPort = port
}
