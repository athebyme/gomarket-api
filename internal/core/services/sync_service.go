package services

import (
	"context"
	"gomarketplace_api/internal/core/ports"
	"time"
)

// SyncService реализует сервис синхронизации
type SyncService struct {
	productService ProdService
	txPort         ports.TransactionPort
	logger         ports.LoggerPort
	interval       time.Duration
	tenantID       string
}

// NewSyncService создает новый экземпляр SyncService
func NewSyncService(
	productService ProdService,
	txPort ports.TransactionPort,
	logger ports.LoggerPort,
	interval time.Duration,
	tenantID string,
) *SyncService {
	return &SyncService{
		productService: productService,
		txPort:         txPort,
		logger:         logger,
		interval:       interval,
		tenantID:       tenantID,
	}
}

// StartPeriodicSync запускает периодическую синхронизацию
func (s *SyncService) StartPeriodicSync(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	// Первая синхронизация
	s.SyncAllSuppliers(ctx)

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("Остановка периодической синхронизации")
			return
		case <-ticker.C:
			s.SyncAllSuppliers(ctx)
		}
	}
}

// SyncAllSuppliers синхронизирует все поставщики
func (s *SyncService) SyncAllSuppliers(ctx context.Context) map[int]int {
	supplierIDs := []int{1} // ID поставщика Wholesaler
	results := make(map[int]int)

	for _, supplierID := range supplierIDs {
		count, err := s.SyncSupplier(ctx, supplierID)
		if err != nil {
			s.logger.Error("Ошибка синхронизации поставщика",
				"supplierID", supplierID,
				"error", err)
			results[supplierID] = -1
		} else {
			results[supplierID] = count
		}
	}

	return results
}

// SyncSupplier синхронизирует конкретного поставщика
func (s *SyncService) SyncSupplier(ctx context.Context, supplierID int) (int, error) {
	result, err := s.txPort.ExecuteInTransactionWithTenant(ctx, func(ctx context.Context, tx ports.TransactionalPorts) (interface{}, error) {
		return s.productService.SyncProductsFromSupplier(ctx, supplierID, s.tenantID)
	}, s.tenantID)

	if err != nil {
		return 0, err
	}

	return result.(int), nil
}
