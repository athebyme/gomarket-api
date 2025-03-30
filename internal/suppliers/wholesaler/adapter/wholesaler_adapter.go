package adapter

import (
	"context"
	"encoding/json"
	"fmt"
	"gomarketplace_api/internal/core/models"
	"gomarketplace_api/internal/core/ports"
	"gomarketplace_api/internal/suppliers/wholesaler/pkg/requests"
	"gomarketplace_api/internal/wildberries/pkg/clients"
	"strconv"
	"time"
)

type WholesalerAdapter struct {
	wsClient     *clients.WServiceClient
	supplierID   int
	supplierName string
	logger       ports.LoggerPort
}

func NewWholesalerAdapter(wsClientURL string, supplierID int, logger ports.LoggerPort) (*WholesalerAdapter, error) {
	client, err := clients.NewWServiceClient(wsClientURL, logger)
	if err != nil {
		return nil, err
	}

	return &WholesalerAdapter{
		wsClient:     client,
		supplierID:   supplierID,
		supplierName: "wholesaler",
		logger:       logger,
	}, nil
}

// SyncProducts получает все товары от поставщика Wholesaler
func (a *WholesalerAdapter) SyncProducts(ctx context.Context) ([]*models.Product, error) {
	// Получаем глобальные ID товаров
	idsResult, err := a.wsClient.FetcherChain.Fetch(ctx, "globalids", nil)
	if err != nil {
		return nil, fmt.Errorf("ошибка получения ID товаров: %w", err)
	}

	globalIDs, ok := idsResult.([]int)
	if !ok {
		return nil, fmt.Errorf("неверный формат данных ID товаров")
	}

	a.logger.Info("Получено товаров от поставщика",
		"supplierName", a.supplierName,
		"count", len(globalIDs))

	// Собираем все необходимые данные о товарах
	appData, err := a.fetchAllAppellations(ctx)
	if err != nil {
		return nil, err
	}

	descData, err := a.fetchAllDescriptions(ctx)
	if err != nil {
		return nil, err
	}

	brandData, err := a.fetchAllBrands(ctx)
	if err != nil {
		return nil, err
	}

	// Создаем модели товаров
	products := make([]*models.Product, 0, len(globalIDs))
	for _, id := range globalIDs {
		// Получаем данные для текущего товара
		appellation, _ := appData[id].(string)
		description, _ := descData[id].(string)
		brand, _ := brandData[id].(string)

		if appellation == "" {
			a.logger.Warn("Пропуск товара без наименования", "globalID", id)
			continue
		}

		// Базовые данные товара в JSON
		baseData, _ := json.Marshal(map[string]interface{}{
			"name":        appellation,
			"description": description,
			"brand":       brand,
		})

		// Создаем модель товара
		product := &models.Product{
			ID:         strconv.Itoa(id),
			SupplierID: a.supplierID,
			BaseData:   baseData,
			CreatedAt:  time.Now(),
			UpdatedAt:  time.Now(),
		}

		products = append(products, product)
	}

	return products, nil
}

// fetchAllAppellations получает все наименования товаров
func (a *WholesalerAdapter) fetchAllAppellations(ctx context.Context) (map[int]interface{}, error) {
	result, err := a.wsClient.FetcherChain.Fetch(ctx, "appellations", requests.AppellationsRequest{
		FilterRequest: requests.FilterRequest{ProductIDs: []int{}},
	})
	if err != nil {
		return nil, fmt.Errorf("ошибка получения наименований: %w", err)
	}

	appellations, ok := result.(map[int]interface{})
	if !ok {
		return nil, fmt.Errorf("неверный формат данных наименований")
	}

	return appellations, nil
}

// fetchAllDescriptions получает все описания товаров
func (a *WholesalerAdapter) fetchAllDescriptions(ctx context.Context) (map[int]interface{}, error) {
	result, err := a.wsClient.FetcherChain.Fetch(ctx, "descriptions", requests.DescriptionRequest{
		FilterRequest:            requests.FilterRequest{ProductIDs: []int{}},
		IncludeEmptyDescriptions: false,
	})
	if err != nil {
		return nil, fmt.Errorf("ошибка получения описаний: %w", err)
	}

	descriptions, ok := result.(map[int]interface{})
	if !ok {
		return nil, fmt.Errorf("неверный формат данных описаний")
	}

	return descriptions, nil
}

// fetchAllBrands получает все бренды товаров
func (a *WholesalerAdapter) fetchAllBrands(ctx context.Context) (map[int]interface{}, error) {
	result, err := a.wsClient.FetcherChain.Fetch(ctx, "brands", requests.BrandRequest{
		FilterRequest: requests.FilterRequest{ProductIDs: []int{}},
	})
	if err != nil {
		return nil, fmt.Errorf("ошибка получения брендов: %w", err)
	}

	brands, ok := result.(map[int]interface{})
	if !ok {
		return nil, fmt.Errorf("неверный формат данных брендов")
	}

	return brands, nil
}

// Остальные методы интерфейса SupplierPort
func (a *WholesalerAdapter) FetchProductDetails(ctx context.Context, productID string) (*models.Product, error) {
	// Реализация получения деталей товара
	// ...
	return nil, nil
}

func (a *WholesalerAdapter) FetchInventory(ctx context.Context, productIDs []string) (map[string]int, error) {
	// Реализация получения запасов
	// ...
	return nil, nil
}

func (a *WholesalerAdapter) FetchPrices(ctx context.Context, productIDs []string) (map[string]*models.ProductPrice, error) {
	// Реализация получения цен
	// ...
	return nil, nil
}

func (a *WholesalerAdapter) FetchMedia(ctx context.Context, productIDs []string) (map[string][]*models.ProductMedia, error) {
	// Реализация получения медиа
	// ...
	return nil, nil
}

func (a *WholesalerAdapter) GetSupplierID() int {
	return a.supplierID
}

func (a *WholesalerAdapter) GetSupplierName() string {
	return a.supplierName
}
