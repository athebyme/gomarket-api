package ports

import (
	"context"
	"gomarketplace_api/internal/core/models"
)

// SupplierPort defines the interface that must be implemented by all supplier adapters.
// This is the boundary between the core system and external supplier systems.
type SupplierPort interface {
	// SyncProducts retrieves all products from the supplier
	SyncProducts(ctx context.Context) ([]*models.Product, error)

	// FetchProductDetails gets detailed information for a specific product
	FetchProductDetails(ctx context.Context, coreProductID string) (*models.Product, error)

	// FetchInventory gets current inventory levels for the specified products
	FetchInventory(ctx context.Context, productIDs []string) (map[string]int, error)

	// FetchPrices gets current prices for the specified products
	FetchPrices(ctx context.Context, productIDs []string) (map[string]*models.ProductPrice, error)

	// FetchMedia gets media items (images, videos) for the specified products
	FetchMedia(ctx context.Context, productIDs []string) (map[string][]*models.ProductMedia, error)

	// GetSupplierID returns the unique identifier for this supplier
	GetSupplierID() int

	// GetSupplierName returns the name of this supplier
	GetSupplierName() string
}
