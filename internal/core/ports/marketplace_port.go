package ports

import (
	"context"
	"gomarketplace_api/internal/core/models"
)

// MarketplacePort defines the interface that must be implemented by all marketplace adapters.
// This is the boundary between the core system and external marketplace systems.
type MarketplacePort interface {
	// SyncProducts uploads or updates product information in the marketplace
	SyncProducts(ctx context.Context, products []*models.Product) error

	// UpdateInventory updates inventory levels in the marketplace
	UpdateInventory(ctx context.Context, inventoryMap map[string]int) error

	// UpdatePrices updates prices in the marketplace
	UpdatePrices(ctx context.Context, priceMap map[string]*models.ProductPrice) error

	// FetchMarketplaceProducts gets products currently in the marketplace
	FetchMarketplaceProducts(ctx context.Context) ([]*models.MarketplaceProduct, error)

	// UploadMedia uploads media files to the marketplace
	UploadMedia(ctx context.Context, productID string, media []*models.ProductMedia) error

	// GetMarketplaceID returns the unique identifier for this marketplace
	GetMarketplaceID() int

	// GetMarketplaceName returns the name of this marketplace
	GetMarketplaceName() string
}

// MarketplaceProduct represents a product that exists in a specific marketplace
type MarketplaceProduct struct {
	ID            string `json:"id"`
	MarketplaceID int    `json:"marketplace_id"`
	CoreProductID string `json:"core_product_id"`
	ExternalID    string `json:"external_id"` // ID in the marketplace system
	Status        string `json:"status"`      // "active", "pending", "rejected", etc.
	StatusMessage string `json:"status_message,omitempty"`
}
