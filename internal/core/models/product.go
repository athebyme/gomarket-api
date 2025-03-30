package models

import (
	"encoding/json"
	"time"
)

// Product represents a unified product entity aggregated from different suppliers.
// This structure is stored in the core.products table.
type Product struct {
	ID         string `json:"id"`
	SupplierID int    `json:"supplier_id"`
	// BaseData contains common product information in JSON format
	// (name, description, price, etc.)
	BaseData json.RawMessage `db:"base_data" json:"base_data"`
	// Metadata can store additional supplier-specific attributes
	Metadata  json.RawMessage `db:"metadata" json:"metadata,omitempty"`
	CreatedAt time.Time       `db:"created_at" json:"created_at"`
	UpdatedAt time.Time       `db:"updated_at" json:"updated_at"`
}

// ProductInventory represents current stock information for a product
type ProductInventory struct {
	ProductID  string    `json:"product_id"`
	SupplierID int       `json:"supplier_id"`
	Quantity   int       `json:"quantity"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// ProductPrice represents pricing information for a product
type ProductPrice struct {
	ProductID    string    `json:"product_id"`
	SupplierID   int       `json:"supplier_id"`
	BasePrice    float64   `json:"base_price"`
	SpecialPrice float64   `json:"special_price,omitempty"`
	Currency     string    `json:"currency"`
	StartDate    time.Time `json:"start_date,omitempty"`
	EndDate      time.Time `json:"end_date,omitempty"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// ProductMedia stores information about product images and videos
type ProductMedia struct {
	ID        string    `json:"id"`
	ProductID string    `json:"product_id"`
	Type      string    `json:"type"` // "image", "video", etc.
	URL       string    `json:"url"`
	Position  int       `json:"position"`
	CreatedAt time.Time `json:"created_at"`
}
