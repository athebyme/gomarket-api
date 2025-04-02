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

// ProductFilter представляет структурированную модель для фильтрации продуктов
type ProductFilter struct {
	// Основные поля фильтрации
	ID           string   `json:"id,omitempty"`
	SupplierID   int      `json:"supplier_id,omitempty"`
	Name         string   `json:"name,omitempty"`
	Description  string   `json:"description,omitempty"`
	CategoryID   string   `json:"category_id,omitempty"`
	CategoryIDs  []string `json:"category_ids,omitempty"`

	// Фильтрация по цене
	MinPrice     float64  `json:"min_price,omitempty"`
	MaxPrice     float64  `json:"max_price,omitempty"`

	// Фильтрация по инвентарю
	InStock      *bool    `json:"in_stock,omitempty"`
	MinStock     int      `json:"min_stock,omitempty"`

	// Фильтрация по статусу
	Status       string   `json:"status,omitempty"`
	Statuses     []string `json:"statuses,omitempty"`

	// Фильтрация по времени
	CreatedAfter  int64   `json:"created_after,omitempty"`  // Unix timestamp
	CreatedBefore int64   `json:"created_before,omitempty"` // Unix timestamp
	UpdatedAfter  int64   `json:"updated_after,omitempty"`  // Unix timestamp
	UpdatedBefore int64   `json:"updated_before,omitempty"` // Unix timestamp

	// Фильтрация по маркетплейсам
	MarketplaceID int     `json:"marketplace_id,omitempty"`

	// Полнотекстовый поиск
	SearchQuery   string  `json:"search_query,omitempty"`

	// Произвольные атрибуты для фильтрации
	Attributes    map[string]interface{} `json:"attributes,omitempty"`

	// Преобразует фильтр в map для использования в запросах
	ToMap() map[string]interface{}
}

// ToMap преобразует ProductFilter в map для использования в запросах
func (f *ProductFilter) ToMap() map[string]interface{} {
	result := make(map[string]interface{})

	if f.ID != "" {
		result["id"] = f.ID
	}

	if f.SupplierID != 0 {
		result["supplier_id"] = f.SupplierID
	}

	if f.Name != "" {
		result["name"] = f.Name
	}

	if f.Description != "" {
		result["description"] = f.Description
	}

	if f.CategoryID != "" {
		result["category_id"] = f.CategoryID
	}

	if len(f.CategoryIDs) > 0 {
		result["category_ids"] = f.CategoryIDs
	}

	if f.MinPrice > 0 {
		result["min_price"] = f.MinPrice
	}

	if f.MaxPrice > 0 {
		result["max_price"] = f.MaxPrice
	}

	if f.InStock != nil {
		result["in_stock"] = *f.InStock
	}

	if f.MinStock > 0 {
		result["min_stock"] = f.MinStock
	}

	if f.Status != "" {
		result["status"] = f.Status
	}

	if len(f.Statuses) > 0 {
		result["statuses"] = f.Statuses
	}

	if f.CreatedAfter > 0 {
		result["created_after"] = f.CreatedAfter
	}

	if f.CreatedBefore > 0 {
		result["created_before"] = f.CreatedBefore
	}

	if f.UpdatedAfter > 0 {
		result["updated_after"] = f.UpdatedAfter
	}

	if f.UpdatedBefore > 0 {
		result["updated_before"] = f.UpdatedBefore
	}

	if f.MarketplaceID > 0 {
		result["marketplace_id"] = f.MarketplaceID
	}

	if f.SearchQuery != "" {
		result["search_query"] = f.SearchQuery
	}

	if f.Attributes != nil && len(f.Attributes) > 0 {
		for key, value := range f.Attributes {
			result["attr_"+key] = value
		}
	}

	return result
}

// Pagination представляет расширенную модель для пагинации
type Pagination struct {
	Page       int    `json:"page"`       // Номер страницы (начиная с 1)
	PageSize   int    `json:"page_size"`  // Размер страницы
	TotalItems int64  `json:"total_items"` // Общее количество элементов
	TotalPages int    `json:"total_pages"` // Общее количество страниц
	SortBy     string `json:"sort_by"`     // Поле для сортировки
	SortDesc   bool   `json:"sort_desc"`   // Сортировка по убыванию
	HasNext    bool   `json:"has_next"`    // Есть ли следующая страница
	HasPrev    bool   `json:"has_prev"`    // Есть ли предыдущая страница
}

// NewPagination создает новый экземпляр Pagination с заданными параметрами
func NewPagination(page, pageSize int, sortBy string, sortDesc bool) *Pagination {
	if page < 1 {
		page = 1
	}

	if pageSize < 1 {
		pageSize = 10
	}

	return &Pagination{
		Page:       page,
		PageSize:   pageSize,
		SortBy:     sortBy,
		SortDesc:   sortDesc,
		TotalItems: 0,
		TotalPages: 0,
		HasNext:    false,
		HasPrev:    false,
	}
}


// SetTotal устанавливает общее количество элементов и пересчитывает зависимые поля
func (p *Pagination) SetTotal(totalItems int64) {
	p.TotalItems = totalItems
	p.TotalPages = int((totalItems + int64(p.PageSize) - 1) / int64(p.PageSize))
	p.HasNext = p.Page < p.TotalPages
	p.HasPrev = p.Page > 1
}

// GetOffset возвращает смещение для SQL запроса
func (p *Pagination) GetOffset() int {
	return (p.Page - 1) * p.PageSize
}

// GetLimit возвращает лимит для SQL запроса
func (p *Pagination) GetLimit() int {
	return p.PageSize
}

// GetSortOrder возвращает строку порядка сортировки для SQL запроса
func (p *Pagination) GetSortOrder() string {
	if p.SortBy == "" {
		return "created_at DESC"
	}

	direction := "ASC"
	if p.SortDesc {
		direction = "DESC"
	}

	return p.SortBy + " " + direction
}


// PagedResult представляет результат запроса с пагинацией
type PagedResult struct {
	Items      interface{} `json:"items"`      // Элементы текущей страницы
	Pagination *Pagination `json:"pagination"` // Информация о пагинации
}

// NewPagedResult создает новый результат с пагинацией
func NewPagedResult(items interface{}, pagination *Pagination) *PagedResult {
	return &PagedResult{
		Items:      items,
		Pagination: pagination,
	}
}

// ProductCategory представляет категорию продуктов
type ProductCategory struct {
	ID           string   `json:"id"`
	Name         string   `json:"name"`
	Description  string   `json:"description,omitempty"`
	ParentID     string   `json:"parent_id,omitempty"`
	Level        int      `json:"level"`
	Path         string   `json:"path"`
	ImageURL     string   `json:"image_url,omitempty"`
	SubCategories []string `json:"sub_categories,omitempty"`
}

// ProductHistoryRecord представляет запись в истории изменений продукта
type ProductHistoryRecord struct {
	ID             string          `json:"id"`
	ProductID      string          `json:"product_id"`
	ChangeType     string          `json:"change_type"` // "create", "update", "delete"
	Before         *Product `json:"before,omitempty"`
	After          *Product `json:"after,omitempty"`
	ChangedBy      string          `json:"changed_by,omitempty"`
	ChangedAt      int64           `json:"changed_at"`
	ChangeComment  string          `json:"change_comment,omitempty"`
}