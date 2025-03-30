package an_msc

import (
	"context"
	"database/sql"
	dbMigrate "gomarketplace_api/internal/suppliers/an_msc/migration"
	"gomarketplace_api/pkg/business/service/csv_to_postgres"
	"gomarketplace_api/pkg/business/service/csv_to_postgres/converters"
	"gomarketplace_api/pkg/dbconnect"
	"gomarketplace_api/pkg/dbconnect/migration"
	"log"
	"strings"
	"time"
)

type AnManager struct {
	db *sql.DB
}

func NewAnManager(connectionStr dbconnect.Database) *AnManager {
	if connectionStr == nil {
		return nil
	}

	db, err := connectionStr.Connect()
	if err != nil {
		return nil
	}

	return &AnManager{db: db}
}

func (m *AnManager) Run() error {
	migrationApply := []migration.MigrationInterface{
		&dbMigrate.AnMscSchemaMigration{},
		&dbMigrate.AnMscProductsMigration{},
	}

	for _, _migration := range migrationApply {
		if err := _migration.UpMigration(m.db); err != nil {
			log.Printf("Migration failed: %v", err)
			return err
		}
	}
	log.Println("An msc migrations applied successfully!")

	columns := []string{"code", "article",
		"title", "group_code", "group_title", "category_code", "category_title", "tmn", "msk", "nsk", "start_price",
		"price", "discount", "image", "image1", "image2", "material", "size",
		"length", "width", "color", "weight", "battery", "waterproof", "country",
		"manufacturer", "barcode", "new", "hit", "description", "collection",
		"video", "url", "rst", "spb", "fixed_price", "pieces", "brand_code",
		"brand_title", "created", "three_d", "width_packed", "height_packed", "length_packed",
		"weight_packed", "modification_code", "images", "retail_price", "kdr", "category_new_code",
		"category_new_title", "embed3d", "minsk", "ast", "barcodes", "retail_price_minsk",
		"marked"}

	columnConverters := map[string]converters.ColumnConverter{
		// DECIMAL(10,2) columns
		"tmn":                converters.DecimalConverter,
		"nsk":                converters.DecimalConverter,
		"start_price":        converters.DecimalConverter,
		"price":              converters.DecimalConverter,
		"length":             converters.DecimalConverter,
		"width":              converters.DecimalConverter,
		"weight":             converters.DecimalConverter,
		"width_packed":       converters.DecimalConverter,
		"height_packed":      converters.DecimalConverter,
		"length_packed":      converters.DecimalConverter,
		"weight_packed":      converters.DecimalConverter,
		"retail_price":       converters.DecimalConverter,
		"retail_price_minsk": converters.DecimalConverter,

		// INT columns
		"msk":           converters.IntConverter,
		"waterproof":    converters.IntConverter,
		"new":           converters.IntConverter,
		"hit":           converters.IntConverter,
		"spb":           converters.IntConverter,
		"pieces":        converters.IntConverter,
		"marked":        converters.IntConverter,
		"category_code": converters.IntConverter,
		"brand_code":    converters.IntConverter,

		"3d": func(cell string) (interface{}, error) {
			if strings.TrimSpace(cell) == "" {
				return nil, nil
			}
			return cell, nil
		},

		"fixed_price": func(cell string) (interface{}, error) {
			cell = strings.TrimSpace(cell)
			if cell == "" || strings.EqualFold(cell, "NULL") {
				return nil, nil
			}
			return cell, nil
		},

		"created": func(cell string) (interface{}, error) {
			if strings.TrimSpace(cell) == "" {
				return nil, nil
			}
			return time.Parse("2006-01-02 15:04:05", cell)
		},
	}

	fetcher := csv_to_postgres.NewHTTPFetcher()
	csvProc := csv_to_postgres.NewProcessor(columns)
	csvProc.SetNewConverters(columnConverters)

	postgresUpd := csv_to_postgres.NewPostgresUpdater(
		m.db,
		"an_msc",
		"products",
		columns)

	csvUpdater := csv_to_postgres.NewUpdater(
		"http://www.sexoptovik.ru/mp/an_price_and_num_rrc.inf",
		"http://www.sexoptovik.ru/mp/an_price_and_num_rrc.csv",
		"an_msc.products",
		fetcher,
		csvProc,
		postgresUpd)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*2)
	defer cancel()

	config := csv_to_postgres.UpdateConfig{
		ForeignKeys:      nil,
		OnConflictAction: "NOTHING",
		ConflictColumns:  []string{"code"},
	}

	if err := csvUpdater.Execute(ctx, []string{"code", "article",
		"title", "group_code", "group_title", "tmn", "msk", "nsk", "start_price",
		"price", "discount", "image", "image1", "image2", "material", "size",
		"length", "width", "color", "weight", "battery", "waterproof", "country",
		"manufacturer", "barcode", "new", "hit", "description", "collection",
		"video", "url", "rst", "spb", "fixed_price", "pieces", "brand_code",
		"brand_title", "created", "three_d", "width_packed", "height_packed", "length_packed",
		"weight_packed", "modification_code", "images", "retail_price", "kdr", "category_new_code",
		"category_new_title", "embed3d", "minsk", "ast", "barcodes", "retail_price_minsk",
		"marked"}, m.db, "02.01.06 15:04:05", config); err != nil {
		log.Fatalf("Ошибка обновления: %v", err)
	}

	return nil
}
