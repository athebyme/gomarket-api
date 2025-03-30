package app

import (
	"context"
	"gomarketplace_api/internal/migrations/infrastructure"
	"gomarketplace_api/pkg/business/service/csv_to_postgres"
	"gomarketplace_api/pkg/dbconnect"
	"gomarketplace_api/pkg/dbconnect/migration"
	"log"
	"time"
)

type WholesalerServer struct {
	dbconnect.Database
}

func NewWServer(dbCon dbconnect.Database) *WholesalerServer {
	return &WholesalerServer{dbCon}
}

func (s *WholesalerServer) Run() error {
	var db, err = s.Connect()
	if err != nil {
		log.Printf("Error connecting to PostgreSQL: %s\n", err)
		return err
	}

	log.Printf("Preparing migrations now.")
	migrationApply := []migration.MigrationInterface{
		&infrastructure.WholesalerSchema{},
		&infrastructure.MigrationsSchema{},
		&infrastructure.Metadata{},
		&infrastructure.WholesalerProducts{},
		&infrastructure.WholesalerDescriptions{},
		&infrastructure.WholesalerPrice{},
		&infrastructure.WholesalerStock{},
		&infrastructure.WholesalerMedia{},
		&infrastructure.ProductSize{},
	}

	for _, _migration := range migrationApply {
		log.Printf("Applying migration: %T\n", _migration)
		if err := _migration.UpMigration(db); err != nil {
			log.Printf("Migration failed: %v", err)
			return err
		}
	}
	log.Println("Wholesaler migrations applied successfully!")

	// ------------------ products update ------------------
	fetcher := csv_to_postgres.NewHTTPFetcher()
	csvProc := csv_to_postgres.NewProcessor([]string{"global_id", "model", "appellation", "category",
		"brand", "country", "product_type", "features",
		"sex", "color", "dimension", "package", "empty",
		"media", "barcodes", "material", "package_battery"})

	postgresUpd := csv_to_postgres.NewPostgresUpdater(
		db,
		"wholesaler",
		"products",
		[]string{"global_id", "model", "appellation", "category",
			"brand", "country", "product_type", "features",
			"sex", "color", "dimension", "package", "empty",
			"media", "barcodes", "material", "package_battery"})

	csvUpdater := csv_to_postgres.NewUpdater(
		"http://sexoptovik.ru/files/all_prod_info.inf",
		"http://sexoptovik.ru/files/all_prod_info.csv",
		"last_update_products",
		fetcher,
		csvProc,
		postgresUpd)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*1)
	defer cancel()

	config := csv_to_postgres.UpdateConfig{
		ForeignKeys:      nil,
		OnConflictAction: "NOTHING",
		ConflictColumns:  []string{"global_id"},
	}

	if err := csvUpdater.Execute(ctx, nil, db, "", config); err != nil {
		log.Printf("Ошибка обновления: %v", err)
		return err
	}
	// ---------------------------------------------------

	// ------------------ prices update ------------------
	csvProc.SetNewColumnNaming([]string{"id товара", "цена"})
	postgresUpd.SetNewTableName("price").SetNewColumnNaming([]string{"global_id", "price"})
	csvUpdater.
		SetNewInfUrl("http://sexoptovik.ru/files/all_prod_prices.inf").
		SetNewCSVUrl("http://sexoptovik.ru/files/all_prod_prices.csv").
		SetNewLastModCol("last_update_prices")

	ctx, cancel = context.WithTimeout(context.Background(), time.Minute*1)
	defer cancel()

	config = csv_to_postgres.UpdateConfig{
		ForeignKeys: []csv_to_postgres.ForeignKey{
			{
				ReferenceTable:  "products",
				ReferenceColumn: "global_id",
				Columns:         []string{"global_id"},
			},
		},
		OnConflictAction: "UPDATE SET price = EXCLUDED.price",
		ConflictColumns:  []string{"global_id"},
	}
	if err := csvUpdater.Execute(ctx, []string{"global_id", "price"}, db, "", config); err != nil {
		log.Printf("Ошибка обновления: %v", err)
		return err
	}
	// ---------------------------------------------------

	// ------------------ stocks update ------------------
	csvProc.SetNewColumnNaming([]string{"id товара", "наличие"})
	postgresUpd.SetNewTableName("stocks").SetNewColumnNaming([]string{"global_id", "stocks"})

	csvUpdater.
		SetNewInfUrl("http://sexoptovik.ru/files/all_prod_prices__.inf").
		SetNewCSVUrl("http://sexoptovik.ru/files/all_prod_prices__.csv").
		SetNewLastModCol("last_update_stocks")

	ctx, cancel = context.WithTimeout(context.Background(), time.Minute*1)
	defer cancel()

	config = csv_to_postgres.UpdateConfig{
		ForeignKeys: []csv_to_postgres.ForeignKey{
			{
				ReferenceTable:  "products",
				ReferenceColumn: "global_id",
				Columns:         []string{"global_id"},
			},
		},
		OnConflictAction: "UPDATE SET stocks = EXCLUDED.stocks",
		ConflictColumns:  []string{"global_id"},
	}

	if err := csvUpdater.Execute(ctx, []string{"global_id", "stocks"}, db, "", config); err != nil {
		log.Printf("Ошибка обновления: %v", err)
		return err
	}
	// ---------------------------------------------------

	// ------------------ descriptions update ------------------
	csvProc.SetNewColumnNaming([]string{"global_id", "product_description"})
	postgresUpd.SetNewTableName("descriptions").SetNewColumnNaming([]string{"global_id", "product_description"})

	csvUpdater.
		SetNewInfUrl("http://sexoptovik.ru/files/all_prod_info.inf").
		SetNewCSVUrl("http://www.sexoptovik.ru/files/all_prod_d33_.csv").
		SetNewLastModCol("last_update_description")

	ctx, cancel = context.WithTimeout(context.Background(), time.Minute*1)
	defer cancel()

	config = csv_to_postgres.UpdateConfig{
		ForeignKeys: []csv_to_postgres.ForeignKey{
			{
				ReferenceTable:  "products",
				ReferenceColumn: "global_id",
				Columns:         []string{"global_id"},
			},
		},
		OnConflictAction: "NOTHING",
		ConflictColumns:  []string{"global_id"},
	}

	if err := csvUpdater.Execute(ctx, []string{"global_id", "product_description"}, db, "", config); err != nil {
		log.Printf("Ошибка обновления: %v", err)
		return err
	}

	return nil
	// ---------------------------------------------------

	// ------------------ обновления с инициализацией репо ------------------
	//mediaRepo := repositories.NewMediaRepository(db)
	//err = mediaRepo.Populate()
	//if err != nil {
	//	log.Fatalf("Error populating media table: %s\n", err)
	//}
	// ПОМЕНЯТЬ WRITER !
	//sizeRepo := repositories.NewSizeRepository(db, os.Stderr)
	//err = sizeRepo.Populate()
	//if err != nil {
	//	log.Fatalf("Error populating media table: %s\n", err)
	//}
}
