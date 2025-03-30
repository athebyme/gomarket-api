package wb

import (
	"database/sql"
	"fmt"
	"log"
)

type CreateWBProductsTable struct{}

func (m *CreateWBProductsTable) UpMigration(db *sql.DB) error {
	if ok, err := checkAndSkipMigration(db, "wildberries.products"); err != nil {
		return err
	} else if ok {
		return nil
	}
	query := `
	CREATE TABLE IF NOT EXISTS wildberries.products (
		global_id INT PRIMARY KEY,
		category_id INT NOT NULL,
		distance FLOAT NOT NULL,
	    FOREIGN KEY (category_id) REFERENCES wildberries.categories(category_id)
	);`
	if err := executeAndMarkMigration(db, query, "wildberries.products"); err != nil {
		return err
	}
	log.Println("Migration 'wildberries.products' completed successfully.")
	return nil
}

type CreateWBCategoriesTable struct{}

func (m *CreateWBCategoriesTable) UpMigration(db *sql.DB) error {
	if ok, err := checkAndSkipMigration(db, "wildberries.categories"); err != nil {
		return err
	} else if ok {
		return nil
	}
	query := `
	CREATE TABLE IF NOT EXISTS wildberries.categories (
		category_id INT PRIMARY KEY,
		category VARCHAR(255) NOT NULL,
		parent_category_id INT,
		parent_category_name VARCHAR(255) NOT NULL
	);`
	if err := executeAndMarkMigration(db, query, "wildberries.categories"); err != nil {
		return err
	}
	log.Println("Migration 'wildberries.categories' completed successfully.")
	return nil
}

type CreateWBSchema struct{}

func (m *CreateWBSchema) UpMigration(db *sql.DB) error {
	query := `
	CREATE SCHEMA IF NOT EXISTS wildberries;`
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create schema wildberries: %w", err)
	}
	return nil
}

/*
Вопрос: нужно ли это хранить в бд ?
*/
type WBNomenclatures struct{}

func (m *WBNomenclatures) UpMigration(db *sql.DB) error {
	if ok, err := checkAndSkipMigration(db, "wildberries.nomenclatures"); err != nil {
		return err
	} else if ok {
		return nil
	}

	query := `
		CREATE TABLE IF NOT EXISTS wildberries.nomenclatures (
		    nomenclature_id SERIAL PRIMARY KEY,
			global_id INT UNIQUE ,
			nm_id INT UNIQUE,	
			imt_id INT UNIQUE,
            nm_uuid UUID UNIQUE,
            vendor_code VARCHAR(255) UNIQUE,
			subject_id INT,
		    wb_brand VARCHAR(255),
			created_at TIMESTAMP WITH TIME ZONE,
			updated_at TIMESTAMP WITH TIME ZONE,
			FOREIGN KEY(global_id) REFERENCES wholesaler.products(global_id)
		);
	`
	if err := executeAndMarkMigration(db, query, "wildberries.nomenclatures"); err != nil {
		return err
	}
	log.Println("Migration 'wildberries.nomenclatures' completed successfully.")
	return nil
}

type WBCardsActual struct{}

func (m *WBCardsActual) UpMigration(db *sql.DB) error {
	if ok, err := checkAndSkipMigration(db, "wildberries.cards_actual"); err != nil {
		return err
	} else if ok {
		return nil
	}
	query := `CREATE TABLE IF NOT EXISTS wildberries.cards_actual(
    			version_id SERIAL PRIMARY KEY,
				global_id INT,
				nm_id INT NOT NULL, -- ID номенклатуры
				vendor_code VARCHAR(255),
				version INT NOT NULL, -- Версия карточки
				version_data JSONB, -- Полный JSON объект карточки
				created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
				updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
				FOREIGN KEY(global_id) REFERENCES wildberries.nomenclatures(global_id),
    			FOREIGN KEY (nm_id) REFERENCES wildberries.nomenclatures(nm_id),
    			FOREIGN KEY (vendor_code) REFERENCES wildberries.nomenclatures(vendor_code)
			)`

	if err := executeAndMarkMigration(db, query, "wildberries.cards_actual"); err != nil {
		return err
	}
	log.Println("Migration 'wildberries.cards_actual' completed successfully.")
	return nil
}

type WBNomenclaturesHistory struct{}

func (m *WBNomenclaturesHistory) UpMigration(db *sql.DB) error {
	if ok, err := checkAndSkipMigration(db, "wildberries.nomenclatures_history"); err != nil {
		return err
	} else if ok {
		return nil
	}
	query := `CREATE TABLE IF NOT EXISTS wildberries.nomenclatures_history(
    			version_id SERIAL PRIMARY KEY,
				global_id INT,
				nm_id INT NOT NULL, -- ID номенклатуры
				vendor_code VARCHAR(255),
				version INT NOT NULL, -- Версия карточки
				version_data JSONB, -- Полный JSON объект карточки
				created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
				updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
				FOREIGN KEY(global_id) REFERENCES wildberries.nomenclatures(global_id),
				FOREIGN KEY (nm_id) REFERENCES wildberries.nomenclatures(nm_id),
    			FOREIGN KEY (vendor_Code) REFERENCES wildberries.nomenclatures(vendor_code)
			)`

	if err := executeAndMarkMigration(db, query, "wildberries.nomenclatures_history"); err != nil {
		return err
	}
	log.Println("Migration 'wildberries.nomenclatures_history' completed successfully.")
	return nil
}

type WBChanges struct{}

func (m *WBChanges) UpMigration(db *sql.DB) error {
	if ok, err := checkAndSkipMigration(db, "wildberries.changes"); err != nil {
		return err
	} else if ok {
		return nil
	}

	query := `
        CREATE TABLE IF NOT EXISTS wildberries.changes (
            change_id SERIAL PRIMARY KEY,            -- Уникальный идентификатор изменения
            nm_id INT NOT NULL,                      -- ID номенклатуры, к которой относится изменение
            vendor_code VARCHAR(255) NOT NULL,
            version INT NOT NULL,                    -- Версия карточки на момент изменения
            changed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),  -- Время изменения
            description TEXT,                         -- Описание изменения (например, "Обновление данных", "Откат версии" и т.д.)
			FOREIGN KEY (nm_id) REFERENCES wildberries.nomenclatures(nm_id),
			FOREIGN KEY (vendor_Code) REFERENCES wildberries.nomenclatures(vendor_code)
        )
    `

	if err := executeAndMarkMigration(db, query, "wildberries.changes"); err != nil {
		return err
	}
	log.Println("Migration 'wildberries.changes' completed successfully.")
	return nil
}

type WBCardsHistory struct{}

func (m *WBCardsHistory) UpMigration(db *sql.DB) error {
	if ok, err := checkAndSkipMigration(db, "wildberries.cards_history"); err != nil {
		return err
	} else if ok {
		return nil
	}

	query := `
		CREATE TABLE IF NOT EXISTS wildberries.cards_history (
		version_id SERIAL PRIMARY KEY,
		global_id INT,
		nm_id INT NOT NULL, -- ID номенклатуры
		vendor_code VARCHAR(255),
		version INT NOT NULL, -- Версия карточки
		version_data JSONB, -- Полный JSON объект карточки
		created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
		updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
		FOREIGN KEY(global_id) REFERENCES wildberries.nomenclatures(global_id)
);

		);
	`
	if err := executeAndMarkMigration(db, query, "wildberries.cards_history"); err != nil {
		return err
	}
	log.Println("Migration 'wildberries.cards_history' completed successfully.")
	return nil
}

type WBCharacteristics struct{}

func (m *WBCharacteristics) UpMigration(db *sql.DB) error {
	if ok, err := checkAndSkipMigration(db, "wildberries.characteristics"); err != nil {
		return err
	} else if ok {
		return nil
	}

	query := `
		CREATE TABLE IF NOT EXISTS wildberries.characteristics (
			id SERIAL PRIMARY KEY,
			charc_id INT UNIQUE,
		    name TEXT,
			required BOOLEAN,
			subject_id INT,
		    unit_name VARCHAR(15),
		    max_count INT,
			popular BOOLEAN,
		    charc_type INT,
			FOREIGN KEY (subject_id) REFERENCES wildberries.categories(category_id)
		);
	`

	if err := executeAndMarkMigration(db, query, "wildberries.characteristics"); err != nil {
		return err
	}
	log.Println("Migration 'wildberries.characteristics' completed successfully.")
	return nil
}

func checkAndSkipMigration(db *sql.DB, migrationName string) (bool, error) {
	var migrationExists bool
	err := db.QueryRow("SELECT EXISTS (SELECT 1 FROM migrations.migrations WHERE name = $1)", migrationName).Scan(&migrationExists)
	if err != nil {
		return migrationExists, fmt.Errorf("failed to check migration status: %w", err)
	}
	if migrationExists {
		log.Printf("Migration '%s' already completed. Skipping.\n", migrationName)
		return migrationExists, nil
	}
	return migrationExists, nil
}

func executeAndMarkMigration(db *sql.DB, query string, migrationName string) error {
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to execute migration '%s': %w", migrationName, err)
	}
	_, err = db.Exec("INSERT INTO migrations.migrations (name, time) VALUES ($1, current_timestamp)", migrationName)
	if err != nil {
		return fmt.Errorf("failed to mark migration '%s' as complete: %w", migrationName, err)
	}
	return nil
}
