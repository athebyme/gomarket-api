package infrastructure

import (
	"database/sql"
	"fmt"
	"log"
)

const (
	RegistrySchemaMigration     = "registry.schema"
	RegistryArticularAccounting = "registry.articular"
	RegistrySupplierMigration   = "registry.supplier"
)

type RegistrySchema struct{}

func (m *RegistrySchema) UpMigration(db *sql.DB) error {
	var migrationExists bool

	err := db.QueryRow("SELECT EXISTS (SELECT 1 FROM migrations.migrations WHERE name = $1)", RegistrySchemaMigration).Scan(&migrationExists)
	if err != nil {
		return fmt.Errorf("failed to check migration status: %w", err)
	}

	if migrationExists {
		log.Printf("Migration '%s' already completed. Skipping.", RegistrySchemaMigration)
		return nil
	}

	query :=
		`
        CREATE SCHEMA IF NOT EXISTS registry;
        `
	_, err = db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create %s table: %w", RegistrySchemaMigration, err)
	}

	_, err = db.Exec("INSERT INTO migrations.migrations (name, time) VALUES ($1, current_timestamp)", RegistrySchemaMigration)
	if err != nil {
		return fmt.Errorf("failed to mark '%s' migration as complete: %w", RegistrySchemaMigration, err)
	}

	log.Printf("Migration '%s' completed successfully.", RegistrySchemaMigration)
	return nil
}

type RegistryArticularAccountingTable struct{}

func (m *RegistryArticularAccountingTable) UpMigration(db *sql.DB) error {
	var migrationExists bool

	err := db.QueryRow("SELECT EXISTS (SELECT 1 FROM migrations.migrations WHERE name = $1)", RegistryArticularAccounting).Scan(&migrationExists)
	if err != nil {
		return fmt.Errorf("failed to check migration status: %w", err)
	}

	if migrationExists {
		log.Printf("Migration '%s' already completed. Skipping.", RegistryArticularAccounting)
		return nil
	}

	query := fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS registry.articular (
            id SERIAL PRIMARY KEY,
            supplier_id INTEGER NOT NULL,
            supplier_articul VARCHAR(100) NOT NULL,
            internal_articul VARCHAR(100) NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
            CONSTRAINT fk_supplier
                FOREIGN KEY(supplier_id)
                    REFERENCES registry.supplier(id)
                    ON DELETE CASCADE,
            CONSTRAINT unique_supplier_articul UNIQUE(supplier_id, supplier_articul),
            CONSTRAINT unique_internal_articul UNIQUE(internal_articul)
        );
    `)
	_, err = db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create %s table: %w", RegistryArticularAccounting, err)
	}

	_, err = db.Exec("INSERT INTO migrations.migrations (name, time) VALUES ($1, current_timestamp)", RegistryArticularAccounting)
	if err != nil {
		return fmt.Errorf("failed to mark '%s' migration as complete: %w", RegistryArticularAccounting, err)
	}

	log.Printf("Migration '%s' completed successfully.", RegistryArticularAccounting)
	return nil
}

type RegistrySupplierTable struct{}

func (m *RegistrySupplierTable) UpMigration(db *sql.DB) error {
	var migrationExists bool

	err := db.QueryRow("SELECT EXISTS (SELECT 1 FROM migrations.migrations WHERE name = $1)", RegistrySupplierMigration).Scan(&migrationExists)
	if err != nil {
		return fmt.Errorf("failed to check migration status for supplier: %w", err)
	}

	if migrationExists {
		log.Printf("Migration '%s' already completed. Skipping.", RegistrySupplierMigration)
		return nil
	}

	query := `
        CREATE TABLE IF NOT EXISTS registry.supplier (
            id SERIAL PRIMARY KEY,
            code VARCHAR(50) NOT NULL UNIQUE,
            name VARCHAR(255) NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
    `
	_, err = db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create supplier table: %w", err)
	}

	_, err = db.Exec("INSERT INTO migrations.migrations (name, time) VALUES ($1, current_timestamp)", RegistrySupplierMigration)
	if err != nil {
		return fmt.Errorf("failed to mark '%s' migration as complete: %w", RegistrySupplierMigration, err)
	}

	log.Printf("Migration '%s' completed successfully.", RegistrySupplierMigration)
	return nil
}
