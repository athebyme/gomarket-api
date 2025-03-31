package config

import (
	"gomarketplace_api/config/values"
	"gopkg.in/yaml.v3"
	"os"
	"strconv"
)

type Config interface {
}

type MarketplaceConfig interface {
}

type WildberriesConfig struct {
	ApiKey     string                         `yaml:"api_key" env:"WB_API_KEY"`
	WbValues   values.WildberriesValues       `yaml:"default_values"`
	WbBanned   values.WildberriesBannedBrands `yaml:"brands"`
	WbIdentity values.Identity                `yaml:"identity"`
}

type AppConfig struct {
	Wildberries *WildberriesConfig `yaml:"wildberries"`
	Postgres    *PostgresConfig    `yaml:"postgres"`
}

// LoadConfig загружает конфигурацию из YAML-файла и переопределяет значения из переменных среды
func (c *AppConfig) LoadConfig(filename string) (*AppConfig, error) {
	// Загрузка из файла
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	decoder := yaml.NewDecoder(file)
	config := &AppConfig{}
	if err := decoder.Decode(config); err != nil {
		return nil, err
	}

	config.overrideFromEnv()

	return config, nil
}

// overrideFromEnv переопределяет значения конфигурации из переменных среды
func (c *AppConfig) overrideFromEnv() {
	// Wildberries
	if apiKey := os.Getenv("WB_API_KEY"); apiKey != "" {
		c.Wildberries.ApiKey = apiKey
	}

	// Postgres
	if host := os.Getenv("POSTGRES_HOST"); host != "" {
		c.Postgres.Host = host
	}
	if port := os.Getenv("POSTGRES_PORT"); port != "" {
		c.Postgres.Port = port
	}
	if user := os.Getenv("POSTGRES_USER"); user != "" {
		c.Postgres.User = user
	}
	if password := os.Getenv("POSTGRES_PASSWORD"); password != "" {
		c.Postgres.Password = password
	}
	if dbName := os.Getenv("POSTGRES_NAME"); dbName != "" {
		c.Postgres.DBName = dbName
	}

	// Переопределение значений пакета
	if height := os.Getenv("WB_PACKAGE_HEIGHT"); height != "" {
		if val, err := strconv.Atoi(height); err == nil {
			c.Wildberries.WbValues.PackageHeight = val
		}
	}
	if width := os.Getenv("WB_PACKAGE_WIDTH"); width != "" {
		if val, err := strconv.Atoi(width); err == nil {
			c.Wildberries.WbValues.PackageWidth = val
		}
	}
	if length := os.Getenv("WB_PACKAGE_LENGTH"); length != "" {
		if val, err := strconv.Atoi(length); err == nil {
			c.Wildberries.WbValues.PackageLength = val
		}
	}

	// Переопределение кода идентификации
	if code := os.Getenv("WB_IDENTITY_CODE"); code != "" {
		if val, err := strconv.Atoi(code); err == nil {
			c.Wildberries.WbIdentity.Code = val
		}
	}
}
