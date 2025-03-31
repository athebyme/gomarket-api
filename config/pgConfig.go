package config

import (
	"flag"
	"fmt"
)

type DatabaseConfig interface {
	GetConnectionString() string
}

type PostgresConfig struct {
	Host     string `yaml:"host" env:"POSTGRES_HOST"`
	Port     string `yaml:"port" env:"POSTGRES_PORT"`
	User     string `yaml:"username" env:"POSTGRES_USER"`
	Password string `yaml:"password" env:"POSTGRES_PASSWORD"`
	DBName   string `yaml:"db_name" env:"POSTGRES_NAME"`
}

func (pc *PostgresConfig) GetConnectionString() string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		pc.Host, pc.Port, pc.User, pc.Password, pc.DBName)
}

func GetPostgresConfig() *PostgresConfig {
	return &PostgresConfig{
		Host:     *flag.String("POSTGRES_HOST", "localhost", "Postgres host"),
		Port:     *flag.String("POSTGRES_PORT", "5432", "Postgres port"),
		User:     *flag.String("POSTGRES_USER", "postgres", "Postgres user"),
		Password: *flag.String("POSTGRES_PASSWORD", "postgres", "Postgres password"),
		DBName:   *flag.String("POSTGRES_NAME", "postgres", "Postgres database name"),
	}
}
