package infrastructure

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	"gomarketplace_api/internal/auth/config"
	"gomarketplace_api/internal/core/ports"
	redis_cache "gomarketplace_api/internal/infrastructure/cache/redis"
	zap_logger "gomarketplace_api/internal/infrastructure/logger/zap"
	"gomarketplace_api/internal/infrastructure/messaging/kafka"
	"gomarketplace_api/internal/infrastructure/repositories/gorm"
	"gomarketplace_api/internal/infrastructure/transactions"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// InfrastructureConfig содержит настройки для инициализации инфраструктуры
type InfrastructureConfig struct {
	// Настройки базы данных
	DBHost            string
	DBPort            string
	DBUser            string
	DBPassword        string
	DBName            string
	DBSSLMode         string
	DBMaxConnections  int
	DBIdleConnections int

	// Настройки Redis
	RedisHost     string
	RedisPort     string
	RedisPassword string
	RedisDB       int

	// Настройки Kafka
	KafkaBrokers []string

	// Настройки JWT
	JWTConfig *config.JwtConfig

	// Настройки логирования
	LogLevel ports.LogLevel
	LogJSON  bool
}

// Инициализатор инфраструктуры
type InfrastructureInitializer struct {
	config InfrastructureConfig
	logger ports.LoggerPort
}

// NewInfrastructureInitializer создает экземпляр инициализатора
func NewInfrastructureInitializer(config InfrastructureConfig) (*InfrastructureInitializer, error) {
	// Создаем базовый логгер
	var loggerPort ports.LoggerPort
	var err error

	if config.LogJSON {
		loggerPort, err = zap_logger.NewZapLoggerPort(config.LogLevel)
	} else {
		loggerPort, err = zap_logger.NewZapLoggerPortDev(config.LogLevel)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create logger: %w", err)
	}

	return &InfrastructureInitializer{
		config: config,
		logger: loggerPort,
	}, nil
}

// LoadFromEnv загружает конфигурацию из переменных окружения
func LoadConfigFromEnv() (InfrastructureConfig, error) {
	dbMaxConnections, err := strconv.Atoi(getEnvOrDefault("DB_MAX_CONNECTIONS", "20"))
	if err != nil {
		dbMaxConnections = 20
	}

	dbIdleConnections, err := strconv.Atoi(getEnvOrDefault("DB_IDLE_CONNECTIONS", "10"))
	if err != nil {
		dbIdleConnections = 10
	}

	redisDB, err := strconv.Atoi(getEnvOrDefault("REDIS_DB", "0"))
	if err != nil {
		redisDB = 0
	}

	logLevel, err := strconv.Atoi(getEnvOrDefault("LOG_LEVEL", "1"))
	if err != nil {
		logLevel = 1 // Info по умолчанию
	}

	logJSON := getEnvOrDefault("LOG_JSON", "false") == "true"

	config := InfrastructureConfig{
		// Настройки базы данных
		DBHost:            getEnvOrDefault("DB_HOST", "localhost"),
		DBPort:            getEnvOrDefault("DB_PORT", "5432"),
		DBUser:            getEnvOrDefault("DB_USER", "postgres"),
		DBPassword:        getEnvOrDefault("DB_PASSWORD", "postgres"),
		DBName:            getEnvOrDefault("DB_NAME", "marketplace"),
		DBSSLMode:         getEnvOrDefault("DB_SSLMODE", "disable"),
		DBMaxConnections:  dbMaxConnections,
		DBIdleConnections: dbIdleConnections,

		// Настройки Redis
		RedisHost:     getEnvOrDefault("REDIS_HOST", "localhost"),
		RedisPort:     getEnvOrDefault("REDIS_PORT", "6379"),
		RedisPassword: getEnvOrDefault("REDIS_PASSWORD", ""),
		RedisDB:       redisDB,

		// Настройки Kafka
		KafkaBrokers: []string{getEnvOrDefault("KAFKA_BROKER", "localhost:9092")},

		// Настройки JWT
		JWTConfig: &config.JwtConfig{
			JWTSecret:          getEnvOrDefault("JWT_SECRET", "your-secret-key"),
			AccessTokenExpiry:  15, // 15 минут
			RefreshTokenExpiry: 7,  // 7 дней
			DBConnection:       getEnvOrDefault("DB_CONNECTION", ""),
		},

		// Настройки логирования
		LogLevel: ports.LogLevel(logLevel),
		LogJSON:  logJSON,
	}

	return config, nil
}

// getEnvOrDefault возвращает значение переменной окружения или default значение
func getEnvOrDefault(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

// Initialize инициализирует все компоненты инфраструктуры
func (i *InfrastructureInitializer) Initialize() (*InfrastructureComponents, error) {
	i.logger.Info("Initializing infrastructure components")

	// Инициализируем соединение с базой данных
	db, err := i.initDatabase()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	// Инициализируем соединение с Redis
	redisClient, err := i.initRedis()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize Redis: %w", err)
	}

	// Создаем хранилище на основе GORM
	storagePort := repositories.NewGormStoragePort(db)

	// Создаем адаптер для кэша
	cachePort := redis_cache.NewRedisCachePort(redisClient)

	// Создаем адаптер для сообщений
	messagingPort := kafka.NewKafkaMessagingPort(i.config.KafkaBrokers)

	// Создаем транзакционные адаптеры
	txStorageAdapter := transactions.NewTransactionalStorageAdapter(storagePort)
	txCacheAdapter := transactions.NewTransactionalCacheAdapter(cachePort)
	txMessagingAdapter := transactions.NewTransactionalMessagingAdapter(messagingPort)

	// Создаем менеджер транзакций
	txManager := transactions.NewTransactionManager(
		db,
		txStorageAdapter,
		txCacheAdapter,
		txMessagingAdapter,
		i.logger,
	)

	// Создаем итоговую структуру с компонентами
	components := &InfrastructureComponents{
		DB:              db,
		RedisClient:     redisClient,
		Logger:          i.logger,
		StoragePort:     storagePort,
		CachePort:       cachePort,
		MessagingPort:   messagingPort,
		TransactionPort: txManager,
	}

	i.logger.Info("Infrastructure components initialized successfully")
	return components, nil
}

// initDatabase инициализирует соединение с базой данных
func (i *InfrastructureInitializer) initDatabase() (*gorm.DB, error) {
	dsn := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		i.config.DBHost,
		i.config.DBPort,
		i.config.DBUser,
		i.config.DBPassword,
		i.config.DBName,
		i.config.DBSSLMode,
	)

	// Настраиваем логгер GORM
	gormLogger := logger.New(
		&gormLogWriter{i.logger},
		logger.Config{
			SlowThreshold:             200 * time.Millisecond,
			LogLevel:                  logger.Info,
			IgnoreRecordNotFoundError: true,
			Colorful:                  false,
		},
	)

	// Подключаемся к базе данных
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: gormLogger,
	})
	if err != nil {
		return nil, err
	}

	// Настраиваем пул соединений
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}

	sqlDB.SetMaxOpenConns(i.config.DBMaxConnections)
	sqlDB.SetMaxIdleConns(i.config.DBIdleConnections)
	sqlDB.SetConnMaxLifetime(time.Hour)

	i.logger.Info("Database connection established")
	return db, nil
}

// initRedis инициализирует соединение с Redis
func (i *InfrastructureInitializer) initRedis() (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", i.config.RedisHost, i.config.RedisPort),
		Password: i.config.RedisPassword,
		DB:       i.config.RedisDB,
	})

	// Проверяем соединение с Redis
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.Ping(ctx).Result()
	if err != nil {
		return nil, err
	}

	i.logger.Info("Redis connection established")
	return client, nil
}

// InfrastructureComponents содержит все инициализированные компоненты
type InfrastructureComponents struct {
	DB              *gorm.DB
	RedisClient     *redis.Client
	Logger          ports.LoggerPort
	StoragePort     ports.StoragePort
	CachePort       ports.CachePort
	MessagingPort   ports.MessagingPort
	TransactionPort ports.TransactionPort
}

// Cleanup освобождает ресурсы
func (c *InfrastructureComponents) Cleanup() error {
	c.Logger.Info("Cleaning up infrastructure components")

	// Закрываем соединение с базой данных
	sqlDB, err := c.DB.DB()
	if err == nil {
		if err := sqlDB.Close(); err != nil {
			c.Logger.Error("Failed to close database connection", "error", err)
		}
	}

	// Закрываем соединение с Redis
	if err := c.RedisClient.Close(); err != nil {
		c.Logger.Error("Failed to close Redis connection", "error", err)
	}

	// Закрываем соединение с Kafka
	if err := c.MessagingPort.Close(); err != nil {
		c.Logger.Error("Failed to close Kafka connection", "error", err)
	}

	c.Logger.Info("Infrastructure components cleaned up")
	return nil
}

// Адаптер для логирования GORM через наш логгер
type gormLogWriter struct {
	logger ports.LoggerPort
}

// Printf реализует интерфейс io.Writer для GORM логгера
func (w *gormLogWriter) Printf(format string, args ...interface{}) {
	w.logger.Debug(fmt.Sprintf(format, args...))
}
