package infrastructure

import (
	"context"
	"fmt"
	transactionsadapters "gomarketplace_api/internal/infrastructure/transactional/adapters"
	transactionmanager "gomarketplace_api/internal/infrastructure/transactional/transactions"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"gomarketplace_api/internal/auth/config"
	"gomarketplace_api/internal/core/ports"
	redis_cache "gomarketplace_api/internal/infrastructure/cache/redis"
	zap_logger "gomarketplace_api/internal/infrastructure/logger/zap"
	"gomarketplace_api/internal/infrastructure/messaging/kafka"
	"gomarketplace_api/internal/infrastructure/repositories/gorm"
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
	DBConnMaxLifetime time.Duration
	DBConnMaxIdleTime time.Duration
	DBSlowThreshold   time.Duration
	DBLogLevel        ports.LogLevel

	// Настройки повторного подключения
	ReconnectMaxAttempts int
	ReconnectBaseDelay   time.Duration
	ReconnectMaxDelay    time.Duration

	// Настройки Redis
	RedisHost              string
	RedisPort              string
	RedisPassword          string
	RedisDB                int
	RedisPoolSize          int
	RedisMinIdleConns      int
	RedisConnMaxLifetime   time.Duration
	RedisConnMaxIdleTime   time.Duration
	RedisHealthCheckPeriod time.Duration

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
	config       InfrastructureConfig
	logger       ports.LoggerPort
	shutdownChan chan struct{}
	wg           sync.WaitGroup
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
		config:       config,
		logger:       loggerPort,
		shutdownChan: make(chan struct{}),
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

	dbConnMaxLifetime, err := time.ParseDuration(getEnvOrDefault("DB_CONN_MAX_LIFETIME", "1h"))
	if err != nil {
		dbConnMaxLifetime = 1 * time.Hour
	}

	dbConnMaxIdleTime, err := time.ParseDuration(getEnvOrDefault("DB_CONN_MAX_IDLE_TIME", "30m"))
	if err != nil {
		dbConnMaxIdleTime = 30 * time.Minute
	}

	dbSlowThreshold, err := time.ParseDuration(getEnvOrDefault("DB_SLOW_THRESHOLD", "200ms"))
	if err != nil {
		dbSlowThreshold = 200 * time.Millisecond
	}

	redisDB, err := strconv.Atoi(getEnvOrDefault("REDIS_DB", "0"))
	if err != nil {
		redisDB = 0
	}

	redisPoolSize, err := strconv.Atoi(getEnvOrDefault("REDIS_POOL_SIZE", "10"))
	if err != nil {
		redisPoolSize = 10
	}

	redisMinIdleConns, err := strconv.Atoi(getEnvOrDefault("REDIS_MIN_IDLE_CONNS", "5"))
	if err != nil {
		redisMinIdleConns = 5
	}

	redisConnMaxLifetime, err := time.ParseDuration(getEnvOrDefault("REDIS_CONN_MAX_LIFETIME", "1h"))
	if err != nil {
		redisConnMaxLifetime = 1 * time.Hour
	}

	redisConnMaxIdleTime, err := time.ParseDuration(getEnvOrDefault("REDIS_CONN_MAX_IDLE_TIME", "30m"))
	if err != nil {
		redisConnMaxIdleTime = 30 * time.Minute
	}

	redisHealthCheckPeriod, err := time.ParseDuration(getEnvOrDefault("REDIS_HEALTH_CHECK_PERIOD", "1m"))
	if err != nil {
		redisHealthCheckPeriod = 1 * time.Minute
	}

	reconnectMaxAttempts, err := strconv.Atoi(getEnvOrDefault("RECONNECT_MAX_ATTEMPTS", "10"))
	if err != nil {
		reconnectMaxAttempts = 10
	}

	reconnectBaseDelay, err := time.ParseDuration(getEnvOrDefault("RECONNECT_BASE_DELAY", "1s"))
	if err != nil {
		reconnectBaseDelay = 1 * time.Second
	}

	reconnectMaxDelay, err := time.ParseDuration(getEnvOrDefault("RECONNECT_MAX_DELAY", "1m"))
	if err != nil {
		reconnectMaxDelay = 1 * time.Minute
	}

	logLevel, err := strconv.Atoi(getEnvOrDefault("LOG_LEVEL", "1"))
	if err != nil {
		logLevel = 1 // Info по умолчанию
	}

	dbLogLevel, err := strconv.Atoi(getEnvOrDefault("DB_LOG_LEVEL", "1"))
	if err != nil {
		dbLogLevel = 1 // Info по умолчанию
	}

	logJSON := getEnvOrDefault("LOG_JSON", "false") == "true"

	cfg := InfrastructureConfig{
		// Настройки базы данных
		DBHost:            getEnvOrDefault("DB_HOST", "localhost"),
		DBPort:            getEnvOrDefault("DB_PORT", "5432"),
		DBUser:            getEnvOrDefault("DB_USER", "postgres"),
		DBPassword:        getEnvOrDefault("DB_PASSWORD", "postgres"),
		DBName:            getEnvOrDefault("DB_NAME", "marketplace"),
		DBSSLMode:         getEnvOrDefault("DB_SSLMODE", "disable"),
		DBMaxConnections:  dbMaxConnections,
		DBIdleConnections: dbIdleConnections,
		DBConnMaxLifetime: dbConnMaxLifetime,
		DBConnMaxIdleTime: dbConnMaxIdleTime,
		DBSlowThreshold:   dbSlowThreshold,
		DBLogLevel:        ports.LogLevel(dbLogLevel),

		// Настройки повторного подключения
		ReconnectMaxAttempts: reconnectMaxAttempts,
		ReconnectBaseDelay:   reconnectBaseDelay,
		ReconnectMaxDelay:    reconnectMaxDelay,

		// Настройки Redis
		RedisHost:              getEnvOrDefault("REDIS_HOST", "localhost"),
		RedisPort:              getEnvOrDefault("REDIS_PORT", "6379"),
		RedisPassword:          getEnvOrDefault("REDIS_PASSWORD", ""),
		RedisDB:                redisDB,
		RedisPoolSize:          redisPoolSize,
		RedisMinIdleConns:      redisMinIdleConns,
		RedisConnMaxLifetime:   redisConnMaxLifetime,
		RedisConnMaxIdleTime:   redisConnMaxIdleTime,
		RedisHealthCheckPeriod: redisHealthCheckPeriod,

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

	return cfg, nil
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
	messagingPort, err := i.initMessaging()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize messaging: %w", err)
	}

	// Создаем транзакционные адаптеры
	txStorageAdapter := transactionsadapters.NewTransactionalStorageAdapter(storagePort)
	txCacheAdapter := transactionsadapters.NewTransactionalCacheAdapter(cachePort)
	txMessagingAdapter := transactionsadapters.NewTransactionalMessagingAdapter(messagingPort)

	// Создаем менеджер транзакций
	txManager := transactionmanager.NewTransactionManager(
		db,
		txStorageAdapter,
		txCacheAdapter,
		txMessagingAdapter,
		i.logger,
	)

	// Запускаем мониторинг соединений
	i.startConnectionMonitoring(db, redisClient)

	// Создаем итоговую структуру с компонентами
	components := &InfrastructureComponents{
		DB:              db,
		RedisClient:     redisClient,
		Logger:          i.logger,
		StoragePort:     storagePort,
		CachePort:       cachePort,
		MessagingPort:   messagingPort,
		TransactionPort: txManager,
		initializer:     i,
	}

	i.logger.Info("Infrastructure components initialized successfully")
	return components, nil
}

// initDatabase инициализирует соединение с базой данных с механизмом переподключения
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

	// Настраиваем логгер GORM с расширенными возможностями
	gormLogger := newEnhancedGormLogger(
		i.logger,
		i.config.DBSlowThreshold,
		convertLogLevel(i.config.DBLogLevel),
	)

	var db *gorm.DB
	var err error

	// Используем механизм повторных попыток с экспоненциальной задержкой
	err = withExponentialBackoff(i.config.ReconnectMaxAttempts, i.config.ReconnectBaseDelay, i.config.ReconnectMaxDelay,
		func() error {
			db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{
				Logger:                                   gormLogger,
				DisableForeignKeyConstraintWhenMigrating: true,
				PrepareStmt:                              true,
			})
			return err
		}, i.logger)

	if err != nil {
		return nil, fmt.Errorf("failed to connect to database after %d attempts: %w",
			i.config.ReconnectMaxAttempts, err)
	}

	// Настраиваем пул соединений
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}

	sqlDB.SetMaxOpenConns(i.config.DBMaxConnections)
	sqlDB.SetMaxIdleConns(i.config.DBIdleConnections)
	sqlDB.SetConnMaxLifetime(i.config.DBConnMaxLifetime)
	sqlDB.SetConnMaxIdleTime(i.config.DBConnMaxIdleTime)

	// Проверяем соединение
	if err := sqlDB.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	i.logger.Info("Database connection established",
		"host", i.config.DBHost,
		"port", i.config.DBPort,
		"database", i.config.DBName,
		"max_connections", i.config.DBMaxConnections)

	return db, nil
}

// initRedis инициализирует соединение с Redis с механизмом переподключения
func (i *InfrastructureInitializer) initRedis() (*redis.Client, error) {
	var client *redis.Client

	// Создаем клиент Redis с расширенными настройками пула соединений
	options := &redis.Options{
		Addr:         fmt.Sprintf("%s:%s", i.config.RedisHost, i.config.RedisPort),
		Password:     i.config.RedisPassword,
		DB:           i.config.RedisDB,
		PoolSize:     i.config.RedisPoolSize,
		MinIdleConns: i.config.RedisMinIdleConns,
		MaxConnAge:   i.config.RedisConnMaxLifetime,
		IdleTimeout:  i.config.RedisConnMaxIdleTime,
		// Добавляем возможность автоматического переподключения
		OnConnect: func(ctx context.Context, cn *redis.Conn) error {
			i.logger.Debug("Redis connection established", "host", i.config.RedisHost, "port", i.config.RedisPort)
			return nil
		},
	}

	// Используем механизм повторных попыток с экспоненциальной задержкой
	err := withExponentialBackoff(i.config.ReconnectMaxAttempts, i.config.ReconnectBaseDelay, i.config.ReconnectMaxDelay,
		func() error {
			client = redis.NewClient(options)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			return client.Ping(ctx).Err()
		}, i.logger)

	if err != nil {
		return nil, fmt.Errorf("failed to connect to Redis after %d attempts: %w",
			i.config.ReconnectMaxAttempts, err)
	}

	i.logger.Info("Redis connection established",
		"host", i.config.RedisHost,
		"port", i.config.RedisPort,
		"pool_size", i.config.RedisPoolSize)

	return client, nil
}

// initMessaging инициализирует соединение с брокером сообщений
func (i *InfrastructureInitializer) initMessaging() (ports.MessagingPort, error) {
	messagingPort := kafka.NewKafkaMessagingPort(i.config.KafkaBrokers)

	// Проверяем соединение с Kafka
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	topics, err := messagingPort.ListTopics(ctx)
	if err != nil {
		i.logger.Warn("Failed to list Kafka topics, but continuing", "error", err)
		// Решаем продолжить даже при ошибке, так как Kafka может быть недоступна при запуске
	} else {
		i.logger.Info("Kafka connection established",
			"brokers", i.config.KafkaBrokers,
			"topic_count", len(topics))
	}

	return messagingPort, nil
}

// withExponentialBackoff выполняет функцию с экспоненциальной задержкой между попытками
func withExponentialBackoff(maxAttempts int, baseDelay, maxDelay time.Duration, fn func() error, logger ports.LoggerPort) error {
	var err error
	delay := baseDelay

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err = fn()
		if err == nil {
			return nil
		}

		if attempt == maxAttempts {
			break
		}

		logger.Warn("Connection attempt failed, retrying...",
			"attempt", attempt,
			"max_attempts", maxAttempts,
			"delay", delay.String(),
			"error", err)

		time.Sleep(delay)

		// Увеличиваем задержку экспоненциально, но не больше максимальной
		delay = time.Duration(float64(delay) * 1.5)
		if delay > maxDelay {
			delay = maxDelay
		}
	}

	return err
}

// startConnectionMonitoring запускает регулярный мониторинг соединений
func (i *InfrastructureInitializer) startConnectionMonitoring(db *gorm.DB, redisClient *redis.Client) {
	i.wg.Add(1)

	go func() {
		defer i.wg.Done()

		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				i.checkDatabaseConnection(db)
				i.checkRedisConnection(redisClient)
				i.logConnectionPoolStats(db)
			case <-i.shutdownChan:
				i.logger.Info("Stopping connection monitoring")
				return
			}
		}
	}()

	i.logger.Info("Connection monitoring started")
}

// checkDatabaseConnection проверяет состояние соединения с базой данных
func (i *InfrastructureInitializer) checkDatabaseConnection(db *gorm.DB) {
	sqlDB, err := db.DB()
	if err != nil {
		i.logger.Error("Failed to get SQL DB instance", "error", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := sqlDB.PingContext(ctx); err != nil {
		i.logger.Error("Database connection check failed", "error", err)
		// Здесь можно добавить логику для автоматического переподключения,
		// но нужно учитывать, что GORM имеет встроенный механизм переподключения
	} else {
		i.logger.Debug("Database connection is healthy")
	}
}

// checkRedisConnection проверяет состояние соединения с Redis
func (i *InfrastructureInitializer) checkRedisConnection(redisClient *redis.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := redisClient.Ping(ctx).Err(); err != nil {
		i.logger.Error("Redis connection check failed", "error", err)
		// Redis клиент имеет встроенный механизм переподключения
	} else {
		i.logger.Debug("Redis connection is healthy")
	}
}

// logConnectionPoolStats логирует статистику пула соединений
func (i *InfrastructureInitializer) logConnectionPoolStats(db *gorm.DB) {
	sqlDB, err := db.DB()
	if err != nil {
		i.logger.Error("Failed to get SQL DB instance for stats", "error", err)
		return
	}

	stats := struct {
		MaxOpenConnections int
		OpenConnections    int
		InUseConnections   int
		IdleConnections    int
	}{
		MaxOpenConnections: sqlDB.Stats().MaxOpenConnections,
		OpenConnections:    sqlDB.Stats().OpenConnections,
		InUseConnections:   sqlDB.Stats().InUse,
		IdleConnections:    sqlDB.Stats().Idle,
	}

	i.logger.Debug("Database connection pool stats",
		"max_open", stats.MaxOpenConnections,
		"open", stats.OpenConnections,
		"in_use", stats.InUseConnections,
		"idle", stats.IdleConnections)
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
	initializer     *InfrastructureInitializer
}

// Cleanup освобождает ресурсы
func (c *InfrastructureComponents) Cleanup() error {
	c.Logger.Info("Cleaning up infrastructure components")

	// Сигнализируем о завершении работы мониторинга
	close(c.initializer.shutdownChan)

	// Ожидаем завершения всех горутин мониторинга
	c.initializer.wg.Wait()

	// Закрываем соединение с базой данных
	sqlDB, err := c.DB.DB()
	if err == nil {
		if err := sqlDB.Close(); err != nil {
			c.Logger.Error("Failed to close database connection", "error", err)
		} else {
			c.Logger.Info("Database connection closed successfully")
		}
	}

	// Закрываем соединение с Redis
	if err := c.RedisClient.Close(); err != nil {
		c.Logger.Error("Failed to close Redis connection", "error", err)
	} else {
		c.Logger.Info("Redis connection closed successfully")
	}

	// Закрываем соединение с Kafka
	if err := c.MessagingPort.Close(); err != nil {
		c.Logger.Error("Failed to close Kafka connection", "error", err)
	} else {
		c.Logger.Info("Kafka connection closed successfully")
	}

	c.Logger.Info("Infrastructure components cleaned up")
	return nil
}

// Расширенный логгер для GORM
type enhancedGormLogger struct {
	logger        ports.LoggerPort
	slowThreshold time.Duration
	logLevel      logger.LogLevel
}

// newEnhancedGormLogger создает новый расширенный логгер для GORM
func newEnhancedGormLogger(logger ports.LoggerPort, slowThreshold time.Duration, logLevel logger.LogLevel) *enhancedGormLogger {
	return &enhancedGormLogger{
		logger:        logger,
		slowThreshold: slowThreshold,
		logLevel:      logLevel,
	}
}

// LogMode устанавливает уровень логирования
func (l *enhancedGormLogger) LogMode(level logger.LogLevel) logger.Interface {
	newLogger := *l
	newLogger.logLevel = level
	return &newLogger
}

// Info логирует информационные сообщения
func (l *enhancedGormLogger) Info(ctx context.Context, msg string, data ...interface{}) {
	if l.logLevel >= logger.Info {
		l.logger.InfoWithContext(ctx, msg, convertToLogFields(data...)...)
	}
}

// Warn логирует предупреждения
func (l *enhancedGormLogger) Warn(ctx context.Context, msg string, data ...interface{}) {
	if l.logLevel >= logger.Warn {
		l.logger.WarnWithContext(ctx, msg, convertToLogFields(data...)...)
	}
}

// Error логирует ошибки
func (l *enhancedGormLogger) Error(ctx context.Context, msg string, data ...interface{}) {
	if l.logLevel >= logger.Error {
		l.logger.ErrorWithContext(ctx, msg, convertToLogFields(data...)...)
	}
}

// Trace логирует SQL-запросы
func (l *enhancedGormLogger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	if l.logLevel <= logger.Silent {
		return
	}

	elapsed := time.Since(begin)
	sql, rows := fc()

	// Извлекаем информацию из контекста
	fields := []interface{}{
		"elapsed", elapsed.String(),
		"rows", rows,
		"sql", sql,
	}

	if err != nil {
		fields = append(fields, "error", err)
		l.logger.ErrorWithContext(ctx, "SQL query failed", convertToLogFields(fields...)...)
		return
	}

	if l.slowThreshold != 0 && elapsed > l.slowThreshold {
		l.logger.WarnWithContext(ctx, "Slow SQL query", convertToLogFields(fields...)...)
		return
	}

	if l.logLevel >= logger.Info {
		l.logger.DebugWithContext(ctx, "SQL query executed", convertToLogFields(fields...)...)
	}
}

// convertToLogFields преобразует аргументы в поля для логгера
func convertToLogFields(args ...interface{}) []interface{} {
	return args
}

// convertLogLevel преобразует уровень логирования из ports.LogLevel в logger.LogLevel
func convertLogLevel(level ports.LogLevel) logger.LogLevel {
	switch level {
	case ports.DebugLevel:
		return logger.Info // GORM не имеет уровня Debug, поэтому используем Info
	case ports.InfoLevel:
		return logger.Info
	case ports.WarnLevel:
		return logger.Warn
	case ports.ErrorLevel:
		return logger.Error
	case ports.FatalLevel, ports.PanicLevel:
		return logger.Error // GORM не имеет уровней Fatal/Panic
	default:
		return logger.Info
	}
}
