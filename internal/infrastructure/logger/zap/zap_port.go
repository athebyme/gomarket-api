package logger

import (
	"context"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gomarketplace_api/internal/core/ports"
)

// ZapLoggerPort реализует интерфейс LoggerPort с использованием библиотеки zap
type ZapLoggerPort struct {
	logger *zap.Logger
	level  ports.LogLevel
}

// NewZapLoggerPort создает экземпляр ZapLoggerPort
func NewZapLoggerPort(level ports.LogLevel) (*ZapLoggerPort, error) {
	// Конфигурация для JSON логгера
	zapLevel := convertLogLevel(level)
	config := zap.Config{
		Level:             zap.NewAtomicLevelAt(zapLevel),
		Development:       false,
		Encoding:          "json",
		EncoderConfig:     zap.NewProductionEncoderConfig(),
		OutputPaths:       []string{"stderr"},
		ErrorOutputPaths:  []string{"stderr"},
		DisableCaller:     false,
		DisableStacktrace: false,
	}

	logger, err := config.Build()
	if err != nil {
		return nil, err
	}

	return &ZapLoggerPort{
		logger: logger,
		level:  level,
	}, nil
}

// NewZapLoggerPortDev создает экземпляр ZapLoggerPort для разработки
func NewZapLoggerPortDev(level ports.LogLevel) (*ZapLoggerPort, error) {
	// Конфигурация для консольного логгера в режиме разработки
	zapLevel := convertLogLevel(level)
	config := zap.Config{
		Level:             zap.NewAtomicLevelAt(zapLevel),
		Development:       true,
		Encoding:          "console",
		EncoderConfig:     zap.NewDevelopmentEncoderConfig(),
		OutputPaths:       []string{"stderr"},
		ErrorOutputPaths:  []string{"stderr"},
		DisableCaller:     false,
		DisableStacktrace: false,
	}

	logger, err := config.Build()
	if err != nil {
		return nil, err
	}

	return &ZapLoggerPort{
		logger: logger,
		level:  level,
	}, nil
}

// NewZapLoggerPortWithConfig создает экземпляр ZapLoggerPort с пользовательской конфигурацией
func NewZapLoggerPortWithConfig(config zap.Config, level ports.LogLevel) (*ZapLoggerPort, error) {
	logger, err := config.Build()
	if err != nil {
		return nil, err
	}

	return &ZapLoggerPort{
		logger: logger,
		level:  level,
	}, nil
}

// convertLogLevel конвертирует уровень логирования в формат zap
func convertLogLevel(level ports.LogLevel) zapcore.Level {
	switch level {
	case ports.DebugLevel:
		return zapcore.DebugLevel
	case ports.InfoLevel:
		return zapcore.InfoLevel
	case ports.WarnLevel:
		return zapcore.WarnLevel
	case ports.ErrorLevel:
		return zapcore.ErrorLevel
	case ports.FatalLevel:
		return zapcore.FatalLevel
	case ports.PanicLevel:
		return zapcore.PanicLevel
	default:
		return zapcore.InfoLevel
	}
}

// convertToZapFields конвертирует args в поля zap
func convertToZapFields(args ...interface{}) []zap.Field {
	if len(args) == 0 {
		return nil
	}

	fields := make([]zap.Field, 0, len(args)/2)

	// Проверяем, что args представляют собой пары ключ-значение
	for i := 0; i < len(args); i += 2 {
		key, ok := args[i].(string)
		if !ok {
			continue
		}

		// Если не хватает значения для последнего ключа
		if i+1 >= len(args) {
			fields = append(fields, zap.Any(key, nil))
			break
		}

		fields = append(fields, zap.Any(key, args[i+1]))
	}

	return fields
}

// extractContext извлекает информацию из контекста для логирования
func extractContext(ctx context.Context) []zap.Field {
	fields := make([]zap.Field, 0, 2)

	// Извлекаем ID арендатора из контекста (если есть)
	if tenantID, ok := ctx.Value("tenant_id").(string); ok && tenantID != "" {
		fields = append(fields, zap.String("tenant_id", tenantID))
	}

	// Извлекаем ID трассировки из контекста (если есть)
	if traceID, ok := ctx.Value("trace_id").(string); ok && traceID != "" {
		fields = append(fields, zap.String("trace_id", traceID))
	}

	return fields
}

// Debug логирует сообщение с уровнем Debug
func (l *ZapLoggerPort) Debug(msg string, args ...interface{}) {
	if l.level > ports.DebugLevel {
		return
	}
	l.logger.Debug(msg, convertToZapFields(args...)...)
}

// Info логирует сообщение с уровнем Info
func (l *ZapLoggerPort) Info(msg string, args ...interface{}) {
	if l.level > ports.InfoLevel {
		return
	}
	l.logger.Info(msg, convertToZapFields(args...)...)
}

// Warn логирует сообщение с уровнем Warn
func (l *ZapLoggerPort) Warn(msg string, args ...interface{}) {
	if l.level > ports.WarnLevel {
		return
	}
	l.logger.Warn(msg, convertToZapFields(args...)...)
}

// Error логирует сообщение с уровнем Error
func (l *ZapLoggerPort) Error(msg string, args ...interface{}) {
	if l.level > ports.ErrorLevel {
		return
	}
	l.logger.Error(msg, convertToZapFields(args...)...)
}

// Fatal логирует сообщение с уровнем Fatal и завершает программу
func (l *ZapLoggerPort) Fatal(msg string, args ...interface{}) {
	if l.level > ports.FatalLevel {
		return
	}
	l.logger.Fatal(msg, convertToZapFields(args...)...)
}

// Panic логирует сообщение с уровнем Panic и вызывает панику
func (l *ZapLoggerPort) Panic(msg string, args ...interface{}) {
	if l.level > ports.PanicLevel {
		return
	}
	l.logger.Panic(msg, convertToZapFields(args...)...)
}

// DebugWithContext логирует сообщение с контекстом
func (l *ZapLoggerPort) DebugWithContext(ctx context.Context, msg string, args ...interface{}) {
	if l.level > ports.DebugLevel {
		return
	}
	fields := append(extractContext(ctx), convertToZapFields(args...)...)
	l.logger.Debug(msg, fields...)
}

// InfoWithContext логирует сообщение с контекстом
func (l *ZapLoggerPort) InfoWithContext(ctx context.Context, msg string, args ...interface{}) {
	if l.level > ports.InfoLevel {
		return
	}
	fields := append(extractContext(ctx), convertToZapFields(args...)...)
	l.logger.Info(msg, fields...)
}

// WarnWithContext логирует сообщение с контекстом
func (l *ZapLoggerPort) WarnWithContext(ctx context.Context, msg string, args ...interface{}) {
	if l.level > ports.WarnLevel {
		return
	}
	fields := append(extractContext(ctx), convertToZapFields(args...)...)
	l.logger.Warn(msg, fields...)
}

// ErrorWithContext логирует сообщение с контекстом
func (l *ZapLoggerPort) ErrorWithContext(ctx context.Context, msg string, args ...interface{}) {
	if l.level > ports.ErrorLevel {
		return
	}
	fields := append(extractContext(ctx), convertToZapFields(args...)...)
	l.logger.Error(msg, fields...)
}

// FatalWithContext логирует сообщение с контекстом и завершает программу
func (l *ZapLoggerPort) FatalWithContext(ctx context.Context, msg string, args ...interface{}) {
	if l.level > ports.FatalLevel {
		return
	}
	fields := append(extractContext(ctx), convertToZapFields(args...)...)
	l.logger.Fatal(msg, fields...)
}

// PanicWithContext логирует сообщение с контекстом и вызывает панику
func (l *ZapLoggerPort) PanicWithContext(ctx context.Context, msg string, args ...interface{}) {
	if l.level > ports.PanicLevel {
		return
	}
	fields := append(extractContext(ctx), convertToZapFields(args...)...)
	l.logger.Panic(msg, fields...)
}

// WithFields возвращает новый логгер с добавленными полями
func (l *ZapLoggerPort) WithFields(fields ...ports.LogField) ports.LoggerPort {
	zapFields := make([]zap.Field, len(fields))
	for i, field := range fields {
		zapFields[i] = zap.Any(field.Key, field.Value)
	}

	newLogger := l.logger.With(zapFields...)
	return &ZapLoggerPort{
		logger: newLogger,
		level:  l.level,
	}
}

// WithField возвращает новый логгер с добавленным полем
func (l *ZapLoggerPort) WithField(key string, value interface{}) ports.LoggerPort {
	newLogger := l.logger.With(zap.Any(key, value))
	return &ZapLoggerPort{
		logger: newLogger,
		level:  l.level,
	}
}

// WithTenant возвращает новый логгер с добавленным идентификатором арендатора
func (l *ZapLoggerPort) WithTenant(tenantID string) ports.LoggerPort {
	newLogger := l.logger.With(zap.String("tenant_id", tenantID))
	return &ZapLoggerPort{
		logger: newLogger,
		level:  l.level,
	}
}

// WithTraceID возвращает новый логгер с добавленным идентификатором трассировки
func (l *ZapLoggerPort) WithTraceID(traceID string) ports.LoggerPort {
	newLogger := l.logger.With(zap.String("trace_id", traceID))
	return &ZapLoggerPort{
		logger: newLogger,
		level:  l.level,
	}
}

// SetLevel устанавливает минимальный уровень логирования
func (l *ZapLoggerPort) SetLevel(level ports.LogLevel) {
	l.level = level
	l.logger.Core().Enabled(convertLogLevel(level))
}

// GetLevel возвращает текущий уровень логирования
func (l *ZapLoggerPort) GetLevel() ports.LogLevel {
	return l.level
}

// Flush сбрасывает буферы и гарантирует запись всех сообщений
func (l *ZapLoggerPort) Flush() error {
	return l.logger.Sync()
}

// Sync синхронизирует записи буфера с хранилищем логов
func (l *ZapLoggerPort) Sync() error {
	return l.logger.Sync()
}

// Убедимся, что ZapLoggerPort реализует интерфейс LoggerPort
var _ ports.LoggerPort = (*ZapLoggerPort)(nil)
