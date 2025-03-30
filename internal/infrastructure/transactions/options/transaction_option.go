package options

// TransactionOptions настройки для создания новой транзакции
type TransactionOptions struct {
	// IsolationLevel определяет уровень изоляции транзакции
	IsolationLevel IsolationLevel
	// PropagationBehavior определяет поведение при вложенности транзакций
	PropagationBehavior PropagationBehavior
	// ReadOnly указывает, что транзакция только для чтения
	ReadOnly bool
	// Timeout задает таймаут для транзакции (0 - без таймаута)
	Timeout int
	// TenantID идентификатор арендатора (для многоарендной системы)
	TenantID string
}

// DefaultTransactionOptions возвращает настройки транзакции по умолчанию
func DefaultTransactionOptions() TransactionOptions {
	return TransactionOptions{
		IsolationLevel:      LevelDefault,
		PropagationBehavior: PropagationRequired,
		ReadOnly:            false,
		Timeout:             0,
	}
}

// DefaultTransactionOptionsWithTenant возвращает настройки транзакции по умолчанию для указанного арендатора
func DefaultTransactionOptionsWithTenant(tenantID string) TransactionOptions {
	options := DefaultTransactionOptions()
	options.TenantID = tenantID
	return options
}
