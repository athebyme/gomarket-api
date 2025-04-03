package options

// IsolationLevel определяет уровень изоляции транзакции
type IsolationLevel int

const (
	// LevelDefault использует уровень изоляции по умолчанию для текущего хранилища
	LevelDefault IsolationLevel = iota
	// LevelReadUncommitted разрешает чтение незафиксированных данных
	LevelReadUncommitted
	// LevelReadCommitted разрешает чтение только зафиксированных данных
	LevelReadCommitted
	// LevelWriteCommitted сочетает LevelReadCommitted с блокировками записи
	LevelWriteCommitted
	// LevelRepeatableRead гарантирует, что повторное чтение тех же данных вернет тот же результат
	LevelRepeatableRead
	// LevelSnapshot аналогичен LevelRepeatableRead, но избегает фантомных чтений
	LevelSnapshot
	// LevelSerializable обеспечивает полную изоляцию транзакций
	LevelSerializable
	// LevelLinearizable обеспечивает сериализацию с дополнительными гарантиями упорядоченности
	LevelLinearizable
)
