package csv_to_postgres

type ForeignKey struct {
	ReferenceTable  string   // Имя связанной таблицы
	ReferenceColumn string   // Имя связанного столбца
	Columns         []string // Локальные колонки (для составных ключей)
}

type UpdateConfig struct {
	ForeignKeys      []ForeignKey
	OnConflictAction string   // "NOTHING", "UPDATE", "CASCADE" и т.д.
	ConflictColumns  []string // Колонки для условия CONFLICT
}
