package options

// PropagationBehavior определяет как должны вести себя транзакции при вложенности
type PropagationBehavior int

const (
	// PropagationRequired использует текущую транзакцию, если она существует,
	// иначе создает новую
	PropagationRequired PropagationBehavior = iota
	// PropagationRequiresNew всегда создает новую транзакцию
	PropagationRequiresNew
	// PropagationSupports использует текущую транзакцию, если она существует,
	// иначе выполняет операции без транзакции
	PropagationSupports
	// PropagationNested создает вложенную транзакцию, если текущая существует
	PropagationNested
	// PropagationNever выбрасывает ошибку, если текущая транзакция существует
	PropagationNever
	// PropagationMandatory выбрасывает ошибку, если текущая транзакция не существует
	PropagationMandatory
	// PropagationNotSupported приостанавливает текущую транзакцию, если она существует
	PropagationNotSupported
)
