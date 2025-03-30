package repositories

import (
	"database/sql"
	"fmt"
	"github.com/lib/pq"
	"gomarketplace_api/internal/suppliers/wholesaler/models"
	"gomarketplace_api/internal/suppliers/wholesaler/storage"
	"gomarketplace_api/pkg/logger"
	"io"
	"log"
)

type SizeRepository struct {
	db     *sql.DB
	logger logger.Logger
}

func NewSizeRepository(db *sql.DB, logWriter io.Writer) *SizeRepository {
	debugLogger := logger.NewLogger(logWriter, "[SizeRepository]")
	debugLogger.Log("SizeRepository successfully created.")
	return &SizeRepository{
		db:     db,
		logger: debugLogger,
	}
}
func (r *SizeRepository) GetSizes() ([]models.SizeData, error) {
	query := `
		SELECT 
			sizes.global_id, 
			size_values.descriptor, 
			size_values.value_type, 
			size_values.value,
			size_values.unit
		FROM 
			wholesaler.sizes AS sizes
		JOIN 
			wholesaler.size_values AS size_values
		ON 
			sizes.size_id = size_values.size_id
	`

	rows, err := r.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("ошибка выполнения запроса: %w", err)
	}
	defer rows.Close()

	// Результат в виде списка SizeData
	var result []models.SizeData
	sizeMap := make(map[int]int) // Map для отслеживания индексов GlobalID в result

	for rows.Next() {
		var (
			globalID   int
			descriptor string
			sizeType   string
			value      float64
			unit       string
		)

		if err := rows.Scan(&globalID, &descriptor, &sizeType, &value, &unit); err != nil {
			return nil, fmt.Errorf("ошибка сканирования строки: %w", err)
		}

		// Преобразуем данные в SizeWrapper
		sizeWrapper := models.SizeWrapper{
			Descriptor: models.SizeDescriptorEnum(descriptor),
			Type:       models.SizeTypeEnum(sizeType),
			Value:      value,
			Unit:       unit,
		}

		// Проверяем, есть ли уже GlobalID в result
		if idx, exists := sizeMap[globalID]; exists {
			// Добавляем новый SizeWrapper к существующему товару
			result[idx].Sizes = append(result[idx].Sizes, sizeWrapper)
		} else {
			// Создаем новый SizeData для GlobalID
			sizeMap[globalID] = len(result)
			result = append(result, models.SizeData{
				GlobalID: globalID,
				Sizes:    []models.SizeWrapper{sizeWrapper},
			})
		}
	}

	// Проверка на ошибки после итерации
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ошибка при итерации по строкам: %w", err)
	}

	return result, nil
}

func (r *SizeRepository) GetSizesByIDs(ids []int) ([]models.SizeData, error) {
	// Проверяем, что массив ID не пустой
	if len(ids) == 0 {
		return nil, fmt.Errorf("массив GlobalID не может быть пустым")
	}

	query := fmt.Sprintf(`
		SELECT 
			sizes.global_id, 
			size_values.descriptor, 
			size_values.value_type, 
			size_values.value,
			size_values.unit
		FROM 
			wholesaler.sizes AS sizes
		JOIN 
			wholesaler.size_values AS size_values
		ON 
			sizes.size_id = size_values.size_id
		WHERE 
			sizes.global_id = ANY($1)
	`)

	rows, err := r.db.Query(query, pq.Array(ids))
	if err != nil {
		return nil, fmt.Errorf("ошибка выполнения запроса: %w", err)
	}
	defer rows.Close()

	// Результат в виде списка SizeData
	var result []models.SizeData
	sizeMap := make(map[int]int) // Map для отслеживания индексов GlobalID в result

	for rows.Next() {
		var (
			globalID   int
			descriptor string
			sizeType   string
			value      float64
			unit       string
		)

		if err := rows.Scan(&globalID, &descriptor, &sizeType, &value, &unit); err != nil {
			return nil, fmt.Errorf("ошибка сканирования строки: %w", err)
		}

		// Преобразуем данные в SizeWrapper
		sizeWrapper := models.SizeWrapper{
			Descriptor: models.SizeDescriptorEnum(descriptor),
			Type:       models.SizeTypeEnum(sizeType),
			Value:      value,
			Unit:       unit,
		}

		// Проверяем, есть ли уже GlobalID в result
		if idx, exists := sizeMap[globalID]; exists {
			// Добавляем новый SizeWrapper к существующему товару
			result[idx].Sizes = append(result[idx].Sizes, sizeWrapper)
		} else {
			// Создаем новый SizeData для GlobalID
			sizeMap[globalID] = len(result)
			result = append(result, models.SizeData{
				GlobalID: globalID,
				Sizes:    []models.SizeWrapper{sizeWrapper},
			})
		}
	}

	// Проверка на ошибки после итерации
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ошибка при итерации по строкам: %w", err)
	}

	return result, nil
}

func (r *SizeRepository) Populate() error {
	log.Printf("Updating sizes.")
	productsQuery := `SELECT global_id, COALESCE(dimension, '') AS dimension FROM wholesaler.products`
	rows, err := r.db.Query(productsQuery)
	if err != nil {
		return fmt.Errorf("failed to select from wholesaler.products: %w", err)
	}
	defer rows.Close()

	insertSizeStmt, err := r.db.Prepare("INSERT INTO wholesaler.sizes (global_id) VALUES ($1) RETURNING size_id")
	if err != nil {
		return fmt.Errorf("failed to prepare insertSize statement: %w", err)
	}
	defer insertSizeStmt.Close()

	insertSizeValueStmt, err := r.db.Prepare("INSERT INTO wholesaler.size_values (size_id, descriptor, value_type, value, unit) VALUES ($1, $2, $3, $4, $5)")
	if err != nil {
		return fmt.Errorf("failed to prepare insertSizeValue statement: %w", err)
	}
	defer insertSizeValueStmt.Close()

	for rows.Next() {
		var globalID int
		var dimension string
		if err := rows.Scan(&globalID, &dimension); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		sizeDescriptors, err := storage.ParseSizes(dimension)
		if err != nil {
			return fmt.Errorf("failed to parse sizes for globalID %d: %w", globalID, err)
		}

		var sizeID int
		err = insertSizeStmt.QueryRow(globalID).Scan(&sizeID)
		if err != nil {
			return fmt.Errorf("failed to insert into sizes for globalID %d: %w", globalID, err)
		}

		for _, size := range sizeDescriptors {
			if size.Unit == "" {
				log.Printf("UNIT WARN : %s for %d is not defined", size.Unit, sizeID)
			}
			_, err = insertSizeValueStmt.Exec(sizeID, size.Descriptor, size.Type, size.Value, size.Unit)
			if err != nil {
				return fmt.Errorf("failed to insert into size_values for globalID %d: %w", globalID, err)
			}
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating over rows: %w", err)
	}

	log.Println("Data population completed successfully.")
	return nil
}

func (r *SizeRepository) Close() error {
	return r.db.Close()
}
