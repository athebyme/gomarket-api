package repositories

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/lib/pq"
	"log"
	"strings"
)

type MediaRepository struct {
	db *sql.DB
}

func NewMediaRepository(db *sql.DB) *MediaRepository {
	return &MediaRepository{db: db}
}

func (r *MediaRepository) Populate() error {
	log.Println("Updating media.")

	rows, err := r.db.Query(`
	SELECT global_id 
	FROM wholesaler.products 
	WHERE global_id NOT IN (
		SELECT global_id FROM wholesaler.media WHERE global_id IS NOT NULL
	);
	`)
	if err != nil {
		return fmt.Errorf("failed to fetch global_ids: %w", err)
	}
	defer rows.Close()

	var productIDs []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return fmt.Errorf("failed to scan global_id: %w", err)
		}
		productIDs = append(productIDs, id)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("row iteration error: %w", err)
	}

	if len(productIDs) == 0 {
		log.Println("No products found for media update.")
		return nil
	}

	// Задаем размер изображений (примерное значение, можно изменить по необходимости)
	const imageSize = 1200

	// Получаем URL'ы медиа из products (не цензурированные)
	mediaSources, err := r.GetMediaSourcesByProductIDs(productIDs, false, imageSize)
	if err != nil {
		return fmt.Errorf("failed to get media sources: %w", err)
	}

	// Получаем URL'ы медиа из products (цензурированные)
	mediaSourcesCensored, err := r.GetMediaSourcesByProductIDs(productIDs, true, imageSize)
	if err != nil {
		return fmt.Errorf("failed to get censored media sources: %w", err)
	}

	// Вставляем данные в таблицу wholesaler.media
	for _, id := range productIDs {
		urls := mediaSources[id]
		censoredUrls := mediaSourcesCensored[id]

		// Если для продукта не найдены URL'ы, пропускаем вставку
		if len(urls) == 0 {
			log.Printf("No media URLs found for product %d (non-censored)", id)
			continue
		}
		if len(censoredUrls) == 0 {
			log.Printf("No media URLs found for product %d (censored)", id)
			continue
		}

		_, err = r.db.Exec(`
			INSERT INTO wholesaler.media (global_id, images, images_censored)
			VALUES ($1, $2, $3)
		`, id, pq.Array(urls), pq.Array(censoredUrls))
		if err != nil {
			log.Printf("Failed to insert media for product %d: %v", id, err)
		}
	}

	log.Println("Media population completed successfully.")
	return nil
}

type ImageSize int

const (
	BigSize    ImageSize = 1200
	MediumSize ImageSize = 600
	SmallSize  ImageSize = 400
)

func (r *MediaRepository) GetMediaSources(censored bool) (map[int][]string, error) {
	query := `SELECT global_id, CASE WHEN $1 THEN images_censored ELSE images END FROM wholesaler.media`

	rows, err := r.db.Query(query, censored)
	if err != nil {
		return nil, fmt.Errorf("ошибка выполнения запроса для получения media: %w", err)
	}
	defer rows.Close()

	mediaMap := make(map[int][]string)
	for rows.Next() {
		var globalId int
		var media []string
		if err := rows.Scan(&globalId, pq.Array(&media)); err != nil {
			return nil, fmt.Errorf("ошибка сканирования media: %w", err)
		}

		mediaMap[globalId] = media
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ошибка при итерации по строкам: %w", err)
	}

	return mediaMap, nil
}

func (r *MediaRepository) GetMediaSourcesByProductIDs(productIDs []int, censored bool, imageSize ImageSize) (map[int][]string, error) {
	var err error
	mediaMap := make(map[int][]string)
	for _, productID := range productIDs {
		mediaMap[productID], err = r.GetMediaSourceByProductID(productID, censored, imageSize)
		if err != nil {
			return nil, err
		}
	}
	return mediaMap, nil
}

func (r *MediaRepository) GetMediaSourceByProductID(productID int, censored bool, imageSize ImageSize) ([]string, error) {
	query := `SELECT media FROM wholesaler.products WHERE global_id = $1`
	var media string
	err := r.db.QueryRow(query, productID).Scan(
		&media,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get product media: %w", err)
	}

	sourceKeys := strings.Fields(media)
	mediaUrls := make([]string, len(sourceKeys))

	var format string

	if censored {
		format = "https://x-story.ru/mp/_project/img_sx0_%d/%d_%s_%d.jpg"
		for i, sourceKey := range sourceKeys {
			mediaUrls[i] = fmt.Sprintf(format, imageSize, productID, sourceKey, imageSize)
		}
	} else {
		format = "http://media.athebyme-market.ru/%d/%d.jpg"
		for i, _ := range sourceKeys {
			mediaUrls[i] = fmt.Sprintf(format, productID, i)
		}

		// если проблемы с доменом
		//format = "https://x-story.ru/mp/_project/img_sx_%d/%d_%s_%d.jpg"
		//for i, sourceKey := range sourceKeys {
		//	mediaUrls[i] = fmt.Sprintf(format, imageSize, productID, sourceKey, imageSize)
		//}
	}

	return mediaUrls, nil
}
