package update

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"gomarketplace_api/config"
	"gomarketplace_api/internal/wildberries/business/models/dto/request"
	"gomarketplace_api/internal/wildberries/business/services"
	"gomarketplace_api/internal/wildberries/business/services/builder"
	parse2 "gomarketplace_api/internal/wildberries/business/services/parse"
	"gomarketplace_api/internal/wildberries/business/services/update/filter_utils"
	"gomarketplace_api/pkg/business/service"
	"gomarketplace_api/pkg/logger"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

const uploadCardsUrl = "https://content-api.wildberries.ru/content/v2/cards/upload"

type CardService struct {
	cardBuilder  builder.Proxy
	textService  service.ITextService
	brandService parse2.BrandService
	nmService    NomenclatureService
	dataProvider DataProvider

	config.WildberriesConfig
	logger.Logger
	services.AuthEngine
}

func NewCardService(
	textService service.ITextService,
	writer io.Writer,
	wildberriesConfig config.WildberriesConfig,
	dataProvider DataProvider) *CardService {
	_log := logger.NewLogger(writer, "[ CardService ]")
	cardBuilder := parse2.NewCardBuilderEngine(writer, wildberriesConfig.WbValues)

	return &CardService{
		Logger:            _log,
		cardBuilder:       cardBuilder,
		WildberriesConfig: wildberriesConfig,
		brandService:      parse2.NewBrandServiceWildberries(wildberriesConfig.WbBanned.BannedBrands),
		dataProvider:      dataProvider,
		textService:       textService,
		AuthEngine:        services.NewBearerAuth(wildberriesConfig.ApiKey),
	}
}

func (s *CardService) Prepare(ctx context.Context, ids []int) (interface{}, error) {
	preparationsContext, cancel := context.WithTimeout(ctx, time.Minute*2)
	defer cancel()

	preparationLogger := s.Logger.WithPrefix("[ Preparation stage ] ")
	startTime := time.Now()

	var filtered sync.Map

	globalIDsMap, err := s.dataProvider.GetIds(preparationsContext)
	if err != nil {
		return nil, err
	}
	s.logFilteredIDs(&filtered, ids, globalIDsMap, "Global ID filtering")
	ids = s.extractKeys(globalIDsMap)

	brandsMap, err := s.filterBrands(preparationsContext, ids)
	if err != nil {
		return nil, err
	}
	s.logFilteredIDs(&filtered, ids, brandsMap, "Brand filtering")

	ids = s.extractKeys(brandsMap)

	appellationsMap, err := s.filterAppellations(preparationsContext, ids)
	if err != nil {
		return nil, err
	}
	s.logFilteredIDs(&filtered, ids, appellationsMap, "Appellation filtering")

	ids = s.extractKeys(appellationsMap)

	descriptionsMap, err := s.filterDescriptions(preparationsContext, ids)
	if err != nil {
		return nil, err
	}
	s.logFilteredIDs(&filtered, ids, descriptionsMap, "Description filtering")

	ids = s.extractKeys(descriptionsMap)

	barcodesMap, err := s.filterBarcodes(preparationsContext, ids)
	if err != nil {
		return nil, err
	}
	s.logFilteredIDs(&filtered, ids, barcodesMap, "Barcodes filtering")

	ids = s.extractKeys(barcodesMap)

	filtered.Range(func(key, value any) bool {
		preparationLogger.Log("Excluded ID: %v, Reason: %v", key, value)
		return true
	})

	totalTime := time.Since(startTime)
	preparationLogger.Log("Time spent on preparations : %s", totalTime.String())

	return ids, nil
}

func (s *CardService) PrepareAndUpload(ctx context.Context, ids []int) (interface{}, error) {

	preparationsContext, cancel := context.WithTimeout(ctx, time.Minute*3)
	defer cancel()

	preparingResult, err := s.Prepare(ctx, ids)
	if err != nil {
		return nil, err
	}

	ids, ok := preparingResult.([]int)
	if !ok {
		return nil, fmt.Errorf("PrepareAndUpload returned invalid result")
	}

	appellations, err := s.filterAppellations(preparationsContext, ids)
	if err != nil {
		return nil, err
	}

	descriptions, err := s.filterDescriptions(preparationsContext, ids)
	if err != nil {
		return nil, err
	}

	brands, err := s.filterBrands(preparationsContext, ids)
	if err != nil {
		return nil, err
	}

	prices, err := s.filterPrice(preparationsContext, ids)
	if err != nil {
		return nil, err
	}

	var cards []request.CreateCardRequestData
	for _, id := range ids {
		card, err := s.cardBuilder.WithBrand(brands[id]).
			WithDescription(descriptions[id]).
			WithTitle(appellations[id]).
			WithVendorCode(fmt.Sprintf("id-%d-%d", id, s.WbIdentity.Code)).
			WithPrice(prices[id] * 2).
			Build()
		if err != nil {
			return nil, err
		}
		cards = append(cards, *card.(*request.CreateCardRequestData))
	}
	return cards, nil
}

func (s *CardService) SendToServerModels(models interface{}) ([]byte, int, error) {
	return s.sendToServer(uploadCardsUrl, models)
}

func (s *CardService) sendToServer(url string, models interface{}) ([]byte, int, error) {
	log.Printf("Sending models to server...")

	requestBody, err := json.Marshal(models)
	if err != nil {
		return nil, 500, fmt.Errorf("failed to marshal update request: %w", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(requestBody))
	if err != nil {
		return nil, 500, fmt.Errorf("failed to create update request: %w", err)
	}

	requestBodySize := len(requestBody)
	requestBodySizeMB := float64(requestBodySize) / (1 << 20)
	log.Printf("Request Body Size: %d bytes (%.2f MB)\n", requestBodySize, requestBodySizeMB)

	req.Header.Set("Content-Type", "application/json")
	s.SetApiKey(req)

	log.Printf("Sending request body: %v", string(requestBody))
	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, 500, fmt.Errorf("failed to upload models: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("failed to read response body: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		var errorResponse map[string]interface{}
		if err := json.Unmarshal(bodyBytes, &errorResponse); err != nil {
			return nil, resp.StatusCode, fmt.Errorf("failed to unmarshal error response: %w", err)
		}

		errorDetails, err := json.MarshalIndent(errorResponse, "", "  ")
		if err != nil {
			return nil, resp.StatusCode, fmt.Errorf("failed to format error details: %w", err)
		}

		log.Printf("Upload failed with status: %d, error details: %s", resp.StatusCode, string(errorDetails))
		return bodyBytes, resp.StatusCode, fmt.Errorf("upload failed with status: %d", resp.StatusCode)
	}

	return bodyBytes, resp.StatusCode, nil
}

func (s *CardService) filterAppellations(ctx context.Context, ids []int) (map[int]string, error) {
	if len(ids) == 0 {
		return nil, fmt.Errorf("expected ids len greater than 0")
	}

	filterContext, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	return filter_utils.FilterData[string, string](
		ids,
		func(ids []int) (map[int]string, error) {
			return s.dataProvider.GetAppellations(filterContext)
		},
		func(id int, appellation string) (string, bool, error) {
			if appellation == "" {
				return "", false, nil
			}
			return s.textService.ClearAndReduce(appellation, 60), true, nil
		},
	)
}

func (s *CardService) filterDescriptions(ctx context.Context, ids []int) (map[int]string, error) {
	if len(ids) == 0 {
		return nil, fmt.Errorf("expected ids len greater than 0")
	}

	filterContext, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	return filter_utils.FilterData[string, string](
		ids,
		func(ids []int) (map[int]string, error) {
			return s.dataProvider.GetDescriptions(filterContext)
		},
		func(id int, desc string) (string, bool, error) {
			if desc == "" {
				return "", false, nil
			}
			return s.textService.ClearAndReduce(desc, 2000), true, nil
		},
	)
}

func (s *CardService) filterBrands(ctx context.Context, ids []int) (map[int]string, error) {
	if len(ids) == 0 {
		return nil, fmt.Errorf("expected ids len greater than 0")
	}

	filterContext, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	return filter_utils.FilterData(
		ids,
		func(ids []int) (map[int]string, error) {
			brands, err := s.dataProvider.GetBrands(filterContext)
			if err != nil {
				return nil, err
			}

			return brands, nil
		},
		func(id int, brand string) (string, bool, error) {
			if s.brandService.IsBanned(brand) || brand == "" {
				return "", false, nil
			}
			return brand, true, nil
		},
	)
}

func (s *CardService) filterPrice(ctx context.Context, ids []int) (map[int]int, error) {
	if len(ids) == 0 {
		return nil, fmt.Errorf("expected ids len greater than 0")
	}

	filterContext, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	return filter_utils.FilterData(
		ids,
		func(ids []int) (map[int]float64, error) {
			prices, err := s.dataProvider.GetPrices(filterContext)
			if err != nil {
				return nil, err
			}
			return prices, nil
		},
		func(id int, price float64) (int, bool, error) {
			if price <= 0 {
				return 0, false, nil
			}
			return int(price), true, nil
		})
}

func (s *CardService) filterBarcodes(ctx context.Context, ids []int) (map[int][]string, error) {
	if len(ids) == 0 {
		return nil, fmt.Errorf("expected ids len greater than 0")
	}

	filterContext, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	// тут была кристина ^_^
	return filter_utils.FilterData(
		ids,
		func(ids []int) (map[int][]string, error) {
			barcodes, err := s.dataProvider.GetBarcodes(filterContext)
			if err != nil {
				return nil, err
			}
			return barcodes, nil
		},
		func(id int, barcodes []string) ([]string, bool, error) {
			if len(barcodes) == 0 {
				return nil, false, fmt.Errorf("expected barcodes len greater than 0")
			}
			return barcodes, true, nil
		},
	)
}

func (s *CardService) filterSizes(ctx context.Context, ids []int) (map[int]interface{}, error) {
	filterContext, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	return filter_utils.FilterData(
		ids,
		func(ids []int) (map[int]interface{}, error) {
			sizes, err := s.dataProvider.GetSizes(filterContext)
			if err != nil {
				return nil, err
			}
			return sizes, nil
		},
		func(id int, size interface{}) (interface{}, bool, error) {
			return size, true, nil
		})
}

// logFilteredIDs записывает исключённые артикулы в sync.Map
func (s *CardService) logFilteredIDs(filtered *sync.Map, originalIDs []int, filteredMap any, reason string) {
	transformed, ok := filteredMap.(map[int]any)
	if !ok {
		return
	}

	originalSet := make(map[int]struct{})
	for _, id := range originalIDs {
		originalSet[id] = struct{}{}
	}

	for id := range originalSet {
		if _, exists := transformed[id]; !exists {
			filtered.Store(id, reason)
		}
	}
}

// extractKeys извлекает ключи из мапы
func (s *CardService) extractKeys(data any) []int {
	transformed, ok := data.(map[int]any)
	if !ok {
		return []int{}
	}

	keys := make([]int, 0, len(transformed))
	for k := range transformed {
		keys = append(keys, k)
	}
	return keys
}
