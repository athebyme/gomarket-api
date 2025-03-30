package providers_impl

import (
	"context"
	"fmt"
	"gomarketplace_api/internal/suppliers/wholesaler/pkg/requests"
	"gomarketplace_api/internal/wildberries/pkg/clients"
	"io"
	"strconv"
	"time"
)

type WbWholesalerAdapter struct {
	client *clients.WServiceClient
}

func NewWbWholesalerAdapter(hostUrl string, writer io.Writer) *WbWholesalerAdapter {
	client, err := clients.NewWServiceClient(hostUrl, writer)
	if err != nil {
		return nil
	}
	return &WbWholesalerAdapter{
		client: client,
	}
}

func (wa *WbWholesalerAdapter) GetIds(ctx context.Context) (map[int]struct{}, error) {
	searchContext, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	idsRaw, err := wa.client.FetcherChain.Fetch(searchContext, "globalids", nil)
	if err != nil {
		return nil, err
	}

	globalIDs, ok := idsRaw.([]int)
	if !ok {
		return nil, fmt.Errorf("unexpected type from Fetch: %T", idsRaw)
	}

	globalIDsMap := make(map[int]struct{}, len(globalIDs))
	for _, id := range globalIDs {
		globalIDsMap[id] = struct{}{}
	}

	idSet := make(map[int]struct{})
	if len(globalIDs) == 0 {
		return globalIDsMap, nil
	}

	return idSet, nil
}

func (wa *WbWholesalerAdapter) GetBrands(ctx context.Context) (map[int]string, error) {
	searchContext, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	result, err := wa.client.FetcherChain.Fetch(searchContext, "brands", requests.BrandRequest{
		FilterRequest: requests.FilterRequest{ProductIDs: []int{}},
	})
	if err != nil {
		return nil, err
	}

	fetchedMap, ok := result.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected type from Fetch: %T", result)
	}

	brandMap := make(map[int]string)
	for key, value := range fetchedMap {
		id, err := strconv.Atoi(key)
		if err != nil {
			return nil, fmt.Errorf("invalid product ID: %v", key)
		}
		brandMap[id], ok = value.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected type from Fetch: %T", value)
		}
	}

	return brandMap, nil
}

func (wa *WbWholesalerAdapter) GetAppellations(ctx context.Context) (map[int]string, error) {
	searchContext, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	appellationsRaw, err := wa.client.FetcherChain.Fetch(searchContext, "appellations", requests.AppellationsRequest{FilterRequest: requests.FilterRequest{
		ProductIDs: []int{},
	}})

	if err != nil {
		return nil, err
	}

	appellations, ok := appellationsRaw.(map[int]string)
	if !ok {
		return nil, fmt.Errorf("unexpected type from Fetch: %T", appellationsRaw)
	}

	return appellations, nil
}

func (wa *WbWholesalerAdapter) GetDescriptions(ctx context.Context) (map[int]string, error) {
	searchContext, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	descriptionsRaw, err := wa.client.FetcherChain.Fetch(searchContext, "descriptions", requests.DescriptionRequest{FilterRequest: requests.FilterRequest{ProductIDs: []int{}}, IncludeEmptyDescriptions: false})
	if err != nil {
		return nil, err
	}

	descriptions, ok := descriptionsRaw.(map[int]string)
	if !ok {
		return nil, fmt.Errorf("unexpected type from Fetch: %T", descriptionsRaw)
	}

	return descriptions, nil
}

func (wa *WbWholesalerAdapter) GetPrices(ctx context.Context) (map[int]float64, error) {
	searchContext, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	pricesRaw, err := wa.client.FetcherChain.Fetch(searchContext, "prices", requests.PriceRequest{FilterRequest: requests.FilterRequest{
		ProductIDs: []int{},
	}})
	if err != nil {
		return nil, err
	}

	pricesInterface, ok := pricesRaw.(map[int]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected type from Fetch: %T", pricesRaw)
	}

	resPrices := make(map[int]float64, len(pricesInterface))

	for id, price := range pricesInterface {
		switch value := price.(type) {
		case map[string]interface{}:
			zValue, ok := value["Z"].(float64)
			if !ok {
				return nil, fmt.Errorf("key 'Z' is missing or not a float64 for product id %d", id)
			}
			resPrices[id] = zValue * 1.15
		case float64:
			resPrices[id] = value
		case float32:
			resPrices[id] = float64(value)
		default:
			return nil, fmt.Errorf("unsupported type of price for product id %d: %T", id, price)
		}
	}

	return resPrices, nil
}
func (wa *WbWholesalerAdapter) GetBarcodes(ctx context.Context) (map[int][]string, error) {
	searchContext, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	barcodesRaw, err := wa.client.FetcherChain.Fetch(searchContext, "barcodes", requests.BarcodeRequest{
		FilterRequest: requests.FilterRequest{ProductIDs: []int{}},
	})
	if err != nil {
		return nil, err
	}

	barcodesMap, ok := barcodesRaw.(map[int]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected type from Fetch: %T", barcodesRaw)
	}

	result := make(map[int][]string, len(barcodesMap))

	for id, barcode := range barcodesMap {
		switch v := barcode.(type) {
		case string:
			result[id] = []string{v}
		case []string:
			result[id] = v
		case []interface{}:
			strSlice := make([]string, 0, len(v))
			for i, item := range v {
				str, ok := item.(string)
				if !ok {
					return nil, fmt.Errorf("element at index %d for product id %d is not a string", i, id)
				}
				strSlice = append(strSlice, str)
			}
			result[id] = strSlice
		default:
			return nil, fmt.Errorf("unsupported type of barcode for product id %d: %T", id, barcode)
		}
	}

	return result, nil
}

func (wa *WbWholesalerAdapter) GetSizes(ctx context.Context) (map[int]interface{}, error) {
	searchContext, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	sizesRaw, err := wa.client.FetcherChain.Fetch(searchContext, "sizes", requests.SizeRequest{
		FilterRequest: requests.FilterRequest{
			ProductIDs: []int{},
		}})
	if err != nil {
		return nil, err
	}

	sizes, ok := sizesRaw.(map[int]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected type from Fetch: %T", sizesRaw)
	}

	return sizes, nil
}
