package app

import (
	"context"
	"database/sql"
	"golang.org/x/time/rate"
	"gomarketplace_api/config"
	"gomarketplace_api/internal/migrations/marketplaces/wb"
	request2 "gomarketplace_api/internal/wildberries/business/models/dto/request"
	"gomarketplace_api/internal/wildberries/business/models/dto/response"
	"gomarketplace_api/internal/wildberries/business/services"
	get2 "gomarketplace_api/internal/wildberries/business/services/get"
	"gomarketplace_api/internal/wildberries/business/services/parse"
	update2 "gomarketplace_api/internal/wildberries/business/services/update"
	"gomarketplace_api/internal/wildberries/business/services/update/operations/domain"
	"gomarketplace_api/internal/wildberries/business/services/update/providers_impl"
	clients2 "gomarketplace_api/internal/wildberries/pkg/clients"
	"gomarketplace_api/internal/wildberries/storage"
	"gomarketplace_api/pkg/business/service"
	"gomarketplace_api/pkg/dbconnect"
	"gomarketplace_api/pkg/dbconnect/migration"
	"gomarketplace_api/pkg/logger"
	"io"
	"log"
	"time"
)

type WildberriesServer struct {
	cardUpdateService *update2.CardUpdateService
	dbconnect.Database
	config.WildberriesConfig
	log    logger.Logger
	writer io.Writer
}

func NewWbServer(connector dbconnect.Database, wbConfig config.WildberriesConfig, writer io.Writer) *WildberriesServer {
	_log := logger.NewLogger(writer, "[WildberriesServer]")
	return &WildberriesServer{Database: connector, WildberriesConfig: wbConfig, log: _log, writer: writer}
}

func (s *WildberriesServer) Run() {
	var authEngine services.AuthEngine
	authEngine = services.NewBearerAuth(s.ApiKey)

	var db, err = s.Connect()
	if err != nil {
		s.log.Log("Error connecting to PostgreSQL: %s\n", err)
	}
	defer db.Close()

	searchConfig := get2.Config{
		WorkerCount:    5,
		MaxRetries:     get2.MaxRetries,
		RetryInterval:  get2.RetryInterval,
		RequestTimeout: get2.RequestTimeout,
	}

	nomenclatureUpdGet := get2.NewSearchEngine(db, authEngine, s.writer, searchConfig)
	s.cardUpdateService = update2.NewCardUpdateService(
		nomenclatureUpdGet,
		service.NewTextService(),
		"http://localhost:8081",
		authEngine,
		s.log,
		parse.NewBrandServiceWildberries(s.WbBanned.BannedBrands),
		s.WbValues,
	)

	migrationApply := []migration.MigrationInterface{
		&wb.CreateWBSchema{},
		&wb.CreateWBCategoriesTable{},
		&wb.CreateWBProductsTable{},
		&wb.WBCharacteristics{},
		&wb.WBNomenclatures{},
		&wb.WBCardsActual{},
		&wb.WBNomenclaturesHistory{},
		&wb.WBChanges{},
	}

	for _, _migration := range migrationApply {
		if err := _migration.UpMigration(db); err != nil {
			log.Fatalf("Migration failed: %v", err)
		}
	}
	s.log.Log("WB migrations applied successfully!")

	//err = s.loadCharcs(db, authEngine)
	//if err != nil {
	//	s.log.FatalLog("Error loading Charcs: %w\n", err)
	//}

	//categories := []int{2865, 5071, 5073, 5067}
	//for _, categoryID := range categories {
	//	s.uploadProducts(authEngine, categoryID)
	//	time.Sleep(30 * time.Second)
	//}
	//

	//
	//time.Sleep(5 * time.Second)
	//
	_, err = s.cardUpdateService.UpdateDBNomenclatures(request2.Settings{
		Sort:   request2.Sort{Ascending: false},
		Filter: request2.Filter{WithPhoto: -1, TagIDs: []int{}, TextSearch: "", AllowedCategoriesOnly: true, ObjectIDs: []int{}, Brands: []string{}, ImtID: 0},
		Cursor: request2.Cursor{Limit: 10000},
	}, "")
	if err != nil {
		return
	}

	//count, err := s.cardUpdateService.CheckSearchEngine(request2.Settings{
	//	Sort:   request2.Sort{Ascending: false},
	//	Filter: request2.Filter{WithPhoto: -1, TagIDs: []int{}, TextSearch: "", AllowedCategoriesOnly: true, ObjectIDs: []int{}, Brands: []string{}, ImtID: 0},
	//	Cursor: request2.Cursor{Limit: 10000},
	//}, "")
	//if err != nil {
	//	return
	//}
	//
	//log.Printf("Search engine found %d nm's", count)

	//s.updateMediaFiles("http://localhost:8081")
}

func (s *WildberriesServer) updateMediaFiles(wsClientUrl string) {
	s.log.SetPrefix("[ Media Updater ] ")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var authEngine services.AuthEngine
	authEngine = services.NewBearerAuth(s.ApiKey)

	var db, err = s.Connect()
	if err != nil {
		s.log.Log("Error connecting to PostgreSQL: %s\n", err)
	}
	defer db.Close()

	client, err := clients2.NewWServiceClient(wsClientUrl, s.log)
	if err != nil {
		log.Fatal(err)
	}

	searchConfig := get2.Config{
		WorkerCount:    5,
		MaxRetries:     get2.MaxRetries,
		RetryInterval:  get2.RetryInterval,
		RequestTimeout: get2.RequestTimeout,
	}

	nmSearchEngine := get2.NewSearchEngine(db, authEngine, s.log, searchConfig)

	updateOp := domain.NewMediaUpdateOperation(client)
	_, err = updateOp.MediaUrls(ctx, false)
	if err != nil {
		return
	}
	limiter := rate.NewLimiter(rate.Every(time.Minute/50), 15)

	mediaUpateService := update2.NewUpdateService(
		updateOp,
		domain.MediaUploadURL,
		limiter,
		5,
		authEngine)

	nomenclatureChan := make(chan response.Nomenclature)
	go func() {

		settings := request2.Settings{
			Sort:   request2.Sort{Ascending: false},
			Filter: request2.Filter{WithPhoto: -1, TagIDs: []int{}, TextSearch: "", AllowedCategoriesOnly: true, ObjectIDs: []int{}, Brands: []string{}, ImtID: 0},
			Cursor: request2.Cursor{Limit: 10000}}
		locale := ""

		if err := nmSearchEngine.GetNomenclaturesWithLimitConcurrentlyPutIntoChannel(ctx, settings, locale, nomenclatureChan); err != nil {
			log.Printf("Error fetching nomenclatures concurrently: %s", err)
		}
	}()

	updatedCount, err := mediaUpateService.Update(ctx, nomenclatureChan)
	if err != nil {
		log.Fatalf("Ошибка обновления: %s", err)
	}

	metrics := mediaUpateService.Metrics()

	log.Printf("Обновлено карточек: %d", updatedCount)
	log.Printf("Обработано карточек: %d, с ошибками: %d",
		metrics.ProcessedCount.Load(), metrics.ErroredNomenclatures.Load())
}

func (s *WildberriesServer) updateNames() interface{} {
	s.log.Log("Naming updater ")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	updateMedia, err := s.cardUpdateService.UpdateCardNaming(ctx, request2.Settings{
		Sort:   request2.Sort{Ascending: false},
		Filter: request2.Filter{WithPhoto: 1, TagIDs: []int{}, TextSearch: "", AllowedCategoriesOnly: true, ObjectIDs: []int{}, Brands: []string{}, ImtID: 0},
		Cursor: request2.Cursor{Limit: 10000},
	})

	if err != nil {
		s.log.FatalLog("Error updating nomenclatures: %s\n", err)
	}
	s.log.Log("\n\n\nUpdated %d nomenclatures\n", updateMedia)

	return updateMedia
}

func (s *WildberriesServer) uploadProducts(ctx context.Context, auth services.AuthEngine, categoryID int) interface{} {
	wsUrl := "http://localhost:8081"
	textService := service.NewTextService()
	db, err := s.Database.Connect()
	if err != nil {
		s.log.FatalLog("Error connecting to PostgreSQL: %s\n", err)
	}

	searchConfig := get2.Config{
		WorkerCount:    get2.WorkerCount,
		MaxRetries:     get2.MaxRetries,
		RetryInterval:  get2.RetryInterval,
		RequestTimeout: get2.RequestTimeout,
	}

	engine := get2.NewSearchEngine(db, auth, s.writer, searchConfig)
	repo := storage.NewNomenclatureRepository(db)

	nmService := update2.NewNomenclatureService(*engine, *repo)

	// TODO : вынести

	whProvider := providers_impl.NewWbWholesalerAdapter(wsUrl, s.log)

	cardService := update2.NewCardService(textService, s.writer, s.WildberriesConfig, whProvider)

	accuracy := float32(0.3)
	result, err := nmService.GetSetOfUncreatedItemsWithCategories(accuracy, true, categoryID)

	ids := make([]int, len(result))
	for k, _ := range result {
		ids = append(ids, k)
	}

	uploadContext, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()

	resultIDs, err := cardService.PrepareAndUpload(uploadContext, ids)
	if err != nil {
		return nil
	}

	req := []request2.CreateCardRequestWrapper{}
	for i, v := range resultIDs.([]request2.CreateCardRequestData) {
		req = append(req, request2.CreateCardRequestWrapper{})
		req[i].Variants = append(req[i].Variants, v)
		req[i].SubjectID = categoryID
	}

	_, _, err = cardService.SendToServerModels(req)
	if err != nil {
		return nil
	}

	s.log.Log("Category %d. Total count of items : %d", categoryID, len(resultIDs.([]request2.CreateCardRequestData)))
	return struct{}{}
}

func (s *WildberriesServer) loadCharcs(db *sql.DB, engine services.AuthEngine) error {
	charcUpdate := get2.NewUpdateDBCharcs(db, *get2.NewCharacteristicService(engine))
	catsRepo := get2.NewDBCategories(db)
	cats, err := catsRepo.Categories()
	if err != nil {
		s.log.FatalLog(err.Error())
		return err
	}
	catIDs := make([]int, len(cats))
	for i, v := range cats {
		catIDs[i] = v.SubjectID
	}
	_, err = charcUpdate.UpdateDBCharcs(catIDs)
	if err != nil {
		s.log.FatalLog(err.Error())
		return err
	}
	return nil
}

func (s *WildberriesServer) updateByCategoryId() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	updateAppellation, err := s.cardUpdateService.UpdateCardNaming(ctx, request2.Settings{
		Sort:   request2.Sort{Ascending: false},
		Filter: request2.Filter{WithPhoto: -1, TagIDs: []int{}, TextSearch: "", AllowedCategoriesOnly: true, ObjectIDs: []int{5067}, Brands: []string{}, ImtID: 0},
		Cursor: request2.Cursor{Limit: 1500},
	})
	updatePackages, err := s.cardUpdateService.UpdateCardPackages(request2.Settings{
		Sort:   request2.Sort{Ascending: false},
		Filter: request2.Filter{WithPhoto: -1, TagIDs: []int{}, TextSearch: "", AllowedCategoriesOnly: true, ObjectIDs: []int{5067}, Brands: []string{}, ImtID: 0},
		Cursor: request2.Cursor{Limit: 1500},
	})
	if err != nil {
		s.log.FatalLog("Error updating nomenclatures: %s\n", err)
	}
	s.log.Log(
		"Updated appellations for %d nomenclatures\n"+
			"Updated media for %d nomenclatures\n"+
			"Updated packages for %d nomenclatures", updateAppellation, updatePackages)
	return nil
}
