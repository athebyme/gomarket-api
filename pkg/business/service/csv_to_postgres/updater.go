package csv_to_postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"time"
)

// Updater - механизм обновления данных в базе данных на основе CSV документа.
// CSVProcessor должен содержать названия колонок оригинального CSV документа, либо названия колонок должны соответствовать колонкам
// документа по порядку.
// При этом DBUpdater значения columns должны содержать либо названия из оригинального документа, либо названия переименованные в случае,
// если потребовалось переименование в методе Execute.
type Updater struct {
	InfURL     string
	CSVURL     string
	LastModCol string

	Fetcher      Fetcher
	CSVProcessor *Processor
	DBUpdater    *PostgresUpdater
}

func NewUpdater(infURL, csvURL, lastModCol string, fetcher Fetcher, csvProc *Processor, dbUp *PostgresUpdater) *Updater {
	return &Updater{
		InfURL:       infURL,
		CSVURL:       csvURL,
		LastModCol:   lastModCol,
		Fetcher:      fetcher,
		CSVProcessor: csvProc,
		DBUpdater:    dbUp,
	}
}

func (u *Updater) SetNewInfUrl(url string) *Updater {
	if url != "" {
		u.InfURL = url
	}
	return u
}

func (u *Updater) SetNewCSVUrl(url string) *Updater {
	if url != "" {
		u.CSVURL = url
	}
	return u
}

func (u *Updater) SetNewLastModCol(col string) *Updater {
	if col != "" {
		u.LastModCol = col
	}
	return u
}

func (u *Updater) SetNewProcessor(proc *Processor) *Updater {
	if proc != nil && len(proc.columns) > 0 {
		u.CSVProcessor = proc
	}
	return u
}

func (u *Updater) SetNewUpdater(upd *PostgresUpdater) *Updater {
	if upd.DB != nil && len(upd.Columns) > 0 && upd.TableName != "" && upd.Schema != "" {
		u.DBUpdater = upd
	}
	return u
}

// fetchInfTime получает время последнего обновления из inf-файла.
// Обрабатывает ответ, в котором может быть несколько строк:
// первая строка — время в формате "2006-01-02 15:04:05",
// вторая строка — Unix-время (секунды).
func (u *Updater) fetchInfTime(ctx context.Context, timeFormat string) (time.Time, error) {
	body, err := u.Fetcher.Fetch(u.InfURL)
	if err != nil {
		return time.Time{}, err
	}
	defer body.Close()

	data, err := io.ReadAll(body)
	if err != nil {
		return time.Time{}, err
	}
	str := strings.TrimSpace(string(data))
	if str == "" {
		return time.Time{}, fmt.Errorf("inf-файл пустой")
	}

	lines := strings.Split(str, "\n")
	for i, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		if t, err := time.Parse(timeFormat, line); err == nil {
			return t, nil
		}

		if t, err := time.Parse("06.01.02 15:04:05", line); err == nil {
			return t, nil
		}

		if epochSec, err := strconv.ParseInt(line, 10, 64); err == nil {
			return time.Unix(epochSec, 0), nil
		}

		fmt.Printf("Не удалось распарсить строку %d: %q\n", i+1, line)
	}

	return time.Time{}, fmt.Errorf("не удалось определить время из inf-файла")
}
func (u *Updater) getStoredTime(ctx context.Context, db *sql.DB) (time.Time, error) {
	var storedTime time.Time
	err := db.QueryRowContext(ctx,
		"SELECT last_update FROM metadata WHERE key_name = $1",
		u.LastModCol).Scan(&storedTime)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), nil
		}
		return time.Time{}, err
	}
	return storedTime, nil
}

// Execute выполняет процесс обновления, если это необходимо.
func (u *Updater) Execute(ctx context.Context, renaming []string, db *sql.DB, timeFormat string, config UpdateConfig) error {
	if timeFormat == "" {
		timeFormat = "2006-01-02 15:04:05"
	}

	modTime, err := u.fetchInfTime(ctx, timeFormat)
	if err != nil {
		return err
	}
	storedTime, err := u.getStoredTime(ctx, db)
	if err != nil {
		return err
	}

	if modTime.After(storedTime) {
		log.Printf("Начало обновления данных с %s", u.CSVURL)
		body, err := u.Fetcher.Fetch(u.CSVURL)
		if err != nil {
			return err
		}
		defer body.Close()
		dbCtx, cancel := context.WithTimeout(ctx, 4*time.Minute)
		defer cancel()

		convertedData, headers, err := u.CSVProcessor.ProcessCSV(body, renaming)
		if err != nil {
			return err
		}

		u.DBUpdater.SetNewColumnNaming(headers)
		if err := u.DBUpdater.UpdateData(convertedData, dbCtx, config); err != nil {
			return err
		}
		_, err = db.ExecContext(ctx, `
			INSERT INTO metadata (key_name, value, last_update)
			VALUES ($1, $2, $3)
			ON CONFLICT (key_name) DO UPDATE SET last_update = EXCLUDED.last_update
		`, u.LastModCol, u.LastModCol, modTime)
		if err != nil {
			return fmt.Errorf("metadata update error: %w", err)
		}

		log.Printf("Обновление данных завершено успешно.")
	} else {
		log.Printf("Обновление не требуется, данные актуальны.")
	}

	return nil
}
