package handler

// Посещаемость по часам за последние N часов — карточка «Посещаемость» на главной платформы.
//
// Владелец 26.09: «шаг 1 час, вмещает 10 часов, крепится к часовому поясу». Окно — N целых часов
// по часам пояса магазина/смотрящего, последний — текущий (неполный): при 09:40 и N = 10 это
// 00:00…09:00. Часы режутся по местному времени, а не по UTC: у поясов со сдвигом в полчаса
// (Азия/Калькутта +5:30) граница местного часа — не граница часа UTC.
//
// Посетители часа — уникальные за этот час; итог окна — уникальные за всё окно (правило 1,
// period_uniques.go), не сумма часовых. Трафик — только люди, окно — по времени события.

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	hourlyDefaultHours = 10
	hourlyMaxHours     = 48
	hourlyDefaultTZ    = "Europe/Moscow"
)

type HourlyTrafficRequest struct {
	ShopID   string `json:"shopId"`
	TenantID string `json:"tenantId"`
	Hours    int    `json:"hours"`
	TZ       string `json:"tz"`
}

type HourlyTrafficPoint struct {
	// Hour — начало часа по местному времени пояса: "2026-09-26T09:00".
	Hour     string `json:"hour"`
	Visitors int64  `json:"unique_visitors"`
}

type HourlyTrafficResponse struct {
	TZ           string               `json:"tz"`
	Hours        int                  `json:"hours"`
	Points       []HourlyTrafficPoint `json:"points"`
	Visitors     int64                `json:"unique_visitors"`
	VisitorsPrev int64                `json:"unique_visitors_prev"`
}

// hourlyTrafficSQL — уникальные посетители по часам окна [$1, $2); номер часа — от начала окна,
// поэтому часы совпадают с местными часами пояса, в котором окно построено.
var hourlyTrafficSQL = `
	SELECT floor((extract(epoch from e.event_timestamp) - extract(epoch from $1::timestamptz)) / 3600)::int,
		COUNT(DISTINCT e.visitor_id)
	FROM bronze.events e
	WHERE e.shop_id = $3 AND ` + humanOnly("e") + ` AND ` + eventWindow("e", "$1", "$2") + `
	GROUP BY 1`

func HandleHourlyTraffic(ctx context.Context, pool *pgxpool.Pool, payload json.RawMessage) (any, error) {
	var req HourlyTrafficRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}
	if req.ShopID == "" {
		return nil, fmt.Errorf("shopId is required")
	}

	hours := clampHours(req.Hours)
	loc, tz := resolveZone(req.TZ)
	start, end := hourlyWindow(timeNow(), loc, hours)
	prevStart := start.Add(-time.Duration(hours) * time.Hour)

	byHour, err := queryHourlyVisitors(ctx, pool, req.ShopID, start, end)
	if err != nil {
		return nil, err
	}
	cur, err := queryPeriodTraffic(ctx, pool, req.ShopID, start, end)
	if err != nil {
		return nil, err
	}
	prev, err := queryPeriodTraffic(ctx, pool, req.ShopID, prevStart, start)
	if err != nil {
		return nil, err
	}

	return HourlyTrafficResponse{
		TZ:           tz,
		Hours:        hours,
		Points:       buildHourlyPoints(start, hours, loc, byHour),
		Visitors:     cur.visitors,
		VisitorsPrev: prev.visitors,
	}, nil
}

// clampHours — пусто или ноль → 10 часов; больше 48 не отдаём.
func clampHours(h int) int {
	if h <= 0 {
		return hourlyDefaultHours
	}
	if h > hourlyMaxHours {
		return hourlyMaxHours
	}
	return h
}

// resolveZone — пояс по имени IANA; пустое или неизвестное имя → Москва.
func resolveZone(name string) (*time.Location, string) {
	if loc, err := time.LoadLocation(name); name != "" && err == nil {
		return loc, loc.String()
	}
	loc, _ := time.LoadLocation(hourlyDefaultTZ)
	return loc, hourlyDefaultTZ
}

// hourlyWindow — [начало, конец) из hours целых местных часов, последний — текущий час.
// Начало часа берётся по местным часам (time.Date в поясе), не Truncate: Truncate режет по UTC.
func hourlyWindow(now time.Time, loc *time.Location, hours int) (time.Time, time.Time) {
	local := now.In(loc)
	currentHour := time.Date(local.Year(), local.Month(), local.Day(), local.Hour(), 0, 0, 0, loc)
	end := currentHour.Add(time.Hour)
	return end.Add(-time.Duration(hours) * time.Hour), end
}

// buildHourlyPoints — ровно hours точек по возрастанию, пустые часы — нулём. Без базы: для тестов.
func buildHourlyPoints(start time.Time, hours int, loc *time.Location, byHour map[int]int64) []HourlyTrafficPoint {
	points := make([]HourlyTrafficPoint, 0, hours)
	for i := 0; i < hours; i++ {
		at := start.Add(time.Duration(i) * time.Hour).In(loc)
		points = append(points, HourlyTrafficPoint{Hour: at.Format("2006-01-02T15:04"), Visitors: byHour[i]})
	}
	return points
}

func queryHourlyVisitors(ctx context.Context, pool *pgxpool.Pool, shopID string, start, end time.Time) (map[int]int64, error) {
	rows, err := pool.Query(ctx, hourlyTrafficSQL, start, end, shopID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	byHour := make(map[int]int64)
	for rows.Next() {
		var idx int
		var visitors int64
		if err := rows.Scan(&idx, &visitors); err != nil {
			return nil, err
		}
		byHour[idx] = visitors
	}
	return byHour, rows.Err()
}
