package handler

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type ChannelsRequest struct {
	ShopID   string `json:"shopId"`
	TenantID string `json:"tenantId"`
	Period   string `json:"period"`
}

type ChannelEntry struct {
	Source         string  `json:"source"`
	Sessions       int64   `json:"sessions"`
	Orders         int64   `json:"orders"`
	RevenueCents   int64   `json:"revenue_cents"`
	ConversionRate float64 `json:"conversion_rate"`
}

type ChannelTSEntry struct {
	Day      string         `json:"day"`
	Channels []ChannelEntry `json:"channels"`
}

type ChannelsResponse struct {
	Channels   []ChannelEntry   `json:"channels"`
	TimeSeries []ChannelTSEntry `json:"time_series"`
}

func HandleChannels(ctx context.Context, pool *pgxpool.Pool, payload json.RawMessage) (any, error) {
	var req ChannelsRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}
	if req.ShopID == "" || req.Period == "" {
		return nil, fmt.Errorf("shopId and period are required")
	}

	start, end := periodRange(req.Period, timeNow())

	// Aggregate channels
	rows, err := pool.Query(ctx, `
		SELECT
			channel,
			COALESCE(SUM(sessions), 0),
			COALESCE(SUM(orders), 0),
			COALESCE(SUM(revenue_cents), 0)
		FROM silver.daily_channel_attribution
		WHERE shop_id = $1 AND day >= $2 AND day < $3
		GROUP BY channel
		ORDER BY SUM(sessions) DESC
	`, req.ShopID, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var channels []ChannelEntry
	for rows.Next() {
		var c ChannelEntry
		if err := rows.Scan(&c.Source, &c.Sessions, &c.Orders, &c.RevenueCents); err != nil {
			return nil, err
		}
		if c.Sessions > 0 {
			c.ConversionRate = float64(c.Orders) / float64(c.Sessions) * 100
		}
		channels = append(channels, c)
	}

	// Time series by channel
	tsRows, err := pool.Query(ctx, `
		SELECT day::text, channel,
			COALESCE(SUM(sessions), 0),
			COALESCE(SUM(orders), 0),
			COALESCE(SUM(revenue_cents), 0)
		FROM silver.daily_channel_attribution
		WHERE shop_id = $1 AND day >= $2 AND day < $3
		GROUP BY day, channel
		ORDER BY day, SUM(sessions) DESC
	`, req.ShopID, start, end)
	if err != nil {
		return nil, err
	}
	defer tsRows.Close()

	tsMap := make(map[string][]ChannelEntry)
	var dayOrder []string
	for tsRows.Next() {
		var day string
		var c ChannelEntry
		if err := tsRows.Scan(&day, &c.Source, &c.Sessions, &c.Orders, &c.RevenueCents); err != nil {
			return nil, err
		}
		if _, exists := tsMap[day]; !exists {
			dayOrder = append(dayOrder, day)
		}
		tsMap[day] = append(tsMap[day], c)
	}

	var timeSeries []ChannelTSEntry
	for _, day := range dayOrder {
		timeSeries = append(timeSeries, ChannelTSEntry{Day: day, Channels: tsMap[day]})
	}

	return ChannelsResponse{
		Channels:   channels,
		TimeSeries: timeSeries,
	}, nil
}
