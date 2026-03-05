package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	Port         int
	DatabaseURL  string
	RabbitMQURL  string
	CORSOrigins  []string
	BatchSize    int
	FlushSeconds int
}

func Load() (*Config, error) {
	port := 3120
	if v := os.Getenv("PORT"); v != "" {
		p, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid PORT: %w", err)
		}
		port = p
	}

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
	}

	rmqURL := os.Getenv("RABBITMQ_URL")
	if rmqURL == "" {
		return nil, fmt.Errorf("RABBITMQ_URL is required")
	}

	origins := []string{"*"}
	if v := os.Getenv("CORS_ORIGINS"); v != "" {
		origins = strings.Split(v, ",")
		for i := range origins {
			origins[i] = strings.TrimSpace(origins[i])
		}
	}

	batchSize := 1000
	if v := os.Getenv("BATCH_SIZE"); v != "" {
		b, err := strconv.Atoi(v)
		if err == nil && b > 0 {
			batchSize = b
		}
	}

	flushSec := 5
	if v := os.Getenv("FLUSH_SECONDS"); v != "" {
		f, err := strconv.Atoi(v)
		if err == nil && f > 0 {
			flushSec = f
		}
	}

	return &Config{
		Port:         port,
		DatabaseURL:  dbURL,
		RabbitMQURL:  rmqURL,
		CORSOrigins:  origins,
		BatchSize:    batchSize,
		FlushSeconds: flushSec,
	}, nil
}
