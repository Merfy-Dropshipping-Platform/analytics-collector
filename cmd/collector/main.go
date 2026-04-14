package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/merfy/analytics-collector/internal/config"
	"github.com/merfy/analytics-collector/internal/consumer"
	"github.com/merfy/analytics-collector/internal/db"
	"github.com/merfy/analytics-collector/internal/handler"
	"github.com/merfy/analytics-collector/internal/rabbitmq"
	"github.com/merfy/analytics-collector/internal/rpc"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	cfg, err := config.Load()
	if err != nil {
		slog.Error("load config", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Database
	pool, err := db.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("connect db", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	slog.Info("database connected")

	// RabbitMQ Publisher
	pub, err := rabbitmq.NewPublisher(cfg.RabbitMQURL)
	if err != nil {
		slog.Error("connect rabbitmq publisher", "error", err)
		os.Exit(1)
	}
	defer pub.Close()
	slog.Info("rabbitmq publisher connected")

	// Bronze Writer (consumer)
	bw, err := consumer.NewBronzeWriter(pool, cfg.RabbitMQURL, cfg.BatchSize, cfg.FlushSeconds)
	if err != nil {
		slog.Error("create bronze writer", "error", err)
		os.Exit(1)
	}
	defer bw.Close()

	if err := bw.Start(ctx); err != nil {
		slog.Error("start bronze writer", "error", err)
		os.Exit(1)
	}

	// RPC Server
	rpcSrv, err := rpc.NewServer(pool, cfg.RabbitMQURL)
	if err != nil {
		slog.Error("create rpc server", "error", err)
		os.Exit(1)
	}
	defer rpcSrv.Close()

	// Register RPC handlers
	rpcSrv.Register("analytics.dashboard", handler.HandleDashboard)
	rpcSrv.Register("analytics.traffic", handler.HandleTraffic)
	rpcSrv.Register("analytics.revenue", handler.HandleRevenue)
	rpcSrv.Register("analytics.funnel", handler.HandleFunnel)
	rpcSrv.Register("analytics.channels", handler.HandleChannels)
	rpcSrv.Register("analytics.top_products", handler.HandleTopProducts)
	rpcSrv.Register("analytics.pixels.list", handler.HandlePixelsList)
	rpcSrv.Register("analytics.pixels.create", handler.HandlePixelsCreate)
	rpcSrv.Register("analytics.pixels.update", handler.HandlePixelsUpdate)
	rpcSrv.Register("analytics.pixels.delete", handler.HandlePixelsDelete)

	if err := rpcSrv.Start(ctx); err != nil {
		slog.Error("start rpc server", "error", err)
		os.Exit(1)
	}

	// Maintenance loop: refresh matviews every 5 min, partition retention 30 days
	db.StartMaintenanceLoop(ctx, pool, 5*time.Minute, 30*24*time.Hour)

	// HTTP Server
	r := chi.NewRouter()
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(30 * time.Second))
	r.Use(cors.Handler(cors.Options{
		AllowOriginFunc: func(r *http.Request, origin string) bool {
			return true // accept all origins — tracker is a public beacon endpoint
		},
		AllowedMethods:   []string{"GET", "POST", "OPTIONS"},
		AllowedHeaders:   []string{"Content-Type"},
		AllowCredentials: true,
		MaxAge:           3600,
	}))

	collectHandler := handler.NewCollectHandler(pub)
	healthHandler := handler.NewHealthHandler()
	pixelsHTTPHandler := handler.NewPixelsHTTPHandler(pool)

	r.Post("/collect", collectHandler.ServeHTTP)
	r.Get("/health", healthHandler.ServeHTTP)
	r.Get("/pixels", pixelsHTTPHandler.ServeHTTP)

	// Static files (tracker.js, loader.js)
	r.Get("/tracker.js", handler.ServeStatic("/static/tracker.js", "application/javascript", 300))
	r.Get("/loader.js", handler.ServeStatic("/static/loader.js", "application/javascript", 300))

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Port),
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		slog.Info("server starting", "port", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	sig := <-sigCh
	slog.Info("shutdown signal received", "signal", sig)
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("server shutdown", "error", err)
	}

	slog.Info("server stopped")
}
