package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/jobqueue/api/internal/api"
	"github.com/jobqueue/api/internal/config"
	"github.com/jobqueue/api/internal/db"
	"github.com/jobqueue/api/internal/handler"
	"github.com/jobqueue/api/internal/queue"
	"github.com/jobqueue/api/internal/service"
	"github.com/jobqueue/api/internal/store"
	"github.com/jobqueue/api/internal/worker"
)

func main() {
	cfg := config.Load()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	ctx := context.Background()

	var st store.Store
	var q queue.Queue

	if cfg.DatabaseURL != "" {
		pool, err := db.Connect(ctx, cfg.DatabaseURL)
		if err != nil {
			logger.Error("database connect failed", "error", err)
			os.Exit(1)
		}
		defer pool.Close()

		if err := db.Migrate(ctx, pool); err != nil {
			logger.Error("database migration failed", "error", err)
			os.Exit(1)
		}

		pgStore := store.NewPostgresStore(pool)
		if n, err := pgStore.ReconcileQueue(ctx); err != nil {
			logger.Error("queue reconciliation failed", "error", err)
		} else if n > 0 {
			logger.Info("reconciled orphaned queued jobs", "count", n)
		}

		st = pgStore
		q = queue.NewPostgres(pool)
		logger.Info("using PostgreSQL store and queue")
	} else {
		st = store.NewMemoryStore()
		q = queue.NewMemory()
		logger.Info("using in-memory store and queue (set DATABASE_URL for production)")
	}

	handlers := handler.NewRegistry()
	svc := service.New(cfg, st, q, handlers, logger)

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go svc.RunScheduler(runCtx)

	pool := worker.NewPool(cfg.WorkerCount, q, svc, logger)
	pool.Start(runCtx)

	h := api.NewHandler(svc, logger)
	router := api.NewRouter(h, logger)

	server := &http.Server{
		Addr:         cfg.Addr,
		Handler:      router,
		ReadTimeout:  10 * cfg.DefaultTimeout,
		WriteTimeout: 10 * cfg.DefaultTimeout,
		IdleTimeout:  120 * cfg.DefaultTimeout,
	}

	go func() {
		logger.Info("server starting", "addr", cfg.Addr, "workers", cfg.WorkerCount)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logger.Info("shutdown initiated")

	cancel()
	q.Close()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("server shutdown error", "error", err)
	}

	pool.Wait()
	logger.Info("shutdown complete")
}
