package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	Addr              string
	DatabaseURL       string
	WorkerCount       int
	DefaultMaxRetries int
	DefaultTimeout    time.Duration
	DefaultPriority   int
	BackoffBase       time.Duration
	BackoffMax        time.Duration
	SchedulerInterval time.Duration
	ShutdownTimeout   time.Duration
}

func Load() Config {
	addr := envOr("ADDR", "")
	if addr == "" {
		addr = ":" + envOr("PORT", "8080")
	}
	return Config{
		Addr:              addr,
		DatabaseURL:       envOr("DATABASE_URL", ""),
		WorkerCount:       envIntOr("WORKER_COUNT", 4),
		DefaultMaxRetries: envIntOr("DEFAULT_MAX_RETRIES", 3),
		DefaultTimeout:    envDurationOr("DEFAULT_TIMEOUT", 30*time.Second),
		DefaultPriority:   envIntOr("DEFAULT_PRIORITY", 0),
		BackoffBase:       envDurationOr("BACKOFF_BASE", time.Second),
		BackoffMax:        envDurationOr("BACKOFF_MAX", 5*time.Minute),
		SchedulerInterval: envDurationOr("SCHEDULER_INTERVAL", 100*time.Millisecond),
		ShutdownTimeout:   envDurationOr("SHUTDOWN_TIMEOUT", 30*time.Second),
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envIntOr(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func envDurationOr(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
