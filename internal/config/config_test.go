package config

import (
	"os"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	// Clear env vars that could affect test
	for _, key := range []string{"ADDR", "WORKER_COUNT", "DEFAULT_MAX_RETRIES", "DEFAULT_TIMEOUT", "BACKOFF_BASE"} {
		os.Unsetenv(key)
	}

	cfg := Load()
	if cfg.Addr != ":8080" {
		t.Fatalf("addr: got %s", cfg.Addr)
	}
	if cfg.WorkerCount != 4 {
		t.Fatalf("workers: got %d", cfg.WorkerCount)
	}
	if cfg.DefaultMaxRetries != 3 {
		t.Fatalf("retries: got %d", cfg.DefaultMaxRetries)
	}
	if cfg.DefaultTimeout != 30*time.Second {
		t.Fatalf("timeout: got %v", cfg.DefaultTimeout)
	}
}

func TestLoadFromEnv(t *testing.T) {
	t.Setenv("ADDR", ":9090")
	t.Setenv("WORKER_COUNT", "16")
	t.Setenv("DEFAULT_TIMEOUT", "45s")
	t.Setenv("BACKOFF_BASE", "2s")

	cfg := Load()
	if cfg.Addr != ":9090" {
		t.Fatalf("addr: got %s", cfg.Addr)
	}
	if cfg.WorkerCount != 16 {
		t.Fatalf("workers: got %d", cfg.WorkerCount)
	}
	if cfg.DefaultTimeout != 45*time.Second {
		t.Fatalf("timeout: got %v", cfg.DefaultTimeout)
	}
	if cfg.BackoffBase != 2*time.Second {
		t.Fatalf("backoff: got %v", cfg.BackoffBase)
	}
}

func TestLoadInvalidEnvUsesFallback(t *testing.T) {
	t.Setenv("WORKER_COUNT", "not-a-number")
	t.Setenv("DEFAULT_TIMEOUT", "invalid")

	cfg := Load()
	if cfg.WorkerCount != 4 {
		t.Fatalf("workers: got %d, want default 4", cfg.WorkerCount)
	}
	if cfg.DefaultTimeout != 30*time.Second {
		t.Fatalf("timeout: got %v, want default", cfg.DefaultTimeout)
	}
}
