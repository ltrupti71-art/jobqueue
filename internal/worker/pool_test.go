package worker_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jobqueue/api/internal/config"
	"github.com/jobqueue/api/internal/domain"
	"github.com/jobqueue/api/internal/handler"
	"github.com/jobqueue/api/internal/queue"
	"github.com/jobqueue/api/internal/service"
	"github.com/jobqueue/api/internal/store"
	"github.com/jobqueue/api/internal/worker"
)

type countingProcessor struct {
	called atomic.Int32
}

func (p *countingProcessor) ProcessJob(ctx context.Context, jobID string) {
	p.called.Add(1)
}

func TestPoolProcessesJobs(t *testing.T) {
	reg := handler.NewRegistry()
	cfg := config.Config{
		DefaultMaxRetries: 3,
		DefaultTimeout:    5 * time.Second,
		BackoffBase:       10 * time.Millisecond,
		BackoffMax:        50 * time.Millisecond,
	}
	st := store.NewMemoryStore()
	q := queue.NewMemory()
	svc := service.New(cfg, st, q, reg)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool := worker.NewPool(2, q, svc, logger)
	pool.Start(ctx)
	t.Cleanup(func() {
		cancel()
		q.Close()
		pool.Wait()
	})

	_, err := svc.Submit(ctx, domain.SubmitJobRequest{
		Type: "echo", Payload: json.RawMessage(`{"done":true}`),
	})
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		jobs, _ := st.ListByState(ctx, domain.StateSucceeded)
		if len(jobs) == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("worker pool did not process job in time")
}

func TestPoolStopWaitsForWorkers(t *testing.T) {
	q := queue.NewMemory()
	proc := &countingProcessor{}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	ctx, cancel := context.WithCancel(context.Background())
	pool := worker.NewPool(1, q, proc, logger)
	pool.Start(ctx)

	cancel()
	q.Close()
	pool.Wait()
}

func TestPoolMultipleWorkersConcurrent(t *testing.T) {
	reg := handler.NewRegistry()
	reg.Register("sleep", func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(50 * time.Millisecond):
			return json.RawMessage(`{}`), nil
		}
	})

	cfg := config.Config{DefaultMaxRetries: 1, DefaultTimeout: 5 * time.Second}
	st := store.NewMemoryStore()
	q := queue.NewMemory()
	svc := service.New(cfg, st, q, reg)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool := worker.NewPool(4, q, svc, logger)
	pool.Start(ctx)
	t.Cleanup(func() {
		cancel()
		q.Close()
		pool.Wait()
	})

	for i := 0; i < 4; i++ {
		_, _ = svc.Submit(ctx, domain.SubmitJobRequest{
			Type: "sleep", Payload: json.RawMessage(`{"duration":"30ms"}`),
		})
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		succeeded, _ := st.CountByState(ctx, domain.StateSucceeded)
		if succeeded == 4 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("expected 4 jobs succeeded concurrently")
}
