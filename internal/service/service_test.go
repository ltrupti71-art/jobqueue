package service

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jobqueue/api/internal/config"
	"github.com/jobqueue/api/internal/domain"
	"github.com/jobqueue/api/internal/handler"
	"github.com/jobqueue/api/internal/queue"
	"github.com/jobqueue/api/internal/store"
)

func testConfig() config.Config {
	return config.Config{
		DefaultMaxRetries: 3,
		DefaultTimeout:    5 * time.Second,
		DefaultPriority:   0,
		BackoffBase:       10 * time.Millisecond,
		BackoffMax:        100 * time.Millisecond,
		SchedulerInterval: 10 * time.Millisecond,
	}
}

func newTestService(t *testing.T, reg *handler.Registry) (*Service, queue.Queue) {
	t.Helper()
	q := queue.NewMemory()
	svc := New(testConfig(), store.NewMemoryStore(), q, reg)
	return svc, q
}

func TestSubmitAndProcessSuccess(t *testing.T) {
	reg := handler.NewRegistry()
	svc, q := newTestService(t, reg)
	ctx := context.Background()

	job, err := svc.Submit(ctx, domain.SubmitJobRequest{
		Type:    "echo",
		Payload: json.RawMessage(`{"hello":"world"}`),
	})
	if err != nil {
		t.Fatal(err)
	}

	id, ok, _ := q.Dequeue(ctx)
	if !ok || id != job.ID {
		t.Fatalf("expected job %s in queue, got %q", job.ID, id)
	}

	svc.ProcessJob(ctx, id)

	got, err := svc.Get(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != domain.StateSucceeded {
		t.Fatalf("state: got %s, want succeeded", got.State)
	}
	if got.AttemptCount != 1 {
		t.Fatalf("attempts: got %d, want 1", got.AttemptCount)
	}
}

func TestRetryThenDeadLetter(t *testing.T) {
	var calls atomic.Int32
	reg := handler.NewRegistry()
	reg.Register("fail", func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
		calls.Add(1)
		return nil, errors.New("transient error")
	})

	svc, q := newTestService(t, reg)
	ctx := context.Background()

	job, err := svc.Submit(ctx, domain.SubmitJobRequest{
		Type:       "fail",
		Payload:    json.RawMessage(`{}`),
		MaxRetries: 2,
	})
	if err != nil {
		t.Fatal(err)
	}

	for attempt := 0; attempt < 2; attempt++ {
		if attempt > 0 {
			q.PromoteDue(time.Now().Add(time.Minute))
		}
		id, ok, _ := q.Dequeue(ctx)
		if !ok {
			t.Fatalf("attempt %d: expected job in queue", attempt)
		}
		svc.ProcessJob(ctx, id)
	}

	got, err := svc.Get(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != domain.StateDeadLettered {
		t.Fatalf("state: got %s, want dead_lettered", got.State)
	}
	if got.AttemptCount != 2 {
		t.Fatalf("attempts: got %d, want 2", got.AttemptCount)
	}
	if got.LastError != "transient error" {
		t.Fatalf("last error: got %q", got.LastError)
	}
	if calls.Load() != 2 {
		t.Fatalf("handler calls: got %d, want 2", calls.Load())
	}
}

func TestCancelQueuedJob(t *testing.T) {
	reg := handler.NewRegistry()
	svc, _ := newTestService(t, reg)
	ctx := context.Background()

	job, err := svc.Submit(ctx, domain.SubmitJobRequest{
		Type:    "echo",
		Payload: json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatal(err)
	}

	cancelled, err := svc.Cancel(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.State != domain.StateCancelled {
		t.Fatalf("state: got %s", cancelled.State)
	}
}

func TestDrain(t *testing.T) {
	reg := handler.NewRegistry()
	svc, _ := newTestService(t, reg)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		_, err := svc.Submit(ctx, domain.SubmitJobRequest{
			Type:    "echo",
			Payload: json.RawMessage(`{}`),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	result, err := svc.Drain(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Cancelled != 3 {
		t.Fatalf("cancelled: got %d, want 3", result.Cancelled)
	}

	depth, err := svc.QueueDepth(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if depth.Pending != 0 || depth.Delayed != 0 {
		t.Fatalf("queue not empty: %+v", depth)
	}
}

func TestQueueDepth(t *testing.T) {
	reg := handler.NewRegistry()
	svc, _ := newTestService(t, reg)
	ctx := context.Background()

	_, _ = svc.Submit(ctx, domain.SubmitJobRequest{Type: "echo", Payload: json.RawMessage(`{}`)})
	_, _ = svc.Submit(ctx, domain.SubmitJobRequest{Type: "echo", Payload: json.RawMessage(`{}`)})

	depth, err := svc.QueueDepth(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if depth.Pending != 2 {
		t.Fatalf("pending: got %d, want 2", depth.Pending)
	}
}

func TestSubmitInvalidType(t *testing.T) {
	svc, _ := newTestService(t, handler.NewRegistry())
	_, err := svc.Submit(context.Background(), domain.SubmitJobRequest{
		Type:    "nonexistent",
		Payload: json.RawMessage(`{}`),
	})
	if !errors.Is(err, ErrInvalidJobType) {
		t.Fatalf("got %v, want ErrInvalidJobType", err)
	}
}

func TestSubmitInvalidTimeout(t *testing.T) {
	svc, _ := newTestService(t, handler.NewRegistry())
	_, err := svc.Submit(context.Background(), domain.SubmitJobRequest{
		Type:              "echo",
		Payload:           json.RawMessage(`{}`),
		TimeoutPerAttempt: "not-a-duration",
	})
	if err == nil {
		t.Fatal("expected invalid timeout error")
	}
}

func TestSubmitUsesDefaults(t *testing.T) {
	svc, _ := newTestService(t, handler.NewRegistry())
	job, err := svc.Submit(context.Background(), domain.SubmitJobRequest{
		Type:    "echo",
		Payload: json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if job.MaxRetries != 3 {
		t.Fatalf("max_retries: got %d", job.MaxRetries)
	}
	if job.Priority != 0 {
		t.Fatalf("priority: got %d", job.Priority)
	}
}

func TestSubmitCustomParams(t *testing.T) {
	svc, _ := newTestService(t, handler.NewRegistry())
	job, err := svc.Submit(context.Background(), domain.SubmitJobRequest{
		Type:              "echo",
		Payload:           json.RawMessage(`{}`),
		Priority:          99,
		MaxRetries:        7,
		TimeoutPerAttempt: "15s",
	})
	if err != nil {
		t.Fatal(err)
	}
	if job.Priority != 99 || job.MaxRetries != 7 || job.TimeoutPerAttempt != 15*time.Second {
		t.Fatalf("got priority=%d retries=%d timeout=%v", job.Priority, job.MaxRetries, job.TimeoutPerAttempt)
	}
}

func TestCancelNotFound(t *testing.T) {
	svc, _ := newTestService(t, handler.NewRegistry())
	_, err := svc.Cancel(context.Background(), "missing-id")
	if !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("got %v", err)
	}
}

func TestCancelNonQueuedJob(t *testing.T) {
	svc, q := newTestService(t, handler.NewRegistry())
	ctx := context.Background()

	job, _ := svc.Submit(ctx, domain.SubmitJobRequest{Type: "echo", Payload: json.RawMessage(`{}`)})
	id, _, _ := q.Dequeue(ctx)
	svc.ProcessJob(ctx, id)

	_, err := svc.Cancel(ctx, job.ID)
	if !errors.Is(err, ErrJobNotCancellable) {
		t.Fatalf("got %v", err)
	}
}

func TestCancelAlreadyCancelled(t *testing.T) {
	svc, _ := newTestService(t, handler.NewRegistry())
	ctx := context.Background()

	job, _ := svc.Submit(ctx, domain.SubmitJobRequest{Type: "echo", Payload: json.RawMessage(`{}`)})
	_, _ = svc.Cancel(ctx, job.ID)

	_, err := svc.Cancel(ctx, job.ID)
	if !errors.Is(err, ErrJobNotCancellable) {
		t.Fatalf("got %v", err)
	}
}

func TestProcessJobNotFound(t *testing.T) {
	svc, _ := newTestService(t, handler.NewRegistry())
	svc.ProcessJob(context.Background(), "missing") // should not panic
}

func TestProcessJobStateGuard(t *testing.T) {
	svc, q := newTestService(t, handler.NewRegistry())
	ctx := context.Background()

	job, _ := svc.Submit(ctx, domain.SubmitJobRequest{Type: "echo", Payload: json.RawMessage(`{}`)})
	id, _, _ := q.Dequeue(ctx)
	svc.ProcessJob(ctx, id)

	// Re-enqueue same ID manually; ProcessJob should no-op because state != queued
	q.Enqueue(job.ID, 1, time.Now())
	svc.ProcessJob(ctx, job.ID)

	got, _ := svc.Get(ctx, job.ID)
	if got.State != domain.StateSucceeded {
		t.Fatalf("state should remain succeeded, got %s", got.State)
	}
	if got.AttemptCount != 1 {
		t.Fatalf("attempt count should not increase, got %d", got.AttemptCount)
	}
}

func TestProcessJobUnknownHandler(t *testing.T) {
	svc, q := newTestService(t, handler.NewRegistry())
	ctx := context.Background()
	st := store.NewMemoryStore()
	now := time.Now().UTC()

	// Manually insert job with unknown type
	job := &domain.Job{
		ID: "orphan", Type: "ghost", Payload: json.RawMessage(`{}`),
		State: domain.StateQueued, MaxRetries: 1, TimeoutPerAttempt: time.Second,
		AvailableAt: now, CreatedAt: now, UpdatedAt: now,
	}
	_ = st.Create(ctx, job)
	svc = New(testConfig(), st, q, handler.NewRegistry())
	q.Enqueue("orphan", 0, now)

	svc.ProcessJob(ctx, "orphan")
	got, _ := svc.Get(ctx, "orphan")
	if got.State != domain.StateDeadLettered {
		t.Fatalf("state: got %s, want dead_lettered", got.State)
	}
}

func TestProcessJobTimeout(t *testing.T) {
	reg := handler.NewRegistry()
	reg.Register("slow", func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Second):
			return json.RawMessage(`{}`), nil
		}
	})

	cfg := testConfig()
	cfg.DefaultTimeout = 50 * time.Millisecond
	q := queue.NewMemory()
	svc := New(cfg, store.NewMemoryStore(), q, reg)
	ctx := context.Background()

	job, _ := svc.Submit(ctx, domain.SubmitJobRequest{
		Type: "slow", Payload: json.RawMessage(`{}`), MaxRetries: 1,
	})
	id, _, _ := q.Dequeue(ctx)
	svc.ProcessJob(ctx, id)

	got, _ := svc.Get(ctx, job.ID)
	if got.State != domain.StateDeadLettered {
		t.Fatalf("state: got %s, want dead_lettered after timeout", got.State)
	}
}

func TestRetryThenSuccess(t *testing.T) {
	var calls atomic.Int32
	reg := handler.NewRegistry()
	reg.Register("flaky", func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
		if calls.Add(1) == 1 {
			return nil, errors.New("first attempt fails")
		}
		return json.RawMessage(`{"recovered":true}`), nil
	})

	svc, q := newTestService(t, reg)
	ctx := context.Background()

	job, _ := svc.Submit(ctx, domain.SubmitJobRequest{
		Type: "flaky", Payload: json.RawMessage(`{}`), MaxRetries: 3,
	})

	// First attempt fails
	id, _, _ := q.Dequeue(ctx)
	svc.ProcessJob(ctx, id)

	mid, _ := svc.Get(ctx, job.ID)
	if mid.State != domain.StateQueued {
		t.Fatalf("after first fail: got state %s", mid.State)
	}
	if mid.LastError == "" {
		t.Fatal("expected last_error set")
	}

	// Second attempt succeeds
	q.PromoteDue(time.Now().Add(time.Minute))
	id, _, _ = q.Dequeue(ctx)
	svc.ProcessJob(ctx, id)

	got, _ := svc.Get(ctx, job.ID)
	if got.State != domain.StateSucceeded {
		t.Fatalf("state: got %s", got.State)
	}
	if got.AttemptCount != 2 {
		t.Fatalf("attempts: got %d", got.AttemptCount)
	}
}

func TestListDeadLettered(t *testing.T) {
	reg := handler.NewRegistry()
	reg.Register("fail", func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
		return nil, errors.New("boom")
	})
	svc, q := newTestService(t, reg)
	ctx := context.Background()

	job, _ := svc.Submit(ctx, domain.SubmitJobRequest{
		Type: "fail", Payload: json.RawMessage(`{}`), MaxRetries: 1,
	})
	id, _, _ := q.Dequeue(ctx)
	svc.ProcessJob(ctx, id)

	dead, err := svc.ListDeadLettered(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(dead) != 1 || dead[0].ID != job.ID {
		t.Fatalf("got %d dead-lettered jobs", len(dead))
	}
}

func TestQueueDepthRunningAndDead(t *testing.T) {
	reg := handler.NewRegistry()
	reg.Register("hang", func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	cfg := testConfig()
	cfg.DefaultTimeout = time.Hour
	q := queue.NewMemory()
	svc := New(cfg, store.NewMemoryStore(), q, reg)
	ctx := context.Background()

	_, _ = svc.Submit(ctx, domain.SubmitJobRequest{Type: "hang", Payload: json.RawMessage(`{}`)})

	// Simulate running state
	id, _, _ := q.Dequeue(ctx)
	go svc.ProcessJob(ctx, id)
	time.Sleep(20 * time.Millisecond)

	depth, err := svc.QueueDepth(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if depth.Running != 1 {
		t.Fatalf("running: got %d, want 1", depth.Running)
	}
}

func TestRunSchedulerPromotesDelayed(t *testing.T) {
	svc, q := newTestService(t, handler.NewRegistry())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go svc.RunScheduler(ctx)

	future := time.Now().Add(30 * time.Millisecond)
	q.Enqueue("delayed-job", 1, future)

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		id, ok, _ := q.Dequeue(context.Background())
		if ok && id == "delayed-job" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("scheduler did not promote delayed job in time")
}

func TestGetNotFound(t *testing.T) {
	svc, _ := newTestService(t, handler.NewRegistry())
	_, err := svc.Get(context.Background(), "missing")
	if !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("got %v", err)
	}
}
