package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jobqueue/api/internal/api"
	"github.com/jobqueue/api/internal/config"
	"github.com/jobqueue/api/internal/domain"
	"github.com/jobqueue/api/internal/handler"
	"github.com/jobqueue/api/internal/queue"
	"github.com/jobqueue/api/internal/service"
	"github.com/jobqueue/api/internal/store"
	"github.com/jobqueue/api/internal/worker"
)

type integrationEnv struct {
	server *httptest.Server
	svc    *service.Service
	store  store.Store
	queue  *queue.Queue
	ctx    context.Context
	cancel context.CancelFunc
	pool   *worker.Pool
}

func startIntegration(t *testing.T, reg *handler.Registry, workerCount int) *integrationEnv {
	t.Helper()

	cfg := config.Config{
		DefaultMaxRetries: 3,
		DefaultTimeout:    2 * time.Second,
		BackoffBase:       20 * time.Millisecond,
		BackoffMax:        200 * time.Millisecond,
		SchedulerInterval: 20 * time.Millisecond,
		WorkerCount:       workerCount,
	}
	st := store.NewMemoryStore()
	q := queue.New()
	svc := service.New(cfg, st, q, reg)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	ctx, cancel := context.WithCancel(context.Background())

	go svc.RunScheduler(ctx)

	env := &integrationEnv{
		svc:    svc,
		store:  st,
		queue:  q,
		ctx:    ctx,
		cancel: cancel,
	}

	if workerCount > 0 {
		env.startWorkers(workerCount, logger)
	}

	t.Cleanup(func() {
		env.cancel()
		env.queue.Close()
		if env.pool != nil {
			env.pool.Wait()
		}
	})

	h := api.NewHandler(svc, logger)
	env.server = httptest.NewServer(api.NewRouter(h, logger))
	return env
}

func (e *integrationEnv) startWorkers(count int, logger *slog.Logger) {
	if e.pool != nil {
		return
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	}
	e.pool = worker.NewPool(count, e.queue, e.svc, logger)
	e.pool.Start(e.ctx)
}

func waitForJobState(t *testing.T, baseURL, jobID string, want domain.JobState, timeout time.Duration) domain.JobResponse {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(baseURL + "/jobs/" + jobID)
		if err != nil {
			t.Fatal(err)
		}
		var job domain.JobResponse
		_ = json.NewDecoder(resp.Body).Decode(&job)
		resp.Body.Close()
		if job.State == want {
			return job
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("job %s did not reach state %s within %v", jobID, want, timeout)
	return domain.JobResponse{}
}

func TestIntegrationSubmitProcessSuccess(t *testing.T) {
	env := startIntegration(t, handler.NewRegistry(), 2)
	defer env.server.Close()

	resp, err := http.Post(env.server.URL+"/jobs", "application/json",
		bytes.NewBufferString(`{"type":"echo","payload":{"msg":"integration"}}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var submitted domain.JobResponse
	if err := json.NewDecoder(resp.Body).Decode(&submitted); err != nil {
		t.Fatal(err)
	}

	job := waitForJobState(t, env.server.URL, submitted.ID, domain.StateSucceeded, 2*time.Second)
	if job.AttemptCount != 1 {
		t.Fatalf("attempts: got %d", job.AttemptCount)
	}
	if !bytes.Contains(job.Result, []byte("integration")) {
		t.Fatalf("result: %s", job.Result)
	}
}

func TestIntegrationRetryThenDeadLetter(t *testing.T) {
	var calls atomic.Int32
	reg := handler.NewRegistry()
	reg.Register("flaky", func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
		calls.Add(1)
		return nil, errors.New("fail")
	})

	env := startIntegration(t, reg, 1)
	defer env.server.Close()

	resp, err := http.Post(env.server.URL+"/jobs", "application/json",
		bytes.NewBufferString(`{"type":"flaky","payload":{},"max_retries":2}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var submitted domain.JobResponse
	_ = json.NewDecoder(resp.Body).Decode(&submitted)

	job := waitForJobState(t, env.server.URL, submitted.ID, domain.StateDeadLettered, 3*time.Second)
	if job.AttemptCount != 2 {
		t.Fatalf("attempts: got %d, want 2", job.AttemptCount)
	}
	if job.LastError != "fail" {
		t.Fatalf("last_error: got %q", job.LastError)
	}
	if calls.Load() != 2 {
		t.Fatalf("handler calls: got %d", calls.Load())
	}

	deadResp, _ := http.Get(env.server.URL + "/dead-letter")
	defer deadResp.Body.Close()
	var dead []domain.JobResponse
	_ = json.NewDecoder(deadResp.Body).Decode(&dead)
	if len(dead) != 1 {
		t.Fatalf("dead-letter count: got %d", len(dead))
	}
}

func TestIntegrationRetryThenSuccess(t *testing.T) {
	var calls atomic.Int32
	reg := handler.NewRegistry()
	reg.Register("recover", func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
		if calls.Add(1) == 1 {
			return nil, errors.New("temporary")
		}
		return json.RawMessage(`{"ok":true}`), nil
	})

	env := startIntegration(t, reg, 1)
	defer env.server.Close()

	resp, _ := http.Post(env.server.URL+"/jobs", "application/json",
		bytes.NewBufferString(`{"type":"recover","payload":{},"max_retries":3}`))
	defer resp.Body.Close()

	var submitted domain.JobResponse
	_ = json.NewDecoder(resp.Body).Decode(&submitted)

	job := waitForJobState(t, env.server.URL, submitted.ID, domain.StateSucceeded, 3*time.Second)
	if job.AttemptCount != 2 {
		t.Fatalf("attempts: got %d, want 2", job.AttemptCount)
	}
}

func TestIntegrationPriorityOrdering(t *testing.T) {
	var order []string
	var mu sync.Mutex

	reg := handler.NewRegistry()
	reg.Register("track", func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
		var p struct {
			Name string `json:"name"`
		}
		_ = json.Unmarshal(payload, &p)
		mu.Lock()
		order = append(order, p.Name)
		mu.Unlock()
		return json.RawMessage(`{}`), nil
	})

	// Start without workers so both jobs land in the queue before any dequeue
	env := startIntegration(t, reg, 0)
	defer env.server.Close()

	for _, body := range []string{
		`{"type":"track","payload":{"name":"low"},"priority":1}`,
		`{"type":"track","payload":{"name":"high"},"priority":100}`,
	} {
		resp, _ := http.Post(env.server.URL+"/jobs", "application/json", bytes.NewBufferString(body))
		resp.Body.Close()
	}

	env.startWorkers(1, nil)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(order)
		mu.Unlock()
		if n == 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 {
		t.Fatalf("expected 2 processed jobs, got %d", len(order))
	}
	if order[0] != "high" || order[1] != "low" {
		t.Fatalf("order: got %v, want [high low]", order)
	}
}

func TestIntegrationCancelQueuedJob(t *testing.T) {
	env := startIntegration(t, handler.NewRegistry(), 0)
	defer env.server.Close()

	resp, _ := http.Post(env.server.URL+"/jobs", "application/json",
		bytes.NewBufferString(`{"type":"echo","payload":{}}`))
	var job domain.JobResponse
	_ = json.NewDecoder(resp.Body).Decode(&job)
	resp.Body.Close()

	req, _ := http.NewRequest(http.MethodPost, env.server.URL+"/jobs/"+job.ID+"/cancel", nil)
	cancelResp, _ := http.DefaultClient.Do(req)
	defer cancelResp.Body.Close()

	if cancelResp.StatusCode != http.StatusOK {
		t.Fatalf("cancel status: %d", cancelResp.StatusCode)
	}

	got := waitForJobState(t, env.server.URL, job.ID, domain.StateCancelled, 500*time.Millisecond)
	if got.State != domain.StateCancelled {
		t.Fatalf("state: got %s", got.State)
	}
}

func TestIntegrationQueueDepthUnderLoad(t *testing.T) {
	reg := handler.NewRegistry()
	reg.Register("slow", func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(200 * time.Millisecond):
			return json.RawMessage(`{}`), nil
		}
	})

	env := startIntegration(t, reg, 1)
	defer env.server.Close()

	for i := 0; i < 5; i++ {
		resp, _ := http.Post(env.server.URL+"/jobs", "application/json",
			bytes.NewBufferString(`{"type":"slow","payload":{}}`))
		resp.Body.Close()
	}

	depthResp, _ := http.Get(env.server.URL + "/queue/depth")
	defer depthResp.Body.Close()
	var depth domain.QueueDepth
	_ = json.NewDecoder(depthResp.Body).Decode(&depth)

	if depth.TotalActive < 2 {
		t.Fatalf("expected backlog under load, got %+v", depth)
	}
}

func TestIntegrationDrainPendingJobs(t *testing.T) {
	env := startIntegration(t, handler.NewRegistry(), 0)
	defer env.server.Close()

	for i := 0; i < 3; i++ {
		resp, _ := http.Post(env.server.URL+"/jobs", "application/json",
			bytes.NewBufferString(`{"type":"echo","payload":{}}`))
		resp.Body.Close()
	}

	req, _ := http.NewRequest(http.MethodPost, env.server.URL+"/queue/drain", nil)
	drainResp, _ := http.DefaultClient.Do(req)
	defer drainResp.Body.Close()

	var result domain.DrainResult
	_ = json.NewDecoder(drainResp.Body).Decode(&result)
	if result.Cancelled != 3 {
		t.Fatalf("cancelled: got %d, want 3", result.Cancelled)
	}

	depthResp, _ := http.Get(env.server.URL + "/queue/depth")
	defer depthResp.Body.Close()
	var depth domain.QueueDepth
	_ = json.NewDecoder(depthResp.Body).Decode(&depth)
	if depth.Pending != 0 || depth.Delayed != 0 {
		t.Fatalf("queue not empty: %+v", depth)
	}
}

func TestIntegrationHandlerTimeout(t *testing.T) {
	reg := handler.NewRegistry()
	reg.Register("hang", func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(10 * time.Second):
			return json.RawMessage(`{}`), nil
		}
	})

	cfg := config.Config{
		DefaultMaxRetries: 1,
		DefaultTimeout:    50 * time.Millisecond,
		BackoffBase:       10 * time.Millisecond,
		BackoffMax:        50 * time.Millisecond,
		SchedulerInterval: 10 * time.Millisecond,
	}
	st := store.NewMemoryStore()
	q := queue.New()
	svc := service.New(cfg, st, q, reg)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go svc.RunScheduler(ctx)
	pool := worker.NewPool(1, q, svc, logger)
	pool.Start(ctx)
	t.Cleanup(func() {
		cancel()
		q.Close()
		pool.Wait()
	})

	h := api.NewHandler(svc, logger)
	server := httptest.NewServer(api.NewRouter(h, logger))
	defer server.Close()

	resp, _ := http.Post(server.URL+"/jobs", "application/json",
		bytes.NewBufferString(`{"type":"hang","payload":{},"max_retries":1}`))
	var submitted domain.JobResponse
	_ = json.NewDecoder(resp.Body).Decode(&submitted)
	resp.Body.Close()

	job := waitForJobState(t, server.URL, submitted.ID, domain.StateDeadLettered, 3*time.Second)
	if job.AttemptCount != 1 {
		t.Fatalf("attempts: got %d", job.AttemptCount)
	}
}
