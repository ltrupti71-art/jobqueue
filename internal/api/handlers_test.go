package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jobqueue/api/internal/config"
	"github.com/jobqueue/api/internal/domain"
	"github.com/jobqueue/api/internal/handler"
	"github.com/jobqueue/api/internal/queue"
	"github.com/jobqueue/api/internal/service"
	"github.com/jobqueue/api/internal/store"
)

type testEnv struct {
	server *httptest.Server
	svc    *service.Service
	store  store.Store
	queue  queue.Queue
}

func newTestEnv(t *testing.T, reg *handler.Registry) testEnv {
	t.Helper()
	cfg := config.Config{
		DefaultMaxRetries: 3,
		DefaultTimeout:    5 * time.Second,
		BackoffBase:       10 * time.Millisecond,
		BackoffMax:        100 * time.Millisecond,
	}
	st := store.NewMemoryStore()
	q := queue.NewMemory()
	svc := service.New(cfg, st, q, store.NewMemoryScheduleStore(), reg, nil)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	h := NewHandler(svc, logger)
	return testEnv{
		server: httptest.NewServer(NewRouter(h, logger)),
		svc:    svc,
		store:  st,
		queue:  q,
	}
}

func postJSON(t *testing.T, url string, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func decodeJSON(t *testing.T, r io.Reader, v any) {
	t.Helper()
	if err := json.NewDecoder(r).Decode(v); err != nil {
		t.Fatal(err)
	}
}

func TestSubmitAndGetJob(t *testing.T) {
	env := newTestEnv(t, handler.NewRegistry())
	defer env.server.Close()

	resp := postJSON(t, env.server.URL+"/jobs",
		`{"type":"echo","payload":{"msg":"hi"},"priority":5,"max_retries":2,"timeout_per_attempt":"10s"}`)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status: got %d, want 202", resp.StatusCode)
	}

	var submitted domain.JobResponse
	decodeJSON(t, resp.Body, &submitted)
	if submitted.ID == "" || submitted.State != domain.StateQueued {
		t.Fatalf("unexpected submit response: %+v", submitted)
	}

	getResp, err := http.Get(env.server.URL + "/jobs/" + submitted.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer getResp.Body.Close()

	var fetched domain.JobResponse
	decodeJSON(t, getResp.Body, &fetched)
	if fetched.ID != submitted.ID {
		t.Fatalf("id mismatch")
	}
}

func TestSubmitInvalidJSON(t *testing.T) {
	env := newTestEnv(t, handler.NewRegistry())
	defer env.server.Close()

	resp := postJSON(t, env.server.URL+"/jobs", `{invalid`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d", resp.StatusCode)
	}
}

func TestSubmitMissingType(t *testing.T) {
	env := newTestEnv(t, handler.NewRegistry())
	defer env.server.Close()

	resp := postJSON(t, env.server.URL+"/jobs", `{"payload":{}}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d", resp.StatusCode)
	}
}

func TestSubmitUnknownJobType(t *testing.T) {
	env := newTestEnv(t, handler.NewRegistry())
	defer env.server.Close()

	resp := postJSON(t, env.server.URL+"/jobs", `{"type":"unknown","payload":{}}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d", resp.StatusCode)
	}
}

func TestSubmitDefaultEmptyPayload(t *testing.T) {
	env := newTestEnv(t, handler.NewRegistry())
	defer env.server.Close()

	resp := postJSON(t, env.server.URL+"/jobs", `{"type":"echo"}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status: got %d", resp.StatusCode)
	}
}

func TestGetJobNotFound(t *testing.T) {
	env := newTestEnv(t, handler.NewRegistry())
	defer env.server.Close()

	resp, err := http.Get(env.server.URL + "/jobs/nonexistent-id")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status: got %d", resp.StatusCode)
	}
}

func TestCancelJob(t *testing.T) {
	env := newTestEnv(t, handler.NewRegistry())
	defer env.server.Close()

	submitResp := postJSON(t, env.server.URL+"/jobs", `{"type":"echo","payload":{}}`)
	var job domain.JobResponse
	decodeJSON(t, submitResp.Body, &job)
	submitResp.Body.Close()

	req, _ := http.NewRequest(http.MethodPost, env.server.URL+"/jobs/"+job.ID+"/cancel", nil)
	cancelResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer cancelResp.Body.Close()

	if cancelResp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d", cancelResp.StatusCode)
	}
	var cancelled domain.JobResponse
	decodeJSON(t, cancelResp.Body, &cancelled)
	if cancelled.State != domain.StateCancelled {
		t.Fatalf("state: got %s", cancelled.State)
	}
}

func TestCancelJobNotFound(t *testing.T) {
	env := newTestEnv(t, handler.NewRegistry())
	defer env.server.Close()

	req, _ := http.NewRequest(http.MethodPost, env.server.URL+"/jobs/missing/cancel", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status: got %d", resp.StatusCode)
	}
}

func TestCancelJobConflict(t *testing.T) {
	env := newTestEnv(t, handler.NewRegistry())
	defer env.server.Close()

	submitResp := postJSON(t, env.server.URL+"/jobs", `{"type":"echo","payload":{}}`)
	var job domain.JobResponse
	decodeJSON(t, submitResp.Body, &job)
	submitResp.Body.Close()

	id, _, _ := env.queue.Dequeue(context.Background())
	env.svc.ProcessJob(context.Background(), id)

	req, _ := http.NewRequest(http.MethodPost, env.server.URL+"/jobs/"+job.ID+"/cancel", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status: got %d, want 409", resp.StatusCode)
	}
}

func TestQueueDepthEndpoint(t *testing.T) {
	env := newTestEnv(t, handler.NewRegistry())
	defer env.server.Close()

	_, _ = http.Post(env.server.URL+"/jobs", "application/json", bytes.NewBufferString(`{"type":"echo","payload":{}}`))

	resp, err := http.Get(env.server.URL + "/queue/depth")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var depth domain.QueueDepth
	decodeJSON(t, resp.Body, &depth)
	if depth.Pending < 1 {
		t.Fatalf("expected pending jobs, got %+v", depth)
	}
}

func TestDrainEndpoint(t *testing.T) {
	env := newTestEnv(t, handler.NewRegistry())
	defer env.server.Close()

	_, _ = http.Post(env.server.URL+"/jobs", "application/json", bytes.NewBufferString(`{"type":"echo","payload":{}}`))

	req, _ := http.NewRequest(http.MethodPost, env.server.URL+"/queue/drain", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var result domain.DrainResult
	decodeJSON(t, resp.Body, &result)
	if result.Cancelled != 1 {
		t.Fatalf("cancelled: got %d, want 1", result.Cancelled)
	}
}

func TestListDeadLetterEndpoint(t *testing.T) {
	reg := handler.NewRegistry()
	reg.Register("fail", func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
		return nil, errors.New("always fails")
	})
	env := newTestEnv(t, reg)
	defer env.server.Close()

	submitResp := postJSON(t, env.server.URL+"/jobs", `{"type":"fail","payload":{},"max_retries":1}`)
	var job domain.JobResponse
	decodeJSON(t, submitResp.Body, &job)
	submitResp.Body.Close()

	id, _, _ := env.queue.Dequeue(context.Background())
	env.svc.ProcessJob(context.Background(), id)

	resp, err := http.Get(env.server.URL + "/dead-letter")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var dead []domain.JobResponse
	decodeJSON(t, resp.Body, &dead)
	if len(dead) != 1 || dead[0].ID != job.ID {
		t.Fatalf("expected 1 dead-lettered job, got %d", len(dead))
	}
}

func TestHealthEndpoint(t *testing.T) {
	env := newTestEnv(t, handler.NewRegistry())
	defer env.server.Close()

	resp, err := http.Get(env.server.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d", resp.StatusCode)
	}
}

func TestSubmitInvalidTimeout(t *testing.T) {
	env := newTestEnv(t, handler.NewRegistry())
	defer env.server.Close()

	resp := postJSON(t, env.server.URL+"/jobs", `{"type":"echo","timeout_per_attempt":"bad"}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400", resp.StatusCode)
	}
}

func TestLoggingMiddleware(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	Logging(logger)(next).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/test", nil))

	if !bytes.Contains(buf.Bytes(), []byte("request")) {
		t.Fatalf("expected log output, got: %s", buf.String())
	}
}

func TestRecoveryMiddleware(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	panicHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("test panic")
	})
	handler := Recovery(logger)(panicHandler)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status: got %d", rec.Code)
	}
}
