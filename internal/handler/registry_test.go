package handler

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestRegistryRegisterGetList(t *testing.T) {
	r := NewRegistry()
	r.Register("custom", EchoHandler)

	if _, ok := r.Get("custom"); !ok {
		t.Fatal("expected custom handler")
	}
	if _, ok := r.Get("nonexistent"); ok {
		t.Fatal("expected missing handler")
	}

	names := r.List()
	if len(names) < 3 {
		t.Fatalf("expected at least echo, sleep, custom; got %v", names)
	}
}

func TestEchoHandler(t *testing.T) {
	payload := json.RawMessage(`{"key":"value"}`)
	got, err := EchoHandler(context.Background(), payload)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatalf("got %s, want %s", got, payload)
	}
}

func TestSleepHandlerSuccess(t *testing.T) {
	payload := json.RawMessage(`{"duration":"10ms"}`)
	got, err := SleepHandler(context.Background(), payload)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(got), "10ms") {
		t.Fatalf("unexpected result: %s", got)
	}
}

func TestSleepHandlerInvalidPayload(t *testing.T) {
	_, err := SleepHandler(context.Background(), json.RawMessage(`not-json`))
	if err == nil {
		t.Fatal("expected error for invalid payload")
	}
}

func TestSleepHandlerInvalidDuration(t *testing.T) {
	_, err := SleepHandler(context.Background(), json.RawMessage(`{"duration":"not-a-duration"}`))
	if err == nil {
		t.Fatal("expected error for invalid duration")
	}
}

func TestSleepHandlerContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := SleepHandler(ctx, json.RawMessage(`{"duration":"10s"}`))
	if err == nil {
		t.Fatal("expected context error")
	}
}

func TestSleepHandlerRespectsTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err := SleepHandler(ctx, json.RawMessage(`{"duration":"5s"}`))
	if err == nil {
		t.Fatal("expected timeout/cancel error")
	}
}
