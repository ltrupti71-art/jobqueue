package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

type Handler func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error)

type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

func NewRegistry() *Registry {
	r := &Registry{handlers: make(map[string]Handler)}
	r.Register("echo", EchoHandler)
	r.Register("sleep", SleepHandler)
	return r
}

func (r *Registry) Register(name string, h Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[name] = h
}

func (r *Registry) Get(name string) (Handler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[name]
	return h, ok
}

func (r *Registry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.handlers))
	for name := range r.handlers {
		names = append(names, name)
	}
	return names
}

func EchoHandler(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	return payload, nil
}

func SleepHandler(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	var req struct {
		Duration string `json:"duration"`
	}
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, fmt.Errorf("invalid sleep payload: %w", err)
	}
	d, err := time.ParseDuration(req.Duration)
	if err != nil {
		return nil, fmt.Errorf("invalid duration: %w", err)
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(d):
	}
	result, _ := json.Marshal(map[string]string{"slept": req.Duration})
	return result, nil
}
