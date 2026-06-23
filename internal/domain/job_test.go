package domain

import (
	"encoding/json"
	"testing"
	"time"
)

func TestJobToResponse(t *testing.T) {
	started := time.Now().UTC()
	completed := started.Add(time.Second)
	job := &Job{
		ID:                "abc",
		Type:              "echo",
		State:             StateSucceeded,
		Priority:          5,
		MaxRetries:        3,
		TimeoutPerAttempt: 30 * time.Second,
		AttemptCount:      1,
		Result:            json.RawMessage(`{"ok":true}`),
		CreatedAt:         started,
		UpdatedAt:         completed,
		StartedAt:         &started,
		CompletedAt:       &completed,
	}

	resp := job.ToResponse()
	if resp.ID != "abc" {
		t.Fatalf("id: got %s", resp.ID)
	}
	if resp.TimeoutPerAttempt != "30s" {
		t.Fatalf("timeout: got %s", resp.TimeoutPerAttempt)
	}
	if resp.State != StateSucceeded {
		t.Fatalf("state: got %s", resp.State)
	}
	if string(resp.Result) != `{"ok":true}` {
		t.Fatalf("result: got %s", resp.Result)
	}
}
