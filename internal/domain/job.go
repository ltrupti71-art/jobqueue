package domain

import (
	"encoding/json"
	"time"
)

type JobState string

const (
	StateQueued       JobState = "queued"
	StateRunning      JobState = "running"
	StateSucceeded    JobState = "succeeded"
	StateFailed       JobState = "failed"
	StateDeadLettered JobState = "dead_lettered"
	StateCancelled    JobState = "cancelled"
)

type Job struct {
	ID                string          `json:"id"`
	Type              string          `json:"type"`
	Payload           json.RawMessage `json:"payload"`
	Priority          int             `json:"priority"`
	MaxRetries        int             `json:"max_retries"`
	TimeoutPerAttempt time.Duration   `json:"timeout_per_attempt"`
	State             JobState        `json:"state"`
	AttemptCount      int             `json:"attempt_count"`
	LastError         string          `json:"last_error,omitempty"`
	Result            json.RawMessage `json:"result,omitempty"`
	ScheduleID        string          `json:"schedule_id,omitempty"`
	AvailableAt       time.Time       `json:"available_at"`
	CreatedAt         time.Time       `json:"created_at"`
	UpdatedAt         time.Time       `json:"updated_at"`
	StartedAt         *time.Time      `json:"started_at,omitempty"`
	CompletedAt       *time.Time      `json:"completed_at,omitempty"`
}

type SubmitJobRequest struct {
	Type              string          `json:"type"`
	Payload           json.RawMessage `json:"payload"`
	Priority          int             `json:"priority"`
	MaxRetries        int             `json:"max_retries"`
	TimeoutPerAttempt string          `json:"timeout_per_attempt"`
	RunAt             string          `json:"run_at"`
	Delay             string          `json:"delay"`
}

type JobResponse struct {
	ID                string          `json:"id"`
	Type              string          `json:"type"`
	State             JobState        `json:"state"`
	Priority          int             `json:"priority"`
	MaxRetries        int             `json:"max_retries"`
	TimeoutPerAttempt string          `json:"timeout_per_attempt"`
	AttemptCount      int             `json:"attempt_count"`
	LastError         string          `json:"last_error,omitempty"`
	Result            json.RawMessage `json:"result,omitempty"`
	ScheduleID        string          `json:"schedule_id,omitempty"`
	AvailableAt       time.Time       `json:"available_at"`
	CreatedAt         time.Time       `json:"created_at"`
	UpdatedAt         time.Time       `json:"updated_at"`
	StartedAt         *time.Time      `json:"started_at,omitempty"`
	CompletedAt       *time.Time      `json:"completed_at,omitempty"`
}

func (j *Job) ToResponse() JobResponse {
	return JobResponse{
		ID:                j.ID,
		Type:              j.Type,
		State:             j.State,
		Priority:          j.Priority,
		MaxRetries:        j.MaxRetries,
		TimeoutPerAttempt: j.TimeoutPerAttempt.String(),
		AttemptCount:      j.AttemptCount,
		LastError:         j.LastError,
		Result:            j.Result,
		ScheduleID:        j.ScheduleID,
		AvailableAt:       j.AvailableAt,
		CreatedAt:         j.CreatedAt,
		UpdatedAt:         j.UpdatedAt,
		StartedAt:         j.StartedAt,
		CompletedAt:       j.CompletedAt,
	}
}

type QueueDepth struct {
	Pending       int `json:"pending"`
	Delayed       int `json:"delayed"`
	Running       int `json:"running"`
	DeadLettered  int `json:"dead_lettered"`
	TotalActive   int `json:"total_active"`
}

type DrainResult struct {
	Cancelled int      `json:"cancelled"`
	JobIDs    []string `json:"job_ids"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}
