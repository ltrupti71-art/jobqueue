package domain

import (
	"encoding/json"
	"time"
)

type ScheduleState string

const (
	ScheduleActive    ScheduleState = "active"
	ScheduleCancelled ScheduleState = "cancelled"
)

type Schedule struct {
	ID                string          `json:"id"`
	Type              string          `json:"type"`
	Payload           json.RawMessage `json:"payload"`
	Priority          int             `json:"priority"`
	MaxRetries        int             `json:"max_retries"`
	TimeoutPerAttempt time.Duration   `json:"timeout_per_attempt"`
	CronExpr          string          `json:"cron"`
	Timezone          string          `json:"timezone"`
	State             ScheduleState   `json:"state"`
	NextRunAt         time.Time       `json:"next_run_at"`
	LastRunAt         *time.Time      `json:"last_run_at,omitempty"`
	CreatedAt         time.Time       `json:"created_at"`
	UpdatedAt         time.Time       `json:"updated_at"`
}

type CreateScheduleRequest struct {
	Type              string          `json:"type"`
	Payload           json.RawMessage `json:"payload"`
	Cron              string          `json:"cron"`
	Timezone          string          `json:"timezone"`
	Priority          int             `json:"priority"`
	MaxRetries        int             `json:"max_retries"`
	TimeoutPerAttempt string          `json:"timeout_per_attempt"`
}

type ScheduleResponse struct {
	ID                string          `json:"id"`
	Type              string          `json:"type"`
	Payload           json.RawMessage `json:"payload"`
	Cron              string          `json:"cron"`
	Timezone          string          `json:"timezone"`
	State             ScheduleState   `json:"state"`
	Priority          int             `json:"priority"`
	MaxRetries        int             `json:"max_retries"`
	TimeoutPerAttempt string          `json:"timeout_per_attempt"`
	NextRunAt         time.Time       `json:"next_run_at"`
	LastRunAt         *time.Time      `json:"last_run_at,omitempty"`
	CreatedAt         time.Time       `json:"created_at"`
	UpdatedAt         time.Time       `json:"updated_at"`
}

func (s *Schedule) ToResponse() ScheduleResponse {
	return ScheduleResponse{
		ID:                s.ID,
		Type:              s.Type,
		Payload:           s.Payload,
		Cron:              s.CronExpr,
		Timezone:          s.Timezone,
		State:             s.State,
		Priority:          s.Priority,
		MaxRetries:        s.MaxRetries,
		TimeoutPerAttempt: s.TimeoutPerAttempt.String(),
		NextRunAt:         s.NextRunAt,
		LastRunAt:         s.LastRunAt,
		CreatedAt:         s.CreatedAt,
		UpdatedAt:         s.UpdatedAt,
	}
}
