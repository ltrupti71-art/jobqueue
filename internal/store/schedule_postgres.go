package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jobqueue/api/internal/domain"
	"github.com/jobqueue/api/internal/schedule"
)

type PostgresScheduleStore struct {
	pool *pgxpool.Pool
}

func NewPostgresScheduleStore(pool *pgxpool.Pool) *PostgresScheduleStore {
	return &PostgresScheduleStore{pool: pool}
}

func (s *PostgresScheduleStore) Create(ctx context.Context, sch *domain.Schedule) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO schedules (
			id, type, payload, priority, max_retries, timeout_per_attempt_ms,
			cron_expr, timezone, state, next_run_at, last_run_at, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
		sch.ID, sch.Type, sch.Payload, sch.Priority, sch.MaxRetries,
		sch.TimeoutPerAttempt.Milliseconds(), sch.CronExpr, sch.Timezone,
		string(sch.State), sch.NextRunAt, sch.LastRunAt,
		sch.CreatedAt, sch.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert schedule: %w", err)
	}
	return nil
}

func (s *PostgresScheduleStore) Get(ctx context.Context, id string) (*domain.Schedule, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, type, payload, priority, max_retries, timeout_per_attempt_ms,
			cron_expr, timezone, state, next_run_at, last_run_at, created_at, updated_at
		FROM schedules WHERE id = $1`, id)
	sch, err := scanSchedule(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return sch, err
}

func (s *PostgresScheduleStore) Update(ctx context.Context, sch *domain.Schedule) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE schedules SET
			type = $2, payload = $3, priority = $4, max_retries = $5,
			timeout_per_attempt_ms = $6, cron_expr = $7, timezone = $8,
			state = $9, next_run_at = $10, last_run_at = $11, updated_at = $12
		WHERE id = $1`,
		sch.ID, sch.Type, sch.Payload, sch.Priority, sch.MaxRetries,
		sch.TimeoutPerAttempt.Milliseconds(), sch.CronExpr, sch.Timezone,
		string(sch.State), sch.NextRunAt, sch.LastRunAt, sch.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("update schedule: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *PostgresScheduleStore) List(ctx context.Context) ([]*domain.Schedule, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, type, payload, priority, max_retries, timeout_per_attempt_ms,
			cron_expr, timezone, state, next_run_at, last_run_at, created_at, updated_at
		FROM schedules ORDER BY created_at ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanSchedules(rows)
}

func (s *PostgresScheduleStore) ListDue(ctx context.Context, now time.Time) ([]*domain.Schedule, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, type, payload, priority, max_retries, timeout_per_attempt_ms,
			cron_expr, timezone, state, next_run_at, last_run_at, created_at, updated_at
		FROM schedules
		WHERE state = 'active' AND next_run_at <= $1
		ORDER BY next_run_at ASC`, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanSchedules(rows)
}

// FireDue atomically claims one due schedule, enqueues a job run, and advances next_run_at.
func (s *PostgresScheduleStore) FireDue(ctx context.Context, now time.Time) (*domain.Job, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var sch domain.Schedule
	var state string
	var timeoutMS int64
	err = tx.QueryRow(ctx, `
		SELECT id, type, payload, priority, max_retries, timeout_per_attempt_ms,
			cron_expr, timezone, state, next_run_at, last_run_at, created_at, updated_at
		FROM schedules
		WHERE id = (
			SELECT id FROM schedules
			WHERE state = 'active' AND next_run_at <= $1
			ORDER BY next_run_at ASC
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)`, now).Scan(
		&sch.ID, &sch.Type, &sch.Payload, &sch.Priority, &sch.MaxRetries, &timeoutMS,
		&sch.CronExpr, &sch.Timezone, &state, &sch.NextRunAt, &sch.LastRunAt,
		&sch.CreatedAt, &sch.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	sch.State = domain.ScheduleState(state)
	sch.TimeoutPerAttempt = time.Duration(timeoutMS) * time.Millisecond

	loc, err := schedule.LoadLocation(sch.Timezone)
	if err != nil {
		return nil, err
	}
	nextRunAt, err := schedule.NextRun(sch.CronExpr, loc, sch.NextRunAt)
	if err != nil {
		return nil, err
	}

	runAt := sch.NextRunAt
	job := jobFromSchedule(&sch, runAt, now)
	if err := insertJobWithSchedule(ctx, tx, job); err != nil {
		return nil, err
	}
	if err := upsertQueueEntry(ctx, tx, job.ID, job.Priority, job.AvailableAt); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE schedules SET last_run_at = $2, next_run_at = $3, updated_at = $4
		WHERE id = $1`,
		sch.ID, runAt, nextRunAt, now,
	); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return job, nil
}

func jobFromSchedule(sch *domain.Schedule, runAt, now time.Time) *domain.Job {
	return &domain.Job{
		ID:                uuid.NewString(),
		Type:              sch.Type,
		Payload:           sch.Payload,
		Priority:          sch.Priority,
		MaxRetries:        sch.MaxRetries,
		TimeoutPerAttempt: sch.TimeoutPerAttempt,
		State:             domain.StateQueued,
		ScheduleID:        sch.ID,
		AvailableAt:       runAt,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
}

func scanSchedules(rows pgx.Rows) ([]*domain.Schedule, error) {
	var out []*domain.Schedule
	for rows.Next() {
		sch, err := scanSchedule(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, sch)
	}
	return out, rows.Err()
}

func scanSchedule(row scannable) (*domain.Schedule, error) {
	var sch domain.Schedule
	var state string
	var timeoutMS int64
	err := row.Scan(
		&sch.ID, &sch.Type, &sch.Payload, &sch.Priority, &sch.MaxRetries, &timeoutMS,
		&sch.CronExpr, &sch.Timezone, &state, &sch.NextRunAt, &sch.LastRunAt,
		&sch.CreatedAt, &sch.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	sch.State = domain.ScheduleState(state)
	sch.TimeoutPerAttempt = time.Duration(timeoutMS) * time.Millisecond
	return &sch, nil
}

func insertJobWithSchedule(ctx context.Context, tx pgx.Tx, job *domain.Job) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO jobs (
			id, type, payload, priority, max_retries, timeout_per_attempt_ms,
			state, attempt_count, last_error, result, schedule_id,
			available_at, created_at, updated_at, started_at, completed_at
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16
		)`,
		job.ID, job.Type, job.Payload, job.Priority, job.MaxRetries,
		job.TimeoutPerAttempt.Milliseconds(),
		string(job.State), job.AttemptCount, job.LastError, nullableJSON(job.Result),
		nullableScheduleID(job.ScheduleID),
		job.AvailableAt, job.CreatedAt, job.UpdatedAt, job.StartedAt, job.CompletedAt,
	)
	return err
}

func nullableScheduleID(id string) any {
	if id == "" {
		return nil
	}
	return id
}
