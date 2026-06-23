package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jobqueue/api/internal/domain"
)

// AtomicCreator persists a job and enqueues it in a single transaction.
type AtomicCreator interface {
	CreateAndEnqueue(ctx context.Context, job *domain.Job) error
}

// Reconciler re-enqueues queued jobs missing from the shared queue (crash recovery).
type Reconciler interface {
	ReconcileQueue(ctx context.Context) (int, error)
}

type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

func (s *PostgresStore) Create(ctx context.Context, job *domain.Job) error {
	_, err := s.pool.Exec(ctx, `
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
	if err != nil {
		return fmt.Errorf("insert job: %w", err)
	}
	return nil
}

func (s *PostgresStore) CreateAndEnqueue(ctx context.Context, job *domain.Job) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if err := insertJob(ctx, tx, job); err != nil {
		return err
	}
	if err := upsertQueueEntry(ctx, tx, job.ID, job.Priority, job.AvailableAt); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *PostgresStore) ReconcileQueue(ctx context.Context) (int, error) {
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO job_queue (job_id, priority, available_at)
		SELECT id, priority, available_at FROM jobs WHERE state = 'queued'
		ON CONFLICT (job_id) DO NOTHING`)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

func (s *PostgresStore) Get(ctx context.Context, id string) (*domain.Job, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, type, payload, priority, max_retries, timeout_per_attempt_ms,
			state, attempt_count, last_error, result, schedule_id,
			available_at, created_at, updated_at, started_at, completed_at
		FROM jobs WHERE id = $1`, id)

	job, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return job, err
}

func (s *PostgresStore) Update(ctx context.Context, job *domain.Job) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs SET
			type = $2, payload = $3, priority = $4, max_retries = $5,
			timeout_per_attempt_ms = $6, state = $7, attempt_count = $8,
			last_error = $9, result = $10, available_at = $11,
			updated_at = $12, started_at = $13, completed_at = $14
		WHERE id = $1`,
		job.ID, job.Type, job.Payload, job.Priority, job.MaxRetries,
		job.TimeoutPerAttempt.Milliseconds(), string(job.State), job.AttemptCount,
		job.LastError, nullableJSON(job.Result), job.AvailableAt,
		job.UpdatedAt, job.StartedAt, job.CompletedAt,
	)
	if err != nil {
		return fmt.Errorf("update job: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *PostgresStore) ListByState(ctx context.Context, state domain.JobState) ([]*domain.Job, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, type, payload, priority, max_retries, timeout_per_attempt_ms,
			state, attempt_count, last_error, result, schedule_id,
			available_at, created_at, updated_at, started_at, completed_at
		FROM jobs WHERE state = $1 ORDER BY created_at ASC`, string(state))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []*domain.Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func (s *PostgresStore) CountByState(ctx context.Context, state domain.JobState) (int, error) {
	var count int
	err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM jobs WHERE state = $1`, string(state)).Scan(&count)
	return count, err
}

type scannable interface {
	Scan(dest ...any) error
}

func scanJob(row scannable) (*domain.Job, error) {
	var job domain.Job
	var state string
	var timeoutMS int64
	var result []byte
	var scheduleID *string

	err := row.Scan(
		&job.ID, &job.Type, &job.Payload, &job.Priority, &job.MaxRetries, &timeoutMS,
		&state, &job.AttemptCount, &job.LastError, &result, &scheduleID,
		&job.AvailableAt, &job.CreatedAt, &job.UpdatedAt, &job.StartedAt, &job.CompletedAt,
	)
	if err != nil {
		return nil, err
	}
	job.State = domain.JobState(state)
	job.TimeoutPerAttempt = time.Duration(timeoutMS) * time.Millisecond
	if len(result) > 0 {
		job.Result = json.RawMessage(result)
	}
	if scheduleID != nil {
		job.ScheduleID = *scheduleID
	}
	return &job, nil
}

func nullableJSON(raw json.RawMessage) []byte {
	if len(raw) == 0 {
		return nil
	}
	return raw
}

func insertJob(ctx context.Context, tx pgx.Tx, job *domain.Job) error {
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

func upsertQueueEntry(ctx context.Context, tx pgx.Tx, jobID string, priority int, availableAt time.Time) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO job_queue (job_id, priority, available_at) VALUES ($1,$2,$3)
		ON CONFLICT (job_id) DO UPDATE SET
			priority = EXCLUDED.priority,
			available_at = EXCLUDED.available_at`,
		jobID, priority, availableAt,
	)
	return err
}
