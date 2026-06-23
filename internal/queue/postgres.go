package queue

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const postgresPollInterval = 100 * time.Millisecond

// PostgresQueue is a shared priority queue backed by PostgreSQL (FOR UPDATE SKIP LOCKED).
type PostgresQueue struct {
	pool     *pgxpool.Pool
	mu       sync.Mutex
	closed   bool
	notifyCh chan struct{}
}

func NewPostgres(pool *pgxpool.Pool) *PostgresQueue {
	return &PostgresQueue{
		pool:     pool,
		notifyCh: make(chan struct{}, 1),
	}
}

func (q *PostgresQueue) Enqueue(jobID string, priority int, availableAt time.Time) {
	ctx := context.Background()
	_, _ = q.pool.Exec(ctx, `
		INSERT INTO job_queue (job_id, priority, available_at) VALUES ($1,$2,$3)
		ON CONFLICT (job_id) DO UPDATE SET
			priority = EXCLUDED.priority,
			available_at = EXCLUDED.available_at`,
		jobID, priority, availableAt,
	)
	q.signal()
}

func (q *PostgresQueue) PromoteDue(_ time.Time) int {
	// Dequeue filters on available_at; no separate delayed promotion needed.
	return 0
}

func (q *PostgresQueue) Dequeue(ctx context.Context) (string, bool, error) {
	for {
		if q.isClosed() {
			return "", false, nil
		}

		jobID, ok, err := q.tryDequeue(ctx)
		if err != nil {
			return "", false, err
		}
		if ok {
			return jobID, true, nil
		}

		select {
		case <-ctx.Done():
			return "", false, ctx.Err()
		case <-q.notifyCh:
		case <-time.After(postgresPollInterval):
		}
	}
}

func (q *PostgresQueue) tryDequeue(ctx context.Context) (string, bool, error) {
	tx, err := q.pool.Begin(ctx)
	if err != nil {
		return "", false, err
	}
	defer tx.Rollback(ctx)

	var jobID string
	err = tx.QueryRow(ctx, `
		DELETE FROM job_queue
		WHERE job_id = (
			SELECT job_id FROM job_queue
			WHERE available_at <= NOW()
			ORDER BY priority DESC, available_at ASC
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		RETURNING job_id`,
	).Scan(&jobID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", false, nil
		}
		return "", false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return "", false, err
	}
	return jobID, true, nil
}

func (q *PostgresQueue) Remove(jobID string) bool {
	tag, err := q.pool.Exec(context.Background(),
		`DELETE FROM job_queue WHERE job_id = $1`, jobID)
	return err == nil && tag.RowsAffected() > 0
}

func (q *PostgresQueue) Depth() (pending, delayed int) {
	ctx := context.Background()
	_ = q.pool.QueryRow(ctx, `
		SELECT
			COUNT(*) FILTER (WHERE available_at <= NOW()),
			COUNT(*) FILTER (WHERE available_at > NOW())
		FROM job_queue`).Scan(&pending, &delayed)
	return pending, delayed
}

func (q *PostgresQueue) Drain() []string {
	ctx := context.Background()
	rows, err := q.pool.Query(ctx, `DELETE FROM job_queue RETURNING job_id`)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			continue
		}
		ids = append(ids, id)
	}
	return ids
}

func (q *PostgresQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.signal()
}

func (q *PostgresQueue) isClosed() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.closed
}

func (q *PostgresQueue) signal() {
	select {
	case q.notifyCh <- struct{}{}:
	default:
	}
}
