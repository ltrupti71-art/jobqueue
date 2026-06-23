-- Jobs table: durable job records shared across all app instances.
CREATE TABLE IF NOT EXISTS jobs (
    id                      UUID PRIMARY KEY,
    type                    TEXT NOT NULL,
    payload                 JSONB NOT NULL DEFAULT '{}',
    priority                INT NOT NULL DEFAULT 0,
    max_retries             INT NOT NULL,
    timeout_per_attempt_ms  BIGINT NOT NULL,
    state                   TEXT NOT NULL,
    attempt_count           INT NOT NULL DEFAULT 0,
    last_error              TEXT NOT NULL DEFAULT '',
    result                  JSONB,
    available_at            TIMESTAMPTZ NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL,
    updated_at              TIMESTAMPTZ NOT NULL,
    started_at              TIMESTAMPTZ,
    completed_at            TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs (state);

-- Shared queue table: dequeue uses FOR UPDATE SKIP LOCKED for multi-instance safety.
CREATE TABLE IF NOT EXISTS job_queue (
    job_id       UUID PRIMARY KEY REFERENCES jobs (id) ON DELETE CASCADE,
    priority     INT NOT NULL,
    available_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_job_queue_dequeue ON job_queue (available_at ASC, priority DESC);
