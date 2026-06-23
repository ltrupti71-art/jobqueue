-- Recurring job schedules (cron-style).
CREATE TABLE IF NOT EXISTS schedules (
    id                      UUID PRIMARY KEY,
    type                    TEXT NOT NULL,
    payload                 JSONB NOT NULL DEFAULT '{}',
    priority                INT NOT NULL DEFAULT 0,
    max_retries             INT NOT NULL,
    timeout_per_attempt_ms  BIGINT NOT NULL,
    cron_expr               TEXT NOT NULL,
    timezone                TEXT NOT NULL DEFAULT 'UTC',
    state                   TEXT NOT NULL,
    next_run_at             TIMESTAMPTZ NOT NULL,
    last_run_at             TIMESTAMPTZ,
    created_at              TIMESTAMPTZ NOT NULL,
    updated_at              TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_schedules_due ON schedules (next_run_at)
    WHERE state = 'active';

-- Link scheduled job runs back to their recurring schedule.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS schedule_id UUID REFERENCES schedules (id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_jobs_schedule_id ON jobs (schedule_id)
    WHERE schedule_id IS NOT NULL;
