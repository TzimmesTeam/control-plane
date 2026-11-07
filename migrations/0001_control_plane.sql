CREATE TYPE run_status AS ENUM (
    'created', 'splitting', 'running', 'aggregating',
    'succeeded', 'failed', 'canceled'
);

CREATE TYPE stage_name AS ENUM ('prep', 'cashflow', 'pv', 'aggregate');

CREATE TYPE batch_status AS ENUM ('pending', 'dispatched', 'done', 'failed', 'dead');

CREATE TABLE runs (
    run_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    portfolio_id    UUID NOT NULL,
    report_dttm     DATE NOT NULL,
    source_uri      TEXT NOT NULL,
    curves_uri      TEXT NOT NULL,
    context_uri     TEXT,
    status          run_status NOT NULL DEFAULT 'created',
    current_stage   stage_name,
    total_deals     INTEGER NOT NULL DEFAULT 0,
    total_batches   INTEGER NOT NULL DEFAULT 0,
    report_link     TEXT,
    error           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ
);

CREATE TABLE batches (
    run_id          UUID NOT NULL REFERENCES runs (run_id) ON DELETE CASCADE,
    batch_id        UUID NOT NULL,
    seq             INTEGER NOT NULL,
    ids_uri         TEXT NOT NULL,
    deal_count      INTEGER NOT NULL,
    weight          BIGINT NOT NULL,
    currency        TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (run_id, batch_id),
    CONSTRAINT uq_batches_seq UNIQUE (run_id, seq)
);

CREATE TABLE batch_stages (
    run_id          UUID NOT NULL,
    batch_id        UUID NOT NULL,
    stage           stage_name NOT NULL,
    status          batch_status NOT NULL DEFAULT 'pending',
    attempt         INTEGER NOT NULL DEFAULT 0,
    output_links    JSONB,
    stats           JSONB,
    error           TEXT,
    dispatched_at   TIMESTAMPTZ,
    done_at         TIMESTAMPTZ,
    lease_until     TIMESTAMPTZ,

    PRIMARY KEY (run_id, batch_id, stage),
    FOREIGN KEY (run_id, batch_id) REFERENCES batches (run_id, batch_id) ON DELETE CASCADE
);

CREATE TABLE aggregations (
    run_id          UUID PRIMARY KEY REFERENCES runs (run_id) ON DELETE CASCADE,
    portfolio_id    UUID NOT NULL,
    status          batch_status NOT NULL DEFAULT 'pending',
    attempt         INTEGER NOT NULL DEFAULT 0,
    batches_uri     TEXT,
    report_link     TEXT,
    stats           JSONB,
    error           TEXT,
    dispatched_at   TIMESTAMPTZ,
    done_at         TIMESTAMPTZ,
    lease_until     TIMESTAMPTZ
);

CREATE TABLE processed_events (
    run_id          UUID NOT NULL,
    batch_id        UUID NOT NULL,
    stage           stage_name NOT NULL,
    attempt         INTEGER NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (run_id, batch_id, stage, attempt)
);

CREATE INDEX idx_batch_stages_lease
    ON batch_stages (status, lease_until)
    WHERE status = 'dispatched';

CREATE INDEX idx_batch_stages_progress
    ON batch_stages (run_id, stage, status);

CREATE INDEX idx_runs_active
    ON runs (status)
    WHERE status IN ('created', 'splitting', 'running', 'aggregating');
