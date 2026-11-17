BEGIN;
CREATE TABLE IF NOT EXISTS cp_runs (
 id UUID PRIMARY KEY,
 snapshot_id TEXT NOT NULL UNIQUE,
 body JSONB NOT NULL CHECK(body->>'state' IN ('waiting','preparing','running','aggregating','succeeded','failed','canceled')),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS cp_runs_state ON cp_runs((body->>'state'),updated_at);
CREATE TABLE IF NOT EXISTS cp_batches (
 run_id UUID NOT NULL REFERENCES cp_runs(id),
 id UUID NOT NULL,
 seq INTEGER NOT NULL CHECK(seq>0),
 body JSONB NOT NULL CHECK(body->>'stage' IN ('cashflow','pv','done','failed')),
 PRIMARY KEY(run_id,id), UNIQUE(run_id,seq)
);
CREATE INDEX IF NOT EXISTS cp_batches_stage ON cp_batches(run_id,(body->>'stage'));
CREATE TABLE IF NOT EXISTS cp_tasks (
 id UUID PRIMARY KEY,
 run_id UUID NOT NULL REFERENCES cp_runs(id),
 payload JSONB NOT NULL,
 state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','publishing','sent','done')),
 available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 lease_until TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS cp_tasks_delivery ON cp_tasks(state,available_at,lease_until) WHERE state<>'done';
COMMIT;
