-- =============================================================================
-- payment_events: the work table every dispatcher claims from.
-- Lifecycle: NEW -> CLAIMED (lease) -> STARTED (workflow running) -> DONE | FAILED
-- =============================================================================
CREATE TABLE payment_events (
    id            BIGSERIAL    PRIMARY KEY,
    event_type    VARCHAR(40)  NOT NULL,
    payload       JSONB        NOT NULL,
    status        VARCHAR(16)  NOT NULL DEFAULT 'NEW',
    -- Future-dated payments become claimable shortly before they are due.
    available_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    claimed_at    TIMESTAMPTZ,
    started_at    TIMESTAMPTZ,
    finished_at   TIMESTAMPTZ,
    attempts      INT          NOT NULL DEFAULT 0,
    workflow_id   VARCHAR(80),
    outcome       TEXT,
    last_error    TEXT,
    CONSTRAINT payment_events_status_chk
        CHECK (status IN ('NEW', 'CLAIMED', 'STARTED', 'DONE', 'FAILED'))
);

-- Claim path: small partial index, stays fast however large the table grows.
CREATE INDEX payment_events_claimable
    ON payment_events (event_type, available_at)
    WHERE status = 'NEW';

-- In-flight count and lease recovery.
CREATE INDEX payment_events_inflight
    ON payment_events (event_type, claimed_at)
    WHERE status IN ('CLAIMED', 'STARTED');

-- =============================================================================
-- dispatch_config: runtime limits per event type, read at the start of every
-- dispatcher run. Ops (and later the load governor) change these without a deploy.
-- =============================================================================
CREATE TABLE dispatch_config (
    event_type          VARCHAR(40) PRIMARY KEY,
    batch_size          INT         NOT NULL,
    max_inflight        INT         NOT NULL,
    starts_per_second   INT         NOT NULL,
    parallelism         INT         NOT NULL,   -- option 2 only: concurrent start calls
    time_budget_seconds INT         NOT NULL,   -- ~80% of the schedule interval
    lease_seconds       INT         NOT NULL,
    enabled             BOOLEAN     NOT NULL DEFAULT TRUE,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO dispatch_config
    (event_type, batch_size, max_inflight, starts_per_second, parallelism, time_budget_seconds, lease_seconds)
VALUES
    ('INBOUND_PAYMENT',   200, 2000, 200, 8,  45, 120),
    ('PAYMENT_RETURNED',  100, 1000, 100, 4,  45, 120),
    ('PAYMENT_SCHEDULED', 200, 5000, 100, 4,  45, 120),
    ('PAYMENT_POSTED',    500, 3000, 300, 8,  25, 120),
    ('PAYMENT_SETTLED',   500, 2000, 100, 4, 240, 300);

-- =============================================================================
-- Option 2 doorbell: one NOTIFY per event type per INSERT statement.
-- The payload is only the event type; the table stays the source of truth.
-- Postgres also folds identical notifications inside one transaction.
-- =============================================================================
CREATE FUNCTION notify_payment_events() RETURNS trigger AS $$
DECLARE
    t TEXT;
BEGIN
    FOR t IN SELECT DISTINCT event_type FROM new_rows LOOP
        PERFORM pg_notify('payment_events', t);
    END LOOP;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER payment_events_notify
    AFTER INSERT ON payment_events
    REFERENCING NEW TABLE AS new_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION notify_payment_events();
