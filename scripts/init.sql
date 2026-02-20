-- Postgres schema for payments + transactional outbox

CREATE TABLE IF NOT EXISTS payments (
  transaction_id TEXT PRIMARY KEY,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS outbox_events (
  id BIGSERIAL PRIMARY KEY,
  aggregate_type TEXT NOT NULL DEFAULT 'payment',
  aggregate_id TEXT NOT NULL,
  event_type TEXT NOT NULL DEFAULT 'payment.received',
  payload JSONB NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending', -- pending | processing | published
  attempt_count INT NOT NULL DEFAULT 0,
  last_error TEXT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ NULL,
  dispatcher_delivered_at TIMESTAMPTZ NULL
);

-- Avoid duplicate 'payment.received' events for the same Transaction ID
CREATE UNIQUE INDEX IF NOT EXISTS outbox_events_payment_received_unique
  ON outbox_events (aggregate_id, event_type);

CREATE INDEX IF NOT EXISTS outbox_events_status_id_idx
  ON outbox_events (status, id);

