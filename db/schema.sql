-- ============================================================
-- F1 Alert Engine — Database Schema
-- Engine: SQLite
-- ============================================================

-- ── Table 1: raw_events ──────────────────────────────────────
-- Stores every raw race event coming off the 'race_events' topic.
-- Acts as the immutable source-of-truth / audit log.
CREATE TABLE IF NOT EXISTS raw_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id        TEXT    NOT NULL UNIQUE,        -- uuid generated at insert time
    driver_id       TEXT    NOT NULL,
    event_type      TEXT    NOT NULL,               -- lap_time | pit_stop | position_change | telemetry
    race_status     TEXT,                           -- Green | SC | VSC
    weather         TEXT,                           -- Sunny | Rain
    lap_time_secs   REAL,                           -- populated for lap_time events
    pit_stop_secs   REAL,                           -- populated for pit_stop events
    position_gained INTEGER,                        -- populated for position_change events
    speed_kmh       REAL,                           -- populated for telemetry events
    raw_payload     TEXT    NOT NULL,               -- full JSON blob for replay / debugging
    received_at     REAL    NOT NULL                -- Unix timestamp (time.time())
);

-- Indexes for fast driver + time-range queries
CREATE INDEX IF NOT EXISTS idx_raw_events_driver    ON raw_events (driver_id);
CREATE INDEX IF NOT EXISTS idx_raw_events_timestamp ON raw_events (received_at);
CREATE INDEX IF NOT EXISTS idx_raw_events_type      ON raw_events (event_type);


-- ── Table 2: alerts ──────────────────────────────────────────
-- Stores every processed alert produced by the unified consumer pipeline.
-- Includes the AI commentary for the HIGH/CRITICAL alerts.
CREATE TABLE IF NOT EXISTS alerts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    alert_id        TEXT    NOT NULL UNIQUE,        -- uuid from the pipeline
    driver_id       TEXT    NOT NULL,
    priority        TEXT    NOT NULL,               -- CRITICAL | HIGH | MEDIUM | LOW
    insight_type    TEXT    NOT NULL,               -- slow_pit | aggressive_move | performance_drop
    alert_message   TEXT    NOT NULL,               -- human-readable rule-engine message
    ai_commentary   TEXT,                           -- AI broadcaster commentary (NULL for LOW/MEDIUM)
    requires_ai     INTEGER NOT NULL DEFAULT 0,     -- 0=false, 1=true  (SQLite has no BOOLEAN)
    created_at      REAL    NOT NULL,               -- Unix timestamp from the pipeline
    stored_at       REAL    NOT NULL                -- Unix timestamp of DB insert
);

-- Indexes for dashboard queries (by driver, priority, time)
CREATE INDEX IF NOT EXISTS idx_alerts_driver    ON alerts (driver_id);
CREATE INDEX IF NOT EXISTS idx_alerts_priority  ON alerts (priority);
CREATE INDEX IF NOT EXISTS idx_alerts_timestamp ON alerts (created_at);
