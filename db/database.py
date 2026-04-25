"""
db/database.py
--------------
SQLite helper module for the F1 Alert Engine.

Provides:
  - init_db()          — creates tables from schema.sql if they don't exist
  - insert_raw_event() — persists a raw race event
  - insert_alert()     — persists a processed alert with AI commentary
"""

import sqlite3
import json
import time
import uuid
import os

# The database file lives in the db/ folder alongside this module
DB_PATH = os.path.join(os.path.dirname(__file__), 'f1_race_data.db')
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), 'schema.sql')


def get_connection() -> sqlite3.Connection:
    """Returns a new SQLite connection with WAL mode for better concurrency."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")   # safer for concurrent reads/writes
    conn.row_factory = sqlite3.Row             # rows behave like dicts
    return conn


def init_db():
    """
    Reads schema.sql and executes it against the database.
    Safe to call multiple times — uses CREATE TABLE IF NOT EXISTS.
    """
    with open(SCHEMA_PATH, 'r', encoding='utf-8') as f:
        schema_sql = f.read()

    conn = get_connection()
    try:
        conn.executescript(schema_sql)
        conn.commit()
        print(f"[DB] Initialised. Database file: {DB_PATH}")
    finally:
        conn.close()


def insert_raw_event(event: dict):
    """
    Inserts a raw race event into the raw_events table.

    Args:
        event: dict — the deserialized JSON event from the 'race_events' Kafka topic.
    """
    conn = get_connection()
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO raw_events
                (event_id, driver_id, event_type, race_status, weather,
                 lap_time_secs, pit_stop_secs, position_gained,
                 speed_kmh, raw_payload, received_at)
            VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(uuid.uuid4()),                          # event_id
                event.get('driver_id', 'Unknown'),          # driver_id
                event.get('event_type', 'unknown'),         # event_type
                event.get('race_status'),                   # race_status
                event.get('weather'),                       # weather
                event.get('lap_time_seconds'),              # lap_time_secs
                event.get('pit_stop_duration'),             # pit_stop_secs
                event.get('position_gained'),               # position_gained
                event.get('speed_kmh'),                     # speed_kmh
                json.dumps(event),                          # raw_payload (full blob)
                event.get('timestamp', time.time()),        # received_at
            )
        )
        conn.commit()
    finally:
        conn.close()


def insert_alert(alert: dict):
    """
    Inserts a processed alert into the alerts table.

    Args:
        alert: dict — the enriched alert dict produced by the unified consumer pipeline.
    """
    conn = get_connection()
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO alerts
                (alert_id, driver_id, priority, insight_type,
                 alert_message, ai_commentary, requires_ai,
                 created_at, stored_at)
            VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                alert.get('alert_id', str(uuid.uuid4())),  # alert_id
                alert.get('driver_id', 'Unknown'),          # driver_id
                alert.get('priority', 'LOW'),               # priority
                alert.get('insight_type', 'unknown'),       # insight_type
                alert.get('alert_message', ''),             # alert_message
                alert.get('ai_commentary'),                 # ai_commentary (can be NULL)
                1 if alert.get('requires_ai') else 0,       # requires_ai (SQLite bool)
                alert.get('created_at', time.time()),       # created_at
                time.time(),                                # stored_at
            )
        )
        conn.commit()
    finally:
        conn.close()


def fetch_recent_alerts(limit: int = 50) -> list[dict]:
    """
    Returns the most recent alerts as a list of dicts.
    Useful for the dashboard or CLI inspection.
    """
    conn = get_connection()
    try:
        cursor = conn.execute(
            "SELECT * FROM alerts ORDER BY created_at DESC LIMIT ?", (limit,)
        )
        return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


def fetch_recent_events(limit: int = 50) -> list[dict]:
    """Returns the most recent raw events as a list of dicts."""
    conn = get_connection()
    try:
        cursor = conn.execute(
            "SELECT * FROM raw_events ORDER BY received_at DESC LIMIT ?", (limit,)
        )
        return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()
