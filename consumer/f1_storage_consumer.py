"""
🏎️ F1 Storage Consumer
-----------------------
Reads from TWO Kafka topics in parallel threads:
  • 'race_events'  → stores raw telemetry into the raw_events table
  • 'ai_insights'  → stores processed alerts into the alerts table

This consumer is the persistence layer of the F1 Alert Engine pipeline.

Run: python consumer/f1_storage_consumer.py
"""

import json
import sys
import os
import threading
import time
from kafka import KafkaConsumer

# ── Path fix so we can import db/ from the project root ───────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from db.database import init_db, insert_raw_event, insert_alert

# ── Configuration ─────────────────────────────────────────────────
KAFKA_BROKER    = 'localhost:9092'
EVENTS_TOPIC    = 'race_events'
INSIGHTS_TOPIC  = 'ai_insights'

# ── Initialise the database (creates tables if not present) ────────
init_db()

print("=" * 60)
print("🗄️  F1 STORAGE CONSUMER")
print(f"   Persisting '{EVENTS_TOPIC}' → raw_events table")
print(f"   Persisting '{INSIGHTS_TOPIC}'  → alerts table")
print("=" * 60 + "\n")


# ══════════════════════════════════════════════════════════════════
# THREAD 1 — Raw Events Consumer
# ══════════════════════════════════════════════════════════════════
def consume_raw_events():
    """Background thread: reads race_events and stores raw telemetry."""
    consumer = KafkaConsumer(
        EVENTS_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='storage_events_group',
        api_version=(2, 8, 1)
    )
    print(f"[Events Thread] Listening to '{EVENTS_TOPIC}'...")

    for message in consumer:
        try:
            event = json.loads(message.value.decode('utf-8'))
            insert_raw_event(event)
            driver = event.get('driver_id', '?')
            etype  = event.get('event_type', '?')
            print(f"  [DB ✔] raw_events  ← {driver} | {etype}")
        except Exception as e:
            print(f"  [DB ✘] raw_events error: {e}")


# ══════════════════════════════════════════════════════════════════
# THREAD 2 — AI Insights (Alerts) Consumer
# ══════════════════════════════════════════════════════════════════
def consume_ai_insights():
    """Background thread: reads ai_insights and stores processed alerts."""
    consumer = KafkaConsumer(
        INSIGHTS_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='storage_insights_group',
        api_version=(2, 8, 1)
    )
    print(f"[Insights Thread] Listening to '{INSIGHTS_TOPIC}'...")

    for message in consumer:
        try:
            alert = json.loads(message.value.decode('utf-8'))
            insert_alert(alert)
            driver   = alert.get('driver_id', '?')
            priority = alert.get('priority', '?')
            print(f"  [DB ✔] alerts      ← {driver} | {priority}")
        except Exception as e:
            print(f"  [DB ✘] alerts error: {e}")


# ══════════════════════════════════════════════════════════════════
# MAIN — Spin up both threads and wait
# ══════════════════════════════════════════════════════════════════
try:
    t_events   = threading.Thread(target=consume_raw_events,  daemon=True)
    t_insights = threading.Thread(target=consume_ai_insights, daemon=True)

    t_events.start()
    t_insights.start()

    # Keep the main thread alive
    while True:
        time.sleep(5)

except KeyboardInterrupt:
    print("\n[Storage Consumer] Stopped.")
