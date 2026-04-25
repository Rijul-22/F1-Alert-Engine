"""
🏎️ F1 Unified Consumer
Combines the Strategy Engine + Alert Prioritizer + AI Broadcaster + DB Storage into a single pipeline.
Reads from 'race_events' -> Processes inline -> Saves to SQLite -> Publishes to 'ai_insights'

Run: python consumer/f1_unified_consumer.py
"""

import json
import time
import random
import uuid
import os
import sys
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv

# ── Path fix so db/ package is importable when run from any directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from db.database import init_db, insert_raw_event, insert_alert

# Load environment variables from .env
load_dotenv()

# ── Configuration ──────────────────────────────────────────────────
INPUT_TOPIC = 'race_events'
OUTPUT_TOPIC = 'ai_insights'
DLQ_TOPIC = 'failed_events'
KAFKA_BROKER = 'localhost:9092'
MAX_RETRIES = 3

# ── Rule Engine Thresholds (from .env) ────────────────────────────
SLOW_PIT_THRESHOLD = float(os.getenv('SLOW_PIT_THRESHOLD_SECONDS', 3.0))
LAP_DROP_THRESHOLD = float(os.getenv('LAP_DROP_THRESHOLD_SECONDS', 0.5))
AGGRESSIVE_MOVE_THRESHOLD = int(os.getenv('AGGRESSIVE_MOVE_POSITION_GAIN', 2))

# ── Kafka Setup ────────────────────────────────────────────────────
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='unified_consumer_group',
    api_version=(2, 8, 1)
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    api_version=(2, 8, 1),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ── Initialise SQLite database ────────────────────────────────────
init_db()

print("=" * 60)
print("🏎️  F1 UNIFIED CONSUMER")
print(f"    Reading from: '{INPUT_TOPIC}'")
print(f"    Publishing to: '{OUTPUT_TOPIC}'")
print("    Pipeline: Strategy Rules → Priority → AI Commentary → DB")
print(f"    [Config] Slow Pit: >{SLOW_PIT_THRESHOLD}s | Lap Drop: >{LAP_DROP_THRESHOLD}s | Aggressive Move: >={AGGRESSIVE_MOVE_THRESHOLD} pos")
print("=" * 60 + "\n")

# ── State ──────────────────────────────────────────────────────────
driver_lap_state = {}

# ══════════════════════════════════════════════════════════════════
# STAGE 1: Strategy Rules (from f1_strategy_consumer.py)
# ══════════════════════════════════════════════════════════════════
def apply_strategy_rules(event):
    """Applies rule-based intelligence to detect noteworthy race events."""
    driver = event.get('driver_id', 'Unknown')
    event_type = event.get('event_type', 'unknown')
    strategy_insight = None

    if event_type == "pit_stop":
        duration = event.get('pit_stop_duration', 0)
        if duration > SLOW_PIT_THRESHOLD:
            strategy_insight = {
                "insight_type": "slow_pit",
                "message": f"Slow pit stop detected: {duration} seconds.",
                "severity": "medium",
                "metric": duration
            }

    elif event_type == "position_change":
        gained = event.get('position_gained', 0)
        if gained >= AGGRESSIVE_MOVE_THRESHOLD:
            strategy_insight = {
                "insight_type": "aggressive_move",
                "message": f"Aggressive move! Gained {gained} positions.",
                "severity": "high",
                "metric": gained
            }

    elif event_type == "lap_time":
        current_lap = event.get('lap_time_seconds', 0)
        previous_lap = driver_lap_state.get(driver)
        driver_lap_state[driver] = current_lap

        if previous_lap is not None:
            if current_lap > previous_lap + LAP_DROP_THRESHOLD:
                strategy_insight = {
                    "insight_type": "performance_drop",
                    "message": f"Performance drop: Lap time increased by {round(current_lap - previous_lap, 2)}s compared to previous lap.",
                    "severity": "low",
                    "metric": round(current_lap - previous_lap, 2)
                }

    return strategy_insight

# ══════════════════════════════════════════════════════════════════
# STAGE 2: Priority Assignment (from f1_alert_consumer.py)
# ══════════════════════════════════════════════════════════════════
def assign_priority(strategy_data):
    """Assigns priority levels based on the type of strategy insight."""
    insight_type = strategy_data.get('insight_type')

    if insight_type == "aggressive_move":
        return "CRITICAL"
    elif insight_type == "slow_pit":
        return "HIGH"
    elif insight_type == "performance_drop":
        metric = strategy_data.get("metric", 0)
        return "HIGH" if metric > 1.0 else "MEDIUM"

    return "LOW"

# ══════════════════════════════════════════════════════════════════
# STAGE 3: AI Commentary (from f1_ai_consumer.py)
# ══════════════════════════════════════════════════════════════════
def generate_ai_commentary(alert_data):
    """Local Broadcaster Engine — generates Martin Brundle style commentary."""
    driver = alert_data.get('driver_id')
    message = alert_data.get('alert_message', '')

    if "pit stop" in message.lower():
        templates = [
            f"Oh dear, a painfully slow stop for {driver}! The mechanics are struggling, and that's going to cost them dearly out on track.",
            f"Disaster in the pit lane for {driver}! Every second feels like a lifetime when you're strapped in there watching the clock.",
            f"{driver} is stationary for far too long! The team has really fumbled that tire change."
        ]
    elif "position" in message.lower() or "aggressive" in message.lower():
        templates = [
            f"Brilliant driving from {driver}! They've sent it down the inside and made up crucial places!",
            f"Look at {driver} go! That is absolute maximum commitment to slice through the field like that.",
            f"What a sensational and aggressive move by {driver}! They are showing no fear out there today."
        ]
    elif "performance" in message.lower() or "lap time" in message.lower():
        templates = [
            f"The telemetry doesn't lie, {driver} is bleeding lap time here. Are the tires falling off the cliff?",
            f"We are seeing a significant drop in pace from {driver}. The team needs to figure out this performance issue immediately.",
            f"{driver} is struggling for grip out there! The lap times are drifting away from the front runners."
        ]
    else:
        templates = [
            f"Big drama surrounding {driver} right now, we need to keep a close eye on this situation!",
            f"Unbelievable scenes! {driver} is right in the thick of the action right now."
        ]

    return random.choice(templates)

# ══════════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ══════════════════════════════════════════════════════════════════
try:
    for message in consumer:
        raw_event_bytes = message.value
        success = False
        last_error = None

        for attempt in range(MAX_RETRIES):
            try:
                # Deserialize
                raw_event = json.loads(raw_event_bytes.decode('utf-8'))
                driver = raw_event.get('driver_id', 'Unknown')

                # ── STAGE 0: Persist raw event to DB ──
                insert_raw_event(raw_event)

                # ── STAGE 1: Apply Strategy Rules ──
                insight = apply_strategy_rules(raw_event)

                if insight:
                    priority = assign_priority(insight)

                    # ── STAGE 2: Build Alert ──
                    alert = {
                        "alert_id": str(uuid.uuid4()),
                        "timestamp": time.time(),
                        "driver_id": driver,
                        "priority": priority,
                        "alert_message": insight["message"],
                        "insight_type": insight["insight_type"],
                        "requires_ai": priority in ["CRITICAL", "HIGH"]
                    }

                    # ── STAGE 3: Generate Commentary (for HIGH/CRITICAL) ──
                    if alert["requires_ai"]:
                        ai_text = generate_ai_commentary(alert)
                        alert["ai_commentary"] = ai_text
                    else:
                        alert["ai_commentary"] = alert["alert_message"]

                    alert["processed_at"] = time.time()

                    # ── STAGE 4: Persist alert to DB ──
                    insert_alert(alert)

                    # ── OUTPUT ──
                    print(f"[{priority}] {driver}: {insight['message']}")
                    if alert.get("requires_ai"):
                        print(f"  🎙️ {alert['ai_commentary']}")

                    producer.send(OUTPUT_TOPIC, key=driver.encode('utf-8'), value=alert)

                success = True
                break

            except Exception as e:
                last_error = str(e)
                print(f"[Warning] Processing error: {e}. Retry {attempt + 1}/{MAX_RETRIES}")
                time.sleep(1)

        if not success:
            print(f"[Error] Message failed after {MAX_RETRIES} retries. Routing to DLQ.")
            dlq_message = {
                "consumer": "unified_consumer_group",
                "error": last_error,
                "raw_message": raw_event_bytes.decode('utf-8', errors='replace')
            }
            producer.send(DLQ_TOPIC, key=b'dlq', value=dlq_message)

except KeyboardInterrupt:
    print("\nUnified Consumer stopped.")
finally:
    producer.flush()
    producer.close()
    consumer.close()
