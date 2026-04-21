import json
import time
from kafka import KafkaConsumer, KafkaProducer

# Topics
INPUT_TOPIC = 'race_events'
OUTPUT_TOPIC = 'strategy_events'
KAFKA_BROKER = 'localhost:9092'

# Initialize Consumer
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='strategy_engine_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Initialize Producer for the next topic
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"Starting F1 Strategy Consumer (Rule Engine).")
print(f"Listening to '{INPUT_TOPIC}' -> Publishing to '{OUTPUT_TOPIC}'...\n")

# State tracking for performance logic
driver_lap_state = {}

def process_event(event):
    """Applies rule-based intelligence based on the SRS definitions."""
    driver = event.get('driver_id', 'Unknown')
    event_type = event.get('event_type', 'unknown')
    strategy_insight = None

    if event_type == "pit_stop":
        duration = event.get('pit_stop_duration', 0)
        # Rule: Pit stop > 3.0s is slow
        if duration > 3.0:
            strategy_insight = {
                "insight_type": "slow_pit",
                "message": f"Slow pit stop detected: {duration} seconds.",
                "severity": "medium",
                "metric": duration
            }
            
    elif event_type == "position_change":
        gained = event.get('position_gained', 0)
        # Rule: Position gain >= 2 is an aggressive move
        if gained >= 2:
            strategy_insight = {
                "insight_type": "aggressive_move",
                "message": f"Aggressive move! Gained {gained} positions.",
                "severity": "high",
                "metric": gained
            }
            
    elif event_type == "lap_time":
        current_lap = event.get('lap_time_seconds', 0)
        previous_lap = driver_lap_state.get(driver)
        
        # Track the lap time
        driver_lap_state[driver] = current_lap
        
        if previous_lap is not None:
            # Rule: Lap time explicitly higher means performance drop 
            # (We set threshold of 0.5s to filter minor fluctuations)
            if current_lap > previous_lap + 0.5:
                strategy_insight = {
                    "insight_type": "performance_drop",
                    "message": f"Performance drop: Lap time increased by {round(current_lap - previous_lap, 2)}s compared to previous lap.",
                    "severity": "low",
                    "metric": round(current_lap - previous_lap, 2)
                }

    # If an insight was formed, enrich the message and return
    if strategy_insight:
        return {
            "timestamp": time.time(),
            "driver_id": driver,
            "original_event": event_type,
            "strategy": strategy_insight
        }
    return None

try:
    for message in consumer:
        raw_event = message.value
        
        # Apply the rules
        insight = process_event(raw_event)
        
        if insight:
            # Output the insight locally
            print(f"[{insight['driver_id']}] INSIGHT -> {insight['strategy']['message']}")
            
            # Forward the insight down the pipeline
            producer.send(OUTPUT_TOPIC, key=insight['driver_id'].encode('utf-8'), value=insight)
            
except KeyboardInterrupt:
    print("\nStrategy Consumer stopped.")
finally:
    producer.flush()
    producer.close()
    consumer.close()
