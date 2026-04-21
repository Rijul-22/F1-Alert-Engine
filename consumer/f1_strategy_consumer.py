import json
import time
from kafka import KafkaConsumer, KafkaProducer

# Topics
INPUT_TOPIC = 'race_events'
OUTPUT_TOPIC = 'strategy_events'
DLQ_TOPIC = 'failed_events'
KAFKA_BROKER = 'localhost:9092'
MAX_RETRIES = 3

# Initialize Consumer
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='strategy_engine_group'
    # Removed value_deserializer to handle raw bytes manually and catch parsing errors
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
        raw_event_bytes = message.value
        success = False
        last_error = None
        
        for attempt in range(MAX_RETRIES):
            try:
                # 1. Deserialize within the try block to gracefully handle JSON parse errors
                raw_event = json.loads(raw_event_bytes.decode('utf-8'))
                
                # 2. Apply the rules
                insight = process_event(raw_event)
                
                if insight:
                    # Output the insight locally
                    print(f"[{insight['driver_id']}] INSIGHT -> {insight['strategy']['message']}")
                    
                    # Forward the insight down the pipeline
                    producer.send(OUTPUT_TOPIC, key=insight['driver_id'].encode('utf-8'), value=insight)
                
                success = True
                break
                
            except Exception as e:
                last_error = str(e)
                print(f"[Warning] Error processing message in strategy_consumer: {e}. Retry {attempt + 1}/{MAX_RETRIES}")
                time.sleep(1)
                
        if not success:
            print(f"[Error] Message failed after {MAX_RETRIES} retries. Routing to DLQ: {DLQ_TOPIC}")
            dlq_message = {
                "consumer": "strategy_engine_group",
                "error": last_error,
                "raw_message": raw_event_bytes.decode('utf-8', errors='replace')
            }
            producer.send(DLQ_TOPIC, key=b'dlq', value=dlq_message)
            
except KeyboardInterrupt:
    print("\nStrategy Consumer stopped.")
finally:
    producer.flush()
    producer.close()
    consumer.close()
