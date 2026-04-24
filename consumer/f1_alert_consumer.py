import json
import time
import uuid
from kafka import KafkaConsumer, KafkaProducer

# Topics
INPUT_TOPIC = 'strategy_events'
OUTPUT_TOPIC = 'alerts'
DLQ_TOPIC = 'failed_events'
KAFKA_BROKER = 'localhost:9092'
MAX_RETRIES = 3

# Initialize Consumer
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='alert_generation_group',
    api_version=(2, 8, 1)
    # Removed value_deserializer
)

# Initialize Producer for the next topic
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    api_version=(2, 8, 1),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"Starting F1 Alert Consumer.")
print(f"Listening to '{INPUT_TOPIC}' -> Publishing prioritized alerts to '{OUTPUT_TOPIC}'...\n")

def assign_priority(strategy_data):
    """Assigns priority levels based on the type of strategy insight."""
    insight_type = strategy_data.get('insight_type')
    
    # Priority mapping logic
    if insight_type == "aggressive_move":
        return "CRITICAL"
    elif insight_type == "slow_pit":
        return "HIGH"
    elif insight_type == "performance_drop":
        # Additional thresholds for priority
        metric = strategy_data.get("metric", 0)
        if metric > 1.0:
            return "HIGH"
        else:
            return "MEDIUM"
            
    return "LOW"

try:
    for message in consumer:
        raw_event_bytes = message.value
        success = False
        last_error = None
        
        for attempt in range(MAX_RETRIES):
            try:
                event = json.loads(raw_event_bytes.decode('utf-8'))
                driver = event.get('driver_id', 'Unknown')
                strategy = event.get('strategy', {})
                
                priority = assign_priority(strategy)
                
                # Construct the final Alert package
                alert = {
                    "alert_id": str(uuid.uuid4()),
                    "timestamp": time.time(),
                    "driver_id": driver,
                    "priority": priority,
                    "alert_message": strategy.get("message", "No message provided."),
                    "insight_type": strategy.get("insight_type"),
                    "requires_ai": True if priority in ["CRITICAL", "HIGH"] else False
                }
                
                # Display locally
                print(f"[{priority}] ALERT FOR {driver.upper()}: {alert['alert_message']}")
                
                # Forward to the alerts topic
                producer.send(OUTPUT_TOPIC, key=driver.encode('utf-8'), value=alert)
                
                success = True
                break
                
            except Exception as e:
                last_error = str(e)
                print(f"[Warning] Error processing message in alert_consumer: {e}. Retry {attempt + 1}/{MAX_RETRIES}")
                time.sleep(1)
                
        if not success:
            print(f"[Error] Message failed after {MAX_RETRIES} retries. Routing to DLQ: {DLQ_TOPIC}")
            dlq_message = {
                "consumer": "alert_generation_group",
                "error": last_error,
                "raw_message": raw_event_bytes.decode('utf-8', errors='replace')
            }
            producer.send(DLQ_TOPIC, key=b'dlq', value=dlq_message)
            
except KeyboardInterrupt:
    print("\nAlert Consumer stopped.")
finally:
    producer.flush()
    producer.close()
    consumer.close()
