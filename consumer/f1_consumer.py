import json
import time
from kafka import KafkaConsumer, KafkaProducer

# Initialize Kafka Consumer
# Connecting to the existing Kafka container
TOPIC_NAME = 'race_events'
DLQ_TOPIC = 'failed_events'
MAX_RETRIES = 3

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',  # start reading from earliest message if no offset is committed
    enable_auto_commit=True,
    group_id='f1_base_consumer_group',
    api_version=(2, 8, 1)
    # Removed value_deserializer
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    api_version=(2, 8, 1),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"Starting F1 Real-Time Consumer. Listening to topic: {TOPIC_NAME}")
print("Waiting for race events...\n")

try:
    for message in consumer:
        raw_event_bytes = message.value
        success = False
        last_error = None
        
        for attempt in range(MAX_RETRIES):
            try:
                event = json.loads(raw_event_bytes.decode('utf-8'))
                driver = event.get('driver_id', 'Unknown')
                event_type = event.get('event_type', 'unknown')
                
                # Basic parsing & printing log, which acts as the foundation for the Strategy/Alert Consumer
                print(f"[{driver}] Event: {event_type.upper()}")
                
                if event_type == "lap_time":
                    print(f"   -> Lap Time: {event.get('lap_time_seconds')} seconds")
                elif event_type == "pit_stop":
                    print(f"   -> Pit Stop Duration: {event.get('pit_stop_duration')} seconds")
                elif event_type == "position_change":
                    gained = event.get('position_gained', 0)
                    status = "gained" if gained > 0 else "lost"
                    print(f"   -> {status.capitalize()} {abs(gained)} position(s)")
                elif event_type == "telemetry":
                    print(f"   -> Speed: {event.get('speed_kmh')} km/h | Tire Deg: {event.get('tire_degradation_pct')}%")
                    
                print("-" * 40)
                
                success = True
                break
                
            except Exception as e:
                last_error = str(e)
                print(f"[Warning] Error processing message in f1_consumer: {e}. Retry {attempt + 1}/{MAX_RETRIES}")
                time.sleep(1)
                
        if not success:
            print(f"[Error] Message failed after {MAX_RETRIES} retries. Routing to DLQ: {DLQ_TOPIC}")
            dlq_message = {
                "consumer": "f1_base_consumer_group",
                "error": last_error,
                "raw_message": raw_event_bytes.decode('utf-8', errors='replace')
            }
            producer.send(DLQ_TOPIC, key=b'dlq', value=dlq_message)
            
except KeyboardInterrupt:
    print("\nConsumer stopped.")
finally:
    producer.flush()
    producer.close()
    consumer.close()
