import json
from kafka import KafkaConsumer

# Initialize Kafka Consumer
# Connecting to the existing Kafka container
TOPIC_NAME = 'race_events'

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',  # start reading from earliest message if no offset is committed
    enable_auto_commit=True,
    group_id='f1_base_consumer_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"Starting F1 Real-Time Consumer. Listening to topic: {TOPIC_NAME}")
print("Waiting for race events...\n")

try:
    for message in consumer:
        event = message.value
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
        
except KeyboardInterrupt:
    print("\nConsumer stopped.")
finally:
    consumer.close()
