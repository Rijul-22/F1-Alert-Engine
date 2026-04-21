import json
import time
import random
from kafka import KafkaProducer

# Initialize Kafka Producer
# We are using the existing Kafka container running on localhost:9092
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'race_events'

DRIVERS = ["Hamilton", "Verstappen", "Leclerc", "Norris", "Alonso", "Perez"]
EVENT_TYPES = ["lap_time", "pit_stop", "position_change", "telemetry"]

def generate_race_event():
    """Simulates a race event based on the PRD specification."""
    driver = random.choice(DRIVERS)
    event_type = random.choice(EVENT_TYPES)
    
    event = {
        "timestamp": time.time(),
        "driver_id": driver,
        "event_type": event_type,
    }
    
    if event_type == "lap_time":
        event["lap_time_seconds"] = round(random.uniform(70.0, 95.0), 3)
    elif event_type == "pit_stop":
        event["pit_stop_duration"] = round(random.uniform(2.0, 5.0), 2)
    elif event_type == "position_change":
        event["position_gained"] = random.choice([-1, 1, 2])
    elif event_type == "telemetry":
        event["speed_kmh"] = round(random.uniform(100.0, 340.0), 2)
        event["tire_degradation_pct"] = round(random.uniform(0.1, 5.0), 2)
        
    return event

print(f"Starting F1 Real-Time Producer. Publishing to topic: {TOPIC_NAME}")
print("Press Ctrl+C to stop.")

try:
    while True:
        # Generate a simulated event
        race_event = generate_race_event()
        
        # Publish to Kafka
        # Partitioning by driver_id as requested in the SRS to maintain order for a specific driver
        producer.send(TOPIC_NAME, key=race_event['driver_id'].encode('utf-8'), value=race_event)
        print(f"Produced: {race_event}")
        
        # Sleep to simulate real-time stream
        time.sleep(random.uniform(0.5, 2.0))
        
except KeyboardInterrupt:
    print("\nProducer stopped.")
finally:
    producer.flush()
    producer.close()
