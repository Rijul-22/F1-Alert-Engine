import json
import time
import random
from kafka import KafkaProducer

# Initialize Kafka Producer
# We are using the existing Kafka container running on localhost:9092
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    api_version=(2, 8, 1),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'race_events'

DRIVERS = ["Hamilton", "Verstappen", "Leclerc", "Norris", "Alonso", "Perez"]
EVENT_TYPES = ["lap_time", "pit_stop", "position_change", "telemetry"]

# Global Race State
race_state = {
    "status": "Green", # Green, SC, VSC
    "weather": "Sunny", # Sunny, Rain
    "status_duration": 0
}

def update_race_state():
    """Randomly update global race conditions to simulate bursts and correlations"""
    if race_state["status_duration"] > 0:
        race_state["status_duration"] -= 1
        if race_state["status_duration"] == 0:
            race_state["status"] = "Green"
            return {"event_type": "race_control", "message": "Green Flag"}

    if race_state["status"] == "Green":
        rand = random.random()
        if rand < 0.05: # 5% chance of SC
            race_state["status"] = "SC"
            race_state["status_duration"] = random.randint(4, 8)
            return {"event_type": "race_control", "message": "Safety Car Deployed"}
        elif rand < 0.10: # 5% chance of VSC
            race_state["status"] = "VSC"
            race_state["status_duration"] = random.randint(2, 5)
            return {"event_type": "race_control", "message": "Virtual Safety Car Deployed"}
            
    if race_state["weather"] == "Sunny" and random.random() < 0.02:
        race_state["weather"] = "Rain"
        return {"event_type": "weather_alert", "message": "Rain detected, track is wet"}
    elif race_state["weather"] == "Rain" and random.random() < 0.02:
        race_state["weather"] = "Sunny"
        return {"event_type": "weather_alert", "message": "Rain stopped, track is drying"}
        
    return None

def generate_race_event():
    """Simulates a race event based on the PRD specification."""
    # 1. First see if there is a global state change
    state_event = update_race_state()
    if state_event:
        return {
            "timestamp": time.time(),
            "driver_id": "RACE_CONTROL",
            "event_type": state_event["event_type"],
            "message": state_event["message"]
        }

    # 2. Otherwise generate a regular driver event
    driver = random.choice(DRIVERS)
    
    # Simulate weather-triggered tire change events
    if race_state["weather"] == "Rain" and random.random() < 0.1:
        return {
            "timestamp": time.time(),
            "driver_id": driver,
            "event_type": "pit_stop",
            "pit_stop_duration": round(random.uniform(2.2, 4.5), 2),
            "reason": "weather_tire_change",
            "tire_compound": "Wet"
        }

    event_type = random.choice(EVENT_TYPES)
    
    event = {
        "timestamp": time.time(),
        "driver_id": driver,
        "event_type": event_type,
    }
    
    if event_type == "lap_time":
        if race_state["status"] in ["SC", "VSC"]:
            # Realistic correlation: all lap times converge to a slow time behind Safety Car
            event["lap_time_seconds"] = round(random.uniform(115.0, 120.0), 3)
        elif race_state["weather"] == "Rain":
            event["lap_time_seconds"] = round(random.uniform(85.0, 95.0), 3)
        else:
            event["lap_time_seconds"] = round(random.uniform(70.0, 75.0), 3)
    elif event_type == "pit_stop":
        event["pit_stop_duration"] = round(random.uniform(2.0, 5.0), 2)
        event["tire_compound"] = "Soft" if race_state["weather"] == "Sunny" else "Intermediate"
    elif event_type == "position_change":
        if race_state["status"] in ["SC", "VSC"]:
            # No overtaking under SC
            event["position_gained"] = 0
        else:
            event["position_gained"] = random.choice([-1, 1, 2])
    elif event_type == "telemetry":
        if race_state["status"] in ["SC", "VSC"]:
            event["speed_kmh"] = round(random.uniform(100.0, 160.0), 2)
        else:
            event["speed_kmh"] = round(random.uniform(200.0, 340.0), 2)
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
        time.sleep(4)
        
except KeyboardInterrupt:
    print("\nProducer stopped.")
finally:
    producer.flush()
    producer.close()
