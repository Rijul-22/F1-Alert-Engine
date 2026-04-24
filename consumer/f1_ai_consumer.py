import json
import time
import random
from kafka import KafkaConsumer, KafkaProducer

# Topics
INPUT_TOPIC = 'alerts'
OUTPUT_TOPIC = 'ai_insights'
DLQ_TOPIC = 'failed_events'
KAFKA_BROKER = 'localhost:9092'
MAX_RETRIES = 3

# Initialize Consumer
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='ai_commentary_group',
    api_version=(2, 8, 1)
    # Removed value_deserializer
)

# Initialize Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    api_version=(2, 8, 1),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"Starting F1 AI Consumer (Local Broadcaster Engine).")
print(f"Listening to '{INPUT_TOPIC}' -> Publishing Contextual Commentary to '{OUTPUT_TOPIC}'...\n")

def generate_ai_commentary(alert_data):
    """Local Mock AI to bypass all Rate Limits for the demonstration."""
    driver = alert_data.get('driver_id')
    priority = alert_data.get('priority')
    message = alert_data.get('alert_message')
    
    # Simulate a tiny processing delay
    time.sleep(1)
    
    if "pit stop" in message.lower():
        templates = [
            f"Oh dear, a painfully slow stop for {driver}! The mechanics are struggling, and that's going to cost them dearly out on track.",
            f"Disaster in the pit lane for {driver}! Every second feels like a lifetime when you're strapped in there watching the clock.",
            f"{driver} is stationary for far too long! The team has really fumbled that tire change."
        ]
    elif "position" in message.lower() or "aggressive move" in message.lower():
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

try:
    for message in consumer:
        raw_event_bytes = message.value
        success = False
        last_error = None
        
        for attempt in range(MAX_RETRIES):
            try:
                alert = json.loads(raw_event_bytes.decode('utf-8'))
                
                # Only process alerts that are flagged as needing AI (High/Critical)
                if alert.get('requires_ai', False):
                    driver = alert.get('driver_id', 'Unknown')
                    print(f"---\nProcessing AI insight for {driver} ({alert.get('priority')})...")
                    
                    # Generate the commentary
                    ai_text = generate_ai_commentary(alert)
                    print(f"🎙️ Commentary: {ai_text}")
                    
                    # Enrich the alert and push it to the final topic
                    alert['ai_commentary'] = ai_text
                    alert['processed_at'] = time.time()
                    
                    producer.send(OUTPUT_TOPIC, key=driver.encode('utf-8'), value=alert)
                
                success = True
                break
                
            except Exception as e:
                last_error = str(e)
                print(f"[Warning] Error processing message in ai_consumer: {e}. Retry {attempt + 1}/{MAX_RETRIES}")
                time.sleep(1)
                
        if not success:
            print(f"[Error] Message failed after {MAX_RETRIES} retries. Routing to DLQ: {DLQ_TOPIC}")
            dlq_message = {
                "consumer": "ai_commentary_group",
                "error": last_error,
                "raw_message": raw_event_bytes.decode('utf-8', errors='replace')
            }
            producer.send(DLQ_TOPIC, key=b'dlq', value=dlq_message)
            
except KeyboardInterrupt:
    print("\nAI Consumer stopped.")
finally:
    producer.flush()
    producer.close()
    consumer.close()
