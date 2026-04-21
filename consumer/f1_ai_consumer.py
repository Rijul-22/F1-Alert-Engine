import os
import json
import time
from kafka import KafkaConsumer, KafkaProducer
try:
    import google.generativeai as genai
    from dotenv import load_dotenv
except ImportError:
    print("Missing requirements! Please run: pip install google-generativeai python-dotenv")
    exit(1)

# Load environment variables (API Key)
load_dotenv()
API_KEY = os.getenv("GEMINI_API_KEY")

if not API_KEY or API_KEY == "your_api_key_here":
    print("ERROR: GEMINI_API_KEY is not set in the .env file!")
    exit(1)

# Configure Gemini AI
genai.configure(api_key=API_KEY)
model = genai.GenerativeModel('gemini-1.5-flash')

# Topics
INPUT_TOPIC = 'alerts'
OUTPUT_TOPIC = 'ai_insights'
KAFKA_BROKER = 'localhost:9092'

# Initialize Consumer
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='ai_commentary_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Initialize Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"Starting F1 AI Consumer (Gemini Engine).")
print(f"Listening to '{INPUT_TOPIC}' -> Publishing AI Commentary to '{OUTPUT_TOPIC}'...\n")

def generate_ai_commentary(alert_data):
    """Hits the Gemini API to convert raw data into a broadcaster-style insight."""
    driver = alert_data.get('driver_id')
    priority = alert_data.get('priority')
    message = alert_data.get('alert_message')
    
    prompt = f"""
    You are an expert Formula 1 commentator like Martin Brundle. 
    You have just received this telemetry alert for a driver:
    Driver: {driver}
    Severity: {priority}
    Telemetry Trigger: {message}
    
    Write a short, engaging 1-2 sentence live broadcast commentary reacting to this event.
    No hashtags, keep it professional but exciting.
    """
    
    try:
        response = model.generate_content(prompt)
        return response.text.strip()
    except Exception as e:
        print(f"Gemini API Error: {e}")
        return "Broadcaster AI temporarily offline."

try:
    for message in consumer:
        alert = message.value
        
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
            
except KeyboardInterrupt:
    print("\nAI Consumer stopped.")
finally:
    producer.flush()
    producer.close()
    consumer.close()
