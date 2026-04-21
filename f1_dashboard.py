import streamlit as st
import pandas as pd
import json
import threading
import time
from kafka import KafkaConsumer

st.set_page_config(page_title="F1 Live Intelligence", page_icon="🏎️", layout="wide")

# ── Styling (Adopted from app.py) ──────────────────────────────────
st.markdown("""
<div style="text-align:center; padding:1rem 0;">
    <h1 style="color:#e10600;">🏎️ F1 Live Race Intelligence</h1>
    <p style="color:#888; font-size:1.1rem;">Real-Time Telemetry Insights • Powered by Apache Kafka & Gemini AI</p>
</div>
""", unsafe_allow_html=True)

# ── Initialize Thread-Safe State ───────────────────────────────────
# st.session_state cannot be modified from a background thread in modern Streamlit.
@st.cache_resource
def get_data_store():
    return {"events": [], "total_alerts": 0, "critical_alerts": 0, "listener_started": False}

store = get_data_store()

# ── Kafka Background Thread ────────────────────────────────────────
def consume_kafka():
    """Background thread to listen to the final ai_insights topic."""
    try:
        # Connecting to our Kafka broker
        consumer = KafkaConsumer(
            'ai_insights',
            bootstrap_servers='localhost:9092',
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='streamlit_dashboard_group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        for message in consumer:
            alert = message.value
            
            # Save to global store safely
            store["events"].insert(0, alert)
            
            if len(store["events"]) > 100:
                store["events"].pop()
                
            store["total_alerts"] += 1
            if alert.get('priority') in ['CRITICAL', 'HIGH']:
                store["critical_alerts"] += 1

    except Exception as e:
        print(f"Error in Kafka consumer thread: {e}")

# Start the daemon thread only once per session
if not store["listener_started"]:
    store["listener_started"] = True
    threading.Thread(target=consume_kafka, daemon=True).start()

# ── Auto-Refresh Toggle ────────────────────────────────────────────
st.sidebar.markdown("### ⚙️ Dashboard Controls")
auto_refresh = st.sidebar.checkbox("Live Auto-Refresh", value=True)
refresh_rate = st.sidebar.slider("Refresh Interval (seconds)", 1, 10, 2)

if st.sidebar.button("Clear Log"):
    store["events"].clear()
    store["total_alerts"] = 0
    store["critical_alerts"] = 0

# ── Top Metrics ────────────────────────────────────────────────────
col1, col2, col3 = st.columns(3)
col1.metric("Total Insights Generated", store["total_alerts"])
col2.metric("Critical/High Alerts", store["critical_alerts"])
col3.metric("Live AI Broadcasters", "Active 🟢")

st.divider()

# ── Main Feed ──────────────────────────────────────────────────────
tab1, tab2 = st.tabs(["📺 Live Broadcaster Feed", "🚨 Raw Telemetry Log"])

with tab1:
    st.subheader("Live Race Commentary")
    if len(store["events"]) == 0:
        st.info("Waiting for race events... (Ensure the producer and consumers are running)")
    else:
        for ev in store["events"]:
            driver = ev.get('driver_id', 'Unknown')
            ai_text = ev.get('ai_commentary', '')
            priority = ev.get('priority', 'LOW')
            
            # Styling based on priority
            color = "#e10600" if priority in ["CRITICAL", "HIGH"] else "#2c3e50"
            border_color = "#e10600" if priority == "CRITICAL" else "#ddd"
            
            if ai_text:
                st.markdown(f"""
                <div style="border-left: 5px solid {border_color}; padding: 10px; margin-bottom: 15px; background-color: #f9f9f9; border-radius: 5px;">
                    <h4 style="margin: 0; color: {color};">[{priority}] {driver.upper()}</h4>
                    <p style="margin: 5px 0; font-size: 1.1rem; font-style: italic; color: #333333;">🎙️ "{ai_text}"</p>
                </div>
                """, unsafe_allow_html=True)

with tab2:
    st.subheader("Raw JSON Telemetry Stream")
    if len(store["events"]) > 0:
        # Convert events to a dataframe for cleaner viewing
        df = pd.DataFrame(store["events"])
        # Drop columns that clutter the view
        display_df = df[['timestamp', 'priority', 'driver_id', 'insight_type', 'alert_message']]
        st.dataframe(display_df, use_container_width=True, hide_index=True)
    else:
        st.text("No data to display yet.")

# ── Execute Rerun ──────────────────────────────────────────────────
if auto_refresh:
    time.sleep(refresh_rate)
    st.rerun()
