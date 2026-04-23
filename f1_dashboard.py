"""
🏎️ F1 Live Race Intelligence Dashboard
Run: python -m streamlit run f1_dashboard.py
"""

import streamlit as st
import pandas as pd
import json
import threading
import time
from datetime import datetime
from kafka import KafkaConsumer
from collections import Counter

st.set_page_config(page_title="F1 Live Intelligence", page_icon="🏎️", layout="wide")

# ── Styling ────────────────────────────────────────────────────────
st.markdown("""
<div style="text-align:center; padding:1rem 0;">
    <h1 style="color:#e10600;">🏎️ F1 Live Race Intelligence</h1>
    <p style="color:#888; font-size:1.1rem;">Real-Time Telemetry Insights • Powered by Apache Kafka & AI Engine</p>
</div>
""", unsafe_allow_html=True)

# ── Thread-Safe State ──────────────────────────────────────────────
@st.cache_resource
def get_data_store():
    return {
        "events": [],
        "total_alerts": 0,
        "critical_alerts": 0,
        "listener_started": False
    }

store = get_data_store()

def safe_deserialize(x):
    """Safely decode JSON, return None on failure."""
    try:
        return json.loads(x.decode('utf-8'))
    except:
        return None

# ── Kafka Background Thread ───────────────────────────────────────
def consume_kafka():
    """Background thread to listen to the ai_insights topic."""
    try:
        consumer = KafkaConsumer(
            'ai_insights',
            bootstrap_servers='localhost:9092',
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id=f'dashboard-{int(time.time())}',
            api_version=(2, 8, 1),
            value_deserializer=safe_deserialize
        )

        for message in consumer:
            alert = message.value
            if not alert or not isinstance(alert, dict):
                continue

            store["events"].insert(0, alert)

            if len(store["events"]) > 200:
                store["events"].pop()

            store["total_alerts"] += 1
            if alert.get('priority') in ['CRITICAL', 'HIGH']:
                store["critical_alerts"] += 1

    except Exception as e:
        print(f"Error in Kafka consumer thread: {e}")

# Start daemon thread once
if not store["listener_started"]:
    store["listener_started"] = True
    threading.Thread(target=consume_kafka, daemon=True).start()

# ── Sidebar Controls ──────────────────────────────────────────────
st.sidebar.markdown("### ⚙️ Dashboard Controls")
auto_refresh = st.sidebar.checkbox("Live Auto-Refresh", value=True)
refresh_rate = st.sidebar.slider("Refresh Interval (seconds)", 1, 10, 2)

if st.sidebar.button("🗑️ Clear All Data"):
    store["events"].clear()
    store["total_alerts"] = 0
    store["critical_alerts"] = 0

# ── Top Metrics ────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Insights", store["total_alerts"])
col2.metric("Critical/High", store["critical_alerts"])

# Calculate unique drivers
if store["events"]:
    unique_drivers = len(set(ev.get('driver_id', '') for ev in store["events"]))
    col3.metric("Drivers Tracked", unique_drivers)
else:
    col3.metric("Drivers Tracked", 0)

col4.metric("Pipeline Status", "Active 🟢")

st.divider()

# ── Tabs ───────────────────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs(["📺 Live Broadcaster Feed", "📊 Race Analytics", "🚨 Raw Telemetry Log"])

# ══════════════════════════════════════════════════════════════════
# TAB 1: Live Broadcaster Feed
# ══════════════════════════════════════════════════════════════════
with tab1:
    st.subheader("Live Race Commentary")
    if len(store["events"]) == 0:
        st.info("Waiting for race events... (Ensure the producer and unified consumer are running)")
    else:
        for ev in store["events"][:30]:  # Show latest 30
            driver = ev.get('driver_id', 'Unknown')
            ai_text = ev.get('ai_commentary', '')
            priority = ev.get('priority', 'LOW')

            color = "#e10600" if priority in ["CRITICAL", "HIGH"] else "#2c3e50"
            border_color = "#e10600" if priority == "CRITICAL" else ("#ff8c00" if priority == "HIGH" else "#ddd")

            if ai_text:
                st.markdown(f"""
                <div style="border-left: 5px solid {border_color}; padding: 10px; margin-bottom: 15px; background-color: #f9f9f9; border-radius: 5px;">
                    <h4 style="margin: 0; color: {color};">[{priority}] {driver.upper()}</h4>
                    <p style="margin: 5px 0; font-size: 1.1rem; font-style: italic; color: #333333;">🎙️ "{ai_text}"</p>
                    <small style="color: #999;">{ev.get('insight_type', '')}</small>
                </div>
                """, unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════
# TAB 2: Race Analytics (NEW — matching SNS style)
# ══════════════════════════════════════════════════════════════════
with tab2:
    if len(store["events"]) == 0:
        st.info("Waiting for data to build analytics...")
    else:
        df = pd.DataFrame(store["events"])

        # ── Alert Trend Line Chart ─────────────────────────────
        st.subheader("Alert Trend Over Time")
        if 'timestamp' in df.columns:
            df['time'] = pd.to_datetime(df['timestamp'], unit='s')
            df_sorted = df.sort_values('time')

            # Group by 10-second windows
            df_sorted['time_bucket'] = df_sorted['time'].dt.floor('10s')
            trend = df_sorted.groupby('time_bucket').size().reset_index(name='alert_count')
            trend = trend.set_index('time_bucket')
            st.line_chart(trend['alert_count'], color="#e10600")

        st.divider()

        # ── Priority Distribution ──────────────────────────────
        col_a, col_b = st.columns(2)

        with col_a:
            st.subheader("Priority Distribution")
            if 'priority' in df.columns:
                priority_counts = df['priority'].value_counts().reset_index()
                priority_counts.columns = ['Priority', 'Count']
                st.bar_chart(priority_counts.set_index('Priority'), color="#e10600")

        with col_b:
            st.subheader("Driver Alert Frequency")
            if 'driver_id' in df.columns:
                driver_counts = df['driver_id'].value_counts().reset_index()
                driver_counts.columns = ['Driver', 'Alerts']
                st.bar_chart(driver_counts.set_index('Driver'), color="#2c3e50")

        st.divider()

        # ── Insight Type Breakdown ─────────────────────────────
        st.subheader("Insight Type Breakdown")
        if 'insight_type' in df.columns:
            insight_counts = df['insight_type'].value_counts().reset_index()
            insight_counts.columns = ['Insight Type', 'Count']
            st.bar_chart(insight_counts.set_index('Insight Type'), color="#ff8c00")

        st.divider()

        # ── Per-Driver Summary Table ───────────────────────────
        st.subheader("Per-Driver Summary")
        if 'driver_id' in df.columns and 'priority' in df.columns:
            summary = df.groupby('driver_id').agg(
                Total_Alerts=('priority', 'count'),
                Critical=('priority', lambda x: (x == 'CRITICAL').sum()),
                High=('priority', lambda x: (x == 'HIGH').sum()),
                Medium=('priority', lambda x: (x == 'MEDIUM').sum()),
            ).reset_index().rename(columns={'driver_id': 'Driver'})
            summary = summary.sort_values('Total_Alerts', ascending=False)
            st.dataframe(summary, use_container_width=True, hide_index=True)

# ══════════════════════════════════════════════════════════════════
# TAB 3: Raw Telemetry Log
# ══════════════════════════════════════════════════════════════════
with tab3:
    st.subheader("Raw JSON Telemetry Stream")
    if len(store["events"]) > 0:
        df = pd.DataFrame(store["events"])
        cols_to_show = [c for c in ['timestamp', 'priority', 'driver_id', 'insight_type', 'alert_message'] if c in df.columns]
        if cols_to_show:
            st.dataframe(df[cols_to_show], use_container_width=True, hide_index=True)
        else:
            st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.text("No data to display yet.")

# ── Auto-Refresh ──────────────────────────────────────────────────
if auto_refresh:
    time.sleep(refresh_rate)
    st.rerun()
