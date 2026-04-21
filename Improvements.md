# F1 Alert Engine — Full Improvements & Changes List

---

## 1. Add a Dead Letter Queue (DLQ) Topic

- Create a new Kafka topic: `failed_events`
- Route any event that fails processing after N retries to this topic instead of dropping it
- Allows inspection and debugging of malformed or unprocessable messages
- Directly satisfies the SRS NFR for "no data loss" and "retry mechanisms"

---

## 2. Enrich Simulated Data in the Producer

- Add **Safety Car events** as a distinct event type in `f1_producer.py`
- Add **Virtual Safety Car (VSC)** period events
- Add **weather-triggered tire change** events
- Simulate realistic bursts and correlations (e.g., after a Safety Car, all lap times converge)
- Makes the rule engine demonstrations significantly more compelling

---

## 3. Externalize Rule Engine Thresholds to `.env`

- Move all hardcoded thresholds out of `f1_strategy_consumer.py`
- Add to `.env`:
  - `SLOW_PIT_THRESHOLD_SECONDS=25`
  - `LAP_DROP_PERCENT=3`
  - `AGGRESSIVE_MOVE_POSITION_GAIN=2`
- Makes the system configurable without touching source code

---

## 4. Define a Database Schema for the Storage Consumer

- Define at least two tables:
  - `events` — raw incoming events with timestamps and driver ID
  - `alerts` — processed alerts with priority, message, and AI explanation
- Add proper indexing by `driver_id` and `timestamp`
- This fulfills the PRD's promise of historical analysis, debugging, and replay functionality

---

## 5. Add Consumer Lag Monitoring to the Dashboard

- Display **consumer lag** (offset difference between latest message and last consumed offset) for each consumer group in `f1_dashboard.py`
- Demonstrates real-world Kafka operational awareness
- Directly supports the SRS NFR on performance monitoring

---

## 6. Add JSON Schema Validation at the Producer

- Create a `schemas/` folder in the project with `.json` schema files for each event type
- Add `jsonschema` validation in `f1_producer.py` before publishing any message to Kafka
- Prevents downstream consumers from receiving malformed or incomplete events
- Lightweight alternative to a full Kafka Schema Registry

---

## 7. Add Overtake / Position-Change Events to the Producer

- The PRD (Section 4.1) lists Overtakes and Position Changes as supported events but they appear missing from the producer
- Add position-change events to `f1_producer.py`
- Ensure the rule engine's `position gain ≥ 2 → aggressive move` logic has real data to trigger on
- This is one of the most visually compelling alerts for a demo

---

## 8. Add Explicit Consumer Group IDs to All Consumers

- Set explicit `group.id` values in each consumer (`f1_strategy_consumer.py`, `f1_alert_consumer.py`, `f1_ai_consumer.py`, storage consumer)
- Enables independent monitoring of each consumer group via Kafka logs or UI tools
- Makes it easy to prove consumer group isolation during a demo

---

## 9. Add a Docker Compose Healthcheck for the Kafka Broker

- Add a `healthcheck` block to the Kafka service in `docker-compose.yml`
- Ensures consumers do not attempt to start before the Kafka broker is fully ready
- Prevents the most common demo failure: consumer crashing on startup before Kafka is up

---

## 10. Log Topic + Partition + Offset for Every Published Message

- In `f1_producer.py`, log the topic name, partition number, and offset for every successfully produced message
- Provides visible proof that partitioning by `driver_id` is working correctly
- Useful for debugging and for demonstrating Kafka concepts to an examiner

---

## 11. Add a Race Laps Counter to the Dashboard

- Display a live laps counter in `f1_dashboard.py` (e.g., "Lap 12 / 57")
- Makes the dashboard feel like a live race broadcast rather than an infinite random event stream
- Improves the overall demo experience significantly

---

## 12. Add a 5th Consumer: `race_summary_consumer`

- Create a new consumer `f1_race_summary_consumer.py`
- At the end of a simulated race (or every N laps), aggregate all events and produce a final summary including:
  - Fastest lap and driver
  - Most pit stops
  - Most overtakes
  - Total alerts broken down by severity (Critical / High / Medium)
- Demonstrates **stateful stream processing** — a core Kafka concept
- Gives the dashboard a "race replay" capability

---

## Summary Table

| # | Change | Area | Effort |
|---|--------|------|--------|
| 1 | Make AI Consumer async | `f1_ai_consumer.py` | Medium |
| 2 | Add AI fallback template | `f1_ai_consumer.py` | Low |
| 3 | Add Dead Letter Queue topic | Architecture | Low |
| 4 | Enrich simulated producer data | `f1_producer.py` | Medium |
| 5 | Externalize rule thresholds to `.env` | `f1_strategy_consumer.py` | Low |
| 6 | Define database schema for storage | Storage Consumer | Medium |
| 7 | Add consumer lag to dashboard | `f1_dashboard.py` | Medium |
| 8 | Add JSON schema validation | `f1_producer.py` + `schemas/` | Low |
| 9 | Add overtake/position-change events | `f1_producer.py` | Low |
| 10 | Add explicit consumer group IDs | All consumers | Low |
| 11 | Add Docker Compose Kafka healthcheck | `docker-compose.yml` | Low |
| 12 | Log topic + partition + offset | `f1_producer.py` | Low |
| 13 | Add race laps counter to dashboard | `f1_dashboard.py` | Low |
| 14 | Add `race_summary_consumer` (5th consumer) | New file | High |