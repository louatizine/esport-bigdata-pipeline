# Architecture & Design Patterns

## 🏗 System Architecture

### High-Level Data Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                          RIOT GAMES API                             │
│                    (Match Data, Player Stats)                       │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             │ HTTP REST API calls
                             │
                             v
┌─────────────────────────────────────────────────────────────────────┐
│                      INGESTION LAYER                                │
│                  (Kafka Producers - Python)                         │
│                                                                     │
│  • riot_producer.py                                                 │
│  • Fetch data from Riot API                                         │
│  • Serialize to JSON/Avro                                           │
│  • Publish to Kafka topics                                          │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             │ Kafka Messages
                             │
                             v
┌─────────────────────────────────────────────────────────────────────┐
│                      STREAMING LAYER                                │
│                    (Apache Kafka + Zookeeper)                       │
│                                                                     │
│  Topics:                                                            │
│  • esports.matches         (match events)                           │
│  • esports.player_stats    (player performance)                     │
│  • esports.game_events     (in-game actions)                        │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             │ Kafka Consumer API
                             │
                             v
┌─────────────────────────────────────────────────────────────────────┐
│                   PROCESSING LAYER                                  │
│                (Spark Structured Streaming)                         │
│                                                                     │
│  Streaming Jobs:                                                    │
│  • Real-time match analytics                                        │
│  • Player performance scoring                                       │
│  • Win rate calculations                                            │
│  • Anomaly detection                                                │
│                                                                     │
│  Batch Jobs:                                                        │
│  • Daily aggregations                                               │
│  • Historical trend analysis                                        │
│  • Leaderboard generation                                           │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             │ Parquet writes
                             │
                             v
┌─────────────────────────────────────────────────────────────────────┐
│                      STORAGE LAYER                                  │
│                    (Data Lake - Parquet)                            │
│                                                                     │
│  Medallion Architecture:                                            │
│                                                                     │
│  [RAW]      → Raw API responses (JSON)                              │
│  [BRONZE]   → Unprocessed events from Kafka                         │
│  [SILVER]   → Cleaned, validated, deduplicated                      │
│  [GOLD]     → Aggregated business metrics                           │
│                                                                     │
│  Optional:                                                          │
│  • MongoDB (document store for metadata)                            │
│  • PostgreSQL (relational for user management)                      │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             │ Reads from Gold layer
                             │
          ┌──────────────────┴─────────────────┐
          │                                    │
          v                                    v
┌──────────────────────┐          ┌──────────────────────┐
│   ML PIPELINES       │          │   ANALYTICS LAYER    │
│                      │          │                      │
│ • Feature engineering│          │ • Dashboards         │
│ • Model training     │          │ • Reporting          │
│ • Predictions        │          │ • Alerts             │
│ • A/B testing        │          │                      │
│                      │          │ Tools:               │
│ Tools:               │          │ • Streamlit          │
│ • Spark MLlib        │          │ • Grafana            │
│ • scikit-learn       │          │ • Jupyter            │
└──────────────────────┘          └──────────────────────┘
```

---

## 🔧 Technology Stack Details

### Orchestration
- **Docker Compose**: Multi-container orchestration
- **Profiles**: `core`, `spark`, `optional` for modular deployment

### Messaging & Streaming
- **Apache Kafka 3.7**: Distributed event streaming platform
- **Zookeeper 3.9**: Kafka coordination service
- **Bitnami Images**: Production-ready, well-maintained containers

### Processing
- **Apache Spark 3.5**: Unified analytics engine
  - Structured Streaming for real-time
  - Batch for historical analysis
  - MLlib for machine learning

### Storage
- **Parquet**: Columnar storage format (optimal for analytics)
- **Data Lake**: Medallion architecture (Bronze → Silver → Gold)
- **Optional DBs**:
  - MongoDB 6: Document store
  - PostgreSQL 15: Relational database

### Development
- **Python 3.11+**: Primary language
- **GitHub Codespaces**: Cloud development environment
- **VS Code**: IDE with Docker/Python extensions

---

## 📊 Data Models

### Kafka Topics Schema

#### `esports.matches`
```json
{
  "match_id": "string",
  "game_mode": "string",
  "game_duration": "integer",
  "game_start_timestamp": "long",
  "participants": [
    {
      "summoner_name": "string",
      "champion": "string",
      "kills": "integer",
      "deaths": "integer",
      "assists": "integer",
      "team": "string"
    }
  ],
  "winning_team": "string"
}
```

#### `esports.player_stats`
```json
{
  "summoner_id": "string",
  "summoner_name": "string",
  "tier": "string",
  "rank": "string",
  "win_rate": "double",
  "total_games": "integer",
  "timestamp": "long"
}
```

---

## 🗂 Data Lake Schema

### Bronze Layer
- Raw Kafka events (unchanged)
- Partitioned by: `date`, `hour`
- Format: Parquet with snappy compression

### Silver Layer
- Cleaned and validated data
- Deduplicated by business keys
- Type conversions and standardization
- Partitioned by: `date`, `game_mode`

### Gold Layer
- Aggregated metrics
- Pre-computed KPIs
- Optimized for query performance
- Partitioned by: `date`, `metric_type`

---

## 🔄 Processing Patterns

### Streaming Pattern (Micro-batch)
```python
# Pseudo-code
spark.readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:9092") \
  .option("subscribe", "esports.matches") \
  .load() \
  .selectExpr("CAST(value AS STRING)") \
  .writeStream \
  .format("parquet") \
  .option("path", "data/bronze/matches") \
  .option("checkpointLocation", "data/checkpoints/matches") \
  .trigger(processingTime="30 seconds") \
  .start()
```

### Batch Pattern (Daily aggregation)
```python
# Pseudo-code
df = spark.read.parquet("data/silver/matches") \
  .filter(col("date") == yesterday) \
  .groupBy("game_mode", "champion") \
  .agg(
    count("*").alias("games_played"),
    avg("win_rate").alias("avg_win_rate")
  ) \
  .write.mode("append") \
  .partitionBy("date") \
  .parquet("data/gold/champion_stats")
```

---

## 🛡 Design Principles

### 1. **Separation of Concerns**
- Each module has a single responsibility
- `ingestion/` only fetches and produces
- `streaming/` only processes streams
- `batch/` only runs analytics
- `ml/` only handles models

### 2. **Configuration as Code**
- All settings in `.env` or `conf/`
- No hardcoded values in source code
- Environment-driven behavior

### 3. **Idempotency**
- Kafka topics are idempotent-safe
- Batch jobs use `mode("append")` with deduplication
- Checkpoint locations for exactly-once semantics

### 4. **Scalability**
- Horizontal: Add more Spark workers
- Vertical: Adjust memory/cores in `docker-compose.yml`
- Kafka partitions enable parallel consumption

### 5. **Observability**
- Centralized logging (`conf/logging.yaml`)
- Spark UI at http://localhost:8080
- Kafka monitoring via console tools

### 6. **Modularity**
- Docker profiles for optional components
- Separate requirements files per module
- Pluggable ML pipelines

---

## 🔐 Security Considerations

### Production Checklist
- [ ] Enable Kafka SASL/SSL authentication
- [ ] Use Spark encryption (`spark.ssl.enabled`)
- [ ] Store secrets in secrets manager (not `.env`)
- [ ] Enable network policies/firewall rules
- [ ] Use read-only mounts for configs
- [ ] Implement rate limiting on Riot API calls
- [ ] Add monitoring and alerting (Prometheus + Grafana)

---

## 📈 Scaling Strategies

### Vertical Scaling
```yaml
# In docker-compose.yml
spark-worker:
  environment:
    - SPARK_WORKER_MEMORY=4G
    - SPARK_WORKER_CORES=2
```

### Horizontal Scaling
```bash
# Add more workers
docker compose up -d --scale spark-worker=3
```

### Kafka Partitioning
```bash
# Create topics with more partitions
kafka-topics.sh --create \
  --topic esports.matches \
  --partitions 10 \
  --replication-factor 2
```

---

## 🧪 Testing Strategy

### Unit Tests
- Test individual transformations
- Mock Kafka producers/consumers
- Use `pytest` with fixtures

### Integration Tests
- Test end-to-end flows
- Use Docker Compose for test environment
- Verify data in data lake

### Performance Tests
- Measure throughput (events/second)
- Monitor Spark job execution time
- Profile memory usage

---

**Reference:** This architecture follows the Lambda Architecture pattern with a focus on real-time stream processing complemented by batch reprocessing capabilities.
