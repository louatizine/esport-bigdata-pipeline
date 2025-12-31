# Phase 3: Spark Structured Streaming - Implementation Complete ✅

## 📋 Overview

Phase 3 implements a production-ready **Spark Structured Streaming** pipeline for real-time esports analytics. The system consumes data from Kafka topics, processes streaming events, and writes results to a Parquet-based data lake.

---

## 🎯 Implementation Summary

### ✅ Completed Components

1. **Folder Structure** - Clean, modular organization
2. **Schema Definitions** - Explicit schemas for matches and players
3. **Utility Modules** - Spark session management and structured logging
4. **Streaming Jobs** - Match and player stream processors
5. **Main Orchestrator** - Coordinated execution of multiple streams
6. **Documentation** - Comprehensive README and examples
7. **Helper Scripts** - Easy-to-use runner scripts

---

## 📁 Project Structure

```
spark/
├── main.py                      # Main orchestrator for all streaming jobs
├── README.md                    # Comprehensive documentation
├── .env.example                 # Environment variable template
│
├── streaming/                   # Streaming job implementations
│   ├── __init__.py
│   ├── match_stream.py         # Kafka → Spark → Parquet (matches)
│   └── player_stream.py        # Kafka → Spark → Parquet (players)
│
├── schemas/                     # Data schemas
│   ├── __init__.py
│   ├── match_schema.py         # Match data StructType definitions
│   └── player_schema.py        # Player data StructType definitions
│
└── utils/                       # Shared utilities
    ├── __init__.py
    ├── spark_session.py        # SparkSession builder & config
    └── logger.py               # Structured logging utility

requirements/
└── spark-streaming.txt         # Python dependencies

scripts/
└── run_spark_streaming.sh      # Shell script for spark-submit
```

---

## 🚀 Quick Start

### 1️⃣ Set Environment Variables

```bash
export KAFKA_BOOTSTRAP_SERVERS="kafka:9092"
export KAFKA_TOPIC_MATCHES="esports-matches"
export KAFKA_TOPIC_PLAYERS="esports-players"
export SPARK_MASTER_URL="local[*]"
export DATA_LAKE_PATH="/workspaces/esport-bigdata-pipeline/data"
export LOG_LEVEL="INFO"
```

### 2️⃣ Install Dependencies

```bash
pip install -r requirements/spark-streaming.txt
```

### 3️⃣ Run Streaming Jobs

**Option A: Using Python directly**
```bash
cd spark
python main.py --job all
```

**Option B: Using spark-submit**
```bash
./scripts/run_spark_streaming.sh --job all --await
```

**Option C: Individual jobs**
```bash
# Match stream only
python main.py --job matches

# Player stream only
python main.py --job players
```

---

## 🏗️ Architecture

### Data Flow

```
┌─────────────┐
│   Kafka     │
│   Topics    │
└──────┬──────┘
       │
       │ JSON Messages
       │
       ▼
┌─────────────────────────────────┐
│  Spark Structured Streaming     │
│  ┌───────────────────────────┐  │
│  │  1. Read from Kafka       │  │
│  │  2. Parse JSON            │  │
│  │  3. Apply Schema          │  │
│  │  4. Transform Data        │  │
│  │  5. Add Timestamps        │  │
│  └───────────────────────────┘  │
└──────────────┬──────────────────┘
               │
               │ Structured Data
               │
               ▼
┌───────────────────────────────┐
│     Data Lake (Parquet)       │
│  ┌─────────────────────────┐  │
│  │  /processed/matches/    │  │
│  │  /processed/players/    │  │
│  │  /checkpoints/          │  │
│  └─────────────────────────┘  │
└───────────────────────────────┘
```

### Component Interactions

```
main.py
  │
  ├─→ MatchStreamProcessor
  │     ├─→ read_kafka_stream()
  │     ├─→ process_stream()
  │     └─→ write_to_parquet()
  │
  └─→ PlayerStreamProcessor
        ├─→ read_kafka_stream()
        ├─→ process_stream()
        └─→ write_to_parquet()
```

---

## 📊 Features Implementation

### Match Stream Processor (`match_stream.py`)

**Input:** `KAFKA_TOPIC_MATCHES` (JSON messages)

**Processing:**
- ✅ Parse JSON with explicit schema
- ✅ Extract match metadata (ID, tournament, teams)
- ✅ Calculate duration in minutes
- ✅ Add processing timestamp
- ✅ Flag completed vs live matches
- ✅ Handle malformed records safely

**Output:** Parquet files partitioned by `status`
```
data/processed/matches/
├── status=finished/
├── status=live/
└── status=scheduled/
```

**Key Features:**
- Earliest offset consumption
- 10-second micro-batches
- Automatic checkpointing
- Schema enforcement
- Graceful error handling

---

### Player Stream Processor (`player_stream.py`)

**Input:** `KAFKA_TOPIC_PLAYERS` (JSON messages)

**Processing:**
- ✅ Parse JSON with explicit schema
- ✅ Extract player profile data
- ✅ Calculate derived metrics (KDA, win rate)
- ✅ Enrich with full name and status
- ✅ Add activity flags
- ✅ Handle missing values

**Output:** Parquet files partitioned by `status` and `role`
```
data/processed/players/
├── status=active/
│   ├── role=Top/
│   ├── role=Jungle/
│   ├── role=Mid/
│   ├── role=ADC/
│   └── role=Support/
└── status=inactive/
```

**Key Features:**
- Performance metric calculations
- Multi-level partitioning
- Null-safe transformations
- Automatic enrichment

---

### Spark Session Builder (`spark_session.py`)

**Capabilities:**
- ✅ Kafka SQL connector integration
- ✅ Optimized streaming configurations
- ✅ Docker-compatible settings
- ✅ Memory management (driver/executor)
- ✅ Kryo serialization
- ✅ Graceful shutdown support
- ✅ Configurable UI port

**Key Configurations:**
```python
{
    "spark.streaming.stopGracefullyOnShutdown": "true",
    "spark.sql.streaming.schemaInference": "false",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.shuffle.partitions": "10",
    "spark.checkpoint.compress": "true"
}
```

---

### Structured Logger (`logger.py`)

**Features:**
- ✅ Consistent formatting across all components
- ✅ Configurable log levels via `LOG_LEVEL` env var
- ✅ Context-aware logging with key-value pairs
- ✅ Timestamp inclusion
- ✅ Module-level logger instances

**Example Output:**
```
2025-12-31 14:30:45 | match_stream | INFO | Starting match streaming pipeline
2025-12-31 14:30:46 | match_stream | INFO | Successfully connected to Kafka stream
2025-12-31 14:30:47 | match_stream | INFO | Streaming query started | query_name=match_stream | query_id=abc123
```

---

### Main Orchestrator (`main.py`)

**Capabilities:**
- ✅ Run individual or all streaming jobs
- ✅ Shared SparkSession for efficiency
- ✅ Signal handling (SIGINT/SIGTERM)
- ✅ Graceful shutdown
- ✅ Query monitoring
- ✅ Command-line arguments

**CLI Options:**
```bash
--job {matches|players|all}    # Which job(s) to run
--await-termination             # Wait for query termination
```

**Usage Examples:**
```bash
# Run all jobs and monitor
python main.py --job all

# Run matches only and await
python main.py --job matches --await-termination

# Run players only
python main.py --job players
```

---

## 🔧 Configuration

### Environment Variables Reference

| Variable | Purpose | Default | Required |
|----------|---------|---------|----------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker addresses | - | ✅ Yes |
| `KAFKA_TOPIC_MATCHES` | Match events topic | `esports-matches` | No |
| `KAFKA_TOPIC_PLAYERS` | Player events topic | `esports-players` | No |
| `SPARK_MASTER_URL` | Spark master URL | `local[*]` | No |
| `DATA_LAKE_PATH` | Base data directory | `/workspaces/.../data` | No |
| `PROCESSED_DATA_PATH` | Processed data dir | `{DATA_LAKE_PATH}/processed` | No |
| `LOG_LEVEL` | Logging verbosity | `INFO` | No |
| `SPARK_DRIVER_MEMORY` | Driver memory | `2g` | No |
| `SPARK_EXECUTOR_MEMORY` | Executor memory | `2g` | No |
| `SPARK_SHUFFLE_PARTITIONS` | Shuffle parallelism | `10` | No |
| `SPARK_UI_PORT` | Spark web UI port | `4040` | No |

---

## 📝 Schema Definitions

### Match Schema Fields

```python
{
    "match_id": "string",              # Primary key
    "tournament_name": "string",       # Tournament identifier
    "team_1_name": "string",           # Team A
    "team_2_name": "string",           # Team B
    "winner_name": "string",           # Match winner
    "match_duration": "integer",       # Duration in seconds
    "team_1_kills": "integer",         # Team A kills
    "team_2_kills": "integer",         # Team B kills
    "status": "string",                # finished/live/scheduled
    "started_at": "timestamp",         # Match start time
    "finished_at": "timestamp",        # Match end time
    "processing_timestamp": "timestamp" # ETL timestamp
}
```

### Player Schema Fields

```python
{
    "player_id": "string",             # Primary key
    "summoner_name": "string",         # In-game name
    "current_team_name": "string",     # Team affiliation
    "role": "string",                  # Position (Top/Jungle/Mid/ADC/Support)
    "total_games": "integer",          # Career games
    "total_wins": "integer",           # Career wins
    "win_rate": "double",              # Win percentage
    "avg_kills": "double",             # Average kills per game
    "avg_deaths": "double",            # Average deaths per game
    "avg_assists": "double",           # Average assists per game
    "kda_ratio": "double",             # KDA metric
    "active": "boolean",               # Player status
    "processing_timestamp": "timestamp" # ETL timestamp
}
```

---

## 🛡️ Error Handling

### Implemented Safeguards

1. **Malformed JSON**
   - Logged as errors
   - Skipped gracefully
   - No job failures

2. **Missing Fields**
   - Schema allows nulls
   - Default values applied
   - Downstream validation

3. **Kafka Connection Failures**
   - Detailed error logging
   - Automatic retries (Kafka client)
   - Fail-fast on startup

4. **Write Failures**
   - Checkpointing ensures recovery
   - Exactly-once semantics
   - No data loss

5. **Graceful Shutdown**
   - SIGINT/SIGTERM handling
   - Checkpoint flushing
   - Query cleanup

---

## 📈 Monitoring & Observability

### Spark UI
Access the Spark Web UI at:
```
http://localhost:4040
```

**Available Tabs:**
- Jobs (execution progress)
- Stages (task details)
- Storage (cached data)
- Streaming (query metrics)
- SQL (query plans)

### Log Monitoring

**Key Log Messages:**
```log
# Startup
INFO | Creating SparkSession for application: EsportsAnalytics_Streaming
INFO | Starting match streaming pipeline
INFO | Streaming query started | query_name=match_stream

# Runtime
INFO | Query match_stream status | is_active=True | is_data_available=True

# Errors
ERROR | Failed to read from Kafka: Connection refused
ERROR | Query match_stream has stopped unexpectedly
```

### Query Metrics

Monitor via `StreamingQuery.status`:
```python
{
    "isActive": true,
    "isDataAvailable": true,
    "message": "Processing new data",
    "isTriggerActive": false,
    "isWatermarkPresent": false
}
```

---

## 🧪 Testing

### Manual Testing

1. **Start services**
   ```bash
   # Start Kafka (Phase 2)
   docker-compose up -d kafka
   ```

2. **Produce test data**
   ```bash
   python scripts/test_kafka_producer.py
   ```

3. **Run streaming job**
   ```bash
   cd spark
   python main.py --job all
   ```

4. **Verify output**
   ```bash
   ls -lh data/processed/matches/
   ls -lh data/processed/players/
   ```

5. **Read Parquet files**
   ```python
   from pyspark.sql import SparkSession

   spark = SparkSession.builder.appName("test").getOrCreate()

   # Read matches
   matches_df = spark.read.parquet("data/processed/matches")
   matches_df.show(10, truncate=False)

   # Read players
   players_df = spark.read.parquet("data/processed/players")
   players_df.show(10, truncate=False)
   ```

---

## 🔒 Production Readiness

### ✅ Production Features

- [x] No hardcoded values (all configuration via env vars)
- [x] Comprehensive error handling
- [x] Graceful shutdown mechanisms
- [x] Checkpointing for fault tolerance
- [x] Structured logging
- [x] Schema enforcement
- [x] Partitioned output
- [x] Modular, maintainable code
- [x] Extensive documentation
- [x] CLI support

### 🎯 Production Recommendations

1. **Resource Tuning**
   ```bash
   export SPARK_DRIVER_MEMORY="4g"
   export SPARK_EXECUTOR_MEMORY="4g"
   export SPARK_SHUFFLE_PARTITIONS="50"
   ```

2. **Monitoring Integration**
   - Connect Spark metrics to Prometheus
   - Set up Grafana dashboards
   - Configure alerting (PagerDuty, Slack)

3. **Checkpoint Storage**
   - Use HDFS, S3, or Azure Blob Storage
   - Enable checkpoint compression
   - Regular cleanup of old checkpoints

4. **Backpressure Control**
   ```python
   .option("maxOffsetsPerTrigger", 10000)
   .trigger(processingTime="30 seconds")
   ```

5. **Security**
   - Enable Kafka SSL/SASL
   - Configure Spark authentication
   - Use IAM roles for cloud storage

---

## 📚 Additional Resources

### Created Files

1. `spark/main.py` - Main orchestrator
2. `spark/streaming/match_stream.py` - Match processor
3. `spark/streaming/player_stream.py` - Player processor
4. `spark/schemas/match_schema.py` - Match schema
5. `spark/schemas/player_schema.py` - Player schema
6. `spark/utils/spark_session.py` - Spark session builder
7. `spark/utils/logger.py` - Structured logger
8. `spark/README.md` - Detailed documentation
9. `spark/.env.example` - Environment template
10. `requirements/spark-streaming.txt` - Python dependencies
11. `scripts/run_spark_streaming.sh` - Runner script
12. `PHASE3_SPARK_STREAMING.md` - This file

### References

- [Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Spark-Kafka Integration](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
- [PySpark API](https://spark.apache.org/docs/latest/api/python/)

---

## ✅ Phase 3 Checklist

- [x] Clean folder structure created
- [x] SparkSession configured for streaming + Kafka
- [x] Kafka topic consumption implemented
- [x] Explicit schemas defined
- [x] JSON parsing implemented
- [x] Data transformations applied
- [x] Processing timestamps added
- [x] Parquet output format
- [x] Append mode configured
- [x] Checkpointing enabled
- [x] Structured logging (INFO, ERROR)
- [x] Production-ready code
- [x] Modular architecture
- [x] Well-commented code
- [x] No hardcoded values
- [x] Comprehensive documentation
- [x] Helper scripts
- [x] Environment examples

---

## 🎉 Summary

**Phase 3 is complete!** The Spark Structured Streaming implementation provides:

- ✅ Real-time processing of esports events
- ✅ Fault-tolerant streaming with checkpointing
- ✅ Scalable architecture for high-volume data
- ✅ Clean, maintainable, production-ready code
- ✅ Comprehensive monitoring and logging
- ✅ Flexible configuration via environment variables
- ✅ Easy deployment with Docker compatibility

**Next Steps:**
- Integrate with ML pipelines (Phase 4)
- Add data quality checks
- Implement advanced aggregations
- Create visualization dashboards
- Set up automated testing

---

**Created:** December 31, 2025
**Status:** ✅ Complete
**Version:** 1.0.0
