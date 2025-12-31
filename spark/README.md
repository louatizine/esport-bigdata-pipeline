# Spark Structured Streaming - Phase 3

Production-ready Spark Structured Streaming implementation for the Esports Analytics Platform.

## 📁 Structure

```
spark/
├── main.py                    # Main orchestrator
├── streaming/
│   ├── __init__.py
│   ├── match_stream.py        # Match data processor
│   └── player_stream.py       # Player data processor
├── schemas/
│   ├── __init__.py
│   ├── match_schema.py        # Match data schemas
│   └── player_schema.py       # Player data schemas
└── utils/
    ├── __init__.py
    ├── spark_session.py       # Spark configuration
    └── logger.py              # Structured logging
```

## 🚀 Quick Start

### 1. Set Environment Variables

```bash
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export KAFKA_TOPIC_MATCHES="esports-matches"
export KAFKA_TOPIC_PLAYERS="esports-players"
export SPARK_MASTER_URL="local[*]"
export DATA_LAKE_PATH="/workspaces/esport-bigdata-pipeline/data"
export LOG_LEVEL="INFO"
```

### 2. Run Streaming Jobs

**Run all jobs:**
```bash
cd /workspaces/esport-bigdata-pipeline/spark
python main.py --job all
```

**Run specific job:**
```bash
# Match stream only
python main.py --job matches

# Player stream only
python main.py --job players
```

**Run with await termination:**
```bash
python main.py --job all --await-termination
```

### 3. Run Individual Processors

```bash
# Match stream
python streaming/match_stream.py

# Player stream
python streaming/player_stream.py
```

## 📊 Features

### Match Stream Processor
- ✅ Reads from Kafka topic (configurable via `KAFKA_TOPIC_MATCHES`)
- ✅ Parses JSON with explicit schema validation
- ✅ Adds processing timestamp
- ✅ Calculates match duration in minutes
- ✅ Flags completed matches
- ✅ Writes to Parquet with partitioning by status
- ✅ Checkpointing for fault tolerance

### Player Stream Processor
- ✅ Reads from Kafka topic (configurable via `KAFKA_TOPIC_PLAYERS`)
- ✅ Parses JSON with explicit schema validation
- ✅ Calculates win rate and KDA if missing
- ✅ Enriches with full name and status
- ✅ Adds activity flags
- ✅ Writes to Parquet with partitioning by status and role
- ✅ Checkpointing for fault tolerance

### Spark Configuration
- ✅ Kafka integration with Spark SQL connector
- ✅ Graceful shutdown handling
- ✅ Memory configuration (driver/executor)
- ✅ Kryo serialization
- ✅ Optimized for Docker environments
- ✅ Configurable shuffle partitions

### Logging
- ✅ Structured logging with timestamps
- ✅ Configurable log levels (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- ✅ Context-aware logging with key-value pairs
- ✅ Consistent formatting across all components

## 🔧 Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka bootstrap servers | Required |
| `KAFKA_TOPIC_MATCHES` | Match data topic | `esports-matches` |
| `KAFKA_TOPIC_PLAYERS` | Player data topic | `esports-players` |
| `SPARK_MASTER_URL` | Spark master URL | `local[*]` |
| `DATA_LAKE_PATH` | Base path for data lake | `/workspaces/esport-bigdata-pipeline/data` |
| `PROCESSED_DATA_PATH` | Processed data path | `{DATA_LAKE_PATH}/processed` |
| `LOG_LEVEL` | Logging level | `INFO` |
| `SPARK_DRIVER_MEMORY` | Driver memory | `2g` |
| `SPARK_EXECUTOR_MEMORY` | Executor memory | `2g` |
| `SPARK_SHUFFLE_PARTITIONS` | Shuffle partitions | `10` |
| `SPARK_UI_PORT` | Spark UI port | `4040` |

### Data Output

**Matches:**
```
{DATA_LAKE_PATH}/processed/matches/
├── status=finished/
│   └── part-00000-*.parquet
├── status=live/
│   └── part-00000-*.parquet
└── status=scheduled/
    └── part-00000-*.parquet
```

**Players:**
```
{DATA_LAKE_PATH}/processed/players/
├── status=active/
│   ├── role=Top/
│   ├── role=Jungle/
│   ├── role=Mid/
│   ├── role=ADC/
│   └── role=Support/
└── status=inactive/
    └── ...
```

### Checkpoints

Checkpoints are stored at:
```
{DATA_LAKE_PATH}/checkpoints/match_stream/
{DATA_LAKE_PATH}/checkpoints/player_stream/
```

## 📝 Schema Definitions

### Match Schema
- Match identifiers (match_id, game_id, platform_game_id)
- Tournament information (tournament_id, name, version, patch)
- Team information (team IDs, names)
- Match results (winner, duration, statistics)
- Status and timing (status, started_at, finished_at)
- Metadata (data_source, ingestion_timestamp)

### Player Schema
- Player identifiers (player_id, summoner_name, game_name)
- Personal information (name, nationality)
- Team information (current_team_id, name, role)
- Career statistics (games, wins, losses, win_rate)
- Performance metrics (KDA, CS/min, gold/min)
- Status flags (active, professional)
- Metadata (last_match_date, profile_updated_at)

## 🐛 Error Handling

- **Malformed JSON**: Logged as errors, skipped in processing
- **Missing fields**: Handled with nullable schema fields
- **Kafka connection failures**: Logged and raised with details
- **Write failures**: Checkpointing ensures recovery
- **Graceful shutdown**: SIGINT/SIGTERM handled properly

## 📈 Monitoring

### Spark UI
Access at: `http://localhost:4040` (or configured `SPARK_UI_PORT`)

### Logs
All components log to stdout with structured formatting:
```
2025-12-31 10:30:45 | match_stream | INFO | Processing match stream data
2025-12-31 10:30:45 | match_stream | INFO | Stream processing completed successfully
```

### Query Status
Monitor streaming queries via logs:
```
Query match_stream status | is_active=True | is_data_available=True | message=Processing new data
```

## 🧪 Testing

### Test with Sample Data

1. **Start Kafka** (if not running)
2. **Produce test data** to Kafka topics
3. **Run streaming job**
4. **Verify output** in data lake

```bash
# Check output files
ls -lh /workspaces/esport-bigdata-pipeline/data/processed/matches/
ls -lh /workspaces/esport-bigdata-pipeline/data/processed/players/

# Read Parquet files
python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test').getOrCreate()
df = spark.read.parquet('/workspaces/esport-bigdata-pipeline/data/processed/matches')
df.show(10, truncate=False)
"
```

## 🔒 Production Considerations

1. **Resource Management**: Adjust `SPARK_DRIVER_MEMORY` and `SPARK_EXECUTOR_MEMORY` based on workload
2. **Checkpointing**: Ensure checkpoint directory is on reliable storage
3. **Monitoring**: Integrate with monitoring tools (Prometheus, Grafana)
4. **Error Handling**: Configure alerting for streaming failures
5. **Backpressure**: Tune `maxOffsetsPerTrigger` based on throughput
6. **Partitioning**: Adjust partition columns based on query patterns

## 📚 References

- [Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Spark-Kafka Integration](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
- [PySpark API Documentation](https://spark.apache.org/docs/latest/api/python/)
