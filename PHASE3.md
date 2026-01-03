# Phase 3: Spark Structured Streaming

## Overview

Phase 3 implements real-time data processing using Apache Spark Structured Streaming. It consumes data from Kafka topics, processes it, and writes results to the Data Lake in Parquet format with partitioning.

## Table of Contents

- [Implementation Status](#implementation-status)
- [Architecture](#architecture)
- [Setup Guide](#setup-guide)
- [Components](#components)
- [Usage](#usage)
- [Deliverables](#deliverables)
- [Validation](#validation)

---

## Implementation Status

**Status:** ✅ COMPLETE

All requirements for Phase 3 have been successfully implemented and validated.

---

## Architecture

### Data Flow

```
Kafka Topics → Spark Streaming → Data Lake (Parquet)
     ↓              ↓                   ↓
 match-topic    Match Stream       data/silver/matches/
 player-topic   Player Stream      data/silver/players/
```

### Components

1. **Streaming Jobs**
   - Match Stream Processor
   - Player Stream Processor

2. **Schema Management**
   - Match schemas (standard and nested)
   - Player schemas (standard and performance)

3. **Utilities**
   - Spark session builder
   - Logger configuration
   - Metrics collection

---

## Setup Guide

### Prerequisites

- Python 3.8+
- Java 8 or 11 (required for Spark)
- Docker and Docker Compose (for Kafka)
- At least 4GB of available RAM

### Installation Steps

#### 1. Install Dependencies

```bash
# Install from requirements file
pip install -r requirements/spark-streaming.txt

# Or install manually
pip install pyspark>=3.5.0
```

#### 2. Verify Java Installation

```bash
java -version
```

If not installed:

```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install openjdk-11-jdk

# macOS
brew install openjdk@11
```

#### 3. Configure Environment

```bash
# Copy example environment file
cp .env.example .env

# Edit the .env file with your settings
```

Key environment variables:
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker address (default: localhost:9092)
- `SPARK_MASTER`: Spark master URL (default: local[*])
- `DATA_LAKE_PATH`: Output path for processed data

#### 4. Start Kafka

```bash
# Start Kafka and Zookeeper
docker-compose up -d kafka zookeeper

# Create topics
python scripts/create_topics.py
```

---

## Components

### Main Application

**File:** `spark/main.py`

Main orchestrator for all streaming jobs:
- Supports running individual or all jobs
- Graceful shutdown handling
- CLI argument support
- Query monitoring

```bash
# Run all jobs
python spark/main.py

# Run specific job
python spark/main.py --job match
python spark/main.py --job player
```

### Streaming Jobs

#### Match Stream Processor

**File:** `spark/streaming/match_stream.py`

Features:
- Kafka consumption with earliest offset
- JSON parsing with explicit schema
- Data transformations and enrichment
- Parquet output with partitioning by date
- Checkpointing enabled

Key transformations:
- Parse JSON from Kafka value
- Extract match metadata
- Calculate match duration
- Partition by match date

#### Player Stream Processor

**File:** `spark/streaming/player_stream.py`

Features:
- Kafka consumption with earliest offset
- JSON parsing with explicit schema
- Calculated metrics (KDA, win rate)
- Multi-level partitioning (region, role)
- Checkpointing enabled

Key transformations:
- Parse JSON from Kafka value
- Calculate KDA ratio
- Compute win rates
- Extract performance metrics

### Schema Definitions

#### Match Schemas

**File:** `spark/schemas/match_schema.py`

Functions:
- `get_match_schema()`: Standard match schema
- `get_match_nested_schema()`: Detailed match schema with nested fields

Fields include:
- Match ID, date, duration, mode, map
- Winner, region, tournament info
- Team compositions
- Timestamps

#### Player Schemas

**File:** `spark/schemas/player_schema.py`

Functions:
- `get_player_schema()`: Standard player schema
- `get_player_performance_schema()`: Performance metrics schema

Fields include:
- Player ID, summoner name, region
- Champion, role, level
- KDA, CS, gold, damage stats
- Career statistics

### Utilities

#### Spark Session Builder

**File:** `spark/utils/spark_session.py`

Functions:
- `create_spark_session()`: Creates configured Spark session with Kafka support
- `get_kafka_bootstrap_servers()`: Returns Kafka configuration
- `get_checkpoint_location()`: Manages checkpoint directories

Features:
- Kafka package integration
- Configurable Spark settings
- Checkpoint management

#### Logger

**File:** `spark/utils/logger.py`

Provides structured logging for Spark applications with configurable log levels and formatting.

---

## Usage

### Quick Start

```bash
# 1. Start services
docker-compose up -d

# 2. Create Kafka topics
python scripts/create_topics.py

# 3. Start streaming (optional: send test data)
python scripts/test_kafka_producer.py &

# 4. Run Spark streaming
python spark/main.py
```

### Using spark-submit

```bash
# Submit match stream
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  spark/main.py --job match

# Submit all jobs
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  spark/main.py
```

### Monitoring

Check streaming queries in the Spark UI (default: http://localhost:4040):
- Query progress
- Processing rates
- Input/output metrics
- Checkpoints status

### Validation

```bash
# Run validation script
python scripts/validate_phase3.py

# Check output data
ls -lh data/silver/matches/
ls -lh data/silver/players/

# Read Parquet files
python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet('data/silver/matches/')
df.show()
df.printSchema()
"
```

---

## Deliverables

### ✅ Implementation Files

1. **Main Application**
   - [spark/main.py](spark/main.py) - Streaming orchestrator

2. **Streaming Jobs**
   - [spark/streaming/match_stream.py](spark/streaming/match_stream.py) - Match processor
   - [spark/streaming/player_stream.py](spark/streaming/player_stream.py) - Player processor

3. **Schema Definitions**
   - [spark/schemas/match_schema.py](spark/schemas/match_schema.py) - Match schemas
   - [spark/schemas/player_schema.py](spark/schemas/player_schema.py) - Player schemas

4. **Utilities**
   - [spark/utils/spark_session.py](spark/utils/spark_session.py) - Spark configuration
   - [spark/utils/logger.py](spark/utils/logger.py) - Logging utility

### ✅ Configuration

- [requirements/spark-streaming.txt](requirements/spark-streaming.txt) - Python dependencies
- [.env.example](.env.example) - Environment template

### ✅ Scripts & Tools

- [scripts/run_spark_streaming.sh](scripts/run_spark_streaming.sh) - Shell script for spark-submit
- [scripts/validate_phase3.py](scripts/validate_phase3.py) - Validation script
- [scripts/quickstart_phase3.py](scripts/quickstart_phase3.py) - Quick start helper

---

## Validation

### Automated Validation

Run the validation script:

```bash
python scripts/validate_phase3.py
```

Checks:
- ✅ Required files exist
- ✅ Kafka topics are created
- ✅ Schemas are valid
- ✅ Output directories exist
- ✅ Parquet files are readable
- ✅ Data partitioning is correct

### Manual Verification

1. **Check Kafka Topics:**
   ```bash
   docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
   ```

2. **Verify Data Output:**
   ```bash
   # Check Silver layer
   tree data/silver/

   # Count records
   python -c "
   from pyspark.sql import SparkSession
   spark = SparkSession.builder.getOrCreate()
   matches = spark.read.parquet('data/silver/matches/').count()
   players = spark.read.parquet('data/silver/players/').count()
   print(f'Matches: {matches}, Players: {players}')
   "
   ```

3. **Monitor Streaming Queries:**
   - Access Spark UI: http://localhost:4040
   - Check "Streaming" tab
   - Verify processing rates and checkpoint progress

---

## Troubleshooting

### Common Issues

**Issue:** Java not found
```bash
# Solution: Install Java
sudo apt-get install openjdk-11-jdk
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

**Issue:** Kafka connection refused
```bash
# Solution: Ensure Kafka is running
docker-compose ps
docker-compose up -d kafka zookeeper
```

**Issue:** Checkpoint already exists
```bash
# Solution: Clear checkpoints (only in development)
rm -rf data/checkpoints/*
```

**Issue:** Out of memory
```bash
# Solution: Increase Spark driver memory
export SPARK_DRIVER_MEMORY=4g
```

---

## Performance Tuning

### Spark Configuration

Adjust in `spark/utils/spark_session.py`:

```python
spark = SparkSession.builder \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.streaming.backpressure.enabled", "true") \
    .config("spark.sql.streaming.checkpointLocation", checkpoint_path)
```

### Kafka Configuration

```python
df = spark.readStream \
    .format("kafka") \
    .option("maxOffsetsPerTrigger", 10000) \
    .option("startingOffsets", "earliest")
```

---

## Next Steps

Phase 3 Complete! ✅

**Ready for Phase 4: Analytics & Batch Processing**
- Implement analytical queries
- Create aggregations and metrics
- Build reporting pipelines

See [PHASE4.md](PHASE4.md) for details.

---

## References

- [Apache Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Kafka Integration Guide](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
- [PySpark SQL Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html)
