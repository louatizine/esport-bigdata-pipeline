# Phase 3 Setup Guide

This guide will help you set up and run the Spark Structured Streaming pipeline.

## 📦 Prerequisites

- Python 3.8+
- Java 8 or 11 (required for Spark)
- Docker and Docker Compose (for Kafka)
- At least 4GB of available RAM

## 🚀 Step-by-Step Setup

### Step 1: Install PySpark

```bash
# Install from requirements file
pip install -r requirements/spark-streaming.txt

# Or install manually
pip install pyspark>=3.5.0
```

### Step 2: Verify Java Installation

Spark requires Java. Check if Java is installed:

```bash
java -version
```

If not installed, install Java:

```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install openjdk-11-jdk

# macOS
brew install openjdk@11

# Or use the dev container which already has Java
```

### Step 3: Set Environment Variables

Create a `.env` file or export variables:

```bash
# Copy the example file
cp spark/.env.example .env

# Edit the file with your values
nano .env

# Or export directly:
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export KAFKA_TOPIC_MATCHES="esports-matches"
export KAFKA_TOPIC_PLAYERS="esports-players"
export SPARK_MASTER_URL="local[*]"
export DATA_LAKE_PATH="/workspaces/esport-bigdata-pipeline/data"
export LOG_LEVEL="INFO"
```

### Step 4: Start Kafka (from Phase 2)

```bash
# Start Kafka and Zookeeper
docker-compose up -d kafka zookeeper

# Verify Kafka is running
docker ps | grep kafka
```

### Step 5: Create Kafka Topics

```bash
# Run the topic creation script from Phase 2
python scripts/create_topics.py

# Or manually:
docker exec -it kafka kafka-topics.sh --create \
  --topic esports-matches \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

docker exec -it kafka kafka-topics.sh --create \
  --topic esports-players \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

### Step 6: Validate Installation

```bash
# Run the validation script
python scripts/validate_phase3.py
```

Expected output: All checks should pass except environment variables (if not set).

### Step 7: Create Data Directories

```bash
# Create required directories
mkdir -p data/processed/matches
mkdir -p data/processed/players
mkdir -p data/checkpoints
```

### Step 8: Test with Sample Data (Optional)

```bash
# Produce some test data to Kafka
python scripts/test_kafka_producer.py
```

### Step 9: Run Streaming Jobs

**Option A: Run all jobs**
```bash
cd spark
python main.py --job all
```

**Option B: Run individual jobs**
```bash
# Match stream only
python main.py --job matches

# Player stream only
python main.py --job players
```

**Option C: Use spark-submit**
```bash
./scripts/run_spark_streaming.sh --job all --await
```

### Step 10: Verify Output

```bash
# Check if Parquet files are created
ls -lh data/processed/matches/
ls -lh data/processed/players/

# Read the data (using PySpark)
python3 << EOF
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("verify").getOrCreate()

# Check matches
matches = spark.read.parquet("data/processed/matches")
print(f"Total matches: {matches.count()}")
matches.show(5, truncate=False)

# Check players
players = spark.read.parquet("data/processed/players")
print(f"Total players: {players.count()}")
players.show(5, truncate=False)

spark.stop()
EOF
```

## 🐛 Troubleshooting

### Issue: "No module named 'pyspark'"

**Solution:**
```bash
pip install pyspark>=3.5.0
```

### Issue: "KAFKA_BOOTSTRAP_SERVERS environment variable not set"

**Solution:**
```bash
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
# Or add to .env file
```

### Issue: "Failed to read from Kafka: Connection refused"

**Solution:**
1. Check if Kafka is running: `docker ps | grep kafka`
2. Verify Kafka address: `netstat -tulpn | grep 9092`
3. Check docker-compose logs: `docker-compose logs kafka`

### Issue: "Java not found"

**Solution:**
```bash
# Install Java 11
sudo apt-get install openjdk-11-jdk

# Set JAVA_HOME
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$PATH:$JAVA_HOME/bin
```

### Issue: Spark UI not accessible

**Solution:**
1. Check if port 4040 is available
2. Try different port: `export SPARK_UI_PORT=4041`
3. Access via: `http://localhost:4040`

### Issue: "Permission denied" on data directories

**Solution:**
```bash
# Fix permissions
chmod -R 755 data/
```

### Issue: Streaming query stops immediately

**Solution:**
1. Check Kafka topics exist: `docker exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092`
2. Verify data is being produced to topics
3. Check logs for errors
4. Ensure checkpoint directory is writable

## 🔍 Monitoring

### View Spark UI
```bash
# Open in browser
http://localhost:4040

# Or check specific tabs
http://localhost:4040/jobs/
http://localhost:4040/streaming/
```

### Monitor Logs
```bash
# Run with verbose logging
export LOG_LEVEL=DEBUG
python spark/main.py --job all
```

### Check Kafka Topics
```bash
# List topics
docker exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092

# Check topic data
docker exec kafka kafka-console-consumer.sh \
  --topic esports-matches \
  --bootstrap-server localhost:9092 \
  --from-beginning \
  --max-messages 5
```

### Monitor Checkpoints
```bash
# Check checkpoint directories
ls -lh data/checkpoints/match_stream/
ls -lh data/checkpoints/player_stream/
```

## ✅ Success Criteria

Your setup is successful when:

1. ✅ Validation script passes all file structure checks
2. ✅ PySpark imports successfully
3. ✅ Kafka is running and topics are created
4. ✅ Streaming jobs start without errors
5. ✅ Parquet files appear in `data/processed/`
6. ✅ Spark UI is accessible at http://localhost:4040
7. ✅ Log messages show "Streaming query active"

## 📚 Next Steps

After successful setup:

1. **Integrate with Phase 2**: Connect to real Kafka producers
2. **Monitor Performance**: Check Spark UI metrics
3. **Tune Configuration**: Adjust memory and parallelism
4. **Add Alerting**: Set up monitoring and alerts
5. **Scale**: Move to distributed Spark cluster

## 🆘 Getting Help

If you encounter issues:

1. Check logs: `data/checkpoints/*/metadata`
2. Review Spark UI: http://localhost:4040
3. Verify Kafka connectivity: `telnet localhost 9092`
4. Check Docker logs: `docker-compose logs kafka`
5. Review Phase 3 documentation: `spark/README.md`

## 📝 Environment Variables Reference

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | Yes | - | Kafka broker addresses |
| `KAFKA_TOPIC_MATCHES` | No | `esports-matches` | Match events topic |
| `KAFKA_TOPIC_PLAYERS` | No | `esports-players` | Player events topic |
| `SPARK_MASTER_URL` | No | `local[*]` | Spark master URL |
| `DATA_LAKE_PATH` | No | `./data` | Data lake directory |
| `LOG_LEVEL` | No | `INFO` | Logging level |
| `SPARK_DRIVER_MEMORY` | No | `2g` | Driver memory |
| `SPARK_EXECUTOR_MEMORY` | No | `2g` | Executor memory |
| `SPARK_SHUFFLE_PARTITIONS` | No | `10` | Shuffle partitions |

---

**Ready to proceed?** Run `python scripts/validate_phase3.py` to check your setup!
