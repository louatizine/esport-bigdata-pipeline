# Quick Start Guide

This guide will help you get the Esport Big Data Pipeline up and running in minutes.

## Prerequisites

- ✅ Docker and Docker Compose installed
- ✅ GitHub Codespaces (recommended) or local development environment
- ✅ Riot Games API key ([Get one here](https://developer.riotgames.com/))
- ✅ Python 3.11+ (included in dev container)

## 🚀 Quick Start (5 minutes)

### 1. Clone the Repository

```bash
git clone https://github.com/louatizine/esport-bigdata-pipeline.git
cd esport-bigdata-pipeline
```

### 2. Start Core Services

Start Kafka and Zookeeper:

```bash
docker compose --profile core up -d
```

**Expected output:**
```
✔ Container zookeeper  Started
✔ Container kafka      Started
```

**Wait 15 seconds** for services to fully initialize.

### 3. Install Python Dependencies

```bash
pip install -r requirements/ingestion.txt
```

### 4. Configure Riot API Key

Create your `.env` file:

```bash
cp .env.example .env
```

Edit `.env` and set your Riot API key:

```bash
RIOT_API_KEY=RGAPI-your-actual-key-here
```

### 5. Validate Installation

Run the comprehensive validation script:

```bash
PYTHONPATH=$PWD KAFKA_BROKER=localhost:9092 python src/ingestion/validate_ingestion.py
```

**Expected output:**
```
✅ PASS: Kafka Broker
✅ PASS: Topics Exist
✅ PASS: Producer
✅ PASS: Riot Api
✅ PASS: Data Flow
✅ PASS: Error Handling

Result: 6/6 tests passed
```

### 6. Ingest Data from Riot API

```bash
PYTHONPATH=$PWD python src/ingestion/riot_producer.py
```

**What happens:**
- Fetches match data from Riot API
- Publishes JSON events to Kafka topics
- Handles rate limiting automatically
- Logs all operations

## 🔍 Verify Data Flow

### Check Kafka Topics

```bash
# List all topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Expected output:
# esport-matches
# esport-players
# esport-rankings
```

### Consume Messages

```bash
# Read messages from esport-matches topic
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic esport-matches \
  --from-beginning \
  --max-messages 5
```

### Check Service Status

```bash
docker ps
```

**Expected output:**
```
CONTAINER ID   IMAGE                                 STATUS    PORTS
abc123         confluentinc/cp-kafka:7.5.0           Up        0.0.0.0:9092->9092/tcp
def456         confluentinc/cp-zookeeper:7.5.0       Up        0.0.0.0:2181->2181/tcp
```

## 🛠 Troubleshooting

### Issue: Kafka not reachable

**Symptom:** `NoBrokersAvailable` error

**Solution:**
```bash
# Restart services
docker compose --profile core down
docker compose --profile core up -d

# Wait 15 seconds
sleep 15

# Re-run validation
PYTHONPATH=$PWD KAFKA_BROKER=localhost:9092 python src/ingestion/validate_ingestion.py
```

### Issue: Riot API returns 403 Forbidden

**Symptom:** API validation fails with 403 status code

**Possible causes:**
1. Invalid API key
2. API key expired
3. Wrong region specified

**Solution:**
1. Get a new API key from [Riot Developer Portal](https://developer.riotgames.com/)
2. Update `.env` file with new key
3. Check `RIOT_PLATFORM` and `RIOT_ROUTING` values in `.env`

### Issue: Topics don't exist

**Solution:**
```bash
# Topics are auto-created by Kafka
# Or manually create them:
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic esport-matches --partitions 3 --replication-factor 1

docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic esport-players --partitions 3 --replication-factor 1

docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic esport-rankings --partitions 3 --replication-factor 1
```

### Issue: Python import errors

**Symptom:** `ModuleNotFoundError: No module named 'src'`

**Solution:**
```bash
# Always set PYTHONPATH
export PYTHONPATH=/workspaces/esport-bigdata-pipeline

# Or run with PYTHONPATH inline
PYTHONPATH=$PWD python src/ingestion/riot_producer.py
```

## 📊 Next Steps

### Start Spark Cluster (Phase 3)

```bash
docker compose --profile spark up -d
```

Access Spark UI:
- Spark Master: http://localhost:8080
- Spark Worker: http://localhost:8081

### Run Batch Jobs

```bash
PYTHONPATH=$PWD python src/batch/jobs/batch_job_template.py
```

### Deploy Streaming Jobs

```bash
PYTHONPATH=$PWD python src/streaming/jobs/streaming_job_template.py
```

### Add Monitoring (Optional)

```bash
# Start MongoDB and PostgreSQL
docker compose --profile optional up -d
```

## 🎯 Common Workflows

### Daily Data Ingestion

```bash
# Start services if not running
docker compose --profile core up -d

# Run producer for specific summoner
PYTHONPATH=$PWD python src/ingestion/riot_producer.py

# Monitor Kafka messages
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic esport-matches \
  --from-beginning
```

### Clean Restart

```bash
# Stop all services
docker compose --profile core --profile spark --profile optional down

# Remove volumes (CAUTION: deletes all data)
docker volume rm esport-bigdata-pipeline_kafka_data
docker volume rm esport-bigdata-pipeline_zookeeper_data

# Start fresh
docker compose --profile core up -d
```

### View Logs

```bash
# Kafka logs
docker logs kafka -f

# Zookeeper logs
docker logs zookeeper -f

# All services
docker compose logs -f
```

## 📖 Additional Resources

- [Architecture Documentation](ARCHITECTURE.md)
- [Ingestion Guide](INGESTION_GUIDE.md)
- [Docker Commands Reference](DOCKER_COMMANDS.md)
- [Setup Checklist](SETUP_CHECKLIST.md)
- [Phase 2 Validation Results](PHASE2_VALIDATION_RESULTS.md)

## 🤝 Getting Help

If you encounter issues:

1. Check service status: `docker ps`
2. View logs: `docker logs kafka`
3. Run validation: `python src/ingestion/validate_ingestion.py`
4. Consult [INGESTION_GUIDE.md](INGESTION_GUIDE.md)
5. Review [PHASE2_VALIDATION_RESULTS.md](PHASE2_VALIDATION_RESULTS.md)

## ✅ Success Criteria

You're ready to proceed if:

- ✅ All Docker containers are running
- ✅ Validation script shows 6/6 tests passed
- ✅ Kafka topics contain messages
- ✅ No errors in `docker logs kafka`
- ✅ Producer successfully ingests data

**Congratulations!** Your Esport Big Data Pipeline is operational. 🎉
