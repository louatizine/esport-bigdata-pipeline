# Docker Commands Quick Reference

## 🚀 Starting Services One-by-One

### 1. Start Zookeeper
```bash
docker compose --profile core up -d zookeeper
docker compose logs -f zookeeper
```

### 2. Start Kafka
```bash
docker compose --profile core up -d kafka
docker compose logs -f kafka
```

**Verify Kafka is ready:**
```bash
docker compose exec kafka kafka-broker-api-versions.sh --bootstrap-server kafka:9092
```

### 3. Create Kafka Topics
```bash
# Create a topic for match data
docker compose exec kafka kafka-topics.sh \
  --create \
  --topic esports.matches \
  --bootstrap-server kafka:9092 \
  --partitions 3 \
  --replication-factor 1

# Create a topic for player stats
docker compose exec kafka kafka-topics.sh \
  --create \
  --topic esports.player_stats \
  --bootstrap-server kafka:9092 \
  --partitions 3 \
  --replication-factor 1

# List all topics
docker compose exec kafka kafka-topics.sh --list --bootstrap-server kafka:9092
```

### 4. Start Spark Master
```bash
docker compose --profile spark up -d spark-master
docker compose logs -f spark-master
```

**Access Spark UI:** http://localhost:8080

### 5. Start Spark Worker
```bash
docker compose --profile spark up -d spark-worker
docker compose logs -f spark-worker
```

**Access Worker UI:** http://localhost:8081

### 6. Start Optional Services (MongoDB, PostgreSQL)
```bash
# Start MongoDB
docker compose --profile optional up -d mongodb

# Start PostgreSQL
docker compose --profile optional up -d postgres

# Or start both
docker compose --profile optional up -d
```

---

## 🔍 Monitoring & Debugging

### Check Running Services
```bash
docker compose ps
```

### View Logs
```bash
# All services
docker compose logs

# Specific service
docker compose logs kafka

# Follow logs in real-time
docker compose logs -f spark-master

# Last 100 lines
docker compose logs --tail=100 kafka
```

### Execute Commands Inside Containers
```bash
# Kafka shell
docker compose exec kafka bash

# Spark master shell
docker compose exec spark-master bash

# MongoDB shell
docker compose exec mongodb mongosh -u root -p root
```

---

## 🧪 Testing Kafka

### Produce Test Messages
```bash
docker compose exec kafka kafka-console-producer.sh \
  --topic esports.matches \
  --bootstrap-server kafka:9092
```

### Consume Test Messages
```bash
docker compose exec kafka kafka-console-consumer.sh \
  --topic esports.matches \
  --bootstrap-server kafka:9092 \
  --from-beginning
```

---

## 🔧 Spark Job Submission

### Submit a Streaming Job
```bash
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --conf spark.executor.memory=1g \
  /workspace/src/streaming/jobs/streaming_job_template.py
```

### Submit a Batch Job
```bash
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.executor.memory=1g \
  /workspace/src/batch/jobs/batch_job_template.py
```

---

## 🛑 Stopping & Cleaning

### Stop All Services
```bash
# Stop services (keep volumes)
docker compose --profile core --profile spark --profile optional down

# Or stop all profiles at once
docker compose down
```

### Remove Volumes (Clean Slate)
```bash
# WARNING: This deletes all data!
docker compose down -v
```

### Restart a Specific Service
```bash
docker compose restart kafka
```

### Rebuild a Service (After Config Changes)
```bash
docker compose up -d --force-recreate spark-master
```

---

## 🔁 Full Restart Sequence

```bash
# Stop everything
docker compose down

# Start core services
docker compose --profile core up -d

# Wait 10-15 seconds for Kafka to be ready
sleep 15

# Create topics
docker compose exec kafka kafka-topics.sh \
  --create --if-not-exists \
  --topic esports.matches \
  --bootstrap-server kafka:9092 \
  --partitions 3 \
  --replication-factor 1

# Start Spark
docker compose --profile spark up -d

# Verify all services
docker compose ps
```

---

## 📊 Resource Monitoring

### Check Resource Usage
```bash
docker stats
```

### Inspect Network
```bash
docker network ls
docker network inspect esport-bigdata-pipeline_default
```

### Inspect Volumes
```bash
docker volume ls
docker volume inspect esport-bigdata-pipeline_kafka_data
```

---

## 🆘 Troubleshooting

### Kafka Not Starting
```bash
# Check Zookeeper is running
docker compose logs zookeeper

# Restart Kafka
docker compose restart kafka
```

### Spark Worker Not Connecting
```bash
# Check Spark master logs
docker compose logs spark-master

# Verify worker can reach master
docker compose exec spark-worker ping spark-master
```

### Clear All Docker Resources (Nuclear Option)
```bash
# WARNING: Removes ALL Docker containers, images, volumes!
docker system prune -a --volumes
```

---

**Pro tip:** Use `docker compose watch` for auto-reloading during development (requires Docker Compose v2.22+)
