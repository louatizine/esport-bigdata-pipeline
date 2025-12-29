# Project Setup Checklist

## ✅ Initial Setup (One-Time)

- [ ] **Copy environment template**
  ```bash
  cp .env.example .env
  ```

- [ ] **Add your Riot API key to `.env`**
  ```bash
  # Edit .env and replace:
  RIOT_API_KEY=your_actual_riot_api_key_here
  ```

- [ ] **Verify Docker is running**
  ```bash
  docker --version
  docker compose version
  ```

---

## 🐳 Start Core Services

- [ ] **Start Zookeeper**
  ```bash
  docker compose --profile core up -d zookeeper
  ```
  Wait ~10 seconds, then verify:
  ```bash
  docker compose logs zookeeper | grep "binding to port"
  ```

- [ ] **Start Kafka**
  ```bash
  docker compose --profile core up -d kafka
  ```
  Wait ~15 seconds, then verify:
  ```bash
  docker compose exec kafka kafka-broker-api-versions.sh --bootstrap-server kafka:9092
  ```

- [ ] **Create Kafka topics**
  ```bash
  # Matches topic
  docker compose exec kafka kafka-topics.sh \
    --create --if-not-exists \
    --topic esports.matches \
    --bootstrap-server kafka:9092 \
    --partitions 3 \
    --replication-factor 1

  # Player stats topic
  docker compose exec kafka kafka-topics.sh \
    --create --if-not-exists \
    --topic esports.player_stats \
    --bootstrap-server kafka:9092 \
    --partitions 3 \
    --replication-factor 1

  # Verify
  docker compose exec kafka kafka-topics.sh --list --bootstrap-server kafka:9092
  ```

---

## ⚡ Start Spark Cluster

- [ ] **Start Spark Master**
  ```bash
  docker compose --profile spark up -d spark-master
  ```
  Verify at: http://localhost:8080

- [ ] **Start Spark Worker**
  ```bash
  docker compose --profile spark up -d spark-worker
  ```
  Verify at: http://localhost:8081

---

## 🗄️ Optional: Start Databases

- [ ] **Start MongoDB** (optional)
  ```bash
  docker compose --profile optional up -d mongodb
  ```
  Connect: `mongodb://root:root@localhost:27017`

- [ ] **Start PostgreSQL** (optional)
  ```bash
  docker compose --profile optional up -d postgres
  ```
  Connect: `postgresql://postgres:postgres@localhost:5432/esports`

---

## 🐍 Install Python Dependencies

- [ ] **Install ingestion dependencies**
  ```bash
  pip install -r requirements/ingestion.txt
  ```

- [ ] **Install Spark job dependencies**
  ```bash
  pip install -r requirements/spark.txt
  ```

- [ ] **Install ML dependencies**
  ```bash
  pip install -r requirements/ml.txt
  ```

- [ ] **Install dev tools**
  ```bash
  pip install -r requirements/dev.txt
  ```

---

## 🧪 Verify Everything Works

- [ ] **Check all services are running**
  ```bash
  docker compose ps
  ```
  Expected: zookeeper, kafka, spark-master, spark-worker all "Up"

- [ ] **Test Kafka producer/consumer**
  
  Terminal 1 (Consumer):
  ```bash
  docker compose exec kafka kafka-console-consumer.sh \
    --topic esports.matches \
    --bootstrap-server kafka:9092 \
    --from-beginning
  ```
  
  Terminal 2 (Producer):
  ```bash
  docker compose exec kafka kafka-console-producer.sh \
    --topic esports.matches \
    --bootstrap-server kafka:9092
  ```
  Type a test message and press Enter. It should appear in Terminal 1.

- [ ] **Check Spark cluster**
  - Master UI: http://localhost:8080 (should show 1 worker)
  - Worker UI: http://localhost:8081 (should show connected to master)

---

## 📊 Monitor Resources

- [ ] **Check Docker resource usage**
  ```bash
  docker stats
  ```

- [ ] **View service logs**
  ```bash
  # All services
  docker compose logs

  # Specific service
  docker compose logs -f kafka
  ```

---

## 🛑 Shutdown Checklist

- [ ] **Stop all services gracefully**
  ```bash
  docker compose down
  ```

- [ ] **Remove volumes (clean slate)**
  ```bash
  # WARNING: Deletes all data!
  docker compose down -v
  ```

---

## 🔄 Daily Development Workflow

1. **Start services** (if not already running)
   ```bash
   docker compose --profile core --profile spark up -d
   ```

2. **Check logs for errors**
   ```bash
   docker compose logs --tail=50
   ```

3. **Develop your code** in `src/`

4. **Test your producer**
   ```bash
   python src/ingestion/riot_producer.py
   ```

5. **Submit Spark job**
   ```bash
   docker compose exec spark-master spark-submit \
     --master spark://spark-master:7077 \
     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
     /workspace/src/streaming/jobs/your_job.py
   ```

6. **Stop services when done**
   ```bash
   docker compose down
   ```

---

## 🆘 Troubleshooting

### Kafka won't start
```bash
# Check Zookeeper first
docker compose logs zookeeper

# Restart Kafka
docker compose restart kafka
```

### Spark worker not connecting
```bash
# Check master logs
docker compose logs spark-master

# Verify network
docker compose exec spark-worker ping spark-master
```

### Permission denied on scripts
```bash
chmod +x scripts/*.sh
```

### Clean slate restart
```bash
# Stop and remove everything
docker compose down -v

# Start fresh
docker compose --profile core up -d
# Wait 15 seconds
docker compose --profile spark up -d
```

---

**Reference:** See [DOCKER_COMMANDS.md](./DOCKER_COMMANDS.md) for detailed command documentation.
