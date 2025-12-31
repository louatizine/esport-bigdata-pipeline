# 🧪 Complete Testing Guide - Phase 1 through Phase 5

**Last Updated:** December 31, 2024
**Purpose:** Validate all implemented phases work correctly

---

## 📋 Pre-Testing Checklist

### Required Infrastructure
- [ ] Docker and Docker Compose installed
- [ ] Python 3.11+ installed
- [ ] Sufficient disk space (5GB+)
- [ ] Riot Games API key (optional for Phase 2)

### Environment Setup
```bash
# Navigate to project root
cd /workspaces/esport-bigdata-pipeline

# Check Docker is running
docker --version
docker compose version

# Check Python
python --version
```

---

## 🔄 Phase-by-Phase Testing

### **Phase 1: Infrastructure Setup**

#### What to Test
- Docker Compose services (Kafka, Zookeeper)
- Directory structure
- Configuration files

#### Commands
```bash
# 1. Start Zookeeper
docker compose --profile core up -d zookeeper

# 2. Verify Zookeeper is running
docker compose logs zookeeper | tail -20

# 3. Start Kafka
docker compose --profile core up -d kafka

# 4. Verify Kafka is running
docker compose logs kafka | tail -20

# 5. Test Kafka broker
docker compose exec kafka kafka-broker-api-versions.sh --bootstrap-server kafka:9092
```

#### Expected Results
```
✅ Zookeeper container running on port 2181
✅ Kafka container running on port 9092
✅ Kafka broker responds to API version query
✅ No error messages in logs
```

#### Verify Directory Structure
```bash
ls -la data/
# Should see: raw/ bronze/ silver/ gold/

ls -la conf/
# Should see: logging.yaml kafka/ spark/
```

---

### **Phase 2: Kafka Data Ingestion**

#### What to Test
- Kafka topic creation
- Data ingestion (optional - requires Riot API key)
- Topic validation

#### Commands
```bash
# 1. Create Kafka topics
python scripts/create_topics.py

# Expected output:
# ✅ Topic 'esports-matches' created
# ✅ Topic 'esports-players' created

# 2. List topics to verify
docker compose exec kafka kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --list

# Expected output:
# esports-matches
# esports-players

# 3. Describe topics
docker compose exec kafka kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --topic esports-matches
```

#### Expected Results
```
✅ Topics created: esports-matches, esports-players
✅ Partitions: 3 for each topic
✅ Replication factor: 1
✅ No errors in topic creation
```

#### Optional: Test Producer (requires Riot API key)
```bash
# Set API key
export RIOT_API_KEY="your-api-key-here"

# Run producer (will send test data)
python src/ingestion/riot_producer.py

# In another terminal, consume messages
docker compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic esports-matches \
  --from-beginning \
  --max-messages 5
```

---

### **Phase 3: Spark Structured Streaming**

#### What to Test
- Spark session creation
- Streaming job execution
- Parquet output generation

#### Prerequisites
```bash
# Install Spark dependencies
pip install -r requirements/spark.txt

# Set environment variables
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export KAFKA_TOPIC_MATCHES="esports-matches"
export KAFKA_TOPIC_PLAYERS="esports-players"
export SPARK_MASTER_URL="local[*]"
export DATA_LAKE_PATH="/workspaces/esport-bigdata-pipeline/data"
export PROCESSED_DATA_PATH="/workspaces/esport-bigdata-pipeline/data/silver"
export LOG_LEVEL="INFO"
```

#### Test Streaming Jobs

**Option 1: If you have data in Kafka**
```bash
cd spark

# Run all streaming jobs
python main.py --job all

# Expected output:
# Starting match stream...
# Starting player stream...
# ✅ All streams started successfully
```

**Option 2: Generate test data first**
```bash
# Create sample test data
python scripts/test_kafka_producer.py

# Then run streaming
cd spark
python main.py --job all
```

#### Verify Output
```bash
# Check processed data was created
ls -lh /workspaces/esport-bigdata-pipeline/data/silver/matches/
ls -lh /workspaces/esport-bigdata-pipeline/data/silver/players/

# Should see Parquet files like:
# part-00000-*.snappy.parquet
# _SUCCESS
```

#### Expected Results
```
✅ Spark session created successfully
✅ Streaming queries running
✅ Parquet files created in /data/silver/matches/
✅ Parquet files created in /data/silver/players/
✅ Checkpoint directories created
✅ No errors in streaming logs
```

#### Quick Data Check
```bash
# Count records using PySpark
python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Test').getOrCreate()
matches = spark.read.parquet('data/silver/matches')
print(f'Total matches: {matches.count()}')
matches.show(5)
"
```

---

### **Phase 4: Analytics & Aggregations**

#### What to Test
- Analytics job execution
- Spark SQL queries
- Analytics output generation

#### Prerequisites
```bash
# Ensure Phase 3 data exists
ls -lh data/silver/matches/
ls -lh data/silver/players/

# Set environment
export PROCESSED_DATA_PATH="/workspaces/esport-bigdata-pipeline/data/silver"
export DATA_LAKE_PATH="/workspaces/esport-bigdata-pipeline/data/gold"
```

#### Run Analytics
```bash
cd spark

# Run all analytics
python analytics_main.py all

# Expected output:
# ===============================================================================
# JOB 1/3: MATCH ANALYTICS
# ===============================================================================
# Loaded X match records
# Generated 7 match analytics views
# ...
# ===============================================================================
# JOB 2/3: PLAYER ANALYTICS
# ===============================================================================
# ...
# ===============================================================================
# TOP 10 PLAYERS BY COMPOSITE SCORE
# ===============================================================================
# 1. PlayerName (Team) - Role - Score: XX.XX
# ...
```

#### Verify Analytics Output
```bash
# Check analytics outputs
ls -lh data/gold/analytics/matches/
ls -lh data/gold/analytics/players/
ls -lh data/gold/analytics/rankings/

# Count output datasets
find data/gold/analytics -name "_SUCCESS" | wc -l
# Should output: 21
```

#### Expected Results
```
✅ Match analytics: 7 outputs
✅ Player analytics: 8 outputs
✅ Ranking analytics: 6 outputs
✅ Total: 21 Parquet datasets
✅ Top 10 players displayed
✅ No errors in analytics execution
```

#### Validate Analytics
```bash
cd spark
python validate_analytics.py

# Expected output:
# ===============================================================================
# VALIDATION SUMMARY
# ===============================================================================
# ✅ PASS: data_availability
# ✅ PASS: match_analytics
# ✅ PASS: player_analytics
# ✅ PASS: ranking_analytics
# ✅ PASS: output_files
# ===============================================================================
# FINAL RESULT: 5/5 tests passed
# ===============================================================================
```

---

### **Phase 5: Storage & BI Integration**

#### What to Test
- PostgreSQL database setup
- Data loading to PostgreSQL
- MongoDB loading (optional)
- Streamlit dashboard

#### Prerequisites

**1. Start PostgreSQL**
```bash
# Start PostgreSQL container
docker compose up -d postgres

# Wait for it to be ready (10-15 seconds)
sleep 15

# Verify it's running
docker compose ps postgres
```

**2. Initialize PostgreSQL Schema**
```bash
# Connect to PostgreSQL
docker compose exec -it postgres psql -U postgres -d esports_analytics

# Inside psql, run:
\i /workspaces/esport-bigdata-pipeline/src/storage/postgres/schema.sql

# Or from command line:
docker compose exec -T postgres psql -U postgres -d esports_analytics < src/storage/postgres/schema.sql

# Verify tables created
docker compose exec postgres psql -U postgres -d esports_analytics -c "\dt"

# Expected output:
# List of relations
#  Schema |         Name              | Type  |  Owner
# --------+---------------------------+-------+----------
#  public | matches_analytics         | table | postgres
#  public | player_analytics          | table | postgres
#  public | team_rankings             | table | postgres
#  public | match_team_performance    | table | postgres
#  public | player_role_stats         | table | postgres
```

**3. Install Python Dependencies**
```bash
pip install -r requirements/storage.txt
```

**4. Set Environment Variables**
```bash
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=postgres
export POSTGRES_DATABASE=esports_analytics
export PROCESSED_DATA_PATH="/workspaces/esport-bigdata-pipeline/data/silver"
export DATA_LAKE_PATH="/workspaces/esport-bigdata-pipeline/data/gold"
```

#### Load Data to PostgreSQL
```bash
# Run storage loader
python src/storage/storage_main.py --target postgres --skip-mongodb

# Expected output:
# ===============================================================================
# STORAGE ORCHESTRATOR STARTED
# ===============================================================================
#
# ===============================================================================
# POSTGRESQL LOAD STARTED
# ===============================================================================
#
# [1/2] Loading match data...
# ✅ Successfully loaded X match records to PostgreSQL
# ✅ Successfully loaded X team performance records to PostgreSQL
#
# [2/2] Loading player data...
# ✅ Successfully loaded X player records to PostgreSQL
# ✅ Successfully loaded X team ranking records to PostgreSQL
# ✅ Successfully loaded X role stats records to PostgreSQL
#
# ===============================================================================
# POSTGRESQL LOAD COMPLETED
# ===============================================================================
# PostgreSQL records: X,XXX
# Total duration: XX.XX seconds
# ===============================================================================
```

#### Validate PostgreSQL Data
```bash
# Run validation script
python validate_storage.py

# Expected output:
# ===============================================================================
# VALIDATING POSTGRESQL TABLES
# ===============================================================================
# ✅ matches_analytics: X,XXX records
# ✅ player_analytics: XXX records
# ✅ team_rankings: XX records
# ✅ match_team_performance: XX records
# ✅ player_role_stats: X records
#
# ===============================================================================
# VALIDATING POSTGRESQL VIEWS
# ===============================================================================
# ✅ vw_active_players: XXX records
# ✅ vw_top_players_kda: 100 records
# ✅ vw_top_players_winrate: 100 records
# ... (all 11 views)
#
# ===============================================================================
# FINAL RESULT: ✅ ALL VALIDATIONS PASSED
# ===============================================================================
```

#### Query PostgreSQL Directly
```bash
# Test sample queries
docker compose exec postgres psql -U postgres -d esports_analytics -c "
SELECT * FROM vw_player_kpis;
"

# Expected output:
# total_players | active_players | overall_avg_win_rate | overall_avg_kda
# --------------+----------------+----------------------+-----------------
#           567 |            423 |                48.50 |            3.45

docker compose exec postgres psql -U postgres -d esports_analytics -c "
SELECT summoner_name, kda_ratio, win_rate
FROM vw_top_players_kda
LIMIT 5;
"

# Expected output:
#  summoner_name | kda_ratio | win_rate
# ---------------+-----------+----------
#  Player1       |      5.23 |    65.00
#  Player2       |      4.89 |    62.50
#  ...
```

#### Launch Streamlit Dashboard
```bash
# Start Streamlit
streamlit run streamlit_app.py

# Expected output:
#   You can now view your Streamlit app in your browser.
#   Local URL: http://localhost:8501
#   Network URL: http://172.x.x.x:8501
```

**Access Dashboard:**
- Open browser: http://localhost:8501
- Should see 5 pages in sidebar:
  1. Overview
  2. Player Analytics
  3. Team Analytics
  4. Match Analytics
  5. Rankings

#### Expected Results
```
✅ PostgreSQL schema created (5 tables, 11 views)
✅ Data loaded successfully
✅ All validation tests pass
✅ Sample queries return data
✅ Streamlit dashboard accessible
✅ Dashboard displays data correctly
```

---

## 🔍 What You Should Find

### Data Structure
```
data/
├── raw/              # (empty or raw API responses)
├── bronze/           # (empty - for future use)
├── silver/           # Processed streaming data
│   ├── matches/
│   │   └── *.parquet files
│   └── players/
│       └── *.parquet files
└── gold/             # Analytics outputs
    └── analytics/
        ├── matches/ (7 folders with parquet)
        ├── players/ (8 folders with parquet)
        └── rankings/ (6 folders with parquet)
```

### Database Tables (PostgreSQL)
```
esports_analytics database:
├── matches_analytics (X,XXX rows)
├── player_analytics (XXX rows)
├── team_rankings (XX rows)
├── match_team_performance (XX rows)
└── player_role_stats (5 rows - one per role)
```

### Dashboard Content
- **Overview Page:** KPIs, top players, role comparison chart
- **Player Analytics:** Filterable player table, KDA vs Win Rate scatter plot
- **Team Analytics:** Team rankings, win rate charts
- **Match Analytics:** Tournament statistics
- **Rankings:** Top 50 players by KDA and Win Rate

---

## 🐛 Common Issues & Solutions

### Issue: Kafka won't start
```bash
# Check logs
docker compose logs kafka

# Common fix: Remove old data
docker compose down -v
docker compose --profile core up -d zookeeper kafka
```

### Issue: No data in Phase 3 output
```bash
# Verify Kafka has data
docker compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic esports-matches \
  --from-beginning \
  --max-messages 1

# If no data, run test producer
python scripts/test_kafka_producer.py
```

### Issue: Analytics fails - no input data
```bash
# Check Phase 3 output exists
ls -lh data/silver/matches/
ls -lh data/silver/players/

# If missing, re-run Phase 3
cd spark
python main.py --job all
```

### Issue: PostgreSQL connection failed
```bash
# Check PostgreSQL is running
docker compose ps postgres

# Restart if needed
docker compose restart postgres

# Wait 15 seconds, then retry
sleep 15
```

### Issue: Streamlit can't connect to database
```bash
# Verify environment variables
echo $POSTGRES_HOST
echo $POSTGRES_PORT

# Set them if missing
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DATABASE=esports_analytics
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=postgres

# Restart Streamlit
streamlit run streamlit_app.py
```

### Issue: JDBC driver not found (Spark to PostgreSQL)
```bash
# Download PostgreSQL JDBC driver
wget https://jdbc.postgresql.org/download/postgresql-42.7.1.jar -P /tmp/

# Set Spark classpath
export SPARK_CLASSPATH=/tmp/postgresql-42.7.1.jar

# Or add to spark-defaults.conf
echo "spark.jars /tmp/postgresql-42.7.1.jar" >> conf/spark/spark-defaults.conf
```

---

## ✅ Complete Testing Checklist

Use this to track your progress:

- [ ] **Phase 1:** Zookeeper running
- [ ] **Phase 1:** Kafka running
- [ ] **Phase 1:** Directory structure exists
- [ ] **Phase 2:** Kafka topics created
- [ ] **Phase 2:** Topics verified
- [ ] **Phase 3:** Streaming jobs run successfully
- [ ] **Phase 3:** Parquet files in /data/silver/
- [ ] **Phase 4:** Analytics jobs complete
- [ ] **Phase 4:** 21 output datasets in /data/gold/analytics/
- [ ] **Phase 4:** Validation script passes
- [ ] **Phase 5:** PostgreSQL container running
- [ ] **Phase 5:** Schema initialized (5 tables, 11 views)
- [ ] **Phase 5:** Data loaded to PostgreSQL
- [ ] **Phase 5:** Storage validation passes
- [ ] **Phase 5:** Streamlit dashboard accessible
- [ ] **Phase 5:** Dashboard displays data

---

## 📊 Expected Metrics Summary

After successful testing, you should see:

| Component | Metric | Expected Value |
|-----------|--------|----------------|
| Kafka Topics | Count | 2 |
| Phase 3 Outputs | Parquet datasets | 2 (matches, players) |
| Phase 4 Outputs | Parquet datasets | 21 |
| PostgreSQL Tables | Count | 5 |
| PostgreSQL Views | Count | 11 |
| PostgreSQL Indexes | Count | 15+ |
| Streamlit Pages | Count | 5 |
| Total Records | Varies | Depends on data volume |

---

## 🚀 Quick Full Test Script

Run all tests in sequence:

```bash
#!/bin/bash
# full_test.sh - Run complete pipeline test

set -e  # Exit on error

echo "🧪 Starting Full Pipeline Test..."

# Phase 1
echo "📦 Phase 1: Infrastructure"
docker compose --profile core up -d zookeeper kafka
sleep 20

# Phase 2
echo "📨 Phase 2: Kafka Topics"
python scripts/create_topics.py

# Phase 3 (requires data - skip if no Riot API key)
# echo "⚡ Phase 3: Streaming"
# cd spark && python main.py --job all &
# SPARK_PID=$!
# sleep 30
# kill $SPARK_PID
# cd ..

# Phase 4
echo "📊 Phase 4: Analytics"
cd spark
python analytics_main.py all
cd ..

# Phase 5
echo "💾 Phase 5: Storage"
docker compose up -d postgres
sleep 15
docker compose exec -T postgres psql -U postgres -d esports_analytics < src/storage/postgres/schema.sql
python src/storage/storage_main.py --target postgres --skip-mongodb
python validate_storage.py

echo "✅ All tests complete!"
echo "🎯 Launch dashboard: streamlit run streamlit_app.py"
```

---

## 📝 Testing Notes

Take notes as you test:
- Record any error messages
- Note execution times
- Track data volumes
- Document any customizations needed

---

**Ready to Test?** Start with Phase 1 and work sequentially through Phase 5!

Good luck! 🚀
