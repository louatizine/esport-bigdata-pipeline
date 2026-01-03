# 🚀 Quick Start Guide

This guide will help you get the Esports Big Data Pipeline up and running in minutes.

## Prerequisites

- Docker and Docker Compose installed
- Python 3.9+ installed
- Valid Riot API key ([Get one here](https://developer.riotgames.com/))
- MongoDB Atlas account (free M0 tier) - Optional for cloud storage

## 🎯 Step-by-Step Setup

### 1. Configure Environment Variables

Create a `.env` file in the project root:

```bash
# Riot Games API
RIOT_API_KEY=your_riot_api_key_here
RIOT_REGION=asia
RIOT_PLATFORM=kr

# MongoDB Atlas (Optional - for cloud storage)
MONGODB_ATLAS_URI=mongodb+srv://username:password@cluster.mongodb.net/
MONGODB_DATABASE=esports_analytics
```

### 2. Start Infrastructure Services

Use the automated startup script for proper service initialization:

```bash
./start_services.sh
```

This script will:
- ✅ Start Kafka, Zookeeper, and Kafka-UI
- ✅ Wait for Kafka to be fully ready
- ✅ Create required Kafka topics
- ✅ Verify all services are running

**Manual alternative:**
```bash
# Start services
docker-compose --profile core up -d

# Wait 15-30 seconds for Kafka to be ready

# Create topics
bash scripts/create_topics.sh
```

### 3. Verify Services

Check that all services are running:

```bash
docker-compose ps
```

Expected services:
- **kafka** - Up on port 9092
- **zookeeper** - Up on port 2181
- **kafka-ui** - Up on port 8090

Access Kafka UI: http://localhost:8090

### 4. Ingest Data from Riot API

Fetch real match data and publish to Kafka:

```bash
PYTHONPATH=$PWD python src/ingestion/riot_producer.py
```

This will:
- Fetch match data for professional players
- Publish matches to `esport-matches` topic
- Publish player stats to `esport-players` topic

**Expected output:**
```
✅ Published match LR_4890238001 to Kafka
✅ Published match KR_7463811087 to Kafka
✅ Published 3 matches successfully
```

### 5. View Real-Time Dashboard

Launch the Streamlit dashboard to visualize Kafka data:

```bash
streamlit run streamlit_kafka_dashboard.py --server.port 8502
```

Access dashboard: http://localhost:8502

The dashboard shows:
- 📊 Real-time match statistics
- 🎮 Champion pick rates
- 📈 Gold difference trends
- 🔄 Auto-refresh every 30 seconds

### 6. Process Data with Spark (Optional)

Run Spark streaming to process Kafka data into Parquet:

```bash
bash scripts/run_spark_streaming.sh
```

This will:
- Read from Kafka topics
- Process and transform data
- Write to Parquet files in `data/silver/`

### 7. Load to MongoDB Atlas (Optional)

Upload processed analytics to MongoDB Atlas:

```bash
PYTHONPATH=$PWD python src/storage/storage_main.py
```

## 📊 Architecture Overview

```
Riot API → Kafka → Spark → Parquet/MongoDB Atlas
                    ↓
             Streamlit Dashboard
```

## 🛠️ Common Commands

### Stop All Services
```bash
docker-compose down
```

### Restart Services
```bash
docker-compose --profile core restart
```

### View Kafka Logs
```bash
docker logs kafka -f
```

### Clean Data Directories
```bash
rm -rf data/silver/* data/gold/* data/checkpoints/*
```

## 🔍 Troubleshooting

### Dashboard Shows "No Data Available"

**Solution:** Ingest data first
```bash
PYTHONPATH=$PWD python src/ingestion/riot_producer.py
```

### "NoBrokersAvailable" Error

**Solution 1:** Use the startup script (recommended)
```bash
./start_services.sh
```

**Solution 2:** Wait for Kafka to be fully ready
```bash
# Restart Kafka
docker-compose restart kafka

# Wait 15-20 seconds before starting dashboard
sleep 20
streamlit run streamlit_kafka_dashboard.py --server.port 8502
```

The dashboard now has built-in retry logic (3 attempts with 2s delays).

### Riot API Rate Limit Errors

**Solution:** Wait 2 minutes between ingestion runs or use a production API key.

### MongoDB Atlas SSL/TLS Issues in Devcontainer

**Known Issue:** SSL certificate verification may fail in devcontainer environments.

**Workaround:** Use local MongoDB for development:
```bash
docker-compose up -d mongodb
# Update .env: MONGODB_URI=mongodb://localhost:27017
```

## 📁 Data Locations

- **Raw Kafka Data:** In-memory (check Kafka-UI)
- **Processed Data:** `data/silver/matches/`, `data/silver/players/`
- **Analytics:** `data/gold/` (after Spark analytics)
- **Cloud Backup:** MongoDB Atlas (if configured)

## 🎓 Next Steps

1. **Explore the Pipeline:**
   - [PHASE2.md](PHASE2.md) - Data Ingestion
   - [PHASE3.md](PHASE3.md) - Data Processing
   - [PHASE4.md](PHASE4.md) - Data Storage
   - [PHASE5.md](PHASE5.md) - Data Visualization

2. **Customize:**
   - Modify `src/ingestion/riot_producer.py` to fetch different players
   - Update Spark transformations in `spark/streaming/`
   - Enhance dashboard in `streamlit_kafka_dashboard.py`

3. **Production Deployment:**
   - Review [ARCHITECTURE.md](ARCHITECTURE.md) for scalability
   - Configure persistent volumes in `docker-compose.yml`
   - Set up monitoring and alerting

## 🆘 Support

- **Documentation:** See all `PHASE*.md` files
- **Architecture:** [ARCHITECTURE.md](ARCHITECTURE.md)
- **MongoDB Setup:** [MONGODB_ATLAS_SETUP.md](MONGODB_ATLAS_SETUP.md)
- **Project Structure:** [PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md)

---

**Happy Analytics! 🎮📊**
