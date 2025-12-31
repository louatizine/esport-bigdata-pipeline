# Phase 3: Quick Reference Index

**Jump to any section quickly!**

---

## 📖 Documentation

### Main Documentation
- **[spark/README.md](spark/README.md)** - Complete technical documentation with examples
- **[PHASE3_SPARK_STREAMING.md](PHASE3_SPARK_STREAMING.md)** - Implementation details and architecture
- **[PHASE3_SETUP_GUIDE.md](PHASE3_SETUP_GUIDE.md)** - Step-by-step setup instructions
- **[PHASE3_DELIVERABLES.md](PHASE3_DELIVERABLES.md)** - Summary of all deliverables

---

## 💻 Source Code

### Main Application
- **[spark/main.py](spark/main.py)** - Main orchestrator for streaming jobs

### Streaming Jobs
- **[spark/streaming/match_stream.py](spark/streaming/match_stream.py)** - Match data processor
- **[spark/streaming/player_stream.py](spark/streaming/player_stream.py)** - Player data processor

### Schemas
- **[spark/schemas/match_schema.py](spark/schemas/match_schema.py)** - Match data schema definitions
- **[spark/schemas/player_schema.py](spark/schemas/player_schema.py)** - Player data schema definitions

### Utilities
- **[spark/utils/spark_session.py](spark/utils/spark_session.py)** - SparkSession builder & configuration
- **[spark/utils/logger.py](spark/utils/logger.py)** - Structured logging utility

---

## 🔧 Configuration

- **[spark/.env.example](spark/.env.example)** - Environment variable template
- **[requirements/spark-streaming.txt](requirements/spark-streaming.txt)** - Python dependencies

---

## 🛠️ Scripts & Tools

### Helper Scripts
- **[scripts/run_spark_streaming.sh](scripts/run_spark_streaming.sh)** - Shell script for spark-submit
- **[scripts/validate_phase3.py](scripts/validate_phase3.py)** - Validation script
- **[scripts/quickstart_phase3.py](scripts/quickstart_phase3.py)** - Quick start helper

---

## 🚀 Common Commands

### Run Streaming Jobs
```bash
# Run all jobs
python spark/main.py --job all

# Run specific job
python spark/main.py --job matches
python spark/main.py --job players

# Using spark-submit
./scripts/run_spark_streaming.sh --job all
```

### Validation & Setup
```bash
# Validate installation
python scripts/validate_phase3.py

# Quick start guide
python scripts/quickstart_phase3.py

# Install dependencies
pip install -r requirements/spark-streaming.txt
```

### Environment Setup
```bash
# Copy environment template
cp spark/.env.example .env

# Set required variables
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export DATA_LAKE_PATH="/workspaces/esport-bigdata-pipeline/data"
```

---

## 📊 Directory Structure

```
spark/
├── main.py                    # Main entry point
├── README.md                  # Documentation
├── .env.example               # Config template
├── streaming/                 # Streaming jobs
│   ├── match_stream.py
│   └── player_stream.py
├── schemas/                   # Data schemas
│   ├── match_schema.py
│   └── player_schema.py
└── utils/                     # Utilities
    ├── spark_session.py
    └── logger.py

scripts/
├── run_spark_streaming.sh     # Spark submit runner
├── validate_phase3.py         # Validation
└── quickstart_phase3.py       # Quick start

requirements/
└── spark-streaming.txt        # Dependencies
```

---

## 🔍 What to Read First?

### For Quick Start
1. [PHASE3_SETUP_GUIDE.md](PHASE3_SETUP_GUIDE.md) - Setup instructions
2. [scripts/quickstart_phase3.py](scripts/quickstart_phase3.py) - Run this script
3. [spark/README.md](spark/README.md) - Usage examples

### For Understanding Architecture
1. [PHASE3_SPARK_STREAMING.md](PHASE3_SPARK_STREAMING.md) - Architecture & design
2. [spark/main.py](spark/main.py) - Main orchestrator
3. [spark/streaming/match_stream.py](spark/streaming/match_stream.py) - Example processor

### For Development
1. [spark/schemas/match_schema.py](spark/schemas/match_schema.py) - Schema examples
2. [spark/utils/spark_session.py](spark/utils/spark_session.py) - Spark configuration
3. [spark/utils/logger.py](spark/utils/logger.py) - Logging utilities

### For Troubleshooting
1. [PHASE3_SETUP_GUIDE.md](PHASE3_SETUP_GUIDE.md) - Troubleshooting section
2. [scripts/validate_phase3.py](scripts/validate_phase3.py) - Validation tool
3. [spark/README.md](spark/README.md) - FAQ section

---

## 📌 Key Environment Variables

```bash
# Required
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Optional (with defaults)
KAFKA_TOPIC_MATCHES=esports-matches
KAFKA_TOPIC_PLAYERS=esports-players
SPARK_MASTER_URL=local[*]
DATA_LAKE_PATH=/workspaces/esport-bigdata-pipeline/data
LOG_LEVEL=INFO
```

---

## 🎯 Common Tasks

### Install & Setup
```bash
pip install -r requirements/spark-streaming.txt
cp spark/.env.example .env
python scripts/validate_phase3.py
```

### Run Jobs
```bash
cd spark
python main.py --job all
```

### Monitor
```bash
# Spark UI
http://localhost:4040

# Check output
ls -lh data/processed/matches/
ls -lh data/processed/players/
```

### Debug
```bash
export LOG_LEVEL=DEBUG
python spark/main.py --job all
```

---

## 📞 Getting Help

1. Check [PHASE3_SETUP_GUIDE.md](PHASE3_SETUP_GUIDE.md) troubleshooting section
2. Run validation: `python scripts/validate_phase3.py`
3. Review logs in terminal output
4. Check Spark UI at http://localhost:4040
5. Verify Kafka connectivity

---

## ✅ Quick Checklist

Before running:
- [ ] PySpark installed (`pip install pyspark`)
- [ ] Java installed (`java -version`)
- [ ] Kafka running (`docker ps | grep kafka`)
- [ ] Environment variables set
- [ ] Data directories created
- [ ] Kafka topics created

To verify:
- [ ] Run `python scripts/validate_phase3.py`
- [ ] All checks pass (except PySpark if not installed)
- [ ] Spark UI accessible at http://localhost:4040

---

**Last Updated:** December 31, 2025
**Phase:** 3 - Spark Structured Streaming
**Status:** ✅ Complete
