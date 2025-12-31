# Esport Big Data Pipeline

A production-ready **Big Data analytics platform** for esports using **Riot Games API**, built with **Apache Kafka**, **Apache Spark Structured Streaming**, and **Docker**. Designed for **GitHub Codespaces** and modular scalability.

---

## 🏗 Architecture Overview

```
┌─────────────────┐
│  Riot Games API │
└────────┬────────┘
         │
         v
┌────────────────────┐       ┌──────────────────────┐
│ Kafka Producers    │──────>│  Kafka Topics        │
│ (Data Ingestion)   │       │  (Event Streaming)   │
└────────────────────┘       └──────────┬───────────┘
                                        │
                                        v
                         ┌──────────────────────────────┐
                         │  Spark Structured Streaming  │
                         │  (Real-time Processing)      │
                         └──────────────┬───────────────┘
                                        │
                                        v
                         ┌──────────────────────────────┐
                         │  Data Lake (Parquet)         │
                         │  Bronze → Silver → Gold      │
                         └──────────────┬───────────────┘
                                        │
                    ┌───────────────────┴──────────────────┐
                    │                                      │
                    v                                      v
         ┌──────────────────┐                  ┌──────────────────┐
         │  Batch Analytics │                  │  ML Pipelines    │
         │  (Spark Jobs)    │                  │  (Training)      │
         └──────────────────┘                  └──────────────────┘
                    │                                      │
                    └───────────────────┬──────────────────┘
                                        v
                              ┌─────────────────────┐
                              │  Visualization      │
                              │  (Streamlit/Grafana)│
                              └─────────────────────┘
```

---

## ✅ Implementation Status

| Phase | Component | Status | Documentation |
|-------|-----------|--------|---------------|
| **Phase 1** | Infrastructure Setup | ✅ Complete | [QUICKSTART.md](QUICKSTART.md) |
| **Phase 2** | Kafka Data Ingestion | ✅ Complete | [INGESTION_GUIDE.md](INGESTION_GUIDE.md) |
| **Phase 3** | Spark Structured Streaming | ✅ Complete | [spark/README.md](spark/README.md) |
| **Phase 4** | Analytics & Aggregations | ✅ Complete | [PHASE4_ANALYTICS.md](PHASE4_ANALYTICS.md) |
| **Phase 5** | Storage & BI Integration | ✅ Complete | [PHASE5_STORAGE.md](PHASE5_STORAGE.md) |
| **Phase 6** | Advanced ML & Visualization | 🔄 Planned | - |

### Phase 5 Highlights (Latest)
- **Storage Systems:** PostgreSQL (5 tables, 11 views) + MongoDB (3 collections)
- **Streamlit Dashboard:** 5 interactive pages (Overview, Players, Teams, Matches, Rankings)
- **BI Optimization:** 15+ indexes, auto-update triggers, window functions
- **JDBC Loaders:** Idempotent Spark-to-PostgreSQL data loading
- **Lines of Code:** 1,800+ lines
- **Documentation:** [Quick Start](PHASE5_QUICKSTART.md) | [Full Docs](PHASE5_STORAGE.md) | [Status](PHASE5_STATUS.md)
- **Dashboard:** http://localhost:8501

---

## 📁 Project Structure

```
esport-bigdata-pipeline/
├── .devcontainer/
│   └── devcontainer.json         # GitHub Codespaces configuration
├── conf/
│   ├── logging.yaml               # Python logging config
│   ├── kafka/
│   │   └── topics.yaml            # Kafka topic definitions
│   └── spark/
│       └── spark-defaults.conf    # Spark default settings
├── data/
│   ├── raw/                       # Raw ingestion layer
│   ├── bronze/                    # Unprocessed data
│   ├── silver/                    # Cleaned data
│   └── gold/                      # Aggregated analytics
├── requirements/
│   ├── ingestion.txt              # Kafka producer dependencies
│   ├── spark.txt                  # Spark job dependencies
│   ├── ml.txt                     # ML pipeline dependencies
│   ├── visualization.txt          # Dashboard dependencies
│   └── dev.txt                    # Development tools
├── scripts/
│   ├── create_topics.sh           # Kafka topic creation script
│   └── spark-submit-example.sh    # Spark job submission template
├── src/
│   ├── common/                    # Shared utilities
│   │   └── logging_config.py      # Logging setup
│   ├── config/
│   │   └── settings.py            # Environment settings
│   ├── ingestion/
│   │   └── riot_producer.py       # Riot API → Kafka producer
│   ├── streaming/
│   │   └── jobs/                  # Spark Structured Streaming jobs
│   ├── batch/
│   │   └── jobs/                  # Spark batch analytics
│   ├── ml/
│   │   └── pipelines/             # ML training pipelines
│   ├── storage/                   # Data lake abstractions
│   └── visualization/             # Dashboards
├── .env.example                   # Environment template
├── .gitignore
├── .dockerignore
├── docker-compose.yml             # Orchestration of all services
└── README.md
```

---

## 🚀 Getting Started

### Prerequisites

- **GitHub Codespaces** (recommended) or local Docker + Docker Compose
- **Riot Games API Key**: [Get one here](https://developer.riotgames.com/)

### Setup Steps

1. **Clone the repository** (or open in Codespaces):
   ```bash
   git clone https://github.com/louatizine/esport-bigdata-pipeline.git
   cd esport-bigdata-pipeline
   ```

2. **Configure environment**:
   ```bash
   cp .env.example .env
   # Edit .env and set your RIOT_API_KEY
   ```

3. **Start services one-by-one** (see below)

---

## 🐳 Docker Commands (Run One-by-One)

### Start Zookeeper
```bash
docker compose --profile core up -d zookeeper
```

**Verify:**
```bash
docker compose logs zookeeper
```

---

### Start Kafka
```bash
docker compose --profile core up -d kafka
```

**Verify:**
```bash
docker compose logs kafka
docker compose exec kafka kafka-broker-api-versions.sh --bootstrap-server kafka:9092
```

---

### Create Kafka Topics (Optional)
```bash
docker compose exec kafka kafka-topics.sh \
  --create \
  --topic esports.matches \
  --bootstrap-server kafka:9092 \
  --partitions 3 \
  --replication-factor 1
```

**List topics:**
```bash
docker compose exec kafka kafka-topics.sh --list --bootstrap-server kafka:9092
```

---

### Start Spark Master
```bash
docker compose --profile spark up -d spark-master
```

**Verify:**
```bash
docker compose logs spark-master
# Spark UI: http://localhost:8080
```

---

### Start Spark Worker
```bash
docker compose --profile spark up -d spark-worker
```

**Verify:**
```bash
docker compose logs spark-worker
# Worker UI: http://localhost:8081
```

---

### Start Optional Services (MongoDB, PostgreSQL)
```bash
docker compose --profile optional up -d mongodb postgres
```

**Verify:**
```bash
docker compose ps
```

---

### Stop All Services
```bash
docker compose --profile core --profile spark --profile optional down
```

**Remove volumes (clean slate):**
```bash
docker compose down -v
```

---

## 🔧 Development Workflow

### Install Python Dependencies
```bash
pip install -r requirements/ingestion.txt
pip install -r requirements/spark.txt
pip install -r requirements/ml.txt
pip install -r requirements/dev.txt
```

### Run a Kafka Producer (Example)
```bash
python src/ingestion/riot_producer.py
```

### Submit a Spark Job (Example)
```bash
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /workspace/src/streaming/jobs/streaming_job_template.py
```

---

## 🧪 Testing & Linting

```bash
# Format code
black src/

# Lint
flake8 src/

# Type checking
mypy src/

# Run tests
pytest
```

---

## 📊 Data Lake Layers

| Layer    | Description                          | Location         |
|----------|--------------------------------------|------------------|
| **Raw**  | Unprocessed API responses            | `data/raw/`      |
| **Bronze** | Raw ingested events from Kafka     | `data/bronze/`   |
| **Silver** | Cleaned, deduplicated, validated   | `data/silver/`   |
| **Gold**   | Aggregated, business-level metrics | `data/gold/`     |

---

## 🛠 Technology Stack

- **Data Streaming**: Apache Kafka + Zookeeper
- **Processing**: Apache Spark (Structured Streaming + Batch)
- **Storage**: Parquet (Data Lake)
- **Orchestration**: Docker Compose
- **Language**: Python 3.11+
- **Optional**: MongoDB, PostgreSQL (persistence)
- **Visualization**: Streamlit / Grafana (TBD)

---

## 📝 Notes

- Keep `.env` out of version control (already in `.gitignore`)
- Use profiles (`core`, `spark`, `optional`) to control which services run
- Data lake directories are Git-tracked via `.gitkeep` but ignored for content
- Spark configs are mounted read-only from `conf/spark/`

---

## 📜 License

MIT (or adjust per your needs)

---

## 🤝 Contributing

1. Fork the repo
2. Create a feature branch
3. Follow code style (black, flake8)
4. Submit a PR

---

**Happy streaming! 🚀**