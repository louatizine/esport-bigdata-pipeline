# Esports Big Data Pipeline - Project Structure

## 🎯 Active Components

### Core Pipeline
```
Riot Games API → Kafka → Spark → Parquet Files → Streamlit Dashboard
```

### Directory Structure

```
├── README.md                          # Main documentation
├── ARCHITECTURE.md                     # System architecture
├── MONGODB_ATLAS_SETUP.md             # MongoDB Atlas setup guide
├── PHASE2.md - PHASE5.md              # Phase implementation guides
├── .env                               # Environment configuration
├── docker-compose.yml                 # Docker services configuration
│
├── conf/                              # Configuration files
│   ├── logging.yaml                   # Logging configuration
│   └── kafka/
│       └── topics.yaml                # Kafka topic definitions
│
├── data/                              # Data storage
│   ├── bronze/                        # Raw data (unused currently)
│   ├── silver/                        # Processed data (unused currently)
│   ├── gold/                          # Analytics data (unused currently)
│   ├── checkpoints/                   # Spark streaming checkpoints
│   └── processed/                     # Parquet output from Spark
│       └── matches/                   # Match data storage
│
├── requirements/                      # Python dependencies
│   ├── ingestion.txt                  # Kafka + Riot API
│   ├── spark.txt                      # PySpark
│   ├── storage.txt                    # MongoDB
│   └── visualization.txt              # Streamlit
│
├── scripts/                           # Utility scripts
│   ├── create_topics.py               # Create Kafka topics
│   ├── create_topics.sh               # Kafka topics shell script
│   ├── test_kafka_producer.py         # Kafka producer test
│   └── run_spark_streaming.sh         # Spark streaming launcher
│
├── src/                               # Source code
│   ├── common/                        # Shared utilities
│   │   ├── logging_config.py          # Logging setup
│   │   └── __init__.py
│   │
│   ├── ingestion/                     # Data ingestion
│   │   ├── kafka_config.py            # Kafka configuration
│   │   ├── riot_producer.py           # Riot API → Kafka producer
│   │   ├── validate_ingestion.py      # Ingestion validation
│   │   └── __init__.py
│   │
│   └── storage/                       # Data storage
│       ├── mongodb_atlas_loader.py    # MongoDB Atlas loader
│       ├── storage_main.py            # Storage orchestrator
│       └── mongodb/
│           ├── load_documents.py      # Document loader
│           └── __init__.py
│
├── spark/                             # Spark processing
│   ├── main.py                        # Streaming orchestrator
│   ├── analytics_main.py              # Analytics orchestrator
│   ├── validate_analytics.py          # Analytics validation
│   │
│   ├── schemas/                       # Data schemas
│   │   ├── match_schema.py
│   │   └── player_schema.py
│   │
│   ├── streaming/                     # Streaming jobs
│   │   ├── match_stream.py            # Match streaming processor
│   │   └── player_stream.py           # Player streaming processor
│   │
│   ├── analytics/                     # Analytics jobs
│   │   ├── match_analytics.py         # Match analytics
│   │   ├── player_analytics.py        # Player analytics
│   │   └── ranking_analytics.py       # Ranking analytics
│   │
│   └── utils/                         # Spark utilities
│       ├── spark_session.py           # Spark session factory
│       ├── logger.py                  # Logging utilities
│       └── metrics.py                 # Metrics utilities
│
└── streamlit_kafka_dashboard.py      # 🎨 Real-time dashboard (Kafka → UI)
```

## 🚀 Active Services

1. **Kafka** (localhost:9092)
   - Event streaming platform
   - Topics: esport-matches, esport-players, esport-rankings

2. **Zookeeper** (localhost:2181)
   - Kafka coordination service

3. **Kafka-UI** (localhost:8090)
   - Web interface for Kafka monitoring

4. **Streamlit Dashboard** (localhost:8502)
   - Real-time data visualization
   - Reads directly from Kafka

## 📊 Data Flow

1. **Ingestion**: `python src/ingestion/riot_producer.py`
   - Fetches matches from Riot Games API
   - Publishes to Kafka topic: esport-matches

2. **Processing**: `python spark/main.py --job matches`
   - Consumes from Kafka
   - Transforms data
   - Writes to Parquet files

3. **Visualization**: `streamlit run streamlit_kafka_dashboard.py`
   - Reads from Kafka
   - Displays real-time analytics

## 🔧 Key Files

- **.env**: Environment variables (API keys, database URIs)
- **docker-compose.yml**: Infrastructure services
- **riot_producer.py**: Real data ingestion
- **match_stream.py**: Spark streaming logic
- **streamlit_kafka_dashboard.py**: Dashboard application

## 📝 Notes

- MongoDB Atlas integration code exists but SSL connection issues in devcontainer
- Processed data stored in Parquet format at `data/processed/matches/`
- Spark analytics modules exist but schema mismatch with Riot API data
