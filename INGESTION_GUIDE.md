# Quick Start Guide - Riot API Ingestion

## Prerequisites

1. **Get a Riot API Key**: https://developer.riotgames.com/
2. **Ensure Kafka is running**:
   ```bash
   docker compose --profile core up -d
   ```

## Setup

1. **Configure environment variables**:
   ```bash
   cp .env.example .env
   # Edit .env and set:
   # RIOT_API_KEY=your_actual_api_key_here
   # TEST_SUMMONER_NAME=SomeSummonerName
   # MATCH_COUNT=10
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements/ingestion.txt
   ```

3. **Create Kafka topics**:
   ```bash
   python scripts/create_topics.py
   ```

## Running the Producer

### Option 1: From Host Machine
```bash
# Set Kafka broker to localhost for external access
export KAFKA_BROKER=localhost:9092

# Run the producer
python src/ingestion/riot_producer.py
```

### Option 2: Inside Docker (Recommended)
```bash
# Copy .env into the container or mount it
docker compose exec kafka bash

# Inside container:
pip install -r requirements/ingestion.txt
python src/ingestion/riot_producer.py
```

### Option 3: Custom Python Script
```python
from src.ingestion.riot_producer import RiotKafkaProducer, RiotAPIClient
from src.ingestion.kafka_config import KafkaConfig

# Initialize
kafka_config = KafkaConfig()
riot_client = RiotAPIClient(api_key="YOUR_KEY", region="americas", platform="na1")
producer = RiotKafkaProducer(kafka_config, riot_client)

# Ingest matches
producer.ingest_matches_for_summoner("Faker", match_count=5)
producer.close()
```

## Verify Data in Kafka

```bash
# List topics
docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --list

# Consume messages from esport-matches topic
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic esport-matches \
  --from-beginning \
  --max-messages 1 | jq
```

## Configuration Options

Environment variables in `.env`:

| Variable | Default | Description |
|----------|---------|-------------|
| `RIOT_API_KEY` | - | **Required** - Your Riot API key |
| `RIOT_API_REGION` | `americas` | Regional routing (americas, asia, europe, sea) |
| `RIOT_API_PLATFORM` | `na1` | Platform (na1, euw1, kr, br1, etc.) |
| `KAFKA_BROKER` | `kafka:9092` | Kafka broker address |
| `KAFKA_TOPIC_MATCHES` | `esport-matches` | Topic for match data |
| `KAFKA_TOPIC_PLAYERS` | `esport-players` | Topic for player data |
| `KAFKA_TOPIC_RANKINGS` | `esport-rankings` | Topic for ranking data |
| `TEST_SUMMONER_NAME` | `Doublelift` | Summoner to ingest matches for |
| `MATCH_COUNT` | `10` | Number of matches to fetch |

## Features

✅ **Rate Limiting** - Respects Riot API rate limits (20 req/sec default)  
✅ **Error Handling** - Retries with exponential backoff  
✅ **Logging** - Comprehensive logging at INFO level  
✅ **Kafka Partitioning** - Uses match ID as key for consistent partitioning  
✅ **Modular Design** - Separate concerns (API client, Kafka config, producer)  

## Troubleshooting

**Issue: `RIOT_API_KEY not found`**
- Solution: Create `.env` file with `RIOT_API_KEY=your_key`

**Issue: `Connection refused to kafka:9092`**
- Solution: Use `KAFKA_BROKER=localhost:9092` when running outside Docker

**Issue: `401 Unauthorized from Riot API`**
- Solution: Verify your API key is valid and not expired

**Issue: `Summoner not found`**
- Solution: Use a valid summoner name for your platform (na1, euw1, etc.)

## Next Steps

1. **Extend to fetch player stats** → Publish to `esport-players` topic
2. **Add ranking data ingestion** → Publish to `esport-rankings` topic
3. **Schedule periodic ingestion** → Use cron or Airflow
4. **Implement Spark streaming consumer** → Process Kafka data in real-time
