#!/bin/bash

# Esports Big Data Pipeline - Service Startup Script
# Ensures services start in correct order with proper health checks

set -e

echo "🚀 Starting Esports Analytics Pipeline..."
echo "=========================================="

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Step 1: Start Kafka infrastructure
echo -e "\n${YELLOW}Step 1/4:${NC} Starting Kafka infrastructure..."
docker-compose --profile core up -d

# Step 2: Wait for Kafka to be ready
echo -e "\n${YELLOW}Step 2/4:${NC} Waiting for Kafka to be ready..."
echo "This may take 15-30 seconds..."

MAX_WAIT=60
WAIT_TIME=0

while [ $WAIT_TIME -lt $MAX_WAIT ]; do
    if docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; then
        echo -e "${GREEN}✅ Kafka is ready!${NC}"
        break
    fi
    echo -n "."
    sleep 2
    WAIT_TIME=$((WAIT_TIME + 2))
done

if [ $WAIT_TIME -ge $MAX_WAIT ]; then
    echo -e "\n⚠️  Warning: Kafka may not be fully ready yet"
fi

# Step 3: Verify/Create Kafka topics
echo -e "\n${YELLOW}Step 3/4:${NC} Setting up Kafka topics..."
sleep 5  # Additional buffer
bash scripts/create_topics.sh

echo -e "${GREEN}✅ Topics created${NC}"

# Step 4: Show service status
echo -e "\n${YELLOW}Step 4/4:${NC} Service Status"
echo "=========================================="
docker-compose ps

echo ""
echo -e "${GREEN}✅ All services are ready!${NC}"
echo ""
echo "📊 Available Services:"
echo "  • Kafka:       localhost:9092"
echo "  • Kafka-UI:    http://localhost:8090"
echo "  • Zookeeper:   localhost:2181"
echo ""
echo "🎯 Next Steps:"
echo "  1. Ingest data:    PYTHONPATH=\$PWD python src/ingestion/riot_producer.py"
echo "  2. Start dashboard: streamlit run streamlit_kafka_dashboard.py --server.port 8502"
echo ""
echo "=========================================="
