#!/bin/bash
# Quick Diagnostic Script - Check current project status

echo "======================================================================"
echo "🔍 ESPORTS ANALYTICS PLATFORM - DIAGNOSTIC REPORT"
echo "======================================================================"
echo ""

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to check status
check_status() {
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ $1${NC}"
        return 0
    else
        echo -e "${RED}❌ $1${NC}"
        return 1
    fi
}

check_warning() {
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ $1${NC}"
        return 0
    else
        echo -e "${YELLOW}⚠️  $1${NC}"
        return 1
    fi
}

# 1. Docker Services
echo "📦 DOCKER SERVICES"
echo "----------------------------------------------------------------------"
docker compose ps
echo ""

# 2. Kafka Topics
echo "📨 KAFKA TOPICS"
echo "----------------------------------------------------------------------"
if docker compose ps kafka 2>/dev/null | grep -q "Up"; then
    echo "Kafka is running. Checking topics..."
    docker compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --list 2>/dev/null || echo "⚠️  Cannot connect to Kafka"
else
    echo "⚠️  Kafka is not running"
fi
echo ""

# 3. Data Lake Structure
echo "💾 DATA LAKE STRUCTURE"
echo "----------------------------------------------------------------------"
for dir in data/raw data/bronze data/silver data/gold; do
    if [ -d "$dir" ]; then
        count=$(find "$dir" -name "*.parquet" 2>/dev/null | wc -l)
        echo "✅ $dir exists ($count parquet files)"
    else
        echo "❌ $dir missing"
    fi
done
echo ""

# 4. Phase 3 Output (Processed Data)
echo "⚡ PHASE 3: PROCESSED DATA"
echo "----------------------------------------------------------------------"
if [ -d "data/silver/matches" ]; then
    match_count=$(find data/silver/matches -name "*.parquet" 2>/dev/null | wc -l)
    echo "✅ Matches: $match_count parquet files"
else
    echo "❌ Matches: No data found"
fi

if [ -d "data/silver/players" ]; then
    player_count=$(find data/silver/players -name "*.parquet" 2>/dev/null | wc -l)
    echo "✅ Players: $player_count parquet files"
else
    echo "❌ Players: No data found"
fi
echo ""

# 5. Phase 4 Output (Analytics)
echo "📊 PHASE 4: ANALYTICS OUTPUT"
echo "----------------------------------------------------------------------"
if [ -d "data/gold/analytics" ]; then
    analytics_count=$(find data/gold/analytics -name "_SUCCESS" 2>/dev/null | wc -l)
    echo "✅ Analytics datasets: $analytics_count (expected: 21)"

    if [ -d "data/gold/analytics/matches" ]; then
        match_analytics=$(find data/gold/analytics/matches -name "_SUCCESS" | wc -l)
        echo "   ├── Matches: $match_analytics datasets"
    fi

    if [ -d "data/gold/analytics/players" ]; then
        player_analytics=$(find data/gold/analytics/players -name "_SUCCESS" | wc -l)
        echo "   ├── Players: $player_analytics datasets"
    fi

    if [ -d "data/gold/analytics/rankings" ]; then
        ranking_analytics=$(find data/gold/analytics/rankings -name "_SUCCESS" | wc -l)
        echo "   └── Rankings: $ranking_analytics datasets"
    fi
else
    echo "❌ Analytics: No data found"
fi
echo ""

# 6. PostgreSQL Status
echo "🐘 POSTGRESQL STATUS"
echo "----------------------------------------------------------------------"
if docker compose ps postgres 2>/dev/null | grep -q "Up"; then
    echo "✅ PostgreSQL container is running"

    # Check if tables exist
    table_count=$(docker compose exec -T postgres psql -U postgres -d esports_analytics -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE';" 2>/dev/null | tr -d ' ')

    if [ ! -z "$table_count" ] && [ "$table_count" -gt 0 ]; then
        echo "✅ PostgreSQL tables: $table_count (expected: 5)"
    else
        echo "⚠️  PostgreSQL tables: Not initialized (run schema.sql)"
    fi

    # Check if views exist
    view_count=$(docker compose exec -T postgres psql -U postgres -d esports_analytics -t -c "SELECT COUNT(*) FROM information_schema.views WHERE table_schema = 'public';" 2>/dev/null | tr -d ' ')

    if [ ! -z "$view_count" ] && [ "$view_count" -gt 0 ]; then
        echo "✅ PostgreSQL views: $view_count (expected: 11)"
    else
        echo "⚠️  PostgreSQL views: Not initialized"
    fi
else
    echo "❌ PostgreSQL container is not running"
fi
echo ""

# 7. Python Dependencies
echo "🐍 PYTHON DEPENDENCIES"
echo "----------------------------------------------------------------------"
python -c "import pyspark" 2>/dev/null && echo "✅ PySpark installed" || echo "❌ PySpark not installed"
python -c "import psycopg2" 2>/dev/null && echo "✅ psycopg2 installed" || echo "⚠️  psycopg2 not installed (needed for Phase 5)"
python -c "import streamlit" 2>/dev/null && echo "✅ Streamlit installed" || echo "⚠️  Streamlit not installed (needed for dashboard)"
python -c "import pymongo" 2>/dev/null && echo "✅ PyMongo installed" || echo "⚠️  PyMongo not installed (optional)"
echo ""

# 8. Environment Variables
echo "🔧 ENVIRONMENT VARIABLES"
echo "----------------------------------------------------------------------"
[ ! -z "$KAFKA_BOOTSTRAP_SERVERS" ] && echo "✅ KAFKA_BOOTSTRAP_SERVERS: $KAFKA_BOOTSTRAP_SERVERS" || echo "⚠️  KAFKA_BOOTSTRAP_SERVERS not set"
[ ! -z "$DATA_LAKE_PATH" ] && echo "✅ DATA_LAKE_PATH: $DATA_LAKE_PATH" || echo "⚠️  DATA_LAKE_PATH not set"
[ ! -z "$POSTGRES_HOST" ] && echo "✅ POSTGRES_HOST: $POSTGRES_HOST" || echo "⚠️  POSTGRES_HOST not set"
echo ""

# Summary
echo "======================================================================"
echo "📋 SUMMARY & NEXT STEPS"
echo "======================================================================"
echo ""
echo "Current Status:"
echo "  • Docker Services: Running (check above for details)"
echo "  • Data Pipeline: Check parquet file counts above"
echo "  • PostgreSQL: Check table/view counts above"
echo ""
echo "Quick Commands:"
echo "  • Full test: bash TESTING_GUIDE.md instructions"
echo "  • Start Kafka: docker compose --profile core up -d zookeeper kafka"
echo "  • Run Phase 4: cd spark && python analytics_main.py all"
echo "  • Load to DB: python src/storage/storage_main.py --target postgres"
echo "  • Launch UI: streamlit run streamlit_app.py"
echo ""
echo "Documentation:"
echo "  • Testing Guide: TESTING_GUIDE.md"
echo "  • Phase 5 Quick Start: PHASE5_QUICKSTART.md"
echo ""
echo "======================================================================"
