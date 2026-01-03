# Phase 5: Storage & BI Integration

## Overview

Phase 5 implements storage and BI integration, loading analytics data from Parquet into PostgreSQL and MongoDB, with a Streamlit-based interactive dashboard for business intelligence.

## Table of Contents

- [Implementation Status](#implementation-status)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Components](#components)
- [Storage Schema](#storage-schema)
- [Streamlit Dashboard](#streamlit-dashboard)
- [Deliverables](#deliverables)
- [Validation](#validation)
- [Troubleshooting](#troubleshooting)

---

## Implementation Status

**Status:** ✅ **PRODUCTION READY**

**Total Code:** 1,800+ lines (Python + SQL)

All requirements for Phase 5 have been successfully implemented and validated.

### Deliverables Summary

| Component | Files | Lines | Status |
|-----------|-------|-------|--------|
| PostgreSQL Schema | 1 file | 450 | ✅ Complete |
| PostgreSQL Loaders | 2 files | 525 | ✅ Complete |
| MongoDB Loader | 1 file | 330 | ✅ Complete |
| Main Orchestrator | 1 file | 195 | ✅ Complete |
| Streamlit Dashboard | 1 file | 550 | ✅ Complete |
| **Total** | **7 files** | **1,800+** | ✅ Complete |

---

## Architecture

### Data Flow

```
┌─────────────────────────────────────────────────────────┐
│                  PHASE 5 ARCHITECTURE                   │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  INPUT DATA                                            │
│  ├── /data/silver/matches (Parquet - Phase 3)         │
│  ├── /data/silver/players (Parquet - Phase 3)         │
│  └── /data/gold/analytics (Parquet - Phase 4)         │
│                                                         │
│  STORAGE LOADERS (Spark JDBC)                          │
│  ├── PostgreSQL Loaders                               │
│  │   ├── load_matches.py                              │
│  │   └── load_players.py                              │
│  └── MongoDB Loader                                    │
│      └── load_documents.py                             │
│                                                         │
│  STORAGE SYSTEMS                                       │
│  ├── PostgreSQL (Primary)                             │
│  │   ├── 5 Tables                                     │
│  │   ├── 11 BI Views                                  │
│  │   ├── 15+ Indexes                                  │
│  │   └── Auto-update Triggers                         │
│  └── MongoDB (Optional)                                │
│      └── 3 Collections                                 │
│                                                         │
│  BI LAYER                                              │
│  ├── Streamlit Dashboard (5 pages)                    │
│  ├── Grafana (via views)                              │
│  └── Direct SQL queries                                │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

### Processing Layers

1. **Bronze Layer** (Phase 2): Raw data ingestion
2. **Silver Layer** (Phase 3): Cleaned and structured data
3. **Gold Layer** (Phase 4): Analytics and aggregations
4. **Storage Layer** (Phase 5): **Persistent databases + BI** ← Current Phase

---

## Quick Start

### Prerequisites

- Phase 3 & 4 complete with data in `/data/silver/` and `/data/gold/`
- PostgreSQL database running
- MongoDB running (optional)
- Python 3.8+
- PySpark 3.5+

### Setup Environment

```bash
cd /workspaces/esport-bigdata-pipeline

# Set required environment variables
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DB=esports
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=postgres
export MONGODB_URI=mongodb://localhost:27017
```

### Initialize Database

```bash
# Create PostgreSQL tables and views
psql -h localhost -U postgres -d esports -f src/storage/postgres/schema.sql
```

### Load Data

```bash
# Load all data (PostgreSQL + MongoDB)
python src/storage/storage_main.py all

# Load PostgreSQL only
python src/storage/storage_main.py postgres

# Load MongoDB only
python src/storage/storage_main.py mongodb
```

### Launch Dashboard

```bash
# Start Streamlit app
streamlit run streamlit_app.py

# Access at http://localhost:8501
```

### Validate Implementation

```bash
python validate_storage.py
```

---

## Components

### Directory Structure

```
src/storage/
├── postgres/
│   ├── __init__.py
│   ├── schema.sql                  # PostgreSQL DDL (tables, views, indexes)
│   ├── load_matches.py             # Match data loader (245 lines)
│   └── load_players.py             # Player data loader (280 lines)
├── mongodb/
│   ├── __init__.py
│   └── load_documents.py           # MongoDB document loader (330 lines)
└── storage_main.py                 # Main orchestrator (195 lines)

streamlit_app.py                    # Interactive BI dashboard (550 lines)
requirements/storage.txt            # Phase 5 dependencies
validate_storage.py                 # Validation script
```

### Main Orchestrator

**File:** `src/storage/storage_main.py` (195 lines)

Centralized execution manager for all storage operations:

```python
# Load all data
python storage_main.py all

# Load specific storage
python storage_main.py postgres
python storage_main.py mongodb
```

Features:
- CLI argument support
- Error handling and logging
- Transaction management
- Idempotent operations

### PostgreSQL Loaders

**Files:**
- `src/storage/postgres/load_matches.py` (245 lines)
- `src/storage/postgres/load_players.py` (280 lines)

Features:
- Spark JDBC connector for fast bulk loading
- Truncate + insert for idempotency
- Batch processing support
- Connection pooling
- Automatic type conversion

Example:
```python
from src.storage.postgres.load_matches import load_match_data

load_match_data(
    spark=spark,
    parquet_path="/data/silver/matches",
    jdbc_url="jdbc:postgresql://localhost:5432/esports"
)
```

### MongoDB Loader

**File:** `src/storage/mongodb/load_documents.py` (330 lines)

Features:
- Spark MongoDB connector
- Document-oriented data loading
- Upsert operations
- Collection indexing
- Flexible schema support

Example:
```python
from src.storage.mongodb.load_documents import load_rankings

load_rankings(
    spark=spark,
    parquet_path="/data/gold/analytics/rankings",
    mongodb_uri="mongodb://localhost:27017",
    database="esports"
)
```

---

## Storage Schema

### PostgreSQL Tables

#### 1. matches_analytics

Match-level analytics data.

```sql
CREATE TABLE matches_analytics (
    match_id VARCHAR(100) PRIMARY KEY,
    tournament_name VARCHAR(200),
    status VARCHAR(50),
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    duration_seconds INTEGER,
    winner_name VARCHAR(200),
    loser_name VARCHAR(200),
    teams TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Indexes:** tournament_name, status, started_at, winner_name, teams

#### 2. player_analytics

Player performance metrics.

```sql
CREATE TABLE player_analytics (
    player_id VARCHAR(100) PRIMARY KEY,
    summoner_name VARCHAR(100),
    team VARCHAR(100),
    role VARCHAR(50),
    champion_name VARCHAR(100),
    status VARCHAR(50),
    total_matches INTEGER,
    wins INTEGER,
    losses INTEGER,
    win_rate DECIMAL(5,2),
    avg_kills DECIMAL(8,2),
    avg_deaths DECIMAL(8,2),
    avg_assists DECIMAL(8,2),
    kda_ratio DECIMAL(8,2),
    avg_cs DECIMAL(10,2),
    avg_gold INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Indexes:** team, role, status, win_rate, kda_ratio

#### 3. team_rankings

Team rankings by performance.

```sql
CREATE TABLE team_rankings (
    team_name VARCHAR(100) PRIMARY KEY,
    rank INTEGER,
    total_matches INTEGER,
    wins INTEGER,
    losses INTEGER,
    win_rate DECIMAL(5,2),
    avg_kda DECIMAL(8,2),
    total_kills INTEGER,
    total_deaths INTEGER,
    total_assists INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Indexes:** rank, win_rate, avg_kda

#### 4. match_team_performance

Aggregated team statistics.

```sql
CREATE TABLE match_team_performance (
    id SERIAL PRIMARY KEY,
    team_name VARCHAR(100) UNIQUE,
    total_matches INTEGER,
    wins INTEGER,
    losses INTEGER,
    win_rate DECIMAL(5,2),
    avg_match_duration INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### 5. player_role_stats

Aggregated role statistics.

```sql
CREATE TABLE player_role_stats (
    id SERIAL PRIMARY KEY,
    role VARCHAR(50) UNIQUE,
    total_players INTEGER,
    avg_kda DECIMAL(8,2),
    avg_win_rate DECIMAL(5,2),
    avg_kills DECIMAL(8,2),
    avg_deaths DECIMAL(8,2),
    avg_assists DECIMAL(8,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### PostgreSQL BI Views

#### 11 BI-Optimized Views

1. **vw_active_players** - Active players with team info
2. **vw_top_players_kda** - Top 100 by KDA (min 10 games)
3. **vw_top_players_winrate** - Top 100 by win rate
4. **vw_match_stats_by_status** - Match statistics by status
5. **vw_tournament_stats** - Tournament-level metrics
6. **vw_team_performance** - Team performance summary
7. **vw_role_comparison** - Role performance comparison
8. **vw_daily_matches** - Daily match volume (time series)
9. **vw_player_kpis** - Aggregated player KPIs
10. **vw_match_kpis** - Aggregated match KPIs
11. **vw_grafana_time_series** - Grafana-compatible time series

Example view:
```sql
CREATE OR REPLACE VIEW vw_top_players_kda AS
SELECT
    player_id,
    summoner_name,
    team,
    role,
    kda_ratio,
    win_rate,
    total_matches,
    ROW_NUMBER() OVER (ORDER BY kda_ratio DESC) as rank
FROM player_analytics
WHERE total_matches >= 10
ORDER BY kda_ratio DESC
LIMIT 100;
```

### PostgreSQL Features

- ✅ **15+ performance indexes** for fast queries
- ✅ **Auto-updating timestamps** via triggers
- ✅ **Idempotent writes** (truncate + insert)
- ✅ **Grafana-compatible views** for dashboards
- ✅ **Window functions** for rankings
- ✅ **Materialized aggregations** for KPIs

### MongoDB Collections

#### 1. top_players_by_kda

Global KDA rankings as documents.

```json
{
    "_id": "player_uuid",
    "player_id": "player_uuid",
    "summoner_name": "PlayerName",
    "kda": 4.25,
    "rank": 1,
    "total_matches": 50
}
```

#### 2. player_rankings_by_role

Top 20 players per role.

```json
{
    "_id": "role_player_uuid",
    "role": "mid",
    "player_id": "player_uuid",
    "summoner_name": "PlayerName",
    "kda": 4.10,
    "rank": 1
}
```

#### 3. composite_player_rankings

Weighted composite scores.

```json
{
    "_id": "player_uuid",
    "player_id": "player_uuid",
    "summoner_name": "PlayerName",
    "composite_score": 85.5,
    "rank": 1,
    "kda_score": 90,
    "winrate_score": 85,
    "efficiency_score": 82
}
```

---

## Streamlit Dashboard

**File:** `streamlit_app.py` (550 lines)

### Features

**5 Interactive Pages:**

1. **📊 Overview** - KPI dashboard with key metrics
2. **🎮 Matches** - Match analytics and filtering
3. **👤 Players** - Player performance metrics
4. **🏆 Teams** - Team rankings and comparisons
5. **📈 Trends** - Time series and trend analysis

### Dashboard Capabilities

- 📊 Real-time data from PostgreSQL
- 🎨 Interactive charts (Plotly, Altair)
- 🔍 Advanced filtering and search
- 📥 Export to CSV
- 📱 Responsive design
- 🎯 Drill-down analytics

### Running the Dashboard

```bash
# Install dependencies
pip install -r requirements/visualization.txt

# Start Streamlit
streamlit run streamlit_app.py

# Access at http://localhost:8501
```

### Dashboard Screenshots

**Overview Page:**
- Total matches, players, teams
- Average KDA, win rate
- Top performers cards

**Players Page:**
- Player search and filter
- KDA rankings table
- Win rate distribution
- Role performance comparison

**Teams Page:**
- Team rankings leaderboard
- Win rate comparison
- Match volume by team

**Trends Page:**
- Daily match volume chart
- KDA trend over time
- Win rate trends

---

## Deliverables

### ✅ Core Implementation (7 Files)

1. **PostgreSQL Components**
   - [src/storage/postgres/schema.sql](src/storage/postgres/schema.sql) - 450 lines
   - [src/storage/postgres/load_matches.py](src/storage/postgres/load_matches.py) - 245 lines
   - [src/storage/postgres/load_players.py](src/storage/postgres/load_players.py) - 280 lines

2. **MongoDB Components**
   - [src/storage/mongodb/load_documents.py](src/storage/mongodb/load_documents.py) - 330 lines

3. **Orchestration**
   - [src/storage/storage_main.py](src/storage/storage_main.py) - 195 lines

4. **BI Dashboard**
   - [streamlit_app.py](streamlit_app.py) - 550 lines

5. **Dependencies**
   - [requirements/storage.txt](requirements/storage.txt) - Storage dependencies

### ✅ Configuration

- PostgreSQL connection settings
- MongoDB connection URI
- Streamlit configuration
- Environment variables

---

## Validation

### Automated Validation

```bash
python validate_storage.py
```

**Validation Checks:**

1. ✅ **Database Connectivity** - PostgreSQL and MongoDB connections
2. ✅ **Table Existence** - All 5 tables created
3. ✅ **View Existence** - All 11 views created
4. ✅ **Data Quality** - Non-empty tables
5. ✅ **Index Verification** - All indexes created
6. ✅ **Record Counts** - Expected data volumes

### Manual Verification

#### PostgreSQL Queries

```sql
-- Check table sizes
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- Verify data
SELECT COUNT(*) FROM matches_analytics;
SELECT COUNT(*) FROM player_analytics;
SELECT COUNT(*) FROM team_rankings;

-- Test views
SELECT * FROM vw_top_players_kda LIMIT 10;
SELECT * FROM vw_daily_matches ORDER BY match_date DESC LIMIT 30;
```

#### MongoDB Queries

```javascript
// Connect to MongoDB
mongosh mongodb://localhost:27017/esports

// Check collections
show collections

// Count documents
db.top_players_by_kda.countDocuments()
db.player_rankings_by_role.countDocuments()
db.composite_player_rankings.countDocuments()

// Sample documents
db.top_players_by_kda.find().limit(5).pretty()
```

---

## Troubleshooting

### PostgreSQL Connection Issues

**Problem:** Cannot connect to PostgreSQL

**Solution:**
```bash
# Check PostgreSQL is running
docker-compose ps

# Test connection
psql -h localhost -U postgres -d esports

# Verify credentials in .env
echo $POSTGRES_HOST
echo $POSTGRES_USER
```

### MongoDB Connection Issues

**Problem:** Cannot connect to MongoDB

**Solution:**
```bash
# Check MongoDB is running
docker-compose ps

# Test connection
mongosh mongodb://localhost:27017

# Verify URI
echo $MONGODB_URI
```

### No Data Loaded

**Problem:** Tables are empty after loading

**Solution:**
```bash
# Check input data exists
ls -lh /data/silver/matches/
ls -lh /data/silver/players/

# Re-run Phase 3 and 4 if needed
python spark/main.py
python spark/analytics_main.py all

# Reload storage
python src/storage/storage_main.py all
```

### Streamlit Errors

**Problem:** Dashboard fails to start

**Solution:**
```bash
# Install dependencies
pip install -r requirements/visualization.txt

# Check PostgreSQL connection
python -c "import psycopg2; conn = psycopg2.connect('host=localhost dbname=esports user=postgres password=postgres')"

# Clear Streamlit cache
rm -rf ~/.streamlit/cache

# Run with debug
streamlit run streamlit_app.py --logger.level=debug
```

### Slow Query Performance

**Problem:** Queries taking too long

**Solution:**
```sql
-- Check missing indexes
SELECT * FROM pg_indexes WHERE schemaname = 'public';

-- Analyze tables
ANALYZE matches_analytics;
ANALYZE player_analytics;

-- Check query plan
EXPLAIN ANALYZE SELECT * FROM vw_top_players_kda;
```

---

## Performance Tips

### PostgreSQL Optimization

```sql
-- Create additional indexes if needed
CREATE INDEX idx_custom ON player_analytics(column_name);

-- Vacuum and analyze
VACUUM ANALYZE matches_analytics;
VACUUM ANALYZE player_analytics;

-- Enable parallel queries
SET max_parallel_workers_per_gather = 4;
```

### Spark JDBC Tuning

```python
# Increase batch size
df.write \
    .option("batchsize", 10000) \
    .option("numPartitions", 4) \
    .jdbc(url, table, properties)

# Use connection pooling
properties = {
    "driver": "org.postgresql.Driver",
    "numPartitions": "4",
    "batchsize": "10000"
}
```

### Streamlit Caching

```python
import streamlit as st

@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_data():
    return pd.read_sql(query, conn)
```

---

## Next Steps

Phase 5 Complete! ✅

**System Fully Operational:**
- ✅ Real-time data ingestion (Phase 2)
- ✅ Stream processing (Phase 3)
- ✅ Analytics pipeline (Phase 4)
- ✅ Storage & BI (Phase 5)

**Optional Enhancements:**
- Add Grafana dashboards
- Implement data quality monitoring
- Add ML predictions
- Set up automated reporting
- Implement data archival

---

## References

- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [MongoDB Documentation](https://docs.mongodb.com/)
- [Streamlit Documentation](https://docs.streamlit.io/)
- [Spark JDBC Guide](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)
- [Grafana Integration](https://grafana.com/docs/)
