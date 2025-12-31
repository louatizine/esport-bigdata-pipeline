# Phase 5: Storage & BI Integration

## Overview

Phase 5 implements storage and BI integration, loading analytics data from Parquet into PostgreSQL and MongoDB, with a Streamlit-based interactive dashboard for business intelligence.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    PHASE 5 STORAGE                      │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  INPUT: /data/gold/analytics (Parquet from Phase 4)    │
│         /data/silver (Parquet from Phase 3)            │
│                                                         │
│  PROCESSING: Spark JDBC Writers                        │
│    ├── PostgreSQL (Primary Analytics Storage)         │
│    │   - matches_analytics                            │
│    │   - player_analytics                             │
│    │   - team_rankings                                │
│    │   - match_team_performance                       │
│    │   - player_role_stats                            │
│    │                                                   │
│    └── MongoDB (Optional Document Storage)             │
│        - top_players_by_kda                           │
│        - player_rankings_by_role                      │
│        - composite_player_rankings                    │
│                                                         │
│  OUTPUT: Queryable databases + Streamlit Dashboard     │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

## Directory Structure

```
src/storage/
├── postgres/
│   ├── __init__.py
│   ├── schema.sql                  # PostgreSQL DDL (tables, views, indexes)
│   ├── load_matches.py             # Match data loader
│   └── load_players.py             # Player data loader
├── mongodb/
│   ├── __init__.py
│   └── load_documents.py           # MongoDB document loader
└── storage_main.py                 # Main orchestrator

streamlit_app.py                    # Interactive BI dashboard
requirements/storage.txt            # Phase 5 dependencies
```

## PostgreSQL Schema

### Tables

1. **matches_analytics** - Match-level analytics
   - Primary Key: `match_id`
   - Indexes: tournament_name, status, started_at, winner_name, teams

2. **player_analytics** - Player performance metrics
   - Primary Key: `player_id`
   - Indexes: team, role, status, win_rate, kda_ratio

3. **team_rankings** - Team rankings by performance
   - Primary Key: `team_name`
   - Indexes: rank, win_rate, kda

4. **match_team_performance** - Aggregated team stats
   - Primary Key: `id`
   - Unique: `team_name`

5. **player_role_stats** - Aggregated role statistics
   - Primary Key: `id`
   - Unique: `role`

### BI-Optimized Views

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

### Features

- ✅ Auto-updating timestamps with triggers
- ✅ Comprehensive indexes for query performance
- ✅ Grafana-compatible views
- ✅ Window functions for rankings
- ✅ Materialized aggregations

## MongoDB Collections

1. **top_players_by_kda** - Top players ranked by KDA
2. **player_rankings_by_role** - Role-specific rankings
3. **composite_player_rankings** - Composite score rankings

### Indexes
- Unique indexes on `player_id`
- Performance indexes on rankings and scores
- Compound indexes for complex queries

## Environment Variables

```bash
# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DATABASE=esports_analytics

# MongoDB (optional)
MONGO_HOST=localhost
MONGO_PORT=27017
MONGO_USER=
MONGO_PASSWORD=
MONGO_DATABASE=esports_analytics

# Data paths
PROCESSED_DATA_PATH=/data/silver
DATA_LAKE_PATH=/data/gold
```

## Usage

### 1. Initialize PostgreSQL Schema

```bash
# Connect to PostgreSQL
psql -h localhost -U postgres -d esports_analytics

# Execute schema
\i src/storage/postgres/schema.sql
```

### 2. Load Data to PostgreSQL

```bash
# Load all data (PostgreSQL + MongoDB)
python src/storage/storage_main.py --target all

# Load only PostgreSQL
python src/storage/storage_main.py --target postgres

# Load only MongoDB
python src/storage/storage_main.py --target mongodb

# Skip MongoDB
python src/storage/storage_main.py --skip-mongodb
```

### 3. Load Individual Components

```bash
# Match data only
python src/storage/postgres/load_matches.py

# Player data only
python src/storage/postgres/load_players.py

# MongoDB documents only
python src/storage/mongodb/load_documents.py
```

### 4. Launch Streamlit Dashboard

```bash
streamlit run streamlit_app.py
```

Access at: http://localhost:8501

## Streamlit Dashboard Features

### Pages

1. **Overview**
   - Total players, matches, tournaments
   - Avg win rate and match duration
   - Top 10 players by KDA and win rate
   - Role performance comparison

2. **Player Analytics**
   - Filter by role and team
   - Player statistics table
   - KDA vs Win Rate scatter plot
   - Performance metrics

3. **Team Analytics**
   - Top 20 teams by win rate
   - Win rate comparison
   - Avg kills and gold per game
   - Team rankings

4. **Match Analytics**
   - Tournament statistics
   - Match distribution
   - Avg match duration
   - Kills per match

5. **Rankings**
   - KDA rankings (top 50)
   - Win rate rankings (top 50)
   - Detailed player statistics

### Features

- ✅ Real-time data from PostgreSQL
- ✅ Interactive filters (role, team, limit)
- ✅ Plotly visualizations
- ✅ Responsive design
- ✅ 5-minute cache for performance
- ✅ Sidebar navigation

## Data Flow

```
Phase 3 (Parquet)      Phase 4 (Parquet)
       ↓                      ↓
   /silver/matches      /gold/analytics/
   /silver/players      ├── matches/
                        ├── players/
                        └── rankings/
       ↓                      ↓
       └──────────┬───────────┘
                  ↓
          Storage Loaders
          (Spark JDBC)
                  ↓
       ┌──────────┴──────────┐
       ↓                     ↓
   PostgreSQL            MongoDB
   (Primary)          (Optional)
       ↓                     ↓
   BI Tools          Document Queries
   - Streamlit
   - Grafana
   - Tableau
```

## Performance Optimizations

### PostgreSQL

1. **Indexes** - All critical columns indexed
2. **Views** - Pre-computed aggregations
3. **Triggers** - Auto-updating timestamps
4. **JDBC Batch Writes** - Efficient bulk inserts

### Streamlit

1. **Caching** - 5-minute TTL on queries
2. **Connection Pooling** - SQLAlchemy engine
3. **Limit Results** - Configurable top N
4. **Lazy Loading** - Load data on page view

## JDBC Configuration

For Spark to connect to PostgreSQL, add the JDBC driver:

```bash
# Download PostgreSQL JDBC driver
wget https://jdbc.postgresql.org/download/postgresql-42.7.1.jar -P /opt/spark/jars/

# Or add to spark-submit
spark-submit \
  --jars /path/to/postgresql-42.7.1.jar \
  src/storage/storage_main.py
```

Or configure in `conf/spark/spark-defaults.conf`:
```
spark.jars /opt/spark/jars/postgresql-42.7.1.jar
```

## Idempotent Writes

All loaders use `mode="overwrite"` which:
1. Truncates existing table data
2. Inserts new data
3. Maintains schema
4. Ensures data consistency

No duplicate records on re-runs.

## Error Handling

- ✅ Connection validation before operations
- ✅ Try-except blocks with detailed logging
- ✅ Graceful degradation (MongoDB optional)
- ✅ SparkSession cleanup in finally blocks
- ✅ Database connection cleanup

## Monitoring & Logging

All operations include:
- Record counts
- Execution times
- Success/failure status
- Detailed error messages
- Progress tracking

Example output:
```
===============================================================================
STORAGE ORCHESTRATOR STARTED
Start time: 2024-12-31 10:00:00
===============================================================================

===============================================================================
POSTGRESQL LOAD STARTED
===============================================================================

[1/2] Loading match data...
===============================================================================
LOADING MATCHES ANALYTICS TO POSTGRESQL
===============================================================================
Reading matches from: /data/silver/matches
Total match records to load: 1,234
Writing to PostgreSQL table: matches_analytics
✅ Successfully loaded 1,234 match records to PostgreSQL

[2/2] Loading player data...
===============================================================================
LOADING PLAYER ANALYTICS TO POSTGRESQL
===============================================================================
Reading players from: /data/silver/players
Total player records to load: 567
Writing to PostgreSQL table: player_analytics
✅ Successfully loaded 567 player records to PostgreSQL

===============================================================================
STORAGE ORCHESTRATOR COMPLETED SUCCESSFULLY
===============================================================================
PostgreSQL records: 1,801
MongoDB documents: 321
Total duration: 45.23 seconds
===============================================================================
```

## Grafana Integration

Use the provided views for Grafana dashboards:

1. **Time Series Panel** - `vw_grafana_time_series`
2. **Table Panel** - Any view
3. **Stat Panel** - `vw_player_kpis`, `vw_match_kpis`

Example Grafana query:
```sql
SELECT
  time,
  metric as tournament,
  value as duration
FROM vw_grafana_time_series
WHERE $__timeFilter(time)
ORDER BY time
```

## Dependencies

- Apache Spark 3.5.0
- PostgreSQL 13+ (JDBC driver required)
- MongoDB 5+ (optional)
- Python 3.11+
- Streamlit 1.29+
- SQLAlchemy 2.0+
- PyMongo 4.6+

## Troubleshooting

### JDBC Driver Not Found
**Error:** `java.lang.ClassNotFoundException: org.postgresql.Driver`

**Solution:**
```bash
# Download JDBC driver
wget https://jdbc.postgresql.org/download/postgresql-42.7.1.jar

# Add to Spark jars
export SPARK_CLASSPATH=/path/to/postgresql-42.7.1.jar
```

### Database Connection Failed
**Error:** `could not connect to server`

**Solution:**
```bash
# Check PostgreSQL is running
docker compose ps postgres

# Verify environment variables
echo $POSTGRES_HOST
echo $POSTGRES_PORT

# Test connection
psql -h $POSTGRES_HOST -U $POSTGRES_USER -d $POSTGRES_DATABASE
```

### No Data in Tables
**Error:** Tables exist but no data

**Solution:**
```bash
# Verify Phase 3 and 4 data exists
ls -lh /data/silver/matches/
ls -lh /data/gold/analytics/

# Re-run storage load
python src/storage/storage_main.py --target all
```

### Streamlit Connection Error
**Error:** Cannot connect to database

**Solution:**
```bash
# Set environment variables before launching
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DATABASE=esports_analytics

streamlit run streamlit_app.py
```

## Testing

```bash
# 1. Load test data
python src/storage/storage_main.py --target postgres

# 2. Verify in PostgreSQL
psql -h localhost -U postgres -d esports_analytics -c "SELECT COUNT(*) FROM matches_analytics;"
psql -h localhost -U postgres -d esports_analytics -c "SELECT COUNT(*) FROM player_analytics;"

# 3. Test views
psql -h localhost -U postgres -d esports_analytics -c "SELECT * FROM vw_player_kpis;"

# 4. Launch dashboard
streamlit run streamlit_app.py
```

## Future Enhancements

1. **Incremental Loads** - Update only changed records
2. **Data Validation** - Schema validation before load
3. **Monitoring Dashboard** - Load statistics tracking
4. **Automated Scheduling** - Cron/Airflow integration
5. **Data Quality Checks** - Completeness and accuracy metrics
6. **Multi-tenant Support** - Separate databases per region
7. **API Layer** - REST API for external integrations

## Contact

For issues or questions, refer to the main project README.

---

**Phase 5 Status:** ✅ Complete
**Implementation Date:** December 2024
**Production Ready:** Yes
