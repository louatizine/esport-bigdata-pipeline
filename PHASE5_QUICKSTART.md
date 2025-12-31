# Phase 5 Quick Start Guide

## 🚀 Quick Setup

### 1. Prerequisites

```bash
# Start PostgreSQL (via Docker Compose)
docker compose up -d postgres

# Install Python dependencies
pip install -r requirements/storage.txt
```

### 2. Initialize Database

```bash
# Connect to PostgreSQL
psql -h localhost -U postgres -d esports_analytics

# Create schema
\i src/storage/postgres/schema.sql

# Verify tables created
\dt

# Verify views created
\dv

# Exit
\q
```

### 3. Load Data

```bash
cd /workspaces/esport-bigdata-pipeline

# Set environment variables
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=postgres
export POSTGRES_DATABASE=esports_analytics
export PROCESSED_DATA_PATH=/data/silver
export DATA_LAKE_PATH=/data/gold

# Load all data (PostgreSQL + MongoDB)
python src/storage/storage_main.py --target all

# Or load only PostgreSQL
python src/storage/storage_main.py --target postgres --skip-mongodb
```

### 4. Launch Dashboard

```bash
# Set database credentials
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DATABASE=esports_analytics
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=postgres

# Launch Streamlit
streamlit run streamlit_app.py
```

Access dashboard at: **http://localhost:8501**

---

## 📊 Verify Data

### PostgreSQL

```bash
# Check record counts
psql -h localhost -U postgres -d esports_analytics -c "
SELECT
    'matches_analytics' as table_name, COUNT(*) as count FROM matches_analytics
UNION ALL
SELECT 'player_analytics', COUNT(*) FROM player_analytics
UNION ALL
SELECT 'team_rankings', COUNT(*) FROM team_rankings;
"

# View top players
psql -h localhost -U postgres -d esports_analytics -c "
SELECT * FROM vw_top_players_kda LIMIT 10;
"

# View KPIs
psql -h localhost -U postgres -d esports_analytics -c "
SELECT * FROM vw_player_kpis;
"
```

### MongoDB (Optional)

```bash
# Connect to MongoDB
mongo mongodb://localhost:27017/esports_analytics

# Check collections
show collections

# Count documents
db.top_players_by_kda.count()
db.composite_player_rankings.count()

# View top players
db.top_players_by_kda.find().limit(5).pretty()
```

---

## 🔧 Commands

### Load Individual Components

```bash
# Match data only
python src/storage/postgres/load_matches.py

# Player data only
python src/storage/postgres/load_players.py

# MongoDB documents only
python src/storage/mongodb/load_documents.py
```

### Query Examples

```sql
-- Top 10 players by KDA
SELECT * FROM vw_top_players_kda LIMIT 10;

-- Top teams by win rate
SELECT * FROM vw_team_performance LIMIT 20;

-- Match statistics by status
SELECT * FROM vw_match_stats_by_status;

-- Player KPIs
SELECT * FROM vw_player_kpis;

-- Active players for a specific team
SELECT * FROM vw_active_players
WHERE current_team_name = 'T1';

-- Role comparison
SELECT * FROM vw_role_comparison;
```

---

## 📁 Output Structure

### PostgreSQL Tables

```
esports_analytics database
├── matches_analytics (1,234 rows)
├── player_analytics (567 rows)
├── team_rankings (45 rows)
├── match_team_performance (45 rows)
└── player_role_stats (5 rows)
```

### PostgreSQL Views

```
├── vw_active_players
├── vw_top_players_kda
├── vw_top_players_winrate
├── vw_match_stats_by_status
├── vw_tournament_stats
├── vw_team_performance
├── vw_role_comparison
├── vw_daily_matches
├── vw_player_kpis
├── vw_match_kpis
└── vw_grafana_time_series
```

### MongoDB Collections

```
esports_analytics database
├── top_players_by_kda (100 docs)
├── player_rankings_by_role (120 docs)
└── composite_player_rankings (100 docs)
```

---

## 🐛 Troubleshooting

### Issue: JDBC Driver Not Found

**Error:** `java.lang.ClassNotFoundException: org.postgresql.Driver`

**Solution:**
```bash
# Download PostgreSQL JDBC driver
wget https://jdbc.postgresql.org/download/postgresql-42.7.1.jar -P /opt/spark/jars/

# Or set Spark classpath
export SPARK_CLASSPATH=/path/to/postgresql-42.7.1.jar
```

### Issue: Database Connection Failed

**Error:** `psycopg2.OperationalError: could not connect to server`

**Solution:**
```bash
# Check PostgreSQL is running
docker compose ps postgres

# Start if not running
docker compose up -d postgres

# Test connection
psql -h localhost -U postgres -d esports_analytics -c "SELECT 1;"
```

### Issue: No Data Loaded

**Error:** "No match data to load"

**Solution:**
```bash
# Verify Phase 3 data exists
ls -lh /data/silver/matches/
ls -lh /data/silver/players/

# If missing, run Phase 3 first
cd /workspaces/esport-bigdata-pipeline/spark
python main.py --job all
```

### Issue: Streamlit Connection Error

**Error:** "Cannot connect to database"

**Solution:**
```bash
# Set environment variables
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DATABASE=esports_analytics
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=postgres

# Restart Streamlit
streamlit run streamlit_app.py
```

---

## 📊 Streamlit Dashboard Pages

### 1. Overview
- Total players, matches, tournaments
- Avg win rate and match duration
- Top 10 players by KDA
- Top 10 players by win rate
- Role performance chart

### 2. Player Analytics
- Filter by role and team
- Top N players selector
- Player statistics table
- KDA vs Win Rate scatter plot

### 3. Team Analytics
- Top 20 teams by win rate
- Win rate comparison chart
- Avg kills per game
- Avg gold per game

### 4. Match Analytics
- Tournament statistics
- Match distribution by tournament
- Avg match duration trends

### 5. Rankings
- KDA rankings (top 50)
- Win rate rankings (top 50)
- Detailed player stats

---

## ⚡ Performance Tips

### PostgreSQL

1. **Create indexes** (already in schema.sql)
2. **Use views** for complex queries
3. **VACUUM ANALYZE** periodically:
   ```sql
   VACUUM ANALYZE matches_analytics;
   VACUUM ANALYZE player_analytics;
   ```

### Streamlit

1. **Cache enabled** - 5-minute TTL
2. **Limit results** - Use top N selector
3. **Filter data** - Use role/team filters

---

## 🔄 Update Data

To refresh data after new analytics runs:

```bash
# Re-run storage load (overwrites existing data)
python src/storage/storage_main.py --target all

# Dashboard will auto-refresh every 5 minutes (cache TTL)
# Or force refresh by restarting Streamlit
```

---

## 📈 Expected Output

### Load Summary
```
===============================================================================
STORAGE ORCHESTRATOR COMPLETED SUCCESSFULLY
===============================================================================
PostgreSQL records: 1,801
MongoDB documents: 321
Total duration: 45.23 seconds
===============================================================================
```

### Dashboard KPIs
- Total Players: 567 (423 active)
- Total Matches: 1,234
- Avg Win Rate: 48.5%
- Avg Match Duration: 32.4 min

---

## 📚 Documentation

- **Full Docs:** [PHASE5_STORAGE.md](PHASE5_STORAGE.md)
- **Schema:** [src/storage/postgres/schema.sql](src/storage/postgres/schema.sql)
- **Main README:** [README.md](README.md)

---

**Phase 5 Status:** ✅ Complete
**Streamlit:** http://localhost:8501
**PostgreSQL:** localhost:5432
