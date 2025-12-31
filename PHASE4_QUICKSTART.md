# Phase 4 Quick Start Guide

## 🚀 Quick Commands

### Setup Environment
```bash
cd /workspaces/esport-bigdata-pipeline

# Set required environment variables
export PROCESSED_DATA_PATH=/data/silver
export DATA_LAKE_PATH=/data/gold
export SPARK_DRIVER_MEMORY=2g
export SPARK_EXECUTOR_MEMORY=2g
export LOG_LEVEL=INFO
```

### Run All Analytics
```bash
cd spark
python analytics_main.py all
```

### Run Individual Analytics
```bash
# Match analytics only
python analytics_main.py matches

# Player analytics only
python analytics_main.py players

# Ranking analytics only
python analytics_main.py rankings
```

### Validate Implementation
```bash
python validate_analytics.py
```

### Check Outputs
```bash
# List match analytics outputs
ls -lh /data/gold/analytics/matches/

# List player analytics outputs
ls -lh /data/gold/analytics/players/

# List ranking analytics outputs
ls -lh /data/gold/analytics/rankings/

# Count output datasets
find /data/gold/analytics -name "_SUCCESS" | wc -l
# Expected: 21
```

---

## 📂 Expected Outputs

### Matches (7 outputs)
- `matches_by_status/`
- `avg_duration_by_status/`
- `team_performance/`
- `tournament_stats/`
- `daily_match_volume/`
- `kill_statistics/`
- `gold_efficiency/`

### Players (8 outputs)
- `player_win_rate/`
- `player_kda_metrics/`
- `player_efficiency/`
- `active_players_by_role/`
- `team_composition/`
- `high_performers/`
- `player_activity/`
- `role_performance/`

### Rankings (6 outputs)
- `top_players_by_kda/`
- `top_players_by_role/`
- `team_rankings/`
- `player_percentiles/`
- `composite_player_rankings/`
- `player_performance_trends/`

---

## 🐛 Troubleshooting

### No Data in Output
**Problem:** Analytics run but no Parquet files generated
**Solution:**
```bash
# Check Phase 3 output exists
ls -lh /data/silver/matches/
ls -lh /data/silver/players/

# Re-run Phase 3 if needed
cd spark
python main.py --job all
```

### Memory Errors
**Problem:** `OutOfMemoryError` or Java heap space
**Solution:**
```bash
# Increase Spark memory
export SPARK_DRIVER_MEMORY=4g
export SPARK_EXECUTOR_MEMORY=4g

# Or edit spark/utils/spark_session.py
```

### SQL Errors
**Problem:** SQL syntax errors or missing views
**Solution:**
```bash
# Check SQL files for typos
cat spark/sql/match_metrics.sql
cat spark/sql/player_metrics.sql
cat spark/sql/ranking.sql

# Ensure proper semicolons and syntax
```

### Import Errors
**Problem:** `ModuleNotFoundError` for analytics modules
**Solution:**
```bash
# Ensure you're in the spark/ directory
cd /workspaces/esport-bigdata-pipeline/spark

# Or set PYTHONPATH
export PYTHONPATH=/workspaces/esport-bigdata-pipeline/spark:$PYTHONPATH
```

---

## 📊 Read Analytics Results

### Using PySpark
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadAnalytics").getOrCreate()

# Read match analytics
matches_by_status = spark.read.parquet("/data/gold/analytics/matches/matches_by_status")
matches_by_status.show()

# Read player rankings
top_players = spark.read.parquet("/data/gold/analytics/rankings/composite_player_rankings")
top_players.orderBy("overall_rank").show(20)

# Read team performance
team_perf = spark.read.parquet("/data/gold/analytics/matches/team_performance")
team_perf.orderBy("win_rate", ascending=False).show()
```

### Using Pandas
```python
import pandas as pd

# Read with PyArrow engine
matches_by_status = pd.read_parquet("/data/gold/analytics/matches/matches_by_status")
print(matches_by_status)

# Read player rankings
top_players = pd.read_parquet("/data/gold/analytics/rankings/composite_player_rankings")
print(top_players.sort_values("overall_rank").head(20))
```

---

## 🔍 Sample Analytics Queries

### Top 10 Players by KDA
```python
from analytics.ranking_analytics import RankingAnalytics

ranking = RankingAnalytics()
top_10 = ranking.get_top_rankings(limit=10)

for player in top_10:
    print(f"{player['overall_rank']}. {player['summoner_name']} - Score: {player['composite_score']:.2f}")
```

### Match Statistics Summary
```python
from analytics.match_analytics import MatchAnalytics

match = MatchAnalytics()
match.run_match_analytics()
summary = match.get_summary_stats()
print(summary)
```

### Player Performance by Role
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
role_perf = spark.read.parquet("/data/gold/analytics/players/role_performance")
role_perf.show()
```

---

## ⏱️ Expected Runtime

- **Match Analytics:** ~10-30 seconds (depends on data size)
- **Player Analytics:** ~15-40 seconds (depends on data size)
- **Ranking Analytics:** ~20-50 seconds (window functions are expensive)
- **Total (All Jobs):** ~1-2 minutes

---

## 📝 Logs Location

Logs are printed to stdout/stderr by default:

```bash
# Redirect to file
python analytics_main.py all > analytics_$(date +%Y%m%d_%H%M%S).log 2>&1

# View logs in real-time
python analytics_main.py all 2>&1 | tee analytics.log
```

---

## 🎯 Next Steps

After running Phase 4 analytics:

1. **Verify Outputs:** Check all 21 datasets exist
2. **Explore Results:** Query analytics with PySpark/Pandas
3. **Integrate Visualization:** Feed data to dashboards (Phase 6)
4. **Machine Learning:** Use analytics as features (Phase 5)
5. **Schedule Jobs:** Set up automated analytics runs

---

**Phase 4 Status:** ✅ Complete
**Documentation:** [PHASE4_ANALYTICS.md](PHASE4_ANALYTICS.md)
**Implementation Summary:** [PHASE4_IMPLEMENTATION_SUMMARY.md](PHASE4_IMPLEMENTATION_SUMMARY.md)
