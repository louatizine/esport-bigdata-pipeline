# Phase 4: Analytics & Batch Processing

## Overview

Phase 4 implements comprehensive analytical processing on the processed data from Phase 3. It includes match analytics, player analytics, and ranking computations using Spark SQL and advanced window functions.

## Table of Contents

- [Implementation Status](#implementation-status)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Components](#components)
- [Analytics Outputs](#analytics-outputs)
- [Deliverables](#deliverables)
- [Validation](#validation)
- [Troubleshooting](#troubleshooting)

---

## Implementation Status

**Status:** ✅ **PRODUCTION READY**

**Total Code:** 1,648 lines (Python + SQL)

All requirements for Phase 4 have been successfully implemented and validated.

### Deliverables Summary

| Component | Files | Lines | Status |
|-----------|-------|-------|--------|
| Match Analytics | 2 files | 328 | ✅ Complete |
| Player Analytics | 2 files | 374 | ✅ Complete |
| Ranking Analytics | 2 files | 391 | ✅ Complete |
| Main Orchestrator | 1 file | 182 | ✅ Complete |
| Validation | 1 file | 284 | ✅ Complete |
| **Total** | **10 files** | **1,648** | ✅ Complete |

---

## Architecture

### Data Flow

```
┌─────────────────────────────────────────────────────────┐
│                    PHASE 4 ANALYTICS                    │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  INPUT: /data/silver (Parquet from Phase 3)            │
│    ├── matches/                                        │
│    └── players/                                        │
│                                                         │
│  PROCESSING: Spark SQL Analytics                       │
│    ├── Match Analytics (7 aggregations)               │
│    ├── Player Analytics (8 aggregations)              │
│    └── Ranking Analytics (6 window functions)         │
│                                                         │
│  OUTPUT: /data/gold/analytics (21 Parquet datasets)    │
│    ├── matches/ (7 outputs)                           │
│    ├── players/ (8 outputs)                           │
│    └── rankings/ (6 outputs)                          │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

### Processing Layers

1. **Bronze Layer** (Phase 2): Raw data ingestion
2. **Silver Layer** (Phase 3): Cleaned and structured data
3. **Gold Layer** (Phase 4): **Analytics and aggregations** ← Current Phase

---

## Quick Start

### Prerequisites

- Phase 3 complete with data in `/data/silver/`
- Python 3.8+
- PySpark 3.5+

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

### Run Analytics

```bash
cd spark

# Run all analytics
python analytics_main.py all

# Run individual analytics
python analytics_main.py matches   # Match analytics only
python analytics_main.py players   # Player analytics only
python analytics_main.py rankings  # Ranking analytics only
```

### Validate Implementation

```bash
python validate_analytics.py
```

### Check Outputs

```bash
# List outputs
ls -lh /data/gold/analytics/matches/
ls -lh /data/gold/analytics/players/
ls -lh /data/gold/analytics/rankings/

# Count output datasets (expected: 21)
find /data/gold/analytics -name "_SUCCESS" | wc -l
```

---

## Components

### Main Orchestrator

**File:** `spark/analytics_main.py` (182 lines)

Centralized execution manager for all analytics jobs:

```python
# Run all analytics
python analytics_main.py all

# Run specific analytics
python analytics_main.py matches
python analytics_main.py players
python analytics_main.py rankings
```

Features:
- CLI argument support
- Error handling and logging
- Performance tracking
- Graceful shutdown

### Match Analytics

**Files:**
- `spark/analytics/match_analytics.py` (204 lines)
- `spark/sql/match_metrics.sql` (124 lines)

**7 Aggregations Implemented:**

1. **matches_by_status** - Match counts grouped by status
2. **avg_duration_by_status** - Average duration per status
3. **team_performance** - Team win rates and statistics
4. **tournament_stats** - Tournament activity metrics
5. **daily_match_volume** - Daily match count trends
6. **kill_statistics** - Kill patterns and averages
7. **gold_efficiency** - Economic efficiency metrics

Example:
```python
from spark.analytics.match_analytics import MatchAnalytics

analytics = MatchAnalytics(spark, input_path, output_path)
analytics.run_all_analytics()
```

### Player Analytics

**Files:**
- `spark/analytics/player_analytics.py` (218 lines)
- `spark/sql/player_metrics.sql` (156 lines)

**8 Aggregations Implemented:**

1. **player_win_rate** - Win rate rankings by player
2. **player_kda_metrics** - KDA statistics and averages
3. **player_efficiency** - CS and gold per minute efficiency
4. **active_players_by_role** - Active player counts by role
5. **team_composition** - Team roster analysis
6. **high_performers** - Top 10% high-performing players
7. **player_activity** - Player activity status tracking
8. **role_performance** - Performance comparison by role

Example:
```python
from spark.analytics.player_analytics import PlayerAnalytics

analytics = PlayerAnalytics(spark, input_path, output_path)
analytics.run_all_analytics()
```

### Ranking Analytics

**Files:**
- `spark/analytics/ranking_analytics.py` (201 lines)
- `spark/sql/ranking.sql` (190 lines)

**6 Rankings Implemented:**

1. **top_players_by_kda** - Global KDA rankings
2. **top_players_by_role** - Top 20 players per role
3. **team_rankings** - Team leaderboard with scores
4. **player_percentiles** - Performance percentile distributions
5. **composite_player_rankings** - Weighted composite scores
6. **player_performance_trends** - Historical trend analysis

**Window Functions Used:**
- `ROW_NUMBER()` - Sequential ranking
- `RANK()` - Ranking with ties
- `DENSE_RANK()` - Dense ranking
- `PERCENT_RANK()` - Percentile calculations
- `NTILE()` - Quartile/percentile buckets

Example:
```python
from spark.analytics.ranking_analytics import RankingAnalytics

analytics = RankingAnalytics(spark, input_path, output_path)
analytics.run_all_analytics()
```

### Validation Script

**File:** `spark/validate_analytics.py` (284 lines)

Comprehensive validation checks:
- ✅ File existence validation
- ✅ Schema validation
- ✅ Data quality checks
- ✅ Output verification
- ✅ Record count validation

```bash
python validate_analytics.py
```

---

## Analytics Outputs

### Match Analytics (7 Outputs)

All outputs saved in `/data/gold/analytics/matches/`:

| Output | Description | Key Metrics |
|--------|-------------|-------------|
| `matches_by_status/` | Match counts by status | count, status |
| `avg_duration_by_status/` | Duration statistics | avg_duration, min, max |
| `team_performance/` | Team win rates | win_rate, total_matches |
| `tournament_stats/` | Tournament activity | match_count, avg_duration |
| `daily_match_volume/` | Daily trends | date, match_count |
| `kill_statistics/` | Kill patterns | avg_kills, total_kills |
| `gold_efficiency/` | Economic metrics | gold_per_minute, efficiency |

### Player Analytics (8 Outputs)

All outputs saved in `/data/gold/analytics/players/`:

| Output | Description | Key Metrics |
|--------|-------------|-------------|
| `player_win_rate/` | Win rate rankings | player_id, win_rate, rank |
| `player_kda_metrics/` | KDA statistics | avg_kda, kills, deaths, assists |
| `player_efficiency/` | Efficiency metrics | cs_per_min, gold_per_min |
| `active_players_by_role/` | Role distribution | role, active_count |
| `team_composition/` | Team rosters | team_id, player_count |
| `high_performers/` | Top 10% players | player_id, performance_score |
| `player_activity/` | Activity status | player_id, last_match_date |
| `role_performance/` | Role comparisons | role, avg_kda, win_rate |

### Ranking Analytics (6 Outputs)

All outputs saved in `/data/gold/analytics/rankings/`:

| Output | Description | Key Metrics |
|--------|-------------|-------------|
| `top_players_by_kda/` | Global KDA rankings | rank, player_id, kda |
| `top_players_by_role/` | Top 20 per role | role, rank, player_id |
| `team_rankings/` | Team leaderboard | rank, team_id, score |
| `player_percentiles/` | Performance percentiles | player_id, percentile |
| `composite_player_rankings/` | Weighted composite | composite_score, rank |
| `player_performance_trends/` | Trend analysis | player_id, trend_direction |

---

## Deliverables

### ✅ Core Implementation (10 Files)

1. **Analytics Modules**
   - [spark/analytics/match_analytics.py](spark/analytics/match_analytics.py) - 204 lines
   - [spark/analytics/player_analytics.py](spark/analytics/player_analytics.py) - 218 lines
   - [spark/analytics/ranking_analytics.py](spark/analytics/ranking_analytics.py) - 201 lines

2. **SQL Queries**
   - [spark/sql/match_metrics.sql](spark/sql/match_metrics.sql) - 124 lines (7 views)
   - [spark/sql/player_metrics.sql](spark/sql/player_metrics.sql) - 156 lines (8 views)
   - [spark/sql/ranking.sql](spark/sql/ranking.sql) - 190 lines (6 views)

3. **Orchestration & Validation**
   - [spark/analytics_main.py](spark/analytics_main.py) - 182 lines
   - [spark/validate_analytics.py](spark/validate_analytics.py) - 284 lines

4. **Package Files**
   - [spark/analytics/__init__.py](spark/analytics/__init__.py)
   - [spark/sql/__init__.py](spark/sql/__init__.py)

### ✅ Configuration

- Environment variables for paths and Spark settings
- Configurable logging levels
- Parameterized SQL queries

---

## Validation

### Automated Validation

```bash
cd spark
python validate_analytics.py
```

**Validation Checks:**

1. ✅ **File Existence** - All 21 output directories exist
2. ✅ **Success Markers** - `_SUCCESS` files present
3. ✅ **Parquet Files** - Valid Parquet format
4. ✅ **Schema Validation** - Correct column types
5. ✅ **Data Quality** - Non-empty datasets
6. ✅ **Record Counts** - Expected data volumes

### Manual Verification

```bash
# Check match analytics
python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet('/data/gold/analytics/matches/matches_by_status')
df.show()
print(f'Records: {df.count()}')
"

# Check player analytics
python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet('/data/gold/analytics/players/player_win_rate')
df.show(20)
"

# Check rankings
python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet('/data/gold/analytics/rankings/top_players_by_kda')
df.orderBy('rank').show(20)
"
```

---

## Troubleshooting

### No Data in Output

**Problem:** Analytics run but no Parquet files generated

**Solution:**
```bash
# Check Phase 3 output exists
ls -lh /data/silver/matches/
ls -lh /data/silver/players/

# Re-run Phase 3 if needed
cd spark
python main.py
```

### Out of Memory

**Problem:** Spark jobs fail with OOM errors

**Solution:**
```bash
# Increase driver/executor memory
export SPARK_DRIVER_MEMORY=4g
export SPARK_EXECUTOR_MEMORY=4g

# Or configure in spark session
spark = SparkSession.builder \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()
```

### Slow Performance

**Problem:** Analytics jobs taking too long

**Solution:**
```bash
# Optimize shuffle partitions
export SPARK_SQL_SHUFFLE_PARTITIONS=100

# Cache frequently accessed data
df.cache()

# Use coalesce for small outputs
df.coalesce(1).write.parquet(path)
```

### Schema Mismatch

**Problem:** Schema validation fails

**Solution:**
```bash
# Check Silver layer schemas
python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.read.parquet('/data/silver/matches').printSchema()
spark.read.parquet('/data/silver/players').printSchema()
"

# Regenerate Silver layer if needed
python spark/main.py
```

### Missing Outputs

**Problem:** Some analytics outputs are missing

**Solution:**
```bash
# Run specific analytics module
python analytics_main.py matches
python analytics_main.py players
python analytics_main.py rankings

# Check logs for errors
tail -f logs/analytics.log
```

---

## Performance Tips

### Optimize for Large Datasets

```python
# Broadcast small lookup tables
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_df), "key")

# Partition data appropriately
df.repartition(200, "partition_key")

# Use appropriate file formats
df.write.parquet(path, compression="snappy")
```

### Tune Spark Configuration

```python
spark = SparkSession.builder \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()
```

### Monitor Performance

- Access Spark UI: http://localhost:4040
- Check DAG visualization
- Monitor stage execution times
- Review shuffle read/write metrics

---

## SQL Query Examples

### Match Analytics Query

```sql
-- Average duration by status
SELECT
    status,
    COUNT(*) as match_count,
    AVG(duration) as avg_duration,
    MIN(duration) as min_duration,
    MAX(duration) as max_duration
FROM matches
GROUP BY status
ORDER BY match_count DESC
```

### Player Analytics Query

```sql
-- Player win rate
SELECT
    player_id,
    summoner_name,
    COUNT(*) as total_matches,
    SUM(CASE WHEN win = true THEN 1 ELSE 0 END) as wins,
    ROUND(SUM(CASE WHEN win = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as win_rate
FROM players
GROUP BY player_id, summoner_name
HAVING COUNT(*) >= 5
ORDER BY win_rate DESC
```

### Ranking Analytics Query

```sql
-- Top players by KDA with ranking
SELECT
    player_id,
    summoner_name,
    AVG(kda) as avg_kda,
    ROW_NUMBER() OVER (ORDER BY AVG(kda) DESC) as rank,
    DENSE_RANK() OVER (ORDER BY AVG(kda) DESC) as dense_rank
FROM players
GROUP BY player_id, summoner_name
HAVING COUNT(*) >= 10
ORDER BY rank
LIMIT 100
```

---

## Next Steps

Phase 4 Complete! ✅

**Ready for Phase 5: Storage & Persistence**
- Implement MongoDB for document storage
- Set up PostgreSQL for relational data
- Create data loading pipelines
- Build storage validation

See [PHASE5.md](PHASE5.md) for details.

---

## References

- [Apache Spark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [Spark SQL Window Functions](https://spark.apache.org/docs/latest/sql-ref-functions-window.html)
- [PySpark DataFrame API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html)
- [Spark Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
