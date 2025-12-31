# Phase 4: Analytics & Aggregations

## Overview

Phase 4 implements advanced analytics and aggregations using Apache Spark SQL. This phase reads processed Parquet data from Phase 3 (Spark Structured Streaming) and generates comprehensive analytics insights.

## Architecture

```
spark/
├── analytics/
│   ├── __init__.py
│   ├── match_analytics.py      # Match-level aggregations
│   ├── player_analytics.py     # Player-level aggregations
│   └── ranking_analytics.py    # Advanced rankings with window functions
├── sql/
│   ├── __init__.py
│   ├── match_metrics.sql       # Match analytics queries
│   ├── player_metrics.sql      # Player analytics queries
│   └── ranking.sql             # Ranking queries with window functions
└── analytics_main.py           # Main orchestrator
```

## Data Flow

```
PROCESSED_DATA_PATH/matches (Parquet)
PROCESSED_DATA_PATH/players (Parquet)
          ↓
    Analytics Jobs
    (Spark SQL)
          ↓
DATA_LAKE_PATH/analytics/ (Parquet)
  ├── matches/
  │   ├── matches_by_status/
  │   ├── avg_duration_by_status/
  │   ├── team_performance/
  │   ├── tournament_stats/
  │   ├── daily_match_volume/
  │   ├── kill_statistics/
  │   └── gold_efficiency/
  ├── players/
  │   ├── player_win_rate/
  │   ├── player_kda_metrics/
  │   ├── player_efficiency/
  │   ├── active_players_by_role/
  │   ├── team_composition/
  │   ├── high_performers/
  │   ├── player_activity/
  │   ├── role_performance/
  │   └── nationality_stats/
  └── rankings/
      ├── top_players_by_kda/
      ├── top_players_by_role/
      ├── team_rankings/
      ├── player_percentiles/
      ├── tournament_running_totals/
      ├── player_momentum/
      ├── composite_player_rankings/
      └── player_performance_trends/
```

## Analytics Components

### 1. Match Analytics

**File:** `analytics/match_analytics.py`

**Features:**
- Total matches by status
- Average game duration statistics
- Team performance metrics
- Tournament statistics
- Daily match volume trends
- Kill statistics
- Gold efficiency analysis

**SQL Queries:** `sql/match_metrics.sql`

**Key Metrics:**
- Win rates
- Average kills/gold per game
- Match duration percentiles
- Tournament participation

### 2. Player Analytics

**File:** `analytics/player_analytics.py`

**Features:**
- Player win rates and W/L ratios
- KDA (Kill/Death/Assist) metrics
- CS (Creep Score) and gold efficiency
- Active players by role
- Team composition analysis
- High performers identification
- Player activity tracking
- Role performance comparison
- Nationality statistics

**SQL Queries:** `sql/player_metrics.sql`

**Key Metrics:**
- Win rate percentage
- KDA ratio: (Kills + Assists) / Deaths
- CS per minute
- Gold per minute
- Activity levels

### 3. Ranking Analytics

**File:** `analytics/ranking_analytics.py`

**Features:**
- Top players by KDA with ranking functions
- Role-specific rankings
- Team rankings by win rate
- Player performance percentiles
- Tournament running totals
- Player momentum (recent form)
- Composite player rankings (weighted scores)
- Performance trend analysis

**SQL Queries:** `sql/ranking.sql`

**Window Functions Used:**
- `ROW_NUMBER()` - Sequential ranking
- `RANK()` - Ranking with ties
- `DENSE_RANK()` - Dense ranking
- `NTILE()` - Percentile buckets
- `PERCENT_RANK()` - Percentile calculation
- `LAG()` / `LEAD()` - Trend analysis

**Composite Scoring:**
- KDA: 35% weight
- Win Rate: 35% weight
- CS per minute: 15% weight
- Gold per minute: 15% weight

## Environment Variables

```bash
# Data paths
PROCESSED_DATA_PATH=/data/silver      # Input: Phase 3 output
DATA_LAKE_PATH=/data/gold             # Output: Analytics results

# Spark configuration
SPARK_DRIVER_MEMORY=2g
SPARK_EXECUTOR_MEMORY=2g
```

## Usage

### Run All Analytics

```bash
cd /workspaces/esport-bigdata-pipeline/spark
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

### Programmatic Usage

```python
from analytics.match_analytics import MatchAnalytics
from analytics.player_analytics import PlayerAnalytics
from analytics.ranking_analytics import RankingAnalytics

# Match analytics
match_analytics = MatchAnalytics()
match_analytics.run_match_analytics()
summary = match_analytics.get_summary_stats()

# Player analytics
player_analytics = PlayerAnalytics()
player_analytics.run_player_analytics()
summary = player_analytics.get_summary_stats()

# Ranking analytics
ranking_analytics = RankingAnalytics()
ranking_analytics.run_ranking_analytics()
top_players = ranking_analytics.get_top_rankings(limit=10)
```

## SQL Query Structure

All SQL files follow this pattern:

```sql
-- Create temporary views
CREATE OR REPLACE TEMP VIEW view_name AS
SELECT
    column1,
    column2,
    AGG_FUNCTION(column3) as metric
FROM source_table
GROUP BY column1, column2;
```

Views are then saved to Parquet by the Python analytics modules.

## Output Format

All analytics results are saved as Parquet files:

```
/data/gold/analytics/
├── matches/
│   └── team_performance/
│       ├── part-00000-*.parquet
│       └── _SUCCESS
├── players/
│   └── player_kda_metrics/
│       ├── part-00000-*.parquet
│       └── _SUCCESS
└── rankings/
    └── composite_player_rankings/
        ├── part-00000-*.parquet
        └── _SUCCESS
```

## Performance Optimization

- **Partitioning:** Results are not partitioned (aggregated data is small)
- **Compression:** Parquet with Snappy compression
- **Caching:** DataFrames are cached during complex transformations
- **Broadcast Joins:** Small lookup tables are broadcasted
- **Predicate Pushdown:** Filters applied early in query execution

## Monitoring & Logging

All analytics jobs include:
- Structured logging with timestamps
- Record counts for each operation
- Execution time tracking
- Summary statistics reporting
- Error handling with detailed messages

## Dependencies

- Apache Spark 3.5.0
- PySpark
- Python 3.x
- Parquet data from Phase 3

## Error Handling

- Graceful handling of missing data
- Division by zero protection with `NULLIF()`
- NULL handling in aggregations
- Comprehensive exception logging
- SparkSession cleanup in finally blocks

## Future Enhancements

- Real-time analytics dashboard integration
- Incremental analytics (process only new data)
- ML feature engineering integration
- Advanced statistical analysis
- Anomaly detection
- Predictive analytics

## Testing

```bash
# Ensure Phase 3 has generated data
ls -lh /data/silver/matches/
ls -lh /data/silver/players/

# Run analytics
python analytics_main.py all

# Verify output
ls -lh /data/gold/analytics/matches/
ls -lh /data/gold/analytics/players/
ls -lh /data/gold/analytics/rankings/
```

## Troubleshooting

**Issue:** No data in output
- **Solution:** Verify Phase 3 has written data to `/data/silver/`

**Issue:** SQL syntax errors
- **Solution:** Check SQL files for typos, ensure proper semicolon separation

**Issue:** Memory errors
- **Solution:** Increase `SPARK_DRIVER_MEMORY` and `SPARK_EXECUTOR_MEMORY`

**Issue:** Missing views
- **Solution:** Ensure prerequisite views are created before dependent queries

## Contact

For issues or questions, refer to the main project README.
