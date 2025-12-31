#!/usr/bin/env python3
"""Quick loader - Load silver data directly to PostgreSQL for testing."""

from pyspark.sql.window import Window
from pyspark.sql.functions import col, avg, sum as spark_sum, count, max as spark_max, stddev, row_number, rank
from pyspark.sql import SparkSession
import sys
import os
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")))


# PostgreSQL connection
POSTGRES_URL = "jdbc:postgresql://localhost:5432/esports_analytics"
POSTGRES_PROPS = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}


def get_spark():
    return SparkSession.builder \
        .appName("QuickLoader") \
        .master("local[*]") \
        .config("spark.jars", "/usr/share/java/postgresql-jdbc.jar") \
        .getOrCreate()


def load_data():
    print("🚀 Quick PostgreSQL Loader")
    print("=" * 70)

    spark = get_spark()

    # Load silver data
    print("\n📖 Loading silver data...")
    df = spark.read.parquet(
        "/workspaces/esport-bigdata-pipeline/data/silver/matches")
    print(f"✅ Loaded {df.count()} records")

    # Create matches_analytics
    print("\n📊 Creating matches_analytics...")
    matches_analytics = df.groupBy("match_id") \
        .agg(
            spark_max("match_duration").alias("duration"),
            spark_max("game_creation").alias("game_creation"),
            count("*").alias("player_count"),
            avg("kills").alias("avg_kills"),
            avg("deaths").alias("avg_deaths"),
            avg("assists").alias("avg_assists"),
            spark_sum("total_damage").alias("total_damage")
    )

    matches_analytics.write \
        .jdbc(url=POSTGRES_URL, table="matches_analytics", mode="overwrite", properties=POSTGRES_PROPS)
    print(f"✅ Loaded {matches_analytics.count()} matches")

    # Create player_analytics
    print("\n👤 Creating player_analytics...")
    player_analytics = df.groupBy("summoner_name", "champion_name") \
        .agg(
            count("*").alias("games_played"),
            spark_sum(col("win").cast("int")).alias("wins"),
            avg("kills").alias("avg_kills"),
            avg("deaths").alias("avg_deaths"),
            avg("assists").alias("avg_assists"),
            avg("gold_earned").alias("avg_gold"),
            avg("vision_score").alias("avg_vision"),
            avg("cs").alias("avg_cs"),
            spark_max("match_duration").alias("last_game_duration")
    )

    player_analytics.write \
        .jdbc(url=POSTGRES_URL, table="player_analytics", mode="overwrite", properties=POSTGRES_PROPS)
    print(f"✅ Loaded {player_analytics.count()} player records")

    # Create team_rankings
    print("\n🏆 Creating team_rankings...")
    team_data = df.select("summoner_name", "wins", "team_id").distinct()
    window_spec = Window.orderBy(col("total_wins").desc())

    team_rankings = df.groupBy("summoner_name") \
        .agg(
            spark_sum(col("win").cast("int")).alias("total_wins"),
            count("*").alias("total_games")
    ) \
        .withColumn("rank", row_number().over(window_spec)) \
        .limit(100)

    team_rankings.write \
        .jdbc(url=POSTGRES_URL, table="team_rankings", mode="overwrite", properties=POSTGRES_PROPS)
    print(f"✅ Loaded {team_rankings.count()} team rankings")

    print("\n" + "=" * 70)
    print("✅ Quick load complete!")
    print("🎯 PostgreSQL ready for dashboard")

    spark.stop()


if __name__ == "__main__":
    load_data()
