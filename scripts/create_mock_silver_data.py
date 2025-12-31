#!/usr/bin/env python3
"""Create mock silver layer data for testing analytics."""

from pyspark.sql.types import *
from pyspark.sql import SparkSession
import sys
import os
from datetime import datetime, timedelta
import random

sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")))


def create_spark_session():
    return SparkSession.builder \
        .appName("MockSilverData") \
        .master("local[*]") \
        .getOrCreate()


def generate_mock_matches(spark, num_matches=50):
    """Generate mock match data."""

    matches_data = []
    base_time = int(datetime.now().timestamp() * 1000)

    champions = ["Ahri", "Lux", "Zed", "Yasuo", "Lee Sin",
                 "Jinx", "Thresh", "Ekko", "Vi", "Caitlyn"]

    for i in range(num_matches):
        match_id = f"MATCH_{i:04d}"
        game_duration = random.randint(1200, 2400)  # 20-40 minutes
        game_creation = base_time - (i * 3600000)  # 1 hour apart

        # Create 10 participants
        participants = []
        for p in range(10):
            team_id = 100 if p < 5 else 200
            win = random.choice([True, False])

            participant = {
                "match_id": match_id,
                "summoner_name": f"Player_{random.randint(1, 100)}",
                "champion_name": random.choice(champions),
                "team_id": team_id,
                "kills": random.randint(0, 20),
                "deaths": random.randint(0, 15),
                "assists": random.randint(0, 25),
                "total_damage": random.randint(10000, 50000),
                "gold_earned": random.randint(8000, 18000),
                "vision_score": random.randint(10, 80),
                "cs": random.randint(100, 300),
                "win": win,
                "match_duration": game_duration,
                "game_creation": game_creation,
                "game_mode": "CLASSIC",
                "game_type": "MATCHED_GAME",
                "role": random.choice(["TOP", "JUNGLE", "MID", "ADC", "SUPPORT"]),
                "lane": random.choice(["TOP", "JUNGLE", "MIDDLE", "BOTTOM", "SUPPORT"])
            }
            participants.append(participant)

        matches_data.extend(participants)

    # Create schema
    schema = StructType([
        StructField("match_id", StringType(), True),
        StructField("summoner_name", StringType(), True),
        StructField("champion_name", StringType(), True),
        StructField("team_id", IntegerType(), True),
        StructField("kills", IntegerType(), True),
        StructField("deaths", IntegerType(), True),
        StructField("assists", IntegerType(), True),
        StructField("total_damage", IntegerType(), True),
        StructField("gold_earned", IntegerType(), True),
        StructField("vision_score", IntegerType(), True),
        StructField("cs", IntegerType(), True),
        StructField("win", BooleanType(), True),
        StructField("match_duration", IntegerType(), True),
        StructField("game_creation", LongType(), True),
        StructField("game_mode", StringType(), True),
        StructField("game_type", StringType(), True),
        StructField("role", StringType(), True),
        StructField("lane", StringType(), True),
    ])

    df = spark.createDataFrame(matches_data, schema)
    return df


def main():
    print("🎮 Creating Mock Silver Layer Data")
    print("=" * 70)

    spark = create_spark_session()

    # Generate matches
    print("\n📊 Generating mock matches...")
    matches_df = generate_mock_matches(spark, num_matches=50)
    print(f"✅ Generated {matches_df.count()} match participants")

    # Write to silver layer
    output_path = "/workspaces/esport-bigdata-pipeline/data/silver/matches"
    print(f"\n💾 Writing to: {output_path}")

    matches_df.write \
        .mode("overwrite") \
        .parquet(output_path)

    print(f"✅ Data written successfully!")

    # Show sample
    print("\n📋 Sample data:")
    matches_df.select("match_id", "summoner_name", "champion_name",
                      "kills", "deaths", "assists", "win").show(10)

    print("\n" + "=" * 70)
    print("🎉 Mock silver data creation complete!")
    print(f"📂 Location: {output_path}")
    print("🚀 Ready for analytics!")

    spark.stop()


if __name__ == "__main__":
    main()
