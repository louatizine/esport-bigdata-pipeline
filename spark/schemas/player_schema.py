"""
Schema definition for player data from Kafka topics.
Defines the structure of incoming esports player events.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    BooleanType,
    TimestampType
)


def get_player_schema():
    """
    Returns the schema for player data streaming from Kafka.

    Schema includes:
    - Player identifiers and metadata
    - Team association
    - Player statistics
    - Performance metrics

    Returns:
        StructType: Spark schema for player data
    """
    return StructType([
        # Player identifiers
        StructField("player_id", StringType(), False),
        StructField("summoner_name", StringType(), True),
        StructField("game_name", StringType(), True),
        StructField("tag_line", StringType(), True),

        # Personal information
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("nickname", StringType(), True),
        StructField("nationality", StringType(), True),

        # Team information
        StructField("current_team_id", StringType(), True),
        StructField("current_team_name", StringType(), True),
        # Top, Jungle, Mid, ADC, Support
        StructField("role", StringType(), True),

        # Player status
        StructField("active", BooleanType(), True),
        StructField("professional", BooleanType(), True),

        # Career statistics
        StructField("total_games", IntegerType(), True),
        StructField("total_wins", IntegerType(), True),
        StructField("total_losses", IntegerType(), True),
        StructField("win_rate", DoubleType(), True),

        # Performance metrics
        StructField("avg_kills", DoubleType(), True),
        StructField("avg_deaths", DoubleType(), True),
        StructField("avg_assists", DoubleType(), True),
        StructField("kda_ratio", DoubleType(), True),
        StructField("avg_cs_per_min", DoubleType(), True),
        StructField("avg_gold_per_min", DoubleType(), True),

        # Recent performance
        StructField("last_match_date", TimestampType(), True),
        StructField("recent_form", StringType(), True),  # Last 5 games: WWLWL

        # Metadata
        StructField("data_source", StringType(), True),
        StructField("profile_updated_at", TimestampType(), True),
        StructField("ingestion_timestamp", LongType(), True),
    ])


def get_player_performance_schema():
    """
    Returns a schema focused on player performance metrics.
    Use this for real-time player analytics and monitoring.

    Returns:
        StructType: Spark schema for player performance data
    """
    return StructType([
        # Identifiers
        StructField("player_id", StringType(), False),
        StructField("match_id", StringType(), False),
        StructField("game_timestamp", TimestampType(), True),

        # In-game performance
        StructField("champion_played", StringType(), True),
        StructField("position", StringType(), True),
        StructField("kills", IntegerType(), True),
        StructField("deaths", IntegerType(), True),
        StructField("assists", IntegerType(), True),
        StructField("cs", IntegerType(), True),
        StructField("gold_earned", IntegerType(), True),
        StructField("damage_dealt", IntegerType(), True),
        StructField("damage_taken", IntegerType(), True),
        StructField("vision_score", IntegerType(), True),

        # Game context
        StructField("game_duration", IntegerType(), True),
        StructField("team_result", StringType(), True),  # win, loss

        # Calculated metrics
        StructField("kda", DoubleType(), True),
        StructField("cs_per_min", DoubleType(), True),
        StructField("gold_per_min", DoubleType(), True),
        StructField("kill_participation", DoubleType(), True),

        # Metadata
        StructField("ingestion_timestamp", LongType(), True),
    ])
