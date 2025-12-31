"""
Schema definition for match data from Kafka topics.
Defines the structure of incoming esports match events.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    BooleanType,
    ArrayType,
    TimestampType
)


def get_match_schema():
    """
    Returns the schema for match data streaming from Kafka.

    Schema includes:
    - Match metadata (ID, game, tournament)
    - Team information
    - Match results and statistics
    - Timing information

    Returns:
        StructType: Spark schema for match data
    """
    return StructType([
        # Match identifiers
        StructField("match_id", StringType(), False),
        StructField("game_id", StringType(), True),
        StructField("platform_game_id", StringType(), True),

        # Tournament and metadata
        StructField("tournament_id", StringType(), True),
        StructField("tournament_name", StringType(), True),
        StructField("game_version", StringType(), True),
        StructField("patch", StringType(), True),

        # Team information
        StructField("team_1_id", StringType(), True),
        StructField("team_1_name", StringType(), True),
        StructField("team_2_id", StringType(), True),
        StructField("team_2_name", StringType(), True),

        # Match results
        StructField("winner_id", StringType(), True),
        StructField("winner_name", StringType(), True),
        StructField("match_duration", IntegerType(),
                    True),  # Duration in seconds

        # Match statistics
        StructField("team_1_kills", IntegerType(), True),
        StructField("team_2_kills", IntegerType(), True),
        StructField("team_1_gold", IntegerType(), True),
        StructField("team_2_gold", IntegerType(), True),
        StructField("team_1_towers", IntegerType(), True),
        StructField("team_2_towers", IntegerType(), True),

        # Status and timing
        StructField("status", StringType(), True),  # finished, live, scheduled
        StructField("started_at", TimestampType(), True),
        StructField("finished_at", TimestampType(), True),

        # Additional data
        StructField("match_type", StringType(), True),  # best_of, regular
        StructField("game_number", IntegerType(),
                    True),  # Game number in series

        # Metadata
        StructField("data_source", StringType(), True),
        StructField("ingestion_timestamp", LongType(), True),
    ])


def get_match_nested_schema():
    """
    Returns a more complex schema with nested structures for detailed match data.
    Use this for advanced match analytics with player-level details.

    Returns:
        StructType: Nested Spark schema for detailed match data
    """
    player_stats_schema = StructType([
        StructField("player_id", StringType(), True),
        StructField("champion", StringType(), True),
        StructField("kills", IntegerType(), True),
        StructField("deaths", IntegerType(), True),
        StructField("assists", IntegerType(), True),
        StructField("cs", IntegerType(), True),  # Creep score
        StructField("gold", IntegerType(), True),
        StructField("damage", IntegerType(), True),
    ])

    return StructType([
        StructField("match_id", StringType(), False),
        StructField("tournament_name", StringType(), True),
        StructField("team_1_name", StringType(), True),
        StructField("team_2_name", StringType(), True),
        StructField("winner_name", StringType(), True),
        StructField("duration", IntegerType(), True),
        StructField("team_1_players", ArrayType(player_stats_schema), True),
        StructField("team_2_players", ArrayType(player_stats_schema), True),
        StructField("timestamp", TimestampType(), True),
    ])
