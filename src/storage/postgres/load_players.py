"""
PostgreSQL Player Analytics Loader.
Loads player analytics from Parquet to PostgreSQL.
"""

from spark.utils.logger import get_logger
from spark.utils.spark_session import get_spark_session
import os
import sys
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp

# Add parent directories to path
sys.path.append(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))


logger = get_logger(__name__)


class PostgresPlayerLoader:
    """Loads player analytics data into PostgreSQL."""

    def __init__(self, spark: Optional[SparkSession] = None):
        """
        Initialize PostgreSQL player loader.

        Args:
            spark: SparkSession instance (optional, will create if None)
        """
        self.spark = spark or get_spark_session("PostgresPlayerLoader")

        # PostgreSQL connection settings
        self.postgres_host = os.getenv("POSTGRES_HOST", "localhost")
        self.postgres_port = os.getenv("POSTGRES_PORT", "5432")
        self.postgres_db = os.getenv("POSTGRES_DATABASE", "esports_analytics")
        self.postgres_user = os.getenv("POSTGRES_USER", "postgres")
        self.postgres_password = os.getenv("POSTGRES_PASSWORD", "postgres")

        # JDBC URL
        self.jdbc_url = f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

        # JDBC properties
        self.jdbc_properties = {
            "user": self.postgres_user,
            "password": self.postgres_password,
            "driver": "org.postgresql.Driver",
            "stringtype": "unspecified"
        }

        # Data paths
        self.analytics_path = os.getenv(
            "DATA_LAKE_PATH", "/data/gold") + "/analytics"

        logger.info(f"PostgresPlayerLoader initialized")
        logger.info(f"JDBC URL: {self.jdbc_url}")
        logger.info(f"Analytics path: {self.analytics_path}")

    def load_player_analytics(self) -> int:
        """
        Load player analytics data from processed Parquet.

        Returns:
            Number of records loaded
        """
        try:
            logger.info("=" * 80)
            logger.info("LOADING PLAYER ANALYTICS TO POSTGRESQL")
            logger.info("=" * 80)

            # Read processed players from Phase 3
            players_path = os.getenv(
                "PROCESSED_DATA_PATH", "/data/silver") + "/players"

            if not os.path.exists(players_path):
                logger.warning(f"Players path not found: {players_path}")
                return 0

            logger.info(f"Reading players from: {players_path}")
            players_df = self.spark.read.parquet(players_path)

            # Select columns to match PostgreSQL schema
            player_analytics_df = players_df.select(
                col("player_id"),
                col("summoner_name"),
                col("current_team_name"),
                col("role"),
                col("status"),
                col("total_games"),
                col("total_wins"),
                col("total_losses"),
                col("win_rate"),
                col("kda_ratio"),
                col("avg_kills"),
                col("avg_deaths"),
                col("avg_assists"),
                col("avg_cs_per_min"),
                col("avg_gold_per_min"),
                col("recent_form"),
                current_timestamp().alias("processed_at")
            )

            record_count = player_analytics_df.count()
            logger.info(f"Total player records to load: {record_count}")

            if record_count == 0:
                logger.warning("No player data to load")
                return 0

            # Show sample
            logger.info("Sample player data:")
            player_analytics_df.show(5, truncate=False)

            # Write to PostgreSQL (truncate and insert)
            logger.info("Writing to PostgreSQL table: player_analytics")
            logger.info("Mode: overwrite (truncate + insert)")

            player_analytics_df.write \
                .jdbc(
                    url=self.jdbc_url,
                    table="player_analytics",
                    mode="overwrite",  # Truncate and insert
                    properties=self.jdbc_properties
                )

            logger.info(
                f"✅ Successfully loaded {record_count} player records to PostgreSQL")
            return record_count

        except Exception as e:
            logger.error(f"❌ Failed to load player analytics: {e}")
            raise

    def load_team_rankings(self) -> int:
        """
        Load team rankings from analytics.

        Returns:
            Number of records loaded
        """
        try:
            logger.info("=" * 80)
            logger.info("LOADING TEAM RANKINGS TO POSTGRESQL")
            logger.info("=" * 80)

            # Read team rankings analytics
            team_rankings_path = os.path.join(
                self.analytics_path, "rankings", "team_rankings")

            if not os.path.exists(team_rankings_path):
                logger.warning(
                    f"Team rankings path not found: {team_rankings_path}")
                return 0

            logger.info(f"Reading team rankings from: {team_rankings_path}")
            team_rankings_df = self.spark.read.parquet(team_rankings_path)

            # Add processed_at timestamp
            team_rankings_df = team_rankings_df.withColumn(
                "processed_at", current_timestamp())

            record_count = team_rankings_df.count()
            logger.info(f"Total team ranking records to load: {record_count}")

            if record_count == 0:
                logger.warning("No team rankings data to load")
                return 0

            # Show sample
            logger.info("Sample team rankings data:")
            team_rankings_df.show(5, truncate=False)

            # Write to PostgreSQL
            logger.info("Writing to PostgreSQL table: team_rankings")
            logger.info("Mode: overwrite")

            team_rankings_df.write \
                .jdbc(
                    url=self.jdbc_url,
                    table="team_rankings",
                    mode="overwrite",
                    properties=self.jdbc_properties
                )

            logger.info(
                f"✅ Successfully loaded {record_count} team ranking records to PostgreSQL")
            return record_count

        except Exception as e:
            logger.error(f"❌ Failed to load team rankings: {e}")
            raise

    def load_player_role_stats(self) -> int:
        """
        Load player role statistics from analytics.

        Returns:
            Number of records loaded
        """
        try:
            logger.info("=" * 80)
            logger.info("LOADING PLAYER ROLE STATS TO POSTGRESQL")
            logger.info("=" * 80)

            # Read role performance analytics
            role_stats_path = os.path.join(
                self.analytics_path, "players", "role_performance")

            if not os.path.exists(role_stats_path):
                logger.warning(f"Role stats path not found: {role_stats_path}")
                return 0

            logger.info(f"Reading role stats from: {role_stats_path}")
            role_stats_df = self.spark.read.parquet(role_stats_path)

            # Add processed_at timestamp
            role_stats_df = role_stats_df.withColumn(
                "processed_at", current_timestamp())

            record_count = role_stats_df.count()
            logger.info(f"Total role stats records to load: {record_count}")

            if record_count == 0:
                logger.warning("No role stats data to load")
                return 0

            # Show sample
            logger.info("Sample role stats data:")
            role_stats_df.show(5, truncate=False)

            # Write to PostgreSQL
            logger.info("Writing to PostgreSQL table: player_role_stats")
            logger.info("Mode: overwrite")

            role_stats_df.write \
                .jdbc(
                    url=self.jdbc_url,
                    table="player_role_stats",
                    mode="overwrite",
                    properties=self.jdbc_properties
                )

            logger.info(
                f"✅ Successfully loaded {record_count} role stats records to PostgreSQL")
            return record_count

        except Exception as e:
            logger.error(f"❌ Failed to load role stats: {e}")
            raise

    def load_all_player_data(self) -> dict:
        """
        Load all player-related data to PostgreSQL.

        Returns:
            Dictionary with load statistics
        """
        stats = {
            "player_analytics": 0,
            "team_rankings": 0,
            "player_role_stats": 0,
            "total": 0
        }

        try:
            # Load player analytics
            stats["player_analytics"] = self.load_player_analytics()

            # Load team rankings
            stats["team_rankings"] = self.load_team_rankings()

            # Load role stats
            stats["player_role_stats"] = self.load_player_role_stats()

            # Calculate total
            stats["total"] = (
                stats["player_analytics"] +
                stats["team_rankings"] +
                stats["player_role_stats"]
            )

            logger.info("\n" + "=" * 80)
            logger.info("PLAYER DATA LOAD SUMMARY")
            logger.info("=" * 80)
            logger.info(
                f"Player analytics: {stats['player_analytics']:,} records")
            logger.info(f"Team rankings: {stats['team_rankings']:,} records")
            logger.info(
                f"Role statistics: {stats['player_role_stats']:,} records")
            logger.info(f"Total loaded: {stats['total']:,} records")
            logger.info("=" * 80)

            return stats

        except Exception as e:
            logger.error(f"Failed to load player data: {e}")
            raise


def main():
    """Main execution function."""
    try:
        loader = PostgresPlayerLoader()
        stats = loader.load_all_player_data()

        logger.info(f"\n✅ Player data load completed successfully")
        logger.info(f"Total records loaded: {stats['total']:,}")

    except Exception as e:
        logger.error(f"❌ Player data load failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if 'loader' in locals() and loader.spark:
            loader.spark.stop()
            logger.info("SparkSession stopped")


if __name__ == "__main__":
    main()
