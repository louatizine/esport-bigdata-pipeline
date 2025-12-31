"""
Player analytics module.
Processes player data and generates player-level aggregations.
"""

from utils.logger import get_logger
from utils.spark_session import create_spark_session
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, sum as spark_sum

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


logger = get_logger(__name__)


class PlayerAnalytics:
    """Player analytics processor."""

    def __init__(self, spark: SparkSession = None):
        """
        Initialize player analytics.

        Args:
            spark: SparkSession instance (optional, will create if None)
        """
        self.spark = spark or create_spark_session("PlayerAnalytics")
        self.processed_data_path = os.getenv(
            "PROCESSED_DATA_PATH", "/data/silver")
        self.analytics_output_path = os.getenv(
            "DATA_LAKE_PATH", "/data/gold") + "/analytics"
        self.sql_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "sql"
        )

    def load_player_data(self):
        """
        Load player data from Parquet.

        Returns:
            DataFrame: Player data
        """
        try:
            players_path = f"{self.processed_data_path}/players"
            logger.info(f"Loading player data from: {players_path}")

            df = self.spark.read.parquet(players_path)

            logger.info(f"Loaded {df.count()} player records")
            return df

        except Exception as e:
            logger.error(f"Failed to load player data: {e}")
            raise

    def execute_sql_file(self, sql_file: str):
        """
        Execute SQL queries from a file.

        Args:
            sql_file: Name of SQL file (e.g., 'player_metrics.sql')
        """
        try:
            sql_file_path = os.path.join(self.sql_path, sql_file)
            logger.info(f"Executing SQL file: {sql_file_path}")

            with open(sql_file_path, 'r') as f:
                sql_content = f.read()

            # Split by semicolon and execute each statement
            statements = [s.strip()
                          for s in sql_content.split(';') if s.strip()]

            for i, statement in enumerate(statements, 1):
                if statement.upper().startswith(('CREATE', 'SELECT', 'WITH')):
                    logger.debug(f"Executing statement {i}/{len(statements)}")
                    self.spark.sql(statement)

            logger.info(
                f"Successfully executed {len(statements)} SQL statements")

        except Exception as e:
            logger.error(f"Failed to execute SQL file {sql_file}: {e}")
            raise

    def save_view_to_parquet(self, view_name: str, output_subdir: str = "players"):
        """
        Save a temp view to Parquet.

        Args:
            view_name: Name of the temp view
            output_subdir: Subdirectory under analytics output path
        """
        try:
            output_path = f"{self.analytics_output_path}/{output_subdir}/{view_name}"
            logger.info(f"Saving view '{view_name}' to: {output_path}")

            df = self.spark.table(view_name)
            record_count = df.count()

            df.write \
                .mode("overwrite") \
                .parquet(output_path)

            logger.info(f"Saved {record_count} records to {output_path}")

        except Exception as e:
            logger.error(f"Failed to save view '{view_name}': {e}")
            raise

    def run_player_analytics(self):
        """
        Run all player analytics.
        """
        try:
            logger.info("Starting player analytics job")

            # Load player data
            players_df = self.load_player_data()

            # Register as temp view
            players_df.createOrReplaceTempView("players")
            logger.info("Registered 'players' temp view")

            # Execute SQL analytics
            self.execute_sql_file("player_metrics.sql")

            # Save all generated views
            views_to_save = [
                "player_win_rate",
                "player_kda_metrics",
                "player_efficiency",
                "active_players_by_role",
                "team_composition",
                "high_performers",
                "player_activity",
                "role_performance",
                "nationality_stats"
            ]

            for view_name in views_to_save:
                self.save_view_to_parquet(view_name, output_subdir="players")

            logger.info("Player analytics job completed successfully")

        except Exception as e:
            logger.error(f"Player analytics job failed: {e}")
            raise

    def get_summary_stats(self):
        """
        Get summary statistics for player analytics.

        Returns:
            dict: Summary statistics
        """
        try:
            players_df = self.spark.table("players")

            stats = {
                "total_players": players_df.count(),
                "active_players": players_df.filter(col("active") == True).count(),
                "avg_win_rate": players_df.agg(avg("win_rate")).collect()[0][0],
                "avg_kda": players_df.agg(avg("kda_ratio")).collect()[0][0],
                "unique_teams": players_df.select("current_team_name").distinct().count()
            }

            logger.info(f"Summary stats: {stats}")
            return stats

        except Exception as e:
            logger.error(f"Failed to get summary stats: {e}")
            return {}


def main():
    """Main entry point for player analytics."""
    try:
        analytics = PlayerAnalytics()
        analytics.run_player_analytics()

        # Print summary
        summary = analytics.get_summary_stats()
        logger.info(f"Analytics Summary: {summary}")

    except Exception as e:
        logger.error(f"Player analytics failed: {e}")
        raise
    finally:
        # Cleanup
        if 'analytics' in locals() and analytics.spark:
            analytics.spark.stop()


if __name__ == "__main__":
    main()
