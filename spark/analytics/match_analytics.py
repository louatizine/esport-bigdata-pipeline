"""
Match analytics module.
Processes match data and generates match-level aggregations.
"""

from utils.logger import get_logger
from utils.spark_session import create_spark_session
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum as spark_sum, max as spark_max, min as spark_min

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


logger = get_logger(__name__)


class MatchAnalytics:
    """Match analytics processor."""

    def __init__(self, spark: SparkSession = None):
        """
        Initialize match analytics.

        Args:
            spark: SparkSession instance (optional, will create if None)
        """
        self.spark = spark or create_spark_session("MatchAnalytics")
        self.processed_data_path = os.getenv(
            "PROCESSED_DATA_PATH", "/data/silver")
        self.analytics_output_path = os.getenv(
            "DATA_LAKE_PATH", "/data/gold") + "/analytics"
        self.sql_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "sql"
        )

    def load_match_data(self):
        """
        Load match data from Parquet.

        Returns:
            DataFrame: Match data
        """
        try:
            matches_path = f"{self.processed_data_path}/matches"
            logger.info(f"Loading match data from: {matches_path}")

            df = self.spark.read.parquet(matches_path)

            # Add computed columns if needed
            df = df.withColumn(
                "duration_minutes",
                col("match_duration") / 60
            )

            logger.info(f"Loaded {df.count()} match records")
            return df

        except Exception as e:
            logger.error(f"Failed to load match data: {e}")
            raise

    def execute_sql_file(self, sql_file: str):
        """
        Execute SQL queries from a file.

        Args:
            sql_file: Name of SQL file (e.g., 'match_metrics.sql')
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

    def save_view_to_parquet(self, view_name: str, output_subdir: str = "matches"):
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

    def run_match_analytics(self):
        """
        Run all match analytics.
        """
        try:
            logger.info("Starting match analytics job")

            # Load match data
            matches_df = self.load_match_data()

            # Register as temp view
            matches_df.createOrReplaceTempView("matches")
            logger.info("Registered 'matches' temp view")

            # Execute SQL analytics
            self.execute_sql_file("match_metrics.sql")

            # Save all generated views
            views_to_save = [
                "matches_by_status",
                "avg_duration_by_status",
                "team_performance",
                "tournament_stats",
                "daily_match_volume",
                "kill_statistics",
                "gold_efficiency"
            ]

            for view_name in views_to_save:
                self.save_view_to_parquet(view_name, output_subdir="matches")

            logger.info("Match analytics job completed successfully")

        except Exception as e:
            logger.error(f"Match analytics job failed: {e}")
            raise

    def get_summary_stats(self):
        """
        Get summary statistics for match analytics.

        Returns:
            dict: Summary statistics
        """
        try:
            matches_df = self.spark.table("matches")

            stats = {
                "total_matches": matches_df.count(),
                "unique_tournaments": matches_df.select("tournament_name").distinct().count(),
                "unique_teams": matches_df.select("team_1_name").union(
                    matches_df.select("team_2_name")
                ).distinct().count(),
                "avg_duration_seconds": matches_df.agg(
                    avg("match_duration")
                ).collect()[0][0]
            }

            logger.info(f"Summary stats: {stats}")
            return stats

        except Exception as e:
            logger.error(f"Failed to get summary stats: {e}")
            return {}


def main():
    """Main entry point for match analytics."""
    try:
        analytics = MatchAnalytics()
        analytics.run_match_analytics()

        # Print summary
        summary = analytics.get_summary_stats()
        logger.info(f"Analytics Summary: {summary}")

    except Exception as e:
        logger.error(f"Match analytics failed: {e}")
        raise
    finally:
        # Cleanup
        if 'analytics' in locals() and analytics.spark:
            analytics.spark.stop()


if __name__ == "__main__":
    main()
