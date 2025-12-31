"""
PostgreSQL Match Analytics Loader.
Loads match analytics from Parquet to PostgreSQL.
"""

import os
import sys
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp

# Add parent directories to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

from spark.utils.spark_session import get_spark_session
from spark.utils.logger import get_logger


logger = get_logger(__name__)


class PostgresMatchLoader:
    """Loads match analytics data into PostgreSQL."""

    def __init__(self, spark: Optional[SparkSession] = None):
        """
        Initialize PostgreSQL match loader.

        Args:
            spark: SparkSession instance (optional, will create if None)
        """
        self.spark = spark or get_spark_session("PostgresMatchLoader")
        
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
        self.analytics_path = os.getenv("DATA_LAKE_PATH", "/data/gold") + "/analytics"
        
        logger.info(f"PostgresMatchLoader initialized")
        logger.info(f"JDBC URL: {self.jdbc_url}")
        logger.info(f"Analytics path: {self.analytics_path}")

    def load_matches_analytics(self) -> int:
        """
        Load matches analytics data from processed Parquet.
        
        Returns:
            Number of records loaded
        """
        try:
            logger.info("=" * 80)
            logger.info("LOADING MATCHES ANALYTICS TO POSTGRESQL")
            logger.info("=" * 80)
            
            # Read processed matches from Phase 3
            matches_path = os.getenv("PROCESSED_DATA_PATH", "/data/silver") + "/matches"
            
            if not os.path.exists(matches_path):
                logger.warning(f"Matches path not found: {matches_path}")
                return 0
            
            logger.info(f"Reading matches from: {matches_path}")
            matches_df = self.spark.read.parquet(matches_path)
            
            # Select and rename columns to match PostgreSQL schema
            matches_analytics_df = matches_df.select(
                col("match_id"),
                col("tournament_name"),
                col("status"),
                col("started_at"),
                col("ended_at"),
                col("match_duration"),
                col("duration_minutes"),
                col("team_1_name"),
                col("team_1_kills"),
                col("team_1_deaths"),
                col("team_1_assists"),
                col("team_1_gold"),
                col("team_1_towers"),
                col("team_2_name"),
                col("team_2_kills"),
                col("team_2_deaths"),
                col("team_2_assists"),
                col("team_2_gold"),
                col("team_2_towers"),
                col("winner_name"),
                current_timestamp().alias("processed_at")
            )
            
            record_count = matches_analytics_df.count()
            logger.info(f"Total match records to load: {record_count}")
            
            if record_count == 0:
                logger.warning("No match data to load")
                return 0
            
            # Show sample
            logger.info("Sample match data:")
            matches_analytics_df.show(5, truncate=False)
            
            # Write to PostgreSQL (truncate and insert)
            logger.info("Writing to PostgreSQL table: matches_analytics")
            logger.info("Mode: overwrite (truncate + insert)")
            
            matches_analytics_df.write \
                .jdbc(
                    url=self.jdbc_url,
                    table="matches_analytics",
                    mode="overwrite",  # Truncate and insert
                    properties=self.jdbc_properties
                )
            
            logger.info(f"✅ Successfully loaded {record_count} match records to PostgreSQL")
            return record_count
            
        except Exception as e:
            logger.error(f"❌ Failed to load matches analytics: {e}")
            raise

    def load_match_team_performance(self) -> int:
        """
        Load aggregated team performance from match analytics.
        
        Returns:
            Number of records loaded
        """
        try:
            logger.info("=" * 80)
            logger.info("LOADING MATCH TEAM PERFORMANCE TO POSTGRESQL")
            logger.info("=" * 80)
            
            # Read team performance analytics
            team_perf_path = os.path.join(self.analytics_path, "matches", "team_performance")
            
            if not os.path.exists(team_perf_path):
                logger.warning(f"Team performance path not found: {team_perf_path}")
                return 0
            
            logger.info(f"Reading team performance from: {team_perf_path}")
            team_perf_df = self.spark.read.parquet(team_perf_path)
            
            # Add processed_at timestamp
            team_perf_df = team_perf_df.withColumn("processed_at", current_timestamp())
            
            record_count = team_perf_df.count()
            logger.info(f"Total team performance records to load: {record_count}")
            
            if record_count == 0:
                logger.warning("No team performance data to load")
                return 0
            
            # Show sample
            logger.info("Sample team performance data:")
            team_perf_df.show(5, truncate=False)
            
            # Write to PostgreSQL
            logger.info("Writing to PostgreSQL table: match_team_performance")
            logger.info("Mode: overwrite")
            
            team_perf_df.write \
                .jdbc(
                    url=self.jdbc_url,
                    table="match_team_performance",
                    mode="overwrite",
                    properties=self.jdbc_properties
                )
            
            logger.info(f"✅ Successfully loaded {record_count} team performance records to PostgreSQL")
            return record_count
            
        except Exception as e:
            logger.error(f"❌ Failed to load team performance: {e}")
            raise

    def load_all_match_data(self) -> dict:
        """
        Load all match-related data to PostgreSQL.
        
        Returns:
            Dictionary with load statistics
        """
        stats = {
            "matches_analytics": 0,
            "match_team_performance": 0,
            "total": 0
        }
        
        try:
            # Load matches analytics
            stats["matches_analytics"] = self.load_matches_analytics()
            
            # Load team performance
            stats["match_team_performance"] = self.load_match_team_performance()
            
            # Calculate total
            stats["total"] = stats["matches_analytics"] + stats["match_team_performance"]
            
            logger.info("\n" + "=" * 80)
            logger.info("MATCH DATA LOAD SUMMARY")
            logger.info("=" * 80)
            logger.info(f"Matches analytics: {stats['matches_analytics']:,} records")
            logger.info(f"Team performance: {stats['match_team_performance']:,} records")
            logger.info(f"Total loaded: {stats['total']:,} records")
            logger.info("=" * 80)
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to load match data: {e}")
            raise


def main():
    """Main execution function."""
    try:
        loader = PostgresMatchLoader()
        stats = loader.load_all_match_data()
        
        logger.info(f"\n✅ Match data load completed successfully")
        logger.info(f"Total records loaded: {stats['total']:,}")
        
    except Exception as e:
        logger.error(f"❌ Match data load failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if 'loader' in locals() and loader.spark:
            loader.spark.stop()
            logger.info("SparkSession stopped")


if __name__ == "__main__":
    main()
