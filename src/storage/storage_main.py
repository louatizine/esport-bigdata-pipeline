"""
Storage Main Orchestrator.
Coordinates loading of analytics data to PostgreSQL and MongoDB.
"""

from spark.utils.logger import get_logger
from spark.utils.spark_session import get_spark_session
from storage.mongodb.load_documents import MongoDocumentLoader
from storage.postgres.load_players import PostgresPlayerLoader
from storage.postgres.load_matches import PostgresMatchLoader
import os
import sys
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession

# Add parent directories to path
sys.path.append(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))


logger = get_logger(__name__)


class StorageOrchestrator:
    """Orchestrates all storage operations."""

    def __init__(self, spark: Optional[SparkSession] = None):
        """
        Initialize storage orchestrator.

        Args:
            spark: SparkSession instance (optional, will create if None)
        """
        self.spark = spark or get_spark_session("StorageOrchestrator")
        self.start_time = None
        self.end_time = None

    def load_to_postgresql(self) -> dict:
        """
        Load all data to PostgreSQL.

        Returns:
            Dictionary with load statistics
        """
        logger.info("\n" + "=" * 80)
        logger.info("POSTGRESQL LOAD STARTED")
        logger.info("=" * 80)

        stats = {
            "matches": {},
            "players": {},
            "total_records": 0
        }

        try:
            # Load match data
            logger.info("\n[1/2] Loading match data...")
            match_loader = PostgresMatchLoader(spark=self.spark)
            stats["matches"] = match_loader.load_all_match_data()

            # Load player data
            logger.info("\n[2/2] Loading player data...")
            player_loader = PostgresPlayerLoader(spark=self.spark)
            stats["players"] = player_loader.load_all_player_data()

            # Calculate total
            stats["total_records"] = (
                stats["matches"].get("total", 0) +
                stats["players"].get("total", 0)
            )

            logger.info("\n" + "=" * 80)
            logger.info("POSTGRESQL LOAD COMPLETED")
            logger.info("=" * 80)
            logger.info(f"Match records: {stats['matches'].get('total', 0):,}")
            logger.info(
                f"Player records: {stats['players'].get('total', 0):,}")
            logger.info(f"Total records: {stats['total_records']:,}")
            logger.info("=" * 80)

            return stats

        except Exception as e:
            logger.error(f"PostgreSQL load failed: {e}")
            raise

    def load_to_mongodb(self) -> dict:
        """
        Load selected data to MongoDB.

        Returns:
            Dictionary with load statistics
        """
        logger.info("\n" + "=" * 80)
        logger.info("MONGODB LOAD STARTED")
        logger.info("=" * 80)

        try:
            mongo_loader = MongoDocumentLoader(spark=self.spark)
            stats = mongo_loader.load_all_documents()
            mongo_loader.close()

            logger.info("\n" + "=" * 80)
            logger.info("MONGODB LOAD COMPLETED")
            logger.info("=" * 80)
            logger.info(f"Total documents: {stats.get('total', 0):,}")
            logger.info("=" * 80)

            return stats

        except Exception as e:
            logger.error(f"MongoDB load failed: {e}")
            # Don't raise - MongoDB is optional
            logger.warning("Continuing without MongoDB...")
            return {"total": 0}

    def run_all_storage_loads(self, skip_mongodb: bool = False) -> dict:
        """
        Run all storage load operations.

        Args:
            skip_mongodb: Skip MongoDB load if True

        Returns:
            Dictionary with overall statistics
        """
        try:
            self.start_time = datetime.now()
            logger.info("\n" + "=" * 80)
            logger.info("STORAGE ORCHESTRATOR STARTED")
            logger.info(f"Start time: {self.start_time}")
            logger.info("=" * 80)

            overall_stats = {
                "postgresql": {},
                "mongodb": {},
                "start_time": self.start_time,
                "end_time": None,
                "duration_seconds": 0
            }

            # Load to PostgreSQL
            overall_stats["postgresql"] = self.load_to_postgresql()

            # Load to MongoDB (optional)
            if not skip_mongodb:
                overall_stats["mongodb"] = self.load_to_mongodb()
            else:
                logger.info("\nSkipping MongoDB load (--skip-mongodb flag)")
                overall_stats["mongodb"] = {"total": 0}

            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()
            overall_stats["end_time"] = self.end_time
            overall_stats["duration_seconds"] = duration

            # Final summary
            logger.info("\n" + "=" * 80)
            logger.info("STORAGE ORCHESTRATOR COMPLETED SUCCESSFULLY")
            logger.info("=" * 80)
            logger.info(
                f"PostgreSQL records: {overall_stats['postgresql'].get('total_records', 0):,}")
            logger.info(
                f"MongoDB documents: {overall_stats['mongodb'].get('total', 0):,}")
            logger.info(f"Total duration: {duration:.2f} seconds")
            logger.info("=" * 80)

            return overall_stats

        except Exception as e:
            logger.error(f"Storage orchestrator failed: {e}")
            raise
        finally:
            if self.spark:
                self.spark.stop()
                logger.info("SparkSession stopped")


def main():
    """Main execution function."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Storage Orchestrator - Load analytics to PostgreSQL and MongoDB")
    parser.add_argument(
        "--target",
        choices=["all", "postgres", "mongodb"],
        default="all",
        help="Target storage system (default: all)"
    )
    parser.add_argument(
        "--skip-mongodb",
        action="store_true",
        help="Skip MongoDB load"
    )

    args = parser.parse_args()

    try:
        orchestrator = StorageOrchestrator()

        if args.target == "all":
            stats = orchestrator.run_all_storage_loads(
                skip_mongodb=args.skip_mongodb)
            logger.info(f"\n✅ All storage loads completed successfully")

        elif args.target == "postgres":
            stats = orchestrator.load_to_postgresql()
            logger.info(f"\n✅ PostgreSQL load completed successfully")
            logger.info(f"Total records: {stats.get('total_records', 0):,}")

        elif args.target == "mongodb":
            stats = orchestrator.load_to_mongodb()
            logger.info(f"\n✅ MongoDB load completed successfully")
            logger.info(f"Total documents: {stats.get('total', 0):,}")

        sys.exit(0)

    except Exception as e:
        logger.error(f"❌ Storage load failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
