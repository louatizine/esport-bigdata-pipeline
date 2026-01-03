"""
MongoDB Document Loader.
Loads analytics data to MongoDB for flexible document storage.
"""

from spark.utils.logger import get_logger
from spark.utils.spark_session import get_spark_session
import os
import sys
from typing import Optional, Dict, List
from datetime import datetime

import pymongo
from pymongo import MongoClient, UpdateOne
from pyspark.sql import SparkSession

# Add parent directories to path
sys.path.append(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))


logger = get_logger(__name__)


class MongoDocumentLoader:
    """Loads analytics data into MongoDB as documents."""

    def __init__(self, spark: Optional[SparkSession] = None):
        """
        Initialize MongoDB document loader.

        Args:
            spark: SparkSession instance (optional, will create if None)
        """
        self.spark = spark or get_spark_session("MongoDocumentLoader")

        # MongoDB connection settings - support both Atlas and local
        # Priority: MONGODB_ATLAS_URI > individual settings > defaults
        atlas_uri = os.getenv("MONGODB_ATLAS_URI", "")

        if atlas_uri:
            # Use MongoDB Atlas connection string
            self.mongo_uri = atlas_uri
            logger.info("Using MongoDB Atlas connection")
        else:
            # Build connection URI from individual settings (local MongoDB)
            self.mongo_host = os.getenv("MONGO_HOST", "localhost")
            self.mongo_port = int(os.getenv("MONGO_PORT", "27017"))
            self.mongo_user = os.getenv("MONGO_USER", "")
            self.mongo_password = os.getenv("MONGO_PASSWORD", "")

            if self.mongo_user and self.mongo_password:
                self.mongo_uri = f"mongodb://{self.mongo_user}:{self.mongo_password}@{self.mongo_host}:{self.mongo_port}"
            else:
                self.mongo_uri = f"mongodb://{self.mongo_host}:{self.mongo_port}"
            logger.info("Using local MongoDB connection")

        self.mongo_db = os.getenv("MONGODB_DATABASE", "esports_analytics")

        # Data paths
        self.analytics_path = os.getenv(
            "DATA_LAKE_PATH", "/data/gold") + "/analytics"

        logger.info(f"MongoDocumentLoader initialized")
        logger.info(f"MongoDB URI: {self.mongo_uri}")
        logger.info(f"Database: {self.mongo_db}")
        logger.info(f"Analytics path: {self.analytics_path}")

        # Initialize MongoDB client
        try:
            self.client = MongoClient(
                self.mongo_uri,
                serverSelectionTimeoutMS=5000
            )
            # Test connection
            self.client.server_info()
            logger.info("✅ MongoDB connection successful")
        except Exception as e:
            logger.error(f"❌ MongoDB connection failed: {e}")
            logger.warning("MongoDB operations will be skipped")
            self.client = None

    def load_top_players_by_kda(self) -> int:
        """
        Load top players by KDA to MongoDB.

        Returns:
            Number of documents loaded
        """
        if not self.client:
            logger.warning("MongoDB client not available, skipping load")
            return 0

        try:
            logger.info("=" * 80)
            logger.info("LOADING TOP PLAYERS BY KDA TO MONGODB")
            logger.info("=" * 80)

            # Read top players analytics
            top_players_path = os.path.join(
                self.analytics_path, "rankings", "top_players_by_kda")

            if not os.path.exists(top_players_path):
                logger.warning(
                    f"Top players path not found: {top_players_path}")
                return 0

            logger.info(f"Reading from: {top_players_path}")
            top_players_df = self.spark.read.parquet(top_players_path)

            # Convert to list of dictionaries
            top_players_list = top_players_df.toPandas().to_dict('records')

            if not top_players_list:
                logger.warning("No top players data to load")
                return 0

            # Add metadata
            for doc in top_players_list:
                doc["loaded_at"] = datetime.utcnow()
                doc["source"] = "spark_analytics"

            # Get collection
            db = self.client[self.mongo_db]
            collection = db["top_players_by_kda"]

            # Create indexes
            collection.create_index([("rank", pymongo.ASCENDING)])
            collection.create_index(
                [("player_id", pymongo.ASCENDING)], unique=True)

            # Delete existing documents and insert new ones
            logger.info("Clearing existing documents...")
            collection.delete_many({})

            logger.info(f"Inserting {len(top_players_list)} documents...")
            result = collection.insert_many(top_players_list)

            logger.info(
                f"✅ Successfully loaded {len(result.inserted_ids)} documents to MongoDB")
            return len(result.inserted_ids)

        except Exception as e:
            logger.error(f"❌ Failed to load top players: {e}")
            raise

    def load_player_rankings_by_role(self) -> int:
        """
        Load player rankings by role to MongoDB.

        Returns:
            Number of documents loaded
        """
        if not self.client:
            logger.warning("MongoDB client not available, skipping load")
            return 0

        try:
            logger.info("=" * 80)
            logger.info("LOADING PLAYER RANKINGS BY ROLE TO MONGODB")
            logger.info("=" * 80)

            # Read rankings by role
            rankings_path = os.path.join(
                self.analytics_path, "rankings", "top_players_by_role")

            if not os.path.exists(rankings_path):
                logger.warning(
                    f"Rankings by role path not found: {rankings_path}")
                return 0

            logger.info(f"Reading from: {rankings_path}")
            rankings_df = self.spark.read.parquet(rankings_path)

            # Convert to list of dictionaries
            rankings_list = rankings_df.toPandas().to_dict('records')

            if not rankings_list:
                logger.warning("No rankings data to load")
                return 0

            # Add metadata
            for doc in rankings_list:
                doc["loaded_at"] = datetime.utcnow()
                doc["source"] = "spark_analytics"

            # Get collection
            db = self.client[self.mongo_db]
            collection = db["player_rankings_by_role"]

            # Create indexes
            collection.create_index([("role", pymongo.ASCENDING)])
            collection.create_index([("role_rank", pymongo.ASCENDING)])
            collection.create_index([
                ("player_id", pymongo.ASCENDING),
                ("role", pymongo.ASCENDING)
            ], unique=True)

            # Delete existing documents and insert new ones
            logger.info("Clearing existing documents...")
            collection.delete_many({})

            logger.info(f"Inserting {len(rankings_list)} documents...")
            result = collection.insert_many(rankings_list)

            logger.info(
                f"✅ Successfully loaded {len(result.inserted_ids)} documents to MongoDB")
            return len(result.inserted_ids)

        except Exception as e:
            logger.error(f"❌ Failed to load rankings by role: {e}")
            raise

    def load_composite_rankings(self) -> int:
        """
        Load composite player rankings to MongoDB.

        Returns:
            Number of documents loaded
        """
        if not self.client:
            logger.warning("MongoDB client not available, skipping load")
            return 0

        try:
            logger.info("=" * 80)
            logger.info("LOADING COMPOSITE RANKINGS TO MONGODB")
            logger.info("=" * 80)

            # Read composite rankings
            composite_path = os.path.join(
                self.analytics_path, "rankings", "composite_player_rankings")

            if not os.path.exists(composite_path):
                logger.warning(
                    f"Composite rankings path not found: {composite_path}")
                return 0

            logger.info(f"Reading from: {composite_path}")
            composite_df = self.spark.read.parquet(composite_path)

            # Convert to list of dictionaries
            composite_list = composite_df.toPandas().to_dict('records')

            if not composite_list:
                logger.warning("No composite rankings data to load")
                return 0

            # Add metadata
            for doc in composite_list:
                doc["loaded_at"] = datetime.utcnow()
                doc["source"] = "spark_analytics"

            # Get collection
            db = self.client[self.mongo_db]
            collection = db["composite_player_rankings"]

            # Create indexes
            collection.create_index([("overall_rank", pymongo.ASCENDING)])
            collection.create_index([("composite_score", pymongo.DESCENDING)])
            collection.create_index(
                [("player_id", pymongo.ASCENDING)], unique=True)

            # Delete existing documents and insert new ones
            logger.info("Clearing existing documents...")
            collection.delete_many({})

            logger.info(f"Inserting {len(composite_list)} documents...")
            result = collection.insert_many(composite_list)

            logger.info(
                f"✅ Successfully loaded {len(result.inserted_ids)} documents to MongoDB")
            return len(result.inserted_ids)

        except Exception as e:
            logger.error(f"❌ Failed to load composite rankings: {e}")
            raise

    def load_all_documents(self) -> dict:
        """
        Load all analytics documents to MongoDB.

        Returns:
            Dictionary with load statistics
        """
        stats = {
            "top_players_by_kda": 0,
            "player_rankings_by_role": 0,
            "composite_rankings": 0,
            "total": 0
        }

        if not self.client:
            logger.warning("MongoDB client not available, skipping all loads")
            return stats

        try:
            # Load top players by KDA
            stats["top_players_by_kda"] = self.load_top_players_by_kda()

            # Load rankings by role
            stats["player_rankings_by_role"] = self.load_player_rankings_by_role()

            # Load composite rankings
            stats["composite_rankings"] = self.load_composite_rankings()

            # Calculate total
            stats["total"] = (
                stats["top_players_by_kda"] +
                stats["player_rankings_by_role"] +
                stats["composite_rankings"]
            )

            logger.info("\n" + "=" * 80)
            logger.info("MONGODB LOAD SUMMARY")
            logger.info("=" * 80)
            logger.info(
                f"Top players by KDA: {stats['top_players_by_kda']:,} documents")
            logger.info(
                f"Rankings by role: {stats['player_rankings_by_role']:,} documents")
            logger.info(
                f"Composite rankings: {stats['composite_rankings']:,} documents")
            logger.info(f"Total loaded: {stats['total']:,} documents")
            logger.info("=" * 80)

            return stats

        except Exception as e:
            logger.error(f"Failed to load documents to MongoDB: {e}")
            raise

    def close(self):
        """Close MongoDB connection."""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")


def main():
    """Main execution function."""
    loader = None
    try:
        loader = MongoDocumentLoader()
        stats = loader.load_all_documents()

        logger.info(f"\n✅ MongoDB load completed successfully")
        logger.info(f"Total documents loaded: {stats['total']:,}")

    except Exception as e:
        logger.error(f"❌ MongoDB load failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if loader:
            loader.close()
            if loader.spark:
                loader.spark.stop()
                logger.info("SparkSession stopped")


if __name__ == "__main__":
    main()
