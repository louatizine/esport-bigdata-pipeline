"""
MongoDB Atlas Storage Main.
Loads all analytics data to MongoDB Atlas (cloud database).
Replaces PostgreSQL with cloud-based MongoDB storage.
"""

from spark.utils.spark_session import get_spark_session
from spark.utils.logger import get_logger
import os
import sys
from datetime import datetime
from typing import Optional, Dict, List
import logging

from pyspark.sql import SparkSession
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure

# Add parent directories to path
sys.path.append(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))


logger = get_logger(__name__)


class MongoAtlasLoader:
    """Loads all analytics data to MongoDB Atlas."""

    def __init__(self, spark: Optional[SparkSession] = None):
        """
        Initialize MongoDB Atlas loader.

        Args:
            spark: SparkSession instance (optional, will create if None)
        """
        self.spark = spark or get_spark_session("MongoAtlasLoader")

        # MongoDB Atlas connection
        atlas_uri = os.getenv("MONGODB_ATLAS_URI", "")
        if not atlas_uri:
            raise ValueError(
                "MONGODB_ATLAS_URI not found in environment variables. "
                "Please set your MongoDB Atlas connection string in .env file."
            )

        self.mongo_uri = atlas_uri
        self.mongo_db_name = os.getenv("MONGODB_DATABASE", "esports_analytics")

        # Data paths
        self.silver_path = os.getenv("PROCESSED_DATA_PATH", "/data/silver")
        self.gold_path = os.getenv("DATA_LAKE_PATH", "/data/gold")
        self.analytics_path = os.path.join(self.gold_path, "analytics")

        logger.info("=" * 80)
        logger.info("MongoDB Atlas Loader Initialized")
        logger.info("=" * 80)
        logger.info(f"Database: {self.mongo_db_name}")
        logger.info(f"Silver path: {self.silver_path}")
        logger.info(f"Gold path: {self.gold_path}")
        logger.info(f"Analytics path: {self.analytics_path}")

        # Test MongoDB connection
        self._test_connection()

    def _test_connection(self):
        """Test MongoDB Atlas connection."""
        try:
            client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000)
            client.server_info()
            db = client[self.mongo_db_name]
            collections = db.list_collection_names()
            client.close()

            logger.info("✅ MongoDB Atlas connection successful")
            logger.info(f"📊 Existing collections: {len(collections)}")
            if collections:
                logger.info(f"Collections: {', '.join(collections[:5])}")
        except ConnectionFailure as e:
            logger.error(f"❌ MongoDB Atlas connection failed: {e}")
            logger.error(
                "Please check your MONGODB_ATLAS_URI and network connection")
            raise
        except Exception as e:
            logger.error(f"❌ Unexpected error testing MongoDB connection: {e}")
            raise

    def _load_parquet_to_mongodb(
        self,
        parquet_path: str,
        collection_name: str,
        id_field: str = "_id"
    ) -> int:
        """
        Load Parquet data to MongoDB collection.

        Args:
            parquet_path: Path to Parquet file/directory
            collection_name: MongoDB collection name
            id_field: Field to use as _id (default: "_id")

        Returns:
            Number of documents loaded
        """
        if not os.path.exists(parquet_path):
            logger.warning(f"Path not found: {parquet_path}")
            return 0

        try:
            logger.info(f"📖 Reading from: {parquet_path}")
            df = self.spark.read.parquet(parquet_path)
            record_count = df.count()

            if record_count == 0:
                logger.warning(f"No records found in {parquet_path}")
                return 0

            logger.info(f"📊 Records to load: {record_count}")

            # Convert to pandas and then to dict for MongoDB
            pandas_df = df.toPandas()

            # Ensure _id field exists
            if id_field != "_id" and id_field in pandas_df.columns:
                pandas_df["_id"] = pandas_df[id_field].astype(str)

            # Convert to list of dictionaries
            documents = pandas_df.to_dict('records')

            # Connect to MongoDB and insert
            client = MongoClient(self.mongo_uri)
            db = client[self.mongo_db_name]
            collection = db[collection_name]

            # Clear existing data (optional - remove if you want to append)
            logger.info(f"🗑️  Clearing existing data in {collection_name}")
            collection.delete_many({})

            # Insert documents
            logger.info(
                f"💾 Inserting {len(documents)} documents into {collection_name}")
            result = collection.insert_many(documents, ordered=False)

            # Create indexes
            self._create_indexes(collection, collection_name)

            client.close()

            logger.info(
                f"✅ Successfully loaded {len(result.inserted_ids)} documents to {collection_name}")
            return len(result.inserted_ids)

        except Exception as e:
            logger.error(
                f"❌ Error loading {parquet_path} to {collection_name}: {e}")
            return 0

    def _create_indexes(self, collection, collection_name: str):
        """Create indexes for better query performance."""
        try:
            if "match" in collection_name.lower():
                collection.create_index("match_id")
                collection.create_index("tournament_name")
                collection.create_index("status")
                collection.create_index([("started_at", -1)])
                logger.info(f"📑 Created indexes for {collection_name}")

            elif "player" in collection_name.lower():
                collection.create_index("player_id")
                collection.create_index("summoner_name")
                collection.create_index("team")
                collection.create_index("role")
                collection.create_index([("win_rate", -1)])
                collection.create_index([("kda_ratio", -1)])
                logger.info(f"📑 Created indexes for {collection_name}")

            elif "team" in collection_name.lower():
                collection.create_index("team_name")
                collection.create_index([("rank", 1)])
                collection.create_index([("win_rate", -1)])
                logger.info(f"📑 Created indexes for {collection_name}")

        except Exception as e:
            logger.warning(
                f"Could not create indexes for {collection_name}: {e}")

    def load_all_data(self) -> Dict[str, int]:
        """
        Load all data to MongoDB Atlas.

        Returns:
            Dictionary with collection names and document counts
        """
        logger.info("\n" + "=" * 80)
        logger.info("🚀 MONGODB ATLAS LOAD STARTED")
        logger.info("=" * 80)
        start_time = datetime.now()

        stats = {}

        # 1. Load Silver Layer Data (processed matches and players)
        logger.info("\n📂 LOADING SILVER LAYER DATA")
        logger.info("-" * 80)

        stats["matches"] = self._load_parquet_to_mongodb(
            os.path.join(self.silver_path, "matches"),
            "matches",
            "match_id"
        )

        stats["players"] = self._load_parquet_to_mongodb(
            os.path.join(self.silver_path, "players"),
            "players",
            "player_id"
        )

        # 2. Load Gold Layer Analytics
        logger.info("\n📈 LOADING ANALYTICS DATA (GOLD LAYER)")
        logger.info("-" * 80)

        # Match Analytics
        match_analytics_collections = [
            "matches_by_status",
            "avg_duration_by_status",
            "team_performance",
            "tournament_stats",
            "daily_match_volume",
            "kill_statistics",
            "gold_efficiency"
        ]

        for collection in match_analytics_collections:
            path = os.path.join(self.analytics_path, "matches", collection)
            stats[f"analytics_{collection}"] = self._load_parquet_to_mongodb(
                path, collection, "_id"
            )

        # Player Analytics
        player_analytics_collections = [
            "player_win_rate",
            "player_kda_metrics",
            "player_efficiency",
            "active_players_by_role",
            "team_composition",
            "high_performers",
            "player_activity",
            "role_performance"
        ]

        for collection in player_analytics_collections:
            path = os.path.join(self.analytics_path, "players", collection)
            stats[f"analytics_{collection}"] = self._load_parquet_to_mongodb(
                path, collection, "_id"
            )

        # Ranking Analytics
        ranking_collections = [
            "top_players_by_kda",
            "top_players_by_role",
            "team_rankings",
            "player_percentiles",
            "composite_player_rankings",
            "player_performance_trends"
        ]

        for collection in ranking_collections:
            path = os.path.join(self.analytics_path, "rankings", collection)
            stats[f"analytics_{collection}"] = self._load_parquet_to_mongodb(
                path, collection, "_id"
            )

        # Summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        total_documents = sum(stats.values())
        successful_collections = sum(
            1 for count in stats.values() if count > 0)

        logger.info("\n" + "=" * 80)
        logger.info("✅ MONGODB ATLAS LOAD COMPLETE")
        logger.info("=" * 80)
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Total collections: {len(stats)}")
        logger.info(f"Successful collections: {successful_collections}")
        logger.info(f"Total documents loaded: {total_documents:,}")
        logger.info("=" * 80)

        # Print detailed stats
        logger.info("\n📊 DETAILED STATISTICS:")
        logger.info("-" * 80)
        for collection, count in sorted(stats.items()):
            status = "✅" if count > 0 else "⚠️"
            logger.info(f"{status} {collection:.<50} {count:>10,} docs")

        return stats

    def verify_data(self) -> Dict[str, any]:
        """
        Verify loaded data in MongoDB Atlas.

        Returns:
            Dictionary with verification results
        """
        logger.info("\n" + "=" * 80)
        logger.info("🔍 VERIFYING MONGODB ATLAS DATA")
        logger.info("=" * 80)

        try:
            client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000)
            db = client[self.mongo_db_name]

            collections = db.list_collection_names()
            logger.info(f"📊 Total collections: {len(collections)}")

            verification = {}
            for coll_name in collections:
                collection = db[coll_name]
                count = collection.count_documents({})
                verification[coll_name] = {
                    "count": count,
                    "indexes": len(collection.index_information())
                }
                logger.info(
                    f"  • {coll_name}: {count:,} documents, {verification[coll_name]['indexes']} indexes")

            client.close()

            logger.info("=" * 80)
            logger.info("✅ VERIFICATION COMPLETE")
            logger.info("=" * 80)

            return verification

        except Exception as e:
            logger.error(f"❌ Verification failed: {e}")
            return {}


def main():
    """Main entry point."""
    logger.info("=" * 80)
    logger.info("🌩️  MONGODB ATLAS STORAGE LOADER")
    logger.info("=" * 80)

    # Check environment variables
    if not os.getenv("MONGODB_ATLAS_URI"):
        logger.error("❌ MONGODB_ATLAS_URI not set in environment")
        logger.error(
            "Please add your MongoDB Atlas connection string to .env file")
        logger.error("Get it from: https://cloud.mongodb.com/")
        sys.exit(1)

    try:
        # Initialize loader
        loader = MongoAtlasLoader()

        # Load all data
        stats = loader.load_all_data()

        # Verify data
        verification = loader.verify_data()

        # Check if any data was loaded
        if sum(stats.values()) == 0:
            logger.warning(
                "⚠️  No data was loaded. Please run Phase 3 and Phase 4 first:")
            logger.warning("  1. python spark/main.py  (Phase 3: Streaming)")
            logger.warning(
                "  2. python spark/analytics_main.py all  (Phase 4: Analytics)")
            logger.warning("  3. Then run this script again")
        else:
            logger.info("\n🎉 SUCCESS! Data is now available in MongoDB Atlas")
            logger.info(f"You can access it at: https://cloud.mongodb.com/")
            logger.info(f"Database: {loader.mongo_db_name}")

    except KeyboardInterrupt:
        logger.info("\n⚠️  Interrupted by user")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
