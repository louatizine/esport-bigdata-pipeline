"""
Phase 5 Storage Validation Script.
Validates PostgreSQL and MongoDB data loading.
"""

from spark.utils.logger import get_logger
import os
import sys
from typing import Optional

import psycopg2
from psycopg2 import sql

# Add parent directories to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


logger = get_logger(__name__)


def get_postgres_connection():
    """Get PostgreSQL connection."""
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            database=os.getenv("POSTGRES_DATABASE", "esports_analytics"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", "postgres")
        )
        return conn
    except Exception as e:
        logger.error(f"PostgreSQL connection failed: {e}")
        return None


def validate_postgres_tables(conn) -> dict:
    """Validate PostgreSQL tables exist and have data."""
    logger.info("=" * 80)
    logger.info("VALIDATING POSTGRESQL TABLES")
    logger.info("=" * 80)

    results = {}
    expected_tables = [
        "matches_analytics",
        "player_analytics",
        "team_rankings",
        "match_team_performance",
        "player_role_stats"
    ]

    cursor = conn.cursor()

    for table in expected_tables:
        try:
            # Check if table exists
            cursor.execute(
                sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table))
            )
            count = cursor.fetchone()[0]

            if count > 0:
                logger.info(f"✅ {table}: {count:,} records")
                results[table] = {"exists": True,
                                  "count": count, "status": "✅"}
            else:
                logger.warning(f"⚠️  {table}: exists but empty")
                results[table] = {"exists": True, "count": 0, "status": "⚠️"}

        except Exception as e:
            logger.error(f"❌ {table}: {e}")
            results[table] = {"exists": False, "count": 0, "status": "❌"}

    cursor.close()
    return results


def validate_postgres_views(conn) -> dict:
    """Validate PostgreSQL views exist."""
    logger.info("\n" + "=" * 80)
    logger.info("VALIDATING POSTGRESQL VIEWS")
    logger.info("=" * 80)

    results = {}
    expected_views = [
        "vw_active_players",
        "vw_top_players_kda",
        "vw_top_players_winrate",
        "vw_match_stats_by_status",
        "vw_tournament_stats",
        "vw_team_performance",
        "vw_role_comparison",
        "vw_daily_matches",
        "vw_player_kpis",
        "vw_match_kpis",
        "vw_grafana_time_series"
    ]

    cursor = conn.cursor()

    for view in expected_views:
        try:
            cursor.execute(
                sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(view))
            )
            count = cursor.fetchone()[0]
            logger.info(f"✅ {view}: {count:,} records")
            results[view] = {"exists": True, "count": count, "status": "✅"}

        except Exception as e:
            logger.error(f"❌ {view}: {e}")
            results[view] = {"exists": False, "count": 0, "status": "❌"}

    cursor.close()
    return results


def validate_postgres_indexes(conn) -> dict:
    """Validate PostgreSQL indexes exist."""
    logger.info("\n" + "=" * 80)
    logger.info("VALIDATING POSTGRESQL INDEXES")
    logger.info("=" * 80)

    cursor = conn.cursor()

    # Query to get all indexes
    cursor.execute("""
        SELECT
            tablename,
            COUNT(*) as index_count
        FROM pg_indexes
        WHERE schemaname = 'public'
        GROUP BY tablename
        ORDER BY tablename;
    """)

    results = {}
    total_indexes = 0

    for table, count in cursor.fetchall():
        logger.info(f"✅ {table}: {count} indexes")
        results[table] = count
        total_indexes += count

    logger.info(f"\nTotal indexes: {total_indexes}")

    cursor.close()
    return results


def validate_mongodb_collections():
    """Validate MongoDB collections (optional)."""
    logger.info("\n" + "=" * 80)
    logger.info("VALIDATING MONGODB COLLECTIONS")
    logger.info("=" * 80)

    try:
        import pymongo

        mongo_host = os.getenv("MONGO_HOST", "localhost")
        mongo_port = int(os.getenv("MONGO_PORT", "27017"))
        mongo_db = os.getenv("MONGO_DATABASE", "esports_analytics")

        client = pymongo.MongoClient(
            f"mongodb://{mongo_host}:{mongo_port}", serverSelectionTimeoutMS=5000)

        # Test connection
        client.server_info()

        db = client[mongo_db]

        results = {}
        expected_collections = [
            "top_players_by_kda",
            "player_rankings_by_role",
            "composite_player_rankings"
        ]

        for collection_name in expected_collections:
            count = db[collection_name].count_documents({})

            if count > 0:
                logger.info(f"✅ {collection_name}: {count:,} documents")
                results[collection_name] = {
                    "exists": True, "count": count, "status": "✅"}
            else:
                logger.warning(f"⚠️  {collection_name}: exists but empty")
                results[collection_name] = {
                    "exists": True, "count": 0, "status": "⚠️"}

        client.close()
        return results

    except Exception as e:
        logger.warning(f"MongoDB validation skipped: {e}")
        logger.info("MongoDB is optional - this is not a failure")
        return {}


def test_sample_queries(conn):
    """Test sample queries to verify data integrity."""
    logger.info("\n" + "=" * 80)
    logger.info("TESTING SAMPLE QUERIES")
    logger.info("=" * 80)

    cursor = conn.cursor()

    # Test 1: Player KPIs
    try:
        cursor.execute("SELECT * FROM vw_player_kpis;")
        kpis = cursor.fetchone()
        logger.info("\n✅ Player KPIs:")
        logger.info(f"   Total players: {kpis[0]:,}")
        logger.info(f"   Active players: {kpis[1]:,}")
        logger.info(f"   Avg win rate: {kpis[2]:.2f}%")
        logger.info(f"   Avg KDA: {kpis[3]:.2f}")
    except Exception as e:
        logger.error(f"❌ Player KPIs query failed: {e}")

    # Test 2: Match KPIs
    try:
        cursor.execute("SELECT * FROM vw_match_kpis;")
        kpis = cursor.fetchone()
        logger.info("\n✅ Match KPIs:")
        logger.info(f"   Total matches: {kpis[0]:,}")
        logger.info(f"   Total tournaments: {kpis[1]:,}")
        logger.info(f"   Avg duration: {kpis[3]:.2f} min")
    except Exception as e:
        logger.error(f"❌ Match KPIs query failed: {e}")

    # Test 3: Top 5 players by KDA
    try:
        cursor.execute(
            "SELECT summoner_name, kda_ratio, win_rate FROM vw_top_players_kda LIMIT 5;")
        logger.info("\n✅ Top 5 Players by KDA:")
        for rank, (name, kda, wr) in enumerate(cursor.fetchall(), 1):
            logger.info(f"   {rank}. {name}: KDA {kda:.2f}, WR {wr:.2f}%")
    except Exception as e:
        logger.error(f"❌ Top players query failed: {e}")

    cursor.close()


def main():
    """Run all validations."""
    logger.info("\n" + "=" * 80)
    logger.info("PHASE 5 STORAGE VALIDATION")
    logger.info("=" * 80)

    validation_results = {
        "postgres_tables": {},
        "postgres_views": {},
        "postgres_indexes": {},
        "mongodb_collections": {},
        "sample_queries": True
    }

    # 1. Connect to PostgreSQL
    conn = get_postgres_connection()

    if not conn:
        logger.error("❌ Cannot connect to PostgreSQL - validation failed")
        return False

    logger.info("✅ PostgreSQL connection successful\n")

    # 2. Validate tables
    validation_results["postgres_tables"] = validate_postgres_tables(conn)

    # 3. Validate views
    validation_results["postgres_views"] = validate_postgres_views(conn)

    # 4. Validate indexes
    validation_results["postgres_indexes"] = validate_postgres_indexes(conn)

    # 5. Validate MongoDB (optional)
    validation_results["mongodb_collections"] = validate_mongodb_collections()

    # 6. Test sample queries
    test_sample_queries(conn)

    # Close connection
    conn.close()
    logger.info("\nPostgreSQL connection closed")

    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 80)

    # Count successes
    table_success = sum(
        1 for v in validation_results["postgres_tables"].values() if v["status"] == "✅")
    view_success = sum(
        1 for v in validation_results["postgres_views"].values() if v["status"] == "✅")

    logger.info(f"PostgreSQL Tables: {table_success}/5 ✅")
    logger.info(f"PostgreSQL Views: {view_success}/11 ✅")
    logger.info(
        f"PostgreSQL Indexes: {sum(validation_results['postgres_indexes'].values())} total")

    if validation_results["mongodb_collections"]:
        mongo_success = sum(
            1 for v in validation_results["mongodb_collections"].values() if v["status"] == "✅")
        logger.info(f"MongoDB Collections: {mongo_success}/3 ✅")
    else:
        logger.info("MongoDB Collections: Skipped (optional)")

    logger.info("\n" + "=" * 80)

    # Determine overall status
    all_tables_ok = table_success == 5
    all_views_ok = view_success == 11

    if all_tables_ok and all_views_ok:
        logger.info("FINAL RESULT: ✅ ALL VALIDATIONS PASSED")
        logger.info("=" * 80)
        return True
    else:
        logger.warning("FINAL RESULT: ⚠️  SOME VALIDATIONS FAILED")
        logger.info("=" * 80)
        return False


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"Validation failed with exception: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
