"""
Analytics Validation Script.
Tests Phase 4 analytics functionality.
"""

from utils.logger import get_logger
from utils.spark_session import create_spark_session
from analytics.ranking_analytics import RankingAnalytics
from analytics.player_analytics import PlayerAnalytics
from analytics.match_analytics import MatchAnalytics
import os
import sys
from pprint import pprint

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


logger = get_logger(__name__)


def validate_data_availability():
    """Validate that processed data exists."""
    logger.info("=" * 80)
    logger.info("VALIDATING DATA AVAILABILITY")
    logger.info("=" * 80)

    processed_path = os.getenv("PROCESSED_DATA_PATH", "/data/silver")

    # Check matches
    matches_path = os.path.join(processed_path, "matches")
    if os.path.exists(matches_path):
        logger.info(f"✅ Matches data found: {matches_path}")
        # Check for parquet files
        has_data = any(f.endswith('.parquet') for root, dirs,
                       files in os.walk(matches_path) for f in files)
        if has_data:
            logger.info("✅ Matches Parquet files found")
        else:
            logger.warning("⚠️  No Parquet files found in matches directory")
    else:
        logger.error(f"❌ Matches data NOT found: {matches_path}")

    # Check players
    players_path = os.path.join(processed_path, "players")
    if os.path.exists(players_path):
        logger.info(f"✅ Players data found: {players_path}")
        has_data = any(f.endswith('.parquet') for root, dirs,
                       files in os.walk(players_path) for f in files)
        if has_data:
            logger.info("✅ Players Parquet files found")
        else:
            logger.warning("⚠️  No Parquet files found in players directory")
    else:
        logger.error(f"❌ Players data NOT found: {players_path}")


def validate_match_analytics():
    """Validate match analytics."""
    logger.info("\n" + "=" * 80)
    logger.info("VALIDATING MATCH ANALYTICS")
    logger.info("=" * 80)

    try:
        match_analytics = MatchAnalytics()

        # Check if data can be loaded
        matches_df = match_analytics.load_match_data()
        count = matches_df.count()
        logger.info(f"✅ Loaded {count} match records")

        if count > 0:
            # Show sample data
            logger.info("\nSample match data:")
            matches_df.select("match_id", "status", "tournament_name",
                              "duration_minutes").show(5, truncate=False)

            # Run analytics
            logger.info("\nRunning match analytics...")
            match_analytics.run_match_analytics()

            # Get summary
            summary = match_analytics.get_summary_stats()
            logger.info("\n✅ Match analytics summary:")
            pprint(summary)
        else:
            logger.warning("⚠️  No match data available for analytics")

        return True

    except Exception as e:
        logger.error(f"❌ Match analytics validation failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def validate_player_analytics():
    """Validate player analytics."""
    logger.info("\n" + "=" * 80)
    logger.info("VALIDATING PLAYER ANALYTICS")
    logger.info("=" * 80)

    try:
        player_analytics = PlayerAnalytics()

        # Check if data can be loaded
        players_df = player_analytics.load_player_data()
        count = players_df.count()
        logger.info(f"✅ Loaded {count} player records")

        if count > 0:
            # Show sample data
            logger.info("\nSample player data:")
            players_df.select("player_id", "summoner_name", "role",
                              "kda_ratio", "win_rate").show(5, truncate=False)

            # Run analytics
            logger.info("\nRunning player analytics...")
            player_analytics.run_player_analytics()

            # Get summary
            summary = player_analytics.get_summary_stats()
            logger.info("\n✅ Player analytics summary:")
            pprint(summary)
        else:
            logger.warning("⚠️  No player data available for analytics")

        return True

    except Exception as e:
        logger.error(f"❌ Player analytics validation failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def validate_ranking_analytics():
    """Validate ranking analytics."""
    logger.info("\n" + "=" * 80)
    logger.info("VALIDATING RANKING ANALYTICS")
    logger.info("=" * 80)

    try:
        ranking_analytics = RankingAnalytics()

        # Check if data can be loaded
        players_df = ranking_analytics.load_player_data()
        count = players_df.count()
        logger.info(f"✅ Loaded {count} player records for ranking")

        if count > 0:
            # Run analytics
            logger.info("\nRunning ranking analytics...")
            ranking_analytics.run_ranking_analytics()

            # Get top players
            top_players = ranking_analytics.get_top_rankings(limit=10)

            if top_players:
                logger.info("\n✅ TOP 10 PLAYERS BY COMPOSITE SCORE:")
                logger.info("-" * 80)
                for player in top_players:
                    logger.info(
                        f"{player['overall_rank']:2d}. {player['summoner_name']:20s} "
                        f"({player['current_team_name']:15s}) - {player['role']:10s} - "
                        f"Score: {player['composite_score']:.2f} - "
                        f"KDA: {player['kda']:.2f} - WR: {player['win_rate']:.2f}%"
                    )
            else:
                logger.warning("⚠️  No ranking data generated")
        else:
            logger.warning("⚠️  No player data available for ranking")

        return True

    except Exception as e:
        logger.error(f"❌ Ranking analytics validation failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def validate_output_files():
    """Validate analytics output files."""
    logger.info("\n" + "=" * 80)
    logger.info("VALIDATING OUTPUT FILES")
    logger.info("=" * 80)

    analytics_path = os.getenv("DATA_LAKE_PATH", "/data/gold") + "/analytics"

    expected_outputs = {
        "matches": [
            "matches_by_status",
            "avg_duration_by_status",
            "team_performance",
            "tournament_stats",
            "daily_match_volume",
            "kill_statistics",
            "gold_efficiency"
        ],
        "players": [
            "player_win_rate",
            "player_kda_metrics",
            "player_efficiency",
            "active_players_by_role",
            "team_composition",
            "high_performers",
            "player_activity",
            "role_performance"
        ],
        "rankings": [
            "top_players_by_kda",
            "top_players_by_role",
            "team_rankings",
            "player_percentiles",
            "composite_player_rankings",
            "player_performance_trends"
        ]
    }

    all_exist = True
    for category, outputs in expected_outputs.items():
        logger.info(f"\n{category.upper()} outputs:")
        for output in outputs:
            output_path = os.path.join(analytics_path, category, output)
            if os.path.exists(output_path):
                # Check for parquet files
                has_data = any(f.endswith('.parquet') for root, dirs,
                               files in os.walk(output_path) for f in files)
                if has_data:
                    logger.info(f"  ✅ {output}")
                else:
                    logger.warning(f"  ⚠️  {output} (no Parquet files)")
                    all_exist = False
            else:
                logger.error(f"  ❌ {output} (missing)")
                all_exist = False

    return all_exist


def main():
    """Run all validations."""
    logger.info("\n" + "=" * 80)
    logger.info("PHASE 4 ANALYTICS VALIDATION")
    logger.info("=" * 80)

    results = {
        "data_availability": False,
        "match_analytics": False,
        "player_analytics": False,
        "ranking_analytics": False,
        "output_files": False
    }

    # 1. Check data availability
    validate_data_availability()

    # 2. Validate match analytics
    results["match_analytics"] = validate_match_analytics()

    # 3. Validate player analytics
    results["player_analytics"] = validate_player_analytics()

    # 4. Validate ranking analytics
    results["ranking_analytics"] = validate_ranking_analytics()

    # 5. Validate output files
    results["output_files"] = validate_output_files()

    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 80)

    passed = sum(1 for v in results.values() if v)
    total = len(results)

    for test, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        logger.info(f"{status}: {test}")

    logger.info("\n" + "=" * 80)
    logger.info(f"FINAL RESULT: {passed}/{total} tests passed")
    logger.info("=" * 80)

    return passed == total


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"Validation failed with exception: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
