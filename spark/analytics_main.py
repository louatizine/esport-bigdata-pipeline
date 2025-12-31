"""
Analytics main orchestrator.
Runs all analytics jobs in sequence.
"""

from utils.logger import get_logger
from utils.spark_session import create_spark_session
from analytics.ranking_analytics import RankingAnalytics
from analytics.player_analytics import PlayerAnalytics
from analytics.match_analytics import MatchAnalytics
import os
import sys
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


logger = get_logger(__name__)


class AnalyticsOrchestrator:
    """Orchestrates all analytics jobs."""

    def __init__(self):
        """Initialize analytics orchestrator."""
        self.spark = create_spark_session("AnalyticsOrchestrator")
        self.start_time = None
        self.end_time = None

    def run_all_analytics(self):
        """
        Run all analytics jobs in sequence.
        """
        try:
            self.start_time = datetime.now()
            logger.info("=" * 80)
            logger.info("ANALYTICS ORCHESTRATOR STARTED")
            logger.info(f"Start time: {self.start_time}")
            logger.info("=" * 80)

            # Job 1: Match Analytics
            logger.info("\n" + "=" * 80)
            logger.info("JOB 1/3: MATCH ANALYTICS")
            logger.info("=" * 80)
            match_analytics = MatchAnalytics(spark=self.spark)
            match_analytics.run_match_analytics()
            match_summary = match_analytics.get_summary_stats()
            logger.info(f"Match analytics summary: {match_summary}")

            # Job 2: Player Analytics
            logger.info("\n" + "=" * 80)
            logger.info("JOB 2/3: PLAYER ANALYTICS")
            logger.info("=" * 80)
            player_analytics = PlayerAnalytics(spark=self.spark)
            player_analytics.run_player_analytics()
            player_summary = player_analytics.get_summary_stats()
            logger.info(f"Player analytics summary: {player_summary}")

            # Job 3: Ranking Analytics
            logger.info("\n" + "=" * 80)
            logger.info("JOB 3/3: RANKING ANALYTICS")
            logger.info("=" * 80)
            ranking_analytics = RankingAnalytics(spark=self.spark)
            ranking_analytics.run_ranking_analytics()

            # Get top players
            top_players = ranking_analytics.get_top_rankings(limit=10)
            logger.info("\n" + "=" * 80)
            logger.info("TOP 10 PLAYERS BY COMPOSITE SCORE")
            logger.info("=" * 80)
            for player in top_players:
                logger.info(
                    f"{player['overall_rank']:2d}. {player['summoner_name']:20s} "
                    f"({player['current_team_name']:15s}) - {player['role']:10s} - "
                    f"Score: {player['composite_score']:.2f}"
                )

            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()

            logger.info("\n" + "=" * 80)
            logger.info("ANALYTICS ORCHESTRATOR COMPLETED SUCCESSFULLY")
            logger.info(f"End time: {self.end_time}")
            logger.info(f"Total duration: {duration:.2f} seconds")
            logger.info("=" * 80)

        except Exception as e:
            logger.error(f"Analytics orchestrator failed: {e}")
            raise
        finally:
            if self.spark:
                self.spark.stop()
                logger.info("SparkSession stopped")

    def run_match_analytics_only(self):
        """Run only match analytics."""
        try:
            logger.info("Running match analytics only")
            match_analytics = MatchAnalytics(spark=self.spark)
            match_analytics.run_match_analytics()
            logger.info("Match analytics completed")
        except Exception as e:
            logger.error(f"Match analytics failed: {e}")
            raise
        finally:
            if self.spark:
                self.spark.stop()

    def run_player_analytics_only(self):
        """Run only player analytics."""
        try:
            logger.info("Running player analytics only")
            player_analytics = PlayerAnalytics(spark=self.spark)
            player_analytics.run_player_analytics()
            logger.info("Player analytics completed")
        except Exception as e:
            logger.error(f"Player analytics failed: {e}")
            raise
        finally:
            if self.spark:
                self.spark.stop()

    def run_ranking_analytics_only(self):
        """Run only ranking analytics."""
        try:
            logger.info("Running ranking analytics only")
            ranking_analytics = RankingAnalytics(spark=self.spark)
            ranking_analytics.run_ranking_analytics()
            logger.info("Ranking analytics completed")
        except Exception as e:
            logger.error(f"Ranking analytics failed: {e}")
            raise
        finally:
            if self.spark:
                self.spark.stop()


def main():
    """
    Main entry point.

    Supports command-line arguments:
    - python analytics_main.py all (default)
    - python analytics_main.py matches
    - python analytics_main.py players
    - python analytics_main.py rankings
    """
    import argparse

    parser = argparse.ArgumentParser(description="Run analytics jobs")
    parser.add_argument(
        "job",
        nargs="?",
        default="all",
        choices=["all", "matches", "players", "rankings"],
        help="Which analytics job to run (default: all)"
    )

    args = parser.parse_args()

    orchestrator = AnalyticsOrchestrator()

    try:
        if args.job == "all":
            orchestrator.run_all_analytics()
        elif args.job == "matches":
            orchestrator.run_match_analytics_only()
        elif args.job == "players":
            orchestrator.run_player_analytics_only()
        elif args.job == "rankings":
            orchestrator.run_ranking_analytics_only()

    except Exception as e:
        logger.error(f"Analytics job failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
