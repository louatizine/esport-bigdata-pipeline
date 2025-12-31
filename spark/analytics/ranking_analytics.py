"""
Ranking analytics module.
Generates advanced rankings using window functions.
"""

from utils.logger import get_logger
from utils.spark_session import create_spark_session
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, rank, dense_rank, percent_rank
from pyspark.sql.window import Window

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


logger = get_logger(__name__)


class RankingAnalytics:
    """Ranking analytics processor."""

    def __init__(self, spark: SparkSession = None):
        """
        Initialize ranking analytics.

        Args:
            spark: SparkSession instance (optional, will create if None)
        """
        self.spark = spark or create_spark_session("RankingAnalytics")
        self.processed_data_path = os.getenv(
            "PROCESSED_DATA_PATH", "/data/silver")
        self.analytics_output_path = os.getenv(
            "DATA_LAKE_PATH", "/data/gold") + "/analytics"
        self.sql_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "sql"
        )

    def load_required_data(self):
        """
        Load all required data (players, matches).

        Returns:
            tuple: (players_df, matches_df)
        """
        try:
            players_path = f"{self.processed_data_path}/players"
            matches_path = f"{self.processed_data_path}/matches"

            logger.info(f"Loading player data from: {players_path}")
            players_df = self.spark.read.parquet(players_path)

            logger.info(f"Loading match data from: {matches_path}")
            matches_df = self.spark.read.parquet(matches_path)

            # Add computed columns for matches
            matches_df = matches_df.withColumn(
                "duration_minutes",
                col("match_duration") / 60
            )

            logger.info(
                f"Loaded {players_df.count()} players and {matches_df.count()} matches")
            return players_df, matches_df

        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            raise

    def execute_sql_file(self, sql_file: str):
        """
        Execute SQL queries from a file.

        Args:
            sql_file: Name of SQL file (e.g., 'ranking.sql')
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

    def save_view_to_parquet(self, view_name: str, output_subdir: str = "rankings"):
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

    def create_prerequisite_views(self):
        """
        Create prerequisite views needed by ranking.sql.
        These are views from match_metrics.sql that ranking.sql depends on.
        """
        try:
            logger.info("Creating prerequisite views for rankings")

            # Create team_performance view (needed for team_rankings)
            matches_df = self.spark.table("matches")

            # Team performance calculation
            team_stats = matches_df.filter(col("status") == "finished").selectExpr(
                "team_1_name as team_name",
                "CASE WHEN winner_name = team_1_name THEN 1 ELSE 0 END as wins",
                "CASE WHEN winner_name = team_1_name THEN 0 ELSE 1 END as losses",
                "team_1_kills as total_kills",
                "team_1_gold as total_gold"
            ).unionAll(
                matches_df.filter(col("status") == "finished").selectExpr(
                    "team_2_name as team_name",
                    "CASE WHEN winner_name = team_2_name THEN 1 ELSE 0 END as wins",
                    "CASE WHEN winner_name = team_2_name THEN 0 ELSE 1 END as losses",
                    "team_2_kills as total_kills",
                    "team_2_gold as total_gold"
                )
            )

            team_performance = team_stats.groupBy("team_name").agg(
                count("*").alias("total_matches"),
                spark_sum("wins").alias("total_wins"),
                spark_sum("losses").alias("total_losses"),
                (spark_sum("wins") * 100.0 / count("*")).alias("win_rate"),
                spark_sum("total_kills").alias("total_kills"),
                spark_sum("total_gold").alias("total_gold"),
                avg("total_kills").alias("avg_kills_per_game"),
                avg("total_gold").alias("avg_gold_per_game")
            )

            team_performance.createOrReplaceTempView("team_performance")
            logger.info("Created team_performance view")

            # Create daily_match_volume view (needed for tournament_running_totals)
            from pyspark.sql.functions import to_date, avg as avg_func, sum as sum_func

            daily_volume = matches_df.filter(col("started_at").isNotNull()).groupBy(
                to_date("started_at").alias("match_date"),
                "tournament_name"
            ).agg(
                count("*").alias("total_matches"),
                count("tournament_name").alias("tournaments"),
                sum_func((col("status") == "finished").cast(
                    "int")).alias("completed_matches"),
                sum_func((col("status") == "live").cast(
                    "int")).alias("live_matches"),
                avg_func("match_duration").alias("avg_duration")
            ).orderBy(col("match_date").desc())

            daily_volume.createOrReplaceTempView("daily_match_volume")
            logger.info("Created daily_match_volume view")

        except Exception as e:
            logger.error(f"Failed to create prerequisite views: {e}")
            raise

    def run_ranking_analytics(self):
        """
        Run all ranking analytics.
        """
        try:
            logger.info("Starting ranking analytics job")

            # Load required data
            players_df, matches_df = self.load_required_data()

            # Register as temp views
            players_df.createOrReplaceTempView("players")
            matches_df.createOrReplaceTempView("matches")
            logger.info("Registered temp views")

            # Create prerequisite views
            self.create_prerequisite_views()

            # Execute SQL analytics
            self.execute_sql_file("ranking.sql")

            # Save all generated views
            views_to_save = [
                "top_players_by_kda",
                "top_players_by_role",
                "team_rankings",
                "player_percentiles",
                "tournament_running_totals",
                "player_momentum",
                "composite_player_rankings",
                "player_performance_trends"
            ]

            for view_name in views_to_save:
                self.save_view_to_parquet(view_name, output_subdir="rankings")

            logger.info("Ranking analytics job completed successfully")

        except Exception as e:
            logger.error(f"Ranking analytics job failed: {e}")
            raise

    def get_top_rankings(self, limit: int = 10):
        """
        Get top players by overall ranking.

        Args:
            limit: Number of top players to return

        Returns:
            list: Top players
        """
        try:
            top_players = self.spark.sql(f"""
                SELECT
                    overall_rank,
                    summoner_name,
                    current_team_name,
                    role,
                    composite_score
                FROM composite_player_rankings
                WHERE overall_rank <= {limit}
                ORDER BY overall_rank
            """)

            results = top_players.collect()
            logger.info(f"Retrieved top {limit} players")

            return [row.asDict() for row in results]

        except Exception as e:
            logger.error(f"Failed to get top rankings: {e}")
            return []


def main():
    """Main entry point for ranking analytics."""
    try:
        from pyspark.sql.functions import avg as avg_func, sum as spark_sum

        analytics = RankingAnalytics()
        analytics.run_ranking_analytics()

        # Print top 10 players
        top_players = analytics.get_top_rankings(limit=10)
        logger.info("=" * 60)
        logger.info("TOP 10 PLAYERS BY COMPOSITE SCORE")
        logger.info("=" * 60)
        for player in top_players:
            logger.info(
                f"{player['overall_rank']}. {player['summoner_name']} "
                f"({player['current_team_name']}) - {player['role']} - "
                f"Score: {player['composite_score']}"
            )

    except Exception as e:
        logger.error(f"Ranking analytics failed: {e}")
        raise
    finally:
        # Cleanup
        if 'analytics' in locals() and analytics.spark:
            analytics.spark.stop()


if __name__ == "__main__":
    main()
