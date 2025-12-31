"""
Main orchestrator for Spark Structured Streaming jobs.

Coordinates multiple streaming jobs (matches and players) and manages
their lifecycle. Supports running jobs individually or concurrently.
"""

import os
import sys
import argparse
import signal
from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed

from streaming.match_stream import MatchStreamProcessor
from streaming.player_stream import PlayerStreamProcessor
from utils.spark_session import create_spark_session
from utils.logger import get_logger
from utils.metrics import StreamingMetrics, monitor_query_health


logger = get_logger(__name__)

# Global flag for graceful shutdown
shutdown_requested = False


def signal_handler(signum, frame):
    """
    Handle shutdown signals gracefully.

    Args:
        signum: Signal number
        frame: Current stack frame
    """
    global shutdown_requested
    logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
    shutdown_requested = True


def run_match_stream(spark):
    """
    Run the match streaming job.

    Args:
        spark: Shared SparkSession

    Returns:
        StreamingQuery: Active streaming query
    """
    try:
        logger.info("Starting match stream processor")
        processor = MatchStreamProcessor(spark)
        query = processor.run()
        return query
    except Exception as e:
        logger.error(f"Failed to start match stream: {str(e)}")
        raise


def run_player_stream(spark):
    """
    Run the player streaming job.

    Args:
        spark: Shared SparkSession

    Returns:
        StreamingQuery: Active streaming query
    """
    try:
        logger.info("Starting player stream processor")
        processor = PlayerStreamProcessor(spark)
        query = processor.run()
        return query
    except Exception as e:
        logger.error(f"Failed to start player stream: {str(e)}")
        raise


def run_all_streams(spark):
    """
    Run all streaming jobs concurrently.

    Args:
        spark: Shared SparkSession

    Returns:
        List[StreamingQuery]: List of active streaming queries
    """
    logger.info("=" * 70)
    logger.info("STARTING ALL STREAMING JOBS")
    logger.info("=" * 70)

    queries = []

    try:
        # Start match stream
        match_query = run_match_stream(spark)
        queries.append(match_query)
        logger.info(f"Match stream started: {match_query.name}")

        # Start player stream
        player_query = run_player_stream(spark)
        queries.append(player_query)
        logger.info(f"Player stream started: {player_query.name}")

        logger.info(f"All {len(queries)} streaming jobs are running")
        return queries

    except Exception as e:
        logger.error(f"Failed to start all streams: {str(e)}")
        # Stop any queries that were started
        for query in queries:
            try:
                query.stop()
            except:
                pass
        raise


def monitor_streams(queries: List):
    """
    Monitor running streaming queries and handle failures.

    Args:
        queries: List of StreamingQuery objects to monitor
    """
    logger.info("Monitoring streaming queries...")

    # Create metrics trackers for each query
    metrics_trackers = [StreamingMetrics(q) for q in queries]

    try:
        while not shutdown_requested:
            all_healthy = True

            for query, metrics in zip(queries, metrics_trackers):
                # Check if query is still active
                if not query.isActive:
                    logger.error(
                        f"Query {query.name} has stopped unexpectedly",
                        query_id=query.id
                    )

                    # Check for exceptions
                    if query.exception():
                        logger.error(
                            f"Query {query.name} failed with exception",
                            exception=str(query.exception())
                        )

                    all_healthy = False
                else:
                    # Monitor health with automatic logging
                    healthy = monitor_query_health(
                        query, log_interval_batches=10)
                    all_healthy = all_healthy and healthy

            if not all_healthy:
                return False

            # Sleep before next check
            import time
            time.sleep(30)

        return True

    except KeyboardInterrupt:
        logger.info("Monitoring interrupted by user")
        return True


def stop_all_queries(queries: List):
    """
    Stop all streaming queries gracefully.

    Args:
        queries: List of StreamingQuery objects to stop
    """
    logger.info("Stopping all streaming queries...")

    for query in queries:
        try:
            if query.isActive:
                logger.info(f"Stopping query: {query.name}")
                query.stop()
                logger.info(f"Query {query.name} stopped successfully")
        except Exception as e:
            logger.error(f"Error stopping query {query.name}: {str(e)}")


def main():
    """
    Main entry point for the streaming application.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Esports Analytics Spark Structured Streaming"
    )
    parser.add_argument(
        "--job",
        choices=["matches", "players", "all"],
        default="all",
        help="Which streaming job(s) to run (default: all)"
    )
    parser.add_argument(
        "--await-termination",
        action="store_true",
        help="Wait for streaming queries to terminate"
    )

    args = parser.parse_args()

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logger.info("=" * 70)
    logger.info("ESPORTS ANALYTICS - SPARK STRUCTURED STREAMING")
    logger.info("=" * 70)
    logger.info(f"Job mode: {args.job}")
    logger.info(
        f"Kafka Bootstrap Servers: {os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'Not set')}")
    logger.info(f"Data Lake Path: {os.getenv('DATA_LAKE_PATH', 'Not set')}")
    logger.info("=" * 70)

    queries = []
    spark = None

    try:
        # Create shared Spark session
        spark = create_spark_session("EsportsAnalytics_Streaming")

        # Run the specified job(s)
        if args.job == "matches":
            logger.info("Running MATCHES streaming job only")
            query = run_match_stream(spark)
            queries.append(query)

        elif args.job == "players":
            logger.info("Running PLAYERS streaming job only")
            query = run_player_stream(spark)
            queries.append(query)

        else:  # all
            logger.info("Running ALL streaming jobs")
            queries = run_all_streams(spark)

        # Monitor or wait for termination
        if args.await_termination:
            logger.info("Waiting for queries to terminate (Ctrl+C to stop)...")
            for query in queries:
                query.awaitTermination()
        else:
            logger.info("Monitoring queries (Ctrl+C to stop)...")
            monitor_streams(queries)

    except KeyboardInterrupt:
        logger.info("Received interrupt signal. Shutting down...")

    except Exception as e:
        logger.critical(f"Fatal error in main: {str(e)}", exc_info=True)
        sys.exit(1)

    finally:
        # Cleanup
        if queries:
            stop_all_queries(queries)

        if spark:
            logger.info("Stopping SparkSession...")
            spark.stop()

        logger.info("=" * 70)
        logger.info("STREAMING APPLICATION TERMINATED")
        logger.info("=" * 70)


if __name__ == "__main__":
    main()
