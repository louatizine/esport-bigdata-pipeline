"""
Streaming metrics and monitoring utilities.

Provides tools for tracking streaming query health and performance.
"""

from typing import Dict, Optional
from pyspark.sql.streaming import StreamingQuery
from utils.logger import get_logger


logger = get_logger(__name__)


class StreamingMetrics:
    """
    Track and log Spark Structured Streaming query metrics.

    Provides easy access to query progress, status, and performance metrics.
    """

    def __init__(self, query: StreamingQuery):
        """
        Initialize metrics tracker for a streaming query.

        Args:
            query: StreamingQuery to monitor
        """
        self.query = query
        self.query_name = query.name
        self.query_id = query.id

    def is_active(self) -> bool:
        """Check if query is currently active."""
        return self.query.isActive

    def get_status(self) -> Dict:
        """
        Get current query status.

        Returns:
            Dict with status information
        """
        if not self.query.isActive:
            return {
                "status": "inactive",
                "query_name": self.query_name,
                "query_id": self.query_id
            }

        status = self.query.status
        return {
            "status": "active",
            "query_name": self.query_name,
            "query_id": self.query_id,
            "message": status.get("message", ""),
            "is_trigger_active": status.get("isTriggerActive", False),
            "is_data_available": status.get("isDataAvailable", False)
        }

    def get_progress(self) -> Optional[Dict]:
        """
        Get latest progress metrics from last micro-batch.

        Returns:
            Dict with progress metrics or None if no progress available
        """
        if not self.query.isActive:
            return None

        progress = self.query.lastProgress
        if not progress:
            return None

        return {
            "batch_id": progress.get("batchId", -1),
            "num_input_rows": progress.get("numInputRows", 0),
            "input_rows_per_second": progress.get("inputRowsPerSecond", 0.0),
            "processed_rows_per_second": progress.get("processedRowsPerSecond", 0.0),
            "batch_duration_ms": progress.get("durationMs", {}).get("triggerExecution", 0),
            "sources": self._extract_source_metrics(progress),
            "sink": self._extract_sink_metrics(progress)
        }

    def _extract_source_metrics(self, progress: Dict) -> list:
        """Extract source-specific metrics from progress."""
        sources = []
        for source in progress.get("sources", []):
            sources.append({
                "description": source.get("description", ""),
                "start_offset": source.get("startOffset", ""),
                "end_offset": source.get("endOffset", ""),
                "num_input_rows": source.get("numInputRows", 0),
                "input_rows_per_second": source.get("inputRowsPerSecond", 0.0),
                "processed_rows_per_second": source.get("processedRowsPerSecond", 0.0)
            })
        return sources

    def _extract_sink_metrics(self, progress: Dict) -> Dict:
        """Extract sink-specific metrics from progress."""
        sink = progress.get("sink", {})
        return {
            "description": sink.get("description", ""),
            "num_output_rows": sink.get("numOutputRows", -1)
        }

    def log_metrics(self, include_progress: bool = True):
        """
        Log current metrics to logger.

        Args:
            include_progress: Whether to include detailed progress metrics
        """
        status = self.get_status()

        logger.info(
            f"Query status: {self.query_name}",
            **status
        )

        if include_progress:
            progress = self.get_progress()
            if progress:
                logger.info(
                    f"Query progress: {self.query_name}",
                    **{k: v for k, v in progress.items() if k not in ["sources", "sink"]}
                )

    def get_recent_progress(self, num_recent: int = 5) -> list:
        """
        Get recent progress reports.

        Args:
            num_recent: Number of recent progress reports to retrieve

        Returns:
            List of recent progress dictionaries
        """
        if not self.query.isActive:
            return []

        recent = self.query.recentProgress
        if not recent:
            return []

        return recent[-num_recent:] if len(recent) > num_recent else recent

    def print_summary(self):
        """Print a human-readable summary of query metrics."""
        print(f"\n{'=' * 70}")
        print(f"Streaming Query Summary: {self.query_name}")
        print(f"{'=' * 70}")

        status = self.get_status()
        print(f"Status: {status['status'].upper()}")
        print(f"Query ID: {status['query_id']}")

        if status['status'] == 'active':
            print(f"Message: {status['message']}")

            progress = self.get_progress()
            if progress:
                print(f"\nLatest Batch:")
                print(f"  Batch ID: {progress['batch_id']}")
                print(f"  Input Rows: {progress['num_input_rows']}")
                print(
                    f"  Input Rate: {progress['input_rows_per_second']:.2f} rows/sec")
                print(
                    f"  Process Rate: {progress['processed_rows_per_second']:.2f} rows/sec")
                print(f"  Duration: {progress['batch_duration_ms']} ms")

        print(f"{'=' * 70}\n")


def monitor_query_health(query: StreamingQuery, log_interval_batches: int = 10):
    """
    Monitor query health and log warnings for issues.

    Args:
        query: StreamingQuery to monitor
        log_interval_batches: How often to log metrics (in batches)

    Returns:
        bool: True if query is healthy, False otherwise
    """
    metrics = StreamingMetrics(query)

    if not metrics.is_active():
        logger.error(f"Query {query.name} is not active")

        # Check for exception
        if query.exception():
            logger.error(
                f"Query failed with exception",
                query_name=query.name,
                exception=str(query.exception())
            )
        return False

    progress = metrics.get_progress()
    if not progress:
        logger.warning(f"No progress available for query {query.name}")
        return True

    # Check for potential issues
    batch_id = progress['batch_id']
    input_rate = progress['input_rows_per_second']
    process_rate = progress['processed_rows_per_second']

    # Log periodically
    if batch_id % log_interval_batches == 0:
        metrics.log_metrics(include_progress=True)

    # Warn if processing is slower than input
    if input_rate > 0 and process_rate < input_rate * 0.8:
        logger.warning(
            f"Query {query.name} may be falling behind",
            batch_id=batch_id,
            input_rate=input_rate,
            process_rate=process_rate,
            backlog_risk="HIGH"
        )

    return True
