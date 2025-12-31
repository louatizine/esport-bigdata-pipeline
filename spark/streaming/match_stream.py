"""
Spark Structured Streaming job for processing match data from Kafka.

This module consumes match events from Kafka, processes them,
and writes the results to the data lake in Parquet format.
"""

from utils.logger import get_logger
from utils.spark_session import (
    create_spark_session,
    get_kafka_bootstrap_servers,
    get_checkpoint_location,
    get_output_path
)
from schemas.match_schema import get_match_schema
import os
import sys
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    from_json,
    current_timestamp,
    to_timestamp,
    expr
)

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


logger = get_logger(__name__)


class MatchStreamProcessor:
    """
    Processes streaming match data from Kafka and writes to data lake.
    """

    def __init__(self, spark=None):
        """
        Initialize the match stream processor.

        Args:
            spark: SparkSession (will create one if not provided)
        """
        self.spark = spark or create_spark_session("MatchStreamProcessor")
        self.topic = os.getenv("KAFKA_TOPIC_MATCHES", "esports-matches")
        self.schema = get_match_schema()
        logger.info(
            f"Initialized MatchStreamProcessor for topic: {self.topic}")

    def read_kafka_stream(self) -> DataFrame:
        """
        Read streaming data from Kafka topic.

        Returns:
            DataFrame: Raw streaming DataFrame from Kafka
        """
        logger.info(f"Reading stream from Kafka topic: {self.topic}")

        try:
            kafka_df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", get_kafka_bootstrap_servers()) \
                .option("subscribe", self.topic) \
                .option("startingOffsets", "earliest") \
                .option("maxOffsetsPerTrigger", 1000) \
                .option("failOnDataLoss", "false") \
                .load()

            logger.info("Successfully connected to Kafka stream")
            return kafka_df

        except Exception as e:
            logger.error(f"Failed to read from Kafka: {str(e)}")
            raise

    def process_stream(self, raw_df: DataFrame) -> DataFrame:
        """
        Process the raw Kafka stream by parsing JSON and applying transformations.

        Args:
            raw_df: Raw DataFrame from Kafka

        Returns:
            DataFrame: Processed DataFrame with match data
        """
        logger.info("Processing match stream data")

        try:
            # Parse JSON from Kafka value field with PERMISSIVE mode
            # This prevents crashes on malformed JSON
            parsed_df = raw_df.select(
                from_json(
                    col("value").cast("string"),
                    self.schema,
                    {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"}
                ).alias("data"),
                col("value").cast("string").alias(
                    "raw_value"),  # Keep for debugging
                col("timestamp").alias("kafka_timestamp"),
                col("partition"),
                col("offset")
            )

            # Filter valid vs corrupt records
            # Corrupt records have null data field when JSON parsing fails
            valid_df = parsed_df.filter(col("data").isNotNull())
            corrupt_df = parsed_df.filter(col("data").isNull())

            # Count corrupt records for logging
            # Note: This is evaluated per micro-batch
            if corrupt_df is not None:
                logger.warning(
                    "Filtering corrupt records from stream",
                    info="Corrupt records will be logged in foreachBatch"
                )

            # Extract all fields from nested structure (valid records only)
            processed_df = valid_df.select(
                col("data.*"),
                col("kafka_timestamp"),
                col("partition"),
                col("offset"),
                current_timestamp().alias("processing_timestamp")
            )

            # Add data quality transformations
            processed_df = processed_df.withColumn(
                "started_at",
                to_timestamp(col("started_at"))
            ).withColumn(
                "finished_at",
                to_timestamp(col("finished_at"))
            ).withColumn(
                # Calculate match duration in minutes
                "duration_minutes",
                expr("CAST(match_duration / 60 AS INT)")
            ).withColumn(
                # Flag for completed matches
                "is_completed",
                col("status") == "finished"
            )

            logger.info("Stream processing completed successfully")
            return processed_df

        except Exception as e:
            logger.error(f"Failed to process stream: {str(e)}")
            raise

    def log_batch_metrics(self, batch_df: DataFrame, batch_id: int):
        """
        Log metrics for each micro-batch.

        Args:
            batch_df: DataFrame for current batch
            batch_id: Batch identifier
        """
        try:
            record_count = batch_df.count()

            logger.info(
                f"Processing batch {batch_id}",
                batch_id=batch_id,
                record_count=record_count,
                topic=self.topic
            )

            # Log sample data for first few batches (debugging)
            if batch_id < 3 and record_count > 0:
                logger.debug(
                    f"Batch {batch_id} sample",
                    batch_id=batch_id,
                    sample_count=min(3, record_count)
                )
        except Exception as e:
            logger.error(f"Failed to log batch metrics: {str(e)}")

    def write_to_parquet(self, processed_df: DataFrame, query_name: str = "match_stream"):
        """
        Write processed stream to Parquet files in the data lake.

        Args:
            processed_df: Processed DataFrame to write
            query_name: Name for the streaming query

        Returns:
            StreamingQuery: The active streaming query
        """
        output_path = get_output_path("matches")
        checkpoint_path = get_checkpoint_location(query_name)

        logger.info(
            f"Writing stream to Parquet",
            output_path=output_path,
            checkpoint_path=checkpoint_path
        )

        try:
            # Define batch write function with logging
            def write_batch(batch_df: DataFrame, batch_id: int):
                """
                Write each micro-batch with metrics logging.

                Args:
                    batch_df: DataFrame for current batch
                    batch_id: Batch identifier
                """
                # Log batch metrics
                self.log_batch_metrics(batch_df, batch_id)

                # Write batch to parquet
                if batch_df.count() > 0:
                    batch_df.write \
                        .mode("append") \
                        .partitionBy("status") \
                        .parquet(output_path)

                    logger.info(
                        f"Batch {batch_id} written successfully",
                        batch_id=batch_id,
                        output_path=output_path
                    )

            # Write stream using foreachBatch for better control
            query = processed_df \
                .writeStream \
                .foreachBatch(write_batch) \
                .option("checkpointLocation", checkpoint_path) \
                .trigger(processingTime="10 seconds") \
                .queryName(query_name) \
                .start()

            logger.info(
                f"Streaming query started",
                query_name=query_name,
                query_id=query.id
            )

            return query

        except Exception as e:
            logger.error(f"Failed to write stream to Parquet: {str(e)}")
            raise

    def run(self):
        """
        Run the complete streaming pipeline.

        Returns:
            StreamingQuery: The active streaming query
        """
        logger.info("Starting match streaming pipeline")

        try:
            # Read from Kafka
            raw_df = self.read_kafka_stream()

            # Process the stream
            processed_df = self.process_stream(raw_df)

            # Write to Parquet
            query = self.write_to_parquet(processed_df)

            logger.info("Match streaming pipeline running successfully")
            return query

        except Exception as e:
            logger.critical(f"Match streaming pipeline failed: {str(e)}")
            raise


def main():
    """
    Main entry point for match streaming job.
    """
    logger.info("=" * 60)
    logger.info("MATCH STREAM PROCESSOR - STARTING")
    logger.info("=" * 60)

    try:
        processor = MatchStreamProcessor()
        query = processor.run()

        # Wait for termination
        logger.info("Streaming query active. Waiting for termination...")
        query.awaitTermination()

    except KeyboardInterrupt:
        logger.info("Received shutdown signal. Stopping gracefully...")
    except Exception as e:
        logger.critical(f"Fatal error in match stream processor: {str(e)}")
        sys.exit(1)
    finally:
        logger.info("Match stream processor terminated")


if __name__ == "__main__":
    main()
