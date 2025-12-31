"""
SparkSession builder and configuration for streaming applications.
Centralized configuration for Spark with Kafka integration.
"""

import os
from pyspark.sql import SparkSession
from utils.logger import get_logger


logger = get_logger(__name__)


def create_spark_session(
    app_name: str,
    master_url: str = None,
    enable_hive: bool = False
) -> SparkSession:
    """
    Create and configure a SparkSession for structured streaming with Kafka.

    This function initializes a Spark session with:
    - Kafka integration packages
    - Optimized configurations for streaming
    - Docker-compatible settings
    - Memory and executor configurations

    Args:
        app_name: Name of the Spark application
        master_url: Spark master URL (defaults to env variable or local)
        enable_hive: Whether to enable Hive support

    Returns:
        SparkSession: Configured Spark session
    """
    logger.info(f"Creating SparkSession for application: {app_name}")

    # Get Spark master URL from environment or use provided value
    master = master_url or os.getenv("SPARK_MASTER_URL", "local[*]")
    logger.info(f"Using Spark master: {master}")

    # Build Spark session
    builder = SparkSession.builder \
        .appName(app_name) \
        .master(master)

    # Add Kafka integration packages
    # Using Kafka SQL connector for Spark Structured Streaming
    kafka_package = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    builder = builder.config("spark.jars.packages", kafka_package)

    # Configure Spark for streaming performance
    configs = {
        # Streaming configurations
        "spark.streaming.stopGracefullyOnShutdown": "true",
        "spark.sql.streaming.schemaInference": "false",

        # Memory configurations
        "spark.driver.memory": os.getenv("SPARK_DRIVER_MEMORY", "2g"),
        "spark.executor.memory": os.getenv("SPARK_EXECUTOR_MEMORY", "2g"),

        # Serialization
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",

        # Dynamic allocation (disabled for streaming)
        "spark.dynamicAllocation.enabled": "false",

        # Shuffle configurations
        "spark.sql.shuffle.partitions": os.getenv("SPARK_SHUFFLE_PARTITIONS", "10"),

        # Checkpoint compression
        "spark.checkpoint.compress": "true",

        # UI configurations
        "spark.ui.enabled": "true",
        "spark.ui.port": os.getenv("SPARK_UI_PORT", "4040"),
    }

    # Apply all configurations
    for key, value in configs.items():
        builder = builder.config(key, value)

    # Enable Hive support if requested
    if enable_hive:
        logger.info("Enabling Hive support")
        builder = builder.enableHiveSupport()

    # Create the session
    try:
        spark = builder.getOrCreate()

        # Set log level for Spark
        log_level = os.getenv("LOG_LEVEL", "INFO").upper()
        spark.sparkContext.setLogLevel(log_level)

        logger.info(
            "SparkSession created successfully",
            spark_version=spark.version,
            master=master,
            app_name=app_name
        )

        return spark

    except Exception as e:
        logger.error(f"Failed to create SparkSession: {str(e)}")
        raise


def get_kafka_bootstrap_servers() -> str:
    """
    Get Kafka bootstrap servers from environment.

    Returns:
        str: Kafka bootstrap servers

    Raises:
        ValueError: If KAFKA_BOOTSTRAP_SERVERS is not set
    """
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap_servers:
        raise ValueError(
            "KAFKA_BOOTSTRAP_SERVERS environment variable not set")

    logger.info(f"Using Kafka bootstrap servers: {bootstrap_servers}")
    return bootstrap_servers


def get_checkpoint_location(job_name: str) -> str:
    """
    Get checkpoint location for streaming query.

    Args:
        job_name: Name of the streaming job

    Returns:
        str: Checkpoint directory path
    """
    base_path = os.getenv(
        "DATA_LAKE_PATH", "/workspaces/esport-bigdata-pipeline/data")
    checkpoint_path = f"{base_path}/checkpoints/{job_name}"

    logger.info(f"Using checkpoint location: {checkpoint_path}")
    return checkpoint_path


def get_output_path(data_type: str) -> str:
    """
    Get output path for processed data.

    Args:
        data_type: Type of data (matches, players, etc.)

    Returns:
        str: Output directory path
    """
    processed_path = os.getenv(
        "PROCESSED_DATA_PATH",
        os.getenv("DATA_LAKE_PATH",
                  "/workspaces/esport-bigdata-pipeline/data") + "/processed"
    )
    output_path = f"{processed_path}/{data_type}"

    logger.info(f"Using output path for {data_type}: {output_path}")
    return output_path
