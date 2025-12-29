"""Project settings definitions.

Provide a single place to read environment variables and map to
strongly-named settings objects. Keep implementation minimal.
"""

from __future__ import annotations

import os


class Settings:
    """Environment-driven settings (minimal; extend per needs)."""

    project_name: str = os.getenv("PROJECT_NAME", "esport-bigdata-pipeline")
    riot_api_key: str | None = os.getenv("RIOT_API_KEY")

    kafka_broker: str = os.getenv("KAFKA_BROKER", "kafka:9092")
    zookeeper_connect: str = os.getenv("ZOOKEEPER_CONNECT", "zookeeper:2181")
    kafka_topic_prefix: str = os.getenv("KAFKA_TOPIC_PREFIX", "esports")

    spark_master_url: str = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
    spark_app_name: str = os.getenv("SPARK_APP_NAME", "esports-pipeline")

    data_lake_root: str = os.getenv("DATA_LAKE_ROOT", "/workspace/data")


settings = Settings()
