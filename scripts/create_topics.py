#!/usr/bin/env python3
"""Standalone script to create Kafka topics for the esports pipeline."""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.common.logging_config import configure_logging
from src.ingestion.kafka_config import KafkaConfig


def main():
    """Create all required Kafka topics."""
    configure_logging("conf/logging.yaml")
    
    kafka_config = KafkaConfig()
    print(f"Creating topics on broker: {kafka_config.broker}")
    print(f"  - {kafka_config.topic_matches}")
    print(f"  - {kafka_config.topic_players}")
    print(f"  - {kafka_config.topic_rankings}")
    
    kafka_config.create_topics(num_partitions=3, replication_factor=1)
    print("Topic creation complete!")


if __name__ == "__main__":
    main()
