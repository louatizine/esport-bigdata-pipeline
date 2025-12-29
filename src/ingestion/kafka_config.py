"""Kafka configuration and producer factory.

Centralizes Kafka connection logic and topic management.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
import json

logger = logging.getLogger(__name__)


class KafkaConfig:
    """Kafka configuration and client factory."""

    def __init__(self):
        """Initialize Kafka configuration from environment variables."""
        self.broker = os.getenv("KAFKA_BROKER", "kafka:9092")
        self.topic_matches = os.getenv("KAFKA_TOPIC_MATCHES", "esport-matches")
        self.topic_players = os.getenv("KAFKA_TOPIC_PLAYERS", "esport-players")
        self.topic_rankings = os.getenv("KAFKA_TOPIC_RANKINGS", "esport-rankings")

    def create_producer(self) -> KafkaProducer:
        """Create and return a configured Kafka producer.

        Returns
        -------
        KafkaProducer
            Configured producer with JSON serialization.
        """
        logger.info("Creating Kafka producer for broker: %s", self.broker)
        producer = KafkaProducer(
            bootstrap_servers=self.broker,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",  # Wait for all replicas to acknowledge
            retries=3,
            max_in_flight_requests_per_connection=1,  # Ensure ordering
        )
        logger.info("Kafka producer created successfully")
        return producer

    def create_topics(self, num_partitions: int = 3, replication_factor: int = 1) -> None:
        """Create Kafka topics if they don't exist.

        Parameters
        ----------
        num_partitions : int
            Number of partitions per topic.
        replication_factor : int
            Replication factor (use 1 for single-broker setup).
        """
        admin_client = KafkaAdminClient(
            bootstrap_servers=self.broker,
            client_id="topic-creator"
        )

        topics = [
            NewTopic(
                name=self.topic_matches,
                num_partitions=num_partitions,
                replication_factor=replication_factor
            ),
            NewTopic(
                name=self.topic_players,
                num_partitions=num_partitions,
                replication_factor=replication_factor
            ),
            NewTopic(
                name=self.topic_rankings,
                num_partitions=num_partitions,
                replication_factor=replication_factor
            ),
        ]

        for topic in topics:
            try:
                admin_client.create_topics([topic], validate_only=False)
                logger.info("Created Kafka topic: %s", topic.name)
            except TopicAlreadyExistsError:
                logger.info("Topic already exists: %s", topic.name)
            except Exception as e:
                logger.error("Failed to create topic %s: %s", topic.name, e)

        admin_client.close()
        logger.info("Topic creation process completed")
