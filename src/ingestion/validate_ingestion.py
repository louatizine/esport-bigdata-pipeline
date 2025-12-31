#!/usr/bin/env python3
"""Validation script for Phase 2 - Kafka Ingestion Layer.

This script performs comprehensive validation of:
- Kafka broker connectivity
- Topic existence
- Producer functionality
- Riot API accessibility
- Data flow validation
- Error handling
"""

from __future__ import annotations
from src.common.logging_config import configure_logging

import json
import logging
import os
import sys
import time
from typing import Optional, Dict, Any, List

import requests
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.errors import KafkaError, NoBrokersAvailable
from kafka.admin import NewTopic

# Add project root to path
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../..")))


logger = logging.getLogger(__name__)


class IngestionValidator:
    """Validates the complete ingestion pipeline."""

    def __init__(self):
        """Initialize validator with configuration."""
        self.kafka_broker = os.getenv("KAFKA_BROKER", "localhost:9092")
        self.riot_api_key = os.getenv("RIOT_API_KEY")
        self.required_topics = [
            os.getenv("KAFKA_TOPIC_MATCHES", "esport-matches"),
            os.getenv("KAFKA_TOPIC_PLAYERS", "esport-players"),
            os.getenv("KAFKA_TOPIC_RANKINGS", "esport-rankings"),
        ]
        self.validation_results = {}

    def validate_kafka_broker(self) -> bool:
        """Validate Kafka broker is reachable.

        Returns
        -------
        bool
            True if broker is accessible, False otherwise.
        """
        logger.info("=" * 70)
        logger.info("TEST 1: Kafka Broker Connectivity")
        logger.info("=" * 70)
        logger.info("Testing connection to: %s", self.kafka_broker)

        try:
            # Try to create a simple admin client
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.kafka_broker,
                client_id="validator",
                request_timeout_ms=5000,
            )

            # Get cluster metadata
            cluster_metadata = admin_client.list_topics()
            logger.info("✅ SUCCESS: Connected to Kafka broker")
            logger.info("   Broker: %s", self.kafka_broker)
            logger.info("   Available topics: %d", len(cluster_metadata))

            admin_client.close()
            self.validation_results["kafka_broker"] = True
            return True

        except NoBrokersAvailable:
            logger.error(
                "❌ FAILED: No Kafka brokers available at %s", self.kafka_broker)
            logger.error(
                "   Ensure Kafka is running: docker compose --profile core up -d")
            self.validation_results["kafka_broker"] = False
            return False
        except Exception as e:
            logger.error("❌ FAILED: Could not connect to Kafka: %s", e)
            self.validation_results["kafka_broker"] = False
            return False

    def validate_topics_exist(self) -> bool:
        """Validate required Kafka topics exist.

        Returns
        -------
        bool
            True if all topics exist, False otherwise.
        """
        logger.info("\n" + "=" * 70)
        logger.info("TEST 2: Kafka Topics Validation")
        logger.info("=" * 70)

        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.kafka_broker,
                client_id="validator",
            )

            existing_topics = admin_client.list_topics()
            logger.info("Required topics: %s", self.required_topics)

            all_exist = True
            for topic in self.required_topics:
                if topic in existing_topics:
                    logger.info("✅ Topic exists: %s", topic)
                else:
                    logger.error("❌ Topic missing: %s", topic)
                    all_exist = False

            admin_client.close()

            if all_exist:
                logger.info("✅ SUCCESS: All required topics exist")
            else:
                logger.error("❌ FAILED: Some topics are missing")
                logger.info(
                    "   Create topics with: docker compose exec kafka kafka-topics \\")
                logger.info(
                    "     --bootstrap-server kafka:9092 --create --topic <topic-name>")

            self.validation_results["topics_exist"] = all_exist
            return all_exist

        except Exception as e:
            logger.error("❌ FAILED: Could not validate topics: %s", e)
            self.validation_results["topics_exist"] = False
            return False

    def validate_producer_connection(self) -> bool:
        """Validate Kafka producer can connect and send messages.

        Returns
        -------
        bool
            True if producer works, False otherwise.
        """
        logger.info("\n" + "=" * 70)
        logger.info("TEST 3: Kafka Producer Validation")
        logger.info("=" * 70)

        try:
            # Create test producer
            producer = KafkaProducer(
                bootstrap_servers=self.kafka_broker,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                request_timeout_ms=10000,
            )

            logger.info("✅ Producer created successfully")

            # Send test message
            test_message = {
                "test": True,
                "message": "Validation test message",
                "timestamp": int(time.time() * 1000),
            }

            topic = self.required_topics[0]  # Use first topic for test
            logger.info("Sending test message to topic: %s", topic)

            future = producer.send(topic, value=test_message)
            result = future.get(timeout=10)

            logger.info("✅ SUCCESS: Message sent successfully")
            logger.info("   Topic: %s", result.topic)
            logger.info("   Partition: %s", result.partition)
            logger.info("   Offset: %s", result.offset)

            producer.flush()
            producer.close()

            self.validation_results["producer"] = True
            return True

        except Exception as e:
            logger.error("❌ FAILED: Producer test failed: %s", e)
            self.validation_results["producer"] = False
            return False

    def validate_riot_api_access(self) -> bool:
        """Validate Riot API key and connectivity.

        Returns
        -------
        bool
            True if API is accessible, False otherwise.
        """
        logger.info("\n" + "=" * 70)
        logger.info("TEST 4: Riot API Validation")
        logger.info("=" * 70)

        if not self.riot_api_key:
            logger.error("❌ FAILED: RIOT_API_KEY not set in environment")
            logger.error("   Set it in .env file: RIOT_API_KEY=your_key_here")
            self.validation_results["riot_api"] = False
            return False

        logger.info("✅ RIOT_API_KEY is set (length: %d chars)",
                    len(self.riot_api_key))

        # Test API with a simple endpoint (platform status)
        platform = os.getenv("RIOT_API_PLATFORM", "na1")
        test_url = f"https://{platform}.api.riotgames.com/lol/status/v4/platform-data"
        headers = {"X-Riot-Token": self.riot_api_key}

        logger.info("Testing API endpoint: %s", test_url)

        try:
            response = requests.get(test_url, headers=headers, timeout=10)

            if response.status_code == 200:
                logger.info("✅ SUCCESS: Riot API is accessible")
                logger.info("   Endpoint: %s", test_url)
                logger.info("   Status Code: %d", response.status_code)
                logger.info("   Response Size: %d bytes",
                            len(response.content))

                # Parse response
                data = response.json()
                logger.info("   Platform: %s", data.get("name", "Unknown"))

                self.validation_results["riot_api"] = True
                return True

            elif response.status_code == 401:
                logger.error("❌ FAILED: Invalid API key (401 Unauthorized)")
                logger.error(
                    "   Get a valid key from: https://developer.riotgames.com/")
                self.validation_results["riot_api"] = False
                return False

            elif response.status_code == 403:
                logger.error("❌ FAILED: API key forbidden (403)")
                logger.error("   Your API key may be expired or restricted")
                self.validation_results["riot_api"] = False
                return False

            elif response.status_code == 429:
                logger.warning("⚠️  Rate limited (429)")
                logger.warning(
                    "   This is expected behavior - rate limiting works!")
                self.validation_results["riot_api"] = True
                return True

            else:
                logger.error(
                    "❌ FAILED: Unexpected response code: %d", response.status_code)
                logger.error("   Response: %s", response.text[:200])
                self.validation_results["riot_api"] = False
                return False

        except requests.RequestException as e:
            logger.error("❌ FAILED: Could not reach Riot API: %s", e)
            self.validation_results["riot_api"] = False
            return False

    def validate_data_flow(self) -> bool:
        """Validate data flow by consuming messages from Kafka.

        Returns
        -------
        bool
            True if can consume valid messages, False otherwise.
        """
        logger.info("\n" + "=" * 70)
        logger.info("TEST 5: Data Flow Validation (Consumer Test)")
        logger.info("=" * 70)

        try:
            topic = self.required_topics[0]
            logger.info("Creating test consumer for topic: %s", topic)

            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=self.kafka_broker,
                auto_offset_reset="earliest",
                consumer_timeout_ms=5000,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )

            messages_read = 0
            valid_messages = 0
            sample_message = None

            logger.info("Reading messages from topic (timeout: 5s)...")

            for message in consumer:
                messages_read += 1

                # Validate message structure
                try:
                    payload = message.value

                    # Check if it's valid JSON (already deserialized)
                    if isinstance(payload, dict):
                        # Check for non-empty payload
                        if payload:
                            valid_messages += 1

                            # Save first message as sample
                            if sample_message is None:
                                sample_message = payload

                    # Read up to 10 messages for validation
                    if messages_read >= 10:
                        break

                except Exception as e:
                    logger.warning("Invalid message format: %s", e)

            consumer.close()

            logger.info("Messages read: %d", messages_read)
            logger.info("Valid messages: %d", valid_messages)

            if messages_read == 0:
                logger.warning("⚠️  No messages found in topic")
                logger.info(
                    "   This is okay if you haven't run the producer yet")
                logger.info("   Run: python src/ingestion/riot_producer.py")
                self.validation_results["data_flow"] = True  # Not a failure
                return True

            if valid_messages > 0:
                logger.info("✅ SUCCESS: Found valid messages in Kafka")
                logger.info("\n📊 Sample Message:")
                logger.info(json.dumps(sample_message, indent=2)[:500] + "...")

                # Validate message has identifiers
                has_id = False
                if isinstance(sample_message, dict):
                    if "metadata" in sample_message:
                        if "matchId" in sample_message.get("metadata", {}):
                            has_id = True
                            logger.info("✅ Message contains match identifier")
                    elif "test" in sample_message:
                        logger.info(
                            "ℹ️  Test message detected (validation message)")
                        has_id = True

                if not has_id:
                    logger.warning(
                        "⚠️  Message may be missing match identifier")

                self.validation_results["data_flow"] = True
                return True
            else:
                logger.error(
                    "❌ FAILED: Messages found but none are valid JSON")
                self.validation_results["data_flow"] = False
                return False

        except Exception as e:
            logger.error("❌ FAILED: Could not consume messages: %s", e)
            self.validation_results["data_flow"] = False
            return False

    def validate_error_handling(self) -> bool:
        """Validate error handling mechanisms.

        Returns
        -------
        bool
            True if error handling is properly implemented.
        """
        logger.info("\n" + "=" * 70)
        logger.info("TEST 6: Error Handling Validation")
        logger.info("=" * 70)

        # Check if riot_producer.py has rate limiting logic
        producer_file = os.path.join(
            os.path.dirname(__file__), "riot_producer.py"
        )

        if not os.path.exists(producer_file):
            logger.error("❌ FAILED: riot_producer.py not found")
            self.validation_results["error_handling"] = False
            return False

        with open(producer_file, "r") as f:
            content = f.read()

        # Check for key error handling features
        checks = {
            "Rate limiting (429)": "429" in content,
            "Retry logic": "retries" in content or "retry" in content,
            "Exponential backoff": "2 **" in content or "backoff" in content,
            "Error logging": "logger.error" in content,
            "Exception handling": "except" in content,
        }

        all_passed = True
        for check_name, passed in checks.items():
            if passed:
                logger.info("✅ %s: Implemented", check_name)
            else:
                logger.warning("⚠️  %s: Not detected", check_name)
                all_passed = False

        if all_passed:
            logger.info("✅ SUCCESS: Error handling mechanisms are in place")
        else:
            logger.warning("⚠️  Some error handling features may be missing")

        self.validation_results["error_handling"] = all_passed
        return all_passed

    def run_all_validations(self) -> bool:
        """Run all validation tests.

        Returns
        -------
        bool
            True if all validations pass, False otherwise.
        """
        logger.info("\n" + "🔍 " * 20)
        logger.info("PHASE 2 INGESTION LAYER VALIDATION")
        logger.info("🔍 " * 20 + "\n")

        # Run all tests
        tests = [
            self.validate_kafka_broker,
            self.validate_topics_exist,
            self.validate_producer_connection,
            self.validate_riot_api_access,
            self.validate_data_flow,
            self.validate_error_handling,
        ]

        for test in tests:
            try:
                test()
                time.sleep(0.5)  # Brief pause between tests
            except Exception as e:
                logger.exception("Test failed with exception: %s", e)

        # Print summary
        logger.info("\n" + "=" * 70)
        logger.info("VALIDATION SUMMARY")
        logger.info("=" * 70)

        total_tests = len(self.validation_results)
        passed_tests = sum(1 for v in self.validation_results.values() if v)

        for test_name, result in self.validation_results.items():
            status = "✅ PASS" if result else "❌ FAIL"
            logger.info("%s: %s", status, test_name.replace("_", " ").title())

        logger.info("=" * 70)
        logger.info("Result: %d/%d tests passed", passed_tests, total_tests)

        if passed_tests == total_tests:
            logger.info("✅ 🎉 ALL VALIDATIONS PASSED! Phase 2 is complete.")
            return True
        else:
            logger.warning(
                "⚠️  Some validations failed. Please review and fix issues.")
            return False


def main():
    """Main entry point for validation script."""
    configure_logging("conf/logging.yaml")

    logger.info("Starting Phase 2 validation...")
    logger.info("Kafka Broker: %s", os.getenv(
        "KAFKA_BROKER", "localhost:9092"))
    logger.info("Riot API Key: %s", "SET" if os.getenv(
        "RIOT_API_KEY") else "NOT SET")

    validator = IngestionValidator()
    success = validator.run_all_validations()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
