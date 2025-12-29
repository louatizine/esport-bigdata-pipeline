"""Riot API to Kafka producer.

Fetches match data from Riot Games API and publishes to Kafka topics.
Implements rate limiting, error handling, and proper logging.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from typing import List, Dict, Any, Optional

import requests
from kafka import KafkaProducer

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.common.logging_config import configure_logging
from src.ingestion.kafka_config import KafkaConfig

logger = logging.getLogger(__name__)


class RiotAPIClient:
    """Client for interacting with Riot Games API."""

    def __init__(self, api_key: str, region: str = "americas", platform: str = "na1"):
        """Initialize Riot API client.

        Parameters
        ----------
        api_key : str
            Riot API key from developer portal.
        region : str
            Regional routing value (americas, asia, europe, sea).
        platform : str
            Platform routing value (na1, euw1, kr, etc.).
        """
        self.api_key = api_key
        self.region = region
        self.platform = platform
        self.base_url_regional = f"https://{region}.api.riotgames.com"
        self.base_url_platform = f"https://{platform}.api.riotgames.com"
        self.headers = {"X-Riot-Token": api_key}
        self.request_count = 0
        self.last_request_time = time.time()

    def _rate_limit_sleep(self, requests_per_second: int = 20) -> None:
        """Implement basic rate limiting.

        Parameters
        ----------
        requests_per_second : int
            Maximum requests per second (default: 20 for development key).
        """
        min_interval = 1.0 / requests_per_second
        elapsed = time.time() - self.last_request_time
        if elapsed < min_interval:
            sleep_time = min_interval - elapsed
            logger.debug("Rate limiting: sleeping for %.3f seconds", sleep_time)
            time.sleep(sleep_time)
        self.last_request_time = time.time()

    def _make_request(self, url: str, retries: int = 3) -> Optional[Dict[str, Any]]:
        """Make HTTP request with retry logic.

        Parameters
        ----------
        url : str
            Full API endpoint URL.
        retries : int
            Number of retry attempts on failure.

        Returns
        -------
        Optional[Dict[str, Any]]
            JSON response data or None on failure.
        """
        self._rate_limit_sleep()

        for attempt in range(retries):
            try:
                response = requests.get(url, headers=self.headers, timeout=10)
                self.request_count += 1

                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 10))
                    logger.warning("Rate limited. Waiting %d seconds...", retry_after)
                    time.sleep(retry_after)
                elif response.status_code == 404:
                    logger.warning("Resource not found: %s", url)
                    return None
                else:
                    logger.error("HTTP %d: %s", response.status_code, response.text)
                    if attempt < retries - 1:
                        time.sleep(2 ** attempt)  # Exponential backoff
                    else:
                        return None

            except requests.RequestException as e:
                logger.error("Request failed (attempt %d/%d): %s", attempt + 1, retries, e)
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)

        return None

    def get_summoner_by_name(self, summoner_name: str) -> Optional[Dict[str, Any]]:
        """Get summoner data by summoner name.

        Parameters
        ----------
        summoner_name : str
            Summoner name (e.g., "Faker").

        Returns
        -------
        Optional[Dict[str, Any]]
            Summoner data including puuid, or None.
        """
        url = f"{self.base_url_platform}/lol/summoner/v4/summoners/by-name/{summoner_name}"
        logger.info("Fetching summoner: %s", summoner_name)
        return self._make_request(url)

    def get_match_ids_by_puuid(
        self, puuid: str, count: int = 20, start: int = 0
    ) -> List[str]:
        """Get match IDs for a player by PUUID.

        Parameters
        ----------
        puuid : str
            Player Universal Unique Identifier.
        count : int
            Number of match IDs to retrieve (max 100).
        start : int
            Starting index for pagination.

        Returns
        -------
        List[str]
            List of match IDs.
        """
        url = (
            f"{self.base_url_regional}/lol/match/v5/matches/by-puuid/{puuid}/ids"
            f"?start={start}&count={count}"
        )
        logger.info("Fetching %d match IDs for PUUID: %s...", count, puuid[:8])
        result = self._make_request(url)
        return result if result else []

    def get_match_details(self, match_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed match data by match ID.

        Parameters
        ----------
        match_id : str
            Match identifier (e.g., "NA1_1234567890").

        Returns
        -------
        Optional[Dict[str, Any]]
            Full match details including metadata and participant info.
        """
        url = f"{self.base_url_regional}/lol/match/v5/matches/{match_id}"
        logger.info("Fetching match details: %s", match_id)
        return self._make_request(url)


class RiotKafkaProducer:
    """Kafka producer for Riot API data ingestion."""

    def __init__(self, kafka_config: KafkaConfig, riot_client: RiotAPIClient):
        """Initialize producer.

        Parameters
        ----------
        kafka_config : KafkaConfig
            Kafka configuration instance.
        riot_client : RiotAPIClient
            Riot API client instance.
        """
        self.kafka_config = kafka_config
        self.riot_client = riot_client
        self.producer = kafka_config.create_producer()
        self.messages_sent = 0

    def publish_match(self, match_data: Dict[str, Any]) -> bool:
        """Publish match data to Kafka.

        Parameters
        ----------
        match_data : Dict[str, Any]
            Full match details from Riot API.

        Returns
        -------
        bool
            True if published successfully, False otherwise.
        """
        try:
            match_id = match_data.get("metadata", {}).get("matchId", "unknown")
            self.producer.send(
                self.kafka_config.topic_matches,
                key=match_id,
                value=match_data
            )
            self.producer.flush()
            self.messages_sent += 1
            logger.info("Published match %s to Kafka topic: %s", match_id, self.kafka_config.topic_matches)
            return True
        except Exception as e:
            logger.error("Failed to publish match to Kafka: %s", e)
            return False

    def ingest_matches_for_summoner(self, summoner_name: str, match_count: int = 20) -> int:
        """Fetch and publish matches for a given summoner.

        Parameters
        ----------
        summoner_name : str
            Summoner name to fetch matches for.
        match_count : int
            Number of recent matches to fetch.

        Returns
        -------
        int
            Number of matches successfully published.
        """
        logger.info("Starting ingestion for summoner: %s", summoner_name)

        # Step 1: Get summoner data to retrieve PUUID
        summoner = self.riot_client.get_summoner_by_name(summoner_name)
        if not summoner:
            logger.error("Could not find summoner: %s", summoner_name)
            return 0

        puuid = summoner.get("puuid")
        if not puuid:
            logger.error("No PUUID found for summoner: %s", summoner_name)
            return 0

        logger.info("Found summoner %s with PUUID: %s", summoner_name, puuid[:8])

        # Step 2: Get list of match IDs
        match_ids = self.riot_client.get_match_ids_by_puuid(puuid, count=match_count)
        if not match_ids:
            logger.warning("No matches found for summoner: %s", summoner_name)
            return 0

        logger.info("Retrieved %d match IDs", len(match_ids))

        # Step 3: Fetch and publish each match
        successful_publishes = 0
        for i, match_id in enumerate(match_ids, 1):
            logger.info("Processing match %d/%d: %s", i, len(match_ids), match_id)

            match_details = self.riot_client.get_match_details(match_id)
            if match_details:
                if self.publish_match(match_details):
                    successful_publishes += 1
            else:
                logger.warning("Could not retrieve details for match: %s", match_id)

            # Small delay between matches to be respectful to API
            time.sleep(0.5)

        logger.info(
            "Ingestion complete: %d/%d matches published successfully",
            successful_publishes,
            len(match_ids)
        )
        return successful_publishes

    def close(self) -> None:
        """Close Kafka producer and clean up resources."""
        logger.info("Closing Kafka producer. Total messages sent: %d", self.messages_sent)
        self.producer.close()


def main():
    """Main entry point for Riot API producer."""
    # Configure logging
    configure_logging("conf/logging.yaml")
    logger.info("Starting Riot API to Kafka producer")

    # Load environment variables
    riot_api_key = os.getenv("RIOT_API_KEY")
    riot_region = os.getenv("RIOT_API_REGION", "americas")
    riot_platform = os.getenv("RIOT_API_PLATFORM", "na1")

    if not riot_api_key:
        logger.error("RIOT_API_KEY not found in environment variables")
        logger.error("Please set RIOT_API_KEY in your .env file")
        sys.exit(1)

    # Initialize Kafka configuration and create topics
    kafka_config = KafkaConfig()
    logger.info("Creating Kafka topics...")
    kafka_config.create_topics(num_partitions=3, replication_factor=1)

    # Initialize Riot API client
    riot_client = RiotAPIClient(
        api_key=riot_api_key,
        region=riot_region,
        platform=riot_platform
    )

    # Initialize Kafka producer
    producer = RiotKafkaProducer(kafka_config, riot_client)

    try:
        # Example: Ingest matches for a well-known summoner
        # Replace with actual summoner name or make this configurable
        test_summoner = os.getenv("TEST_SUMMONER_NAME", "Doublelift")
        match_count = int(os.getenv("MATCH_COUNT", "10"))

        logger.info("Ingesting %d matches for summoner: %s", match_count, test_summoner)
        matches_published = producer.ingest_matches_for_summoner(test_summoner, match_count)

        logger.info("=" * 60)
        logger.info("INGESTION SUMMARY")
        logger.info("=" * 60)
        logger.info("Summoner: %s", test_summoner)
        logger.info("Matches published: %d", matches_published)
        logger.info("Total API requests: %d", riot_client.request_count)
        logger.info("=" * 60)

    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    except Exception as e:
        logger.exception("Unexpected error during ingestion: %s", e)
    finally:
        producer.close()
        logger.info("Producer shutdown complete")


if __name__ == "__main__":
    main()

