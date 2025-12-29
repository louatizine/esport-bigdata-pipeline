#!/usr/bin/env python3
"""Test script to verify Kafka producer setup without calling Riot API.

This creates a mock match and publishes it to Kafka to test the pipeline.
"""

import sys
import os
import json
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.common.logging_config import configure_logging
from src.ingestion.kafka_config import KafkaConfig

def main():
    """Test Kafka producer with mock data."""
    configure_logging("conf/logging.yaml")
    
    print("🧪 Testing Kafka Producer Setup")
    print("=" * 60)
    
    # Initialize Kafka config
    kafka_config = KafkaConfig()
    kafka_config.broker = os.getenv("KAFKA_BROKER", "localhost:9092")
    
    print(f"Kafka Broker: {kafka_config.broker}")
    print(f"Target Topic: {kafka_config.topic_matches}")
    
    # Create producer
    try:
        producer = kafka_config.create_producer()
        print("✅ Kafka producer created successfully")
    except Exception as e:
        print(f"❌ Failed to create producer: {e}")
        return 1
    
    # Create mock match data
    mock_match = {
        "metadata": {
            "dataVersion": "2",
            "matchId": "TEST_MATCH_001",
            "participants": ["player1", "player2"]
        },
        "info": {
            "gameCreation": int(datetime.now().timestamp() * 1000),
            "gameDuration": 1800,
            "gameMode": "CLASSIC",
            "gameType": "MATCHED_GAME",
            "participants": [
                {
                    "summonerName": "TestPlayer1",
                    "championName": "Ahri",
                    "kills": 5,
                    "deaths": 2,
                    "assists": 10,
                    "win": True
                },
                {
                    "summonerName": "TestPlayer2",
                    "championName": "Yasuo",
                    "kills": 3,
                    "deaths": 5,
                    "assists": 7,
                    "win": False
                }
            ]
        }
    }
    
    # Publish test message
    try:
        print("\n📤 Publishing test match...")
        future = producer.send(
            kafka_config.topic_matches,
            key="TEST_MATCH_001",
            value=mock_match
        )
        result = future.get(timeout=10)
        print(f"✅ Message published successfully!")
        print(f"   Topic: {result.topic}")
        print(f"   Partition: {result.partition}")
        print(f"   Offset: {result.offset}")
        
        producer.flush()
        producer.close()
        
        print("\n" + "=" * 60)
        print("🎉 Test completed successfully!")
        print("=" * 60)
        print("\nTo verify the message was received, run:")
        print("docker compose exec kafka kafka-console-consumer \\")
        print("  --bootstrap-server kafka:9092 \\")
        print("  --topic esport-matches \\")
        print("  --from-beginning \\")
        print("  --max-messages 1 | jq")
        
        return 0
        
    except Exception as e:
        print(f"❌ Failed to publish message: {e}")
        producer.close()
        return 1


if __name__ == "__main__":
    sys.exit(main())
