#!/usr/bin/env bash
# Script to create Kafka topics based on conf/kafka/topics.yaml
# Run after Kafka container is up: docker compose up -d kafka

# Placeholder for topic creation logic
# Example usage (manual):
# docker compose exec kafka kafka-topics.sh --create \
#   --topic esports.matches \
#   --bootstrap-server kafka:9092 \
#   --partitions 3 \
#   --replication-factor 1
