#!/usr/bin/env bash
# Example spark-submit wrapper for streaming/batch jobs
# Modify paths and arguments as needed

# Example Spark submit command (executed inside spark-master container):
# docker compose exec spark-master spark-submit \
#   --master spark://spark-master:7077 \
#   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
#   /workspace/src/streaming/jobs/streaming_job_template.py
