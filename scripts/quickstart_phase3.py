#!/usr/bin/env python3
"""
Quick Start - Spark Structured Streaming

This script provides a quick demonstration of the Spark Structured Streaming
implementation. Use this for testing and learning.
"""

import os
import sys
from pathlib import Path

# Set environment defaults
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
os.environ.setdefault("KAFKA_TOPIC_MATCHES", "esports-matches")
os.environ.setdefault("KAFKA_TOPIC_PLAYERS", "esports-players")
os.environ.setdefault("SPARK_MASTER_URL", "local[2]")
os.environ.setdefault(
    "DATA_LAKE_PATH", "/workspaces/esport-bigdata-pipeline/data")
os.environ.setdefault("LOG_LEVEL", "INFO")


def print_banner():
    """Print welcome banner."""
    print("\n" + "=" * 70)
    print("  SPARK STRUCTURED STREAMING - QUICK START")
    print("  Esports Analytics Platform - Phase 3")
    print("=" * 70 + "\n")


def check_prerequisites():
    """Check if basic prerequisites are met."""
    print("Checking prerequisites...\n")

    errors = []

    # Check PySpark
    try:
        import pyspark
        print(f"✓ PySpark installed (version {pyspark.__version__})")
    except ImportError:
        print("✗ PySpark not installed")
        errors.append("PySpark")

    # Check Kafka connection
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    print(f"✓ Kafka servers configured: {kafka_servers}")

    # Check data directory
    data_path = Path(os.getenv("DATA_LAKE_PATH"))
    if data_path.exists():
        print(f"✓ Data lake path exists: {data_path}")
    else:
        print(f"⚠ Data lake path does not exist: {data_path}")
        print(f"  Creating directory...")
        data_path.mkdir(parents=True, exist_ok=True)

    # Check spark directory
    spark_dir = Path("/workspaces/esport-bigdata-pipeline/spark")
    if spark_dir.exists():
        print(f"✓ Spark directory found: {spark_dir}")
    else:
        print(f"✗ Spark directory not found: {spark_dir}")
        errors.append("Spark directory")

    if errors:
        print(f"\n✗ Missing prerequisites: {', '.join(errors)}")
        print("\nPlease run: pip install -r requirements/spark-streaming.txt")
        return False

    print("\n✓ All prerequisites satisfied!\n")
    return True


def show_options():
    """Display available options."""
    print("Available commands:\n")
    print("1. Run all streaming jobs:")
    print("   python spark/main.py --job all\n")

    print("2. Run match stream only:")
    print("   python spark/main.py --job matches\n")

    print("3. Run player stream only:")
    print("   python spark/main.py --job players\n")

    print("4. Run with await termination:")
    print("   python spark/main.py --job all --await-termination\n")

    print("5. Using spark-submit:")
    print("   ./scripts/run_spark_streaming.sh --job all\n")

    print("6. Validate installation:")
    print("   python scripts/validate_phase3.py\n")


def show_environment():
    """Show current environment configuration."""
    print("Current Configuration:")
    print("-" * 70)

    env_vars = [
        "KAFKA_BOOTSTRAP_SERVERS",
        "KAFKA_TOPIC_MATCHES",
        "KAFKA_TOPIC_PLAYERS",
        "SPARK_MASTER_URL",
        "DATA_LAKE_PATH",
        "LOG_LEVEL",
    ]

    for var in env_vars:
        value = os.getenv(var, "Not set")
        print(f"  {var:<30} = {value}")

    print("-" * 70 + "\n")


def main():
    """Main entry point."""
    print_banner()
    show_environment()

    if check_prerequisites():
        show_options()
        print("Ready to start streaming!\n")
    else:
        print("\n⚠ Please fix prerequisites before continuing.\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
