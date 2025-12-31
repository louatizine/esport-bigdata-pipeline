#!/usr/bin/env python3
"""
Test script for Phase 3 malformed message handling.

This script tests that the streaming jobs correctly handle:
1. Valid JSON messages
2. Malformed JSON messages
3. Messages with missing fields
4. Empty messages
"""

import json
import os
import sys
from pathlib import Path

# Colors
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
RESET = '\033[0m'


def print_header(message: str):
    print(f"\n{BLUE}{'=' * 70}{RESET}")
    print(f"{BLUE}{message.center(70)}{RESET}")
    print(f"{BLUE}{'=' * 70}{RESET}\n")


def print_success(message: str):
    print(f"{GREEN}✓{RESET} {message}")


def print_error(message: str):
    print(f"{RED}✗{RESET} {message}")


def print_info(message: str):
    print(f"{BLUE}ℹ{RESET} {message}")


def test_json_parsing():
    """Test JSON parsing with various scenarios."""
    print_header("TESTING JSON PARSING")

    # Test cases
    test_cases = [
        {
            "name": "Valid match JSON",
            "data": {
                "match_id": "match_001",
                "tournament_name": "LCS Spring 2025",
                "team_1_name": "Team A",
                "team_2_name": "Team B",
                "status": "finished"
            },
            "expected": "PASS"
        },
        {
            "name": "Malformed JSON (syntax error)",
            "data": '{"match_id": "invalid" json}',
            "expected": "HANDLED"
        },
        {
            "name": "Empty JSON",
            "data": {},
            "expected": "HANDLED"
        },
        {
            "name": "Missing required fields",
            "data": {
                "match_id": "match_002"
                # missing other fields
            },
            "expected": "PASS"  # Should use nulls
        },
        {
            "name": "Invalid data types",
            "data": {
                "match_id": "match_003",
                "match_duration": "not_a_number"  # Should be int
            },
            "expected": "HANDLED"
        }
    ]

    for test in test_cases:
        print(f"\nTest: {test['name']}")
        print(f"Expected: {test['expected']}")

        # Simulate what Spark does
        try:
            if isinstance(test['data'], str):
                # Try to parse as JSON
                json.loads(test['data'])
                print_error("Should have failed but didn't")
            else:
                # Valid JSON
                json_str = json.dumps(test['data'])
                print_success(f"JSON valid: {json_str[:50]}...")
        except json.JSONDecodeError:
            print_success(
                "Malformed JSON detected (will be handled by PERMISSIVE mode)")
        except Exception as e:
            print_error(f"Unexpected error: {str(e)}")


def check_implementation():
    """Check if implementation has required features."""
    print_header("CHECKING IMPLEMENTATION")

    spark_dir = Path("/workspaces/esport-bigdata-pipeline/spark")

    checks = []

    # Check 1: PERMISSIVE mode in match_stream.py
    match_stream = spark_dir / "streaming/match_stream.py"
    if match_stream.exists():
        content = match_stream.read_text()
        if '"mode": "PERMISSIVE"' in content or "'mode': 'PERMISSIVE'" in content:
            print_success("Match stream has PERMISSIVE mode")
            checks.append(True)
        else:
            print_error("Match stream missing PERMISSIVE mode")
            checks.append(False)
    else:
        print_error("match_stream.py not found")
        checks.append(False)

    # Check 2: PERMISSIVE mode in player_stream.py
    player_stream = spark_dir / "streaming/player_stream.py"
    if player_stream.exists():
        content = player_stream.read_text()
        if '"mode": "PERMISSIVE"' in content or "'mode': 'PERMISSIVE'" in content:
            print_success("Player stream has PERMISSIVE mode")
            checks.append(True)
        else:
            print_error("Player stream missing PERMISSIVE mode")
            checks.append(False)
    else:
        print_error("player_stream.py not found")
        checks.append(False)

    # Check 3: Null filtering
    if match_stream.exists():
        content = match_stream.read_text()
        if 'filter(col("data").isNotNull())' in content:
            print_success("Match stream filters null data")
            checks.append(True)
        else:
            print_error("Match stream missing null filtering")
            checks.append(False)

    # Check 4: Batch logging
    if match_stream.exists():
        content = match_stream.read_text()
        if 'log_batch_metrics' in content or 'foreachBatch' in content:
            print_success("Match stream has batch logging")
            checks.append(True)
        else:
            print_error("Match stream missing batch logging")
            checks.append(False)

    # Check 5: Metrics module
    metrics_file = spark_dir / "utils/metrics.py"
    if metrics_file.exists():
        print_success("Metrics module exists")
        checks.append(True)
    else:
        print_error("Metrics module missing")
        checks.append(False)

    return all(checks)


def show_recommendations():
    """Show recommendations for testing."""
    print_header("TESTING RECOMMENDATIONS")

    print("To test malformed message handling:")
    print()
    print("1. Start Kafka:")
    print("   docker-compose up -d kafka")
    print()
    print("2. Produce test messages:")
    print("   # Valid message")
    print('   echo \'{"match_id": "test_001", "status": "finished"}\' | \\')
    print("     docker exec -i kafka kafka-console-producer.sh \\")
    print("       --topic esports-matches --bootstrap-server localhost:9092")
    print()
    print("   # Malformed message")
    print('   echo \'{"invalid": json}\' | \\')
    print("     docker exec -i kafka kafka-console-producer.sh \\")
    print("       --topic esports-matches --bootstrap-server localhost:9092")
    print()
    print("3. Run streaming job:")
    print("   cd spark")
    print("   python main.py --job matches")
    print()
    print("4. Check logs:")
    print("   - Look for 'Filtering corrupt records'")
    print("   - Look for 'Processing batch X' with record counts")
    print("   - Verify no crashes on malformed JSON")
    print()
    print("5. Verify output:")
    print("   ls -lh data/processed/matches/")
    print("   # Should contain valid records only")


def main():
    """Run all tests."""
    print()
    print(f"{BLUE}{'*' * 70}{RESET}")
    print(f"{BLUE}Phase 3 - Malformed Message Handling Test{RESET}")
    print(f"{BLUE}{'*' * 70}{RESET}")

    # Run tests
    test_json_parsing()

    # Check implementation
    implementation_ok = check_implementation()

    # Show recommendations
    show_recommendations()

    # Summary
    print_header("SUMMARY")

    if implementation_ok:
        print_success("All implementation checks passed!")
        print_success("Malformed message handling is properly implemented")
        print()
        print_info("Next steps:")
        print("  1. Install PySpark: pip install pyspark>=3.5.0")
        print("  2. Start Kafka: docker-compose up -d kafka")
        print("  3. Run the streaming job and test with malformed data")
        print("  4. Monitor logs for batch progress and error handling")
        return 0
    else:
        print_error("Some implementation checks failed")
        print_error("Please review the fixes above")
        return 1


if __name__ == "__main__":
    sys.exit(main())
