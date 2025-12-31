#!/usr/bin/env python3
"""
Validation script for Phase 3: Spark Structured Streaming

This script validates the implementation by checking:
1. File structure
2. Environment variables
3. Python imports
4. Spark connectivity
5. Kafka connectivity (optional)
"""

import os
import sys
from pathlib import Path
from typing import List, Tuple

# Colors for terminal output
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
RESET = '\033[0m'


def print_header(message: str):
    """Print section header."""
    print(f"\n{BLUE}{'=' * 70}{RESET}")
    print(f"{BLUE}{message.center(70)}{RESET}")
    print(f"{BLUE}{'=' * 70}{RESET}\n")


def print_success(message: str):
    """Print success message."""
    print(f"{GREEN}✓{RESET} {message}")


def print_error(message: str):
    """Print error message."""
    print(f"{RED}✗{RESET} {message}")


def print_warning(message: str):
    """Print warning message."""
    print(f"{YELLOW}⚠{RESET} {message}")


def check_file_structure() -> Tuple[bool, List[str]]:
    """Check if all required files exist."""
    print_header("CHECKING FILE STRUCTURE")

    base_path = Path("/workspaces/esport-bigdata-pipeline/spark")

    required_files = [
        "main.py",
        "README.md",
        ".env.example",
        "streaming/__init__.py",
        "streaming/match_stream.py",
        "streaming/player_stream.py",
        "schemas/__init__.py",
        "schemas/match_schema.py",
        "schemas/player_schema.py",
        "utils/__init__.py",
        "utils/spark_session.py",
        "utils/logger.py",
    ]

    missing_files = []
    for file_path in required_files:
        full_path = base_path / file_path
        if full_path.exists():
            print_success(f"Found: {file_path}")
        else:
            print_error(f"Missing: {file_path}")
            missing_files.append(str(file_path))

    success = len(missing_files) == 0
    return success, missing_files


def check_environment_variables() -> Tuple[bool, List[str]]:
    """Check if required environment variables are set."""
    print_header("CHECKING ENVIRONMENT VARIABLES")

    required_vars = {
        "KAFKA_BOOTSTRAP_SERVERS": True,  # Required
    }

    optional_vars = {
        "KAFKA_TOPIC_MATCHES": "esports-matches",
        "KAFKA_TOPIC_PLAYERS": "esports-players",
        "SPARK_MASTER_URL": "local[*]",
        "DATA_LAKE_PATH": "/workspaces/esport-bigdata-pipeline/data",
        "LOG_LEVEL": "INFO",
    }

    missing_vars = []

    # Check required variables
    for var, is_required in required_vars.items():
        value = os.getenv(var)
        if value:
            print_success(f"{var} = {value}")
        else:
            if is_required:
                print_error(f"{var} is not set (REQUIRED)")
                missing_vars.append(var)
            else:
                print_warning(f"{var} is not set (optional)")

    # Check optional variables
    for var, default in optional_vars.items():
        value = os.getenv(var)
        if value:
            print_success(f"{var} = {value}")
        else:
            print_warning(f"{var} is not set (will use default: {default})")

    success = len(missing_vars) == 0
    return success, missing_vars


def check_python_imports() -> Tuple[bool, List[str]]:
    """Check if required Python packages can be imported."""
    print_header("CHECKING PYTHON IMPORTS")

    required_packages = [
        ("pyspark", "pyspark"),
        ("pyspark.sql", "pyspark.sql"),
    ]

    failed_imports = []

    for package_name, import_name in required_packages:
        try:
            __import__(import_name)
            print_success(f"Successfully imported: {package_name}")
        except ImportError as e:
            print_error(f"Failed to import {package_name}: {str(e)}")
            failed_imports.append(package_name)

    success = len(failed_imports) == 0
    return success, failed_imports


def check_spark_modules() -> Tuple[bool, List[str]]:
    """Check if custom Spark modules can be imported."""
    print_header("CHECKING CUSTOM MODULES")

    # Add spark directory to path
    spark_dir = Path("/workspaces/esport-bigdata-pipeline/spark")
    if str(spark_dir) not in sys.path:
        sys.path.insert(0, str(spark_dir))

    modules_to_check = [
        ("schemas.match_schema", "Match schema"),
        ("schemas.player_schema", "Player schema"),
        ("utils.logger", "Logger utility"),
        ("utils.spark_session", "Spark session builder"),
    ]

    failed_imports = []

    for module_name, description in modules_to_check:
        try:
            __import__(module_name)
            print_success(
                f"Successfully imported: {description} ({module_name})")
        except Exception as e:
            print_error(f"Failed to import {description}: {str(e)}")
            failed_imports.append(module_name)

    success = len(failed_imports) == 0
    return success, failed_imports


def check_spark_session() -> bool:
    """Try to create a Spark session."""
    print_header("CHECKING SPARK SESSION CREATION")

    try:
        # Add spark directory to path
        spark_dir = Path("/workspaces/esport-bigdata-pipeline/spark")
        if str(spark_dir) not in sys.path:
            sys.path.insert(0, str(spark_dir))

        # Set minimal environment
        os.environ.setdefault("SPARK_MASTER_URL", "local[1]")
        os.environ.setdefault("LOG_LEVEL", "ERROR")

        from utils.spark_session import create_spark_session

        print("Attempting to create SparkSession...")
        spark = create_spark_session(
            app_name="ValidationTest",
            master_url="local[1]"
        )

        print_success(
            f"SparkSession created successfully (version: {spark.version})")

        # Stop the session
        spark.stop()
        print_success("SparkSession stopped successfully")

        return True

    except Exception as e:
        print_error(f"Failed to create SparkSession: {str(e)}")
        return False


def print_summary(results: dict):
    """Print validation summary."""
    print_header("VALIDATION SUMMARY")

    all_passed = True

    for check_name, (passed, details) in results.items():
        status = f"{GREEN}PASS{RESET}" if passed else f"{RED}FAIL{RESET}"
        print(f"{check_name}: {status}")

        if not passed and details:
            for detail in details:
                print(f"  - {detail}")

        all_passed = all_passed and passed

    print()
    if all_passed:
        print(f"{GREEN}{'=' * 70}{RESET}")
        print(f"{GREEN}✓ ALL CHECKS PASSED - PHASE 3 IMPLEMENTATION VALIDATED{RESET}")
        print(f"{GREEN}{'=' * 70}{RESET}")
    else:
        print(f"{RED}{'=' * 70}{RESET}")
        print(f"{RED}✗ SOME CHECKS FAILED - PLEASE FIX ISSUES ABOVE{RESET}")
        print(f"{RED}{'=' * 70}{RESET}")

    return all_passed


def main():
    """Run all validation checks."""
    print()
    print(f"{BLUE}{'*' * 70}{RESET}")
    print(f"{BLUE}Phase 3: Spark Structured Streaming - Validation Script{RESET}")
    print(f"{BLUE}{'*' * 70}{RESET}")

    results = {}

    # Run all checks
    results["File Structure"] = check_file_structure()
    results["Environment Variables"] = check_environment_variables()
    results["Python Imports"] = check_python_imports()
    results["Custom Modules"] = check_spark_modules()

    # Spark session check (only if previous checks passed)
    if results["Python Imports"][0] and results["Custom Modules"][0]:
        spark_ok = check_spark_session()
        results["Spark Session"] = (spark_ok, [])
    else:
        print_header("SKIPPING SPARK SESSION CHECK")
        print_warning("Skipped due to import failures")
        results["Spark Session"] = (
            False, ["Skipped due to previous failures"])

    # Print summary
    all_passed = print_summary(results)

    # Exit with appropriate code
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
