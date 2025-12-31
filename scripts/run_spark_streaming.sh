#!/bin/bash

###############################################################################
# Spark Streaming Job Runner
#
# This script helps run Spark Structured Streaming jobs using spark-submit
# with proper configurations and package dependencies.
###############################################################################

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default values
JOB_TYPE="all"
SPARK_MASTER="${SPARK_MASTER_URL:-local[*]}"
DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-2g}"
EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-2g}"
KAFKA_PACKAGE="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"

# Script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
SPARK_DIR="$PROJECT_ROOT/spark"

###############################################################################
# Functions
###############################################################################

print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

show_usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Run Spark Structured Streaming jobs for esports analytics.

OPTIONS:
    -j, --job TYPE          Job type: matches, players, all (default: all)
    -m, --master URL        Spark master URL (default: \$SPARK_MASTER_URL or local[*])
    -d, --driver-mem MEM    Driver memory (default: 2g)
    -e, --executor-mem MEM  Executor memory (default: 2g)
    -a, --await             Await termination (default: false)
    -h, --help              Show this help message

EXAMPLES:
    # Run all streaming jobs
    $0 --job all

    # Run only match streaming
    $0 --job matches

    # Run with custom memory settings
    $0 --job all --driver-mem 4g --executor-mem 4g

    # Run and await termination
    $0 --job all --await

ENVIRONMENT VARIABLES:
    SPARK_MASTER_URL            Spark master URL
    KAFKA_BOOTSTRAP_SERVERS     Kafka bootstrap servers (required)
    KAFKA_TOPIC_MATCHES         Matches topic name
    KAFKA_TOPIC_PLAYERS         Players topic name
    DATA_LAKE_PATH              Data lake base path
    LOG_LEVEL                   Logging level (INFO, DEBUG, etc.)

EOF
}

check_prerequisites() {
    print_info "Checking prerequisites..."

    # Check if spark-submit is available
    if ! command -v spark-submit &> /dev/null; then
        print_error "spark-submit not found. Please install Apache Spark."
        exit 1
    fi

    # Check if Python is available
    if ! command -v python3 &> /dev/null; then
        print_error "python3 not found. Please install Python 3."
        exit 1
    fi

    # Check if main.py exists
    if [ ! -f "$SPARK_DIR/main.py" ]; then
        print_error "main.py not found at $SPARK_DIR/main.py"
        exit 1
    fi

    # Check if KAFKA_BOOTSTRAP_SERVERS is set
    if [ -z "$KAFKA_BOOTSTRAP_SERVERS" ]; then
        print_warn "KAFKA_BOOTSTRAP_SERVERS not set. Using default: localhost:9092"
        export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
    fi

    print_info "Prerequisites check passed"
}

run_spark_job() {
    print_info "Starting Spark Structured Streaming job..."
    print_info "Job type: $JOB_TYPE"
    print_info "Spark master: $SPARK_MASTER"
    print_info "Driver memory: $DRIVER_MEMORY"
    print_info "Executor memory: $EXECUTOR_MEMORY"
    print_info "Kafka servers: $KAFKA_BOOTSTRAP_SERVERS"

    # Build spark-submit command
    SPARK_SUBMIT_CMD="spark-submit \
        --master $SPARK_MASTER \
        --driver-memory $DRIVER_MEMORY \
        --executor-memory $EXECUTOR_MEMORY \
        --packages $KAFKA_PACKAGE \
        --conf spark.streaming.stopGracefullyOnShutdown=true \
        --conf spark.sql.streaming.schemaInference=false \
        --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
        --conf spark.sql.shuffle.partitions=${SPARK_SHUFFLE_PARTITIONS:-10} \
        $SPARK_DIR/main.py \
        --job $JOB_TYPE"

    # Add await flag if specified
    if [ "$AWAIT_TERMINATION" = true ]; then
        SPARK_SUBMIT_CMD="$SPARK_SUBMIT_CMD --await-termination"
    fi

    print_info "Executing: $SPARK_SUBMIT_CMD"
    echo ""

    # Execute spark-submit
    eval $SPARK_SUBMIT_CMD
}

run_python_job() {
    print_info "Starting Python Structured Streaming job..."
    print_info "Job type: $JOB_TYPE"

    cd "$SPARK_DIR"

    PYTHON_CMD="python3 main.py --job $JOB_TYPE"

    if [ "$AWAIT_TERMINATION" = true ]; then
        PYTHON_CMD="$PYTHON_CMD --await-termination"
    fi

    print_info "Executing: $PYTHON_CMD"
    echo ""

    eval $PYTHON_CMD
}

###############################################################################
# Main
###############################################################################

# Parse arguments
AWAIT_TERMINATION=false
USE_SPARK_SUBMIT=true

while [[ $# -gt 0 ]]; do
    case $1 in
        -j|--job)
            JOB_TYPE="$2"
            shift 2
            ;;
        -m|--master)
            SPARK_MASTER="$2"
            shift 2
            ;;
        -d|--driver-mem)
            DRIVER_MEMORY="$2"
            shift 2
            ;;
        -e|--executor-mem)
            EXECUTOR_MEMORY="$2"
            shift 2
            ;;
        -a|--await)
            AWAIT_TERMINATION=true
            shift
            ;;
        -p|--python)
            USE_SPARK_SUBMIT=false
            shift
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Validate job type
if [[ ! "$JOB_TYPE" =~ ^(matches|players|all)$ ]]; then
    print_error "Invalid job type: $JOB_TYPE"
    print_error "Valid options: matches, players, all"
    exit 1
fi

# Run the job
check_prerequisites

if [ "$USE_SPARK_SUBMIT" = true ]; then
    run_spark_job
else
    run_python_job
fi

print_info "Job completed"
