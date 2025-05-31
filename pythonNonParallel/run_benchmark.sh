#!/bin/bash

set -e

check_dependencies() {
  echo "Checking dependencies..."
  
  pip install psutil pandas matplotlib
}

usage() {
  echo "Usage: $0 [options]"
  echo ""
  echo "Options:"
  echo "  --datasets DIR   Directory containing datasets"
  echo "  --output DIR     Output directory for results"
  echo "  --reducers NUM   Number of reducers/partitions (default: 2)"
  echo "  --variant VAR    Hadoop variant (standard, combiner, imc) (default: combiner)"
  echo "  --help           Display this help message"
}

DATASETS=""
OUTPUT_DIR="benchmark_results"
REDUCERS=2
VARIANT="combiner"

while [[ $# -gt 0 ]]; do
  key="$1"
  
  case $key in
    --datasets)
      DATASETS="$2"
      shift # past argument
      shift # past value
      ;;
    --output)
      OUTPUT_DIR="$2"
      shift # past argument
      shift # past value
      ;;
    --reducers)
      REDUCERS="$2"
      shift # past argument
      shift # past value
      ;;
    --variant)
      VARIANT="$2"
      shift # past argument
      shift # past value
      ;;
    --help)
      usage
      exit 0
      ;;
    *)    # unknown option
      echo "Unknown option: $1"
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$DATASETS" ]]; then
  echo "ERROR: Datasets directory is required"
  usage
  exit 1
fi

mkdir -p "$OUTPUT_DIR"

check_dependencies

cp "$(dirname "$0")/inverted_index_python.py" .
cp "$(dirname "$0")/benchmark_all.py" .

echo "========================================"
echo " Starting Inverted Index Benchmarks"
echo "========================================"
echo "Datasets directory: $DATASETS"
echo "Output directory: $OUTPUT_DIR"
echo "Reducers/Partitions: $REDUCERS"
echo "Hadoop variant: $VARIANT"
echo "========================================"

python benchmark_all.py --datasets "$DATASETS" --output "$OUTPUT_DIR" --reducers "$REDUCERS" --variant "$VARIANT"

echo "========================================"
echo " Benchmark Complete"
echo "========================================"
echo "Results saved to: $OUTPUT_DIR"
echo "See $OUTPUT_DIR/benchmark_summary.html for the report"
