#!/bin/bash
set -e

usage() {
  echo "Usage: $0 [options] <input_directory> <output_file>"
  echo "Options:"
  echo "  -e, --engine <engine>     Engine to use: hadoop, spark, python, all (default: all)"
  echo "  -r, --reducers <num>      Number of reducers/partitions (default: 2)"
  echo "  -v, --variant <variant>   Hadoop variant: standard, combiner, imc (default: combiner)"
  echo "  -h, --help                Show this help message"
  echo ""
  echo "Example:"
  echo "  $0 --engine hadoop --reducers 4 --variant imc ./datasets/small ./output/small_index.txt"
}

ENGINE="all"
REDUCERS=2
VARIANT="combiner"

while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
    -e|--engine)
      ENGINE="$2"
      shift
      shift
      ;;
    -r|--reducers)
      REDUCERS="$2"
      shift
      shift
      ;;
    -v|--variant)
      VARIANT="$2"
      shift
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      break
      ;;
  esac
done

if [ $# -ne 2 ]; then
  echo "Error: Missing required arguments"
  usage
  exit 1
fi

INPUT_DIR="$1"
OUTPUT_FILE="$2"

if [ ! -d "$INPUT_DIR" ]; then
  echo "Error: Input directory does not exist: $INPUT_DIR"
  exit 1
fi

OUTPUT_DIR=$(dirname "$OUTPUT_FILE")
mkdir -p "$OUTPUT_DIR"

run_hadoop() {
  echo "=== Running Hadoop implementation ==="
  echo "Variant: $VARIANT, Reducers: $REDUCERS"
  ./build_index.sh "$INPUT_DIR" "$OUTPUT_FILE" "$REDUCERS" "$VARIANT"
}

run_spark() {
  echo "=== Running Spark implementation ==="
  echo "Partitions: $REDUCERS"
  ./build_index_spark.sh "$INPUT_DIR" "$OUTPUT_FILE" "$REDUCERS"
}

run_python() {
  echo "=== Running non-parallel Python implementation ==="
  python3 inverted_index_nonparallel.py "$INPUT_DIR" "$OUTPUT_FILE"
}

case "$ENGINE" in
  hadoop)
    run_hadoop
    ;;
  spark)
    run_spark
    ;;
  python)
    run_python
    ;;
  all)
    echo "Running all implementations sequentially"
    # Create different output files for each implementation
    HADOOP_OUTPUT="${OUTPUT_FILE%.txt}_hadoop.txt"
    SPARK_OUTPUT="${OUTPUT_FILE%.txt}_spark.txt"
    PYTHON_OUTPUT="${OUTPUT_FILE%.txt}_python.txt"
    
    run_hadoop "$HADOOP_OUTPUT"
    run_spark "$SPARK_OUTPUT"
    run_python "$PYTHON_OUTPUT"
    
    echo "All implementations completed"
    echo "Output files:"
    echo "  Hadoop: $HADOOP_OUTPUT"
    echo "  Spark: $SPARK_OUTPUT"
    echo "  Python: $PYTHON_OUTPUT"
    ;;
  *)
    echo "Error: Unknown engine '$ENGINE'"
    usage
    exit 1
    ;;
esac

echo "Done!"
