#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 3 ]; then
  echo "Usage: $0 <input_files...> <output_file> <num_partitions>" >&2
  exit 1
fi

args=("$@")
n=${#args[@]}
parts="${args[$((n-1))]}"
out="${args[$((n-2))]}"
inputs=("${args[@]:0:$((n-2))}")

stage="/tmp/spark_input_$$"
in_hdfs="/spark_input_$$"
out_hdfs="/spark_output_$$"

echo "[PREP]"
rm -rf "$stage"; mkdir -p "$stage"
cp "${inputs[@]}" "$stage/"

echo "[HDFS]"
hdfs dfs -rm -r -f "$in_hdfs" "$out_hdfs" || true
hdfs dfs -mkdir "$in_hdfs"
hdfs dfs -put "$stage"/* "$in_hdfs"

echo "[PKG]"
zip -j deps.zip query_index.py

echo "[SPARK]"
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --name InvertedIndexSpark \
  --py-files deps.zip \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON="$(command -v python3)" \
  --conf spark.executorEnv.PYSPARK_PYTHON="$(command -v python3)" \
  spark/inverted_index_spark.py \
  "$in_hdfs" "$out_hdfs" "$parts"

echo "[FETCH]"
mkdir -p "$(dirname "$out")"
tmp=$(mktemp /tmp/merge_XXXX)
hdfs dfs -getmerge "$out_hdfs" "$tmp"
mv -f "$tmp" "$out"

echo "[CLEAN]"
hdfs dfs -rm -r -f "$in_hdfs" "$out_hdfs"
rm -rf "$stage" deps.zip

echo "[DONE] ? $out"
