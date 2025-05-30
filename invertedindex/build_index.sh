#!/usr/bin/env bash
set -euo pipefail    # fail fast & show undefined-var bugs

# ------------------------------------------------------------
# Usage: build_index.sh  <dataset1 … datasetN>  <output_file> \
#                        <num_reducers>  <variant>
# ------------------------------------------------------------
if [ "$#" -lt 4 ]; then
  echo "Usage: $0 <input_files...> <output_file> <num_reducers> <variant>" >&2
  exit 1
fi

# ------------------------- split positional arguments
args=("$@")
argc=${#args[@]}

variant="${args[$((argc-1))]}"
num_reducers="${args[$((argc-2))]}"
output_file="${args[$((argc-3))]}"
input_files=("${args[@]:0:$((argc-3))}")   # all but the last 3

# ----------------------------- stage input locally & to HDFS
stage="/tmp/hadoop_input_$$"
rm -rf "$stage"
mkdir  -p "$stage"
for f in "${input_files[@]}"; do
  cp "$f" "$stage/"
done

hdfs dfs -rm -r -f /input /output  || true
hdfs dfs -mkdir /input
hdfs dfs -put   "$stage"/*  /input

# ------------------------------ launch the MapReduce driver
echo "Running Hadoop job   reducers=$num_reducers   variant=$variant"
hadoop jar invindex.jar /input "$num_reducers" /output "$variant"

# ----------------------------------------- fetch the result
mkdir -p "$(dirname "$output_file")"
tmp_out=$(mktemp "/tmp/invindex_merge_XXXXXX")   # tmp outside ‘output/’
echo "Saving HDFS /output  ?  $output_file"
hdfs dfs -getmerge /output "$tmp_out"
mv -f "$tmp_out" "$output_file"

echo "Output saved to $output_file"
rm -rf "$stage"
