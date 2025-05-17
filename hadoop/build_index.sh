set -e

if [ "$#" -lt 3 ]; then
  echo "Usage: $0 <input_files...> <output_file> <num_reducers>"
  exit 1
fi

args=("$@")
argc=${#args[@]}

num_reducers=${args[$((argc - 1))]}    # last
output_file=${args[$((argc - 2))]}     # second-last
input_files=("${args[@]:0:$((argc - 2))}")   # everything else

rm -rf /tmp/hadoop_input
mkdir -p /tmp/hadoop_input

for f in "${input_files[@]}"; do
  cp "$f" /tmp/hadoop_input/
done
echo "Building inverted index for: ${input_files[*]}  (reducers=$num_reducers)"

# HDFS prep
hdfs dfs -rm -r -f /input  || true
hdfs dfs -rm -r -f /output || true
hdfs dfs -mkdir -p /input
hdfs dfs -put /tmp/hadoop_input/* /input

# Run MapReduce job
hadoop jar invertedindex.jar org.example.invertedindex.Driver /input "$num_reducers" /output

# Merge results
mkdir -p output
hdfs dfs -getmerge /output "$output_file"
echo "Done. Output written to $output_file"
