#!/usr/bin/env bash
set -euo pipefail
#
# Re-create local datasets and sync them to HDFS
#

# ---------- 1.  Local folders -------------------------------------------------
base=/data/dataset
for size in small medium large; do
  sudo mkdir -p   "$base/$size"
  sudo chown hadoop "$base/$size"
done

# ---------- 2.  Download / gunzip --------------------------------------------
echo "↳ downloading (or re-downloading) the three datasets …"

# 5 MB Shakespeare text
wget -qO "$base/small/shakespeare.txt" \
  https://www.gutenberg.org/ebooks/100.txt.utf-8

# ~55 MB Wikipedia stub (chunk 2)
wget -qO "$base/medium/enwiki-latest-stub-meta-current2.xml.gz" \
  https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-stub-meta-current2.xml.gz
gunzip -f "$base/medium/enwiki-latest-stub-meta-current2.xml.gz"

# ~1.8 GB Wikipedia stub (chunk 16)
wget -qO "$base/large/enwiki-latest-stub-meta-current16.xml.gz" \
  https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-stub-meta-current16.xml.gz
gunzip -f "$base/large/enwiki-latest-stub-meta-current16.xml.gz"

echo "   …downloads complete"

# ---------- 3.  Copy into HDFS -----------------------------------------------
hdfs_base=/user/hadoop/input
echo "↳ loading into HDFS under $hdfs_base/{small,medium,large}"

hdfs dfs -mkdir -p                    \
  "$hdfs_base/small"                  \
  "$hdfs_base/medium"                 \
  "$hdfs_base/large"

hdfs dfs -put -f "$base/small/shakespeare.txt"                       "$hdfs_base/small/"
hdfs dfs -put -f "$base/medium/enwiki-latest-stub-meta-current2.xml" "$hdfs_base/medium/"
hdfs dfs -put -f "$base/large/enwiki-latest-stub-meta-current16.xml" "$hdfs_base/large/"

echo "↳ verifying in HDFS …"
hdfs dfs -du -h "$hdfs_base"

echo "Done ✔  (You can now rerun your MapReduce job)"
