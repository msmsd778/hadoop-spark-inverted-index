# Inverted Index with Hadoop MapReduce and Apache Spark

This project builds an **inverted index** for a set of documents using two big data processing frameworks:
- Apache Hadoop MapReduce
- Apache Spark
- Non-parallel Approach
  
An inverted index maps each word to the list of documents it appears in, along with word counts per document.

---

## 🚀 Usage

### 🔧 Prerequisites

- Java 8+
- Hadoop 3.x with YARN
- Spark 3.x
- Python 3 with `pyspark`
- `maven` for building the JAR
- HDFS & YARN must be running
- Flask (if using the web UI)

---

### 🔨 1. Compile the Hadoop Job

```bash
cd invertedindex
mvn clean package
cp target/invertedindex-1.0-shaded.jar invindex.jar
```

---

### 🏃 2. Run with Hadoop

```bash
bash build_index.sh datasets/doc1.txt datasets/doc2.txt output/result.txt 2 combiner
```

Parameters:
- Input files: Any number of local `.txt` files
- Output file: Local file where final index is saved
- Reducers: Number of reducers to use
- Variant: `combiner` or `imc` (Improved Mapper Combiner)

---

### ⚡ 3. Run with Spark

```bash
bash build_index_spark.sh datasets/doc1.txt datasets/doc2.txt output/result.txt 2
```

Parameters:
- Input files: Any number of local `.txt` files
- Output file: Local file where final index is saved
- Partitions: Number of partitions

---

### 🌐 4. (Optional) Run Web UI

```bash
python3 app.py
```

Access at: `http://localhost:5000`

Use the form to select files and variant, and it will launch Hadoop/Spark jobs via a Flask interface.

---

## 📊 Memory & Performance Statistics

After running Hadoop jobs, visit the **ResourceManager UI**:

```
http://<your-host>:8088
```

Then:
1. Click on your Application ID
2. Navigate to the **Counters** tab
3. Extract metrics like:
   - Physical/Virtual Memory
   - Peak Map/Reduce Memory
   - Total Execution Time

You can use these stats to compare different variants or system performance.

---

## 🧹 Cleanup Tips

To free up space:

```bash
# YARN temp
sudo rm -rf /opt/yarn/local/*

# Spark temp
sudo rm -rf /tmp/*
sudo rm -rf /opt/spark/work/*

# HDFS old files
hdfs dfs -rm -r /input /output
```

---

## 📄 License

MIT License

---

## 🙌 Acknowledgements

This project was developed as part of the **Cloud Computing** course at the University of Pisa.
