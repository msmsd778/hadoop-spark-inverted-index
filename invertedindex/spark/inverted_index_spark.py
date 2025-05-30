import sys
from pathlib import Path
from pyspark.sql import SparkSession
from query_index import stem, STOP_WORDS

def tokenize(text: str) -> list[str]:
    cleaned = []
    for ch in text.lower():
        cleaned.append(ch if ch.isalnum() else " ")
    return "".join(cleaned).split()

def main(inp: str, out: str, parts: int):
    spark = SparkSession.builder.appName("Spark-Inverted-Index").getOrCreate()
    sc = spark.sparkContext
    sw = sc.broadcast(STOP_WORDS)
    files = sc.wholeTextFiles(inp)
    pairs = (
        files.flatMap(lambda kv: [
            ((stem(w), Path(kv[0]).name), 1)
            for w in tokenize(kv[1])
            if w not in sw.value and stem(w)
        ])
        .reduceByKey(lambda a,b: a+b, numPartitions=parts)
    )
    word_map = pairs.map(lambda kv: (kv[0][0], f"{kv[0][1]}:{kv[1]}"))
    index = (
        word_map.groupByKey()
                .mapValues(lambda vals: "\t".join(sorted(vals)))
                .map(lambda kv: f"{kv[0]}\t{kv[1]}")
    )
    index.saveAsTextFile(out)
    spark.stop()

if __name__=="__main__":
    if len(sys.argv)!=4: sys.exit(1)
    main(sys.argv[1], sys.argv[2], int(sys.argv[3]))
