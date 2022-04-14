import sys
from operator import add

from pyspark.sql import SparkSession
import pyspark.sql.functions as f 

if __name__ == "__main__":

    spark = SparkSession.builder.appName("WordCount").getOrCreate()

    linesDF = spark.read.text(sys.argv[1])
    counts = linesDF \
        .select(f.explode(f.split("value", " ")).alias("word")) \
        .withColumn("real_words", f.regexp_extract("word", "(\\p{L}+)", 1)) \
        .drop("words") \
        .where(f.col("real_words") != "") \
        .groupBy("real_words") \
        .count() \
        .write.mode("overwrite").csv(sys.argv[2], header=True)

    spark.stop()