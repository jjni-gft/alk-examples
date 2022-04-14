import sys
from operator import add

from pyspark.sql import SparkSession
import pyspark.sql.functions as f 

if __name__ == "__main__":

    spark = SparkSession.builder.appName("WordCount").getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    counts = lines \
        .flatMap(lambda x: x.split(' ')) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(add) \
        .toDF() \
        .write.mode("overwrite").option('header', True).csv(sys.argv[2])

    spark.stop()