

from pyspark.sql import SparkSession
import pyspark.sql.functions as f

spark = SparkSession \
  .builder \
  .appName('csv_app') \
  .getOrCreate()


usersDF = spark.read.option("header", "true")\
    .csv('gs://<bucket>/users/version=1/data_time=20220304/users_20220304.csv')

usersDF.printSchema()

citiesDF = spark.read.option("header", "true")\
    .csv('gs://<bucket>/cities/cities.csv')

joinDF = usersDF.join(citiesDF, usersDF.id == citiesDF.id).select("profession", "city")

finalDF = joinDF.groupBy("profession", "city").count()

finalDF.repartition(1) \
    .write \
    .mode('overwrite') \
    .option("header", True) \
    .csv('gs://<bucket>/join_result')