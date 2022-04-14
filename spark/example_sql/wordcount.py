"""BigQuery I/O PySpark example."""

from pyspark.sql import SparkSession

spark = SparkSession \
  .builder \
  .appName('name_count_sql') \
  .getOrCreate()

# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the connector.
bucket = "dataproc-temp-europe-west3-946831819535-kbs6zijp"
spark.conf.set('temporaryGcsBucket', bucket)

# Load data from BigQuery.
words = spark.read.format('bigquery') \
  .option('table', 'tonal-unity-343118:jezioro_danych.users_native') \
  .load()
words.createOrReplaceTempView('users')

# Perform word count.
word_count = spark.sql(
    'SELECT firstname, count(firstname) AS name_count FROM users GROUP BY firstname')
word_count.show()
word_count.printSchema()


# Saving the data to BigQuery
word_count.write.format('bigquery') \
  .option('table', 'jezioro_danych.user_names') \
  .mode('overwrite') \
  .save()