"""BigQuery I/O PySpark example."""

from pyspark.sql import SparkSession

spark = SparkSession \
  .builder \
  .appName('hive_sql') \
  .enableHiveSupport() \
  .getOrCreate()

# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the connector.
bucket = "dataproc-temp-europe-west3-946831819535-kbs6zijp"
spark.conf.set('temporaryGcsBucket', bucket)

# Perform name count.
name_count = spark.sql(
    'SELECT firstname, count(firstname) AS name_count FROM users_from_file GROUP BY firstname')
name_count.show()
name_count.printSchema()

# Saving the data to BigQuery
name_count.write.format('bigquery') \
  .option('table', 'jezioro_danych.user_names') \
  .mode('overwrite') \
  .save()