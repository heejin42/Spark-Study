from pyspark.sql import SparkSession

spark = (SparkSession
        .builder
        .appName("SparkSQLExampleApp")
        .getOrCreate())

file = "./data/summary-data/parquet/2010-summary.parquet/"""

schema = "'data' STRING, 'delay' INT, 'distance' INT, 'origin' STRING, 'destination' STRING"

df = spark.read.format("parquet").load(file)

df.createOrReplaceTempView("us_delay_filghts_tbl")

spark.sql("SELECT * FROM us_delay_filghts_tbl").show()
