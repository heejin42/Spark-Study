from pyspark.sql import SparkSession
from pyspark.sql.functions import *
# create SparkSession
spark = (SparkSession
        .builder
        .appName("SparkSQLExampleApp")
        .getOrCreate())

# dataset path
csv_file = "./data/departuredelays.csv"

# Read data and create temporary view
df = (spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(csv_file))
df.createOrReplaceTempView("us_delay_flights_tbl")

# schema = "'data' STRING, 'delay' INT, 'origin' STRING, 'destination' STRING"
print("비행거리가 1,000마일 이상인 모든 항공편 조회")
spark.sql("""SELECT distance, origin, destination FROM us_delay_flights_tbl WHERE distance > 1000 ORDER BY distance DESC""").show(10)

print("샌프란시스코(SFO)와 시카고(ORD) 간 2시간 이상 지연이 있었던 항공편 조회 + desc")
spark.sql("""SELECT date, delay, origin, destination
FROM us_delay_flights_tbl
WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
ORDER BY delay DESC""").show(10)

print("모든 미국 항공편에 대해 지연에 대한 표시를 Flight_Delays라는 새 칼럼으로 레이블 추가")
spark.sql("""SELECT date, delay, destination,
            CASE
                WHEN delay > 360 THEN 'Very Long Delays'
                WHEN delay >= 120 AND delay <= 360 THEN 'Long Delays'
                WHEN delay >= 60 AND delay < 120 THEN 'Short Delays'
                WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
                WHEN delay = 0 Then 'No Delays'
                ELSE 'Early'
            END AS Flight_Delays
            FROM us_delay_flights_tbl
            ORDER BY origin, delay DESC""").show(10)