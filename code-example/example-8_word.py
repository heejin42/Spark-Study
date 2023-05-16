from pyspark.sql import *
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import *

def isCorruptedUdf(value):
    return True



# 1단계: 입력 소스 지정
spark = (SparkSession
         .builder
         .appName("CountWord")
         .getOrCreate())

spark.udf.register("isCorruptedUdf", isCorruptedUdf, BooleanType())

lines = (spark
         .readStream.format("socket")
         .option("host", "localhost")
         .option("port", 9999)
         .load())
filteredLines = lines.filter("isCorruptedUdf(value) = False")

# 2단계: 데이터 변형
#words = lines.select(split(col("value"), "\\s").alias("word"))
words = filteredLines.select(split(col("value"), "\\s").alias("word"))
counts = words.groupBy("word").count()
checkpointDir = "..."

# 3단계: 출력 싱크와 모드 결정 (전체 모드)
# 4단계: 처리 세부사항 지정
streamingQuery = (counts
                  .writeStream
                  .format("console")
                  .outputMode("complete")
                  .trigger(processingTime="1 second")
                  .option("checkpointLocation", checkpointDir)
                  .start())

# 5단계: 쿼리 시작
streamingQuery.awaitTermination()