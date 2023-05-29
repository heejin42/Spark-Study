from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import OneHotEncoder, StringIndexer, RFormula
from pyspark.ml.evaluation import RegressionEvaluator

from pyspark.ml.regression import DecisionTreeRegressor, RandomForestRegressor
import pandas as pd

from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

if __name__ == "__main__":
    
    spark = SparkSession.builder \
        .appName('pratice_ml') \
        .config('spark.some.config.option', 'some-value') \
        .getOrCreate()

    filepath = """./code-example/data/sf-airbnb/sf-airbnb-clean.parquet/"""
    airbnbDF = spark.read.parquet(filepath)
    airbnbDF.select("neighbourhood_cleansed", "room_type", "bedrooms", "bathrooms", "number_of_reviews", "price").show(10)
    
    trainDF, testDF = airbnbDF.randomSplit([.8, .2], seed=42)
    print(f"There are {trainDF.count()} rows in the training set, and {testDF.count()} in the test set")

    vecAssembler = VectorAssembler(
        inputCols=['bathrooms'], outputCol="features")
    vecTrainDF = vecAssembler.transform(trainDF)
    vecTrainDF.select("bathrooms", "features", "price").show(10)
    
    
    lr = LinearRegression(featuresCol="features", labelCol="price")
    lrModel = lr.fit(vecTrainDF)
    m = round(lrModel.coefficients[0], 2)
    b = round(lrModel.intercept, 2)
    print(f"""The formula for the linear regression line is price = {m}*bathrooms + {b}""")