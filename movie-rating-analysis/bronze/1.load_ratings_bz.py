# Databricks notebook source
# MAGIC %run
# MAGIC "/Workspace/Users/aavishkarm@outlook.com/Development/projects/databricks_projects/movie-rating-analysis/setup/0.common var"

# COMMAND ----------

def getSchemaRatings():
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, LongType
    schemaRatings = StructType([StructField("userId", IntegerType(), True),
                                StructField("movieId", IntegerType(), True),
                                StructField("rating", DoubleType(), True),
                                StructField("timestamp", LongType(), True)])
    return schemaRatings

def readRawRatings(schemaRatings):
    from pyspark.sql.functions import input_file_name, current_timestamp
    df = spark.read.format("csv")\
                    .option("header", True)\
                    .schema(schemaRatings)\
                    .load(raw_data_loc_ratings)\
                    .withColumn("input_file", input_file_name())\
                    .withColumn("ingested_date", current_timestamp())
    return df

def saveToRatingsBz(df):
    df.write.mode("overwrite").saveAsTable("movie_analysis.ratings_bz")
    print("\ndata ingested to ratings_bz table")

# COMMAND ----------

def runIngestRatingsRaw():
    print("\nstarted ratings raw ingestion")
    schemaRatings = getSchemaRatings()
    df = readRawRatings(schemaRatings)
    saveToRatingsBz(df)
    print("\ncompleted ratings raw ingestion")

# COMMAND ----------

runIngestRatingsRaw()
