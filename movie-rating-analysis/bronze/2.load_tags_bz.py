# Databricks notebook source
# MAGIC %run
# MAGIC "/Workspace/Users/aavishkarm@outlook.com/Development/projects/databricks_projects/movie-rating-analysis/setup/0.common var"

# COMMAND ----------

def getSchemaTags():
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType
    schemaTags = StructType([StructField("userId", IntegerType(), True),
                            StructField("movieId", IntegerType(), True),
                            StructField("tag", StringType(), True),
                            StructField("timestamp", LongType(), True)])
    return schemaTags

def readRawTags(schemaTags):
    from pyspark.sql.functions import input_file_name, current_timestamp
    df = spark.read.format("csv")\
                    .option("header", True)\
                    .schema(schemaTags)\
                    .load(raw_data_loc_tags)\
                    .withColumn("input_file", input_file_name())\
                    .withColumn("ingested_date", current_timestamp())
    return df

def saveToTagsBz(df):
    df.write.mode("overwrite").saveAsTable("movie_analysis.tags_bz")
    print("\ndata ingested to tags_bz table")

# COMMAND ----------

def runIngestTagsRaw():
    print("\nstarted tags raw ingestion")
    schemaTags = getSchemaTags()
    df = readRawTags(schemaTags)
    saveToTagsBz(df)
    print("\ncompleted tags raw ingestion")

# COMMAND ----------

runIngestTagsRaw()
