# Databricks notebook source
# MAGIC %run
# MAGIC "/Workspace/Users/aavishkarm@outlook.com/Development/projects/databricks_projects/movie-rating-analysis/setup/0.common var"

# COMMAND ----------

def getSchemaMovies():
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType
    schemaMovies = StructType([StructField("movieId", IntegerType(), True),
                                StructField("title", StringType(), True),
                                StructField("genres", StringType(), True)])
    return schemaMovies

def readRawMovies(schemaMovies):
    from pyspark.sql.functions import input_file_name
    ingested_date = spark.sql("SELECT current_timestamp()")
    ingested_date = ingested_date.collect()[0][0]
    df = spark.read.format("csv")\
                    .option("header", True)\
                    .schema(schemaMovies)\
                    .load(raw_data_loc_movies)\
                    .withColumn("input_file", input_file_name())\
                    .withColumn("ingested_date", ingested_date)
    return df

def saveToMoviesBz(df):
    df.write.mode("overwrite").saveAsTable("movie_analysis.movies_bz")
    print("\ndata ingested to movies_bz table")

# COMMAND ----------

def runIngestMoviesRaw():
    print("\nstarted movies raw ingestion")
    schemaMovies = getSchemaMovies()
    df = readRawMovies(schemaMovies)
    saveToMoviesBz(df)
    print("\ncompleted movies raw ingestion")

# COMMAND ----------

runIngestMoviesRaw()
