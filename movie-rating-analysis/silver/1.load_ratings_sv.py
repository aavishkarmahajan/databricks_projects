# Databricks notebook source
def readRatingsBz():
    from pyspark.sql.functions import from_unixtime, col, to_date
    return spark.read.table("movie_analysis.ratings_bz")\
                    .withColumnRenamed("userId", "user_id")\
                    .withColumnRenamed("movieId", "movie_id")\
                    .withColumn("timestamp", to_date(from_unixtime(col("timestamp"))))

def saveToRatingsSv(df):
    df.write.mode("overwrite")\
            .format("delta")\
            .saveAsTable("movie_analysis.ratings_sv")
    print("\ndata saved to ratings_sv")

# COMMAND ----------

def runRatingsTransformation():
    print("\nstarted ratings silver transformation")
    df = readRatingsBz()
    saveToRatingsSv(df)
    print("\ncompleted ratings silver transformation")

# COMMAND ----------

runRatingsTransformation()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from movie_analysis.ratings_sv
