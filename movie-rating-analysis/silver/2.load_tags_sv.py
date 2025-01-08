# Databricks notebook source
def readTagsBz():
    from pyspark.sql.functions import from_unixtime, col, to_date
    return spark.read.table("movie_analysis.tags_bz")\
                    .withColumnRenamed("userId", "user_id")\
                    .withColumnRenamed("movieId", "movie_id")\
                    .withColumn("timestamp", to_date(from_unixtime(col("timestamp"))))

def saveToTagsSv(df):
    df.write.mode("overwrite")\
            .format("delta")\
            .saveAsTable("movie_analysis.tags_sv")
    print("\ndata saved to tags_sv")

# COMMAND ----------

def runTagsTransformation():
    print("\nstarted tags silver transformation")
    df = readTagsBz()
    saveToTagsSv(df)
    print("\ncompleted tags silver transformation")

# COMMAND ----------

runTagsTransformation()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from movie_analysis.tags_sv
