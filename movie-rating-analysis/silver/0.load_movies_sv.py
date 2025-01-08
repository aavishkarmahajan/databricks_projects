# Databricks notebook source
def readMoviesBz():
    return spark.read.table("movie_analysis.movies_bz")\
                    .withColumnRenamed("movieId", "movie_id")

def saveToMoviesSv(df):
    df.write.mode("overwrite")\
            .format("delta")\
            .saveAsTable("movie_analysis.movies_sv")    
    print("\ndata saved to movies_sv")


# COMMAND ----------

def runMovieTransformation():
    print("\nstarted movies silver transformation")
    df = readMoviesBz()
    saveToMoviesSv(df)
    print("\ncompleted movies silver transformation")

# COMMAND ----------

runMovieTransformation()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from movie_analysis.movies_sv
