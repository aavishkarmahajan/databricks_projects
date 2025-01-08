# Databricks notebook source
#get movies, ratings, tags silver data
def getMoviesSv():
    return spark.read.table("movie_analysis.movies_sv")\
                    .select("movie_id", "title", "genres")

def getRatingsSv():
    return spark.read.table("movie_analysis.ratings_sv")\
                    .select("user_id", "movie_id", "rating", "timestamp")

def getTagsSv():
    return spark.read.table("movie_analysis.tags_sv")\
                    .select("user_id", "movie_id", "tag", "timestamp")

#show the aggregate number of ratings per year
def aggRatingsPerYear():
    from pyspark.sql.functions import count, year
    df_ratings = getRatingsSv()
    df_ratings.groupBy(year(df_ratings.timestamp).alias("year"))\
                .agg(count(df_ratings.rating).alias("no_of_ratings"))\
                .show()

#show the ratings level distribution
def ratingLevelDistribution():
    from pyspark.sql.functions import count, year
    df_ratings = getRatingsSv()
    df_ratings.groupBy(year(df_ratings.timestamp).alias("year)"), df_ratings.rating)\
                .count()\
                .show()

#show the 18 movies that are tagged but not rated
def moviesTaggedButNotRated():
    df_movies = getMoviesSv()
    df_ratings_all = getRatingsSv()
    df_tags_all = getTagsSv()
    df_ratings = df_ratings_all.select("movie_id").distinct()
    df_tags = df_tags_all.select("movie_id").distinct()
    df_tagged_but_not_rated = df_tags.join(df_ratings, on="movie_id", how="left_anti")
    df_tagged_but_not_rated.join(df_movies, df_movies.movie_id == df_tagged_but_not_rated.movie_id)\
                            .select(df_tagged_but_not_rated.movie_id, df_movies.title)\
                            .orderBy(df_tagged_but_not_rated.movie_id)\
                            .show(truncate=False)

#Focussing on the rated untagged movies with more than 30 user ratings -->
#show the top 10 movies in terms of avg rating and no of ratings
def ratedUntaggedTop10():
    from pyspark.sql.functions import count, avg, round, desc
    df_ratings = getRatingsSv()
    df_tags = getTagsSv()
    df_rated_untagged = df_ratings.join(df_tags, on="movie_id", how="left_anti")
    df_rated_untaggedGr30 = df_rated_untagged.groupBy(df_rated_untagged.movie_id.alias("m_id"))\
                                            .count()\
                                            .filter(count("user_id")>30)
    df_rated_untagged.join(df_rated_untaggedGr30, df_rated_untagged.movie_id == df_rated_untaggedGr30.m_id, "inner")\
                    .groupBy("movie_id")\
                    .agg(round(avg("rating"),2).alias("avg_rating"), count("user_id").alias("no_of_ratings"))\
                    .orderBy(desc("avg_rating"), desc("no_of_ratings"))\
                    .limit(10)\
                    .show()

def tagStatistics():
    from pyspark.sql.functions import count, avg, round
    df_tags = getTagsSv()
    df_tags_per_movie_per_user = df_tags.groupBy("movie_id", "user_id")\
                                .agg(count("tag").alias("cnt_tags"))
    #average number of tags per movie
    df_tags_per_movie_per_user.groupBy("movie_id")\
                    .agg(round(avg("cnt_tags"),2).alias("avg_tags_per_movie"))\
                    .orderBy("movie_id")\
                    .show()

    #average number of tags per user
    df_tags_per_movie_per_user.groupBy("user_id")\
                    .agg(round(avg("cnt_tags"),2).alias("avg_tags_per_user"))\
                    .orderBy("user_id")\
                    .show()

#identify users that tagged movies without rating them
def usersWhoTaggedMoviesWithoutRating():
    df_ratings = getRatingsSv()
    df_tags = getTagsSv()
    df_tagged_not_rated = df_tags.join(df_ratings, on="movie_id", how="left_anti")\
                                .select("user_id")\
                                .distinct()\
                                .orderBy("user_id")\
                                .show()

#what is predominant(frequency based) genre per rating level
def genrePerRatingLevel():
    from pyspark.sql.functions import col, split, explode, desc, count
    df_ratings = getRatingsSv()
    df_movies = getMoviesSv()
    df_ratings_genres = df_ratings.join(df_movies, df_ratings.movie_id == df_movies.movie_id, "inner")\
                .withColumn("genres", explode(split(col("genres"), '\|')))\
                .select(df_movies.movie_id, df_ratings.rating, df_ratings.user_id, "genres")
    df_ratings_genres.groupBy("genres", "rating")\
                    .count()\
                    .orderBy(desc(count("user_id")))\
                    .show()

def tagPerGenre():
    from pyspark.sql.functions import col, split, explode, desc, count
    df_tags = getTagsSv()
    df_movies = getMoviesSv()
    df_tags_genres = df_tags.join(df_movies, df_tags.movie_id == df_movies.movie_id, "inner")\
                .withColumn("genres", explode(split(col("genres"), '\|')))\
                .select(df_movies.movie_id, df_tags.tag, df_tags.user_id, "genres")
    #what is predominant tag per genre
    df_tags_genres.groupBy("genres", "tag")\
                    .count()\
                    .orderBy(desc(count("user_id")))\
                    .show()
    #most tagged genre
    df_tags_genres.groupBy("genres").count().orderBy(desc(count("tag"))).show()

#What are the most predominant(popularity based) movies
def mostPredominantMovies():
    from pyspark.sql.functions import desc, count
    df_rating = getRatingsSv()
    df_movies = getMoviesSv()
    df_movies_rating = df_movies.join(df_rating, df_rating.movie_id == df_movies.movie_id, "inner")\
                                .select(df_movies.movie_id, df_movies.title, df_rating.user_id, df_rating.rating)\
                                .groupBy(df_movies.movie_id, df_movies.title, df_rating.rating)\
                                .count()\
                                .orderBy(desc("count"))\
                                .show()
#Top 10 movies in terms of avg rating(provided more than 30 users reviewed them)
def top10Movies():
    from pyspark.sql.functions import desc, count, col, avg, round
    df_movies = getMoviesSv()
    df_rating = getRatingsSv()
    df_ratedMoreThan30U = df_rating.groupBy("movie_id")\
                                    .agg(count("user_id").alias("cnt_users_rated_by"))\
                                    .filter(count("user_id")>30)
    df_rm30 = df_ratedMoreThan30U.alias("rm30")
    dfr = df_rating.alias("r")
    df_movies_rating = df_rm30.join(dfr, dfr.movie_id == df_rm30.movie_id, "inner")\
                                .select(col("rm30.movie_id"), col("r.rating"))
    df_movies_rating.groupBy("movie_id")\
                    .agg(round(avg("rating"),2).alias("avg_rating"))\
                    .join(df_movies, df_movies.movie_id == df_movies_rating.movie_id, "inner")\
                    .select(df_rating.movie_id, df_movies.title, "avg_rating")\
                    .orderBy(desc("avg_rating"))\
                    .show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC Calling the data analysis functions to test the output data

# COMMAND ----------

aggRatingsPerYear()
  

# COMMAND ----------

ratingLevelDistribution()

# COMMAND ----------

moviesTaggedButNotRated()

# COMMAND ----------

ratedUntaggedTop10()

# COMMAND ----------

tagStatistics()

# COMMAND ----------

usersWhoTaggedMoviesWithoutRating()

# COMMAND ----------

genrePerRatingLevel()

# COMMAND ----------

tagPerGenre()

# COMMAND ----------

mostPredominantMovies()

# COMMAND ----------

top10Movies()
