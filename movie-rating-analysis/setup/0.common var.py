# Databricks notebook source
raw_data_loc_movies = "/mnt/aavistorageacct/movies/raw_movies/"
raw_data_loc_ratings = "/mnt/aavistorageacct/movies/raw_ratings/"
raw_data_loc_tags = "/mnt/aavistorageacct/movies/raw_tags/"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS movie_analysis
