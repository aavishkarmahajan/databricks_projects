# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE bank_loan_analysis;
# MAGIC
# MAGIC USE bank_loan_analysis;
# MAGIC
# MAGIC CREATE TABLE bank_loan_analysis.tbl_watermark(
# MAGIC     process varchar(100),
# MAGIC     process_timestamp timestamp
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bank_loan_analysis.tbl_watermark VALUES("BLA-read-customer-data", current_timestamp());

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bank_loan_analysis.tbl_watermark VALUES("BLA-read-customer-driver-data", current_timestamp());

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bank_loan_analysis.tbl_watermark

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC delete from bank_loan_analysis.tbl_watermark

# COMMAND ----------

# MAGIC %sql
# MAGIC select customerId, count(*) from bank_loan_analysis.tbl_customer
# MAGIC group by customerId
# MAGIC having count(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bank_loan_analysis.tbl_customer where customerId ='CUS96966287260'

# COMMAND ----------

# MAGIC %sql
# MAGIC select inputFile,process_ts, count(*) from bank_loan_analysis.tbl_customer
# MAGIC group by inputFile, process_ts
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select inputFile, process_ts, count(*) from bank_loan_analysis.tbl_customer_driver
# MAGIC group by inputFile, process_ts

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bank_loan_analysis.tbl_customer_driver where customerId = 'CUS77289220724' -- has curr_dbt null -- need 0

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bank_loan_analysis.tbl_customer_driver where monthly_salary is NULL
# MAGIC /*CUS79734737248
# MAGIC CUS60715947236
# MAGIC CUS21688984060*/

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bank_loan_analysis.tbl_customer_driver where customerId = 'CUS61363880324' --has category null

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bank_loan_analysis.tbl_customer_driver where health_score is null

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bank_loan_analysis.tbl_watermark VALUES("BLA-read-loan-transaction-data", current_timestamp());

# COMMAND ----------

# MAGIC %sql
# MAGIC select inputFile, process_ts, count(*) from bank_loan_analysis.tbl_loan_transaction
# MAGIC group by inputFile, process_ts

# COMMAND ----------

# MAGIC %md
# MAGIC DROP TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table bank_loan_analysis.tbl_customer

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table bank_loan_analysis.tbl_customer_driver

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table bank_loan_analysis.tbl_loan_transaction

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists bank_loan_analysis.tbl_customer_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table bank_loan_analysis.tbl_customer_driver_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table bank_loan_analysis.tbl_loan_transaction_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table bank_loan_analysis.tbl_feature_loan_trx

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table bank_loan_analysis.tbl_agg_loan_trx

# COMMAND ----------

# MAGIC %sql
# MAGIC select inputFile, process_ts, count(*) from bank_loan_analysis.tbl_customer_silver
# MAGIC group by inputFile, process_ts

# COMMAND ----------

# MAGIC %sql
# MAGIC select inputFile, process_ts, count(*) from bank_loan_analysis.tbl_customer_driver_silver
# MAGIC group by inputFile, process_ts

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists bank_loan_analysis.tbl_loan_transaction_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC select inputFile, process_ts, count(*) from bank_loan_analysis.tbl_loan_transaction_silver
# MAGIC group by inputFile, process_ts

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table bank_loan_analysis.tbl_customer_driver_silver
