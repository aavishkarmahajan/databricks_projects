# Databricks notebook source
# MAGIC %run
# MAGIC /Workspace/Users/aavishkarm@outlook.com/Development/projects/bank-loan-analysis/setup/0.common_var

# COMMAND ----------

# MAGIC %run
# MAGIC /Workspace/Users/aavishkarm@outlook.com/Development/projects/bank-loan-analysis/utils/0.utils

# COMMAND ----------

def getCustomerDriverFileSchema():
    from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DoubleType
    raw_data_schema = StructType([StructField("date", StringType(), True),
                              StructField("customerId", StringType(), True),
                              StructField("monthly_salary", IntegerType(), True),
                              StructField("health_score", IntegerType(), True),
                              StructField("current_debt", DoubleType(), True),
                              StructField("category", StringType(), True)])
    return raw_data_schema

def readCustomerDriverFiles(custDriverFileProcessTimestamp, raw_customer_driver_data_loc, custDriverFileSchema, ts):
    from pyspark.sql.functions import input_file_name, lit
    df = spark.read.format("csv")\
                    .option("header", True)\
                    .schema(custDriverFileSchema)\
                    .option("modifiedAfter", custDriverFileProcessTimestamp)\
                    .load(raw_customer_driver_data_loc)\
                    .withColumn("inputFile", input_file_name())\
                    .withColumn("is_risk_customer", lit("False"))\
                    .withColumn("process_ts", lit(ts))
    return df

def saveCustomerDriverDataToTable(df):
    df.write.mode("append").saveAsTable("bank_loan_analysis.tbl_customer_driver")
    print("\nsaved customer driver data to table")

# COMMAND ----------

def processCustomerDriverFiles():
    print("\nstarted processing customer driver file")
    ts = getCurrentTimestamp()
    custDriverFileProcessTimestamp = getWatermarkValue('BLA-read-customer-driver-data')
    custDriverFileSchema = getCustomerDriverFileSchema() 
    df = readCustomerDriverFiles(custDriverFileProcessTimestamp, raw_customer_driver_data_loc, custDriverFileSchema, ts)
    if (df.count() != 0):
        saveCustomerDriverDataToTable(df)
        updateWatermarkValue('BLA-read-customer-driver-data', ts)
        print("\ncompleted processing customer driver file")
    else:
        print("\ncustomer driver file already processed")
    return

# COMMAND ----------

processCustomerDriverFiles()
