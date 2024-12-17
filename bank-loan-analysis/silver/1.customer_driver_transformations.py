# Databricks notebook source
# MAGIC %run
# MAGIC /Workspace/Users/aavishkarm@outlook.com/Development/projects/bank-loan-analysis/setup/0.common_var

# COMMAND ----------

# MAGIC %run
# MAGIC /Workspace/Users/aavishkarm@outlook.com/Development/projects/bank-loan-analysis/utils/0.utils

# COMMAND ----------

def readAndTransformCustomerDriver(ts):
    from pyspark.sql.functions import col, when, to_date
    df = spark.read.table("bank_loan_analysis.tbl_customer_driver")\
        .filter(col("process_ts") == ts)\
        .fillna({"monthly_salary": 1500})\
        .fillna({"health_score": 100})\
        .fillna({"current_debt": 0})\
        .fillna({"category": "OTHERS"})\
        .withColumn("is_risk_customer", when(col("health_score") < 100, "True").otherwise("False"))\
        .withColumnRenamed("customerId", "customer_id")
    return df

def createCustomerDriverSilverTempView(df):
    df.createOrReplaceTempView("df_cust_driver_silver_temp_view")

def checkCustomerDriverSilverTableExists():
    try:
        spark.sql("desc bank_loan_analysis.tbl_customer_driver_silver")
        return True
    except:
        return False

def createCustomerDriverSilverTable(df):
    if not checkCustomerDriverSilverTableExists():
        df.write.mode("overwrite").saveAsTable("bank_loan_analysis.tbl_customer_driver_silver")
    else:
        createCustomerDriverSilverTempView(df)
        merge_statement = """MERGE INTO bank_loan_analysis.tbl_customer_driver_silver t
                                USING df_cust_driver_silver_temp_view v
                                ON t.customer_id = v.customer_id
                                WHEN MATCHED THEN UPDATE SET *
                                WHEN NOT MATCHED THEN INSERT *
                            """
        spark.sql(merge_statement)
    print("\ncreated customer driver silver table")

# COMMAND ----------

def processCustomerDriverDataTransformations():
    print("\nprocessing customer driver data transformations")
    ts = getWatermarkValue('BLA-read-customer-driver-data')
    df = readAndTransformCustomerDriver(ts)
    createCustomerDriverSilverTable(df)
    print("\ncompleted customer driver data transformations")


# COMMAND ----------

processCustomerDriverDataTransformations()
