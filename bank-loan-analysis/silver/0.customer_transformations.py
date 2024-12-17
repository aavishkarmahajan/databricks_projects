# Databricks notebook source
# MAGIC %run
# MAGIC /Workspace/Users/aavishkarm@outlook.com/Development/projects/bank-loan-analysis/utils/0.utils

# COMMAND ----------

def readAndTransformCustomerData(ts):
    from pyspark.sql.functions import col
    df = spark.read.table("bank_loan_analysis.tbl_customer")\
        .filter(col("process_ts") == ts)\
        .withColumnRenamed("customerId", "customer_id")\
        .withColumnRenamed("firstName", "first_name")\
        .withColumnRenamed("lastName", "last_name")\
        .withColumnRenamed("phone", "phone_number")\
        .withColumnRenamed("address", "customer_address")\
        .withColumnRenamed("is_active", "active_status")
    return df

def createCustomerSilverTempView(df):
    df.createOrReplaceTempView("df_cust_silver_temp_view")

def checkCustomerSilverTableExists():
    try:
        spark.sql("desc bank_loan_analysis.tbl_customer_silver")
        return True
    except:
        return False
    
def createCustomerSilverTable(df):
    if not checkCustomerSilverTableExists():
        df.write.mode("overwrite").saveAsTable("bank_loan_analysis.tbl_customer_silver")
    else:
        createCustomerSilverTempView(df)
        merge_statement = """MERGE INTO bank_loan_analysis.tbl_customer_silver t
                                USING df_cust_silver_temp_view v
                                ON t.customer_id = v.customer_id
                                WHEN MATCHED THEN UPDATE SET *
                                WHEN NOT MATCHED THEN INSERT *
                            """
        spark.sql(merge_statement)
    print("\ncreated customer silver table")    


# COMMAND ----------

def processCustomerDataTransformations():
    print("\nprocessing customer data transformations")
    ts = getWatermarkValue('BLA-read-customer-data')
    df = readAndTransformCustomerData(ts)
    createCustomerSilverTable(df)
    print("\ncompleted customer data transformations")

# COMMAND ----------

processCustomerDataTransformations()
