# Databricks notebook source
# MAGIC %run
# MAGIC /Workspace/Users/aavishkarm@outlook.com/Development/projects/bank-loan-analysis/setup/0.common_var

# COMMAND ----------

# MAGIC %run
# MAGIC /Workspace/Users/aavishkarm@outlook.com/Development/projects/bank-loan-analysis/utils/0.utils

# COMMAND ----------

def getCustomerFileSchema():
    from pyspark.sql.types import StructType, StructField, StringType
    raw_data_schema = StructType([StructField("customerId", StringType(), True),
                              StructField("firstName", StringType(), True),
                              StructField("lastName", StringType(), True),
                              StructField("phone", StringType(), True),
                              StructField("email", StringType(), True),
                              StructField("gender", StringType(), True),
                              StructField("address", StringType(), True),
                              StructField("is_active", StringType(), True)])
    return raw_data_schema

def readCustomerFiles(custFileProcessTimestamp, raw_customer_data_loc, custFileSchema, ts):
    from pyspark.sql.functions import input_file_name, lit
    df = spark.read.format("csv")\
                    .option("header", True)\
                    .schema(custFileSchema)\
                    .option("modifiedAfter", custFileProcessTimestamp)\
                    .load(raw_customer_data_loc)\
                    .withColumn("inputFile", input_file_name())\
                    .withColumn("process_ts", lit(ts))
    return df

'''
def readCustomerFiles(custFileProcessTimestamp, raw_customer_data_loc, custFileSchema):
    from pyspark.sql.functions import input_file_name, lit
    df = spark.read.format("csv")\
                    .option("header", True)\
                    .schema(custFileSchema)\
                    .option("modifiedAfter", custFileProcessTimestamp)\
                    .load(raw_customer_data_loc)\
                    .withColumn("inputFile", input_file_name())
    return df
'''

def createCustomerTempView(df):
    df.createOrReplaceTempView("df_cust_file_temp_view")

def checkCustomerTableExists():
    try:
        spark.sql("desc bank_loan_analysis.tbl_customer")
        return True
    except:
        return False

def saveCustomerDataToTable(df):
    if not checkCustomerTableExists():
        df.write.mode("overwrite").saveAsTable("bank_loan_analysis.tbl_customer")
    else:
        createCustomerTempView(df)
        merge_statement = """MERGE INTO bank_loan_analysis.tbl_customer t
                                USING df_cust_file_temp_view v
                                ON t.customerId = v.customerId
                                WHEN MATCHED THEN UPDATE SET *
                                WHEN NOT MATCHED THEN INSERT *
                            """
        spark.sql(merge_statement)
    print("\nsaved customer data to table")


# COMMAND ----------

def processCustomerFiles():
    print("\nstarted processing customer file")
    ts = getCurrentTimestamp()
    custFileProcessTimestamp = getWatermarkValue('BLA-read-customer-data')
    custFileSchema = getCustomerFileSchema() 
    df = readCustomerFiles(custFileProcessTimestamp, raw_customer_data_loc, custFileSchema, ts)
    # df = readCustomerFiles(custFileProcessTimestamp, raw_customer_data_loc, custFileSchema)
    if (df.count() != 0):
        saveCustomerDataToTable(df)
        updateWatermarkValue('BLA-read-customer-data', ts)
        # updateWatermarkValue('BLA-read-customer-data')
        print("\ncompleted processing customer file")
    else:
        print("\ncustomer file already processed")
    return

# COMMAND ----------

processCustomerFiles()
