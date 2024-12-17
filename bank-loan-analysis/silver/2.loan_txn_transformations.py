# Databricks notebook source
# MAGIC %run
# MAGIC /Workspace/Users/aavishkarm@outlook.com/Development/projects/bank-loan-analysis/setup/0.common_var

# COMMAND ----------

# MAGIC %run
# MAGIC /Workspace/Users/aavishkarm@outlook.com/Development/projects/bank-loan-analysis/utils/0.utils

# COMMAND ----------

def readAndTransformLoanTxnData(ts):
    from pyspark.sql.functions import col, to_date
    df = spark.read.table("bank_loan_analysis.tbl_loan_transaction")\
        .filter(col("process_ts") == ts)\
        .withColumnRenamed("customerId", "customer_id")\
        .withColumnRenamed("paymentPeriod", "payment_period")\
        .withColumnRenamed("loanAmount", "loan_amount")\
        .withColumnRenamed("currencyType", "currency_type")\
        .withColumnRenamed("evaluationChannel", "evaluation_channel")
    return df

def createLoanTxnSilverTempView(df):
    df.createOrReplaceTempView("df_loan_txn_silver_temp_view")

def checkLoanTxnSilverTableExists():
    try:
        spark.sql("desc bank_loan_analysis.tbl_loan_transaction_silver")
        return True
    except:
        return False
    
def createLoanTxnSilverTable(df):
    if not checkLoanTxnSilverTableExists():
        df.write.mode("overwrite").saveAsTable("bank_loan_analysis.tbl_loan_transaction_silver")
    else:
        createLoanTxnSilverTempView(df)
        merge_statement = """MERGE INTO bank_loan_analysis.tbl_loan_transaction_silver t
                                USING df_loan_txn_silver_temp_view v
                                ON t.customer_id = v.customer_id
                                WHEN MATCHED THEN UPDATE SET *
                                WHEN NOT MATCHED THEN INSERT *
                            """
        spark.sql(merge_statement)
    print("\ncreated loan txn silver table")    


# COMMAND ----------

def processLoanTxnDataTransformations():
    print("\nprocessing loan txn data transformations")
    ts = getWatermarkValue('BLA-read-loan-transaction-data')
    df = readAndTransformLoanTxnData(ts)
    createLoanTxnSilverTable(df)
    print("\ncompleted loan txn data transformations")

# COMMAND ----------

processLoanTxnDataTransformations()
