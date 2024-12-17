# Databricks notebook source
# MAGIC %run
# MAGIC /Workspace/Users/aavishkarm@outlook.com/Development/projects/bank-loan-analysis/setup/0.common_var

# COMMAND ----------

# MAGIC %run
# MAGIC /Workspace/Users/aavishkarm@outlook.com/Development/projects/bank-loan-analysis/utils/0.utils

# COMMAND ----------

def getCustomerDriverData(ts):
    from pyspark.sql.functions import col
    df = spark.read.table("bank_loan_analysis.tbl_customer_driver_silver")\
        .filter(col("process_ts") == ts)
    return df

def getLoanTransactionData(ts):
    from pyspark.sql.functions import col
    df = spark.read.table("bank_loan_analysis.tbl_loan_transaction_silver")\
        .filter(col("process_ts") == ts)
    return df

def createFeatureLoanTransactionData(df_cust_driver, df_loan_trx):
    df = df_loan_trx.join(df_cust_driver, df_cust_driver.customer_id == df_loan_trx.customer_id, how="left")\
                        .select(df_loan_trx.date, df_loan_trx.customer_id, df_loan_trx.payment_period, df_loan_trx.loan_amount,
                                df_loan_trx.currency_type, df_loan_trx.evaluation_channel, df_loan_trx.interest_rate,
                                df_cust_driver.monthly_salary, df_cust_driver.health_score, df_cust_driver.current_debt,
                                df_cust_driver.category, df_cust_driver.is_risk_customer)
    return df

def createfeatureLoanTrxTempView(df):
    df.createOrReplaceTempView("df_feature_loan_trx_temp_view")

def checkFeatureLoanTrxTableExists():
    try:
        spark.sql("desc bank_loan_analysis.tbl_feature_loan_trx")
        return True
    except:
        return False

def saveFeatureLoanTrxDataToTable(df):
    if not checkFeatureLoanTrxTableExists():
        df.write.mode("overwrite").saveAsTable("bank_loan_analysis.tbl_feature_loan_trx")
    else:
        createfeatureLoanTrxTempView(df)
        merge_statement = """MERGE INTO bank_loan_analysis.tbl_feature_loan_trx t
                                USING df_feature_loan_trx_temp_view v
                                ON t.customer_id = v.customer_id
                                WHEN MATCHED THEN UPDATE SET *
                                WHEN NOT MATCHED THEN INSERT *
                            """
        spark.sql(merge_statement)
    print("\nsaved feature loan trx data to table")

# COMMAND ----------

def runFeatureLoanTrxDataCreation():
    print("\nstarted feature loan trx data creation")
    ts_cd = getWatermarkValue('BLA-read-customer-driver-data')
    ts_lt = getWatermarkValue('BLA-read-loan-transaction-data')
    df_cust_driver = getCustomerDriverData(ts_cd)
    df_loan_trx = getLoanTransactionData(ts_lt)
    df_feature_loan_trx = createFeatureLoanTransactionData(df_cust_driver, df_loan_trx)
    saveFeatureLoanTrxDataToTable(df_feature_loan_trx)
    print("\n completed feature loan trx data creation")

# COMMAND ----------

runFeatureLoanTrxDataCreation()
