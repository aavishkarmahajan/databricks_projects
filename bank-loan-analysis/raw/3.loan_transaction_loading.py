# Databricks notebook source
# MAGIC %run
# MAGIC /Workspace/Users/aavishkarm@outlook.com/Development/projects/bank-loan-analysis/setup/0.common_var

# COMMAND ----------

# MAGIC %run
# MAGIC /Workspace/Users/aavishkarm@outlook.com/Development/projects/bank-loan-analysis/utils/0.utils

# COMMAND ----------

def getLoanTransactionFileSchema():
    from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DoubleType
    raw_data_schema = StructType([StructField("date", StringType(), True),
                              StructField("customerId", StringType(), True),
                              StructField("paymentPeriod", IntegerType(), True),
                              StructField("loanAmount", DoubleType(), True),
                              StructField("currencyType", StringType(), True),
                              StructField("evaluationChannel", StringType(), True),
                              StructField("interest_rate", DoubleType(), True)])
    return raw_data_schema

def readLoanTransactionFiles(loanTransactionFileProcessTimestamp, raw_loan_transaction_loc, loanTransactionFileSchema, ts):
    from pyspark.sql.functions import input_file_name, lit, to_date, col
    df = spark.read.format("csv")\
                    .option("header", True)\
                    .schema(loanTransactionFileSchema)\
                    .option("modifiedAfter", loanTransactionFileProcessTimestamp)\
                    .load(raw_loan_transaction_loc)\
                    .withColumn("inputFile", input_file_name())\
                    .withColumn("process_ts", lit(ts))\
                    .withColumn("date", to_date(col("date"), "dd/MM/yyyy"))
    return df

def saveLoanTransactionDataToTable(df):
    df.write.mode("append").saveAsTable("bank_loan_analysis.tbl_loan_transaction")
    print("\nsaved loan transaction data to table")

# COMMAND ----------

def processLoanTransactionFiles():
    print("\nstarted processing loan transaction file")
    ts = getCurrentTimestamp()
    loanTransactionFileProcessTimestamp = getWatermarkValue('BLA-read-loan-transaction-data')
    loanTransactionFileSchema = getLoanTransactionFileSchema() 
    df = readLoanTransactionFiles(loanTransactionFileProcessTimestamp, raw_loan_transaction_loc, loanTransactionFileSchema, ts)
    if (df.count() != 0):
        saveLoanTransactionDataToTable(df)
        updateWatermarkValue('BLA-read-loan-transaction-data', ts)
        print("\ncompleted processing loan transaction file")
    else:
        print("\nloan transaction file already processed")
    return

# COMMAND ----------

processLoanTransactionFiles()
