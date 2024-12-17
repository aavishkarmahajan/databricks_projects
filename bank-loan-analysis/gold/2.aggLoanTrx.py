# Databricks notebook source
def getAndAggregateFeatureLoanTrxData():
    from pyspark.sql.functions import sum, avg, max, min
    df = spark.read.table("bank_loan_analysis.tbl_feature_loan_trx")\
                    .groupBy("date", "payment_period", "currency_type", "evaluation_channel", "category")\
                    .agg(sum("loan_amount").alias("total_loan_amount"),
                         avg("loan_amount").alias("avg_loan_amount"),
                         sum("current_debt").alias("sum_current_debt"), 
                         avg("interest_rate").alias("avg_interest_rate"),
                         max("interest_rate").alias("max_interest_rate"), 
                         min("interest_rate").alias("min_interest_rate"), 
                         avg("health_score").alias("avg_health_score"), 
                         avg("monthly_salary").alias("avg_monthly_salary"))
                    
    return df

def checkAggLoanTrxTableExists():
    try:
        spark.sql("desc bank_loan_analysis.tbl_agg_loan_trx")
        return True
    except:
        return False

def createAggLoanTrxTempView(df):
    df.createOrReplaceTempView("df_agg_loan_trx_temp_view")

def saveAggLoanTrxData(df):
    if not checkAggLoanTrxTableExists():
        df.write.mode("overwrite").saveAsTable("bank_loan_analysis.tbl_agg_loan_trx")
    else:
        createAggLoanTrxTempView(df)
        merge_statement = """MERGE INTO bank_loan_analysis.tbl_agg_loan_trx t
                            USING df_agg_loan_trx_temp_view v
                            ON t.date = v.date 
                                AND t.payment_period = v.payment_period 
                                AND t.currency_type = v.currency_type 
                                AND t.evaluation_channel = v.evaluation_channel 
                                AND t.category = v.category
                            WHEN MATCHED THEN UPDATE SET 
                                t.total_loan_amount = t.total_loan_amount + v.total_loan_amount,
                                t.avg_loan_amount = t.avg_loan_amount + v.avg_loan_amount,
                                t.sum_current_debt = t.sum_current_debt + v.sum_current_debt,
                                t.avg_interest_rate = t.avg_interest_rate + v.avg_interest_rate,
                                t.max_interest_rate = t.max_interest_rate + v.max_interest_rate,
                                t.min_interest_rate = t.min_interest_rate + v.min_interest_rate,
                                t.avg_health_score = t.avg_health_score + v.avg_health_score,
                                t.avg_monthly_salary = t.avg_monthly_salary + v.avg_monthly_salary
                            WHEN NOT MATCHED THEN INSERT *
                            """
        spark.sql(merge_statement)
    print("\nsaved agg loan trx data to table")

# COMMAND ----------

def runAggLoanTrxDataCreation():
    print("\nstarted agg loan trx data creation")
    df = getAndAggregateFeatureLoanTrxData()
    saveAggLoanTrxData(df)
    print("\n completed agg loan trx data creation")

# COMMAND ----------

runAggLoanTrxDataCreation()
