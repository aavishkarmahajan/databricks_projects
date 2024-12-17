# Databricks notebook source
def getWatermarkValue(process_name):
    lts = spark.read.table("bank_loan_analysis.tbl_watermark")\
                    .filter(f"process == '{process_name}'")\
                    .select("process_timestamp").collect()[0][0]
    return lts

'''
def updateWatermarkValue(process_name):
    spark.sql(f"""UPDATE bank_loan_analysis.tbl_watermark
                 SET process_timestamp = current_timestamp()
                 WHERE process = '{process_name}'""")
    print("\nupdated watermark table")
'''

def getCurrentTimestamp():
    timestamp_df = spark.sql("SELECT current_timestamp() AS current_time")
    current_time = timestamp_df.collect()[0][0]
    return current_time

def updateWatermarkValue(process_name, val):
    spark.sql(f"""UPDATE bank_loan_analysis.tbl_watermark
                 SET process_timestamp = '{val}'
                 WHERE process = '{process_name}'""")
    print("\nupdated watermark table")

