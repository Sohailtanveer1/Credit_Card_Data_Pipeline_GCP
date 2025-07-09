from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, when, lag, lead, count, countDistinct, avg, sum, min , max
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("GCPDataPipeleine").getOrCreate()


## 1. 📊 Customer Credit Risk Score by Bureau History
bureau_df = spark.read.parquet("gs://cred_silver1/bureau")
bureau_agg = bureau_df.groupBy("SK_ID_CURR").agg(
    F.sum("AMT_CREDIT_SUM").alias("total_credit"),
    F.sum("AMT_CREDIT_SUM_DEBT").alias("total_debt")
).withColumn("bureau_debt_ratio", col("total_debt") / col("total_credit")).sort('bureau_debt_ratio', ascending =False)
bureau_agg.write.mode("append").parquet("gs://cred_gold1/bureau_agg")


## 2. 🧾 Installment Payment Behavior Index
inst_df = spark.read.parquet("gs://cred_silver1/installments_payments")
inst_pay = inst_df.withColumn("days_late", col("DAYS_ENTRY_PAYMENT") - col("DAYS_INSTALMENT"))
inst_late_avg = inst_pay.groupBy("SK_ID_CURR").agg(F.avg("days_late").alias("avg_days_late")).sort('avg_days_late',ascending = False)
inst_late_avg.write.mode("append").parquet("gs://cred_gold1/inst_late_avg")


## 3. 💼 Previous Application Behavior Profile
prev_df = spark.read.parquet("gs://cred_silver1/previous_application")
prev_app = prev_df.withColumn("app_to_credit_diff", col("AMT_APPLICATION") - col("AMT_CREDIT"))
prev_stats = prev_app.groupBy("SK_ID_CURR").agg(F.avg("app_to_credit_diff").alias("avg_diff_requested_approved")).sort('avg_diff_requested_approved',ascending = False)
prev_stats.write.mode("append").parquet("gs://cred_gold1/prev_app_stats")


## 4. 🧮 Overdue Severity Score from Bureau Balance
bureau_bal = spark.read.parquet("gs://cred_silver1/bureau_balance")
severe_bureau = bureau_bal.filter(col("STATUS") == "5")
severe_counts = severe_bureau.join(bureau_df, "SK_ID_BUREAU").groupBy("SK_ID_CURR").count().alias("severe_dpd_count")
severe_counts.write.mode("append").parquet("gs://cred_gold1/severe_bureau_counts")


## 5. 🏪 POS Loan Repayment Health
pos_df = spark.read.parquet("gs://cred_silver1/POS_CASH_balance")
pos_health = pos_df.groupBy("SK_ID_CURR").agg(F.sum("CNT_INSTALMENT_FUTURE").alias("future_pos_installments"))
pos_health.write.mode("append").parquet("gs://cred_gold1/pos_health")


## 6. 💳 Credit Card Utilization Behavior
cc_df = spark.read.parquet("gs://cred_silver1/credit_card_balance")
cc_util = cc_df.withColumn("utilization", col("AMT_DRAWINGS_CURRENT") / col("AMT_CREDIT_LIMIT_ACTUAL"))
cc_util_avg = cc_util.groupBy("SK_ID_CURR").agg(F.avg("utilization").alias("avg_cc_utilization")).sort('avg_cc_utilization',ascending = False)
cc_util_avg.write.mode("append").parquet("gs://cred_gold1/cc_utilization_avg")


## 7. 📆 Loan Purpose Default Trend
purpose_risk = prev_df.join(inst_late_avg, "SK_ID_CURR").groupBy("NAME_CASH_LOAN_PURPOSE").agg(F.avg("avg_days_late"))
purpose_risk.write.mode("append").parquet("gs://cred_gold1/purpose_risk")


## 8. 🔗 Temporal Application Patterns
temporal_pattern = prev_df.join(inst_late_avg, "SK_ID_CURR").groupBy("WEEKDAY_APPR_PROCESS_START").agg(F.avg("avg_days_late"))
temporal_pattern.write.mode("append").parquet("gs://cred_gold1/temporal_pattern")


## 9. 🔍 Client Contract Portfolio Summary
contract_mix = prev_df.groupBy("SK_ID_CURR").agg(
    F.count("SK_ID_PREV").alias("prev_contract_count"),
    F.countDistinct("NAME_CONTRACT_TYPE").alias("contract_type_diversity")
)
contract_mix.write.mode("append").parquet("gs://cred_gold1/contract_mix")

## stop spark session
spark.stop()