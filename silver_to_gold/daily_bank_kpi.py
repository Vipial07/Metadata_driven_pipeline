# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC -- The process creates a consolidated Daily Banking KPI table by aggregating transaction, customer, account, and credit bureau information. It provides daily transaction volumes and values along with overall customer count, account count, total balances, average credit score, and high-risk customer count. This Gold-layer table acts as a single source of truth for business reporting, dashboarding, and operational performance analysis.
# MAGIC
# MAGIC CREATE OR REPLACE TABLE banking.gold.daily_bank_kpi AS
# MAGIC
# MAGIC WITH txn_daily AS (
# MAGIC     SELECT
# MAGIC         DATE(txn_timestamp) AS txn_date,
# MAGIC         COUNT(txn_id) AS total_transactions,
# MAGIC         SUM(amount) AS total_transaction_amount
# MAGIC     FROM banking.silver.transactions
# MAGIC     GROUP BY DATE(txn_timestamp)
# MAGIC ),
# MAGIC
# MAGIC customer_metrics AS (
# MAGIC     SELECT
# MAGIC         COUNT(DISTINCT customer_id) AS total_customers
# MAGIC     FROM banking.silver.customers
# MAGIC ),
# MAGIC
# MAGIC account_metrics AS (
# MAGIC     SELECT
# MAGIC         COUNT(account_id) AS total_accounts,
# MAGIC         SUM(balance) AS total_balance
# MAGIC     FROM banking.silver.accounts
# MAGIC ),
# MAGIC
# MAGIC credit_metrics AS (
# MAGIC     SELECT
# MAGIC         AVG(credit_score) AS avg_credit_score,
# MAGIC         SUM(
# MAGIC             CASE WHEN risk_grade='HIGH'
# MAGIC             THEN 1 ELSE 0 END
# MAGIC         ) AS high_risk_customers
# MAGIC     FROM banking.silver.credit_bureau_reports
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC t.txn_date,
# MAGIC cm.total_customers,
# MAGIC am.total_accounts,
# MAGIC am.total_balance,
# MAGIC t.total_transactions,
# MAGIC t.total_transaction_amount,
# MAGIC cr.avg_credit_score,
# MAGIC cr.high_risk_customers
# MAGIC
# MAGIC FROM txn_daily t
# MAGIC CROSS JOIN customer_metrics cm
# MAGIC CROSS JOIN account_metrics am
# MAGIC CROSS JOIN credit_metrics cr

# COMMAND ----------

print('daily_bank_kpi')

# COMMAND ----------

count=spark.sql("""select count(*) as count from banking.gold.daily_bank_kpi""").collect()[0]["count"]
dbutils.notebook.exit(str(count))