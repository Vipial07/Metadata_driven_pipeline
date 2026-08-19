-- Databricks notebook source
create or replace table banking.gold.customer_360 as
with account_agg as(
select CUSTOMER_ID,
count(ACCOUNT_ID) as total_accounts,
sum(BALANCE) as total_balance
from banking.silver.accounts
group by customer_id
),
txn_agg as
(select a.customer_id,
count(t.txn_id) as total_txn,
sum(t.AMOUNT) as total_txn_amount
from banking.silver.transactions t
inner join banking.silver.accounts a
on t.account_id = a.account_id
group by a.customer_id
),
 credit_latest AS (
     SELECT *
     FROM (
         SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY customer_id
                    ORDER BY bureau_pull_date DESC
                ) AS rn
         FROM banking.silver.credit_bureau_reports
     )
     WHERE rn = 1
 )

 SELECT
     c.customer_id,
     CONCAT(c.first_name,' ',c.last_name) AS customer_name,
     b.branch_name,
     COALESCE(a.total_accounts,0) AS total_accounts,
     COALESCE(a.total_balance,0) AS total_balance,
     COALESCE(t.total_txn,0) AS total_transactions,
     COALESCE(t.total_txn_amount,0) AS total_transaction_amount,
     cr.credit_score,
     cr.risk_grade,
     cr.external_active_loans,
     cr.external_overdue_amount,

     CASE
         WHEN a.total_balance >= 500000 THEN 'HIGH_VALUE'
         WHEN a.total_balance >= 100000 THEN 'MEDIUM_VALUE'
         ELSE 'LOW_VALUE'
     END AS customer_segment

 FROM banking.silver.customers c

 LEFT JOIN account_agg a
     ON c.customer_id = a.customer_id

 LEFT JOIN txn_agg t
     ON c.customer_id = t.customer_id

 LEFT JOIN banking.silver.branches b
     ON c.branch_code = b.branch_code

 LEFT JOIN credit_latest cr
     ON c.customer_id = cr.customer_id

-- COMMAND ----------

-- MAGIC %python
-- MAGIC count = spark.sql("""
-- MAGIC SELECT COUNT(*) AS cnt
-- MAGIC FROM banking.gold.customer_360
-- MAGIC """).collect()[0]["cnt"]
-- MAGIC
-- MAGIC dbutils.notebook.exit(str(count))