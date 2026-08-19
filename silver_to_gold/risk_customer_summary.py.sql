-- Databricks notebook source
--  A table named risk_customer_summary containing one summarized record for Highly risked customer each high-risk customer, including their credit score, total external loan exposure, overdue amount, and total account balance, for customers with active accounts and verified KYC.

create or replace table banking.gold.risk_customer_summary as
with customer_credit_report (
select * from banking.silver.credit_bureau_reports
where risk_grade='HIGH'),
customer_accounts (
select * from banking.silver.accounts account_status
where account_status.STATUS='ACTIVE'),
customer_details(
    select * from banking.silver.customers
    where KYC_STATUS='VERIFIED'
)
select a.customer_id,a.risk_grade,max(a.credit_score) as credit_score,sum(external_active_loans) as external_active_loans,
sum(external_overdue_amount) as external_overdue_amount,sum(BALANCE) as total_balance
from customer_credit_report a
inner join customer_accounts b
on a.CUSTOMER_ID=b.CUSTOMER_ID
inner join customer_details c
on a.CUSTOMER_ID=c.CUSTOMER_ID
group by a.customer_id,a.risk_grade;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print('risk_customer_summary')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC count=spark.sql("""SELECT COUNT(*) AS cnt FROM banking.gold.risk_customer_summary""").collect()[0]["cnt"]
-- MAGIC
-- MAGIC dbutils.notebook.exit(str(count))