-- Databricks notebook source
CREATE OR REPLACE TABLE banking.gold.transaction_channnel_summary as
select distinct trans.CHANNEL,count(*) as total_transaction,
round(sum(trans.AMOUNT),2) as total_transaction_amount
from banking.silver.transactions trans
inner join banking.silver.accounts acct
on trans.ACCOUNT_ID =acct.ACCOUNT_ID
where trans.STATUS='SUCCESS'
group by trans.CHANNEL;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC count=spark.sql("""SELECT COUNT(*) AS cnt FROM banking.gold.transaction_channnel_summary""").collect()[0]["cnt"]
-- MAGIC
-- MAGIC dbutils.notebook.exit(str(count))