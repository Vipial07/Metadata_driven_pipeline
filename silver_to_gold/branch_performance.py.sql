-- Databricks notebook source
create schema if not exists banking.gold;

-- COMMAND ----------

create or replace table banking.gold.branch_performace as
WITH customer_branch AS (
    SELECT
        c.customer_id,
        c.branch_code
    FROM banking.silver.customers c
),
account_agg AS (
    SELECT
        a.customer_id,
        COUNT(a.account_id) AS total_accounts,
        SUM(a.balance) AS total_balance
    FROM banking.silver.accounts a
    GROUP BY a.customer_id
),
txn_agg AS (
    SELECT
        a.customer_id,
        COUNT(t.txn_id) AS total_transactions,
        SUM(t.amount) AS total_transaction_amount
    FROM banking.silver.transactions t
    JOIN banking.silver.accounts a
        ON t.account_id = a.account_id
    GROUP BY a.customer_id
)
SELECT
    b.branch_code,
    b.branch_name,
    COUNT(DISTINCT cb.customer_id) AS total_customers,
    NVL(SUM(a.total_accounts),0) AS total_accounts,
    NVL(SUM(a.total_balance),0) AS total_deposits,
    NVL(SUM(t.total_transactions),0) AS total_transactions,
    NVL(SUM(t.total_transaction_amount),0) AS total_transaction_amount
FROM banking.silver.branches b
LEFT JOIN customer_branch cb
    ON b.branch_code = cb.branch_code
LEFT JOIN account_agg a
    ON cb.customer_id = a.customer_id
LEFT JOIN txn_agg t
    ON cb.customer_id = t.customer_id
GROUP BY
b.branch_code,
b.branch_name

-- COMMAND ----------

-- MAGIC %python
-- MAGIC count=spark.sql("""SELECT COUNT(*) AS cnt FROM banking.gold.branch_performace""").collect()[0]["cnt"]
-- MAGIC
-- MAGIC dbutils.notebook.exit(str(count))