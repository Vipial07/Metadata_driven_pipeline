-- Databricks notebook source
CREATE CATALOG IF NOT EXISTS banking;

-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS banking.metadata;

-- ==================
-- INSERT INTO metadata.tables
-- ===================

CREATE TABLE IF NOT EXISTS banking.metadata.tables (
    table_id            INT,
    table_name          STRING,
    source_system       STRING,        -- oracledb / blob
    source_schema       STRING,        -- dbo (null for blob)
    schema_list STRING,
    source_table        STRING,        -- table name (null for blob)
    source_path         STRING,        -- blob path (null for sqlserver)
    target_layer        STRING,        -- silver/gold
    bronze_schema       STRING,        -- bronze
    silver_schema       STRING,        -- silver
    gold_schema         STRING,        -- gold
    active_flag         BOOLEAN,
    load_order          INT,
    created_at          TIMESTAMP
)
USING DELTA;

INSERT INTO banking.metadata.tables VALUES
(1, 'customers', 'oracledb', 'banking','CUSTOMER_ID, FIRST_NAME, LAST_NAME, DATE_OF_BIRTH, PAN_NUMBER, EMAIL, PHONE_NUMBER, KYC_STATUS, BRANCH_CODE', 'customers', NULL, 'silver', 'bronze', 'silver', NULL, TRUE, 1, current_timestamp());

INSERT INTO banking.metadata.tables VALUES
(2, 'accounts', 'sqlserver', 'banking','ACCOUNT_ID, CUSTOMER_ID, ACCOUNT_TYPE, BALANCE, CURRENCY, BRANCH_CODE, STATUS, OPENED_DAT, CREATED_AT, UPDATED_AT', 'accounts', NULL, 'silver', 'bronze', 'silver', NULL, TRUE, 2, current_timestamp());
INSERT INTO banking.metadata.tables VALUES
(3, 'transactions', 'sqlserver', 'banking','TXN_ID, ACCOUNT_ID, TXN_TYPE, AMOUNT, TXN_TIMESTAMP, CHANNEL, STATUS', 'transactions', NULL, 'silver', 'bronze', 'silver', NULL, TRUE, 3, current_timestamp());

INSERT INTO banking.metadata.tables VALUES
(4, 'branches', 'oracledb', 'banking',' BRANCH_CODE,BRANCH_NAME,CITY,STATE,REGION,CREATED_AT' ,'branches', NULL, 'silver', 'bronze', 'silver', NULL, TRUE, 4, current_timestamp())
COMMIT;

-- 5. Credit Bureau Reports (Blob CSV)
INSERT INTO banking.metadata.tables VALUES
(5, 'credit_bureau_reports', 'blob','banking','CUSTOMER_ID,CREDIT_SCORE,RISK_GRADE,EXTERNAL_ACTIVE_LOANS,EXTERNAL_OVERDUE_AMOUNT,BUREAU_PULL_DATE',null,
 '/Volumes/banking/source/blob_file/credit_bureau_reports/', 'silver',
 'bronze', 'silver', NULL, TRUE, 5, current_timestamp());

-- 6. Payment Gateway Logs (Blob CSV)
INSERT INTO banking.metadata.tables VALUES
(6, 'payment_gateway_logs', 'blob','banking','TXN_ID,GATEWAY_NAME,GATEWAY_STATUS,RESPONSE_CODE,PROCESSING_TIME_MS,DEVICE_TYPE,GEO_LOCATION,PROCESSED_TIMESTAMP', NULL,
 '/Volumes/banking/source/blob_file/payment_gateway_logs/', 'silver',
 'bronze', 'silver', NULL, TRUE, 6, current_timestamp());


SELECT DISTINCT * FROM banking.metadata.tables;



 

-- ==================
-- INSERT INTO metadata.table_parameters
-- ===================

CREATE TABLE IF NOT EXISTS banking.metadata.table_parameters (
    table_id            INT,
    parameter_name      STRING,        -- load_type / primary_key / watermark_column
    parameter_value     STRING,
    created_at          TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

INSERT INTO banking.metadata.table_parameters VALUES

-- ================= CUSTOMERS =================
(1, 'load_type', 'MERGE', current_timestamp()),
(1, 'primary_key', 'customer_id', current_timestamp()),
(1, 'watermark_column', 'updated_at', current_timestamp());

-- COMMAND ----------

select distinct * from banking.metadata.table_parameters where table_id=1;

-- COMMAND ----------

-- ================= ACCOUNTS =================
INSERT INTO banking.metadata.table_parameters VALUES
(2, 'load_type', 'MERGE', current_timestamp()),
(2, 'primary_key', 'account_id', current_timestamp()),
(2, 'watermark_column', 'updated_at', current_timestamp());

-- ================= TRANSACTIONS =================
INSERT INTO banking.metadata.table_parameters VALUES
(3, 'load_type', 'APPEND', current_timestamp()),
(3, 'primary_key', 'txn_id', current_timestamp()),
(3, 'watermark_column', 'txn_timestamp', current_timestamp());

-- ================= BRANCHES =================
INSERT INTO banking.metadata.table_parameters VALUES
(4, 'load_type', 'FULL', current_timestamp()),
(4, 'primary_key', 'branch_code', current_timestamp());

-- ================= CREDIT BUREAU REPORTS =================
INSERT INTO banking.metadata.table_parameters VALUES
(5, 'load_type', 'MERGE', current_timestamp()),
(5, 'primary_key', 'customer_id', current_timestamp()),
(5, 'watermark_column', 'bureau_pull_date', current_timestamp());

-- ================= PAYMENT GATEWAY LOGS =================
INSERT INTO banking.metadata.table_parameters VALUES
(6, 'load_type', 'APPEND', current_timestamp()),
(6, 'primary_key', 'txn_id', current_timestamp()),
(6, 'watermark_column', 'processed_timestamp', current_timestamp());


-- ==================
-- INSERT INTO metadata.table_watermarks
-- ===================


-- =====================================================
-- OPTIONAL: INITIALIZE WATERMARK TABLE
-- (Only for INCREMENTAL tables)
-- =====================================================


CREATE TABLE IF NOT EXISTS banking.metadata.table_watermarks (
    table_id                INT,
    last_watermark_value    STRING,     -- flexible type storage
    last_updated_at         TIMESTAMP,
    last_run_id             BIGINT
)
USING DELTA
PARTITIONED BY (table_id);

INSERT INTO banking.metadata.table_watermarks VALUES
(1, '1900-01-01 00:00:00.000000', current_timestamp(), NULL),
(2, '1900-01-01 00:00:00.000000', current_timestamp(), NULL),
(3, '1900-01-01 00:00:00', current_timestamp(), NULL),
(5, '1900-01-01', current_timestamp(), NULL),
(6, '1900-01-01 00:00:00', current_timestamp(), NULL);

select * from banking.metadata.table_watermarks;

-- ==================
-- INSERT INTO metadata.pipeline_runs
-- ===================

CREATE TABLE IF NOT EXISTS banking.metadata.pipeline_runs (
    run_id              BIGINT,
    table_id            INT,
    layer               STRING,        -- Bronze / Silver / Gold
    start_time          TIMESTAMP,
    end_time            TIMESTAMP,
    status              STRING,        -- SUCCESS / FAILED
    number_of_records     BIGINT,
    error_message       STRING
	duration	int
	kpi_id	int
)
USING DELTA
PARTITIONED BY (table_id);


alter table banking.bronze.customers add column created_at timestamp;
alter table banking.bronze.customers add column updated_at timestamp;

-- COMMAND ----------

update banking.metadata.tables
set schema_list='ACCOUNT_ID, CUSTOMER_ID, ACCOUNT_TYPE, BALANCE, CURRENCY, BRANCH_CODE, STATUS, OPENED_DATE, CREATED_AT, UPDATED_AT'
where table_id=2;

-- ==================
-- INSERT INTO metadata.gold_layer_kpi
-- ===================

CREATE TABLE IF NOT EXISTS banking.metadata.gold_layer_kpi (
kpi_id	int,
kpi_name	string,
kpi_path	string,
execution_date	date,
created_timestamp	timestamp
)
USING DELTA
PARTITIONED BY (kpi_id);

insert into banking.metadata.gold_layer_kpi values(1,'branch_performance','/Banking_Project/transformations/branch_performance.py',current_date(),current_timestamp());
insert into banking.metadata.gold_layer_kpi values(2,'customer_360','/Banking_Project/transformations/customer_360.py',current_date(),current_timestamp());
insert into banking.metadata.gold_layer_kpi values(3,'daily_bank_kpi','/Banking_Project/transformations/daily_bank_kpi.py',current_date(),current_timestamp());
insert into banking.metadata.gold_layer_kpi values(4,'risk_customer_summary','/Banking_Project/transformations/risk_customer_summary.py',current_date(),current_timestamp());
insert into banking.metadata.gold_layer_kpi values(5,'transaction_channel_summary','/Banking_Project/transformations/transaction_channel_summary.py',current_date(),current_timestamp());

-- ==================
-- INSERT INTO metadata.run_execution_details
-- ===================

CREATE TABLE IF NOT EXISTS banking.metadata.run_execution_details (
run_id	string,
run_start	timestamp,
run_end	timestamp,
run_duration	int)
USING DELTA;

insert into banking.metadata.run_execution_details (run_id,run_start,run_end,run_duration)
values(101,current_timestamp(),current_timestamp(),null)