# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE WIDGET TEXT table_id DEFAULT '1';
# MAGIC CREATE WIDGET TEXT run_id DEFAULT '1';
# MAGIC

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import max
table_id=dbutils.widgets.get("table_id")
run_id=dbutils.widgets.get("run_id")

start_time=datetime.now()

read_metadata=spark.read.format("delta").table("banking.metadata.tables")
read_table_parameter=spark.read.format("delta").table("banking.metadata.table_parameters")
read_table_watermark=spark.read.format("delta").table("banking.metadata.table_watermarks")
table_id = dbutils.widgets.get("table_id")
run_id=dbutils.widgets.get("run_id")
read_metadata=read_metadata.filter(read_metadata.table_id==table_id)
# read_metadata.display()

read_metadata.createOrReplaceTempView('read_metadata')
read_table_parameter.createOrReplaceTempView('read_table_parameter')
read_table_watermark.createOrReplaceTempView('read_table_watermark')

parameters=spark.sql("""
                    select distinct a.source_system,a.table_id,a.table_name,a.schema_list,a.source_table,
                    b.parameter_name,b.parameter_value,a.bronze_schema,c.last_watermark_value,a.silver_schema,a.gold_schema
                    from read_metadata a
                    inner join read_table_parameter b
                    on a.table_id=b.table_id
                    left join read_table_watermark c
                    on a.table_id=c.table_id
                """)
                
# parameters.display()


import json

rows = parameters.collect()

config = {
    "source_system":rows[0]["source_system"],
    "table_id": rows[0]["table_id"],
    "table_name": rows[0]["table_name"],
    "schema_list": [c.strip() for c in rows[0]["schema_list"].split(",")],
    "source_table": rows[0]["source_table"],
    "parameters": {
        row["parameter_name"]: row["parameter_value"]
        for row in rows
    },
    "bronze_schema":rows[0]["bronze_schema"],
    "last_watermark_value":rows[0]["last_watermark_value"],
    "silver_schema":rows[0]["silver_schema"],
    "gold_schema":rows[0]["gold_schema"]
}

json_output = json.dumps(config, indent=4)
# print(json_output)

table_id=dbutils.widgets.get("table_id")
run_id=dbutils.widgets.get("run_id")

print(f"Processing Table: {config['table_name']}")
print(f"Load Type: {config['parameters']['load_type']}")
print(f"Run ID: {run_id}")

from pyspark.sql.functions import current_timestamp
bronze_table = f"banking.{config['bronze_schema']}.{config['table_name']}"
silver_table = f"banking.{config['silver_schema']}.{config['table_name']}"
try:
    bronze_df=spark.read.table(bronze_table)
    bronze_df=bronze_df.withColumn("insert_timestamp",current_timestamp())
    bronze_df=bronze_df.withColumn("update_timestamp",current_timestamp())
    bronze_df.createOrReplaceTempView('bronze_df')
except:
    print("Table doesn't exist")


if config["parameters"]["load_type"] in ("MERGE", "APPEND"):   
    watermark_column=config["parameters"]["watermark_column"]
    print('watermark_column:',watermark_column)


spark.sql(f"""CREATE SCHEMA IF NOT EXISTS banking.{config['silver_schema']}""")

if config['parameters']['load_type']=='FULL':
    print("Full Refresh")
    # bronze_df.printSchema()
    # spark.table(silver_table).printSchema()
    bronze_df.write.mode("overwrite").format("delta").saveAsTable(silver_table)
    # spark.sql(f"OPTIMIZE {bronze_table} ZORDER BY (BRANCH_CODE)")

if config['parameters']['load_type']=='MERGE':
    print("Merge")
    bronze_df.printSchema()
    watermark_df = spark.sql(f"""
        SELECT last_watermark_value
        FROM banking.metadata.table_watermarks
        WHERE table_id = {table_id}
    """)
    watermark_df.display()

    last_watermark = None
    if watermark_df.count() > 0:
        last_watermark = watermark_df.first()["last_watermark_value"]

    if last_watermark:
        print('bronze_df-1st')
        bronze_df.printSchema()
        bronze_df = bronze_df.filter(
             bronze_df[watermark_column] > last_watermark
        )
        print('bronze_df-2st')
        bronze_df.printSchema()
    # bronze_df.printSchema()   
    pkey=str(config["parameters"]["primary_key"])
    print('pkey',pkey)

    spark.sql(f"""create table if not exists banking.{config['silver_schema']}.{config['table_name']} using delta as select * from bronze_df where 1=2""")
    
    df=spark.sql(f"""select * from banking.{config['silver_schema']}.{config['table_name']}""")
    # df.printSchema()
    spark.sql(f"""merge into banking.{config['silver_schema']}.{config['table_name']} as t using
               (select * from bronze_df) as s
               on t.{pkey}=s.{pkey}
               when matched then update set *
               when not matched then insert *""")
    # spark.sql(f"OPTIMIZE {silver_table} ZORDER BY ({pkey})")
    # bronze_df.printSchema()
    df_count=bronze_df.count()
    print("Merge Complete")
    print(df_count)

if config['parameters']['load_type']=='APPEND':
    print("Append")
    # bronze_df.printSchema()
    watermark_df = spark.sql(f"""
        SELECT last_watermark_value
        FROM banking.metadata.table_watermarks
        WHERE table_id = {table_id}
    """)
    # watermark_df.display()

    last_watermark = None
    if watermark_df.count() > 0:
        last_watermark = watermark_df.first()["last_watermark_value"]

    if last_watermark:
        bronze_df = bronze_df.filter(
             bronze_df[watermark_column] > last_watermark
        )

    spark.sql(f"""create table if not exists banking.{config['silver_schema']}.{config['table_name']} using delta as select * from bronze_df where 1=2""")
    
    df=spark.sql(f"""select * from banking.{config['silver_schema']}.{config['table_name']}""")
    # df.printSchema()
    bronze_df.write.mode("append").format("delta").saveAsTable(silver_table)
    # spark.sql(f"OPTIMIZE {silver_table} ZORDER BY ({pkey})")
    print("Append Complete")


df_count=bronze_df.count()
# df_count=999
end_time=datetime.now()
duration=(end_time-start_time).total_seconds()
    # -----------------------------------------
    # #make entry in metadata table
    # -----------------------------------------

entry_exists = spark.sql(f"""
SELECT 1
FROM banking.metadata.pipeline_runs
WHERE run_id = {run_id} AND table_id = {table_id}
""").count() > 0

if entry_exists:
    spark.sql(f"""
        UPDATE banking.metadata.pipeline_runs
        SET end_time ='{end_time}',start_time = '{start_time}', status = 'COMPLETED',table_id={table_id},layer='SILVER',number_of_records={df_count},duration={duration}
        WHERE run_id = {run_id} AND table_id = {table_id}
    """)
else:
    spark.sql(f"""
        INSERT INTO banking.metadata.pipeline_runs(run_id,start_time,end_time,status,table_id,layer,number_of_records,duration)
        VALUES ({run_id},'{start_time}','{end_time}','COMPLETED',{table_id},'SILVER',{df_count},{duration})
    """)

# -----------------------------------------
    # Update Watermark (APPEND & MERGE)
    # -----------------------------------------

if config["parameters"]["load_type"] in ("MERGE", "APPEND") and watermark_column:
    max_value = bronze_df.agg(
        max(bronze_df[watermark_column])
    ).collect()[0][0]

    if max_value:
        spark.sql(f"""
                MERGE INTO banking.metadata.table_watermarks t
                USING (SELECT {table_id} AS table_id) s
                ON t.table_id = s.table_id
                WHEN MATCHED THEN UPDATE SET
                    last_watermark_value = '{max_value}',
                    last_updated_at = current_timestamp(),
                    last_run_id = '{run_id}'
                WHEN NOT MATCHED THEN
                    INSERT (table_id, last_watermark_value, last_updated_at)
                    VALUES ({table_id}, '{max_value}', current_timestamp())
        """)
    
        print("Silver Load Completed Successfully.")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from banking.metadata.pipeline_runs where run_id=1003;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct table_id, parameter_name, parameter_value from banking.metadata.table_parameters where parameter_name='load_type';