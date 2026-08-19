# Databricks notebook source
pip install oracledb

# COMMAND ----------

# %sql
# CREATE WIDGET TEXT table_id DEFAULT 1;
# CREATE WIDGET TEXT run_id DEFAULT 1;


# COMMAND ----------

import oracledb
import os
from dotenv import load_dotenv
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import max,current_timestamp
from datetime import datetime

start_time = datetime.now()

load_dotenv()

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
                    b.parameter_name,b.parameter_value,a.bronze_schema,c.last_watermark_value,a.source_path
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
    "source_path":rows[0]["source_path"]
}

json_output = json.dumps(config, indent=4)
print(json_output)

# print('table_name')
table_name=config["table_name"]
col_list=config["schema_list"]
# print(col_list)
# watermark_column=config["parameters"]["watermark_column"]
# print('watermark_column:',watermark_column)
print('last_watermark_value:',config["last_watermark_value"])

lasst_vwatermark_value=config["last_watermark_value"]
quoted_cols = ",".join([f"{col}" for col in col_list])

columns_l = quoted_cols.split(",")
columns= ', '.join(columns_l)

if config["parameters"]["load_type"] in ("MERGE", "APPEND"):   
    watermark_column=config["parameters"]["watermark_column"]
    print('watermark_column:',watermark_column)

if config["source_system"].upper()=='ORACLEDB':
    local_dsn = {dsn}
    connection = oracledb.connect(
    user={USER_ID},
    password={Password},
    dsn=local_dsn)

    print("Successfully connected to Oracle Database")

    cursor = connection.cursor()

    if config["parameters"]["load_type"] == "FULL":
        query=f"""SELECT {columns}  FROM {table_name}"""
        result=cursor.execute(query).fetchall()
    
    if config["parameters"]["load_type"] in ("MERGE", "APPEND") and watermark_column:
        query=f"""SELECT {columns}  
            FROM {table_name} 
            where {watermark_column} >  TIMESTAMP '{lasst_vwatermark_value}'
            """
        print(query)
        result=cursor.execute(query).fetchall()
    else:
        query=f"""SELECT {columns}  FROM {table_name}"""
        result=cursor.execute(query).fetchall()

    # print(result)

    columns = quoted_cols.split(",")

    # print(columns)
    schema = StructType([
        StructField(col.strip(','), StringType(), True)
        for col in columns
    ])
    df = spark.createDataFrame(result, schema=schema)

    df.createOrReplaceTempView('df')

    df.display()
    df_count=df.count()
    print('df_count')
    print(df_count)
    df.printSchema()

elif config["source_system"].upper()=='BLOB':
    print("inside blob")
    print(config['source_path'])
    
    df=(
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option(
                "cloudFiles.schemaLocation",
                f"/Volumes/banking/source/blob_file/_schema/{table_name}"
            )
            .option("header", "true")
            .load(config['source_path'])
        )


# df.createOrReplaceTempView('df')

# df.display()
# df_count=df.count()
# print('df_count')
# print(df_count)
# df.printSchema()

schema_name = config["bronze_schema"]
spark.sql(f"""CREATE SCHEMA IF NOT EXISTS banking.{schema_name}""")

source_system=config['source_system']
# print(type(source_system))

if source_system.upper()=='ORACLEDB':
    print("1st")

    if config["parameters"]["load_type"] == "FULL":
        df.write.mode("overwrite").saveAsTable(f'banking.{config["bronze_schema"]}.{config["table_name"]}')

    elif config["parameters"]["load_type"] == "MERGE":
        # spark.sql(f"create table if not exists as select * from df where 1=2")
        pkey=str(config["parameters"]["primary_key"])
        print('pkey',pkey)
        spark.sql(f""" CREATE TABLE IF NOT EXISTS banking.{config["bronze_schema"]}.{config["table_name"]} USING DELTA AS SELECT * FROM df WHERE 1 = 2 """)

        from delta.tables import DeltaTable

        target = DeltaTable.forName(
        spark,
        f'banking.{config["bronze_schema"]}.{config["table_name"]}')

        (
            target.alias("t")
            .merge(
            df.alias("s"),
        f"t.{pkey} = s.{pkey}"   # Replace with your merge condition
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


    else:
        # df.write.mode("append").saveAsTable(config["table_name"])
        df.write.mode("append").saveAsTable(f'banking.{config["bronze_schema"]}.{config["table_name"]}')    
elif source_system == "blob":
    print("2nd")

    (
        df.writeStream
        .format("delta")
        .option(
            "checkpointLocation",
            f"/Volumes/banking/source/volume/_checkpoints/{table_name}"
        )            
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(f'banking.{config["bronze_schema"]}.{config["table_name"]}')
    )

    df_count=spark.table(f'banking.{config["bronze_schema"]}.{config["table_name"]}').count()
    
end_time = datetime.now()

duration= (end_time - start_time).total_seconds()

#make entry in metadata table

entry_exists = spark.sql(f"""
    SELECT 1
    FROM banking.metadata.pipeline_runs
    WHERE run_id = {run_id} AND table_id = {table_id}
""").count() > 0

if entry_exists:
    spark.sql(f"""
        UPDATE banking.metadata.pipeline_runs
        SET end_time ='{end_time}',start_time = '{start_time}', status = 'COMPLETED',table_id={table_id},layer='BRONZE',number_of_records={df_count},duration={duration}
        WHERE run_id = {run_id} AND table_id = {table_id}
    """)
else:
    spark.sql(f"""
        INSERT INTO banking.metadata.pipeline_runs(run_id,start_time,end_time,status,table_id,layer,number_of_records,duration)
        VALUES ({run_id},'{start_time}','{end_time}','COMPLETED',{table_id},'BRONZE',{df_count},{duration})
    """)