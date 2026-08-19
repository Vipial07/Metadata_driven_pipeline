# Databricks notebook source


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT run_id DEFAULT '1';

# COMMAND ----------

parameter=spark.sql("""select * from banking.metadata.gold_layer_kpi""")

from datetime import datetime
import json

rows = parameter.collect()
print(rows)

run_id=dbutils.widgets.get("run_id")

for i in range(len(rows)):
    config = {
        "kpi_id":rows[i]["kpi_id"],
        "kpi_name": rows[i]["kpi_name"],
        "kpi_path": rows[i]["kpi_path"],
        "execution_date": rows[i]["execution_date"],
        "created_timestamp": rows[i]["created_timestamp"]
    }

    
    current_user=spark.sql("""SELECT current_user()""").collect()[0][0]
    print(current_user)
    notebook_path = f"/Workspace/Users/{current_user}/{config['kpi_path']}"
    print("Notebook to execute:", notebook_path)    
    
    status = "SUCCESS"
    error_message = None
    records = None

    try:
        start_time=datetime.now()
        result = dbutils.notebook.run(
            notebook_path,
            timeout_seconds=0
        )

    # Expect the notebook to return record count
        if result:
            records = int(result)

        print("Notebook completed successfully")
        print("Records:", records)

        end_time=datetime.now()
        duration=(end_time-start_time).total_seconds()
        entry_exists = spark.sql(f"""
        SELECT 1
        FROM banking.metadata.pipeline_runs
        WHERE run_id = {run_id}
        AND kpi_id = {config['kpi_id']}
        """).count() > 0

        if entry_exists:
            spark.sql(f"""
            UPDATE banking.metadata.pipeline_runs
            SET end_time ='{end_time}',start_time = '{start_time}', status = 'COMPLETED',kpi_id={config['kpi_id']},layer='GOLD',number_of_records={records},duration={duration}
            WHERE run_id = {run_id} AND kpi_id = {config['kpi_id']}""")
        else:
            spark.sql(f"""
            INSERT INTO banking.metadata.pipeline_runs(run_id,start_time,end_time,status,kpi_id,layer,number_of_records,duration)
            VALUES ({run_id},'{start_time}','{end_time}','COMPLETED',{config['kpi_id']},'GOLD',{records},{duration})
            """)
    
    except Exception as e:

        status = "FAILED"
        error_message = str(e)

        print("Notebook failed")
        print(error_message)
        print(config)

# /Workspace/Users/guptavipinkumar651@gmail.com/Banking_Project/transformations/branch_performance.py