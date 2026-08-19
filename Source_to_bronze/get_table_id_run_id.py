# Databricks notebook source
tables_df = spark.table("banking.metadata.tables")
tables_df=tables_df.select("table_id","active_flag")

filtered_df = (
    tables_df
    .filter(tables_df.active_flag == "true")
)

filtered_df=filtered_df.select("table_id").distinct()

rows = filtered_df.collect()

tables_list = [
    {
        "table_id": int(row.table_id) if row.table_id is not None else ""
    }
    for row in rows
]

print("Tables to Process (Full Metadata):")
# print(tables_list)
# print(type(tables_list))
result = [item['table_id'] for item in tables_list]
result.sort()
print(result)
# print(tables_list.values())

dbutils.jobs.taskValues.set(
    key="tables_metadata",
    value=result
)

run_execution_details=spark.table("banking.metadata.run_execution_details").select("run_id").orderBy("run_id",ascending=False).limit(1)
# run_execution_details.display()
run_execution_details=run_execution_details.collect()
run_id=run_execution_details[0]['run_id']
run_id=int(run_id)+1
print(run_id)
dbutils.jobs.taskValues.set(
    key="run_id",
    value=str(run_id)
)
print("Task value 'tables_metadata' has been set.")
