# Databricks notebook source
# MAGIC %run ./Fetch-User-Metadata

# COMMAND ----------

# DBTITLE 1,Read Databricks Dataset IoT Devices JSON
df = spark.read.json("/databricks-datasets/iot/iot_devices.json")
display(df)

# COMMAND ----------

# DBTITLE 1,Write Delta table to external path
df.write.mode("overwrite").save(f"/mnt/adls/{username}/iot/")

# COMMAND ----------

# DBTITLE 1,List filesystem
dbutils.fs.ls(f"/mnt/adls/{username}/iot")

# COMMAND ----------

# DBTITLE 1,Read IoT Devices JSON from ADLS Gen2 filesystem
df2 = spark.read.load(f"/mnt/adls/{username}/iot/")
display(df2)

# COMMAND ----------


