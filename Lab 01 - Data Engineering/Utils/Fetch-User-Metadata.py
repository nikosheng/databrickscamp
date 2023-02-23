# Databricks notebook source

# COMMAND ----------
spark.conf.set("com.databricks.training.module_name", "ap_juice")

user_id = spark.sql('select current_user() as user').collect()[0]['user']
username = user_id.split("@")[0]

module_name = spark.conf.get("com.databricks.training.module_name")

databaseName = (username+"_"+module_name).replace("[^a-zA-Z0-9]", "_") + "_db"
spark.conf.set("com.databricks.training.spark.dbName", databaseName)
spark.conf.set("com.databricks.training.spark.userName", username)

# COMMAND ----------

database_name = spark.conf.get("com.databricks.training.spark.dbName")
username = spark.conf.get("com.databricks.training.spark.userName").replace('.', '_')

displayHTML("""Username is <b style="color:green">{}</b>""".format(username))

# COMMAND ----------

base_table_path = f"dbfs:/FileStore/{username}/deltademoasset/"
local_data_path = f"/dbfs/FileStore/{username}/deltademoasset/"
