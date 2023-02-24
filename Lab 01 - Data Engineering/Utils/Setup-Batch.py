# Databricks notebook source
# MAGIC %run ./Fetch-User-Metadata

# COMMAND ----------

spark.sql(f"DROP DATABASE IF EXISTS {database_name} CASCADE")

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

# COMMAND ----------

spark.sql(f"USE {database_name}")

# COMMAND ----------

# Return to the caller, passing the variables needed for file paths and database

response = user_folder_adls_path + " " + storage_account + " " + container_name + " " + database_name

dbutils.notebook.exit(response)
