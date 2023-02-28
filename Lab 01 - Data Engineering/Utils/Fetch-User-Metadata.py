# Databricks notebook source
import re

spark.conf.set("com.databricks.training.module_name", "ap_juice")

user_id = spark.sql('select current_user() as user').collect()[0]['user']
username = re.sub('[^a-zA-Z0-9]', '', user_id.split("@")[0])

module_name = spark.conf.get("com.databricks.training.module_name")

databaseName = (username+"_"+module_name).replace("[^a-zA-Z0-9]", "_") + "_db"
spark.conf.set("com.databricks.training.spark.dbName", databaseName)
spark.conf.set("com.databricks.training.spark.userName", username)

# COMMAND ----------

storage_account = dbutils.widgets.get("storage_account_name")
dataset_container= dbutils.widgets.get("dataset_container_name")
container_name = dbutils.widgets.get("container_name")

service_credential = dbutils.secrets.get(scope="databricks-key-vault",key="clientsecret")
application_id = dbutils.secrets.get(scope="databricks-key-vault",key="applicationid")
directory_id = dbutils.secrets.get(scope="databricks-key-vault",key="tenantid")

# personal lab folder
dbutils.fs.mkdirs(f"/mnt/adls/workshop")
# dataset folder
dbutils.fs.mkdirs(f"/mnt/adls/dataset")

workshop_folder_adls_path = f"abfss://{container_name}@{storage_account}.dfs.core.chinacloudapi.cn/"
workshop_folder_mount_point = f"/mnt/adls/workshop/"

# Connecting using Service Principal secrets and OAuth
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": application_id,
           "fs.azure.account.oauth2.client.secret": service_credential,
           "fs.azure.account.oauth2.client.endpoint": f"https://login.partner.microsoftonline.cn/{directory_id}/oauth2/token"}

# Mounting Workshop Root Folder, if already mounted, use updateMount instead of mount
dbutils.fs.updateMount(
  source = workshop_folder_adls_path,
  mount_point = workshop_folder_mount_point,
  extra_configs = configs)

dbutils.fs.mkdirs(f"/mnt/adls/workshop/{username}")

# COMMAND ----------

user_folder_adls_path = f"abfss://{container_name}@{storage_account}.dfs.core.chinacloudapi.cn/{username}"
user_folder_mount_point = f"/mnt/adls/workshop/{username}"

dataset_folder_adls_path = f"abfss://{dataset_container}@{storage_account}.dfs.core.chinacloudapi.cn/"
dataset_folder_mount_point = f"/mnt/adls/{dataset_container}"

# Mounting ADLS Storage to DBFS
# Mount only if the directory is not already mounted
dbutils.fs.mount(
  source = user_folder_adls_path,
  mount_point = user_folder_mount_point,
  extra_configs = configs)

# COMMAND ----------

database_name = spark.conf.get("com.databricks.training.spark.dbName")
username = spark.conf.get("com.databricks.training.spark.userName").replace('.', '_')

displayHTML("""Username is <b style="color:green">{}</b>""".format(username))
