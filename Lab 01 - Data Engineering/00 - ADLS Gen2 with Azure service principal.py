# Databricks notebook source
user_id = spark.sql('select current_user() as user').collect()[0]['user']
username = user_id.split("@")[0]

dbutils.fs.mkdirs(f"/mnt/adls/{username}")

# COMMAND ----------

service_credential = dbutils.secrets.get(scope="databricks-key-vault",key="clientsecret")
application_id = dbutils.secrets.get(scope="databricks-key-vault",key="applicationid")
directory_id = dbutils.secrets.get(scope="databricks-key-vault",key="tenantid")

storage_account = "nikodatabricksstorage"
container_name = username

adls_path = f"abfss://{container_name}@{storage_account}.dfs.core.chinacloudapi.cn/"
mount_point = f"/mnt/adls/{username}"

# Connecting using Service Principal secrets and OAuth
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": application_id,
           "fs.azure.account.oauth2.client.secret": service_credential,
           "fs.azure.account.oauth2.client.endpoint": f"https://login.partner.microsoftonline.cn/{directory_id}/oauth2/token"}

# Mounting ADLS Storage to DBFS
# Mount only if the directory is not already mounted
dbutils.fs.mount(
  source = adls_path,
  mount_point = mount_point,
  extra_configs = configs)

# COMMAND ----------

# DBTITLE 1,Read Databricks Dataset IoT Devices JSON
df = spark.read.json("/databricks-datasets/iot/iot_devices.json")
display(df)

# COMMAND ----------

# DBTITLE 1,Write Delta table to external path
df.write.save(f"/mnt/adls/{username}/iot/")

# COMMAND ----------

# DBTITLE 1,List filesystem
dbutils.fs.ls(f"/mnt/adls/{username}/iot/")

# COMMAND ----------

# DBTITLE 1,Read IoT Devices JSON from ADLS Gen2 filesystem
df2 = spark.read.load(f"/mnt/adls/{username}/iot/")
display(df2)
