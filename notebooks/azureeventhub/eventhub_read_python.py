# Databricks notebook source
# MAGIC %md
# MAGIC Read from Azure Event Hub using Pyspark:
# MAGIC https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/PySpark/structured-streaming-pyspark.md
# MAGIC 
# MAGIC Use the following spark jar file to enable Pyspark implementation: https://mvnrepository.com/artifact/com.microsoft.azure/azure-eventhubs-spark_2.12/2.3.17
# MAGIC 
# MAGIC Youtube ADLS Gen 2 Capture Use Case: https://www.youtube.com/watch?v=OIOEsd2Iiik&t=571s
# MAGIC 
# MAGIC Amazing Repo Exercises: https://github.com/tsmatz/azure-databricks-exercise

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Create an Event Hub instance in the previously created Azure Event Hub namespace.
# MAGIC 2. Create a new Shared Access Policy in the Event Hub instance. Copy the connection string generated with the new policy. Note that this connection string has an "EntityPath" component , unlike the RootManageSharedAccessKey connectionstring for the Event Hub namespace.
# MAGIC 3. To enable Azure Event hub Databricks ingestion and transformations, install the Azure Event Hubs Connector for Apache Spark from the Maven repository. For this post, I have installed the version 2.3.18 of the connector, using the following  maven coordinate: "com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.18". This library is the most current package at the time of this writing.

# COMMAND ----------

# Permission is based on File or folder based ACL assignments to the Data Lake filesystem (container) . RBAC assignments to the top level Azure Data Lake resource is not required.
spark.conf.set("fs.azure.account.auth.type.store05.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.store05.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.store05.dfs.core.windows.net", dbutils.secrets.get("myscope", key="clientid"))
spark.conf.set("fs.azure.account.oauth2.client.secret.store05.dfs.core.windows.net", dbutils.secrets.get("myscope", key="clientsecret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint.store05.dfs.core.windows.net", "https://login.microsoftonline.com/{}/oauth2/token".format(dbutils.secrets.get("myscope", key="tenantid")))

# COMMAND ----------

# MAGIC %sh
# MAGIC nslookup labeventhub05.servicebus.windows.net

# COMMAND ----------

# MAGIC %md
# MAGIC Read stream from Azure Event Hub as streaming dataframe using readStream().
# MAGIC Set your namespace, entity, policy name, and key for Azure Event Hub in the following command.

# COMMAND ----------

connectionString = "Endpoint=sb://labeventhub05.servicebus.windows.net/;SharedAccessKeyName=default;SharedAccessKey=E1EljdmP5D8NtMh8ckga3kXMGMMA7ecC0NUnix5Lnns=;EntityPath=labeventhub"

# COMMAND ----------

# Initialize event hub config dictionary with connectionString
ehConf = {}
ehConf['eventhubs.connectionString'] = connectionString

# COMMAND ----------

# Add consumer group to the ehConf dictionary
ehConf['eventhubs.consumerGroup'] = "$Default"

# COMMAND ----------

# Encrypt ehConf connectionString property
ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)

# COMMAND ----------

df = spark.readStream.format("eventhubs").options(**ehConf).load()

# COMMAND ----------

display(df)

# COMMAND ----------

# Write streams into defined sink
from pyspark.sql.types import *
import  pyspark.sql.functions as F

events_schema = StructType([
  StructField("id", StringType(), True),
  StructField("timestamp", StringType(), True),
  StructField("uv", StringType(), True),
  StructField("temperature", StringType(), True),
  StructField("humidity", StringType(), True)])

decoded_df = df.select(F.from_json(F.col("body").cast("string"), events_schema).alias("Payload"))

# COMMAND ----------

display(decoded_df)

# COMMAND ----------

df_events = decoded_df.select(decoded_df.Payload.id, decoded_df.Payload.timestamp, decoded_df.Payload.uv, decoded_df.Payload.temperature, decoded_df.Payload.humidity)

# COMMAND ----------

display(df_events)

# COMMAND ----------

df_out = df_events.writeStream\
  .format("json")\
  .outputMode("append")\
  .option("checkpointLocation", "abfss://checkpointcontainer@store05.dfs.core.windows.net/checkpointapievents")\
  .start("abfss://api-eventhub@store05.dfs.core.windows.net/writedata")

# COMMAND ----------

# read back the data for visual exploration
df_read = spark.read.format("json").load("abfss://api-eventhub@store05.dfs.core.windows.net/writedata")

# COMMAND ----------

display(df_read)

# COMMAND ----------

df_read.printSchema()

# COMMAND ----------

# sample
# query1 = (
#   decoded_df
#     .writeStream
#     .format("memory")
#     .queryName("read_hub")
#     .start()
# )