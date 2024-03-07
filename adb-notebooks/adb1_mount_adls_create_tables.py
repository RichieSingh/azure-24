# Databricks notebook source
storageAccountName = 'azpractise19'

# COMMAND ----------



# COMMAND ----------

storageAccountName = "azpractise19"
storageAccountAccessKey = 'lmhWurgNBMRoQ9RLg/ySzCkFkznTBlGnUhGDO+gB+6o4OPgkv/QGNQTl+0oxhOBWwQUARk2h3U+8+AStpBrXzw=='
#sasToken = <sas-token>
blobContainerName = "main"
mountPoint = "/mnt/dataMain/"

dbutils.fs.mount(
    source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
    mount_point = mountPoint,
extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}
#extra_configs = {'fs.azure.sas.' + blobContainerName + '.' + storageAccountName + '.blob.core.windows.net': sasToken}
)


# COMMAND ----------

filePath = '/mnt/dataMain/public'
dbutils.fs.ls(filePath)

# COMMAND ----------

dbutils.fs.cp('mnt/dataMain/public/TTGovtSalaries_latest.csv','mnt/dataMain/testFolder/TTGovtSalaries_latest.csv')

# COMMAND ----------

spark

# COMMAND ----------

yellow_taxi ='/mnt/dataMain/public/yellow_tripdata_selected.csv'
df = spark.read.csv(yellow_taxi, header='True')
display(df.show(10))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.write.format("delta").saveAsTable("yellow_taxi_selected")


# COMMAND ----------

df_read_taxi = spark.sql('select * from rg_adb_practise.default.yellow_taxi_selected')
df_read_taxi.show()

# COMMAND ----------

df_read_taxi.count()
#1048575

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_data_view AS
# MAGIC SELECT *
# MAGIC FROM CSV.`/mnt/dataMain/public/yellow_tripdata_new.csv`
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO rg_adb_practise.default.yellow_taxi_selected(VendorID,tpep_pickup_datetime,tpep_dropoff_datetime, passenger_count,trip_distance,RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,total_amount)
# MAGIC SELECT * FROM temp_data_view 

# COMMAND ----------


#READ TABLE PROPERTIES TO SEE DELTA 
spark.sql('DESCRIBE TABLE FORMATTED rg_adb_practise.default.yellow_taxi_selected').show()

# COMMAND ----------

# MAGIC %sql
# MAGIC --READ TABLE PROPERTIES TO SEE DELTA 
# MAGIC DESCRIBE HISTORY rg_adb_practise.default.yellow_taxi_selected

# COMMAND ----------

# MAGIC %md
# MAGIC CREATE SILVER LAYER DB

# COMMAND ----------

# MAGIC %sql
# MAGIC --create database silver;
# MAGIC CREATE TABLE rg_adb_practise.silver.yellow_taxi (vendor_id INT,trip_start_time TIMESTAMP, trip_end_time TIMESTAMP, trip_distance FLOAT, passenger_count INT, total_amount DECIMAL(10,2))

# COMMAND ----------

# MAGIC %md
# MAGIC LOAD DATA FROM BRONZE TO SILVER

# COMMAND ----------

# MAGIC %sql
# MAGIC with temp_enrich AS (
# MAGIC   SELECT 
# MAGIC     cast(vendorID as INT) vendor_id  ,
# MAGIC     cast(tpep_pickup_datetime as TIMESTAMP) trip_start_time,
# MAGIC     cast(tpep_dropoff_datetime as TIMESTAMP) trip_end_time,
# MAGIC     cast(trip_distance as FLOAT) trip_distance,
# MAGIC     cast(passenger_count as INT) passenger_count,
# MAGIC     cast(total_amount as DECIMAL(10,2)) as total_amount
# MAGIC
# MAGIC     
# MAGIC   FROM rg_adb_practise.default.yellow_taxi_selected
# MAGIC
# MAGIC )
# MAGIC
# MAGIC MERGE INTO rg_adb_practise.silver.yellow_taxi  AS target
# MAGIC USING temp_enrich AS source
# MAGIC ON target.vendor_id = source.vendor_id   
# MAGIC WHEN MATCHED THEN 
# MAGIC   UPDATE SET 
# MAGIC     target.vendor_id  = source.vendor_id ,   
# MAGIC     target.trip_start_time = source.trip_start_time,
# MAGIC     target.trip_end_time = source.trip_end_time,
# MAGIC     target.trip_distance = source.trip_distance,
# MAGIC     target.passenger_count = source.passenger_count,
# MAGIC     target.total_amount = source.total_amount
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC   INSERT *

# COMMAND ----------


#COPY NEW FILE DATA into yellow_taxi (schema evolution)
dbutils.fs.ls(filePath)

# COMMAND ----------

filePathDelta = '/mnt/dataMain/public/yellow_tripdata_selected_delta.csv'
df_delta_y_taxi = spark.read.csv(filePathDelta, header=True)
display(df_delta_y_taxi)

# COMMAND ----------

df_delta_y_taxi.write.mode('append').option('mergeSchema','true').insertInto('rg_adb_practise.default.yellow_taxi_selected')



# COMMAND ----------

# MAGIC %sql
# MAGIC --NEW COUNT (inserted rows - 3, added 1 new column - trip_city)
# MAGIC select count(*) from rg_adb_practise.default.yellow_taxi_selected;
# MAGIC --DESCRIBE HISTORY rg_adb_practise.default.yellow_taxi_selected

# COMMAND ----------

# MAGIC %sql
# MAGIC --NEW COUNT 
# MAGIC --select count(*) from rg_adb_practise.default.yellow_taxi_selected;
# MAGIC DESCRIBE HISTORY rg_adb_practise.default.yellow_taxi_selected

# COMMAND ----------

# MAGIC %sql
# MAGIC with temp_enrich AS (
# MAGIC   SELECT 
# MAGIC     cast(vendorID as INT) vendor_id  ,
# MAGIC     cast(tpep_pickup_datetime as TIMESTAMP) trip_start_time,
# MAGIC     cast(tpep_dropoff_datetime as TIMESTAMP) trip_end_time,
# MAGIC     cast(trip_distance as FLOAT) trip_distance,
# MAGIC     cast(passenger_count as INT) passenger_count,
# MAGIC     cast(total_amount as DECIMAL(10,2)) as total_amount
# MAGIC
# MAGIC     
# MAGIC   FROM rg_adb_practise.default.yellow_taxi_selected
# MAGIC
# MAGIC )
# MAGIC
# MAGIC MERGE INTO rg_adb_practise.silver.yellow_taxi  AS target
# MAGIC USING temp_enrich AS source
# MAGIC ON target.vendor_id = source.vendor_id   and target.trip_start_time= source.trip_start_time
# MAGIC
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC   INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC --NEW COUNT 
# MAGIC DESCRIBE HISTORY rg_adb_practise.silver.yellow_taxi;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE EXTENDED rg_adb_practise.default.yellow_taxi_selected

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE rg_adb_practise.default.yellow_taxi_selected_copy
# MAGIC (
# MAGIC   vendorID STRING, tpep_pickup_datetime STRING, tpep_dropoff_datetime STRING, passenger_count STRING, 
# MAGIC   trip_distance STRING, RatecodeID STRING, store_and_fwd_flag STRING
# MAGIC
# MAGIC )

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false")

# COMMAND ----------


spark.sql("""MERGE INTO rg_adb_practise.default.yellow_taxi_selected_copy  AS target
USING rg_adb_practise.default.yellow_taxi_selected AS source
ON target.vendorID = source.vendorID

WHEN NOT MATCHED THEN 
  INSERT *""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from rg_adb_practise.default.yellow_taxi_selected_copy
# MAGIC
# MAGIC --truncate table rg_adb_practise.default.yellow_taxi_selected_copy

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE rg_adb_practise.default.yellow_taxi_selected_with_constraints
# MAGIC (
# MAGIC   vendorID STRING, tpep_pickup_datetime STRING, tpep_dropoff_datetime STRING, passenger_count STRING, 
# MAGIC   trip_distance STRING, RatecodeID STRING, store_and_fwd_flag STRING
# MAGIC
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE rg_adb_practise.default.yellow_taxi_selected_copy
# MAGIC ALTER COLUMN vendorID SET NOT NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE rg_adb_practise.default.yellow_taxi_selected_copy
# MAGIC ADD CONSTRAINT total_amount CHECK(total_amount<=100000 or total_amount is null)

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE History rg_adb_practise.default.yellow_taxi_selected_copy
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from rg_adb_practise.default.yellow_taxi_selected_copy 

# COMMAND ----------

# MAGIC %sql
# MAGIC  OPTIMIZE rg_adb_practise.default.yellow_taxi_selected_copy 

# COMMAND ----------

# MAGIC %sql
# MAGIC  OPTIMIZE rg_adb_practise.default.yellow_taxi_selected_copy 
# MAGIC  ZORDER BY trip_distance, total_amount

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM rg_adb_practise.default.yellow_taxi_selected_copy --physicall delete files (older than retention threshold)
# MAGIC --RETAIN 0 HOURS DRY RUN

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE HISTORY rg_adb_practise.default.yellow_taxi_selected_copy
# MAGIC where isBlindAppend = false
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE  table rg_adb_practise.default.logs_test
# MAGIC AS
# MAGIC DESCRIBE HISTORY rg_adb_practise.default.yellow_taxi_selected_copy;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  rg_adb_practise.default.logs_test
# MAGIC where operation like '%VACUUM%'
