-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Ingest and Manipulate a Delta Table Lab
-- MAGIC
-- MAGIC This notebook provides a hands-on review of some of the features Delta Lake brings to the data lakehouse.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Classroom Setup
-- MAGIC
-- MAGIC Run the following cell to configure your working environment for this course.
-- MAGIC
-- MAGIC **NOTE:** The `DA` object is only used in Databricks Academy courses and is not available outside of these courses. It will dynamically reference the information needed to run the course.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-05L

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Begin Lab

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Set your current Catalog to **dbacademy** and your schema to your specific schema.
-- MAGIC
-- MAGIC     **HINT**:
-- MAGIC     - Catalog: `USE CATALOG`
-- MAGIC     - Schema: `IDENTIFIER(DA.schema_name) (or you can hardcode your schema name)`

-- COMMAND ----------

<FILL_IN>

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC USE CATALOG IDENTIFIER(DA.catalog_name);
-- MAGIC USE SCHEMA IDENTIFIER(DA.schema_name);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Run a query to view your current Catalog and schema. Verify that the results show the module's Catalog (**dbacademy**) and your specific schema.

-- COMMAND ----------

<FILL_IN>

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC SELECT current_catalog(), current_schema()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. View the available volumes in your schema and confirm that the **taxi_files** volume is listed.

-- COMMAND ----------

<FILL_IN>

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC SHOW VOLUMES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. List the files in the **taxi_files** volume and check the **name**  column to determine the file types stored in the volume. Ignore any additional files that begin with an underscore (_).
-- MAGIC
-- MAGIC **HINT**: Use the following path format to access the volume: */Volumes/catalog_name/schema_name/volume_name/*.

-- COMMAND ----------

<FILL_IN>

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC %python
-- MAGIC spark.sql(f"LIST '/Volumes/{DA.catalog_name}/{DA.schema_name}/taxi_files'").display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5. Query the volume path directly and preview the data in the file using the appropriate file format. Make sure to use backticks around the path to your volume.
-- MAGIC
-- MAGIC **HINT**: SELECT * FROM \<file-format\>. \`\<path-to-volume-taxi_files\>\`

-- COMMAND ----------

<FILL_IN>

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC %python
-- MAGIC spark.sql(f'''SELECT *
-- MAGIC FROM csv.`/Volumes/{DA.catalog_name}/{DA.schema_name}/taxi_files`
-- MAGIC LIMIT 10
-- MAGIC ''').display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6. Create a table in your schema called **taxitrips_bronze** that contains the following columns:
-- MAGIC | Field Name | Field type |
-- MAGIC | --- | --- |
-- MAGIC | tpep_pickup_datetime | TIMESTAMP |
-- MAGIC | tpep_dropoff_datetime | TIMESTAMP |
-- MAGIC | trip_distance | DOUBLE |
-- MAGIC | fare_amount | DOUBLE |
-- MAGIC | pickup_zip | INT |
-- MAGIC | dropoff_zip | INT |
-- MAGIC
-- MAGIC **NOTE:** The DROP TABLE statement will drop the table if it already exists to avoid errors.

-- COMMAND ----------

<FILL_IN>

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC DROP TABLE IF EXISTS taxitrips_bronze;
-- MAGIC
-- MAGIC CREATE TABLE IF NOT EXISTS taxitrips_bronze (
-- MAGIC   tpep_pickup_datetime TIMESTAMP,
-- MAGIC   tpep_dropoff_datetime TIMESTAMP,
-- MAGIC   trip_distance DOUBLE,
-- MAGIC   fare_amount DOUBLE,
-- MAGIC   pickup_zip INT,
-- MAGIC   dropoff_zip INT
-- MAGIC );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 7. Use the [COPY INTO](https://docs.databricks.com/en/sql/language-manual/delta-copy-into.html) statement to populate the table with files from the **taxi_files** volume into the **taxitrips_bronze** table. Include the following options:
-- MAGIC     - FROM `path-to-tax_files`
-- MAGIC     - FILEFORMAT = '\<file-format\>'
-- MAGIC     - FORMAT_OPTIONS
-- MAGIC       - 'header' = 'true'
-- MAGIC       - 'inferSchema' = 'true'
-- MAGIC
-- MAGIC     Confirm 21,932 rows were inserted.

-- COMMAND ----------

<FILL_IN>

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC %python
-- MAGIC spark.sql(f'''COPY INTO taxitrips_bronze
-- MAGIC   FROM '/Volumes/{DA.catalog_name}/{DA.schema_name}/taxi_files'
-- MAGIC   FILEFORMAT = CSV
-- MAGIC   FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
-- MAGIC   ''').display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 8. Count the number of rows in the **taxitrips_bronze** table. Confirm that the table has 21,932 rows.

-- COMMAND ----------

<FILL_IN>

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC SELECT count(*) as totalrows
-- MAGIC FROM taxitrips_bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 9. View the **taxitrips_bronze** table's history. Confirm version 0 and version 1 are available.

-- COMMAND ----------

<FILL_IN>

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC DESCRIBE HISTORY taxitrips_bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 10. Run the following script to delete all rows where **trip_distance** is less than *1*. Confirm *5,387* rows were deleted.

-- COMMAND ----------

DELETE FROM taxitrips_bronze
  WHERE trip_distance < 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 11. View the **taxitrips_bronze** table's history. View the **operation** column. View the version where the *DELETE* operation occurred.

-- COMMAND ----------

<FILL_IN>

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC DESCRIBE HISTORY taxitrips_bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 12. Run a query to count the total number of rows in the current version of the **taxitrips_bronze** table. Confirm that the current table contains *16,545* rows.
-- MAGIC
-- MAGIC **HINT:** By default the most recent version will be used.

-- COMMAND ----------

<FILL_IN>

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC SELECT count(*) AS totalrows
-- MAGIC FROM taxitrips_bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 13. Query the original version of the table to count the number of rows when it was first created. Confirm that the original table contains *21,932* rows.
-- MAGIC
-- MAGIC **HINT:** FROM \<table> VERSION AS OF \<n>

-- COMMAND ----------

<FILL_IN>

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC SELECT count(*) AS totalrows
-- MAGIC FROM taxitrips_bronze VERSION AS OF 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **CHALLENGE**
-- MAGIC
-- MAGIC
-- MAGIC 14. Whoops! You made a mistake and didn't mean to delete the rows from earlier. Use the [RESTORE](https://docs.databricks.com/en/sql/language-manual/delta-restore.html) statement to restore a Delta table to the original state prior to the *DELETE* operation.

-- COMMAND ----------

<FILL_IN>

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC RESTORE TABLE taxitrips_bronze TO VERSION AS OF 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 15. View the history of the **taxitrips_bronze** table. Confirm the most recent version contains the **operation** *RESTORE*.

-- COMMAND ----------

<FILL_IN>

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC DESCRIBE HISTORY taxitrips_bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 16. Count the total number of rows in the current **taxitrips_bronze** table. Confirm that the most recent version of the table contains *21,932* rows.

-- COMMAND ----------

<FILL_IN>

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC SELECT count(*) as totalrows
-- MAGIC FROM taxitrips_bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 17. Drop the **taxitrips_bronze** table.

-- COMMAND ----------

<FILL_IN>

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC DROP TABLE IF EXISTS taxitrips_bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Summary
-- MAGIC By completing this lab, you should now feel comfortable:
-- MAGIC * Completing standard Delta Lake table creation and data manipulation commands
-- MAGIC * Reviewing table metadata including table history
-- MAGIC * Leverage Delta Lake versioning for snapshot queries and rollbacks
