# Databricks notebook source
# MAGIC %md
# MAGIC # Ingesting Data into Delta Lake

# COMMAND ----------

# MAGIC %md
# MAGIC ## Classroom Setup
# MAGIC
# MAGIC Run the following cell to configure your working environment for this course.
# MAGIC
# MAGIC **NOTE:** The `DA` object is only used in Databricks Academy courses and is not available outside of these courses. It will dynamically reference the information needed to run the course.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-02

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Configure and Explore Your Environment
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Setting Up catalog and Schema
# MAGIC Set the default catalog to **dbacademy** and your unique schema. Then, view the available tables to confirm that no tables currently exist in your schema.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1A. Using SQL Commands

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set the default catalog and schema
# MAGIC USE CATALOG IDENTIFIER(DA.catalog_name);
# MAGIC USE SCHEMA IDENTIFIER(DA.schema_name);
# MAGIC
# MAGIC -- Display available tables in your schema
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1B. Using PySpark

# COMMAND ----------

# Set the default catalog and schema (Requires Spark 3.4.0 or later)
spark.catalog.setCurrentCatalog(DA.catalog_name)
spark.catalog.setCurrentDatabase(DA.schema_name)

# Display available tables in your schema
spark.catalog.listTables(DA.schema_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Viewing the available files
# MAGIC View the available files in your schema's **myfiles** volume. Confirm that only the **employees.csv** file is available.
# MAGIC
# MAGIC **NOTE:** Remember, when referencing data in volumes, use the path provided by Unity Catalog, which always has the following format: */Volumes/catalog_name/schema_name/volume_name/*.

# COMMAND ----------

spark.sql(f"LIST '/Volumes/{DA.catalog_name}/{DA.schema_name}/myfiles/' ").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Delta Lake Ingestion Techniques
# MAGIC **Objective**: Create a Delta table from the **employees.csv**  file using various methods.
# MAGIC
# MAGIC - CREATE TABLE AS (`CTAS`)
# MAGIC - UPLOAD UI (`User Interface`)
# MAGIC - COPY INTO
# MAGIC - AUTOLOADER (`Overview only`, `outside the scope of this module`)

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. CREATE TABLE (CTAS)
# MAGIC 1. Create a table from the **employees.csv** file using the CREATE TABLE AS statement similar to the previous demonstration. Run the query and confirm that the **current_employees_ctas** table was successfully created.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Drop the table if it exists for demonstration purposes
# MAGIC DROP TABLE IF EXISTS current_employees_ctas;
# MAGIC
# MAGIC -- Create the table using CTAS
# MAGIC CREATE TABLE current_employees_ctas
# MAGIC AS
# MAGIC SELECT ID, FirstName, Country, Role 
# MAGIC FROM read_files(
# MAGIC   '/Volumes/' || DA.catalog_name || '/'|| DA.schema_name || '/myfiles/',
# MAGIC   format => 'csv',
# MAGIC   header => true,
# MAGIC   inferSchema => true
# MAGIC  );
# MAGIC
# MAGIC -- Display available tables in your schema
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Query the **current_employees_ctas** table and confirm that it contains 4 rows and 4 columns.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM current_employees_ctas;

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. UPLOAD UI
# MAGIC The add data UI allows you to manually load data into Databricks from a variety of sources.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Complete the following steps to manually download the **employees.csv** file from your volume:
# MAGIC
# MAGIC    a. Select the Catalog icon in the left navigation bar. 
# MAGIC
# MAGIC    b. Click on your catalog **(dbacademy)**
# MAGIC
# MAGIC    c. Select the refresh icon to refresh the **dbacademy** catalog.
# MAGIC
# MAGIC    d. Expand the **dbacademy** catalog. Within the catalog, you should see a variety of schemas (databases).
# MAGIC
# MAGIC    e. Expand your schema. You can locate your schema in the setup notes in the first cell. Notice that your schema contains **Tables** and **Volumes**.
# MAGIC
# MAGIC    f. Expand **Volumes** then **myfiles**. The **myfiles** volume should contain a single CSV file named **employees.csv**. 
# MAGIC
# MAGIC    g. Click on the kebab menu on the right-hand side of the **employees.csv** file and select **Download file.** This will download the CSV file to your browser's download folder.

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Complete the following steps to manually upload the **employees.csv** file to your schema. This will mimic loading a local file to Databricks:
# MAGIC
# MAGIC    a. In the navigation bar select your schema. 
# MAGIC
# MAGIC    b. Click the ellipses (three-dot) icon next to your schema and select **Open in Catalog Explorer**.
# MAGIC
# MAGIC    c. Select the **Create** drop down icon ![create_drop_down](../Includes/images/create_drop_down.png), and select **Table**.
# MAGIC
# MAGIC    d. Select the **employees.csv** you downloaded earlier into the available section in the browser, or select **browse**, navigate to your downloads folder and select the **employees.csv** file.

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Complete the following steps to create the Delta table using the UPLOAD UI.
# MAGIC
# MAGIC    a. In the UI confirm the table will be created in the catalog **dbacademy** and your unique schema. 
# MAGIC
# MAGIC    b. Under **Table name**, name the table **current_employees_ui**.
# MAGIC
# MAGIC    c. Select the **Create table** button at the bottom of the screen to create the table.
# MAGIC
# MAGIC    d. Confirm the table was created successfully. Then close out of the Catalog Explorer browser.
# MAGIC
# MAGIC **Example**
# MAGIC <br></br>
# MAGIC
# MAGIC ![create_table_ui](../Includes/images/create_table_ui.png)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Use the SHOW TABLES statement to view the available tables in your schema. Confirm that the **current_employees_ui** table has been created. 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %md
# MAGIC 5. Lastly, query the table to review its contents.
# MAGIC
# MAGIC **NOTE**: If you did not upload the table using the UPLOAD UI and name it **current_employees_ui** an error will be returned.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM current_employees_ui;

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. COPY INTO
# MAGIC Create a table from the **employees.csv** file using the [COPY INTO](https://docs.databricks.com/en/sql/language-manual/delta-copy-into.html) statement. 
# MAGIC
# MAGIC The `COPY INTO` statement incrementally loads data from a file location into a Delta table. This is a retryable and idempotent operation. Files in the source location that have already been loaded are skipped. This is true even if the files have been modified since they were loaded.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Create an empty table named **current_employees_copyinto** and define the column data types.
# MAGIC
# MAGIC **NOTE:** You can also create an empty table with no columns and evolve the schema with `COPY INTO`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop the table if it exists for demonstration purposes
# MAGIC DROP TABLE IF EXISTS current_employees_copyinto;
# MAGIC
# MAGIC -- Create an empty table with the column data types
# MAGIC CREATE TABLE current_employees_copyinto (
# MAGIC   ID INT,
# MAGIC   FirstName STRING,
# MAGIC   Country STRING,
# MAGIC   Role STRING
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Use the `COPY INTO` statement to load all files from the **myfiles** volume (currently only the **employees.csv** file exists) using the path provided by Unity Catalog. Confirm that the data is loaded into the **current_employees_copyinto** table.
# MAGIC
# MAGIC     Confirm the following:
# MAGIC     - **num_affected_rows** is 4
# MAGIC     - **num_inserted_rows** is 4
# MAGIC     - **num_skipped_correct_files** is 0

# COMMAND ----------

spark.sql(f'''
COPY INTO current_employees_copyinto
  FROM '/Volumes/{DA.catalog_name}/{DA.schema_name}/myfiles/'
  FILEFORMAT = CSV
  FORMAT_OPTIONS (
      'header' = 'true', 
      'inferSchema' = 'true'
    )
  ''').display()

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Query the **current_employees_copyinto** table and confirm that all 4 rows have been copied into the Delta table correctly.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM current_employees_copyinto;

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Run the `COPY INTO` statement again and confirm that it did not re-add the data from the volume that was already loaded. Remember, `COPY INTO` is a retryable and idempotent operation — Files in the source location that have already been loaded are skipped.   
# MAGIC     - **num_affected_rows** is 0
# MAGIC     - **num_inserted_rows** is 0
# MAGIC     - **num_skipped_correct_files** is 0
# MAGIC
# MAGIC

# COMMAND ----------

spark.sql(f'''
COPY INTO current_employees_copyinto
  FROM '/Volumes/{DA.catalog_name}/{DA.schema_name}/myfiles/'
  FILEFORMAT = CSV
  FORMAT_OPTIONS (
      'header' = 'true', 
      'inferSchema' = 'true'
    )
  ''').display()

# COMMAND ----------

# MAGIC %md
# MAGIC 5. Run the script below to create an additional CSV file named **employees2.csv** in your **myfiles** volume. View the results and confirm that your volume now contains two CSV files: the original **employees.csv** file and the new **employees2.csv** file.

# COMMAND ----------

## Create the new employees2.csv file in your volume
DA.create_employees_csv2()

## View the files in the your myfiles volume
files = dbutils.fs.ls(f'/Volumes/{DA.catalog_name}/{DA.schema_name}/myfiles')
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC 6. Query the new **employees2.csv** file directly. Confirm that only 2 rows exist in the CSV file.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   ID, 
# MAGIC   FirstName, 
# MAGIC   Country, 
# MAGIC   Role 
# MAGIC FROM read_files(
# MAGIC   '/Volumes/' || DA.catalog_name || '/' || DA.schema_name || '/myfiles/employees2.csv',
# MAGIC   format => 'csv',
# MAGIC   header => true,
# MAGIC   inferSchema => true
# MAGIC  );

# COMMAND ----------

# MAGIC %md
# MAGIC 7. Execute the `COPY INTO` statement again using your volume's path. Notice that only the 2 rows from the new **employees2.csv** file are added to the **current_employees_copyinto** table.
# MAGIC
# MAGIC     - **num_affected_rows** is 2
# MAGIC     - **num_inserted_rows** is 2
# MAGIC     - **num_skipped_correct_files** is 0

# COMMAND ----------

spark.sql(f'''
COPY INTO current_employees_copyinto
  FROM '/Volumes/{DA.catalog_name}/{DA.schema_name}/myfiles/'
  FILEFORMAT = CSV
  FORMAT_OPTIONS (
      'header' = 'true', 
      'inferSchema' = 'true'
    )
  ''').display()

# COMMAND ----------

# MAGIC %md
# MAGIC 8. View the updated **current_employees_copyinto** table and confirm that it now contains 6 rows, including the new data that was added.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM current_employees_copyinto;

# COMMAND ----------

# MAGIC %md
# MAGIC 9. View table's history. Notice that there are 3 versions.
# MAGIC     - **Version 0** is the initial empty table created by the CREATE TABLE statement.
# MAGIC     - **Version 1** is the first `COPY INTO` statement that loaded the **employees.csv** file into the Delta table.
# MAGIC     - **Version 2** is the second `COPY INTO` statement that only loaded the new **employees2.csv** file into the Delta table.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY current_employees_copyinto;

# COMMAND ----------

# MAGIC %md
# MAGIC ####4. AUTO LOADER
# MAGIC
# MAGIC **NOTE: Auto Loader is outside the scope of this course.**
# MAGIC
# MAGIC Auto Loader incrementally and efficiently processes new data files as they arrive in cloud storage without any additional setup.
# MAGIC
# MAGIC ![autoloader](../Includes/images/autoloader.png)
# MAGIC
# MAGIC The key benefits of using the auto loader are:
# MAGIC - No file state management: The source incrementally processes new files as they land on cloud storage. You don't need to manage any state information on what files arrived.
# MAGIC - Scalable: The source will efficiently track the new files arriving by leveraging cloud services and RocksDB without having to list all the files in a directory. This approach is scalable even with millions of files in a directory.
# MAGIC - Easy to use: The source will automatically set up notification and message queue services required for incrementally processing the files. No setup needed on your side.
# MAGIC
# MAGIC Check out the documentation
# MAGIC [What is Auto Loader](https://docs.databricks.com/en/ingestion/auto-loader/index.html) for more information.

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Cleanup
# MAGIC 1. Drop your demonstration tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS current_employees_ctas;
# MAGIC DROP TABLE IF EXISTS current_employees_ui;
# MAGIC DROP TABLE IF EXISTS current_employees_copyinto;
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Drop the **employees2.csv** file.

# COMMAND ----------

## Remove employees2.csv from the myfiles volume
dbutils.fs.rm(f"/Volumes/{DA.catalog_name}/{DA.schema_name}/myfiles/employees2.csv")
