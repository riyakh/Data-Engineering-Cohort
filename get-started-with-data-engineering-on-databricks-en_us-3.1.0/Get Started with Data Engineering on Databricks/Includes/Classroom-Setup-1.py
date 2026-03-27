# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

import warnings
warnings.filterwarnings("ignore")
import requests

import numpy as np
np.set_printoptions(precision=2)

import logging
logging.getLogger("tensorflow").setLevel(logging.ERROR)

# COMMAND ----------

import pandas as pd

from pyspark.sql import functions as F
from pyspark.sql.functions import col, when
import pyspark.pandas as ps
import time
import re

#from databricks.feature_engineering import FeatureEngineeringClient

#from sklearn.model_selection import train_test_split

# COMMAND ----------

# Initialize DBAcademyHelper
DA = DBAcademyHelper() 
DA.init()

# COMMAND ----------

@DBAcademyHelper.add_method
def create_demo_table(self):
        
    my_catalog = DA.catalog_name
    my_schema = DA.schema_name
    my_volume = DA.paths.working_dir
    my_table = "wine_quality_table"
    full_table_name = f"{my_catalog}.{my_schema}.{my_table}"

    spark.sql(f"USE CATALOG {my_catalog}")
    spark.sql(f"USE SCHEMA {my_schema}")

    # Drop the table if it exists
    if spark.catalog.tableExists(full_table_name):
        spark.sql(f"DROP TABLE {full_table_name}")
        print(f"Delta table dropped at: {full_table_name}")

    data_path = "https://raw.githubusercontent.com/riyakh/Data-Engineering-Cohort/main/Data/wine_quality.csv"
    local_path = f"{my_volume}/wine_quality.csv"

    # Download with rate limit handling
    time.sleep(1)
    r = requests.get(data_path)
    if r.status_code != 200 or len(r.content) < 1000:
        raise Exception(f"Failed to download file: {r.status_code} - {r.text[:200]}")

    with open(local_path, "wb") as f:
        f.write(r.content)
    print(f"File downloaded to: {local_path}")

    df = (spark.sql(f"""
        SELECT * FROM read_files(
            '{local_path}',
            format => 'csv',
            header => true
        )
    """)
        .drop("_rescued_data")
        .withColumn("wine_id", F.monotonically_increasing_id())
        .select(
            "wine_id",
            "fixed_acidity",
            "volatile_acidity",
            "citric_acid",
            "residual_sugar",
            "chlorides",
            "free_sulfur_dioxide",
            "total_sulfur_dioxide",
            "density",
            "pH",
            "sulphates",
            "alcohol",
            "quality",
        )
    )

    df.write.format("delta").mode("overwrite").saveAsTable(my_table)

    print(f"Delta table created at: {full_table_name}")
