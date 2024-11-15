# clone_to_secondary_par.py
#
# Parallelized version of clone_to_secondary.py.
#
# This script clones tables from the catalogs listed in catalogs_to_copy into the bucket specified by dest_bucket, which
# can be an S3 bucket, ADLS storage account, or GCS bucket. Tables will be tracked in a manifest file, also written to 
# dest_bucket, containing the catalog, schema and table name, table type, and write location.
#
# We assume this script is run on a Databricks cluster; if it is run locally, you may need to add additional configuration
# to set up a spark context and authenticate with your Databricks cluster.

import pandas as pd
from itertools import repeat
from concurrent.futures import ThreadPoolExecutor

# helper function to copy tables
def copy_table(catalog, schema, table_name, table_type, dest_bucket):
    try:
      sqlstring = f"CREATE TABLE delta.`{dest_bucket}/{catalog}_{schema}_{table_name}` DEEP CLONE {catalog}.{schema}.{table_name}"
      sql(sqlstring)

      # return the table params in dict; used to build manifest
      return {"catalog":catalog, 
              "schema":schema, 
              "table_name":table_name, 
              "table_type":table_type, 
              "dest_bucket":dest_bucket}
    except Exception:
      return {"catalog":catalog, 
              "schema":schema, 
              "table_name":table_name, 
              "table_type":"ERROR", 
              "dest_bucket":"N/A"}

# script inputs
catalogs_to_copy = ["my_catalog1", "my_catalog2"]
dest_bucket = "s3://path/to/intermediate/location"
manifest_name = "manifest"
num_exec = 4

# initialize lists
copied_table_names = []
copied_table_types = []
copied_table_schemas = []
copied_table_catalogs = []
copied_table_locations = []
system_info = sql("SELECT * FROM system.information_schema.tables")

# loop through all catalogs to copy, then copy all tables excluding system tables.
for catalog in catalogs_to_copy:
  filtered_tables = system_info.filter((system_info.table_catalog == catalog) & (system_info.table_schema != "information_schema")).collect()

  # get schemas, tables and types in list form
  schemas = [row['table_schema'] for row in filtered_tables]
  table_names = [row['table_name'] for row in filtered_tables]
  table_types = [row['table_type'] for row in filtered_tables]  

  # use ThreadPoolExecutor to copy tables in parallel
  with ThreadPoolExecutor(max_workers = num_exec) as executor:
    threads = executor.map(copy_table,
                           repeat(catalog),
                           schemas,
                           table_names,
                           table_types,
                           repeat(dest_bucket))
    
    # wait for threads to execute and build lists for manifest
    for thread in threads:
      copied_table_names.append(thread["table_name"])
      copied_table_types.append(thread["table_type"])
      copied_table_schemas.append(thread["schema"])
      copied_table_catalogs.append(thread["catalog"])
      copied_table_locations.append("{}/{}_{}_{}".format(thread["dest_bucket"],thread["catalog"],thread["schema"],thread["table_name"]))
      print("Copied table {}.{}.{}.".format(thread["catalog"],thread["schema"],thread["table_name"]))

# create the manifest as a df and write to a table in dr target
# this contains catalog, schema, table and location
manifest_df = pd.DataFrame({"catalog": copied_table_catalogs, 
                            "schema": copied_table_schemas, 
                            "table": copied_table_names, 
                            "location": copied_table_locations,
                            "type": copied_table_types})

spark.createDataFrame(manifest_df).write.mode("overwrite").format("delta").save(f"{dest_bucket}/{manifest_name}")
