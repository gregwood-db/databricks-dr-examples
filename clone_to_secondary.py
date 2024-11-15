# clone_to_secondary.py
#
# This script clones tables from the catalogs listed in catalogs_to_copy into the bucket specified by dest_bucket, which
# can be an S3 bucket, ADLS storage account, or GCS bucket. Tables will be tracked in a manifest file, also written to 
# dest_bucket, containing the catalog, schema and table name, table type, and write location.
#
# We assume this script is run on a Databricks cluster; if it is run locally, you may need to add additional configuration
# to set up a spark context and authenticate with your Databricks cluster.

import pandas as pd

# script inputs
catalogs_to_copy = ["my_catalog1, my_catalog2"]
dest_bucket = "s3://path/to/intermediate/location"
manifest_name = "manifest"

# initialize lists
copied_table_names = []
copied_table_types = []
copied_table_schemas = []
copied_table_catalogs = []
copied_table_locations = []
system_info = sql("SELECT * FROM system.information_schema.tables")

# loop through all catalogs to copy, then copy all tables excluding system tables.
for catalog in catalogs_to_copy:
  filtered_tables = system_info.filter((system_info.table_catalog == catalog) & (system_info.table_schema != "information_schema"))
  for table in filtered_tables.collect():
    schema = table['table_schema']
    table_name = table['table_name']
    table_type = table['table_type']
    print(f"Copying table {schema}.{table_name}...")
    sqlstring = f"CREATE TABLE delta.`{dest_bucket}/{catalog}_{schema}_{table_name}` DEEP CLONE {catalog}.{schema}.{table_name}"
    sql(sqlstring)

    # the below will be used to create the manifest table in the secondary region
    copied_table_names.append(table_name)
    copied_table_types.append(table_type)
    copied_table_schemas.append(schema)
    copied_table_catalogs.append(catalog)
    copied_table_locations.append(f"{dest_bucket}/{catalog}_{schema}_{table_name}")

# create the manifest as a df and write to a table in dr target
# this contains catalog, schema, table and location
manifest_df = pd.DataFrame({"catalog": copied_table_catalogs, 
                            "schema": copied_table_schemas, 
                            "table": copied_table_names, 
                            "location": copied_table_locations,
                            "type": copied_table_types})

spark.createDataFrame(manifest_df).write.mode("overwrite").format("delta").save(f"{dest_bucket}/{manifest_name}")
display(manifest_df)
