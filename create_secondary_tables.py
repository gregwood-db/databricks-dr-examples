# create_secondary_tables.py
#
# This script creates tables in the secondary workspace based on the manifest file created by 
# clone_to_secondary or clone_to_secondary_par. It should be run in the secondary workspace, or using
# a local profile connected to the secondary workspace. The dest_bucket and manifest variables
# should be the same as the clone_to_secondary values.

dest_bucket = "s3://path/to/intermediate/location"
manifest_name = "manifest"

# read the manifest table
manifest_df = spark.read.format("delta").load(f"{source_bucket}/{manifest_name}")

# loop through manifest and create each table
for row in manifest_df.collect():
    catalog = row['catalog']
    schema = row['schema']
    table_name = row['table']
    location = row["location"]
    tbl_type = row["type"]

    if tbl_type == "MANAGED":
        print(f"Creating MANAGED table {catalog}.{schema}.{table_name}...")
        sqlstring = f"CREATE TABLE {catalog}.{schema}.{table_name} DEEP CLONE delta.`{location}`"
    elif tbl_type == "EXTERNAL":
        print(f"Creating EXTERNAL table {catalog}.{schema}.{table_name}...")
        sqlstring = f"CREATE TABLE {catalog}.{schema}.{table_name} USING delta LOCATION '{location}'"
    else:
        print(f"Skipping table {catalog}.{schema}.{table_name}; please check manifest file.")
