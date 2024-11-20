# sync_tables.py
#
# Baseline script to sync tables from a primary workspace to a secondary workspace.
#
# NOTE: This script must be run in the PRIMARY workspace. This simplifies and accelerates system table fetch and writes
# spark writes to the target bucket.
#
# This script will attempt to use DEEP CLONE on all tables within the specified catalog(s), and will then create those
# tables in the secondary metastore, within the same catalog and schema. The catalogs and schemas should already be
# created in the secondary metastore by using, i.e., sync_catalogs_and_schemas.py.
#
# Please note that this script uses Severless compute by default to avoid waiting for classic warehouse startup times.
#
# Params that must be specified below:
#   -target_bucket: the bucket, storage account, etc. where data will be written. This _must_ be in the secondary
#    region, not the primary region.
#   -primary_host: the hostname of the primary workspace.
#   -primary_pat: an access token for the primary workspace; must be an ADMIN user.
#   -secondary_host: the hostname of the secondary workspace.
#   -secondary_pat: an access token for the secondary workspace; must be an ADMIN user.
#   -catalogs_to_copy: a list of the catalogs to be replicated between workspaces.
#   -manifest_name: the name of the manifest file that will be generated to track table copies.
#   -num_exec: the number of threads to spawn in the ThreadPoolExecutor.
#   -warehouse_size: the size of the serverless warehouse to be created.
#
# To improve throughput, this script uses TheadPoolExecutors to parallelize submission of statements to the databricks
# warehouse. All table load statuses will be written to the delta table at {target_bucket}/sync_status_{time.time_ns()}.


import time
import pandas as pd
from itertools import repeat
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql as dbsql
from concurrent.futures import ThreadPoolExecutor
from databricks.sdk.service.sql import CreateWarehouseRequestWarehouseType
from databricks.sdk.service.sql import Disposition
from databricks.sdk.service.sql import ExecuteStatementRequestOnWaitTimeout


# helper function to copy tables
def copy_table(w, catalog, schema, table_name, table_type, bucket, warehouse):
    try:
        sqlstring = f"CREATE TABLE delta.`{bucket}/{catalog}_{schema}_{table_name}` DEEP CLONE {catalog}.{schema}.{table_name}"
        w.statement_execution.execute_statement(warehouse_id=warehouse,
                                                wait_timeout="0s",
                                                on_wait_timeout=ExecuteStatementRequestOnWaitTimeout("CONTINUE"),
                                                disposition=Disposition("EXTERNAL_LINKS"),
                                                statement=sqlstring)

        # return the table params in dict; used to build manifest
        return {"catalog": catalog,
                "schema": schema,
                "table_name": table_name,
                "table_type": table_type,
                "location": bucket}

    except Exception:
        return {"catalog": catalog,
                "schema": schema,
                "table_name": table_name,
                "table_type": "COPY_ERROR",
                "location": "N/A"}


# helper function to load tables from a specified location
def load_table(w, catalog, schema, table_name, table_type, location, warehouse):
    if table_type == "MANAGED":
        print(f"Creating MANAGED table {catalog}.{schema}.{table_name}...")
        sqlstring = f"CREATE OR REPLACE TABLE {catalog}.{schema}.{table_name} DEEP CLONE delta.`{location}`"
        w.statement_execution.execute_statement(warehouse_id=warehouse,
                                                wait_timeout="0s",
                                                on_wait_timeout=ExecuteStatementRequestOnWaitTimeout("CONTINUE"),
                                                disposition=Disposition("EXTERNAL_LINKS"),
                                                statement=sqlstring)
        return {"catalog": catalog,
                "schema": schema,
                "table_name": table_name,
                "table_type": table_type,
                "location": location,
                "status": "SUCCESS",
                "creation_time": time.time_ns()}

    elif table_type == "EXTERNAL":
        print(f"Creating EXTERNAL table {catalog}.{schema}.{table_name}...")
        sqlstring = f"CREATE OR REPLACE TABLE {catalog}.{schema}.{table_name} USING delta LOCATION '{location}'"
        w.statement_execution.execute_statement(warehouse_id=warehouse,
                                                wait_timeout="0s",
                                                on_wait_timeout=ExecuteStatementRequestOnWaitTimeout("CONTINUE"),
                                                disposition=Disposition("EXTERNAL_LINKS"),
                                                statement=sqlstring)
        return {"catalog": catalog,
                "schema": schema,
                "table_name": table_name,
                "table_type": table_type,
                "location": location,
                "status": "SUCCESS",
                "creation_time": time.time_ns()}

    else:
        print(f"Skipping table {catalog}.{schema}.{table_name}; please check manifest file.")
        return {"catalog": catalog,
                "schema": schema,
                "table_name": table_name,
                "table_type": table_type,
                "location": location,
                "status": "FAILURE",
                "creation_time": "N/A"}


# script inputs
target_bucket = "<my_bucket_url>"
primary_host = "<primary-workspace-url>"
primary_pat = "<primary-workspace-pat>"
secondary_host = "<secondary-workspace-url>"
secondary_pat = "<secondary-workspace-pat>"
catalogs_to_copy = ["my-catalog1", "my-catalog2"]
manifest_name = "manifest"
num_exec = 4
warehouse_size = "Large"
wh_type = CreateWarehouseRequestWarehouseType("PRO")

# initialize lists
copied_table_names = []
copied_table_types = []
copied_table_schemas = []
copied_table_catalogs = []
copied_table_locations = []

# create the WorkspaceClient pointed at the source WS
w_source = WorkspaceClient(host=primary_host, token=primary_pat)

wh_source = w_source.warehouses.create(name=f'sdk-{time.time_ns()}',
                                       cluster_size=warehouse_size,
                                       max_num_clusters=1,
                                       auto_stop_mins=10,
                                       warehouse_type=wh_type,
                                       enable_serverless_compute=True,
                                       tags=dbsql.EndpointTags(
                                           custom_tags=[
                                               dbsql.EndpointTagPair(key="Owner", value="dr-sync-tool")])).result()

system_info = sql("SELECT * FROM system.information_schema.tables")

# loop through all catalogs to copy, then copy all tables excluding system tables.
# we also skip views; these need to be created separately since they cannot be cloned.
for cat in catalogs_to_copy:
    filtered_tables = system_info.filter(
        (system_info.table_catalog == cat) &
        (system_info.table_schema != "information_schema") &
        (system_info.table_type != "VIEW")).collect()

    # get schemas, tables and types in list form
    schemas = [row['table_schema'] for row in filtered_tables]
    table_names = [row['table_name'] for row in filtered_tables]
    table_types = [row['table_type'] for row in filtered_tables]

    # use ThreadPool to copy tables in parallel
    with ThreadPoolExecutor(max_workers=num_exec) as executor:
        threads = executor.map(copy_table,
                               repeat(w_source),
                               repeat(cat),
                               schemas,
                               table_names,
                               table_types,
                               repeat(target_bucket),
                               repeat(wh_source.id))

        # wait for threads to execute and build lists for manifest
        for thread in threads:
            copied_table_names.append(thread["table_name"])
            copied_table_types.append(thread["table_type"])
            copied_table_schemas.append(thread["schema"])
            copied_table_catalogs.append(thread["catalog"])
            copied_table_locations.append(
                "{}/{}_{}_{}".format(thread["location"], thread["catalog"], thread["schema"], thread["table_name"]))
            print("Copied table {}.{}.{}.".format(thread["catalog"], thread["schema"], thread["table_name"]))

# create the manifest as a df and write to a table in dr target
# this contains catalog, schema, table and location
manifest_df = pd.DataFrame({"catalog": copied_table_catalogs,
                            "schema": copied_table_schemas,
                            "table": copied_table_names,
                            "location": copied_table_locations,
                            "type": copied_table_types})

# write the manifest to the target bucket in case it needs to be accessed later
(spark.createDataFrame(manifest_df)
 .write.mode("overwrite")
 .format("delta")
 .save(f"{target_bucket}/{manifest_name}-{time.time_ns()}"))

# create the WorkspaceClient pointed at the target WS
w_target = WorkspaceClient(host=secondary_host, token=secondary_pat)

# create warehouse to run table creation statements
wh_target = w_target.warehouses.create(name=f'sdk-{time.time_ns()}',
                                       cluster_size=warehouse_size,
                                       max_num_clusters=1,
                                       auto_stop_mins=10,
                                       warehouse_type=wh_type,
                                       enable_serverless_compute=True,
                                       tags=dbsql.EndpointTags(
                                           custom_tags=[
                                               dbsql.EndpointTagPair(key="Owner", value="dr-sync-tool")])).result()

# initialize lists for status tracking
loaded_table_names = []
loaded_table_types = []
loaded_table_schemas = []
loaded_table_catalogs = []
loaded_table_locations = []
loaded_table_status = []
loaded_table_times = []

# create lists of table params for executor submission
collected_manifest = manifest_df.collect()
tbl_catalogs = [row['catalog'] for row in collected_manifest]
tbl_schemas = [row['schema'] for row in collected_manifest]
tbl_names = [row['table'] for row in collected_manifest]
tbl_locs = [row["location"] for row in collected_manifest]
tbl_types = [row["type"] for row in collected_manifest]

# use ThreadPool to load tables in parallel
with ThreadPoolExecutor(max_workers=num_exec) as executor:
    threads = executor.map(load_table,
                           repeat(w_target),
                           tbl_catalogs,
                           tbl_schemas,
                           tbl_names,
                           tbl_types,
                           tbl_locs,
                           repeat(wh_target.id))

    for thread in threads:
        loaded_table_names.append(thread["table_name"])
        loaded_table_types.append(thread["table_type"])
        loaded_table_schemas.append(thread["schema"])
        loaded_table_catalogs.append(thread["catalog"])
        loaded_table_locations.append(thread["location"])
        loaded_table_status.append(thread["status"])
        loaded_table_times.append(thread["creation_time"])
        print("Loaded table {}.{}.{}.".format(thread["catalog"], thread["schema"], thread["table_name"]))

# create the table statuses as a df and write to a table in dr target
status_df = pd.DataFrame({"catalog": loaded_table_catalogs,
                          "schema": loaded_table_schemas,
                          "table": loaded_table_names,
                          "location": loaded_table_locations,
                          "type": loaded_table_types,
                          "status": loaded_table_status,
                          "create_time": loaded_table_times})

# table will get a specific timestamp-based location per run
(spark.createDataFrame(status_df)
 .write.mode("overwrite")
 .format("delta")
 .save(f"{target_bucket}/sync_status_{time.time_ns()}"))
