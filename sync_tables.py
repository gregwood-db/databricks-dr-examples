# sync_tables.py
#
# Baseline script to sync tables from a primary workspace to a secondary workspace.
#
# NOTE: This script must be run in the PRIMARY workspace. This simplifies and accelerates system table fetch and
# spark writes to the target bucket.
#
# This script will attempt to use DEEP CLONE on all tables within the specified catalog(s), and will then create those
# tables in the secondary metastore, within the same catalog and schema. The catalogs and schemas should already be
# created in the secondary metastore by using, i.e., sync_catalogs_and_schemas.py.
#
# Please note that this script uses Severless compute by default to avoid waiting for classic warehouse startup times.
#
# Params that must be specified below:
#   -landing_zone_url: the bucket, storage account, etc. where data will be written. This _must_ be in the secondary
#    region, not the primary region. It _must_ be accessible from both the primary and secondary workspace.
#   -source_host: the hostname of the primary workspace.
#   -source_pat: an access token for the primary workspace; must be an ADMIN user.
#   -target_host: the hostname of the secondary workspace.
#   -target_pat: an access token for the secondary workspace; must be an ADMIN user.
#   -catalogs_to_copy: a list of the catalogs to be replicated between workspaces.
#   -manifest_name: the name of the manifest file that will be generated to track table copies.
#   -num_exec: the number of threads to spawn in the ThreadPoolExecutor.
#   -warehouse_size: the size of the serverless warehouse to be created.
#
# To improve throughput, this script uses TheadPoolExecutors to parallelize submission of statements to the databricks
# warehouse. Table load statuses will be written to the delta table at {landing_zone_url}/sync_status_{time.time_ns()}.


import time
import pandas as pd
from itertools import repeat
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql as dbsql
from concurrent.futures import ThreadPoolExecutor
from databricks.sdk.service.sql import Disposition
from databricks.sdk.service.sql import StatementState
from databricks.sdk.service.sql import CreateWarehouseRequestWarehouseType
from databricks.sdk.service.sql import ExecuteStatementRequestOnWaitTimeout


# helper function to copy tables
def copy_table(w, catalog, schema, table_name, table_type, bucket, warehouse):
    try:
        sqlstring = f"CREATE OR REPLACE TABLE delta.`{bucket}/{catalog}_{schema}_{table_name}` DEEP CLONE {catalog}.{schema}.{table_name}"
        resp = w.statement_execution.execute_statement(warehouse_id=warehouse,
                                                       wait_timeout="0s",
                                                       on_wait_timeout=ExecuteStatementRequestOnWaitTimeout("CONTINUE"),
                                                       disposition=Disposition("EXTERNAL_LINKS"),
                                                       statement=sqlstring)

        while resp.status.state in {StatementState.PENDING, StatementState.RUNNING}:
            resp = w.statement_execution.get_statement(resp.statement_id)
            time.sleep(response_backoff)

        if resp.status.state != StatementState.SUCCEEDED:
            return {"catalog": catalog,
                    "schema": schema,
                    "table_name": table_name,
                    "table_type": f"COPY_ERROR: {resp.status.error.message}",
                    "location": "N/A"}

        # return the table params in dict; used to build manifest
        return {"catalog": catalog,
                "schema": schema,
                "table_name": table_name,
                "table_type": table_type,
                "location": bucket}

    except Exception as e:
        return {"catalog": catalog,
                "schema": schema,
                "table_name": table_name,
                "table_type": f"COPY_ERROR: {e}",
                "location": "N/A"}


# helper function to drop external tables
def drop_table(w, catalog, schema, table_name, warehouse):
    print(f"Dropping table {catalog}.{schema}.{table_name}...")

    try:
        sqlstring = f"DROP TABLE IF EXISTS {catalog}.{schema}.{table_name}"
        resp = w.statement_execution.execute_statement(warehouse_id=warehouse,
                                                       wait_timeout="0s",
                                                       on_wait_timeout=ExecuteStatementRequestOnWaitTimeout("CONTINUE"),
                                                       disposition=Disposition("EXTERNAL_LINKS"),
                                                       statement=sqlstring)

        while resp.status.state in {StatementState.PENDING, StatementState.RUNNING}:
            resp = w.statement_execution.get_statement(resp.statement_id)
            time.sleep(response_backoff)

        if resp.status.state != StatementState.SUCCEEDED:
            return {"status": 0,
                    "catalog": catalog,
                    "schema": schema,
                    "table_name": table_name}

        return {"status": 1,
                "catalog": catalog,
                "schema": schema,
                "table_name": table_name}

    except Exception:
        return {"status": 0,
                "catalog": catalog,
                "schema": schema,
                "table_name": table_name}


# helper function to load tables from a specified location
def load_table(w, catalog, schema, table_name, table_type, location, warehouse):
    if table_type == "MANAGED":
        print(f"Creating MANAGED table {catalog}.{schema}.{table_name}...")
        try:
            sqlstring = f"CREATE OR REPLACE TABLE {catalog}.{schema}.{table_name} DEEP CLONE delta.`{location}`"
            resp = w.statement_execution.execute_statement(warehouse_id=warehouse,
                                                           wait_timeout="0s",
                                                           on_wait_timeout=ExecuteStatementRequestOnWaitTimeout(
                                                               "CONTINUE"),
                                                           disposition=Disposition("EXTERNAL_LINKS"),
                                                           statement=sqlstring)

            while resp.status.state in {StatementState.PENDING, StatementState.RUNNING}:
                resp = w.statement_execution.get_statement(resp.statement_id)
                time.sleep(response_backoff)

            if resp.status.state != StatementState.SUCCEEDED:
                return {"catalog": catalog,
                        "schema": schema,
                        "table_name": table_name,
                        "table_type": table_type,
                        "location": location,
                        "status": f"FAIL: {resp.status.error.message}",
                        "creation_time": time.time_ns()}

            return {"catalog": catalog,
                    "schema": schema,
                    "table_name": table_name,
                    "table_type": table_type,
                    "location": location,
                    "status": "SUCCESS",
                    "creation_time": time.time_ns()}

        except Exception as e:
            return {"catalog": catalog,
                    "schema": schema,
                    "table_name": table_name,
                    "table_type": table_type,
                    "location": location,
                    "status": f"FAIL: {e}",
                    "creation_time": time.time_ns()}

    elif table_type == "EXTERNAL":
        print(f"Creating EXTERNAL table {catalog}.{schema}.{table_name}...")

        try:
            # must drop table if it exists; CREATE_OR_REPLACE does not work when specifying external location
            sqlstring = f"CREATE TABLE {catalog}.{schema}.{table_name} USING delta LOCATION '{location}'"
            resp = w.statement_execution.execute_statement(warehouse_id=warehouse,
                                                           wait_timeout="0s",
                                                           on_wait_timeout=ExecuteStatementRequestOnWaitTimeout("CONTINUE"),
                                                           disposition=Disposition("EXTERNAL_LINKS"),
                                                           statement=sqlstring)

            while resp.status.state in {StatementState.PENDING, StatementState.RUNNING}:
                resp = w.statement_execution.get_statement(resp.statement_id)
                time.sleep(response_backoff)

            if resp.status.state != StatementState.SUCCEEDED:
                return {"catalog": catalog,
                        "schema": schema,
                        "table_name": table_name,
                        "table_type": table_type,
                        "location": location,
                        "status": f"FAIL: {resp.status.error.message}",
                        "creation_time": time.time_ns()}

            return {"catalog": catalog,
                    "schema": schema,
                    "table_name": table_name,
                    "table_type": table_type,
                    "location": location,
                    "status": "SUCCESS",
                    "creation_time": time.time_ns()}

        except Exception as e:
            return {"catalog": catalog,
                    "schema": schema,
                    "table_name": table_name,
                    "table_type": table_type,
                    "location": location,
                    "status": f"FAIL: {e}",
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
landing_zone_url = "<my_bucket_url>"
source_host = "<primary-workspace-url>"
source_pat = "<primary-workspace-pat>"
target_host = "<secondary-workspace-url>"
target_pat = "<secondary-workspace-pat>"
catalogs_to_copy = ["my-catalog1", "my-catalog2"]
manifest_name = "manifest"
num_exec = 4
warehouse_size = "Large"

# other parameters
wh_type = CreateWarehouseRequestWarehouseType("PRO")  # required for serverless warehouse
response_backoff = 0.5  # backoff for checking query state

# initialize lists
copied_table_names = []
copied_table_types = []
copied_table_schemas = []
copied_table_catalogs = []
copied_table_locations = []

# create the WorkspaceClient pointed at the source WS
w_source = WorkspaceClient(host=source_host, token=source_pat)

# create warehouse in the primary workspace
print("Creating warehouse in primary workspace...")
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
                               repeat(landing_zone_url),
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
ts1 = time.time_ns()
(spark.createDataFrame(manifest_df)
 .write.mode("overwrite")
 .format("delta")
 .save(f"{landing_zone_url}/{manifest_name}-{ts1}"))

# create the WorkspaceClient pointed at the target WS
w_target = WorkspaceClient(host=target_host, token=target_pat)

# create warehouse to run table creation statements
print("Creating warehouse in secondary workspace...")
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

# drop external tables before loading due to CREATE TABLE restrictions
external_df = manifest_df[manifest_df['type'] == 'EXTERNAL']
with ThreadPoolExecutor(max_workers=num_exec) as executor:
    threads = executor.map(drop_table,
                           repeat(w_target),
                           list(external_df['catalog']),
                           list(external_df['schema']),
                           list(external_df['table']),
                           repeat(wh_target.id))

    for thread in threads:
        if thread["status"]:
            print("Dropped table {}.{}.{}.".format(thread["catalog"], thread["schema"], thread["table_name"]))
        else:
            print("Error dropping table {}.{}.{}.".format(thread["catalog"], thread["schema"], thread["table_name"]))

# load all tables
with ThreadPoolExecutor(max_workers=num_exec) as executor:
    threads = executor.map(load_table,
                           repeat(w_target),
                           list(manifest_df['catalog']),
                           list(manifest_df['schema']),
                           list(manifest_df['table']),
                           list(manifest_df['type']),
                           list(manifest_df['location']),
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
                          "sync_time": loaded_table_times})

# table will get a specific timestamp-based location per run
ts2 = time.time_ns()
(spark.createDataFrame(status_df)
 .write.mode("overwrite")
 .format("delta")
 .save(f"{landing_zone_url}/sync_status_{ts2}"))
