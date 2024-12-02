# sync_grs_ext.py
#
# Baseline script to sync GRS-replicated tables from a primary metastore to a secondary metastore
#
# NOTE: This script must be run in the PRIMARY workspace. This simplifies and accelerates system table fetch and writes
# spark writes to the target bucket.
#
# This script will attempt to register all _external_ tables in the primary metastore into the secondary metastore. This
# assumes that all storage locations are identical between the two regions, i.e., georeplicated storage has been used.
# Storage URLs are not updated; they are just directly brought over to the secondary metastore.
#
# Please note that this script uses Severless compute by default to avoid waiting for classic warehouse startup times.
#
# Params that must be specified below:
#   -landing_zone_url: the bucket, storage account, etc. where the status table will be written
#   -target_host: the hostname of the secondary workspace.
#   -target_pat: an access token for the secondary workspace; must be an ADMIN user.
#   -catalogs_to_copy: a list of the catalogs to be replicated between workspaces.
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
from databricks.sdk.service.sql import Disposition
from databricks.sdk.service.sql import StatementState
from databricks.sdk.service.sql import CreateWarehouseRequestWarehouseType
from databricks.sdk.service.sql import ExecuteStatementRequestOnWaitTimeout


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
def load_table(w, catalog, schema, table_name, location, warehouse):

    print(f"Creating EXTERNAL table {catalog}.{schema}.{table_name}...")

    try:
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
                    "location": location,
                    "status": f"FAIL: {resp.status.error.message}",
                    "creation_time": time.time_ns()}

        return {"catalog": catalog,
                "schema": schema,
                "table_name": table_name,
                "location": location,
                "status": "SUCCESS",
                "creation_time": time.time_ns()}

    except Exception as e:
        return {"catalog": catalog,
                "schema": schema,
                "table_name": table_name,
                "location": location,
                "status": "FAIL: {e}",
                "creation_time": time.time_ns()}


# script inputs
landing_zone_url = "path/to/storage/"
target_host = "<secondary-workspace-url>"
target_pat = "<secondary-workspace-pat>"
catalogs_to_copy = ["my-catalog1", "my-catalog2"]
num_exec = 4
warehouse_size = "Small"

# other parameters
wh_type = CreateWarehouseRequestWarehouseType("PRO")  # required for serverless warehouse
response_backoff = 0.5  # backoff for checking query state

# initialize lists for status tracking
loaded_table_names = []
loaded_table_types = []
loaded_table_schemas = []
loaded_table_catalogs = []
loaded_table_locations = []
loaded_table_status = []
loaded_table_times = []

# create the WorkspaceClient pointed at the target WS
w_target = WorkspaceClient(host=target_host, token=target_pat)

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

system_info = sql("SELECT * FROM system.information_schema.tables")

# loop through all catalogs to copy, then copy all tables excluding system tables.
# we also skip views; these need to be created separately since they cannot be cloned.
for cat in catalogs_to_copy:
    filtered_tables = system_info.filter(
        (system_info.table_catalog == cat) &
        (system_info.table_schema != "information_schema") &
        (system_info.table_type == "EXTERNAL")).collect()

    # get schemas, tables and types in list form
    schemas = [row['table_schema'] for row in filtered_tables]
    table_names = [row['table_name'] for row in filtered_tables]
    table_locs = [row['storage_path'] for row in filtered_tables]

    with ThreadPoolExecutor(max_workers=num_exec) as executor:
        threads = executor.map(drop_table,
                               repeat(w_target),
                               repeat(cat),
                               schemas,
                               table_names,
                               repeat(wh_target.id))

        for thread in threads:
            if thread["status"]:
                print("Dropped table {}.{}.{}.".format(thread["catalog"], thread["schema"], thread["table_name"]))
            else:
                print(
                    "Error dropping table {}.{}.{}.".format(thread["catalog"], thread["schema"], thread["table_name"]))

    # use ThreadPool to copy tables in parallel
    with ThreadPoolExecutor(max_workers=num_exec) as executor:
        threads = executor.map(load_table,
                               repeat(w_target),
                               repeat(cat),
                               schemas,
                               table_names,
                               table_locs,
                               repeat(wh_target.id))

        # wait for threads to execute and build lists for status table
        for thread in threads:
            loaded_table_names.append(thread["table_name"])
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
 .save(f"{landing_zone_url}/sync_status_{time.time_ns()}"))
