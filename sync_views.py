# sync_tables.py
#
# *EXAMPLE* Script to sync views between workspaces. This will very likely need to be altered in your environment to
# match the use cases, syntax styles, etc. that you use. Please do NOT expect this to work directly.
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
#   -landing_zone_url: the bucket, storage account, etc. where sync status will be written.
#   -target_host: the hostname of the secondary workspace.
#   -target_pat: an access token for the secondary workspace; must be an ADMIN user.
#   -catalogs_to_copy: a list of the catalogs to be replicated between workspaces.
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


# helper function to create a view
def create_view(w, catalog, schema, view_name, warehouse):

    try:
        view_stmt = sql(f"show create table {catalog}.{schema}.{view_name}").collect()[0]["createtab_stmt"]

        resp = w.statement_execution.execute_statement(warehouse_id=warehouse,
                                                       wait_timeout="0s",
                                                       on_wait_timeout=ExecuteStatementRequestOnWaitTimeout("CONTINUE"),
                                                       disposition=Disposition("EXTERNAL_LINKS"),
                                                       statement=view_stmt)

        while resp.status.state in {StatementState.PENDING, StatementState.RUNNING}:
            resp = w.statement_execution.get_statement(resp.statement_id)
            time.sleep(response_backoff)

        if resp.status.state != StatementState.SUCCEEDED:
            return {"catalog": catalog,
                    "schema": schema,
                    "view_name": view_name,
                    "status": f"FAIL: {resp.status.error.message}",
                    "creation_time": time.time_ns()}

        return {"catalog": catalog,
                "schema": schema,
                "view_name": view_name,
                "status": "SUCCESS",
                "creation_time": time.time_ns()}

    except Exception as e:
        return {"catalog": catalog,
                "schema": schema,
                "view_name": view_name,
                f"status": f"FAIL: {e}",
                "creation_time": time.time_ns()}


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

# pull all views from source ws
all_views = sql("SELECT * FROM system.information_schema.views")

# create the WorkspaceClient pointed at the target WS
w_target = WorkspaceClient(host=target_host, token=target_pat)

# create warehouse to run view creation statements
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
loaded_view_names = []
loaded_view_schemas = []
loaded_view_catalogs = []
loaded_view_status = []
loaded_view_times = []

# load all views per catalog
for cat in catalogs_to_copy:
    filtered_views = all_views.filter(
        (all_views.table_catalog == cat) &
        all_views.table_schema != "information_schema").collect()

    # get schemas and view names
    schemas = [row['table_schema'] for row in filtered_views]
    view_names = [row['table_name'] for row in filtered_views]

    with ThreadPoolExecutor(max_workers=num_exec) as executor:
        threads = executor.map(create_view,
                               repeat(w_target),
                               repeat(cat),
                               schemas,
                               view_names,
                               repeat(wh_target.id))

        for thread in threads:
            loaded_view_names.append(thread["view_name"])
            loaded_view_schemas.append(thread["schema"])
            loaded_view_catalogs.append(thread["catalog"])
            loaded_view_status.append(thread["status"])
            loaded_view_times.append(thread["creation_time"])
            print("Loaded view {}.{}.{}.".format(thread["catalog"], thread["schema"], thread["view_name"]))

# create the table statuses as a df and write to a table in dr target
status_df = pd.DataFrame({"catalog": loaded_view_catalogs,
                          "schema": loaded_view_schemas,
                          "table": loaded_view_names,
                          "status": loaded_view_status,
                          "sync_time": loaded_view_times})

# table will get a specific timestamp-based location per run
ts = time.time_ns()
(spark.createDataFrame(status_df)
 .write.mode("overwrite")
 .format("delta")
 .save(f"{landing_zone_url}/view_sync_status_{ts}"))
