# sync_shared_tables.py
#
# This script will create and update a share in the secondary metastore (if it does not exist) and then use DEEP CLONE
# to replicate data to the secondary region. The target catalogs must already exist in the secondary metastore, i.e.,
# sync_catalogs_and_schemas.py should already have been run.
#
# Notes:
#   - ALL tables are created as managed tables when using this script. You can change this behavior by changing the
#     filter conditions on the system table (i.e., use table_type to filter filtered_tables).
#   - Your environment may need to be altered to allow SAS/S3 Presigned URL traffic. This is often the case if a
#     firewall is configured; be sure to check your rules if you receive errors when trying to clone.
#   - This script uses Serverless compute by default to avoid waiting for classic warehouse startup times.
#
# Params that must be specified below:
#   -source_host: the hostname of the primary workspace.
#   -source_pat: an access token for the primary workspace; must be an ADMIN user.
#   -target_host: the hostname of the secondary workspace.
#   -target_pat: an access token for the secondary workspace; must be an ADMIN user.
#   -catalogs_to_copy: a list of the catalogs to be replicated between workspaces.
#   -num_exec: the number of threads to spawn in the ThreadPoolExecutor.
#   -target_share_id: the sharing identifier of the secondary metastore.

import sys
import time
import pandas as pd
from itertools import repeat
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql as dbsql
from concurrent.futures import ThreadPoolExecutor
from databricks.sdk.service.sql import (Disposition, StatementState,
                                        CreateWarehouseRequestWarehouseType, ExecuteStatementRequestOnWaitTimeout)
from databricks.sdk.errors.platform import BadRequest
from databricks.sdk.service.catalog import Privilege, PermissionsChange
from databricks.sdk.service.sharing import (AuthenticationType, SharedDataObjectUpdate,
                                            SharedDataObjectUpdateAction, SharedDataObject,
                                            SharedDataObjectDataObjectType, SharedDataObjectStatus)


# helper function to clone a table from one catalog to another
def clone_table(w, source_catalog, target_catalog, schema, table_name, warehouse):

    print(f"Cloning table {source_catalog}.{schema}.{table_name}...")
    try:
        sqlstring = (f"CREATE OR REPLACE TABLE {target_catalog}.{schema}.{table_name} "
                     f"DEEP CLONE {source_catalog}.{schema}.{table_name}")

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
            return {"catalog": target_catalog,
                    "schema": schema,
                    "table_name": table_name,
                    "status": f"FAIL: {resp.status.error.message}",
                    "creation_time": time.time_ns()}

        return {"catalog": target_catalog,
                "schema": schema,
                "table_name": table_name,
                "status": "SUCCESS",
                "creation_time": time.time_ns()}

    except Exception as e:
        return {"catalog": target_catalog,
                "schema": schema,
                "table_name": table_name,
                "status": f"FAIL: {e}",
                "creation_time": time.time_ns()}


# script inputs
source_host = "<primary-workspace-url>"
source_pat = "<primary-workspace-pat>"
target_host = "<secondary-workspace-url>"
target_pat = "<secondary-workspace-pat>"
catalogs_to_copy = ["my-catalog1", "my-catalog2"]
num_exec = 4
metastore_id = "<secondary-metastore-id>"
warehouse_size = "Large"
landing_zone_url = "/path/to/storage"

# other parameters
wh_type = CreateWarehouseRequestWarehouseType("PRO")  # required for serverless warehouse
response_backoff = 0.5  # backoff for checking query state

# create the WorkspaceClients for source and target workspaces
w_source = WorkspaceClient(host=source_host, token=source_pat)
w_target = WorkspaceClient(host=target_host, token=target_pat)

# create warehouse in secondary to run table creation statements
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

# create the secondary metastore as a recipient
try:
    print(f"Creating recipient with id {metastore_id}...")
    recipient = w_source.recipients.create(name="dr_automation_recipient",
                                           authentication_type=AuthenticationType.DATABRICKS,
                                           data_recipient_global_metastore_id=metastore_id)
except BadRequest:
    print(f"Recipient with id {metastore_id} already exists. Skipping creation...")
    recipient = None

# get all tables in the primary metastore
system_info = spark.sql("SELECT * FROM system.information_schema.tables")

# get local metastore id
local_metastore_id = [r["current_metastore()"] for r in spark.sql("SELECT current_metastore()").collect()][0]

# get remote provider name; it may or may not be the same as local_metastore_id
try:
    remote_provider_name = [p.name for p in w_target.providers.list() if
                            p.data_provider_global_metastore_id == local_metastore_id][0]
except IndexError:
    print("Provider could not be found in target workspace; please check that it was created.")
    sys.exit()

# initalize df lists
cloned_table_names = []
cloned_table_schemas = []
cloned_table_catalogs = []
cloned_table_status = []
cloned_table_times = []

# iterate through all catalogs to share
for cat in catalogs_to_copy:
    filtered_tables = system_info.filter(
        (system_info.table_catalog == cat) &
        (system_info.table_schema != "information_schema") &
        (system_info.table_type != "VIEW")).distinct().collect()

    unique_schemas = {row['table_schema'] for row in filtered_tables}
    all_tables = [row["table_name"] for row in filtered_tables]
    all_schemas = [row["table_schema"] for row in filtered_tables]

    # create the share for the current catalog and update permissions
    print(f"Creating share for catalog {cat}...")
    try:
        share = w_source.shares.create(name=f"{cat}_share")
        _ = w_source.shares.update_permissions(share.name,
                                               changes=[PermissionsChange(add=[Privilege.SELECT],
                                                                          principal=recipient.name)])
        share_name = share.name
    except BadRequest:
        print(f"Share {cat}_share already exists. Skipping creation...")
        share_name = f"{cat}_share"

    # build update object with all schemas in the current catalog
    updates = [
        SharedDataObjectUpdate(action=SharedDataObjectUpdateAction.ADD,
                               data_object=SharedDataObject(name=f"{cat}.{schema}",
                                                            data_object_type=SharedDataObjectDataObjectType.SCHEMA,
                                                            status=SharedDataObjectStatus.ACTIVE))
        for schema in unique_schemas]

    # update the share
    try:
        _ = w_source.shares.update(share_name, updates=updates)
    except Exception as e:
        print(f"Error updating share {share_name}: {e}")

    # create the shared catalog in the target workspace
    try:
        _ = w_target.catalogs.create(name=f"{cat}_share", provider_name=remote_provider_name, share_name=share_name)
    except BadRequest:
        print(f"Shared catalog {cat}_share already exists. Skipping creation.")

    with ThreadPoolExecutor(max_workers=num_exec) as executor:
        threads = executor.map(clone_table,
                               repeat(w_target),
                               repeat(f"{cat}_share"),
                               repeat(cat),
                               all_schemas,
                               all_tables,
                               repeat(wh_target.id))

        for thread in threads:
            cloned_table_names.append(thread["table_name"])
            cloned_table_schemas.append(thread["schema"])
            cloned_table_catalogs.append(thread["catalog"])
            cloned_table_status.append(thread["status"])
            cloned_table_times.append(thread["creation_time"])

            if thread["status"] == "SUCCESS":
                print("Loaded table {}.{}.{}.".format(thread["catalog"], thread["schema"], thread["table_name"]))

# create the table statuses as a df and write to a table in dr target
status_df = pd.DataFrame({"catalog": cloned_table_catalogs,
                          "schema": cloned_table_schemas,
                          "table": cloned_table_names,
                          "status": cloned_table_status,
                          "sync_time": cloned_table_times})

# table will get a specific timestamp-based location per run
ts2 = time.time_ns()
(spark.createDataFrame(status_df)
 .write.mode("overwrite")
 .format("delta")
 .save(f"{landing_zone_url}/sync_status_{ts2}"))
