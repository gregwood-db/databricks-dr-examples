# sync_ext_volumes.py
#
# Baseline script to sync GRS-replicated volumes from a primary metastore to a secondary metastore
#
# NOTE: This script must be run in the PRIMARY workspace.
#
# This script will attempt to register all _external_ volumes in the primary metastore into the secondary metastore.
# This assumes that all storage locations are identical between the two regions, i.e., georeplicated storage has been
# used. Storage URLs are not updated; they are just directly brought over to the secondary metastore. Files within the
# volumes will not be replicated, since the underlying storage will be georeplicated.
#
# Params that must be specified below:
#   -target_host: the hostname of the secondary workspace.
#   -target_pat: an access token for the secondary workspace; must be an ADMIN user.
#   -catalogs_to_copy: a list of the catalogs to be replicated; only volumes within these catalogs will be replicated.
#   -num_exec: the number of threads to spawn in the ThreadPoolExecutor.


from itertools import repeat
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import catalog
from concurrent.futures import ThreadPoolExecutor
from databricks.sdk.errors.platform import ResourceAlreadyExists
from common import (target_pat, target_host,
                    source_pat, source_host,
                    catalogs_to_copy, num_exec)


# helper function to create volumes and set appropriate owner
def create_volume(w, catalog_name, schema_name, volume_name, location, owner):
    print(f"Creating volume {volume_name} in {catalog_name}.{schema_name}...")

    # try creating new volume
    try:
        volume = w.volumes.create(catalog_name=catalog_name,
                                  schema_name=schema_name,
                                  name=volume_name,
                                  storage_location=location,
                                  volume_type=catalog.VolumeType.EXTERNAL)

        _ = w.volumes.update(name=volume.full_name, owner=owner)
        return {"volume": volume.full_name, "status": "success"}

    # if volume already exists, just update the owner (in case it has changed)
    except ResourceAlreadyExists:
        _ = w.volumes.update(name=f"{catalog_name}.{schema_name}.{volume_name}", owner=owner)
        return {"volume": f"{catalog_name}.{schema_name}.{volume_name}", "status": "already_exists"}

    # for any other exception, return the error
    except Exception as e:
        return {"volume": f"{catalog_name}.{schema_name}.{volume_name}", "status": f"ERROR: {e}"}


# create the WorkspaceClient pointed at the target WS
w_target = WorkspaceClient(host=target_host, token=target_pat)

# pull system tables from source ws
system_info = spark.sql("SELECT * FROM system.information_schema.volumes")

# loop through all catalogs to copy, then copy all volumes in these catalogs.
#
# note: we avoid listing volumes and doing a comparison since this would likely be slower than just looping through all
# volumes and dealing with the "already_exists" errors. We attempt to update owners in case the volume already exists
# but the owner has changed.
for cat in catalogs_to_copy:
    filtered_volumes = system_info.filter(
        (system_info.volume_catalog == cat) &
        (system_info.volume_schema != "information_schema") &
        (system_info.volume_type == "EXTERNAL")).collect()

    # get schemas, tables and locations in list form
    schema_names = [row['volume_schema'] for row in filtered_volumes]
    volume_names = [row['volume_name'] for row in filtered_volumes]
    volume_locs = [row['storage_location'] for row in filtered_volumes]
    volume_owners = [row['volume_owner'] for row in filtered_volumes]

    with ThreadPoolExecutor(max_workers=num_exec) as executor:
        threads = executor.map(create_volume,
                               repeat(w_target),
                               repeat(cat),
                               schema_names,
                               volume_names,
                               volume_locs,
                               volume_owners)

        for thread in threads:
            if thread["status"] == "success":
                print("Created volume {}.".format(thread["volume"]))
            elif thread["status"] == "already_exists":
                print("Skipped volume {} because it already exists.".format(thread["volume"]))
            else:
                print("Could not create volume {}; error: {}".format(thread["volume"], thread["status"]))
