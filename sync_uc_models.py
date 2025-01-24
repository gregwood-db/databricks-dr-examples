
from itertools import repeat
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import catalog
from concurrent.futures import ThreadPoolExecutor
from databricks.sdk.errors.platform import ResourceAlreadyExists
from common import (target_pat, target_host,
                    source_pat, source_host,
                    catalogs_to_copy, num_exec)


# helper function to create volumes and set appropriate owner
def create_model(w, catalog_name, schema_name, model_name, location, owner):
    print(f"Creating model {model_name} in {catalog_name}.{schema_name}...")

    # try creating new volume
    try:
        model = w.registered_models.create(catalog_name=catalog_name,
                                         schema_name=schema_name,
                                         name=model_name,
                                         storage_location=location)

        _ = w.registered_models.update(name=model.full_name, owner=owner)
        return {"model": model.full_name, "status": "success"}

    # if volume already exists, just update the owner (in case it has changed)
    except ResourceAlreadyExists:
        _ = w.registered_models.update(name=f"{catalog_name}.{schema_name}.{model_name}", owner=owner)
        return {"model": f"{catalog_name}.{schema_name}.{model_name}", "status": "already_exists"}

    # for any other exception, return the error
    except Exception as e:
        return {"model": f"{catalog_name}.{schema_name}.{model_name}", "status": f"ERROR: {e}"}


# create the WorkspaceClient pointed at the target WS
w_target = WorkspaceClient(host=target_host, token=target_pat)

# pull registered models from list

registered_models = w_target.registered_models.list()

# loop through all registered models to copy, then copy them.
#
# note: we avoid listing models and doing a comparison since this would likely be slower than just looping through all
# models and dealing with the "already_exists" errors. We attempt to update owners in case the volume already exists
# but the owner has changed.
for cat in catalogs_to_copy:
    filtered_models = registered_models.filter(
        (registered_models.catalog_name == cat) &
        (registered_models.schema_name != "information_schema")).collect()

    # get schemas, tables and locations in list form
    schema_names = [model.schema_name for model in filtered_models]
    model_names = [model.name for model in filtered_models]
    model_locs = [model.storage_location for model in filtered_models]
    model_owners = [model.owner for model in filtered_models]

    with ThreadPoolExecutor(max_workers=num_exec) as executor:
        threads = executor.map(create_model,
                               repeat(w_target),
                               repeat(cat),
                               schema_names,
                               model_names,
                               model_locs,
                               model_owners)

        for thread in threads:
            if thread["status"] == "success":
                print("Created model {}.".format(thread["model"]))
            elif thread["status"] == "already_exists":
                print("Skipped model {} because it already exists.".format(thread["model"]))
            else:
                print("Could not create model {}; error: {}".format(thread["model"], thread["status"]))
