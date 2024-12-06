# sync_catalogs_and_schemas.py
#
# This is a minimal script to sync catalogs and schemas between two workspaces.
# To run it, you must first fill out the appropriate mapping files:
#   - catalog_mapping.csv: contains the catalog name and the storage root URL for the catalog in the secondary
#     metastore. Storage root may be omitted if there is not a default storage location for this catalog.
#   - schema_mapping.csv: contains the schema name and the storage root URL for the schema in the secondary
#     metastore. Storage root may be omitted if there is not a default storage location for this schema.
#
# The script will compare the source and target metastores to first get any catalogs that exist in the source but
# not the target, and then will attempt to create those catalogs in the target. It will then do the same for the
# schemas in the source/target. Note that all comparisons here are done on the *name* of the objects; this is
# necessary since all other parameters will change when switching between metastores.
#
# Currently, we use PAT-based auth for the WorkspaceClient objects, so you must provide the host and token manually for
# each workspace. You can update this to use other auth methods if desired.

from databricks.sdk import WorkspaceClient
import pandas as pd

# inputs
catalog_mapping_file = "data/catalog_mapping.csv"
schema_mapping_file = "data/schema_mapping.csv"
source_host = "<primary-ws-hostname>"
source_pat = "<primary-ws-pat>"
target_host = "<secondary-ws-hostname>"
target_pat = "<primary-ws-pat>"
catalogs_to_copy = ["my-catalog1", "my-catalog2"]

# create WorkspaceClient objects
w_source = WorkspaceClient(host=source_host, token=source_pat)
w_target = WorkspaceClient(host=target_host, token=target_pat)

# get source and target catalogs
source_catalogs = [x for x in w_source.catalogs.list() if x.name in catalogs_to_copy]
target_catalogs = [x for x in w_target.catalogs.list() if x.name in catalogs_to_copy]

# compare source and target catalogs
# we can only do this by name since the URL and IDs will change between workspaces
source_catalog_names = [x.name for x in source_catalogs]
target_catalog_names = [x.name for x in target_catalogs]
catalog_diff = list(set(source_catalog_names) - set(target_catalog_names))
catalogs_to_create = [x for x in source_catalogs if x.name in catalog_diff]
catalog_df = pd.read_csv(catalog_mapping_file, keep_default_na=False)

if not catalogs_to_create:
    print("All source catalogs exist in target metastore.")

for catalog in catalogs_to_create:
    # skip shared or external catalogs
    if catalog.connection_name or catalog.share_name:
        print(f"External Catalogs and Shared Catalogs are not currently supported by this script. \
        Skipping {catalog.name}...")
        continue

    # get parameters that map directly between catalogs
    catalog_name = catalog.name
    catalog_comment = catalog.comment
    catalog_options = catalog.options
    catalog_properties = catalog.properties

    print(f"Creating catalog {catalog_name}...")

    # get target storage root based off of catalog name
    try:
        storage_root = catalog_df['target_storage_root'].loc[catalog_df['source_catalog'] == catalog_name].iloc[0]
    except (KeyError, IndexError):
        print(f"Could not create catalog {catalog_name}. Please check mapping file.")
        continue

    # create catalog in target metastore
    if storage_root:
        w_target.catalogs.create(name=catalog_name,
                                 comment=catalog_comment,
                                 options=catalog_options,
                                 properties=catalog_properties,
                                 storage_root=storage_root)
    else:
        w_target.catalogs.create(name=catalog_name,
                                 comment=catalog_comment,
                                 options=catalog_options,
                                 properties=catalog_properties)

    print(f"Created catalog {catalog_name}.")

schema_df = pd.read_csv(schema_mapping_file, keep_default_na=False)

for catalog in source_catalogs:
    source_schemas = [x for x in w_source.schemas.list(catalog.name)]
    target_schemas = [x for x in w_target.schemas.list(catalog.name)]
    source_schema_names = [x.name for x in source_schemas]
    target_schema_names = [x.name for x in target_schemas]
    schema_diff = list(set(source_schema_names) - set(target_schema_names))
    schemas_to_create = [x for x in source_schemas if x.name in schema_diff]

    for schema in schemas_to_create:
        schema_name = schema.name
        schema_comment = schema.comment
        schema_properties = schema.properties

        try:
            storage_root = (schema_df['target_storage_root'].loc[
                (schema_df['source_schema'] == schema_name) &
                (schema_df['source_catalog'] == catalog.name)].iloc[0])
        except (KeyError, IndexError):
            print(f"Could not create catalog {catalog.name}. Please check mapping file.")
            continue

        if storage_root:
            w_target.schemas.create(name=schema_name,
                                    comment=schema_comment,
                                    properties=schema_properties,
                                    catalog_name=catalog.name,
                                    storage_root=storage_root)
        else:
            w_target.schemas.create(name=schema_name,
                                    comment=schema_comment,
                                    properties=schema_properties,
                                    catalog_name=catalog.name)

        print(f"Created schema {catalog.name}.{schema_name}.")
