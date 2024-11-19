# sync_creds_and_locs.py
#
# This is a minimal script to sync storage credentials and external locations between two workspaces.
# To run it, you must first fill out the appropriate mapping files:
#   - <cloud>_cred_mapping.csv: contains the required mapping parameters to translate a credential between two regions.
#     Note that this varies by cloud. For AWS, this is simply the IAM role ARN. For Azure, you will need the SP OR
#     Managed Identity info. Note that for Azure, only ONE of the following should be provided:
#       + target_mgd_id_connector (if using a standard access connector)
#       + target_mgd_id_identity (if using a user-assigned identity)
#       + target_sp fields (if using a Service Principal)
#   - ext_location_mapping.csv: maps the target URL between source and target. For AWS, also has an option to provide
#     an S3 access point for the target region.
#
# Also make sure to set cloud_type to the appropriate choice between aws, azure and gcp.
#
# The script will compare the source and target metastores to first get any storage creds that exist in the source but
# not the target, and then will attempt to create those credentials in the target. It will then do the same for the
# external locations in the source/target. Note that all comparisons here are done on the *name* of the objects; this is
# necessary since all other parameters will change when switching between metastores.
#
# Currently, we use PAT-based auth for the WorkspaceClient objects, so you must provide the host and token manually for
# each workspace. You can update this to use other auth methods if desired. You may also wish to avoid including your
# cloud object information in the provided CSVs, especially for Azure; this could be done by directly interfacing with
# the cloud provider CLI/APIs within this script (or as part of an external workflow).

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import catalog
import pandas as pd

# inputs
cloud_type = "azure"
cred_mapping_file = "data/azure_cred_mapping.csv"
loc_mapping_file = "data/ext_location_mapping.csv"
source_host = "<primary_workspace_hostname>"
source_pat = "<primary_workspace_PAT>"
target_host = "<secondary_workspace_hostname>"
target_pat = "<secondary_workspace_PAT>"

# create WorkspaceClient objects
w_source = WorkspaceClient(host=source_host, token=source_pat)
w_target = WorkspaceClient(host=target_host, token=target_pat)

# get source and target credentials and external locations
source_creds = [x for x in w_source.storage_credentials.list()]
target_creds = [x for x in w_target.storage_credentials.list()]
source_extloc = [x for x in w_source.external_locations.list()]
target_extloc = [x for x in w_target.external_locations.list()]

# compare source and target storage credentials
# we can only do this by name since the URL and IDs will change between workspaces
source_cred_names = [x.name for x in source_creds]
target_cred_names = [x.name for x in target_creds]
cred_diff = list(set(source_cred_names) - set(target_cred_names))
creds_to_create = [x for x in source_creds if x.name in cred_diff]
cred_df = pd.read_csv(cred_mapping_file)

if not creds_to_create:
    print("All source credentials exist in target metastore.")

for cred in creds_to_create:
    # get parameters that map directly between creds
    cred_name = cred.name
    cred_read_only = cred.read_only
    cred_comment = cred.comment
    print(f"Creating storage credential {cred_name}...")

    if cloud_type == "aws":
        # get cred IAM role based off of name
        try:
            iam_role_arn = cred_df['target_iam_role'].loc[cred_df['source_cred_name'] == cred_name].iloc[0]
        except (KeyError, IndexError):
            print(f"Could not create credential {cred_name}. Please check mapping file.")
            continue

        # create storage credential in target WS
        cred_iam_role = catalog.AwsIamRole(role_arn=iam_role_arn)
        w_target.storage_credentials.create(name=cred_name,
                                            read_only=cred_read_only,
                                            comment=cred_comment,
                                            aws_iam_role=cred_iam_role)
    elif cloud_type == "azure":
        # get SP and Mgd ID info based off of name
        try:
            managed_id_connector = \
                cred_df['target_mgd_id_connector'].loc[cred_df['source_cred_name'] == cred_name].iloc[0]
            managed_id_identity = \
                cred_df['target_mgd_id_identity'].loc[cred_df['source_cred_name'] == cred_name].iloc[0]
            sp_directory = cred_df['target_sp_directory'].loc[cred_df['source_cred_name'] == cred_name].iloc[0]
            sp_appid = cred_df['target_sp_appid'].loc[cred_df['source_cred_name'] == cred_name].iloc[0]
            sp_secret = cred_df['target_sp_secret'].loc[cred_df['source_cred_name'] == cred_name].iloc[0]
        except (KeyError, IndexError):
            print(f"Could not create credential {cred_name}. Please check mapping file.")
            continue

        # create storage credential in target WS
        if managed_id_connector:
            cred_mgd_id = catalog.AzureManagedIdentityRequest(access_connector_id=managed_id_connector)
            w_target.storage_credentials.create(name=cred_name,
                                                read_only=cred_read_only,
                                                comment=cred_comment,
                                                azure_managed_identity=cred_mgd_id)
        elif managed_id_identity:
            cred_mgd_id = catalog.AzureManagedIdentityRequest(access_connector_id=managed_id_identity)
            w_target.storage_credentials.create(name=cred_name,
                                                read_only=cred_read_only,
                                                comment=cred_comment,
                                                azure_managed_identity=cred_mgd_id)
        else:
            try:
                cred_sp = catalog.AzureServicePrincipal(directory_id=sp_directory,
                                                        application_id=sp_appid,
                                                        client_secret=sp_secret)
                w_target.storage_credentials.create(name=cred_name,
                                                    read_only=cred_read_only,
                                                    comment=cred_comment,
                                                    azure_service_principal=cred_sp)
            except Exception:
                print(f"Could not create credential {cred_name}. Please make sure that only one of \
                managed_id_connector, managed_id_identity or service_principal info is provided in the mapping.")
                
    elif cloud_type == "gcp":
        print("GCP not yet implemented.")
        continue
    else:
        print("Cloud type must be one of AWS, GCP, or Azure.")
        continue

    print(f"Created storage credential {cred_name}.")

# compare source and target external locations
# we can only do this by name since the URL and IDs will change between workspaces
source_extloc_names = [x.name for x in source_extloc]
target_extloc_names = [x.name for x in target_extloc]
loc_diff = list(set(source_extloc_names) - set(target_extloc_names))
locs_to_create = [x for x in source_extloc if x.name in loc_diff]
loc_df = pd.read_csv(loc_mapping_file)

if not locs_to_create:
    print("All source external locations exist in target metastore.")

for loc in locs_to_create:
    # get parameters that map directly between creds
    loc_name = loc.name
    loc_cred_name = loc.credential_name
    loc_comment = loc.comment
    loc_fallback = loc.fallback
    loc_read_only = loc.read_only
    print(f"Creating external location {loc_name}...")

    if cloud_type == "aws":
        try:
            url = loc_df['target_url'].loc[loc_df['source_loc_name'] == loc_name].iloc[0]
            access_pt = loc_df['target_access_pt'].loc[loc_df['source_loc_name'] == loc_name].iloc[0]
        except (KeyError, IndexError):
            print(f"Could not create location {loc_name}. Please check mapping file.")
            continue

        if access_pt:
            w_target.external_locations.create(name=loc_name,
                                               credential_name=loc_cred_name,
                                               comment=loc_comment,
                                               fallback=loc_fallback,
                                               read_only=loc_read_only,
                                               url=url,
                                               access_point=access_pt)
        else:
            w_target.external_locations.create(name=loc_name,
                                               credential_name=loc_cred_name,
                                               comment=loc_comment,
                                               fallback=loc_fallback,
                                               read_only=loc_read_only,
                                               url=url)
    elif cloud_type == "azure":
        try:
            url = loc_df['target_url'].loc[loc_df['source_loc_name'] == loc_name].iloc[0]
        except (KeyError, IndexError):
            print(f"Could not create location {loc_name}. Please check mapping file.")
            continue

        w_target.external_locations.create(name=loc_name,
                                           credential_name=loc_cred_name,
                                           comment=loc_comment,
                                           fallback=loc_fallback,
                                           read_only=loc_read_only,
                                           url=url)
    elif cloud_type == "gcp":
        print("GCP not yet implemented.")
        continue
    else:
        print("Cloud type must be one of AWS, GCP, or Azure.")
        continue

    print(f"External location {loc_name} created.")
