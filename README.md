# databricks-dr-examples
A collection of minimal example scripts for setting up Disaster Recovery for Databricks.

This code is provided as-is and is meant to serve as a set of baseline examples. You may need to alter these scripts to work in your environment.

### Notes on cross-workspace connectivity
These scripts generally assume that they will be run on a notebook in the primary workspace, and that the workspace can directly access the secondary workspace via SDK; this may not always be true in your environment. You have two options if connectivity issues are preventing scripts from running:
- Alter the workspace networking to allow connectivity; this may involve adjusting firewalls, adding peering, etc.
- Run the scripts remotely using Databricks Connect

In the latter option, the following adjustments need to be made:
- Set up [Databricks Connect](https://docs.databricks.com/en/dev-tools/databricks-connect/python/index.html) in your environment
- In all scripts, add an import statement for Databricks Connect, i.e., `from databricks.connect import DatabricksSession`
- In all scripts, instantiate a Spark Session, i.e., `spark = DatabricksSession.builder.profile("<profile-name>").getOrCreate()`

These changes will allow the code to run remotely on a local machine or cloud VM.

## Repo Contents
Snippets that demonstrate basic functionality (located in /examples/):
- clone_to_secondary.py: performs `DEEP CLONE` on a set of catalogs in the primary to a storage location in the secondary region.
- clone_to_secondary_par.py: parallelized version of clone_to_secondary.py.
- create_tables_simple.py: simple script that must be run *in the secondary region* to register managed/external tables based on the output of clone_to_secondary.py.
- sync_views.py: simple script to sync views; this will need to be updated per your environment.


Code samples that show more comprehensive end-to-end functionality:
- sync_creds_and_locs.py: script to sync storage credentials and external locations between primary and secondary metastores. Run locally or on either primary/secondary.
- sync_catalogs_and_schemas.py: script to sync all catalogs and schemas from a primary metastore to a secondary metastore. Run locally or on either primary/secondary.
- sync_tables.py: performs a deep clone of all managed external tables, and registers those tables in the secondary region.
- sync_grs_ext.py: sync _metadata only_ for external tables that have already been replicated via cloud provider georeplication. No data is copied, and storage URLs on both workspaces will be the same.
- sync_ext_volumes.py: sync _metadata only_ for external volumes that have already been replicated via cloud provider georeplication.
- sync_perms.py: sync all permissions related to UC tables, volumes, schemas and catalogs from primary to secondary metastore.
- sync_shared_tables.py: sync tables using Delta Sharing. All tables will be imported to the secondary region as managed tables.

## How to use this Repository

### Prerequisites
Before running the script, make sure you have the following:

- A Databricks workspace with Admin privileges to access and manage catalogs and schemas.
  - Need to have CREATE CATALOG Privileges on the Metastore
  - Need to have CREATE EXTERNAL LOCATION Privileges on the Metastore
  - Need to have CREATE STORAGE CREDENTIAL Privileges on the Metastore
- Databricks CLI installed and configured with your workspace. Follow the Databricks CLI installation guide for setup instructions.
  - If using in a notebook, make sure the latest version is installed.
  - Requests library installed for making API calls to Databricks/
- Python 3.6+ and pip installed on your local machine.

Clone this repository to your local machine:
```
git clone https://github.com/gregwood-db/databricks-dr-examples.git
cd databricks-dr-examples
```

### Setting up variables and parameters
Set the following variable/parameter values in `common.py`; these will be used throughout the other scripts.
  - `cloud_type`: Cloud provider where workspaces exist (azure, aws, or gcp)
  - `cred_mapping_file`: The mapping file for credentials, i.e., `data/azure_cred_mapping.csv`
  - `loc_mapping_file`: The location mapping file, i.e. `data/ext_location_mapping.csv`
  - `catalog_mapping_file`: The catalog mapping file, i.e. `data/catalog_mapping.csv`
  - `schema_mapping_file`: The schema mapping file, i.e. `data/schema_mapping.csv`
  - `source_host`: Source/Primary Workspace URL, including leading `https://`
  - `target_host`: Target/Secondary Workspace URL, including leading `https://`
  - `source_pat`: Personal Access Tokens (PAT) for Source/Primary Workspace
  - `target_pat`: Personal Access Tokens (PAT) for Target/Secondary Workspace URL
  - `catalogs_to_copy` = A list of strings, containing names of catalogs to replicate
  - `metastore_id`: The global unique metastore ID of the secondary/target metastore
  - `landing_zone_url`: ADLS/S3/GCS location used to land intermediate data in the secondary region
  - `num_exec`: Number of parallel threads to execute (when parallelism is used)
  - `warehouse_size`: The size of the serverless SQL warehouse used in the secondary workspace
  - `response_backoff`: The polling backoff for checking query state when creating tables/views
  - `manifest_name`: the name of the table manifest Delta file, if using sync_tables.py


### Syncing External Locations and Credentials

1. Make sure `common.py` is updated with all relevant parameters

2 Once you have updated the configuration, you can run the script with the following command:

```
python sync_creds_and_locs.py
```

### Syncing Catalogs and Schemas

1. Make sure `common.py` is updated with all relevant parameters

2. Once you have updated the configuration, you can run the script with the following command:

```
python sync_catalogs_and_schemas.py
```

### Syncing Tables

Below are two options for syncing tables. Option 1 leverages Delta sharing to clone the tables, whereas Option 2 requires you to delta deep clone the tables in the source/primary region to an intermediary cloud storage bucket before re-creating the tables as managed tables in the target/secondary metastore.

#### Option 1: Syncing Managed Tables via Delta Sharing

1. Make sure `common.py` is updated with all relevant parameters

2. Once you have updated the configuration, you can run the script with the following command:

```
python sync_shared_tables.py
```

#### Option 2: Syncing Managed Tables with an Intermediary Storage Account

1. Make sure `common.py` is updated with all relevant parameters

2. Once you have updated the configuration, you can run the script with the following command:

```
python sync_tables.py
```

#### Option 3: Syncing External Tables

1. Make sure `common.py` is updated with all relevant parameters

2. Once you have updated the configuration, you can run the script with the following command:

```
python sync_grs_ext.py
```

### Syncing External Volumes

1. Make sure `common.py` is updated with all relevant parameters

2. Once you have updated the configuration, you can run the script with the following command:

```
python sync_ext_volumes.py
```

### Syncing Views

1. Make sure `common.py` is updated with all relevant parameters

2. Once you have updated the configuration, you can run the script with the following command:

```
python sync_views.py
```
