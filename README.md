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
