# databricks-dr-examples
A collection of minimal example scripts for setting up Disaster Recovery for Databricks.

This code is provided as-is and is meant to serve as a set of baseline examples. You may need to alter these scripts to work in your environment.

Scripts in this repo include:
- clone_to_secondary.py: performs `DEEP CLONE` on a set of catalogs in the primary to a storage location in the secondary region.
- clone_to_secondary_par.py: parallelized version of clone_to_secondary.py.
- create_secondary_tables.py: script that can be run *in the secondary region* to register managed/external tables based on the output of create_secondary_tables.py.
- sync_creds_and_locs.py: script to sync storage credentials and external locations between primary and secondary metastores. Run locally or on either primary/secondary.
- sync_catalogs_and_schemas.py: script to sync all catalogs and schemas from a primary metastore to a secondary metastore. Run locally or on either primary/secondary.
