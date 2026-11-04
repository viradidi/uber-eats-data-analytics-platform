# Restored Azure Configuration Inventory

## Purpose

This document records every environment-bound value found in the committed ADF and Databricks exports. The original resource names are retained below as **reference values only**. They must not be assumed to exist or be reusable in the restored environment.

## 1. Azure Data Lake Storage

| Repository artifact | Current reference | Required restore action |
|---|---|---|
| `linkedService/AzureDataLakeStorage1.json` | `https://ubereatsrisstorage.dfs.core.windows.net/` | Confirm the storage account and replace the linked-service URL if the restored account differs |
| Silver notebook default | `abfss://bronze@ubereatsrisstorage.dfs.core.windows.net` | This now matches the committed ADLS linked service; override the `bronze_root` widget only when the restored account differs |
| Dataset filesystem | `bronze` | Create the filesystem/container and confirm ADF and Databricks identities can read and write it |
| Metadata inputs | `metadata/api_metadata_v2 (2).json`, `metadata/csv_metadata_proper (2).json` | Upload the matching committed JSON files to the expected folder before running the master pipeline |
| Silver landing folders | `api/payments`, `api/deliveries`, `api/ratings`, `api/order_status`, `csv/drivers`, `csv/promotions`, `csv/zones`, and SQL folders keyed by the source table | Verify each pipeline writes to the folder expected by its Silver notebook/job |

## 2. SQL Server

Both `linkedService/ls_sql_database.json` and `linkedService/ls_sql_control.json` currently reference:

- Server: `healthcareindia.database.windows.net`
- Database: `free-sql-db-8995565`
- Authentication: system-assigned managed identity

Before deployment:

- Confirm whether this SQL database is still the intended source/control database. Do not assume the historic endpoint is available.
- Restore or provision the source tables and required control tables.
- Apply [the idempotent ADF control-table bootstrap](../sql/migrations/20261104_create_ingestion_control_tables.sql) to create/verify both ADF control tables and seed the four API endpoint controls. Then add only approved SQL source-control rows to `dbo.pipeline_control`, using verified source table/keys/cursors and valid typed starting watermarks.
- Grant the restored Data Factory managed identity the required least-privilege SQL permissions.
- Apply and verify the `datetime2(7)` checkpoint migration before deploying the updated checkpoint dataset.

Run the [control-table bootstrap migration](../sql/migrations/20261104_create_ingestion_control_tables.sql) first, then apply the checkpoint precision migration if the existing column is not already `datetime2(7)`. The repository contains a read-only [control preflight](SQL_INCREMENTAL_CONTROL_PREFLIGHT.sql), checkpoint migration, and [incremental-run acceptance queries](SQL_INCREMENTAL_RUN_ACCEPTANCE_QUERIES.md). The actual active source rows and per-table source contracts are environment data and still need approval.

## 3. Databricks workspace, Jobs, and runtime

The three Databricks linked services currently reference the historic workspace:

- Host: `https://adb-7405609939036313.13.azuredatabricks.net`
- Workspace resource ID: `/subscriptions/3c895dcc-8ce9-4539-89c2-029fbf7ff343/resourceGroups/rg_ubereats/providers/Microsoft.Databricks/workspaces/ubereats-databricks`

Replace these values when using a different workspace and verify managed-identity access.

The linked-service cluster runtime is now aligned on **Databricks Runtime 16.4 LTS / Photon / Scala 2.12**. The local PySpark test requirement is aligned to Spark 3.5.2. Microsoft lists 16.4 LTS support through May 9, 2028; the previous 13.3 LTS support end date was August 22, 2026. See [Azure Databricks runtime versions and compatibility](https://learn.microsoft.com/en-us/azure/databricks/release-notes/runtime/). This alignment applies to the ADF linked-service cluster definitions; it does not update compute configured inside independently managed Jobs. Export and inspect those job definitions too.

ADF invokes these existing job IDs:

| ADF activity | Job ID | Required action |
|---|---:|---|
| API `Job_api` | `1006977938504729` | Confirm the job exists in the restored workspace, export settings, verify notebook/source and task parameters |
| CSV `silver_csv_job_copy1` | `986239993761586` | Confirm the job exists, export settings, verify dispatcher/notebook paths and `file_name` parameter |
| SQL `Job_Silver_copy1` | `121055824417886` | Export and complete the dedicated [SQL Silver job review](EXTERNAL_SQL_SILVER_JOB_REVIEW.md); its notebook/source is not included in this repository |

If new Jobs are created, their IDs will differ. Update the corresponding ADF pipeline JSON references and re-run the repository tests before publishing.

The Gold notebook is invoked at `/Shared/UberEats/Gold_Analytics`. Deploy that notebook and the `src.gold` package/module dependencies to the selected workspace path. Confirm the default catalog `ubereats_databricks1` and `silver`/`gold` schemas exist or override the notebook widgets with the approved catalog/schema names.

## 4. API and Key Vault

| Repository artifact | Current reference | Required restore action |
|---|---|---|
| `linkedService/LS_restapi.json` | `https://ubereats-mock-api.onrender.com` | Confirm the API endpoint is available and returns the expected paginated payloads |
| `pipeline/api_src_inital load.json` | Hard-coded API watermark URL at the same host | Keep it aligned with the REST linked-service endpoint |
| `linkedService/LS_AzureKeyVault.json` | `https://ubereatskeyvalut.vault.azure.net/` | Confirm the restored Key Vault endpoint and grant the Data Factory identity secret-read access |
| API key secret | `ubereatsapikey`, with a pinned historical version in the export | Create/restore the secret and update the secret reference to the approved version; never commit the secret value |

Do not put API keys, tokens, or passwords in GitHub. The existing secret version is an environment-specific reference, not a portable secret.

## 5. CSV source staging in ADLS Gen2

The CSV source path has been changed to remove the retired Windows File Server and self-hosted Integration Runtime dependency.

- ADF dataset `DS_source_csv` now reads from the `AzureDataLakeStorage1` linked service.
- Source filesystem: `bronze`.
- Source folder: `csv_source`.
- The metadata file `csv_metadata_proper (2).json` still maps the three filenames to the existing Silver landing folders: `csv/drivers`, `csv/promotions`, and `csv/zones`.
- The ADF CSV copy activity now uses `AzureBlobFSReadSettings` rather than `FileServerReadSettings`.
- The three source files remain the committed `drivers.csv`, `promotions.csv`, and `zones.csv`.

Use `deployment/azure/stage_csv_files.ps1` to create the filesystem/folder if necessary, upload the committed files, download each file for SHA-256 comparison, and fail on any mismatch. The script uses Azure CLI login authentication; it does not place credentials in the repository.

The legacy File Server linked service and self-hosted Integration Runtime export files have been removed from the deployment artifacts because the active CSV pipeline no longer uses them. No desktop share or SHIR installation is required for the CSV ingestion path.

## 6. Factory and resource identities

The repository contains a factory export named `ubereatssri` in `eastus`. The export now declares only `identity.type = SystemAssigned`; historic read-only `principalId` and `tenantId` values have been removed because the restored factory will receive new identity identifiers. Confirm the actual restored Data Factory name, resource group, and region. Reapply role assignments and SQL/Key Vault/ADLS permissions to the new principal ID after creation. The CSV source no longer requires an Integration Runtime.

## 7. Repeatable source-file staging

Two scripts now reduce manual file preparation:

- `deployment/azure/stage_bronze_metadata.ps1` creates the ADLS filesystem/metadata directory if needed, uploads the two metadata files with Azure CLI login authentication, and verifies the remote files.
- `deployment/azure/stage_csv_files.ps1` uploads `drivers.csv`, `promotions.csv`, and `zones.csv` into `bronze/csv_source`, downloads each file, and verifies the remote copy by SHA-256.

Neither script configures Azure role assignments or SQL/Databricks permissions; those must be applied to the restored identities.

## 8. Pre-run configuration checklist

- [ ] Confirm Azure subscription, resource group, region, budget and resource names.
- [ ] Confirm SQL source/control server and database; restore source/control tables.
- [ ] Apply `20261104_create_ingestion_control_tables.sql` and verify both control tables; confirm the four API endpoints are seeded without resetting existing state.
- [ ] Apply and verify the checkpoint precision migration after the control table exists.
- [ ] Add only approved SQL source-control rows with verified business keys/cursors and non-null starting checkpoints; complete each SQL source contract.
- [ ] Confirm ADLS account, `bronze` filesystem, metadata files, `csv_source` folder and landing folders.
- [ ] Run the CSV staging script and verify all three source files exist in `bronze/csv_source`.
- [ ] Confirm API endpoint and Key Vault secret reference.
- [ ] Confirm Databricks workspace, managed identity, catalog and schemas.
- [ ] Export/inspect all three Databricks Jobs and update job IDs after any recreation.
- [ ] Deploy notebooks, shared Python modules and Gold assets to the expected workspace paths.
- [ ] Confirm current supported runtime and available node SKUs/quotas in the chosen region.
- [ ] Publish ADF resources and validate all references before the first master run.

Only after these environment-bound settings are resolved should the consolidated [restored Azure acceptance plan](RESTORED_AZURE_ACCEPTANCE_PLAN.md) be executed.
