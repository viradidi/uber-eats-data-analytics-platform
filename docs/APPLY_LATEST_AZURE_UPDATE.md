# Apply the Latest Azure Run-Correlation Update

**Purpose:** Deploy only the latest ADF run-correlation and SQL Silver notebook changes to an already deployed environment.

**Previous CI result:** 134 tests passed in [GitHub Actions run 42014868359](https://github.com/viradidi/uber-eats-data-analytics-platform/actions/runs/42014868359), on commit d3a8ad955bab839e91997693e634fe64cb066e44. The scoped deployment scripts and their tests were added after that run, so deploy only after the latest commit also has a green CI run.

## What changed

The master pipeline now passes its run ID to the SQL ingestion child pipeline. The SQL child pipeline passes the parent ID and its own run ID into the repository-owned Silver_SQL_Incremental notebook. The notebook uses the master ADF run ID as its SQL Silver audit run_id when available, allowing those rows to be joined to Gold's pipeline_run_id.

The change affects the following artifacts:

- pipeline/uber_eats_master.json
- pipeline/sql_incremental pipeline.json
- notebooks/Silver_SQL_Incremental.py
- src/common/sql_silver.py is imported as a dependency but is not modified by the run-ID change.

Two focused deployment scripts are included:

- deployment/databricks/import_sql_silver_assets.ps1 imports the SQL Silver notebook and only its required shared Python package files.
- deployment/azure/deploy_run_correlation_update.ps1 updates and reads back only the master and SQL child ADF pipeline definitions.

These scripts do not create resources, apply SQL migrations, change linked services/datasets, start a pipeline, or modify cluster configuration.

## 1. Preflight

Use PowerShell 7 from the repository root. Confirm the target Azure subscription, resource group, Data Factory name, and Databricks workspace host from the environment you already deployed. Do not copy historic values from the repository inventory without checking that they match the currently accepted environment.

Confirm the active repository commit and a green GitHub Actions run before deployment. The scripts verify that the repository pipeline definitions contain the expected run-correlation wiring.

No new SQL table or column is required: the existing SQL Silver audit already stores run_id as a string, and Gold already stores pipeline_run_id.

## 2. Import the SQL Silver notebook and shared module

Install/configure the Databricks CLI and authenticate to the intended workspace. Example profile setup:

\`\`\`powershell
databricks auth login --host https://<DATABRICKS_WORKSPACE_HOST> --profile uber-eats
\`\`\`

Then run:

\`\`\`powershell
.\\deployment\\databricks\\import_sql_silver_assets.ps1 -WorkspaceHost "https://<DATABRICKS_WORKSPACE_HOST>" -ProfileName "uber-eats"
\`\`\`

The script imports and verifies these exact paths:

- /Shared/UberEats/notebooks/Silver_SQL_Incremental
- /Shared/UberEats/src/__init__.py
- /Shared/UberEats/src/common/__init__.py
- /Shared/UberEats/src/common/sql_silver.py

It does not run the notebook.

## 3. Update the two ADF pipeline definitions

Install the Azure PowerShell modules if needed and authenticate to the subscription that contains the existing Data Factory:

\`\`\`powershell
Install-Module Az.Accounts, Az.Resources -Scope CurrentUser
Connect-AzAccount
Set-AzContext -SubscriptionId "<SUBSCRIPTION_ID>"
\`\`\`

Run a preflight using -WhatIf:

\`\`\`powershell
.\\deployment\\azure\\deploy_run_correlation_update.ps1 -SubscriptionId "<SUBSCRIPTION_ID>" -ResourceGroupName "<RESOURCE_GROUP>" -FactoryName "<DATA_FACTORY_NAME>" -WhatIf
\`\`\`

After checking the target context and proposed resources, apply:

\`\`\`powershell
.\\deployment\\azure\\deploy_run_correlation_update.ps1 -SubscriptionId "<SUBSCRIPTION_ID>" -ResourceGroupName "<RESOURCE_GROUP>" -FactoryName "<DATA_FACTORY_NAME>"
\`\`\`

The script updates only these two resources:

- uber_eats_master
- sql_incremental pipeline

It then reads each pipeline back from Azure and verifies that the named resource can be retrieved. It does not start the master pipeline.

## 4. Execute and verify one controlled master run

Start the master pipeline from the existing ADF environment only when its configured source/control rows are approved and the target workspace is ready. Capture the actual master ADF run ID from the run details.

After the run succeeds, query the SQL Silver audit table in the schema associated with each configured SQL Silver target:

\`\`\`sql
SELECT
    run_id,
    control_id,
    source_schema,
    source_table,
    target_table,
    source_row_count,
    target_row_count,
    missing_source_keys,
    status,
    recorded_at_utc
FROM <catalog>.<silver_schema>.sql_incremental_run_audit
WHERE run_id = '<MASTER_ADF_RUN_ID>';
\`\`\`

Then verify the Gold audit:

\`\`\`sql
SELECT
    run_id,
    pipeline_run_id,
    status,
    outputs_json,
    started_at_utc,
    finished_at_utc,
    error_message
FROM <catalog>.gold.gold_run_audit
WHERE pipeline_run_id = '<MASTER_ADF_RUN_ID>';
\`\`\`

Expected correlation: the SQL Silver audit run_id equals the Gold audit pipeline_run_id and the actual master ADF run ID. A master run can process multiple SQL control rows, so multiple SQL Silver audit records may share that run_id; use control_id and source table to distinguish them.

If the run fails, do not manually advance the SQL checkpoint. Inspect the failed ADF activity, landing batch, Delta target, and SQL Silver audit record before deciding whether a bounded replay is safe. Follow the recovery and rerun runbook.

## Completion criteria

- [ ] CI is green for the exact commit deployed.
- [ ] The SQL Silver notebook and required modules were imported and read back.
- [ ] Both ADF pipeline definitions were updated and read back.
- [ ] The actual master ADF run ID was captured.
- [ ] SQL Silver audit rows use that master run ID.
- [ ] Gold audit contains the same value in pipeline_run_id.
- [ ] Source counts, target counts, reconciliation status, and run links were retained from actual service output.

The repository code and deployment scripts can be prepared and validated here. The Azure-side steps require an authenticated PowerShell/Azure context and Databricks CLI session in the target environment; no live Azure deployment is claimed by this document.
