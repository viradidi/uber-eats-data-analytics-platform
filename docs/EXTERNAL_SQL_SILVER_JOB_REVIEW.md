# External SQL Silver Job Review

## Review status

**Pending: exported Databricks job settings and source code are required.**

The ADF export at `pipeline/sql_incremental pipeline.json` invokes Databricks job ID `121055824417886` through `Job_Silver_copy1` and passes the current `table_name`. The activity depends on the ADLS copy succeeding. The checkpoint update depends on this Databricks activity succeeding.

The repository does not contain the job definition or its SQL Silver notebook/source. Therefore, repository evidence cannot establish which landing files the job reads, which target table it writes, its business keys, or whether a repeated interval is safe. This is an evidence gap, not proof that the historical job failed.

## 1. Export the job definition

The repository now includes the export helper at ../deployment/databricks/export_sql_silver_job.ps1. Authenticate the Databricks CLI to the workspace that owns job ID 121055824417886, then run it from the repository root:

~~~powershell
databricks auth login --host <WORKSPACE_HOST> --profile uber-eats-restore
.\deployment\databricks\export_sql_silver_job.ps1 -WorkspaceHost <WORKSPACE_HOST> -ProfileName uber-eats-restore
~~~

By default, the export is written outside the repository under $HOME/UberEats-SQL-Silver-Job-Export. It saves a sanitized job settings JSON, exports workspace notebook/script sources for workspace-backed tasks, and writes a manifest for Git/DBFS/volume paths, child jobs, and potential literal credentials. It does not run or modify the job. Inspect each source file and resolve the manifest's external references before sharing the folder. It does not automatically export notebooks stored only in Git/DBFS/volumes or recursively fetch referenced child jobs; use the recorded manifest references to include those.

If you prefer to export manually, the supported Jobs API endpoint is:

Retrieve the full job configuration from the Databricks workspace that owns job ID 121055824417886.

The supported Jobs API endpoint is:

```http
GET /api/2.2/jobs/get?job_id=121055824417886
```

Save the complete response as `job_121055824417886.json`. Do not save a personal access token or authorization header in the export. The response should retain, where configured:

- Job ID, name, description, and task list.
- Task keys, task types, dependency graph, task parameters, and run conditions.
- Notebook paths or Python/SQL source paths.
- Job-level and task-level parameters, including how `table_name` is received.
- Job clusters or existing cluster references, runtime settings, libraries, and environment dependencies.
- Retry, timeout, concurrency, and queue settings.
- Run-as identity and Git source/commit information, if applicable.
- Any child-job references or tasks that launch another job.

Official reference: [Databricks Jobs API — Get a single job](https://docs.databricks.com/api/jobs/v2/get-job).

## 2. Export every referenced source file

For every task that runs a notebook, script, or child job:

1. Record the exact workspace or Git path from the job settings.
2. Export the notebook as source code (`.py`, `.sql`, or another native source format). The workspace UI supports **File → Export → Source file**.
3. For workspace-hosted notebooks, the Workspace API supports:
   ```http
   GET /api/2.0/workspace/export?path=<absolute_notebook_path>&format=SOURCE&direct_download=true
   ```
4. If the job uses a remote Git source, include the repository URL (without credentials), branch/tag, exact commit SHA used by the job, and the source files at that commit.
5. If a task invokes another job or imports project modules, include those child job definitions and the relevant source/dependency files too.

Official references: [Databricks Workspace Export API](https://docs.databricks.com/api/workspace-files/v1/export) and [Import and export Databricks notebooks](https://docs.databricks.com/aws/en/notebooks/notebook-export-import).

The minimum review package for the SQL branch is the job settings JSON plus every source file needed to follow the task from input selection through target publication. The API and CSV branches now invoke repository-owned Databricks Notebook activities directly, so their previous external Job IDs are no longer required. Review the current API/CSV dispatchers and ADF notebook paths as part of the deployment review. A screenshot alone is not sufficient to establish SQL Silver job behavior.

## 3. Sanitize before sharing

Keep the configuration shape and parameter names, but remove or replace:

- Personal access tokens, client secrets, passwords, private keys, and authorization headers.
- Secret values, connection strings, signed URLs, and credential-bearing environment variables.
- Real customer or payment data.

Use explicit placeholders such as `<REDACTED_SECRET>` rather than deleting configuration keys. Do not share workspace credentials. Keep the job ID, task graph, paths, target table names, parameter names, and non-secret runtime settings visible where possible.

## 4. Code review checklist

The review will follow the complete path from ADF parameters to the Silver target.

### Input selection

- Does the job use `table_name` to select the correct landing folder?
- Does it read only the intended control ID and lower/upper watermark interval?
- Can it accidentally read old files, unrelated tables, or multiple historical batches?
- Is file selection deterministic when more than one file exists?
- Are CSV delimiter, header, encoding, schema inference, null handling, and timestamp parsing explicit?
- Does an empty or missing input fail safely instead of overwriting a populated target?

### Target and key semantics

- What exact Silver catalog, schema, and table is written for each source?
- What is the stable business key, including composite keys where required?
- Are null keys, duplicate keys, and conflicting versions rejected or resolved deterministically?
- Is the target operation append, overwrite, replace-by-partition/range, or Delta `MERGE`?
- For `MERGE`, are the match condition and update/insert rules complete?
- Are deletes or soft deletes propagated, or explicitly out of scope?

### Replay, failure, and concurrency

- Does the same input interval produce the same business-table state when run twice?
- Can a failed task leave a partially committed target?
- Are writes atomic at the intended table/partition boundary?
- Can overlapping or concurrent job runs corrupt the target?
- Do retries repeat a safe operation, or can retries duplicate records?
- Does job success mean that the target write and its required validation have completed?
- Does the downstream checkpoint advance only after the intended job and validation succeed?

The ADF dependency currently protects checkpoint advancement from a failed Databricks activity. It does not make that activity's writes transactional or idempotent.

## 5. Required replay acceptance test

Run this only in an isolated or explicitly approved test environment using a captured SQL extraction interval.

1. Record the source table, business key, exact lower watermark, exact captured upper watermark, control ID, landing object paths, and checkpoint value before the test.
2. Execute the SQL extraction and SQL Silver job once. Preserve job/ADF run IDs, landing counts, Silver counts, duplicate-key results, and reconciliation output.
3. Confirm the checkpoint equals the captured upper bound only after the job and required validation succeed.
4. Reprocess the **same exact lower/upper interval**. Do not sample a new maximum or widen the bounds for the second attempt.
5. Compare the business target before and after the second processing:
   - No unintended duplicate business/event keys.
   - No missing keys relative to the expected source interval.
   - No changed business fields except changes explicitly allowed by the transformation contract.
   - No duplicate or missing partitions/files for the logical batch.
   - Reconciliation results remain within the documented rules.
6. Treat audit/history rows separately: an additional run-audit entry may be expected even when the business target is unchanged.
7. Verify that the checkpoint does not move beyond the captured upper bound and that a failed Silver run leaves it unchanged.

The replay test passes only when the source-specific destination contract explains and the evidence confirms the observed results. Do not assume that a deterministic ADLS filename or a successful job status proves replay safety.

## 6. Review outcome record

Complete this table after examining the export.

| Review item | Verified result |
|---|---|
| Exported job ID and name | Pending |
| Job definition and task graph retained | Pending |
| Source notebook/script and dependencies retained | Pending |
| Input folder/file selection | Pending |
| SQL table-to-Silver target mapping | Pending |
| Business/event key per table | Pending |
| Write mode and merge condition | Pending |
| Duplicate/version/delete policy | Pending |
| Retry and concurrency behavior | Pending |
| Same-interval replay test | Not run |
| Checkpoint behavior on success/failure | Pending |
| Reviewer decision | **Not approved for replay/idempotency claims yet** |

When the export package is available, review it before modifying the job or enabling SQL overlap/replay in the restored pipeline.
