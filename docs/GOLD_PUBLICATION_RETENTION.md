# Gold Publication Retention and Maintenance

## Purpose

The five version tables retain complete Gold snapshots so failed publishes can be investigated and prior releases can be restored. Without lifecycle management, old successful and failed versions accumulate indefinitely.

The manual maintenance notebook `notebooks/Gold_Publication_Maintenance.py` plans and removes only expired, inactive, terminal publication versions. It is deliberately dry-run-first and is not scheduled by ADF.

## Default retention policy

| Rule | Default | Protection |
|---|---:|---|
| Logical retention window | 30 days | Any known publication with activity inside this window is preserved |
| Successful publication floor | 10 releases | The newest ten distinct successful publication IDs remain available even when older than 30 days |
| Active publication | Always | The publication currently selected by `gold_publication_state` is never a cleanup candidate |
| Legacy baseline | Always | `LEGACY_BASELINE_IMPORTED` remains available as the migration fallback |
| In-flight publication | Always until terminal event | Runs whose latest event is `STAGING_STARTED` or `SNAPSHOTS_STAGED` are retained |
| Untracked version ID | Always | A version ID present in a version table without publication history is retained for investigation |
| Unknown/malformed history | Always | Unknown event types or missing event timestamps fail closed |

The minimum configurable logical retention is seven days. The defaults are conservative and should be increased if the business rollback window, downstream reader duration, or recovery requirements are longer.

Only publications that have a known terminal event (`PUBLISHED`, `PUBLISHED_WITH_WARNINGS`, `LEGACY_BASELINE_IMPORTED`, or `FAILED_BEFORE_COMMIT`), have complete timestamps, are older than the retention cutoff, and are outside the successful-release floor can be considered for cleanup.

## Running the maintenance notebook

The notebook must be imported into the Databricks workspace at:

`/Shared/UberEats/notebooks/Gold_Publication_Maintenance`

It requires the shared `src.gold.publication` and `src.gold.retention` modules and access to the Gold publication state, publication history, and all five version tables.

Default widgets:

| Widget | Default | Meaning |
|---|---:|---|
| `retention_days` | 30 | Age cutoff for terminal publication activity |
| `keep_successful_publications` | 10 | Minimum number of latest successful releases to preserve |
| `dry_run` | true | Calculate candidates and row counts without deleting |
| `confirmation` | empty | Must exactly equal `DELETE_OLD_GOLD_PUBLICATIONS` before deletion |
| `optimize_after_cleanup` | false | Optionally compact small files in the five version tables |
| `vacuum_after_cleanup` | false | Optionally remove unreferenced physical data files |
| `vacuum_retention_hours` | 168 | Minimum permitted VACUUM retention (seven days) |

### Recommended operating sequence

1. Run the notebook with the default `dry_run=true`.
2. Review the candidate publication IDs, preserved reasons, and planned row counts in the notebook output and `gold_publication_retention_runs`.
3. Check the candidate IDs against open incidents, in-flight runs, rollback requests, and the required business rollback window.
4. Run actual deletion only in a maintenance window with Gold publishers and manual rollback operations paused. The plan is re-calculated immediately before deletion, and each Delta DELETE also checks that its target publication is not the active pointer at delete time.
5. Set `dry_run=false` and enter the exact confirmation string `DELETE_OLD_GOLD_PUBLICATIONS`.
6. Review `COMPLETED`, `COMPLETED_WITH_REMAINDERS`, or `FAILED_OR_PARTIAL` in the retention-run audit table.
7. Enable `OPTIMIZE` or `VACUUM` only as a separately reviewed maintenance choice.

The cleanup runs separate Delta DELETE statements against the five version tables; there is no cross-table transaction for data removal. If execution fails after some tables have been cleaned, the job records `FAILED_OR_PARTIAL` where possible and can be rerun safely: the DELETE statements are scoped to the same candidate IDs and are idempotent. Do not start a publication or rollback while a multi-table cleanup is running.

## Delta maintenance semantics

A Delta DELETE removes rows from the current logical table snapshot. It does not immediately remove their old physical data files. `VACUUM` is the operation that reclaims unreferenced files after the retention interval. Databricks documents a seven-day default and warns that a shorter interval can invalidate active readers/writers or remove files needed for time travel. This notebook enforces at least 168 hours and does not disable the Delta retention safety check. See the [Delta DELETE reference](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-delete-from) and [VACUUM reference](https://docs.databricks.com/aws/en/sql/language-manual/delta-vacuum).

`OPTIMIZE` is optional and compacts files after deletion. It does not implement publication retention by itself. Databricks documents bin-packing OPTIMIZE as idempotent. See [OPTIMIZE reference](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-optimize).

The notebook does not automatically schedule maintenance, expire publication-history events, delete legacy bootstrap tables, or run OPTIMIZE/VACUUM by default. Review table size, reader duration, time-travel needs and business rollback requirements before adjusting retention.

## Audit evidence

The append-only table `gold_publication_retention_runs` records:

- retention run ID and UTC recording time;
- catalog/schema and dry-run flag;
- logical retention and successful-version floor;
- active publication ID and cutoff;
- candidate publication IDs and inventory count;
- planned/deleted/remaining row counts per output;
- requested OPTIMIZE/VACUUM options and VACUUM retention hours;
- final status and any error summary.

The notebook does not delete rows from `gold_publication_history`, `gold_publication_state`, `gold_run_audit`, `gold_quality_metrics`, or the legacy bootstrap copies.

## Testing and deployment boundary

Local unit tests validate retention selection, successful-release floors, baseline preservation, protection of active/recent/in-flight/untracked/unknown/malformed histories, guarded DELETE SQL, identifier safety and empty candidate handling. Repository checks validate that the maintenance notebook defaults to dry-run and that the Databricks importer includes the notebook and its shared Python modules.

CI does not run the maintenance notebook against Unity Catalog or validate Delta DELETE/VACUUM execution permissions. For the 22 November 2026 restored-environment acceptance, the project owner confirmed that the notebook completed successfully with `dry_run=true`, identified cleanup candidates, and deleted no data. This confirms the non-destructive planning path only; destructive DELETE execution, `OPTIMIZE`, and `VACUUM` have not been confirmed as run and require separate review and approval. The feature is not scheduled automatically. Exact retention-run IDs and notebook output have not yet been attached to the repository.
