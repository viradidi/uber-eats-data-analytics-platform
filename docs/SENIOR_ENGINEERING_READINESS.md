# Senior Engineering Readiness Review

**Project:** Uber Eats Data Analytics Platform  
**Review date:** 2026-11-22  
**Overall state:** Latest Azure enhancement deployment, Gold publication checks and retention dry-run owner-confirmed; repository validation green
**Latest CI evidence:** [GitHub Actions run #830 — all workflow steps passed](https://github.com/viradidi/uber-eats-data-analytics-platform/actions/runs/45045827561) on commit `231c949fae467eaec902b359be9d7baf1f693aac`.

## Executive assessment

The repository demonstrates substantial data-engineering implementation: API, CSV and SQL ingestion; ADF master orchestration; ADLS landing; Databricks/PySpark Silver processing; metadata-driven SQL Silver MERGE; tie-safe composite API cursors; API health/request observability and idempotent fixture generation; five Gold analytics outputs; run-level Gold quality/anomaly metrics; versioned Gold snapshots exposed through stable views and a compare-and-swap active pointer; publication/run audit history; and dry-run-first retention controls.

The project owner confirms that the restored Azure deployment ran without runtime errors, the Gold pipeline completed, all five stable views contain data, publication metrics/history/audit and the active pointer were updated, and the retention dry-run found candidates without deleting data. Repository CI and cloud runtime acceptance remain separate evidence types. Exact Azure run exports and measured runtime counts have not been retained in this repository, so this review does not invent throughput, latency, availability, cost, or incident-recovery metrics.

## Senior-level capability matrix

| Competency | Current evidence | Assessment boundary |
|---|---|---|
| Architecture and platform decomposition | ADF orchestration, ADLS Gen2 landing, Databricks/PySpark transformations, master pipeline dependency gate | Implemented design; not a continuously operated service claim |
| Multi-source ingestion | API, CSV, and SQL control-driven ingestion assets | End-to-end deployment and operation owner-confirmed |
| Incremental processing | Tie-safe composite API cursors, upper-bounded API pagination, bounded SQL watermark intervals, source contracts, checkpoint compare-and-swap, SQL Silver Delta MERGE | Tested contracts are source-specific; strict SQL lower watermark does not capture every late/equal cursor or hard delete |
| Data quality | Required-field/type/key validation, quarantine, Gold KPI checks, merge and full-snapshot reconciliation helpers | Checks exist; each production threshold and source-specific outcome needs retained evidence |
| Reliability and recovery | Checkpoint advances after SQL Silver success; atomic Gold publication pointer; rollback/recovery runbooks; dry-run-first retention controls | No distributed transaction across SQL, ADLS and Databricks; destructive retention remains separately governed |
| Observability | Request IDs and structured API events; SQL Silver and Gold run audits; persisted Gold quality metrics/anomaly flags; publication and retention run history; operational SQL reports | No live dashboard/alerting service or measured SLO is claimed |
| Security | API key environment configuration and fail-closed behavior; Key Vault integration; system-assigned ADF identity; Databricks MSI linked service | Azure role assignments, secret rotation, and effective permissions require environment-level verification |
| Testing and delivery | GitHub Actions validates JSON/notebook artifacts, Python syntax, PowerShell parsing, API behavior, contracts, and local Spark tests | Tests do not run ADF or a live Databricks workspace |
| Analytics engineering | Five Gold outputs with documented grain, KPI logic, SQL reporting, versioned snapshots, stable views and atomic active-pointer publication | Gold computation remains a full-output rebuild per publication rather than incremental Gold processing; no deployed Power BI dashboard is claimed |
| Performance and cost | Configurable environment restore/deployment scripts and a defined Spark/ADF architecture | No benchmark, throughput, cost-per-run, or capacity result is currently retained |

## Implemented operational controls

### 1. Run correlation

The master ADF execution ID is now passed into the SQL ingestion child pipeline and SQL Silver notebook. The SQL Silver audit uses the parent run ID when available, the child pipeline run ID for direct child execution, and a UUID only for manual execution without ADF context. This lets SQL Silver audit rows be grouped with the Gold audit by master run.

**Azure rollout status:** the project owner confirms that the focused run-correlation update has been applied and a subsequent master ADF run has been verified. The SQL Silver and Gold audit records were checked against that execution. Exact run identifiers and raw audit exports have not been added to the repository, so this remains owner-confirmed operational evidence rather than an independently inspectable log bundle. The [rollout procedure](APPLY_LATEST_AZURE_UPDATE.md) is retained for reproducibility.

### 2. Checkpoint safety

The SQL pipeline captures an upper watermark, processes a bounded interval, waits for the SQL Silver notebook to succeed, and then updates the checkpoint using a compare-and-swap condition. ADF pipeline concurrency is set to one. These controls reduce stale checkpoint advancement but do not provide a cross-service transaction or a global lock against unrelated writers.

### 3. Replay and publication

The API uses deterministic composite cursors and a captured upper cursor so new records arriving after the run boundary are deferred to the next run; ADF advances the API checkpoint only after the corresponding Silver dispatcher succeeds. The SQL Silver notebook reads one exact batch file, validates the configured key/schema/type/bounds contract, and applies a version-aware Delta MERGE. Gold builds five output snapshots under a run publication ID, reconciles the staged counts, and changes one compare-and-swap active pointer so stable views move together to the new publication. The recovery runbook requires inspection of destination state before retrying a partially completed activity.

### 4. Quality and reconciliation

The repository provides quality/quarantine and reconciliation helpers, hard Gold output validation, run-level Gold quality metrics and advisory anomaly monitoring, plus read-only operational SQL. The owner confirms the latest quality metrics were persisted and all five views were populated after publication. Anomaly flags remain advisory; hard validation and reconciliation checks remain publication gates. Absence of a quality/reconciliation row is an unknown outcome, not a success. Failure-event logging is best-effort so an event-write error does not mask the original processing exception.

### 5. Security boundaries

The API fails closed when API_KEY is not set. The ADF pipeline uses Key Vault for its API key, and supported ADF-to-Databricks authentication uses managed identity. Never commit credentials, callback signatures, tokens, or customer data. A repository configuration file cannot prove that the corresponding cloud identity has correct least-privilege permissions.

## Remaining senior-level gaps and the correct treatment

These are the remaining maturity/evidence opportunities. They do not negate the confirmed deployment or end-to-end operation.

| Area | Current gap | Correct next action |
|---|---|---|
| Runtime evidence retention | Owner-confirmed deployment, Gold view, publication, audit and retention dry-run results; exact ADF/Databricks run IDs, timestamps, actual counts, and sanitized exports are not attached | Export original run records, publication/retention outputs and reconciliation results from Azure; do not reconstruct or fabricate them |
| Alerting and SLOs | Operational SQL reports exist, but no live alert route or measured service objective is established here | Define service owners and alert thresholds from observed baseline runs; test delivery and escalation |
| Performance/capacity | No representative-volume benchmark or cost-per-run measurement is retained | Benchmark source read, Bronze write, Silver merge, and full Gold rebuild separately; capture input size, cluster/runtime configuration, duration, shuffle/spill, and output count |
| Gold compute and lifecycle | Gold is fully recomputed for each publication; versioned snapshots accumulate and the retention dry-run has been exercised | Benchmark the full rebuild and table growth; introduce incremental/partitioned Gold only with measured evidence. Execute destructive retention/OPTIMIZE/VACUUM only in an approved maintenance window |
| Late-arriving updates | Strict SQL watermark lower bound can miss rows whose cursor is equal to or older than the committed checkpoint | Adopt CDC or a documented overlap window with a stable business key and tested replay-safe merge |
| Hard deletes | Basic watermark extraction does not emit deleted source records | Add CDC tombstones/soft-delete feed or scheduled full-snapshot deletion reconciliation if the source supports it |
| Security governance | Cloud role assignments, secret rotation, and data retention controls are environment-specific | Verify least-privilege access in Azure and document approved classification/retention before onboarding real sensitive data |
| Failure injection | Repository tests cover deterministic contracts, but not every live Azure partial-write/recovery scenario | Exercise controlled failures in an isolated environment, capture checkpoint and target state before/after, and retain only real results |

## Measurement plan — capture facts, not invented targets

Use the following baseline for a representative deployment run. Record the actual value, workload size, configuration, and date; do not fill the fields with estimates.

| Measurement | Capture method | Why it matters |
|---|---|---|
| End-to-end and per-activity duration | ADF run/activity timestamps | Identifies orchestration and dependency bottlenecks |
| Rows/bytes at source, Bronze, Silver, and each Gold output | Source counts, storage metadata, notebook audit and table counts | Shows volume progression and data loss/expansion |
| SQL MERGE input size and target size | SQL Silver audit plus Delta table metrics | Explains merge cost and target growth |
| Spark executor/driver utilization, shuffle, spill, and failures | Databricks run/cluster metrics | Distinguishes compute pressure from source/IO bottlenecks |
| Cost per accepted run | Azure cost export tagged to the project resources and run window | Supports actual cost optimization decisions |
| Data freshness/lag | Source max event/update time compared with published Gold time | Quantifies delivery latency |
| Recovery time and replay outcome | Controlled incident exercise with before/after checkpoint and target comparison | Establishes recoverability rather than assuming it |

Do not publish a numeric SLO, throughput, latency, cost saving, or recovery-time claim until enough representative runs have been measured and the calculation is documented.

## Operational posture

The exported SQL ADF path currently uses zero automatic activity retries. Keep that conservative behavior until the side-effect semantics of the exact source/landing/notebook operation are verified. For failed writes, inspect the landing object, target Delta state, audit records, and checkpoint before choosing a bounded replay. Do not enable blind retries merely to make pipeline runs appear more resilient.

Gold currently computes a full output set for each publication, then stages immutable versioned snapshots and atomically switches the stable views through the active pointer. Atomic publication improves consumer consistency; it does not make the transformation incremental. Treat runtime and cost as workload-dependent, and do not claim arbitrary-volume scalability without benchmarks.

## Interview-ready engineering narrative

A senior-level discussion should be able to explain:
- why ADF handles orchestration while Databricks handles transformations;
- why source-specific ingestion contracts are safer than one generic ingestion abstraction;
- how a fixed upper watermark and compare-and-swap checkpoint update reduce cursor drift and stale writes;
- why those controls do not solve late-arriving equal/older cursor values or hard deletes;
- how the SQL Silver MERGE validates keys, schema, types, versions, and replay behavior;
- how the API composite cursor and upper-bound pagination prevent within-run cursor drift while deferring later arrivals;
- how Gold stages versioned snapshots, validates all five outputs, and uses a compare-and-swap pointer so stable views switch as one publication;
- why Gold computation still rebuilds the full output set and what evidence would justify an incremental design;
- how failure events, audit rows, reconciliation SQL, and a recovery runbook support operations;
- what local CI proves and what it does not prove about Azure runtime behavior.

## Final status

The latest Azure enhancement deployment and the explicitly confirmed Gold publication and retention dry-run checks are complete according to the project owner's confirmation. GitHub Actions run #830 passed the full repository validation workflow. The repository now documents API reliability and incremental controls, run-level Gold quality, atomic versioned publication, retention safeguards, run correlation, and technical limits. Remaining maturity work is focused on attaching original runtime evidence, measured performance/cost baselines, alert/SLO operation, controlled failure-injection evidence, and environment-level governance—not on inventing numbers or relabeling untested capabilities.
