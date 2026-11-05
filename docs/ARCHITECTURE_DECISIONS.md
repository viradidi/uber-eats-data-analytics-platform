# Architecture Decision Records

**Project:** Uber Eats Data Analytics Platform  
**Status:** Accepted design decisions for the repository implementation  
**Review date:** 2026-11-05

This document records the reasoning and trade-offs behind the current architecture. It describes committed artifacts and confirmed design intent; it does not claim an enterprise-wide production certification.

## ADR-001 — Separate orchestration from distributed transformation

**Decision:** Use Azure Data Factory (ADF) to coordinate source ingestion and stage dependencies, ADLS Gen2 as the lake landing/storage layer, and Databricks/PySpark for transformation.

**Why:** The separation keeps scheduling, dependency management, and environment configuration in the orchestration layer while keeping data-intensive transformation logic in version-controlled notebook and Python assets. The master pipeline gates Gold publication on successful completion of the API, CSV, and SQL ingestion branches.

**Consequences and limits:**
- ADF activity success is an orchestration signal, not proof of data correctness; downstream quality and reconciliation checks remain necessary.
- ADF, ADLS, SQL, and Databricks do not participate in one distributed transaction. A failure can leave partial files or committed Delta writes.
- Recovery must inspect the last successful stage and destination state before replaying.

## ADR-002 — Use source-specific ingestion contracts

**Decision:** Keep API, CSV, and SQL ingestion as distinct source patterns, coordinated by the same master pipeline.

**Why:** The sources expose different capabilities. API endpoints use authenticated paginated reads and timestamp filters; CSV sources are file-oriented; SQL sources expose a relational watermark/control-table contract.

**Consequences and limits:**
- Do not apply one generic cursor or deletion policy to all sources.
- An API timestamp filter is only complete when the upstream timestamp is reliable and the endpoint exposes all relevant changes.
- SQL watermark extraction is not change data capture. A watermark cannot identify hard deletes unless the source exposes CDC, tombstones, soft-delete flags, or a separate deletion feed.
- New sources require an approved business key, required-column set, type map, cursor semantics, replay behavior, and deletion policy before activation.

## ADR-003 — Bound SQL incremental extracts and advance checkpoints after downstream success

**Decision:** Capture a fixed upper watermark for each extraction interval, process the bounded range, complete the SQL Silver step, and only then update the control-table checkpoint. The checkpoint update uses a compare-and-swap predicate against the previously observed value; the ADF SQL pipeline is serialized with concurrency set to one.

**Why:** A fixed upper bound prevents the interval from moving while the copy is running. Delaying checkpoint advancement prevents the control state from moving past a batch whose required Silver dependency failed. Compare-and-swap detects a stale or non-unique control-row update instead of silently overwriting it.

**Consequences and limits:**
- The implemented lower predicate is strict (watermark > previous_checkpoint). Rows that become visible later with an equal/older cursor can be missed.
- Pipeline concurrency one serializes that ADF pipeline only; it is not a global lock against other pipelines, administrators, or external writers.
- Compare-and-swap protects the SQL checkpoint write. It does not create a transaction spanning SQL, ADLS, and Delta Lake.
- Use CDC or a source-approved overlap-and-replay design when late-arriving updates must be captured. Do not enable overlap without stable keys, deterministic version semantics, and a tested merge policy.
- A null/uninitialized checkpoint needs a separate, approved initial-load path; it is not silently treated as zero or the beginning of time.

## ADR-004 — Use key-based Delta MERGE for the SQL Silver batch

**Decision:** Publish one exact bounded SQL landing file through a metadata-driven Delta MERGE. Require a configured business key, required columns, complete Spark type map, timestamp bounds, and safe identifiers. The merge selects newer source versions and rejects conflicting or ambiguous batch rows.

**Why:** A keyed merge supports controlled replay of the same interval without treating every batch as a new append. Fail-closed schema and key validation prevents malformed input from being silently published.

**Consequences and limits:**
- The guarantee is specific to the declared key, source version, and target contract.
- Key changes, missing source versions, late changes outside the selected interval, and hard deletes require explicit handling.
- The merge is not proof of end-to-end exactly-once processing across all services.

## ADR-005 — Rebuild the current Gold outputs from validated Silver inputs

**Decision:** The current Gold notebook builds the five documented analytics outputs from Silver DataFrames and writes the outputs using Delta overwrite with schema replacement.

**Why:** A full rebuild keeps the current educational analytics model deterministic and makes its KPI/reconciliation rules straightforward to validate.

**Consequences and limits:**
- The current Gold write path is a full-table refresh, not an incremental Gold model.
- Runtime and compute cost grow with the full Silver input size. This approach should be benchmarked before scaling to larger history or tighter refresh windows.
- A future incremental Gold design should define partitioning, change propagation, late-update behavior, backfill strategy, schema evolution, and a rollback/rebuild path before replacing the current full refresh.

## ADR-006 — Keep cloud execution evidence separate from repository CI

**Decision:** Use GitHub Actions, unit tests, static ADF contract tests, and local-mode PySpark tests for repository validation. Treat Azure deployment and end-to-end acceptance as a separate runtime evidence class.

**Why:** CI can deterministically validate committed files and transformations without live cloud credentials or billable services. It cannot execute an ADF factory or a Databricks workspace unless an explicitly provisioned integration test environment exists.

**Consequences and limits:**
- A green CI run proves only the tests and checks that ran for that commit.
- The project owner has confirmed successful Azure deployment and end-to-end operation; the exact Azure run exports are not retained in this repository.
- Future CI integration tests should use isolated, short-lived, least-privilege resources and explicit cost/cleanup controls. Do not imply the current CI is a cloud integration test.

## ADR-007 — Use environment-backed credentials and managed identity where supported

**Decision:** The API reads its key from the API_KEY environment variable and fails closed when it is absent. ADF retrieves the API key through the Key Vault integration. The ADF factory uses a system-assigned identity, and the Databricks linked service uses managed-identity authentication.

**Why:** Credentials should not be embedded in the API source or notebook code. Managed identity avoids distributing long-lived client secrets for supported service-to-service access.

**Consequences and limits:**
- The repository shows configuration intent; actual Azure role assignments, Key Vault access policies, secret rotation, and workspace permissions must be verified in the target subscription.
- Use least privilege for storage, Key Vault, SQL, and Databricks. Avoid publishing secret values or sensitive endpoints in logs.
- The mock source data is synthetic; any future real data integration needs an explicit classification, retention, and access review.

## ADR-008 — Correlate SQL Silver audit rows to the master ADF execution

**Decision:** The master pipeline passes its run ID to the SQL child pipeline, which passes it to the SQL Silver notebook. The SQL Silver audit run_id uses the master run ID when supplied, falls back to the SQL child run ID for direct child execution, and uses a generated UUID only for manual runs without ADF context.

**Why:** The Gold audit already records the parent ADF pipeline run ID. Reusing that identifier in SQL Silver audit records allows operators to group source-level Silver outcomes with the master orchestration and Gold publication outcome.

**Consequences and limits:**
- Multiple SQL control rows processed during one master run share the same orchestration run_id; use control_id, source table, and ADF activity run details to distinguish each source execution.
- Direct notebook/manual executions may not have an ADF run ID and therefore use a fallback identifier.
- The correlation identifier is not a trace system by itself; preserve ADF activity IDs and notebook output when investigating an incident.

## Revisit triggers

Revisit these decisions when source volume, refresh objectives, data sensitivity, concurrency, or recovery requirements change. In particular, review ADR-003/004 before enabling late-arriving or delete-sensitive sources, and ADR-005 before increasing retained history or shortening Gold refresh windows.
