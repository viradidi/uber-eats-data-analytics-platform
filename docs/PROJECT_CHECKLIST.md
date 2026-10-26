# Project Completion and Evidence Checklist

Use this checklist to distinguish available artifacts from verified implementation.

## Repository and documentation

- [x] Project README created
- [x] Logical architecture documented
- [ ] Verify all README links
- [ ] Add an approved architecture screenshot if useful
- [ ] Record the source and licensing of any reused material

## Ingestion

- [ ] Verify API endpoints and authentication
- [ ] Validate API ingestion output
- [ ] Validate CSV ingestion output
- [x] Review SQL incremental extraction boundaries and checkpoint dependencies in the exported ADF definition
- [ ] Test reruns and partial-failure recovery

## Transformation and quality

- [ ] Validate notebook inputs and outputs in a Spark environment
- [x] Document transformation behavior and reliability findings in `docs/SILVER_TRANSFORMATION_REVIEW.md`
- [x] Add CI tests for required columns, nonblank keys, key uniqueness, allowed CSV domain values, promotion dates/discounts, and zone coordinate ranges in committed sample fixtures
- [x] Add pre-cleaning quality metrics and prepared merge-key checks to Drivers and Promotions Silver notebooks
- [x] Prevent empty flattened API payloads from overwriting existing Payments, Deliveries, Order Status, and Ratings Silver tables
- [ ] Add equivalent schema and required-field checks to the executed API Silver transformations
- [x] Add null-key and duplicate-key checks to the Zones merge input
- [x] Add latitude/longitude validity checks and fail-fast guardrails to Zones
- [x] Persist successful-run quality metrics with run ID, dataset, source/prepared/target counts, status, and timestamp for Drivers, Promotions, and Zones
- [ ] Add failed-run records and rejected-row details with a run identifier
- [x] Persist successful-run quality metrics for Payments, Deliveries, Order Status, and Ratings after successful Delta overwrite
- [ ] Add source-to-target reconciliation

## Security

- [ ] Review linked-service JSON for secrets
- [ ] Confirm no private credentials are tracked
- [ ] Confirm sample data is appropriate for public release
- [ ] Document environment configuration without secrets

## Operations

- [ ] Capture successful pipeline execution evidence
- [ ] Document failure handling and recovery
- [ ] Document deployment prerequisites
- [ ] Record known limitations
- [x] Add CI checks for repository structure, Python syntax, API key configuration, and sample CSV quality (successful run confirmed for the current workflow revision)

## Release

- [ ] Verify the README renders correctly on GitHub
- [ ] Add the repository to the portfolio website
