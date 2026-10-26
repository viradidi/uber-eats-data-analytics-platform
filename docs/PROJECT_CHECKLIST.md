
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

- [ ] Validate notebook inputs and outputs
- [ ] Document transformation rules
- [ ] Add schema and required-field tests
- [ ] Add uniqueness and duplicate checks where applicable
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
- [x] Add CI checks where appropriate (GitHub Actions workflow committed; successful run still to be confirmed)

## Release

- [ ] Review `git status`
- [ ] Review staged files before committing
- [ ] Push documentation changes to GitHub
- [ ] Verify the README renders correctly on GitHub
- [ ] Add the repository to the portfolio website
