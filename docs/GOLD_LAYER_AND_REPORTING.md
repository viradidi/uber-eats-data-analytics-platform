# Gold Layer and Business Reporting

## Purpose

The Gold layer turns validated Silver operational records into documented business-facing datasets. This adds analytical outputs without changing the existing Bronze/Silver ingestion contracts.

The project already contains SQL assets for operational health, quality trends, failures, and reconciliation. Those are operational monitoring reports. This Gold layer addresses a different need: order, payment, delivery, driver, and restaurant-feedback analysis.

## Inputs and outputs

The Gold notebook reads the existing Silver tables in the configured catalog and publishes Delta tables in the Gold schema.

| Gold table | Grain | Primary use |
|---|---|---|
| gold_order_360 | One row per order identifier observed in order events, deliveries, payments, or ratings | Order lifecycle and cross-domain drill-through |
| gold_daily_order_performance | One row per order creation date | Daily volume, fulfilment, paid orders, payment value, delivery duration, and ratings |
| gold_payment_method_performance | One row per payment date, method, and payment status | Payment mix, transaction outcomes, and attempted/successful amount |
| gold_restaurant_feedback | One row per restaurant represented in customer ratings | Rating volume, average score, and five-star share |
| gold_driver_performance | One row per assigned driver represented in delivery records | Delivery records, distinct orders, completed/cancelled records, duration, and completion rate |
| gold_run_audit | One row per Gold notebook execution | Parent ADF run correlation, output counts, status, and timestamps |

The current Gold build uses deliveries, payments, order-status events, ratings, and drivers. Promotions and delivery zones remain in Silver but are not joined into order-level metrics: the current order/delivery records do not expose reliable promo_id or zone_id relationships. Adding a guessed join would create misleading attribution.

Deliveries with a null driver_id remain available in order-level and daily order analytics but are excluded from driver-specific performance. A non-null delivery driver_id must resolve to the drivers dimension; unresolved references fail the Gold build.

## Metric definitions

- **Order creation date:** earliest available order-status event timestamp. For an order without status events, the fallback is pickup time, then first payment time, then first rating time.
- **Current order status:** latest order-status event by timestamp; if no status event exists, use the latest available delivery status; otherwise UNKNOWN.
- **Delivered / cancelled / active orders:** derived from the current order status. Active statuses are PLACED, PREPARING, ASSIGNED, and OUT_FOR_DELIVERY.
- **Paid order:** at least one payment has status SUCCESS or COMPLETED.
- **Successful payment amount:** sum of amounts for payments with status SUCCESS or COMPLETED. Failed attempts are excluded.
- **Average delivery minutes:** latest delivery record's elapsed minutes between pickup and delivery; records with missing timestamps produce a null duration. Negative elapsed times are rejected.
- **Driver completion rate:** delivered delivery records divided by all delivery records represented for a driver. It is a record-level ratio, not a distinct-order ratio.
- **Restaurant feedback:** rating count and average are calculated only from rating records present in Silver. Multiple ratings for an order may be averaged only when they retain the same customer and restaurant identity.

## Gold quality and publication gates

The shared build_gold_outputs transformation validates its inputs and outputs before the notebook starts overwriting any Gold target. A failed check raises an exception and prevents Gold publication from beginning.

Input gates include:

- Required fields and nonblank business keys/statuses.
- Explicit status contracts: order states are PLACED, PREPARING, ASSIGNED, OUT_FOR_DELIVERY, DELIVERED, or CANCELLED; delivery states are PREPARING, ASSIGNED, OUT_FOR_DELIVERY, DELIVERED, or CANCELLED; payment states are SUCCESS, COMPLETED, FAILED, DECLINED, ERROR, PENDING, or PROCESSING. New source states must be added deliberately with tests before they are accepted.
- Unique delivery, payment, status-event, rating, and driver keys at their documented source grains.
- Parseable required timestamps and parseable optional delivery timestamps when supplied.
- Nonnegative, numeric payment amounts and ratings in the inclusive 1–5 range.
- Delivery timestamps that do not precede pickup timestamps; a DELIVERED record must have both timestamps.
- Non-null delivery driver references must resolve to the drivers dimension.
- A given order's ratings must not conflict on customer or restaurant identity.

Output gates include:

- Non-null, unique keys at each documented Gold grain.
- Nonnegative counts and amounts, paid-order counts no greater than total orders, and mutually exclusive delivered/cancelled/active classifications that do not exceed total orders.
- Payment transaction and amount relationships, restaurant rating bounds and five-star shares, and driver completion-rate bounds.
- Reconciliation of the daily order count to dated rows in gold_order_360.
- Reconciliation of successful payment amounts across order-level, daily, and payment-method outputs.

The checks intentionally reject defects rather than silently deduplicating or choosing an arbitrary parent dimension. If a source contract evolves, its accepted values and tests must be updated with that contract. The transformation does not claim to validate every possible business rule or replace source-system reconciliation.

## Publish and orchestration behavior

notebooks/Gold_Analytics.py and its Databricks notebook export use the shared transformation module in src/gold/transformations.py. Each Gold output is published as a full-refresh Delta table using overwrite with schema replacement. The Gold audit record is appended after publication and includes the output row counts plus the parent ADF pipeline run ID when supplied.

The exported ADF master pipeline, pipeline/uber_eats_master.json, runs the API, CSV, and SQL ingestion pipelines concurrently, waits for all three to finish successfully, and only then invokes the Gold notebook. If an ingestion branch fails, the Gold publication activity is not eligible to start.

Before deploying the master pipeline, import the Gold notebook into the Databricks workspace at /Shared/UberEats/Gold_Analytics, make the src.gold module available to the notebook (for example through the repository checkout or a workspace deployment package), and review the Databricks linked service, catalog, schema, storage access, and cluster/runtime configuration. The JSON export is a deployment artifact, not proof of a live ADF run.

## Reporting

The read-only query pack in reports/GOLD_ANALYTICS_REPORTING.sql contains daily KPI, order-state, payment-method, driver-performance, restaurant-feedback, and order drill-through queries. These result sets can be connected to a BI tool after deployment.

This repository does not include a deployed Power BI workspace or published dashboard. The reporting SQL and documented Gold table contracts provide the serving interface for a future dashboard without pretending that one already exists.

## Data scope and limitations

All source records are synthetic demonstration data created for this portfolio project. They are not official Uber Eats data, do not represent actual customer or driver activity, and must not be used to make claims about Uber Eats' real business performance.

Gold outputs are full-refresh snapshots. The current project does not implement a historical snapshot strategy or SCD Type 2 dimension history in Gold. Order-level aggregations depend on the keys and event history available in Silver. Promotion attribution and zone-level fulfilment analysis require explicit foreign-key relationships in the source model before they can be built safely.

The local PySpark unit tests validate transformation behavior and quality gates on small synthetic DataFrames. Static tests inspect the ADF JSON dependency contract. Neither class of test executes the live Data Factory pipeline or a Databricks workspace job.
