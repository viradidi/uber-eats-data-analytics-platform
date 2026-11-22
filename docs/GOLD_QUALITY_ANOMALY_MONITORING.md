# Gold Quality Metrics and Anomaly Monitoring

## Purpose

The existing Gold transformation module enforces hard correctness gates: required fields, stable grains, key uniqueness, value domains, driver referential integrity, KPI bounds, and payment/order/daily reconciliation. This feature adds a different control: quantitative run-over-run telemetry that can surface unusual shifts after those hard gates pass.

Anomaly statuses are advisory. They do not silently change Gold records and do not replace the existing validation gate.

## Metric catalog

| Metric | Dimension key | Unit |
|---|---|---|
| daily_order_count | order date | orders |
| daily_successful_payment_amount | order date | amount |
| daily_average_delivery_minutes | order date | minutes |
| daily_average_customer_rating | order date | rating |
| daily_delivery_completion_rate | order date | ratio |
| daily_cancellation_rate | order date | ratio |
| daily_paid_order_rate | order date | ratio |
| payment_method_failure_rate | payment date + payment method | ratio |
| driver_delivery_volume | driver ID | deliveries |
| driver_delivery_completion_rate | driver ID | ratio |
| driver_average_delivery_minutes | driver ID | minutes |
| restaurant_average_rating | restaurant ID | rating |

Only non-null metric values with a non-null dimension key are recorded. Ratios are stored as values between zero and one.

## Delta history table

The Gold notebook appends observations to `<catalog>.<gold_schema>.gold_quality_metrics`. Each row contains:

- run ID, parent ADF pipeline run ID, and UTC recording time;
- metric name, dimension key, and unit;
- current metric value;
- prior baseline mean, population standard deviation, and observation count;
- relative deviation and z-score where defined;
- status: `INSUFFICIENT_HISTORY`, `NORMAL`, or `ANOMALY`.

Rows are appended only after the Gold transformations and their hard validation gates have succeeded. The table is separate from the five full-refresh Gold business outputs.

## Baseline and anomaly policy

The detector groups historical records by `metric_name`, `dimension_key`, and `metric_unit`. It excludes prior `ANOMALY` observations from the baseline so a previously flagged outlier does not immediately normalize itself. Both `NORMAL` and `INSUFFICIENT_HISTORY` records are eligible for baseline accumulation.

Defaults:

| Parameter | Default | Meaning |
|---|---:|---|
| `quality_min_history_runs` | 5 | Minimum prior observations for this exact metric/dimension |
| `quality_z_score_threshold` | 3.0 | Standard-deviation tolerance |
| `quality_relative_change_threshold` | 0.50 | Relative-change tolerance |
| `quality_absolute_floor` | 0.05 | Prevents a zero/near-zero baseline from producing a zero tolerance |

An anomaly is flagged only when:

```text
absolute_deviation > max(
    z_score_threshold * baseline_population_stddev,
    relative_change_threshold * max(abs(baseline_mean), absolute_floor)
)
```

If fewer than the minimum number of prior observations exist, the status is `INSUFFICIENT_HISTORY`; this is not reported as normal or anomalous. A zero historical standard deviation is handled by the relative-change threshold and absolute floor.

The notebook prints the number of metric rows and anomaly rows, and writes both counts into the existing `gold_run_audit.outputs_json` payload. The ADF pipeline's success condition is unchanged: anomalies are advisory, while exceptions from the pre-existing hard validation gates still fail the Gold notebook.

## Operating guidance

1. Review `ANOMALY` rows by metric, dimension, current value, baseline, deviation ratio, and z-score.
2. Compare the signal with source volume, business calendar, ingestion completeness, and known operational changes before treating it as an incident.
3. Do not automatically discard or overwrite a metric because it is anomalous.
4. Tune notebook widget thresholds only with a documented rationale and regression tests.
5. Define a retention/compaction policy before the history table becomes large. This project appends run-level observations and does not yet implement automatic retention.

## Test coverage and boundaries

Local-mode PySpark tests cover metric generation, dimension isolation, warm-up behavior, stable-baseline anomaly detection, normal deviations, and exclusion of prior anomaly records from the baseline. Repository CI validates the Python module and tests.

The tests do not execute the Databricks notebook, Delta table writes, Unity Catalog permissions, or an Azure Data Factory run. The cloud resources used for the earlier end-to-end project execution were decommissioned; deploying this new quality table requires a fresh workspace deployment and acceptance run. Historical anomaly baselines are meaningful only when repeated runs use consistent metric definitions and comparable source coverage. A data gap or changed business calendar can still affect the signal.
