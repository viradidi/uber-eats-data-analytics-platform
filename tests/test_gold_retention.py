"""Unit tests for safe Gold publication retention and cleanup planning."""

import unittest
from datetime import datetime, timedelta, timezone

from src.gold.retention import (
    build_guarded_delete_sql,
    build_retention_plan,
)


class GoldRetentionPlanTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 11, 22, 12, 0, tzinfo=timezone.utc)
        self.history = [
            # Latest successful release and second-last successful release.
            {"publication_id": "success-1", "event_status": "PUBLISHED", "recorded_at_utc": self.now - timedelta(days=2)},
            {"publication_id": "success-2", "event_status": "PUBLISHED_WITH_WARNINGS", "recorded_at_utc": self.now - timedelta(days=20)},
            # Old successful version outside the 2-release safety floor.
            {"publication_id": "success-old", "event_status": "PUBLISHED", "recorded_at_utc": self.now - timedelta(days=90)},
            # A failed staged run old enough to clean.
            {"publication_id": "failed-old", "event_status": "STAGING_STARTED", "recorded_at_utc": self.now - timedelta(days=90)},
            {"publication_id": "failed-old", "event_status": "FAILED_BEFORE_COMMIT", "recorded_at_utc": self.now - timedelta(days=89)},
            # A recent failed run stays in the incident investigation window.
            {"publication_id": "failed-recent", "event_status": "FAILED_BEFORE_COMMIT", "recorded_at_utc": self.now - timedelta(days=1)},
            # An old run with no terminal event is treated as potentially in flight.
            {"publication_id": "possibly-running", "event_status": "SNAPSHOTS_STAGED", "recorded_at_utc": self.now - timedelta(days=200)},
            # Baseline can be rolled back to and must never be automatically removed.
            {"publication_id": "legacy-baseline", "event_status": "LEGACY_BASELINE_IMPORTED", "recorded_at_utc": self.now - timedelta(days=500)},
            # Active ID is protected even if its terminal event is old.
            {"publication_id": "active-old", "event_status": "PUBLISHED", "recorded_at_utc": self.now - timedelta(days=120)},
            # Unknown events and malformed timestamps fail closed.
            {"publication_id": "future-event", "event_status": "NEW_EVENT_TYPE", "recorded_at_utc": self.now - timedelta(days=120)},
            {"publication_id": "missing-time", "event_status": "FAILED_BEFORE_COMMIT", "recorded_at_utc": None},
        ]
        self.inventory = [
            "success-1",
            "success-2",
            "success-old",
            "failed-old",
            "failed-recent",
            "possibly-running",
            "legacy-baseline",
            "active-old",
            "future-event",
            "missing-time",
            "orphan-without-history",
        ]

    def test_plan_deletes_only_expired_terminal_publications_outside_success_floor(self):
        plan = build_retention_plan(
            self.history,
            self.inventory,
            active_publication_id="active-old",
            now_utc=self.now,
            retention_days=30,
            keep_successful_publications=2,
        )
        self.assertEqual(["failed-old", "success-old"], plan["candidate_publication_ids"])
        self.assertEqual("ACTIVE_PUBLICATION", plan["preserved"]["active-old"])
        self.assertEqual("LEGACY_BASELINE", plan["preserved"]["legacy-baseline"])
        self.assertEqual("RECENT_SUCCESSFUL_RELEASE", plan["preserved"]["success-1"])
        self.assertEqual("RECENT_SUCCESSFUL_RELEASE", plan["preserved"]["success-2"])
        self.assertEqual("WITHIN_RETENTION_WINDOW", plan["preserved"]["failed-recent"])

    def test_untracked_inflight_unknown_and_missing_timestamp_ids_are_preserved(self):
        plan = build_retention_plan(
            self.history,
            self.inventory,
            active_publication_id=None,
            now_utc=self.now,
            retention_days=30,
            keep_successful_publications=1,
        )
        self.assertIn("orphan-without-history", plan["untracked_publication_ids"])
        self.assertIn("possibly-running", plan["in_flight_publication_ids"])
        self.assertIn("future-event", plan["unknown_status_publication_ids"])
        self.assertIn("missing-time", plan["missing_timestamp_publication_ids"])
        self.assertNotIn("orphan-without-history", plan["candidate_publication_ids"])
        self.assertNotIn("possibly-running", plan["candidate_publication_ids"])

    def test_legacy_baseline_is_preserved_indefinitely(self):
        plan = build_retention_plan(
            self.history,
            self.inventory,
            active_publication_id=None,
            now_utc=self.now,
            retention_days=7,
            keep_successful_publications=1,
        )
        self.assertIn("legacy-baseline", plan["preserved"])
        self.assertNotIn("legacy-baseline", plan["candidate_publication_ids"])

    def test_successful_keep_floor_counts_distinct_publications(self):
        plan = build_retention_plan(
            self.history + [
                {"publication_id": "success-1", "event_status": "PUBLISHED_WITH_WARNINGS", "recorded_at_utc": self.now - timedelta(days=1)}
            ],
            self.inventory,
            active_publication_id=None,
            now_utc=self.now,
            retention_days=7,
            keep_successful_publications=2,
        )
        self.assertEqual(2, len(plan["newest_successful_publication_ids"]))
        self.assertIn("success-1", plan["newest_successful_publication_ids"])
        self.assertIn("success-2", plan["newest_successful_publication_ids"])

    def test_timezone_naive_spark_timestamps_are_treated_as_utc(self):
        history = [{
            "publication_id": "old-failed",
            "event_status": "FAILED_BEFORE_COMMIT",
            "recorded_at_utc": datetime(2026, 1, 1, 0, 0, 0),
        }]
        plan = build_retention_plan(
            history,
            ["old-failed"],
            active_publication_id=None,
            now_utc=self.now,
            retention_days=30,
            keep_successful_publications=1,
        )
        self.assertEqual(["old-failed"], plan["candidate_publication_ids"])
        self.assertTrue(plan["cutoff_utc"].endswith("+00:00"))

    def test_invalid_retention_parameters_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "at least 7"):
            build_retention_plan(
                self.history,
                self.inventory,
                active_publication_id=None,
                now_utc=self.now,
                retention_days=6,
            )
        with self.assertRaisesRegex(ValueError, "at least 1"):
            build_retention_plan(
                self.history,
                self.inventory,
                active_publication_id=None,
                now_utc=self.now,
                keep_successful_publications=0,
            )

    def test_missing_and_nonterminal_history_fail_closed(self):
        for event in (
            {"publication_id": "old", "event_status": "FAILED_BEFORE_COMMIT", "recorded_at_utc": None},
            {"publication_id": "old", "event_status": "STAGING_STARTED", "recorded_at_utc": self.now - timedelta(days=500)},
            {"publication_id": "old", "event_status": "FUTURE_STATUS", "recorded_at_utc": self.now - timedelta(days=500)},
        ):
            with self.subTest(event=event):
                plan = build_retention_plan(
                    [event],
                    ["old"],
                    active_publication_id=None,
                    now_utc=self.now,
                    retention_days=30,
                )
                self.assertEqual([], plan["candidate_publication_ids"])

    def test_unknown_event_is_kept_even_when_a_known_failure_event_exists(self):
        history = [
            {"publication_id": "old", "event_status": "FAILED_BEFORE_COMMIT", "recorded_at_utc": self.now - timedelta(days=90)},
            {"publication_id": "old", "event_status": "FUTURE_STATUS", "recorded_at_utc": self.now - timedelta(days=90)},
        ]
        plan = build_retention_plan(
            history,
            ["old"],
            active_publication_id=None,
            now_utc=self.now,
            retention_days=30,
        )
        self.assertEqual([], plan["candidate_publication_ids"])
        self.assertEqual("UNKNOWN_EVENT_STATUS", plan["preserved"]["old"])


class GoldRetentionSqlTests(unittest.TestCase):
    def test_delete_sql_checks_active_pointer_at_delete_time(self):
        sql = build_guarded_delete_sql(
            "catalog",
            "gold",
            "gold_order_360",
            ["publication-1", "publication-2"],
        )
        self.assertIn("DELETE FROM `catalog`.`gold`.`gold_order_360__versions` AS version", sql)
        self.assertIn("NOT EXISTS", sql)
        self.assertIn("`gold_publication_state`", sql)
        self.assertIn("active.active_publication_id = version._publication_id", sql)
        self.assertIn("'publication-1', 'publication-2'", sql)

    def test_sql_literals_are_escaped_and_duplicate_ids_are_collapsed(self):
        sql = build_guarded_delete_sql(
            "catalog",
            "gold",
            "gold_driver_performance",
            ["run'1", "run'1", "run-2"],
        )
        self.assertIn("'run''1', 'run-2'", sql)

    def test_empty_candidate_set_produces_no_delete_statement(self):
        self.assertIsNone(
            build_guarded_delete_sql("catalog", "gold", "gold_order_360", [])
        )

    def test_unsupported_output_name_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "Unsupported Gold output"):
            build_guarded_delete_sql("catalog", "gold", "gold_unknown", ["run-1"])


if __name__ == "__main__":
    unittest.main()
