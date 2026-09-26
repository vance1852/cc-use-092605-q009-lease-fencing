from __future__ import annotations

import json
import sqlite3
import unittest
from datetime import datetime, timezone
from pathlib import Path

from turbine_health.clock import FrozenClock
from turbine_health.errors import Conflict, Forbidden, LeaseConflict
from turbine_health.jsonio import load_json
from turbine_health.service import TrialService


ROOT = Path(__file__).resolve().parents[1]


class ServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.clock = FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc))
        self.service = TrialService(self.connection, self.clock)
        for user_id, role in (
            ("operator", "operator"),
            ("stat", "statistician"),
            ("approver", "approver"),
            ("auditor", "auditor"),
        ):
            self.service.create_user(user_id, user_id, role)
        self.protocol = load_json(ROOT / "fixtures" / "demo_protocol.json")
        self.rows = [
            json.loads(line)
            for line in (ROOT / "fixtures" / "demo_observations.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        self.service.register_robot("operator", "robot-a", "A 型", "厂商")
        self.service.register_build("operator", "build-a", "robot-a", "1.0", "b" * 64)
        self.service.publish_protocol("stat", self.protocol)
        self.service.create_batch("operator", "batch-a", "demo-delivery-v1", 1, "build-a")
        self.service.start_batch("operator", "batch-a", 1)

    def tearDown(self) -> None:
        self.connection.close()

    def _sealed_job(self) -> None:
        self.service.import_observations("operator", "batch-a", "key-1", self.rows)
        self.service.seal_batch("stat", "batch-a", 2)

    def _analysis_count(self) -> int:
        return self.connection.execute("SELECT count(*) FROM analyses").fetchone()[0]

    def _job_events(self, job_id: int) -> list[str]:
        rows = self.connection.execute(
            "SELECT event_type FROM audit_events WHERE entity_type='analysis_job' AND entity_id=? "
            "ORDER BY event_id",
            (str(job_id),),
        ).fetchall()
        return [row[0] for row in rows]

    def test_complete_workflow(self) -> None:
        imported = self.service.import_observations("operator", "batch-a", "key-1", self.rows)
        self.assertEqual(imported["inserted"], 6)
        self.service.seal_batch("stat", "batch-a", 2)
        job = self.service.claim_job("worker", 30)
        self.assertEqual(job["lease_generation"], 1)
        self.assertEqual(job["attempts"], 1)
        self.assertEqual(len(job["input_sha256"]), 64)
        analysis = self.service.complete_job("worker", job["job_id"], "stat", job["lease_generation"])
        self.assertEqual(analysis["input_sha256"], job["input_sha256"])
        self.service.decide("approver", "batch-a", analysis["analysis_id"], "approved", "满足规则")
        report = self.service.report("auditor", "batch-a")
        self.assertEqual(report["batch"]["state"], "decided")
        self.assertEqual(report["analysis"]["result"]["conclusion"], "pass")
        self.assertEqual(report["jobs"][0]["claim_input_sha256"], analysis["input_sha256"])

    def test_idempotent_replay_and_conflict(self) -> None:
        first = self.service.import_observations("operator", "batch-a", "key-1", self.rows)
        second = self.service.import_observations("operator", "batch-a", "key-1", self.rows)
        self.assertEqual(first, second)
        changed = [dict(item) for item in self.rows]
        changed[0] = dict(changed[0])
        changed[0]["metrics"] = dict(changed[0]["metrics"])
        changed[0]["metrics"]["completion_seconds"] = "99"
        with self.assertRaises(Conflict):
            self.service.import_observations("operator", "batch-a", "key-1", changed)
        count = self.connection.execute("SELECT count(*) FROM observations").fetchone()[0]
        self.assertEqual(count, 6)

    def test_import_rolls_back_when_one_source_row_duplicates(self) -> None:
        self.service.import_observations("operator", "batch-a", "key-1", self.rows[:1])
        with self.assertRaises(Conflict):
            self.service.import_observations("operator", "batch-a", "key-2", self.rows[:2])
        count = self.connection.execute("SELECT count(*) FROM observations").fetchone()[0]
        self.assertEqual(count, 1)

    def test_role_separation(self) -> None:
        with self.assertRaises(Forbidden):
            self.service.seal_batch("operator", "batch-a", 2)
        with self.assertRaises(Forbidden):
            self.service.report("operator", "batch-a")

    def test_exclusion_review_and_revoke_leave_history(self) -> None:
        self.service.import_observations("operator", "batch-a", "key-1", self.rows)
        observation_id = self.connection.execute(
            "SELECT observation_id FROM observations ORDER BY observation_id LIMIT 1"
        ).fetchone()[0]
        requested = self.service.request_exclusion("operator", observation_id, "现场记录失效")
        reviewed = self.service.review_exclusion("stat", requested["exclusion_id"], True, "证据充分")
        self.assertEqual(reviewed["status"], "approved")
        revoked = self.service.revoke_exclusion("operator", requested["exclusion_id"], "已找回原始记录")
        self.assertEqual(revoked["status"], "revoked")
        events = self.connection.execute(
            "SELECT event_type FROM audit_events WHERE entity_type='observation' AND entity_id=? ORDER BY event_id",
            (str(observation_id),),
        ).fetchall()
        self.assertEqual([row[0] for row in events], ["exclusion.requested", "exclusion.revoked"])

    def test_failed_job_returns_to_queue_after_delay(self) -> None:
        self._sealed_job()
        job = self.service.claim_job("worker-a", 10)
        failed = self.service.fail_job("worker-a", job["job_id"], job["lease_generation"], "临时计算失败", retry_seconds=5)
        self.assertEqual(failed["state"], "queued")
        self.assertIsNone(self.service.claim_job("worker-b", 10))
        self.clock.advance(seconds=5)
        retried = self.service.claim_job("worker-b", 10)
        self.assertEqual(retried["job_id"], job["job_id"])
        self.assertEqual(retried["attempts"], 2)
        self.assertEqual(retried["lease_generation"], 2)

    def test_lease_can_be_reclaimed_after_expiry(self) -> None:
        self._sealed_job()
        first = self.service.claim_job("worker-a", 10)
        self.clock.advance(seconds=11)
        second = self.service.claim_job("worker-b", 10)
        self.assertEqual(first["job_id"], second["job_id"])
        self.assertEqual(second["lease_owner"], "worker-b")
        self.assertEqual(second["lease_generation"], first["lease_generation"] + 1)
        with self.assertRaises(LeaseConflict):
            self.service.complete_job("worker-a", first["job_id"], "stat", first["lease_generation"])

    def test_stale_result_backflow_leaves_no_trace(self) -> None:
        """甲的迟到结果在乙接管后不得产生分析记录、状态变更或审计残片。"""

        self._sealed_job()
        stale = self.service.claim_job("worker-a", 10)
        job_id = stale["job_id"]
        self.clock.advance(seconds=11)
        takeover = self.service.claim_job("worker-b", 30)
        self.assertEqual(takeover["attempts"], 2)
        events_before = self._job_events(job_id)
        self.assertEqual(events_before, ["analysis_job.claimed", "analysis_job.taken_over"])
        with self.assertRaises(LeaseConflict) as raised:
            self.service.complete_job("worker-a", job_id, "stat", stale["lease_generation"])
        self.assertEqual(raised.exception.code, "lease_conflict")
        self.assertEqual(self._analysis_count(), 0)
        self.assertEqual(self.service.get_batch("batch-a")["state"], "sealed")
        self.assertEqual(self._job_events(job_id), events_before)
        job = self.connection.execute("SELECT * FROM analysis_jobs WHERE job_id=?", (job_id,)).fetchone()
        self.assertEqual(job["state"], "leased")
        self.assertEqual(job["lease_owner"], "worker-b")
        self.assertIsNone(job["completed_by"])

    def test_valid_result_survives_late_failure_and_success(self) -> None:
        """乙落库后，甲的迟到失败与迟到成功都不能覆盖乙的结果。"""

        self._sealed_job()
        stale = self.service.claim_job("worker-a", 10)
        job_id = stale["job_id"]
        self.clock.advance(seconds=11)
        takeover = self.service.claim_job("worker-b", 30)
        completed = self.service.complete_job("worker-b", job_id, "stat", takeover["lease_generation"])
        with self.assertRaises(LeaseConflict):
            self.service.fail_job("worker-a", job_id, stale["lease_generation"], "迟到的失败")
        with self.assertRaises(LeaseConflict):
            self.service.complete_job("worker-a", job_id, "stat", stale["lease_generation"])
        self.assertEqual(self._analysis_count(), 1)
        job = self.connection.execute("SELECT * FROM analysis_jobs WHERE job_id=?", (job_id,)).fetchone()
        self.assertEqual(job["state"], "succeeded")
        self.assertEqual(job["completed_by"], "worker-b")
        self.assertEqual(job["completed_generation"], takeover["lease_generation"])
        self.assertEqual(job["completed_analysis_id"], completed["analysis_id"])
        self.assertEqual(self.service.get_batch("batch-a")["state"], "analyzed")
        persisted = self.connection.execute("SELECT * FROM analyses").fetchone()
        self.assertEqual(persisted["input_sha256"], completed["input_sha256"])
        self.assertEqual(persisted["created_by"], "stat")

    def test_identical_resubmission_replays_original_response(self) -> None:
        self._sealed_job()
        job = self.service.claim_job("worker-a", 30)
        first = self.service.complete_job("worker-a", job["job_id"], "stat", job["lease_generation"])
        replay = self.service.complete_job("worker-a", job["job_id"], "stat", job["lease_generation"])
        self.assertEqual(first, replay)
        self.assertEqual(self._analysis_count(), 1)
        self.assertEqual(self._job_events(job["job_id"]).count("analysis_job.completed"), 1)
        with self.assertRaises(LeaseConflict):
            self.service.complete_job("worker-a", job["job_id"], "stat", job["lease_generation"] + 1)
        with self.assertRaises(LeaseConflict):
            self.service.complete_job("worker-b", job["job_id"], "stat", job["lease_generation"])
        self.assertEqual(self._analysis_count(), 1)

    def test_same_worker_reclaim_invalidates_previous_generation(self) -> None:
        self._sealed_job()
        first = self.service.claim_job("worker-a", 10)
        self.clock.advance(seconds=11)
        second = self.service.claim_job("worker-a", 30)
        self.assertEqual(second["lease_generation"], first["lease_generation"] + 1)
        with self.assertRaises(LeaseConflict):
            self.service.complete_job("worker-a", first["job_id"], "stat", first["lease_generation"])
        completed = self.service.complete_job("worker-a", first["job_id"], "stat", second["lease_generation"])
        self.assertEqual(completed["lease_generation"], second["lease_generation"])

    def test_expired_lease_cannot_complete_or_fail(self) -> None:
        self._sealed_job()
        job = self.service.claim_job("worker-a", 10)
        self.clock.advance(seconds=11)
        with self.assertRaises(LeaseConflict):
            self.service.complete_job("worker-a", job["job_id"], "stat", job["lease_generation"])
        with self.assertRaises(LeaseConflict):
            self.service.fail_job("worker-a", job["job_id"], job["lease_generation"], "超时后上报失败")
        self.assertEqual(self._analysis_count(), 0)
        self.assertEqual(self._job_events(job["job_id"]), ["analysis_job.claimed"])

    def test_input_digest_fence_detects_mid_lease_change(self) -> None:
        self._sealed_job()
        job = self.service.claim_job("worker-a", 30)
        observation_id = self.connection.execute(
            "SELECT observation_id FROM observations ORDER BY observation_id LIMIT 1"
        ).fetchone()[0]
        requested = self.service.request_exclusion("operator", observation_id, "封存后发现异常")
        self.service.review_exclusion("stat", requested["exclusion_id"], True, "证据充分")
        with self.assertRaises(LeaseConflict) as raised:
            self.service.complete_job("worker-a", job["job_id"], "stat", job["lease_generation"])
        self.assertIn("测点输入摘要", str(raised.exception))
        self.assertEqual(self._analysis_count(), 0)
        self.assertEqual(self.service.get_batch("batch-a")["state"], "sealed")
        self.assertEqual(self._job_events(job["job_id"]), ["analysis_job.claimed"])

    def test_revision_fence_rejects_stale_job(self) -> None:
        self._sealed_job()
        job = self.service.claim_job("worker-a", 30)
        self.connection.execute("UPDATE batches SET revision=revision+1 WHERE batch_id='batch-a'")
        with self.assertRaises(LeaseConflict) as raised:
            self.service.complete_job("worker-a", job["job_id"], "stat", job["lease_generation"])
        self.assertIn("修订号", str(raised.exception))
        self.assertEqual(self._analysis_count(), 0)

    def test_fail_job_fence_rejects_other_generations(self) -> None:
        self._sealed_job()
        first = self.service.claim_job("worker-a", 10)
        self.clock.advance(seconds=11)
        second = self.service.claim_job("worker-b", 30)
        with self.assertRaises(LeaseConflict):
            self.service.fail_job("worker-a", first["job_id"], first["lease_generation"], "甲的迟到失败")
        with self.assertRaises(LeaseConflict):
            self.service.fail_job("worker-b", first["job_id"], first["lease_generation"], "乙用错代次")
        job = self.connection.execute(
            "SELECT * FROM analysis_jobs WHERE job_id=?", (first["job_id"],)
        ).fetchone()
        self.assertEqual(job["state"], "leased")
        self.assertEqual(job["lease_owner"], "worker-b")
        failed = self.service.fail_job("worker-b", first["job_id"], second["lease_generation"], "乙主动放弃")
        self.assertEqual(failed["state"], "queued")

    def test_job_history_presents_lifecycle_and_digests(self) -> None:
        self._sealed_job()
        stale = self.service.claim_job("worker-a", 10)
        job_id = stale["job_id"]
        self.clock.advance(seconds=11)
        takeover = self.service.claim_job("worker-b", 30)
        with self.assertRaises(LeaseConflict):
            self.service.complete_job("worker-a", job_id, "stat", stale["lease_generation"])
        completed = self.service.complete_job("worker-b", job_id, "stat", takeover["lease_generation"])
        with self.assertRaises(Forbidden):
            self.service.job_history("operator", job_id)
        history = self.service.job_history("auditor", job_id)
        self.assertEqual([event["event_type"] for event in history["events"]], [
            "analysis_job.claimed", "analysis_job.taken_over", "analysis_job.completed",
        ])
        claimed, taken_over, done = (event["payload"] for event in history["events"])
        self.assertEqual(claimed["worker_id"], "worker-a")
        self.assertEqual(claimed["attempt"], 1)
        self.assertEqual(claimed["lease_generation"], stale["lease_generation"])
        self.assertEqual(claimed["input_sha256"], stale["input_sha256"])
        self.assertEqual(taken_over["worker_id"], "worker-b")
        self.assertEqual(taken_over["attempt"], 2)
        self.assertEqual(taken_over["took_over_from"]["worker_id"], "worker-a")
        self.assertEqual(taken_over["took_over_from"]["lease_generation"], stale["lease_generation"])
        self.assertEqual(done["input_sha256"], completed["input_sha256"])
        self.assertEqual(done["analysis_id"], completed["analysis_id"])
        job = history["job"]
        self.assertEqual(job["attempts"], 2)
        self.assertEqual(job["lease_generation"], takeover["lease_generation"])
        self.assertEqual(job["claim_input_sha256"], completed["input_sha256"])
        report = self.service.report("auditor", "batch-a")
        self.assertEqual(len(report["jobs"]), 1)
        self.assertEqual(report["jobs"][0]["events"][0]["event_type"], "analysis_job.claimed")


if __name__ == "__main__":
    unittest.main()
