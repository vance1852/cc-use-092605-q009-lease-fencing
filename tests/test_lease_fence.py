"""分析任务租约栅栏的竞争场景测试；全部使用冻结时钟复现，不依赖真实等待。"""

from __future__ import annotations

import json
import sqlite3
import unittest
from datetime import datetime, timezone
from pathlib import Path

from turbine_health.clock import FrozenClock
from turbine_health.errors import Forbidden, LeaseConflict
from turbine_health.jsonio import load_json
from turbine_health.service import TrialService


ROOT = Path(__file__).resolve().parents[1]


class LeaseFenceTests(unittest.TestCase):
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
        protocol = load_json(ROOT / "fixtures" / "demo_protocol.json")
        self.rows = [
            json.loads(line)
            for line in (ROOT / "fixtures" / "demo_observations.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        self.service.register_robot("operator", "robot-a", "A 型", "厂商")
        self.service.register_build("operator", "build-a", "robot-a", "1.0", "b" * 64)
        self.service.publish_protocol("stat", protocol)
        self.service.create_batch("operator", "batch-a", "demo-delivery-v1", 1, "build-a")
        self.service.start_batch("operator", "batch-a", 1)
        self.service.import_observations("operator", "batch-a", "key-1", self.rows)
        self.service.seal_batch("stat", "batch-a", 2)

    def tearDown(self) -> None:
        self.connection.close()

    def _counts(self) -> tuple[int, int]:
        analyses = self.connection.execute("SELECT count(*) FROM analyses").fetchone()[0]
        audits = self.connection.execute("SELECT count(*) FROM audit_events").fetchone()[0]
        return analyses, audits

    def _job_row(self, job_id: int) -> sqlite3.Row:
        return self.connection.execute(
            "SELECT * FROM analysis_jobs WHERE job_id=?", (job_id,)
        ).fetchone()

    def _mismatch_terms(self, error: LeaseConflict) -> set[str]:
        return {item["term"] for item in error.details["mismatches"]}

    def test_stale_completion_after_takeover_leaves_no_trace(self) -> None:
        """倒灌事故复现：甲的迟到提交必须整体被拒绝，乙的有效结果不被覆盖。"""
        first = self.service.claim_job("worker-a", 10)
        self.assertEqual(first["attempts"], 1)
        self.assertEqual(first["lease_epoch"], 1)
        self.clock.advance(seconds=11)
        second = self.service.claim_job("worker-b", 10)
        self.assertEqual(second["attempts"], 2)
        self.assertEqual(second["lease_epoch"], 2)
        self.assertEqual(second["lease_owner"], "worker-b")
        self.assertEqual(second["input_sha256"], first["input_sha256"])

        analyses_before, audits_before = self._counts()
        with self.assertRaises(LeaseConflict) as raised:
            self.service.complete_job(
                "worker-a", first["job_id"], "stat", first["lease_epoch"], first["input_sha256"]
            )
        self.assertEqual(raised.exception.code, "lease_conflict")
        self.assertEqual(
            self._mismatch_terms(raised.exception),
            {"lease_owner", "lease_epoch"},
        )
        # 不产生分析记录、状态变更或审计残片
        self.assertEqual(self._counts(), (analyses_before, audits_before))
        job = self._job_row(first["job_id"])
        self.assertEqual((job["state"], job["lease_owner"], job["lease_epoch"]), ("leased", "worker-b", 2))
        self.assertEqual(self.service.get_batch("batch-a")["state"], "sealed")

        # 乙在有效租约内正常落库
        completed = self.service.complete_job(
            "worker-b", second["job_id"], "stat", second["lease_epoch"], second["input_sha256"]
        )
        self.assertEqual(completed["input_sha256"], second["input_sha256"])
        self.assertEqual(self.service.get_batch("batch-a")["state"], "analyzed")
        analyses_after, _ = self._counts()
        self.assertEqual(analyses_after, analyses_before + 1)

        # 甲的迟到成功与迟到失败都覆盖不了乙的结果
        with self.assertRaises(LeaseConflict):
            self.service.complete_job(
                "worker-a", first["job_id"], "stat", first["lease_epoch"], first["input_sha256"]
            )
        with self.assertRaises(LeaseConflict):
            self.service.fail_job("worker-a", first["job_id"], first["lease_epoch"], "迟到失败")
        job = self._job_row(first["job_id"])
        self.assertEqual((job["state"], job["lease_owner"], job["lease_epoch"]), ("succeeded", "worker-b", 2))
        self.assertEqual(self._counts()[0], analyses_after)
        stored = self.connection.execute("SELECT result_json FROM analyses").fetchone()["result_json"]
        self.assertEqual(json.loads(stored), completed["result"])

    def test_expired_lease_cannot_commit_even_without_takeover(self) -> None:
        job = self.service.claim_job("worker-a", 10)
        self.clock.advance(seconds=11)
        analyses_before, audits_before = self._counts()
        with self.assertRaises(LeaseConflict) as raised:
            self.service.complete_job(
                "worker-a", job["job_id"], "stat", job["lease_epoch"], job["input_sha256"]
            )
        self.assertIn("lease_expires_at", self._mismatch_terms(raised.exception))
        self.assertEqual(self._counts(), (analyses_before, audits_before))
        self.assertEqual(self._job_row(job["job_id"])["state"], "leased")

    def test_same_holder_identical_retry_returns_original_response(self) -> None:
        job = self.service.claim_job("worker-a", 30)
        first = self.service.complete_job(
            "worker-a", job["job_id"], "stat", job["lease_epoch"], job["input_sha256"]
        )
        analyses_before, audits_before = self._counts()
        replayed = self.service.complete_job(
            "worker-a", job["job_id"], "stat", job["lease_epoch"], job["input_sha256"]
        )
        self.assertEqual(replayed, first)
        self.assertEqual(self._counts(), (analyses_before, audits_before))
        # 同一持有者但摘要不同不是重放，而是冲突
        with self.assertRaises(LeaseConflict) as raised:
            self.service.complete_job("worker-a", job["job_id"], "stat", job["lease_epoch"], "0" * 64)
        self.assertIn("input_sha256", self._mismatch_terms(raised.exception))

    def test_input_digest_drift_blocks_commit_until_reclaimed(self) -> None:
        job = self.service.claim_job("worker-a", 60)
        observation_id = self.connection.execute(
            "SELECT observation_id FROM observations ORDER BY observation_id LIMIT 1"
        ).fetchone()[0]
        requested = self.service.request_exclusion("operator", observation_id, "现场记录失效")
        self.service.review_exclusion("stat", requested["exclusion_id"], True, "证据充分")

        analyses_before, audits_before = self._counts()
        with self.assertRaises(LeaseConflict) as raised:
            self.service.complete_job(
                "worker-a", job["job_id"], "stat", job["lease_epoch"], job["input_sha256"]
            )
        self.assertIn("input_sha256", self._mismatch_terms(raised.exception))
        self.assertEqual(self._counts(), (analyses_before, audits_before))

        # 原持有者交还任务后重新领取，拿到新的输入摘要即可正常落库
        self.service.fail_job("worker-a", job["job_id"], job["lease_epoch"], "输入已变化")
        reclaimed = self.service.claim_job("worker-a", 60)
        self.assertNotEqual(reclaimed["input_sha256"], job["input_sha256"])
        completed = self.service.complete_job(
            "worker-a", reclaimed["job_id"], "stat", reclaimed["lease_epoch"], reclaimed["input_sha256"]
        )
        self.assertEqual(completed["input_sha256"], reclaimed["input_sha256"])
        self.assertEqual(completed["result"]["excluded_count"], 1)

    def test_batch_revision_drift_blocks_commit(self) -> None:
        job = self.service.claim_job("worker-a", 60)
        self.connection.execute("UPDATE batches SET revision=revision+1 WHERE batch_id='batch-a'")
        analyses_before, audits_before = self._counts()
        with self.assertRaises(LeaseConflict) as raised:
            self.service.complete_job(
                "worker-a", job["job_id"], "stat", job["lease_epoch"], job["input_sha256"]
            )
        self.assertIn("batch_revision", self._mismatch_terms(raised.exception))
        self.assertEqual(self._counts(), (analyses_before, audits_before))

    def test_fail_job_fence_rejects_stale_and_foreign_reports(self) -> None:
        first = self.service.claim_job("worker-a", 10)
        self.clock.advance(seconds=11)
        second = self.service.claim_job("worker-b", 10)
        with self.assertRaises(LeaseConflict):
            self.service.fail_job("worker-a", first["job_id"], first["lease_epoch"], "迟到失败")
        with self.assertRaises(LeaseConflict):
            self.service.fail_job("worker-b", second["job_id"], first["lease_epoch"], "代次错误")
        job = self._job_row(first["job_id"])
        self.assertEqual((job["state"], job["lease_owner"]), ("leased", "worker-b"))

        failed = self.service.fail_job("worker-b", second["job_id"], second["lease_epoch"], "临时计算失败")
        self.assertEqual(failed["state"], "queued")
        history = self.service.job_history("auditor", first["job_id"])
        self.assertEqual(history["events"][-1]["event_type"], "analysis_job.failed")
        self.assertEqual(history["events"][-1]["payload"]["input_sha256"], second["input_sha256"])

    def test_job_history_presents_lease_timeline_with_digests(self) -> None:
        first = self.service.claim_job("worker-a", 10)
        self.clock.advance(seconds=11)
        second = self.service.claim_job("worker-b", 10)
        completed = self.service.complete_job(
            "worker-b", second["job_id"], "stat", second["lease_epoch"], second["input_sha256"]
        )

        with self.assertRaises(Forbidden):
            self.service.job_history("operator", first["job_id"])
        history = self.service.job_history("auditor", first["job_id"])
        self.assertEqual(
            [event["event_type"] for event in history["events"]],
            [
                "analysis_job.claimed",
                "analysis_job.lease_expired",
                "analysis_job.taken_over",
                "analysis_job.completed",
            ],
        )
        claimed, expired, taken_over, persisted = (event["payload"] for event in history["events"])
        self.assertEqual((claimed["worker_id"], claimed["attempts"], claimed["lease_epoch"]), ("worker-a", 1, 1))
        self.assertEqual(expired["previous_owner"], "worker-a")
        self.assertEqual(expired["previous_lease_epoch"], 1)
        self.assertEqual((taken_over["worker_id"], taken_over["attempts"], taken_over["lease_epoch"]), ("worker-b", 2, 2))
        self.assertEqual(taken_over["previous_owner"], "worker-a")
        self.assertEqual(persisted["analysis_id"], completed["analysis_id"])
        for payload in (claimed, expired, taken_over, persisted):
            self.assertEqual(len(payload["input_sha256"]), 64)
        job = history["job"]
        self.assertEqual((job["state"], job["attempts"], job["lease_epoch"]), ("succeeded", 2, 2))
        self.assertEqual(job["input_sha256"], completed["input_sha256"])


if __name__ == "__main__":
    unittest.main()
