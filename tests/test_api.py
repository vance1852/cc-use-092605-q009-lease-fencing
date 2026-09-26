from __future__ import annotations

import json
import sqlite3
import unittest
from datetime import datetime, timezone
from pathlib import Path

from turbine_health.api import JsonApplication
from turbine_health.clock import FrozenClock
from turbine_health.jsonio import load_json
from turbine_health.service import TrialService


ROOT = Path(__file__).resolve().parents[1]


class ApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.app = JsonApplication(TrialService(self.connection))

    def tearDown(self) -> None:
        self.connection.close()

    def test_health(self) -> None:
        response = self.app.handle("GET", "/health")
        self.assertEqual(response.status, 200)
        self.assertEqual(response.body["status"], "ok")

    def test_json_error_shape(self) -> None:
        response = self.app.handle("POST", "/users", body=b"not-json")
        self.assertEqual(response.status, 422)
        self.assertEqual(response.body["error"]["code"], "validation_failed")

    def test_user_route(self) -> None:
        payload = json.dumps({"user_id": "u1", "display_name": "操作员", "role": "operator"}).encode()
        response = self.app.handle("POST", "/users", body=payload)
        self.assertEqual(response.status, 201)
        self.assertEqual(response.body["role"], "operator")


class JobFenceApiTests(unittest.TestCase):
    """租约栅栏在 HTTP 层的可观察形状；时钟冻结以复现接管竞争。"""

    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.clock = FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc))
        self.service = TrialService(self.connection, self.clock)
        self.app = JsonApplication(self.service)
        for user_id, role in (
            ("operator", "operator"),
            ("stat", "statistician"),
            ("auditor", "auditor"),
        ):
            self.service.create_user(user_id, user_id, role)
        protocol = load_json(ROOT / "fixtures" / "demo_protocol.json")
        rows = [
            json.loads(line)
            for line in (ROOT / "fixtures" / "demo_observations.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        self.service.register_robot("operator", "robot-a", "A 型", "厂商")
        self.service.register_build("operator", "build-a", "robot-a", "1.0", "b" * 64)
        self.service.publish_protocol("stat", protocol)
        self.service.create_batch("operator", "batch-a", "demo-delivery-v1", 1, "build-a")
        self.service.start_batch("operator", "batch-a", 1)
        self.service.import_observations("operator", "batch-a", "key-1", rows)
        self.service.seal_batch("stat", "batch-a", 2)

    def tearDown(self) -> None:
        self.connection.close()

    def _claim(self, worker_id: str) -> dict:
        response = self.app.handle(
            "POST", "/jobs/claim",
            body=json.dumps({"worker_id": worker_id, "lease_seconds": 10}).encode(),
        )
        self.assertEqual(response.status, 200)
        return response.body["job"]

    def _complete(self, job: dict, worker_id: str) -> object:
        return self.app.handle(
            "POST", f"/jobs/{job['job_id']}/complete",
            headers={"X-Actor-Id": "stat"},
            body=json.dumps({
                "worker_id": worker_id,
                "lease_epoch": job["lease_epoch"],
                "input_sha256": job["input_sha256"],
            }).encode(),
        )

    def test_stale_complete_returns_identifiable_conflict(self) -> None:
        first = self._claim("worker-a")
        self.assertEqual(first["lease_epoch"], 1)
        self.assertEqual(len(first["input_sha256"]), 64)
        self.clock.advance(seconds=11)
        second = self._claim("worker-b")
        self.assertEqual(second["lease_epoch"], 2)

        stale = self._complete(first, "worker-a")
        self.assertEqual(stale.status, 409)
        self.assertEqual(stale.body["error"]["code"], "lease_conflict")
        terms = {item["term"] for item in stale.body["error"]["details"]["mismatches"]}
        self.assertEqual(terms, {"lease_owner", "lease_epoch"})

        missing = self.app.handle(
            "POST", f"/jobs/{first['job_id']}/complete",
            headers={"X-Actor-Id": "stat"},
            body=json.dumps({"worker_id": "worker-b"}).encode(),
        )
        self.assertEqual(missing.status, 422)

        completed = self._complete(second, "worker-b")
        self.assertEqual(completed.status, 200)
        replayed = self._complete(second, "worker-b")
        self.assertEqual(replayed.status, 200)
        self.assertEqual(replayed.body, completed.body)

    def test_job_history_route_requires_audit_permission(self) -> None:
        first = self._claim("worker-a")
        self.clock.advance(seconds=11)
        self._claim("worker-b")

        anonymous = self.app.handle("GET", f"/jobs/{first['job_id']}/history")
        self.assertEqual(anonymous.status, 422)
        denied = self.app.handle(
            "GET", f"/jobs/{first['job_id']}/history", headers={"X-Actor-Id": "operator"}
        )
        self.assertEqual(denied.status, 403)
        history = self.app.handle(
            "GET", f"/jobs/{first['job_id']}/history", headers={"X-Actor-Id": "auditor"}
        )
        self.assertEqual(history.status, 200)
        self.assertEqual(
            [event["event_type"] for event in history.body["events"]],
            ["analysis_job.claimed", "analysis_job.lease_expired", "analysis_job.taken_over"],
        )
        for event in history.body["events"]:
            self.assertEqual(len(event["payload"]["input_sha256"]), 64)


if __name__ == "__main__":
    unittest.main()
