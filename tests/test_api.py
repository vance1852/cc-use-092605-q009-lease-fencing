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
        self.clock = FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc))
        self.service = TrialService(self.connection, self.clock)
        self.app = JsonApplication(self.service)

    def tearDown(self) -> None:
        self.connection.close()

    def _post(self, path: str, payload: dict, actor: str | None = None):
        headers = {"X-Actor-Id": actor} if actor else {}
        return self.app.handle("POST", path, headers, json.dumps(payload).encode())

    def _get(self, path: str, actor: str | None = None):
        headers = {"X-Actor-Id": actor} if actor else {}
        return self.app.handle("GET", path, headers)

    def _sealed_batch(self) -> None:
        self.service.create_user("operator", "操作员", "operator")
        self.service.create_user("stat", "统计", "statistician")
        self.service.create_user("auditor", "审计", "auditor")
        self.service.register_robot("operator", "robot-a", "A 型", "厂商")
        self.service.register_build("operator", "build-a", "robot-a", "1.0", "b" * 64)
        self.service.publish_protocol("stat", load_json(ROOT / "fixtures" / "demo_protocol.json"))
        self.service.create_batch("operator", "batch-a", "demo-delivery-v1", 1, "build-a")
        self.service.start_batch("operator", "batch-a", 1)
        rows = [
            json.loads(line)
            for line in (ROOT / "fixtures" / "demo_observations.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        self.service.import_observations("operator", "batch-a", "key-1", rows)
        self.service.seal_batch("stat", "batch-a", 2)

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

    def test_job_routes_require_lease_generation(self) -> None:
        self._sealed_batch()
        claimed = self._post("/jobs/claim", {"worker_id": "w1", "lease_seconds": 30})
        self.assertEqual(claimed.status, 200)
        job = claimed.body["job"]
        self.assertEqual(job["lease_generation"], 1)
        self.assertEqual(len(job["input_sha256"]), 64)
        missing = self._post(f"/jobs/{job['job_id']}/complete", {"worker_id": "w1"}, actor="stat")
        self.assertEqual(missing.status, 422)
        missing_fail = self._post(f"/jobs/{job['job_id']}/fail", {"worker_id": "w1", "error": "x"})
        self.assertEqual(missing_fail.status, 422)

    def test_complete_replay_and_history_over_http(self) -> None:
        self._sealed_batch()
        job = self._post("/jobs/claim", {"worker_id": "w1", "lease_seconds": 30}).body["job"]
        completed = self._post(
            f"/jobs/{job['job_id']}/complete",
            {"worker_id": "w1", "lease_generation": job["lease_generation"]},
            actor="stat",
        )
        self.assertEqual(completed.status, 200)
        replay = self._post(
            f"/jobs/{job['job_id']}/complete",
            {"worker_id": "w1", "lease_generation": job["lease_generation"]},
            actor="stat",
        )
        self.assertEqual(replay.status, 200)
        self.assertEqual(replay.body, completed.body)
        denied = self._get(f"/jobs/{job['job_id']}/history", actor="operator")
        self.assertEqual(denied.status, 403)
        history = self._get(f"/jobs/{job['job_id']}/history", actor="auditor")
        self.assertEqual(history.status, 200)
        self.assertEqual(
            [event["event_type"] for event in history.body["events"]],
            ["analysis_job.claimed", "analysis_job.completed"],
        )
        self.assertEqual(history.body["events"][-1]["payload"]["input_sha256"], completed.body["input_sha256"])

    def test_stale_completion_returns_identifiable_conflict(self) -> None:
        self._sealed_batch()
        stale = self._post("/jobs/claim", {"worker_id": "w1", "lease_seconds": 10}).body["job"]
        self.clock.advance(seconds=11)
        takeover = self._post("/jobs/claim", {"worker_id": "w2", "lease_seconds": 30}).body["job"]
        self.assertEqual(takeover["lease_generation"], stale["lease_generation"] + 1)
        conflict = self._post(
            f"/jobs/{stale['job_id']}/complete",
            {"worker_id": "w1", "lease_generation": stale["lease_generation"]},
            actor="stat",
        )
        self.assertEqual(conflict.status, 409)
        self.assertEqual(conflict.body["error"]["code"], "lease_conflict")
        self.assertIn("job_id", conflict.body["error"]["details"])
        failed = self._post(
            f"/jobs/{stale['job_id']}/fail",
            {"worker_id": "w1", "lease_generation": stale["lease_generation"], "error": "迟到"},
        )
        self.assertEqual(failed.status, 409)
        self.assertEqual(failed.body["error"]["code"], "lease_conflict")
        completed = self._post(
            f"/jobs/{takeover['job_id']}/complete",
            {"worker_id": "w2", "lease_generation": takeover["lease_generation"]},
            actor="stat",
        )
        self.assertEqual(completed.status, 200)
        history = self._get(f"/jobs/{takeover['job_id']}/history", actor="auditor")
        self.assertEqual(
            [event["event_type"] for event in history.body["events"]],
            ["analysis_job.claimed", "analysis_job.taken_over", "analysis_job.completed"],
        )


if __name__ == "__main__":
    unittest.main()
