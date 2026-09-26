from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from turbine_health.storage import connect, initialize, inspect_schema, transaction


class StorageTests(unittest.TestCase):
    def test_initialize_is_repeatable(self) -> None:
        connection = sqlite3.connect(":memory:", isolation_level=None)
        connection.row_factory = sqlite3.Row
        try:
            initialize(connection)
            initialize(connection)
            summary = inspect_schema(connection)
        finally:
            connection.close()
        self.assertEqual(summary["missing_tables"], [])
        self.assertEqual(summary["schema_version"], "3")

    def test_v2_database_is_migrated_with_lease_fence_columns(self) -> None:
        connection = sqlite3.connect(":memory:", isolation_level=None)
        connection.row_factory = sqlite3.Row
        try:
            connection.executescript(
                """
                CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                INSERT INTO schema_meta VALUES('schema_version', '2');
                CREATE TABLE analysis_jobs (
                    job_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id TEXT NOT NULL,
                    batch_revision INTEGER NOT NULL,
                    state TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    available_at TEXT NOT NULL,
                    lease_owner TEXT,
                    lease_expires_at TEXT,
                    last_error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                INSERT INTO analysis_jobs(batch_id,batch_revision,state,attempts,available_at,
                    lease_owner,lease_expires_at,created_at,updated_at)
                VALUES('batch-old', 3, 'succeeded', 2, '2026-09-20T00:00:00Z',
                    'worker-old', '2026-09-20T00:01:00Z', '2026-09-20T00:00:00Z', '2026-09-20T00:01:00Z');
                """
            )
            initialize(connection)
            initialize(connection)
            columns = {
                row["name"] for row in connection.execute("PRAGMA table_info(analysis_jobs)").fetchall()
            }
            job = connection.execute("SELECT * FROM analysis_jobs WHERE batch_id='batch-old'").fetchone()
            summary = inspect_schema(connection)
        finally:
            connection.close()
        for expected in (
            "lease_generation", "claim_input_sha256", "completed_by",
            "completed_generation", "completed_analysis_id", "completion_json",
        ):
            self.assertIn(expected, columns)
        self.assertEqual(job["completed_generation"], job["lease_generation"])
        self.assertEqual(summary["schema_version"], "3")
        self.assertEqual(summary["missing_tables"], [])

    def test_transaction_rolls_back_on_error(self) -> None:
        connection = sqlite3.connect(":memory:", isolation_level=None)
        connection.execute("CREATE TABLE items(value TEXT NOT NULL)")
        with self.assertRaises(RuntimeError):
            with transaction(connection):
                connection.execute("INSERT INTO items(value) VALUES('x')")
                raise RuntimeError("stop")
        count = connection.execute("SELECT count(*) FROM items").fetchone()[0]
        connection.close()
        self.assertEqual(count, 0)

    def test_connect_enables_foreign_keys(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            connection = connect(Path(directory) / "test.sqlite3")
            try:
                self.assertEqual(connection.execute("PRAGMA foreign_keys").fetchone()[0], 1)
            finally:
                connection.close()


if __name__ == "__main__":
    unittest.main()
