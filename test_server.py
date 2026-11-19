import http.client
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from contextlib import closing, contextmanager
from pathlib import Path
from unittest.mock import patch

import server


class BackendTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.db = self.root / "data.sqlite3"
        self.log = self.root / "logs" / "server.log"
        self.env = dict(os.environ, PYTHONIOENCODING="utf-8")

    def command(self, *args, database=None):
        return [
            sys.executable, "-B", str(Path(server.__file__).resolve()),
            "--db", str(self.db if database is None else database), "--log-file", str(self.log),
            "--port", "0", *map(str, args),
        ]

    def run_cli(self, *args, database=None, expected_code=0):
        result = subprocess.run(
            self.command(*args, database=database), capture_output=True, encoding="utf-8",
            env=self.env, timeout=15,
        )
        self.assertEqual(result.returncode, expected_code, result.stderr)
        return result

    @contextmanager
    def running(self, *args, database=None):
        with subprocess.Popen(
            self.command("-i", *args, database=database), stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, encoding="utf-8", env=self.env,
        ) as process:
            try:
                address = process.stdout.readline().strip()
                self.assertTrue(address.startswith("Listening on http://127.0.0.1:"), address)
                yield int(address.rsplit(":", 1)[1]), process
            finally:
                if process.poll() is None:
                    try:
                        process.communicate("exit\n" if not process.stdin.closed else None, timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.communicate()
                        raise

    def request(self, port, method, path, payload=None):
        with closing(http.client.HTTPConnection("127.0.0.1", port, timeout=5)) as connection:
            headers = {"Content-Type": "application/json"} if payload is not None else {}
            body = None if payload is None else json.dumps(payload).encode("utf-8")
            connection.request(method, path, body, headers)
            response = connection.getresponse()
            data = response.read()
            return response.status, json.loads(data) if data else None

    def create_legacy_db(self):
        with closing(sqlite3.connect(str(self.db))) as connection, connection:
            connection.execute("CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT NOT NULL)")
            connection.execute("INSERT INTO items (data) VALUES (?)", ('{"name":"legacy","status":"active"}',))

    def test_crud_filters_and_legacy_upgrade(self):
        self.create_legacy_db()
        with self.running() as (port, _):
            first = {"id": 1, "data": {"name": "legacy", "status": "active"}}
            self.assertEqual(self.request(port, "GET", "/health"), (200, {"status": "ok"}))
            self.assertEqual(self.request(port, "GET", "/api/items/1"), (200, first))
            data = {"name": "物资 LEGACY", "status": "active"}
            second = {"id": 2, "data": data}
            self.assertEqual(self.request(port, "POST", "/api/items", data), (201, second))
            self.assertEqual(self.request(port, "GET", "/api/items?name=LEGACY&status=active&limit=1"),
                             (200, {"items": [first], "next_after": 1}))
            self.assertEqual(self.request(port, "GET", "/api/items?after=1"),
                             (200, {"items": [second], "next_after": None}))
            status, page = self.request(port, "GET", "/api/items?sort=-updated_at&limit=1")
            self.assertEqual(status, 200)
            self.assertEqual(page["items"][0]["id"], 2)
            self.assertGreater(page["items"][0]["updated_at"], 0)
            status, next_page = self.request(port, "GET", f"/api/items?sort=-updated_at&after={page['next_after']}")
            self.assertEqual(status, 200)
            self.assertEqual(next_page, {"items": [dict(first, updated_at=None)], "next_after": None})
            self.assertEqual(self.request(port, "PUT", "/api/items/2", {}),
                             (200, {"id": 2, "data": {}}))
            self.assertEqual(self.request(port, "DELETE", "/api/items/2"), (204, None))
            self.assertEqual(self.request(port, "GET", "/api/items/2"), (404, {"error": "not_found"}))
            self.assertEqual(self.request(port, "GET", "/api/items?limit=51"),
                             (400, {"error": "invalid_pagination"}))
        self.assertIn("POST /api/items 201", self.log.read_text(encoding="utf-8"))

    def test_wal_allows_reads_while_writer_holds_lock(self):
        with self.running("--journal-mode", "wal", "--busy-timeout-ms", "300") as (port, _):
            original = {"id": 1, "data": {"name": "original"}}
            self.assertEqual(self.request(port, "POST", "/api/items", original["data"]), (201, original))
            with closing(sqlite3.connect(str(self.db))) as writer:
                self.assertEqual(writer.execute("PRAGMA journal_mode").fetchone(), ("wal",))
                writer.execute("BEGIN EXCLUSIVE")
                writer.execute("UPDATE items SET data=? WHERE id=1", ('{"name":"changed"}',))
                self.assertEqual(self.request(port, "GET", "/api/items/1"), (200, original))
                started = time.monotonic()
                reply = self.request(port, "POST", "/api/items", {"name": "blocked"})
                self.assertEqual(reply, (503, {"error": "database_unavailable"}))
                self.assertGreaterEqual(time.monotonic() - started, 0.25)
                writer.commit()
            self.assertEqual(self.request(port, "GET", "/api/items/1"),
                             (200, {"id": 1, "data": {"name": "changed"}}))
        self.assertIn("SQLite journal mode: wal; lock wait: 300 ms", self.log.read_text(encoding="utf-8"))
        with self.running() as (port, _):
            with closing(sqlite3.connect(str(self.db))) as connection:
                self.assertEqual(connection.execute("PRAGMA journal_mode").fetchone(), ("wal",))
            self.assertEqual(self.request(port, "GET", "/health")[0], 200)
        with self.running("--journal-mode", "delete", "--busy-timeout-ms", 0) as (port, _):
            with closing(sqlite3.connect(str(self.db))) as writer:
                self.assertEqual(writer.execute("PRAGMA journal_mode").fetchone(), ("delete",))
                writer.execute("BEGIN EXCLUSIVE")
                self.assertEqual(self.request(port, "GET", "/health"), (503, {"error": "database_unavailable"}))
            self.assertEqual(self.request(port, "GET", "/health")[0], 200)

    def test_online_wal_backup_and_restore(self):
        backup = self.root / "snapshot.sqlite3"
        restored = self.root / "restored.sqlite3"
        with self.running("--journal-mode", "wal") as (port, _):
            record = {"id": 1, "data": {"name": "first"}}
            self.assertEqual(self.request(port, "POST", "/api/items", record["data"]), (201, record))
            with closing(sqlite3.connect(str(self.db))) as writer:
                writer.execute("BEGIN IMMEDIATE")
                writer.execute("DELETE FROM items")
                self.run_cli("--backup", backup)
            self.run_cli("--backup", backup, expected_code=1)
        self.run_cli("--restore", backup, database=restored)
        with self.running(database=restored) as (port, _):
            self.assertEqual(self.request(port, "GET", "/api/items/1"), (200, record))
        damaged = self.root / "damaged.sqlite3"
        rejected = self.root / "rejected.sqlite3"
        damaged.write_bytes(b"not a database")
        self.run_cli("--restore", damaged, database=rejected, expected_code=1)
        self.assertFalse(rejected.exists())

    def test_self_check_leaves_data_and_settings_untouched(self):
        result = self.run_cli("--check", "--journal-mode", "wal", "--busy-timeout-ms", 1001)
        self.assertIn("wal, lock wait 1001 ms", result.stdout)
        self.assertFalse(self.db.exists())
        self.assertFalse(self.log.parent.exists())
        self.create_legacy_db()
        original = self.db.read_bytes()
        result = self.run_cli("--check", "--journal-mode", "wal")
        self.assertIn("current journal mode: delete", result.stdout)
        self.assertEqual(self.db.read_bytes(), original)
        self.assertEqual(sorted(path.name for path in self.root.iterdir()), [self.db.name])
        with server.ItemStore(self.db, 1001).connect() as connection:
            self.assertEqual(connection.execute("PRAGMA busy_timeout").fetchone(), (1001,))

    def test_self_check_reports_invalid_paths_and_database(self):
        self.run_cli("--check", database=self.root / "missing" / "database.sqlite3", expected_code=1)
        self.log.parent.write_bytes(b"not a directory")
        self.assertIn("Self-check failed", self.run_cli("--check", expected_code=1).stderr)
        self.log.parent.unlink()
        self.db.write_bytes(b"not a database")
        self.assertIn("Self-check failed", self.run_cli("--check", expected_code=1).stderr)
        self.assertIn("between 0 and 60000", self.run_cli("--busy-timeout-ms", 60001, expected_code=2).stderr)

    def test_console_commands_and_clean_exit(self):
        self.create_legacy_db()
        with self.running() as (port, process):
            self.assertEqual(self.request(port, "GET", "/health")[0], 200)
            stdout, stderr = process.communicate("status\nlist 1\nshow 1\nexit\n", timeout=10)
            self.assertEqual(process.returncode, 0, stderr)
            for text in ("Database status: OK", '"name": "legacy"', "Service stopped."):
                self.assertIn(text, stdout)

    def test_log_rotation(self):
        with patch.object(server, "LOG_MAX_BYTES", 100):
            with server.request_logging(self.log, terminal=False):
                for _ in range(10):
                    server.LOGGER.info("request completed")
        self.assertEqual(
            {path.name for path in self.log.parent.iterdir()},
            {"server.log", "server.log.1", "server.log.2", "server.log.3"},
        )


if __name__ == "__main__":
    unittest.main()
