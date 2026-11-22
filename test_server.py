import http.client
import json
import logging
import os
import socket
import sqlite3
import struct
import subprocess
import sys
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from pathlib import Path
from threading import Thread
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
        self.env.pop(server.API_TOKEN_ENV, None)

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

    def request(self, port, method, path, payload=None, *, headers=None, include_headers=False):
        with closing(http.client.HTTPConnection("127.0.0.1", port, timeout=5)) as connection:
            headers = dict(headers or {})
            if payload is not None:
                headers.setdefault("Content-Type", "application/json")
            body = None if payload is None else json.dumps(payload).encode("utf-8")
            connection.request(method, path, body, headers)
            response = connection.getresponse()
            data = response.read()
            result = response.status, json.loads(data) if data else None
            return (*result, response.headers) if include_headers else result

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

    def test_filters_follow_writes_and_ignore_non_string_fields(self):
        with self.running() as (port, _):
            first = {"name": "Straße", "status": "Active"}
            self.assertEqual(self.request(port, "POST", "/api/items", first), (201, {"id": 1, "data": first}))
            numeric = {"name": 42, "status": True}
            self.assertEqual(self.request(port, "POST", "/api/items", numeric), (201, {"id": 2, "data": numeric}))
            self.assertEqual(self.request(port, "GET", "/api/items?name=STRASSE"),
                             (200, {"items": [{"id": 1, "data": first}], "next_after": None}))
            self.assertEqual(self.request(port, "GET", "/api/items?name=STRASSE&status=Active"),
                             (200, {"items": [{"id": 1, "data": first}], "next_after": None}))
            self.assertEqual(self.request(port, "GET", "/api/items?status=Active"),
                             (200, {"items": [{"id": 1, "data": first}], "next_after": None}))
            for query in ("name=4", "status=true", "status=True", "name=STRASSE&status=active"):
                self.assertEqual(self.request(port, "GET", f"/api/items?{query}"),
                                 (200, {"items": [], "next_after": None}), query)
            replacement = {"name": "renamed", "status": "done"}
            self.assertEqual(self.request(port, "PUT", "/api/items/1", replacement),
                             (200, {"id": 1, "data": replacement}))
            self.assertEqual(self.request(port, "GET", "/api/items?name=STRASSE"),
                             (200, {"items": [], "next_after": None}))
            self.assertEqual(self.request(port, "GET", "/api/items?name=NAME&status=done"),
                             (200, {"items": [{"id": 1, "data": replacement}], "next_after": None}))
            self.assertEqual(self.request(port, "DELETE", "/api/items/1"), (204, None))
            self.assertEqual(self.request(port, "GET", "/api/items?name=NAME&status=done"),
                             (200, {"items": [], "next_after": None}))

    def test_legacy_upgrade_keeps_filters_working_after_corrupt_rows(self):
        with closing(sqlite3.connect(str(self.db))) as connection, connection:
            connection.execute("CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT NOT NULL)")
            connection.execute("INSERT INTO items (data) VALUES (?)", ("not json",))
            connection.execute("INSERT INTO items (data) VALUES (?)", ('{"name":"good","status":"active"}',))
        with self.running() as (port, _):
            self.assertEqual(
                self.request(port, "GET", "/api/items?name=GOOD&status=active"),
                (200, {"items": [{"id": 2, "data": {"name": "good", "status": "active"}}], "next_after": None}),
            )
        with closing(sqlite3.connect(str(self.db))) as connection:
            columns = {row[1] for row in connection.execute("PRAGMA table_info(items)")}
        self.assertTrue({"name_folded", "status"} <= columns)

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
        for text in (
            f"Python {sys.version.split()[0]} ({struct.calcsize('P') * 8}-bit)",
            f"SQLite {sqlite3.sqlite_version}", "OK TCP port 127.0.0.1:0 bind",
            "OK log directory write check", "OK database directory writable",
            "wal, lock wait 1001 ms", "OK create", "OK list", "OK list filter", "OK get", "OK replace",
            "OK online backup", "OK delete", "OK restore", "Self-check passed.",
        ):
            self.assertIn(text, result.stdout)
        self.assertEqual(result.stderr, "")
        self.assertFalse(self.db.exists())
        self.assertFalse(self.log.parent.exists())
        self.create_legacy_db()
        original = self.db.read_bytes()
        self.log.parent.mkdir()
        self.log.write_bytes(b"existing log\n")
        result = self.run_cli("--check", "--journal-mode", "wal")
        self.assertIn("current journal mode: delete", result.stdout)
        self.assertEqual(self.db.read_bytes(), original)
        self.assertEqual(self.log.read_bytes(), b"existing log\n")
        self.assertEqual(set(self.root.rglob("*")), {self.db, self.log.parent, self.log})
        with server.ItemStore(self.db, 1001).connect() as connection:
            self.assertEqual(connection.execute("PRAGMA busy_timeout").fetchone(), (1001,))

    def test_self_check_reports_invalid_paths_and_database(self):
        missing = self.root / "missing" / "database.sqlite3"
        self.assertIn("database directory", self.run_cli("--check", database=missing, expected_code=1).stderr)
        self.log.parent.write_bytes(b"not a directory")
        self.assertIn("log directory", self.run_cli("--check", expected_code=1).stderr)
        self.log.parent.unlink()
        self.db.write_bytes(b"not a database")
        self.assertIn("existing database integrity", self.run_cli("--check", expected_code=1).stderr)
        self.assertEqual(list(self.root.iterdir()), [self.db])
        self.assertIn("between 0 and 60000", self.run_cli("--busy-timeout-ms", 60001, expected_code=2).stderr)

    def test_self_check_fails_when_port_is_occupied(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.bind(("127.0.0.1", 0))
            listener.listen()
            port = listener.getsockname()[1]
            result = self.run_cli("--check", "--port", port, expected_code=1)
        self.assertIn(f"Self-check failed: TCP port 127.0.0.1:{port} bind:", result.stderr)
        self.assertFalse(list(self.root.iterdir()))

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

    def test_auth_and_cors_on_preflight_and_actual_requests(self):
        origin = "http://localhost:5173"
        self.env[server.API_TOKEN_ENV] = "test-token-for-regression"
        with self.running("--cors-origin", origin, "--rate-limit", 0) as (port, _):
            browser = {"Origin": origin}
            preflight = {
                **browser,
                "Access-Control-Request-Method": "POST",
                "Access-Control-Request-Headers": "authorization, content-type",
            }
            status, body, headers = self.request(
                port, "OPTIONS", "/api/items", headers=preflight, include_headers=True,
            )
            self.assertEqual((status, body), (204, None))
            self.assertEqual(headers["Access-Control-Allow-Origin"], origin)
            self.assertIn("POST", headers["Access-Control-Allow-Methods"])
            self.assertIn("Authorization", headers["Access-Control-Allow-Headers"])
            self.assertIn("Access-Control-Request-Method", headers["Vary"])
            self.assertEqual(self.request(port, "OPTIONS", "/api/items", headers={
                **preflight, "Access-Control-Request-Headers": "x-extra",
            }), (403, {"error": "cors_headers_not_allowed"}))
            self.assertEqual(self.request(port, "OPTIONS", "/api/items", headers={
                **preflight, "Access-Control-Request-Method": "DELETE",
            }), (405, {"error": "method_not_allowed"}))
            self.assertEqual(self.request(port, "GET", "/health", headers=browser), (200, {"status": "ok"}))
            for auth in ({}, {"Authorization": "Bearer wrong"}, {"Authorization": "Bearer \xe9"}):
                status, body, headers = self.request(
                    port, "GET", "/api/items", headers={**browser, **auth}, include_headers=True,
                )
                self.assertEqual((status, body), (401, {"error": "unauthorized"}))
                self.assertTrue(headers["WWW-Authenticate"].startswith("Bearer "))
                self.assertEqual(headers["Access-Control-Allow-Origin"], origin)
            credentials = {"Authorization": "bearer test-token-for-regression"}
            for rejected_origin in ("http://localhost:5174", "null"):
                status, body, headers = self.request(
                    port, "POST", "/api/items", {"name": "denied"},
                    headers={**credentials, "Origin": rejected_origin}, include_headers=True,
                )
                self.assertEqual((status, body), (403, {"error": "origin_not_allowed"}))
                self.assertIsNone(headers["Access-Control-Allow-Origin"])
            self.assertEqual(self.request(port, "GET", "/api/items", headers=credentials),
                             (200, {"items": [], "next_after": None}))
            status, body, headers = self.request(
                port, "POST", "/api/items", {"name": "allowed"},
                headers={**browser, **credentials}, include_headers=True,
            )
            self.assertEqual((status, body), (201, {"id": 1, "data": {"name": "allowed"}}))
            self.assertEqual(headers["Location"], "/api/items/1")
            self.assertIn("Location", headers["Access-Control-Expose-Headers"])
        self.assertNotIn(self.env[server.API_TOKEN_ENV], self.log.read_text(encoding="utf-8"))

    def test_default_cors_and_config_validation(self):
        with self.running() as (port, _):
            self.assertEqual(self.request(port, "GET", "/api/items", headers={"Origin": "http://localhost:5173"}),
                             (403, {"error": "origin_not_allowed"}))
            self.assertEqual(self.request(port, "GET", "/health", headers={"Origin": f"http://127.0.0.1:{port}"}),
                             (200, {"status": "ok"}))
        self.run_cli("--check", "--cors-origin", "*", expected_code=2)
        self.run_cli("--check", "--cors-origin", "https://example.test/path", expected_code=2)
        self.run_cli("--check", "--rate-limit", -1, expected_code=2)
        self.run_cli("--check", "--max-connections", 0, expected_code=2)
        self.run_cli("--check", "--max-connections", 257, expected_code=2)
        self.env[server.API_TOKEN_ENV] = ""
        self.run_cli("--check", expected_code=2)

    def test_rate_limit_is_shared_and_cors_exposes_retry_after(self):
        origin = "http://localhost:5173"
        with self.running("--cors-origin", origin, "--rate-limit", 3) as (port, _):
            def read(_number):
                return self.request(port, "GET", "/health", headers={
                    "Origin": origin, "X-Forwarded-For": f"192.0.2.{_number + 1}",
                }, include_headers=True)

            with ThreadPoolExecutor(max_workers=4) as pool:
                replies = list(pool.map(read, range(4)))
            self.assertEqual(sorted(status for status, _, _ in replies), [200, 200, 200, 429])
            _, body, headers = next(result for result in replies if result[0] == 429)
            self.assertEqual(body, {"error": "rate_limited"})
            self.assertTrue(1 <= int(headers["Retry-After"]) <= 60)
            self.assertEqual(headers["Access-Control-Allow-Origin"], origin)
            self.assertIn("Retry-After", headers["Access-Control-Expose-Headers"])

        limiter = server.RateLimiter(1)
        with patch.object(server.time, "monotonic", side_effect=[10, 10.1, 69.9, 70]):
            self.assertIsNone(limiter.retry_after())
            self.assertEqual(limiter.retry_after(), 60)
            self.assertEqual(limiter.retry_after(), 1)
            self.assertIsNone(limiter.retry_after())

    def test_connection_limit_applies_backpressure(self):
        store = server.ItemStore(self.db)
        with store.connect() as connection:
            store.initialize(connection=connection)
        httpd = server.ApiServer(("127.0.0.1", 0), store, rate_limit=0, max_connections=1)
        worker = Thread(target=httpd.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
        worker.start()
        try:
            port = httpd.server_port
            with socket.create_connection(("127.0.0.1", port), timeout=5) as first:
                first.sendall(
                    b"POST /api/items HTTP/1.0\r\nContent-Type: application/json\r\nContent-Length: 8\r\n\r\n"
                )
                time.sleep(0.3)
                with socket.create_connection(("127.0.0.1", port), timeout=5) as second:
                    second.sendall(b"GET /health HTTP/1.0\r\n\r\n")
                    second.settimeout(0.5)
                    with self.assertRaises(socket.timeout):
                        second.recv(1)
                    second.settimeout(5)
                    first.sendall(b'{"a": 1}')
                    with first.makefile("rb") as response:
                        self.assertTrue(response.read().startswith(b"HTTP/1.0 201 "))
                    with second.makefile("rb") as response:
                        self.assertTrue(response.read().startswith(b"HTTP/1.0 200 "))
        finally:
            httpd.shutdown()
            worker.join(timeout=5)
            httpd.server_close()
        self.assertFalse(worker.is_alive())

    def test_service_controller_start_stop_restart(self):
        controller = server.ServiceController(busy_timeout_ms=500, rate_limit=0, max_connections=4)
        port = controller.start(self.db, self.log, 0, "wal")
        self.assertTrue(controller.running)
        self.assertEqual(controller.port, port)
        try:
            self.assertEqual(self.request(port, "GET", "/health"), (200, {"status": "ok"}))
            record = {"name": "panel"}
            self.assertEqual(self.request(port, "POST", "/api/items", record),
                             (201, {"id": 1, "data": record}))
        finally:
            controller.stop()
        self.assertFalse(controller.running)
        self.assertIsNone(controller.port)
        self.assertEqual(server.LOGGER.handlers, [])
        with self.assertRaises(OSError):
            socket.create_connection(("127.0.0.1", port), timeout=1)
        port = controller.start(self.db, self.log, 0, None)
        try:
            self.assertEqual(self.request(port, "GET", "/api/items/1"), (200, {"id": 1, "data": record}))
        finally:
            controller.stop()

    def test_service_controller_cleans_up_after_start_failure(self):
        controller = server.ServiceController(busy_timeout_ms=500, rate_limit=0)
        missing = self.root / "missing" / "data.sqlite3"
        with self.assertRaises((OSError, sqlite3.Error)):
            controller.start(missing, self.log, 0, None)
        self.assertFalse(controller.running)
        self.assertIsNone(controller.port)
        self.assertEqual(server.LOGGER.handlers, [])
        port = controller.start(self.db, self.log, 0, None)
        try:
            self.assertEqual(self.request(port, "GET", "/health"), (200, {"status": "ok"}))
        finally:
            controller.stop()

    def test_log_level_tags(self):
        self.assertEqual(server.log_level_tag(logging.DEBUG), "info")
        self.assertEqual(server.log_level_tag(logging.INFO), "info")
        self.assertEqual(server.log_level_tag(logging.WARNING), "warning")
        self.assertEqual(server.log_level_tag(logging.ERROR), "error")
        self.assertEqual(server.log_level_tag(logging.CRITICAL), "error")

    def test_panel_log_filters(self):
        self.assertTrue(server.log_matches("全部", "error", "anything"))
        self.assertTrue(server.log_matches("信息", "muted", "ready"))
        self.assertFalse(server.log_matches("信息", "error", "boom"))
        self.assertTrue(server.log_matches("成功", "success", "started"))
        self.assertFalse(server.log_matches("成功", "info", "started"))
        self.assertTrue(server.log_matches("警告", "warning", "careful"))
        self.assertTrue(server.log_matches("错误", "error", "boom"))
        self.assertTrue(server.log_matches("全部", "info", "GET /api/items", "API"))
        self.assertFalse(server.log_matches("全部", "info", "GET /health", "api"))
        self.assertTrue(server.log_matches("全部", "success", "服务已启动", "启动"))

    def test_unexpected_error_and_http_parser_errors_are_json(self):
        origin = "http://localhost:5173"
        with self.running("--cors-origin", origin) as (port, _):
            self.assertEqual(self.request(port, "POST", "/api/items", {}), (201, {"id": 1, "data": {}}))
            with closing(sqlite3.connect(str(self.db))) as connection, connection:
                connection.execute("UPDATE items SET data=? WHERE id=1", ("invalid stored value",))
            status, body, headers = self.request(
                port, "GET", "/api/items/1", headers={"Origin": origin}, include_headers=True,
            )
            self.assertEqual((status, body), (500, {"error": "internal_server_error"}))
            self.assertEqual(headers["Content-Type"], "application/json; charset=utf-8")
            self.assertEqual(headers["Access-Control-Allow-Origin"], origin)
            status, body, headers = self.request(
                port, "HEAD", "/api/items/1", headers={"Origin": origin}, include_headers=True,
            )
            self.assertEqual((status, body), (500, None))
            self.assertGreater(int(headers["Content-Length"]), 0)
            self.assertEqual(self.request(port, "TRACE", "/health"), (501, {"error": "not_implemented"}))
            with socket.create_connection(("127.0.0.1", port), timeout=5) as connection:
                connection.sendall(b"GET /health invalid-version\r\n\r\n")
                with connection.makefile("rb") as response:
                    response_data = response.read()
                self.assertTrue(response_data.startswith(b"HTTP/1.0 400 "), response_data)
                self.assertTrue(response_data.endswith(b'{"error":"bad_request"}'), response_data)
            self.assertEqual(self.request(port, "GET", "/health"), (200, {"status": "ok"}))
        self.assertIn("JSONDecodeError", self.log.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
