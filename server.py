import argparse
import cmd
import json
import logging
import math
import os
import platform
import re
import secrets
import socket
import sqlite3
import struct
import tempfile
import time
from collections import deque
from contextlib import ExitStack, closing, contextmanager, nullcontext
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from logging.handlers import RotatingFileHandler
from pathlib import Path
from threading import BoundedSemaphore, Event, Lock, Thread
from urllib.parse import parse_qs, urlsplit


MAX_BODY_BYTES = 64 * 1024
MAX_ITEM_ID = (1 << 63) - 1
MAX_ITEM_DEPTH = 64
ITEM_ROUTE = re.compile(r"/api/items/([1-9][0-9]{0,18})")
LISTEN_HOST = "127.0.0.1"
DEFAULT_PORT = 8000
DEFAULT_DB = Path(__file__).resolve().with_name("items.sqlite3")
DEFAULT_LOG = DEFAULT_DB.parent / "logs" / "server.log"
DEFAULT_BUSY_TIMEOUT_MS = 2000
DEFAULT_RATE_LIMIT = 120
DEFAULT_MAX_CONNECTIONS = 32
MAX_MAX_CONNECTIONS = 256
RATE_WINDOW_SECONDS = 60
API_TOKEN_ENV = "PYTHON_BACKEND_TOKEN"
BEARER_TOKEN = re.compile(r"[A-Za-z0-9._~+/-]+=*")
ORIGIN_FORMAT = re.compile(r"https?://(?:[a-z0-9.-]+|\[[a-f0-9:.]+\])(?::[0-9]{1,5})?")
CORS_HEADERS = {"authorization", "content-type"}
LOG_BACKUPS = 3
LOG_MAX_BYTES = 1024 * 1024
LOGGER = logging.getLogger("python_backend")
LIST_SORTS = {"id", "-id", "updated_at", "-updated_at"}
SQLITE_BUSY_RESULTS = {5, 6}


class ApiError(Exception):
    def __init__(self, status, code, headers=()):
        super().__init__(code)
        self.status = status
        self.code = code
        self.headers = headers


class RateLimiter:
    def __init__(self, limit):
        self.limit = limit
        self.arrivals = deque()
        self.lock = Lock()

    def retry_after(self):
        if self.limit == 0:
            return None
        with self.lock:
            now = time.monotonic()
            while self.arrivals and self.arrivals[0] <= now - RATE_WINDOW_SECONDS:
                self.arrivals.popleft()
            if len(self.arrivals) >= self.limit:
                return max(1, math.ceil(self.arrivals[0] + RATE_WINDOW_SECONDS - now))
            self.arrivals.append(now)
            return None


def encode_json(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode(
        "utf-8", errors="backslashreplace"
    )


def finite_float(value):
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("Non-finite JSON number")
    return number


def page_number(value, minimum, maximum):
    if len(value) > len(str(maximum)) or not value.isascii() or not value.isdecimal():
        raise ValueError
    number = int(value)
    if not minimum <= number <= maximum:
        raise ValueError
    return number


def check_item_depth(value, depth=0):
    if depth > MAX_ITEM_DEPTH:
        raise ApiError(400, "item_too_deep")
    if isinstance(value, dict):
        for child in value.values():
            check_item_depth(child, depth + 1)
    elif isinstance(value, list):
        for child in value:
            check_item_depth(child, depth + 1)


def filter_columns(item):
    name = item.get("name") if isinstance(item, dict) else None
    status = item.get("status") if isinstance(item, dict) else None
    return (name.casefold() if isinstance(name, str) else None, status if isinstance(status, str) else None)


class ItemStore:
    def __init__(self, path, busy_timeout_ms=DEFAULT_BUSY_TIMEOUT_MS):
        self.path = path
        self.busy_timeout_ms = busy_timeout_ms

    @contextmanager
    def connect(self):
        with closing(sqlite3.connect(str(self.path))) as connection, connection:
            connection.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms:d}")
            yield connection

    def initialize(self, journal_mode=None, *, connection=None):
        with (self.connect() if connection is None else nullcontext(connection)) as connection:
            current_mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
            if journal_mode is not None and journal_mode != current_mode:
                pragma = {"wal": "PRAGMA journal_mode=WAL", "delete": "PRAGMA journal_mode=DELETE"}[journal_mode]
                current_mode = connection.execute(pragma).fetchone()[0]
                if current_mode != journal_mode:
                    raise sqlite3.OperationalError(f"Could not set journal mode to {journal_mode}; got {current_mode}")
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "CREATE TABLE IF NOT EXISTS items ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT NOT NULL, updated_at INTEGER NOT NULL DEFAULT 0,"
                " name_folded TEXT, status TEXT)"
            )
            columns = {column[1] for column in connection.execute("PRAGMA table_info(items)")}
            if "updated_at" not in columns:
                connection.execute("ALTER TABLE items ADD COLUMN updated_at INTEGER NOT NULL DEFAULT 0")
            missing = [name for name in ("name_folded", "status") if name not in columns]
            for name in missing:
                connection.execute(f"ALTER TABLE items ADD COLUMN {name} TEXT")
            connection.execute("SELECT id, data, updated_at, name_folded, status FROM items LIMIT 0")
            if missing:
                self.backfill_filters(connection)
            connection.execute("CREATE INDEX IF NOT EXISTS items_updated_idx ON items (updated_at, id)")
            connection.execute("CREATE INDEX IF NOT EXISTS items_status_idx ON items (status, id)")
            connection.commit()
            return current_mode

    @staticmethod
    def backfill_filters(connection):
        last_id = 0
        while True:
            rows = connection.execute(
                "SELECT id, data FROM items WHERE id > ? ORDER BY id LIMIT 500", (last_id,)
            ).fetchall()
            if not rows:
                return
            updates = []
            for item_id, data in rows:
                try:
                    item = json.loads(data)
                except (TypeError, ValueError, RecursionError):
                    item = None
                updates.append((*filter_columns(item), item_id))
            connection.executemany("UPDATE items SET name_folded = ?, status = ? WHERE id = ?", updates)
            last_id = rows[-1][0]

    def check(self):
        with self.connect() as connection:
            connection.execute("SELECT id, data, updated_at FROM items LIMIT 1").fetchone()

    def list(self, after, limit, name=None, status=None, sort="id"):
        by_time = sort.endswith("updated_at")
        descending = sort.startswith("-")
        direction = "DESC" if descending else "ASC"
        compare = "<" if descending else ">"
        order_by = f"updated_at {direction}, id {direction}" if by_time else f"id {direction}"
        predicates = []
        parameters = []
        if after is not None:
            if by_time:
                predicates.append(f"(updated_at, id) {compare} (?, ?)")
                parameters.extend(after)
            else:
                predicates.append(f"id {compare} ?")
                parameters.append(after)
        if name is not None:
            predicates.append("instr(name_folded, ?) > 0")
            parameters.append(name.casefold())
        if status is not None:
            predicates.append("status = ?")
            parameters.append(status)
        where = " WHERE " + " AND ".join(predicates) if predicates else ""
        with self.connect() as connection:
            rows = connection.execute(
                f"SELECT id, data, updated_at FROM items{where} ORDER BY {order_by} LIMIT ?",
                (*parameters, limit + 1),
            ).fetchall()
        items = []
        for item_id, data, updated_at in rows[:limit]:
            item = {"id": item_id, "data": json.loads(data)}
            if by_time:
                item["updated_at"] = updated_at or None
            items.append(item)
        next_after = None
        if len(rows) > limit:
            last_id, _, last_updated = rows[limit - 1]
            next_after = f"{last_updated}:{last_id}" if by_time else last_id
        return {"items": items, "next_after": next_after}

    def get(self, item_id):
        with self.connect() as connection:
            row = connection.execute("SELECT data FROM items WHERE id = ?", (item_id,)).fetchone()
        if row is None:
            raise ApiError(404, "not_found")
        return {"id": item_id, "data": json.loads(row[0])}

    def create(self, item):
        with self.connect() as connection:
            cursor = connection.execute(
                "INSERT INTO items (data, updated_at, name_folded, status) VALUES (?, ?, ?, ?)",
                (
                    encode_json(item).decode("utf-8"),
                    time.time_ns() // 1_000_000,
                    *filter_columns(item),
                ),
            )
            return cursor.lastrowid

    def replace(self, item_id, item):
        with self.connect() as connection:
            cursor = connection.execute(
                "UPDATE items SET data = ?, updated_at = ?, name_folded = ?, status = ? WHERE id = ?",
                (
                    encode_json(item).decode("utf-8"),
                    time.time_ns() // 1_000_000,
                    *filter_columns(item),
                    item_id,
                ),
            )
            if cursor.rowcount == 0:
                raise ApiError(404, "not_found")

    def delete(self, item_id):
        with self.connect() as connection:
            cursor = connection.execute("DELETE FROM items WHERE id = ?", (item_id,))
            if cursor.rowcount == 0:
                raise ApiError(404, "not_found")


def validate_snapshot(connection):
    if connection.execute("PRAGMA integrity_check(1)").fetchone() != ("ok",):
        raise ValueError("SQLite integrity check failed")
    columns = {column[1]: column for column in connection.execute("PRAGMA table_info(items)")}
    item_id = columns.get("id")
    data = columns.get("data")
    if item_id is None or item_id[2].upper() != "INTEGER" or item_id[5] != 1 or data is None:
        raise ValueError("The snapshot does not contain a compatible items table")
    connection.execute("SELECT id, data FROM items LIMIT 0")


def copy_snapshot(source, destination):
    for suffix in ("", "-journal", "-wal", "-shm"):
        existing = Path(str(destination) + suffix)
        if existing.exists():
            raise FileExistsError(f"Destination or SQLite sidecar already exists: {existing}")
    source_files = {Path(str(source) + suffix).resolve() for suffix in ("", "-journal", "-wal", "-shm")}
    if destination.resolve() in source_files:
        raise ValueError("Destination conflicts with the source database files")

    busy_since = None

    def check_progress(result, _remaining, _total):
        nonlocal busy_since
        if result not in SQLITE_BUSY_RESULTS:
            busy_since = None
            return
        now = time.monotonic()
        if busy_since is None:
            busy_since = now
        elif now - busy_since >= 5:
            raise ValueError("Timed out waiting for the SQLite snapshot")

    with tempfile.NamedTemporaryFile(prefix=".snapshot-", suffix=".sqlite3", dir=destination.parent, delete=False) as file:
        temporary = Path(file.name)
    try:
        source_uri = source.resolve().as_uri() + "?mode=ro"
        with closing(sqlite3.connect(source_uri, uri=True, timeout=2)) as origin:
            with closing(sqlite3.connect(str(temporary))) as snapshot:
                origin.backup(snapshot, pages=128, progress=check_progress, sleep=0.05)
                validate_snapshot(snapshot)
        if os.name == "nt":
            temporary.rename(destination)
        else:
            os.link(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def log_files_for(path):
    return {path, *(Path(f"{path}.{index}").resolve() for index in range(1, LOG_BACKUPS + 1))}


def database_files_for(path):
    return {Path(str(path) + suffix).resolve() for suffix in ("", "-journal", "-wal", "-shm")}


@contextmanager
def check_step(label):
    try:
        yield
    except Exception as error:
        reason = str(error) or type(error).__name__
        raise RuntimeError(f"{label}: {reason}") from error
    print(f"OK {label}", flush=True)


def check_log_location(log_path):
    for candidate in log_files_for(log_path):
        if candidate.exists():
            with candidate.open("ab"):
                pass
    directory = log_path.parent
    while not directory.exists():
        directory = directory.parent
    with tempfile.TemporaryFile(dir=directory) as file:
        file.write(b"log check")
        file.flush()


def check_database_smoke(store, journal_mode):
    backup = store.path.with_name("backup.sqlite3")
    restored = ItemStore(store.path.with_name("restored.sqlite3"), store.busy_timeout_ms)
    original = {"name": "self-check", "status": "new"}
    replacement = {"name": "self-check updated"}

    with ExitStack() as resources:
        with check_step(f"temporary SQLite write: {journal_mode}, lock wait {store.busy_timeout_ms} ms"):
            connection = resources.enter_context(store.connect())
            store.initialize(journal_mode, connection=connection)
        with check_step("create"):
            item_id = store.create(original)
            if not isinstance(item_id, int) or item_id <= 0:
                raise ValueError(f"expected a positive item ID, got {item_id!r}")
        expected = {"id": item_id, "data": original}
        with check_step("list"):
            if store.list(None, 20) != {"items": [expected], "next_after": None}:
                raise ValueError("list did not return the created item")
        with check_step("list filter"):
            filters = {"name": "SELF-CHECK", "status": "new"}
            if store.list(None, 20, **filters) != {"items": [expected], "next_after": None}:
                raise ValueError("filter did not return the created item")
        with check_step("get"):
            if store.get(item_id) != expected:
                raise ValueError("get did not return the created item")
        expected = {"id": item_id, "data": replacement}
        with check_step("replace"):
            store.replace(item_id, replacement)
            if store.get(item_id) != expected:
                raise ValueError("replacement was not persisted")
            if store.list(None, 20, name="SELF-CHECK", status="new") != {"items": [], "next_after": None}:
                raise ValueError("replacement did not update the filter columns")
        with check_step("online backup"):
            copy_snapshot(store.path, backup)
            if ItemStore(backup, store.busy_timeout_ms).get(item_id) != expected:
                raise ValueError("backup did not contain the replaced item")
        with check_step("delete"):
            store.delete(item_id)
            if store.list(None, 20) != {"items": [], "next_after": None}:
                raise ValueError("the temporary database was not empty after delete")
        with check_step("restore"):
            copy_snapshot(backup, restored.path)
            if restored.get(item_id) != expected:
                raise ValueError("restored data did not match the backup")


def check_configuration(store, log_path, journal_mode, port):
    print(f"Python {platform.python_version()} ({struct.calcsize('P') * 8}-bit)", flush=True)
    print(f"SQLite {sqlite3.sqlite_version}", flush=True)

    with check_step(f"TCP port {LISTEN_HOST}:{port} bind"):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
                probe.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            probe.bind((LISTEN_HOST, port))

    with check_step(f"log directory write check: {log_path.parent}"):
        check_log_location(log_path)

    database = store.path
    current_mode = "delete"
    with ExitStack() as resources:
        with check_step(f"database directory writable: {database.parent}"):
            directory = resources.enter_context(tempfile.TemporaryDirectory(prefix=".db-check-", dir=database.parent))
        if database.exists():
            with check_step(f"existing database integrity and schema: {database}"):
                with database.open("r+b"):
                    pass
                uri = database.resolve().as_uri() + "?mode=ro"
                with closing(sqlite3.connect(uri, uri=True)) as connection:
                    connection.execute(f"PRAGMA busy_timeout={store.busy_timeout_ms:d}")
                    validate_snapshot(connection)
                    current_mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
            print(f"Existing database current journal mode: {current_mode}", flush=True)
        probe_store = ItemStore(Path(directory) / "probe.sqlite3", store.busy_timeout_ms)
        check_database_smoke(probe_store, journal_mode or current_mode)
    print("Self-check passed.", flush=True)


@contextmanager
def request_logging(path, terminal=True):
    path.parent.mkdir(parents=True, exist_ok=True)
    with closing(RotatingFileHandler(path, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUPS, encoding="utf-8")) as log_file:
        with closing(logging.StreamHandler()) as console:
            formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
            LOGGER.setLevel(logging.INFO)
            LOGGER.propagate = False
            handlers = (log_file, console) if terminal else (log_file,)
            for handler in handlers:
                handler.setFormatter(formatter)
                LOGGER.addHandler(handler)
            try:
                yield
            finally:
                for handler in handlers:
                    LOGGER.removeHandler(handler)


class ApiHandler(BaseHTTPRequestHandler):
    timeout = 5

    def handle_one_request(self):
        started = time.perf_counter()
        self.response_code = None
        self.cors_origin = None
        try:
            super().handle_one_request()
        except ApiError as error:
            self.send_api_error(error.status, error.code, error.headers)
        except sqlite3.Error as error:
            self.log_error("SQLite operation failed: %s", error)
            self.send_api_error(503, "database_unavailable")
        except ConnectionError:
            self.close_connection = True
        except Exception:
            self.close_connection = True
            LOGGER.exception("%s Request handler failed", self.client_address[0])
            if self.response_code is None:
                self.send_api_error(500, "internal_server_error")
        finally:
            if self.response_code is not None:
                elapsed = (time.perf_counter() - started) * 1000
                path = getattr(self, "path", "-").partition("?")[0]
                LOGGER.info(
                    "%s %s %s %s %.1fms",
                    self.client_address[0],
                    ascii(self.command or "-")[1:-1],
                    ascii(path)[1:-1],
                    self.response_code,
                    elapsed,
                )

    def parse_request(self):
        if not super().parse_request():
            return False
        self.validate_host()
        origins = self.headers.get_all("Origin", [])
        if len(origins) == 1:
            origin = origins[0].strip()
            host = self.headers.get("Host", "").strip().lower()
            if origin in self.server.cors_origins or origin == f"http://{host}":
                self.cors_origin = origin
        retry_after = self.server.rate_limiter.retry_after()
        if retry_after is not None:
            raise ApiError(429, "rate_limited", (("Retry-After", str(retry_after)),))
        if origins and self.cors_origin is None:
            raise ApiError(403, "origin_not_allowed")
        self.is_preflight = self.command == "OPTIONS" and "Access-Control-Request-Method" in self.headers
        if not self.is_preflight and self.path.partition("?")[0] != "/health":
            self.authenticate()
        return True

    def authenticate(self):
        token = self.server.api_token
        if token is None:
            return
        headers = self.headers.get_all("Authorization", [])
        if len(headers) == 1:
            scheme, _, value = headers[0].strip().partition(" ")
            value = value.lstrip(" ")
            if scheme.lower() == "bearer" and value.isascii() and secrets.compare_digest(value, token):
                return
        raise ApiError(401, "unauthorized", (("WWW-Authenticate", 'Bearer realm="python-backend"'),))

    def dispatch(self):
        path, _, query = self.path.partition("?")
        if path == "/health":
            self.run_method({"GET": self.health})
        elif path == "/api/echo":
            self.run_method({"POST": self.echo})
        elif path == "/api/items":
            self.run_method({"GET": self.list_items, "POST": self.create_item}, query)
        else:
            match = ITEM_ROUTE.fullmatch(path)
            if match is None or int(match[1]) > MAX_ITEM_ID:
                raise ApiError(404, "not_found")
            self.run_method(
                {"GET": self.get_item, "PUT": self.replace_item, "DELETE": self.delete_item},
                int(match[1]),
            )

    do_GET = do_HEAD = do_POST = do_PUT = do_DELETE = do_PATCH = do_OPTIONS = dispatch

    def validate_host(self):
        hosts = self.headers.get_all("Host", [])
        if not hosts and self.request_version == "HTTP/1.0":
            return
        port = self.server.server_port
        local_hosts = {"localhost", "127.0.0.1", f"localhost:{port}", f"127.0.0.1:{port}"}
        if len(hosts) != 1 or hosts[0].strip().lower() not in local_hosts:
            raise ApiError(400, "invalid_host")

    def run_method(self, methods, *args):
        allowed = [*methods, "HEAD"] if "GET" in methods else list(methods)
        if self.is_preflight:
            requested = self.headers.get_all("Access-Control-Request-Method", [])
            if self.cors_origin is None or len(requested) != 1 or not requested[0].strip():
                raise ApiError(400, "invalid_preflight")
            method = requested[0].strip()
        else:
            method = self.command
        if method not in allowed:
            raise ApiError(405, "method_not_allowed", (("Allow", ", ".join(allowed)),))
        if not self.is_preflight:
            methods["GET" if method == "HEAD" else method](*args)
            return

        requested_headers = self.headers.get_all("Access-Control-Request-Headers", [])
        for header_list in requested_headers:
            if any(name.strip().lower() not in CORS_HEADERS for name in header_list.split(",")):
                raise ApiError(403, "cors_headers_not_allowed")
        self.send_json(204, headers=(
            ("Access-Control-Allow-Methods", ", ".join(allowed)),
            ("Access-Control-Allow-Headers", "Authorization, Content-Type"),
        ))

    def health(self):
        self.server.store.check()
        self.send_json(200, {"status": "ok"})

    def echo(self):
        self.send_json(200, self.read_json())

    def list_items(self, query):
        try:
            if ";" in query:
                raise ValueError
            params = {}
            if query:
                params = parse_qs(
                    query, keep_blank_values=True, strict_parsing=True, max_num_fields=5, errors="strict"
                )
            if params.keys() - {"after", "limit", "name", "status", "sort"}:
                raise ValueError
            if any(len(values) != 1 for values in params.values()):
                raise ValueError
        except ValueError:
            raise ApiError(400, "invalid_pagination") from None

        sort = params.get("sort", ["id"])[0]
        if sort not in LIST_SORTS:
            raise ApiError(400, "invalid_sort")
        name = params.get("name", [None])[0]
        status = params.get("status", [None])[0]
        if any(value is not None and not 1 <= len(value) <= 256 for value in (name, status)):
            raise ApiError(400, "invalid_filter")

        try:
            limit = page_number(params.get("limit", ["20"])[0], 1, 50)
            after = None
            if "after" in params:
                value = params["after"][0]
                if sort.endswith("updated_at"):
                    timestamp, item_id = value.split(":")
                    after = (page_number(timestamp, 0, MAX_ITEM_ID), page_number(item_id, 1, MAX_ITEM_ID))
                else:
                    after = page_number(value, 0, MAX_ITEM_ID)
        except ValueError:
            raise ApiError(400, "invalid_pagination") from None
        self.send_json(200, self.server.store.list(after, limit, name=name, status=status, sort=sort))

    def create_item(self, _query):
        data = self.read_item()
        item_id = self.server.store.create(data)
        self.send_json(201, {"id": item_id, "data": data}, (("Location", f"/api/items/{item_id}"),))

    def get_item(self, item_id):
        self.send_json(200, self.server.store.get(item_id))

    def replace_item(self, item_id):
        data = self.read_item()
        self.server.store.replace(item_id, data)
        self.send_json(200, {"id": item_id, "data": data})

    def delete_item(self, item_id):
        self.server.store.delete(item_id)
        self.send_json(204)

    def read_item(self):
        data = self.read_json()
        if not isinstance(data, dict):
            raise ApiError(400, "object_required")
        check_item_depth(data)
        return data

    def read_json(self):
        if "Transfer-Encoding" in self.headers:
            raise ApiError(400, "unsupported_transfer_encoding")
        lengths = self.headers.get_all("Content-Length", [])
        if not lengths:
            raise ApiError(411, "content_length_required")
        length_text = lengths[0].strip()
        if len(lengths) != 1 or not length_text.isascii() or not length_text.isdecimal():
            raise ApiError(400, "invalid_content_length")
        digits = length_text.lstrip("0") or "0"
        if len(digits) > len(str(MAX_BODY_BYTES)) or int(digits) > MAX_BODY_BYTES:
            raise ApiError(413, "body_too_large")
        if self.headers.get_content_type() != "application/json":
            raise ApiError(415, "application_json_required")

        length = int(digits)
        try:
            body = self.rfile.read(length)
        except socket.timeout:
            raise ApiError(408, "body_read_timeout") from None
        if len(body) != length:
            raise ApiError(400, "incomplete_body")
        try:
            return json.loads(body.decode("utf-8"), parse_float=finite_float, parse_constant=finite_float)
        except (ValueError, RecursionError):
            raise ApiError(400, "invalid_json") from None

    def send_error(self, code, message=None, explain=None):
        try:
            error = HTTPStatus(code).name.lower()
        except ValueError:
            error = "http_error"
        self.send_api_error(code, error)

    def send_api_error(self, status, code, headers=()):
        if self.response_code is None:
            if self.request_version == "HTTP/0.9":
                self.request_version = "HTTP/1.0"
            self.send_json(status, {"error": code}, headers)

    def send_json(self, status, data=None, headers=()):
        try:
            body = b"" if status == 204 else encode_json(data)
        except RecursionError:
            raise ApiError(400, "invalid_json") from None
        self.close_connection = True
        self.send_response(status)
        if status != 204:
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
        for name, value in headers:
            self.send_header(name, value)
        vary = "Origin"
        if self.command == "OPTIONS":
            vary += ", Access-Control-Request-Method, Access-Control-Request-Headers"
        self.send_header("Vary", vary)
        if self.cors_origin is not None:
            self.send_header("Access-Control-Allow-Origin", self.cors_origin)
            self.send_header("Access-Control-Expose-Headers", "Location, Retry-After, WWW-Authenticate")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        try:
            self.end_headers()
            if body and self.command != "HEAD":
                self.wfile.write(body)
        except (ConnectionError, socket.timeout):
            pass

    def log_request(self, code="-", size="-"):
        self.response_code = code

    def log_error(self, format, *args):
        LOGGER.error("%s %s", self.client_address[0], ascii(format % args)[1:-1])

    def log_message(self, format, *args):
        LOGGER.info("%s %s", self.client_address[0], ascii(format % args)[1:-1])


class ApiServer(ThreadingHTTPServer):
    allow_reuse_address = False
    request_queue_size = 64

    def __init__(self, address, store, *, cors_origins=(), api_token=None, rate_limit=DEFAULT_RATE_LIMIT,
                 max_connections=DEFAULT_MAX_CONNECTIONS):
        self.store = store
        self.cors_origins = frozenset(cors_origins)
        self.api_token = api_token
        self.rate_limiter = RateLimiter(rate_limit)
        self.connection_slots = BoundedSemaphore(max_connections)
        self.stopping = Event()
        super().__init__(address, ApiHandler)

    def process_request(self, request, client_address):
        while not self.connection_slots.acquire(timeout=1):
            if self.stopping.is_set():
                self.shutdown_request(request)
                return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self.connection_slots.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self.connection_slots.release()

    def shutdown(self):
        self.stopping.set()
        super().shutdown()

    def handle_error(self, request, client_address):
        LOGGER.exception("%s Request handler failed", client_address[0])


class ServiceController:
    def __init__(self, *, busy_timeout_ms=DEFAULT_BUSY_TIMEOUT_MS, cors_origins=(), api_token=None,
                 rate_limit=DEFAULT_RATE_LIMIT, max_connections=DEFAULT_MAX_CONNECTIONS):
        self.busy_timeout_ms = busy_timeout_ms
        self.cors_origins = tuple(cors_origins)
        self.api_token = api_token
        self.rate_limit = rate_limit
        self.max_connections = max_connections
        self.server = None
        self.worker = None
        self.logging = None

    @property
    def running(self):
        return self.server is not None

    @property
    def port(self):
        return None if self.server is None else self.server.server_port

    def start(self, database, log_path, port, journal_mode=None):
        if self.running:
            raise RuntimeError("The service is already running")
        store = ItemStore(database, self.busy_timeout_ms)
        server = ApiServer((LISTEN_HOST, port), store, cors_origins=self.cors_origins,
                           api_token=self.api_token, rate_limit=self.rate_limit,
                           max_connections=self.max_connections)
        logging_context = request_logging(log_path, terminal=False)
        try:
            logging_context.__enter__()
            try:
                mode = store.initialize(journal_mode)
            except BaseException:
                logging_context.__exit__(None, None, None)
                raise
        except BaseException:
            server.server_close()
            raise
        LOGGER.info("SQLite journal mode: %s; lock wait: %d ms", mode, self.busy_timeout_ms)
        worker = Thread(target=server.serve_forever, name="http-server", daemon=True)
        try:
            worker.start()
        except BaseException:
            logging_context.__exit__(None, None, None)
            server.server_close()
            raise
        self.server = server
        self.worker = worker
        self.logging = logging_context
        return server.server_port

    def stop(self):
        server, worker, logging_context = self.server, self.worker, self.logging
        if server is None:
            return
        self.server = self.worker = self.logging = None
        try:
            server.shutdown()
            worker.join(timeout=5)
        finally:
            server.server_close()
            logging_context.__exit__(None, None, None)


class BackendConsole(cmd.Cmd):
    intro = "Backend console. Type help to see available commands."
    prompt = "backend> "

    def __init__(self, server, log_path):
        super().__init__()
        self.server = server
        self.store = server.store
        self.log_path = log_path

    def write(self, text):
        encoding = self.stdout.encoding or "utf-8"
        printable = text.encode(encoding, errors="backslashreplace").decode(encoding)
        print(printable, file=self.stdout, flush=True)

    def onecmd(self, line):
        try:
            return super().onecmd(line)
        except ApiError as error:
            self.write("Item not found." if error.status == 404 else f"Error: {error.code}")
        except (OSError, sqlite3.Error, ValueError) as error:
            self.write(f"Error: {error}")

    def emptyline(self):
        pass

    def do_status(self, argument):
        """status - Show the service address, database status and log path."""
        if argument:
            self.write("Usage: status")
            return
        self.write(f"Listening on http://127.0.0.1:{self.server.server_port}")
        self.write(f"Database: {self.store.path}")
        self.write(f"Log file: {self.log_path}")
        self.store.check()
        self.write("Database status: OK")

    def do_list(self, argument):
        """list [LIMIT [AFTER]] - Browse items in ID order (10 by default, maximum 50)."""
        parts = argument.split()
        try:
            if len(parts) > 2:
                raise ValueError
            limit = page_number(parts[0], 1, 50) if parts else 10
            after = page_number(parts[1], 0, MAX_ITEM_ID) if len(parts) == 2 else None
        except ValueError:
            self.write("Usage: list [LIMIT (1-50) [AFTER (item ID)]]")
            return
        page = self.store.list(after, limit)
        if not page["items"]:
            self.write("No items.")
        for item in page["items"]:
            summary = {"id": item["id"]}
            for field in ("name", "status"):
                value = item["data"].get(field)
                if isinstance(value, str):
                    summary[field] = value[:80] + ("..." if len(value) > 80 else "")
            self.write(json.dumps(summary, ensure_ascii=False))
        if page["next_after"] is not None:
            self.write(f"Next page: list {limit} {page['next_after']}")

    def do_show(self, argument):
        """show ID - Display an item's complete JSON data."""
        try:
            item_id = page_number(argument, 1, MAX_ITEM_ID)
        except ValueError:
            self.write("Usage: show ID")
            return
        self.write(json.dumps(self.store.get(item_id), ensure_ascii=False, indent=2))

    def do_backup(self, argument):
        """backup FILE - Save an online SQLite snapshot to a new file."""
        filename = argument[1:-1] if argument.startswith('"') and argument.endswith('"') else argument
        if not filename:
            self.write('Usage: backup "FILE"')
            return
        destination = Path(filename).expanduser().resolve()
        if destination in log_files_for(self.log_path):
            raise ValueError("Destination conflicts with the service log files")
        copy_snapshot(self.store.path, destination)
        self.write(f"Backup written to {destination}")

    def do_exit(self, argument):
        """exit - Stop the service and return to the terminal."""
        if argument:
            self.write("Usage: exit")
            return False
        return True

    def do_EOF(self, _argument):
        """EOF - Stop the service when the input stream closes."""
        return True


def serve_interactive(server, log_path):
    console = BackendConsole(server, log_path)
    worker = Thread(target=server.serve_forever, name="http-server")
    worker.start()
    try:
        console.cmdloop()
    except KeyboardInterrupt:
        console.write("")
    finally:
        server.shutdown()
        worker.join()
    console.write("Service stopped.")


class QueueLogHandler(logging.Handler):
    def __init__(self, queue):
        super().__init__()
        self.queue = queue

    def emit(self, record):
        try:
            self.queue.put((record.levelno, self.format(record)))
        except Exception:
            self.handleError(record)


def log_level_tag(levelno):
    if levelno >= logging.ERROR:
        return "error"
    if levelno >= logging.WARNING:
        return "warning"
    return "info"


LOG_FILTERS = {
    "全部": None,
    "信息": {"info", "muted"},
    "成功": {"success"},
    "警告": {"warning"},
    "错误": {"error"},
}


def log_matches(level, tag, line, keyword=""):
    allowed = LOG_FILTERS[level]
    if allowed is not None and tag not in allowed:
        return False
    keyword = keyword.strip().casefold()
    return not keyword or keyword in line.casefold()


def serve_panel(args, api_token, database, log_path):
    try:
        import tkinter as tk
        from queue import Empty, Queue
        from tkinter import filedialog, font as tkfont, messagebox, scrolledtext, ttk
        import webbrowser
    except ImportError as error:
        raise RuntimeError("Tkinter is not available in this Python installation") from error

    journal_modes = {"沿用数据库设置": None, "WAL": "wal", "DELETE": "delete"}

    class PanelApp:
        def __init__(self, root):
            self.root = root
            self.controller = ServiceController(
                busy_timeout_ms=args.busy_timeout_ms,
                cors_origins=args.cors_origin,
                api_token=api_token,
                rate_limit=args.rate_limit,
                max_connections=args.max_connections,
            )
            self.log_queue = Queue()
            self.log_entries = deque(maxlen=2000)
            self.gui_handler = None
            self.busy = False
            self.closing = False
            self.port_var = tk.StringVar(value=str(args.port))
            self.database_var = tk.StringVar(value=str(database))
            self.log_var = tk.StringVar(value=str(log_path))
            selected = next((label for label, mode in journal_modes.items() if mode == args.journal_mode),
                            "沿用数据库设置")
            self.mode_var = tk.StringVar(value=selected)
            self.level_var = tk.StringVar(value="全部")
            self.search_var = tk.StringVar()
            self.status_var = tk.StringVar(value="● 已停止")
            self.address_var = tk.StringVar(value="未启动")
            self.build()

        def build(self):
            self.root.title("Python 后端控制面板")
            self.root.geometry("760x560")
            self.root.minsize(640, 480)
            self.root.columnconfigure(0, weight=1)
            self.root.rowconfigure(0, weight=1)

            frame = ttk.Frame(self.root, padding=12)
            frame.grid(row=0, column=0, sticky="nsew")
            frame.columnconfigure(0, weight=1)
            frame.rowconfigure(3, weight=1)

            config = ttk.LabelFrame(frame, text="服务配置", padding=(10, 6))
            config.grid(row=0, column=0, sticky="ew")
            config.columnconfigure(1, weight=1)

            ttk.Label(config, text="端口").grid(row=0, column=0, sticky="w", pady=3, padx=(0, 6))
            self.port_entry = ttk.Entry(config, textvariable=self.port_var, width=10)
            self.port_entry.grid(row=0, column=1, sticky="w", pady=3)

            ttk.Label(config, text="数据库").grid(row=1, column=0, sticky="w", pady=3, padx=(0, 6))
            self.database_entry = ttk.Entry(config, textvariable=self.database_var)
            self.database_entry.grid(row=1, column=1, sticky="ew", pady=3)
            self.database_button = ttk.Button(config, text="浏览…", command=self.browse_database)
            self.database_button.grid(row=1, column=2, padx=(6, 0))

            ttk.Label(config, text="日志文件").grid(row=2, column=0, sticky="w", pady=3, padx=(0, 6))
            self.log_entry = ttk.Entry(config, textvariable=self.log_var)
            self.log_entry.grid(row=2, column=1, sticky="ew", pady=3)
            self.log_button = ttk.Button(config, text="浏览…", command=self.browse_log)
            self.log_button.grid(row=2, column=2, padx=(6, 0))

            ttk.Label(config, text="日志模式").grid(row=3, column=0, sticky="w", pady=3, padx=(0, 6))
            self.mode_box = ttk.Combobox(config, textvariable=self.mode_var, values=tuple(journal_modes),
                                         state="readonly", width=18)
            self.mode_box.grid(row=3, column=1, sticky="w", pady=3)

            status = ttk.Frame(frame)
            status.grid(row=1, column=0, sticky="ew", pady=(12, 0))
            status.columnconfigure(2, weight=1)
            bold = tkfont.nametofont("TkDefaultFont").copy()
            bold.configure(weight="bold")
            self.status_label = ttk.Label(status, textvariable=self.status_var, foreground="#b00020", font=bold)
            self.status_label.grid(row=0, column=0, sticky="w")
            self.address_label = ttk.Label(status, textvariable=self.address_var, foreground="#555555")
            self.address_label.grid(row=0, column=1, padx=(10, 0), sticky="w")

            actions = ttk.Frame(frame)
            actions.grid(row=2, column=0, sticky="ew", pady=10)
            self.start_button = ttk.Button(actions, text="启动", command=self.start)
            self.start_button.pack(side="left")
            self.stop_button = ttk.Button(actions, text="停止", command=self.stop, state="disabled")
            self.stop_button.pack(side="left", padx=(6, 0))
            self.open_button = ttk.Button(actions, text="打开接口", command=self.open_browser, state="disabled")
            self.open_button.pack(side="left", padx=(6, 0))

            log_frame = ttk.LabelFrame(frame, text="运行日志", padding=(6, 6))
            log_frame.grid(row=3, column=0, sticky="nsew")
            log_frame.columnconfigure(0, weight=1)
            log_frame.rowconfigure(1, weight=1)

            toolbar = ttk.Frame(log_frame)
            toolbar.grid(row=0, column=0, sticky="ew", pady=(0, 6))
            ttk.Label(toolbar, text="筛选").pack(side="left")
            self.level_box = ttk.Combobox(toolbar, textvariable=self.level_var, values=tuple(LOG_FILTERS),
                                          state="readonly", width=8)
            self.level_box.pack(side="left", padx=(6, 12))
            ttk.Label(toolbar, text="关键词").pack(side="left")
            self.search_entry = ttk.Entry(toolbar, textvariable=self.search_var, width=24)
            self.search_entry.pack(side="left", padx=(6, 0))

            self.log_text = scrolledtext.ScrolledText(log_frame, height=14, state="disabled", wrap="none")
            self.log_text.grid(row=1, column=0, sticky="nsew")
            for tag, color in (("info", "#1f5fbf"), ("success", "#0a7a2f"),
                               ("warning", "#c2410c"), ("error", "#b00020"), ("muted", "#666666")):
                self.log_text.tag_configure(tag, foreground=color)
            self.level_var.trace_add("write", self.on_filter_changed)
            self.search_var.trace_add("write", self.on_filter_changed)

            ttk.Label(frame, text="关闭窗口会停止服务；日志同时写入上面的日志文件。",
                      foreground="#666666").grid(row=4, column=0, sticky="w", pady=(8, 0))
            self.write_log("面板已就绪。点击“启动”运行服务。\n")

        def on_filter_changed(self, *_args):
            self.render_logs()

        def render_logs(self):
            self.log_text.configure(state="normal")
            self.log_text.delete("1.0", "end")
            for tag, line in self.log_entries:
                if log_matches(self.level_var.get(), tag, line, self.search_var.get()):
                    self.log_text.insert("end", line + "\n", tag)
            self.log_text.see("end")
            self.log_text.configure(state="disabled")

        def write_log(self, text, tag="muted"):
            for line in text.splitlines() or [""]:
                self.log_entries.append((tag, line))
                if log_matches(self.level_var.get(), tag, line, self.search_var.get()):
                    self.append_line(tag, line)

        def append_line(self, tag, line):
            self.log_text.configure(state="normal")
            self.log_text.insert("end", line + "\n", tag)
            if int(self.log_text.index("end-1c").split(".")[0]) > 2000:
                self.log_text.delete("1.0", "201.0")
            self.log_text.see("end")
            self.log_text.configure(state="disabled")

        def set_config_enabled(self, enabled):
            state = "normal" if enabled else "disabled"
            for widget in (self.port_entry, self.database_entry, self.database_button,
                           self.log_entry, self.log_button):
                widget.configure(state=state)
            self.mode_box.configure(state="readonly" if enabled else "disabled")

        def show_running(self, port):
            self.status_var.set("● 运行中")
            self.status_label.configure(foreground="#0a7a2f")
            self.address_var.set(f"http://127.0.0.1:{port}")
            self.start_button.configure(state="disabled")
            self.stop_button.configure(state="normal")
            self.open_button.configure(state="normal")
            self.set_config_enabled(False)

        def show_stopped(self):
            self.status_var.set("● 已停止")
            self.status_label.configure(foreground="#b00020")
            self.address_var.set("未启动")
            self.start_button.configure(state="normal")
            self.stop_button.configure(state="disabled")
            self.open_button.configure(state="disabled")
            self.set_config_enabled(True)

        def parse_port(self):
            text = self.port_var.get().strip()
            if not text.isascii() or not text.isdecimal() or int(text) > 65535:
                raise ValueError("端口必须是 0 到 65535 之间的整数")
            return int(text)

        def parse_path(self, text, label):
            value = text.strip()
            if not value:
                raise ValueError(f"{label}路径不能为空")
            return Path(os.path.expandvars(value)).expanduser().absolute()

        def start(self):
            if self.busy or self.closing or self.controller.running:
                return
            try:
                port = self.parse_port()
                database = self.parse_path(self.database_var.get(), "数据库")
                log_path = self.parse_path(self.log_var.get(), "日志文件")
                journal_mode = journal_modes[self.mode_var.get()]
                if database_files_for(database) & log_files_for(log_path):
                    raise ValueError("数据库和日志文件路径不能冲突")
            except ValueError as error:
                messagebox.showerror("配置错误", str(error), parent=self.root)
                return
            self.busy = True
            self.start_button.configure(state="disabled")
            self.stop_button.configure(state="disabled")
            self.status_var.set("● 正在启动…")
            self.status_label.configure(foreground="#b26a00")
            self.gui_handler = QueueLogHandler(self.log_queue)
            self.gui_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
            LOGGER.addHandler(self.gui_handler)

            def work():
                try:
                    actual = self.controller.start(database, log_path, port, journal_mode)
                except Exception as error:
                    self.root.after(0, lambda error=error: self.start_failed(error))
                else:
                    self.root.after(0, lambda port=actual: self.started(port))

            Thread(target=work, daemon=True).start()

        def start_failed(self, error):
            self.busy = False
            self.detach_log_handler()
            self.flush_logs()
            self.show_stopped()
            self.write_log(f"启动失败：{error}\n", "error")
            if not self.closing:
                messagebox.showerror("启动失败", str(error), parent=self.root)

        def started(self, port):
            self.busy = False
            self.flush_logs()
            self.show_running(port)
            self.write_log(f"服务已启动：http://127.0.0.1:{port}\n", "success")

        def stop(self, close_after=False):
            if self.busy or self.closing and not close_after:
                return
            if not self.controller.running:
                if close_after:
                    self.root.destroy()
                return
            self.busy = True
            self.start_button.configure(state="disabled")
            self.stop_button.configure(state="disabled")
            self.status_var.set("● 正在停止…")
            self.status_label.configure(foreground="#b26a00")

            def work():
                error = None
                try:
                    self.controller.stop()
                except Exception as caught:
                    error = caught
                self.root.after(0, lambda error=error: self.stopped(error, close_after))

            Thread(target=work, daemon=True).start()

        def stopped(self, error, close_after):
            self.busy = False
            self.detach_log_handler()
            self.flush_logs()
            self.show_stopped()
            if error is None:
                self.write_log("服务已停止。\n", "success")
            else:
                self.write_log(f"停止时出错：{error}\n", "error")
            if close_after:
                self.root.destroy()

        def detach_log_handler(self):
            if self.gui_handler is not None:
                LOGGER.removeHandler(self.gui_handler)
                self.gui_handler = None

        def open_browser(self):
            port = self.controller.port
            if port is not None:
                webbrowser.open(f"http://127.0.0.1:{port}/health")

        def flush_logs(self):
            lines = []
            while len(lines) < 200:
                try:
                    lines.append(self.log_queue.get_nowait())
                except Empty:
                    break
            for levelno, text in lines:
                self.write_log(text + "\n", log_level_tag(levelno))

        def poll_logs(self):
            if self.closing:
                return
            self.flush_logs()
            self.root.after(150, self.poll_logs)

        def browse_database(self):
            filename = filedialog.asksaveasfilename(
                parent=self.root, title="选择或新建数据库文件", defaultextension=".sqlite3",
                filetypes=(("SQLite 数据库", "*.sqlite3"), ("所有文件", "*.*")),
                initialdir=str(Path(self.database_var.get()).parent),
            )
            if filename:
                self.database_var.set(filename)

        def browse_log(self):
            filename = filedialog.asksaveasfilename(
                parent=self.root, title="选择日志文件", defaultextension=".log",
                filetypes=(("日志文件", "*.log"), ("所有文件", "*.*")),
                initialdir=str(Path(self.log_var.get()).parent),
            )
            if filename:
                self.log_var.set(filename)

        def close(self):
            self.closing = True
            if self.busy:
                self.root.after(100, self.close)
                return
            if self.controller.running:
                self.stop(close_after=True)
            else:
                self.detach_log_handler()
                self.root.destroy()

    root = tk.Tk()
    panel = PanelApp(root)
    root.protocol("WM_DELETE_WINDOW", panel.close)
    root.after(150, panel.poll_logs)
    try:
        root.mainloop()
    except KeyboardInterrupt:
        panel.closing = True
        try:
            panel.controller.stop()
        finally:
            panel.detach_log_handler()
            root.destroy()


def main():
    parser = argparse.ArgumentParser(description="Run a local JSON API.")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"listen port (default: {DEFAULT_PORT})")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="path to the SQLite database")
    parser.add_argument("--log-file", type=Path, default=DEFAULT_LOG, help="path to the rotating server log")
    parser.add_argument("--journal-mode", choices=("wal", "delete"), help="change the SQLite journal mode on startup")
    parser.add_argument("--busy-timeout-ms", type=int, default=DEFAULT_BUSY_TIMEOUT_MS,
                        help="SQLite lock wait in milliseconds for the service (default: 2000)")
    parser.add_argument("--cors-origin", action="append", default=[], metavar="ORIGIN",
                        help="allow an exact browser origin; repeat to allow multiple origins")
    parser.add_argument("--rate-limit", type=int, default=DEFAULT_RATE_LIMIT, metavar="REQUESTS",
                        help="requests per rolling minute, 0 disables (default: 120)")
    parser.add_argument("--max-connections", type=int, default=DEFAULT_MAX_CONNECTIONS, metavar="N",
                        help=f"maximum simultaneous connections (default: {DEFAULT_MAX_CONNECTIONS})")
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--backup", type=Path, metavar="FILE", help="back up the database to a new file")
    action.add_argument("--restore", type=Path, metavar="FILE", help="restore a snapshot to a new --db file")
    action.add_argument("-i", "--interactive", action="store_true", help="start with an interactive console")
    action.add_argument("--panel", action="store_true", help="start with a desktop control panel")
    action.add_argument("--check", action="store_true", help="check deployment and test SQLite without serving HTTP")
    args = parser.parse_args()
    if not 0 <= args.port <= 65535:
        parser.error("--port must be between 0 and 65535")
    if not 0 <= args.busy_timeout_ms <= 60000:
        parser.error("--busy-timeout-ms must be between 0 and 60000")
    if not 0 <= args.rate_limit <= 100000:
        parser.error("--rate-limit must be between 0 and 100000")
    if not 1 <= args.max_connections <= MAX_MAX_CONNECTIONS:
        parser.error(f"--max-connections must be between 1 and {MAX_MAX_CONNECTIONS}")

    database = args.db.absolute()
    if args.backup is not None or args.restore is not None:
        if args.backup is not None:
            source, destination = database, args.backup.absolute()
        else:
            source, destination = args.restore.absolute(), database
        try:
            copy_snapshot(source, destination)
        except (OSError, sqlite3.Error, ValueError) as error:
            parser.exit(1, f"Could not copy database: {error}\n")
        print(f"Database written to {destination}", flush=True)
        return

    for origin in args.cors_origin:
        try:
            if ORIGIN_FORMAT.fullmatch(origin) is None or urlsplit(origin).port == 0:
                raise ValueError
        except ValueError:
            parser.error("--cors-origin must be an exact http(s) origin with no path or wildcard")
    api_token = os.environ.get(API_TOKEN_ENV)
    if api_token is not None and (len(api_token) > 512 or BEARER_TOKEN.fullmatch(api_token) is None):
        parser.error(f"{API_TOKEN_ENV} must be a nonempty Bearer token of at most 512 characters")

    log_path = args.log_file.resolve()
    database_files = database_files_for(database)
    if database_files & log_files_for(log_path):
        parser.error("database and log file paths must be different")

    if args.panel:
        try:
            serve_panel(args, api_token, database, log_path)
        except Exception as error:
            parser.exit(1, f"Could not start the control panel: {error}\n")
        return

    store = ItemStore(database, args.busy_timeout_ms)
    if args.check:
        try:
            check_configuration(store, log_path, args.journal_mode, args.port)
        except (OSError, RuntimeError, sqlite3.Error, ValueError) as error:
            parser.exit(1, f"Self-check failed: {error}\n")
        return

    try:
        server = ApiServer((LISTEN_HOST, args.port), store, cors_origins=args.cors_origin,
                           api_token=api_token, rate_limit=args.rate_limit,
                           max_connections=args.max_connections)
    except OSError as error:
        parser.exit(1, f"Could not start the server: {error}\n")

    with server:
        try:
            with store.connect() as connection:
                mode = store.initialize(args.journal_mode, connection=connection)
                with request_logging(log_path, terminal=not args.interactive):
                    LOGGER.info("SQLite journal mode: %s; lock wait: %d ms", mode, store.busy_timeout_ms)
                    print(f"Listening on http://127.0.0.1:{server.server_port}", flush=True)
                    if args.interactive:
                        serve_interactive(server, log_path)
                    else:
                        server.serve_forever()
        except sqlite3.Error as error:
            parser.exit(1, f"Could not initialize database ({args.db}): {error}\n")
        except OSError as error:
            parser.exit(1, f"Could not run the server: {error}\n")
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
