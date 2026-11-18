import argparse
import cmd
import json
import logging
import math
import os
import re
import socket
import sqlite3
import tempfile
import time
from contextlib import closing, contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from logging.handlers import RotatingFileHandler
from pathlib import Path
from threading import Thread
from urllib.parse import parse_qs


MAX_BODY_BYTES = 64 * 1024
MAX_ITEM_ID = (1 << 63) - 1
MAX_ITEM_DEPTH = 64
ITEM_ROUTE = re.compile(r"/api/items/([1-9][0-9]{0,18})")
DEFAULT_DB = Path(__file__).resolve().with_name("items.sqlite3")
DEFAULT_LOG = DEFAULT_DB.parent / "logs" / "server.log"
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


def matches_item(data, name, status):
    item = json.loads(data)
    item_name = item.get("name")
    if name is not None and (not isinstance(item_name, str) or name not in item_name.casefold()):
        return False
    return status is None or item.get("status") == status


class ItemStore:
    def __init__(self, path):
        self.path = path

    @contextmanager
    def connect(self):
        with closing(sqlite3.connect(str(self.path), timeout=2)) as connection, connection:
            yield connection

    def initialize(self):
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "CREATE TABLE IF NOT EXISTS items ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT NOT NULL, updated_at INTEGER NOT NULL DEFAULT 0)"
            )
            columns = {column[1] for column in connection.execute("PRAGMA table_info(items)")}
            if "updated_at" not in columns:
                connection.execute("ALTER TABLE items ADD COLUMN updated_at INTEGER NOT NULL DEFAULT 0")
            connection.execute("SELECT id, data, updated_at FROM items LIMIT 0")
            connection.execute("CREATE INDEX IF NOT EXISTS items_updated_idx ON items (updated_at, id)")

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
        has_filters = name is not None or status is not None
        if has_filters:
            predicates.append("matches_item(data, ?, ?)")
            parameters.extend((name.casefold() if name is not None else None, status))
        where = " WHERE " + " AND ".join(predicates) if predicates else ""
        with self.connect() as connection:
            if has_filters:
                connection.create_function("matches_item", 3, matches_item)
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

    def create(self, data):
        with self.connect() as connection:
            cursor = connection.execute(
                "INSERT INTO items (data, updated_at) VALUES (?, ?)", (data, time.time_ns() // 1_000_000)
            )
            return cursor.lastrowid

    def replace(self, item_id, data):
        with self.connect() as connection:
            cursor = connection.execute(
                "UPDATE items SET data = ?, updated_at = ? WHERE id = ?",
                (data, time.time_ns() // 1_000_000, item_id),
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
        try:
            super().handle_one_request()
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

    def dispatch(self):
        try:
            self.validate_host()
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
        except ApiError as error:
            self.send_json(error.status, {"error": error.code}, error.headers)
        except sqlite3.Error as error:
            self.log_error("SQLite operation failed: %s", error)
            self.send_json(503, {"error": "database_unavailable"})

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
        method = "GET" if self.command == "HEAD" else self.command
        if method not in methods:
            allowed = list(methods)
            if "GET" in methods:
                allowed.append("HEAD")
            raise ApiError(405, "method_not_allowed", (("Allow", ", ".join(allowed)),))
        methods[method](*args)

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
        data, encoded = self.read_item()
        item_id = self.server.store.create(encoded)
        self.send_json(201, {"id": item_id, "data": data}, (("Location", f"/api/items/{item_id}"),))

    def get_item(self, item_id):
        self.send_json(200, self.server.store.get(item_id))

    def replace_item(self, item_id):
        data, encoded = self.read_item()
        self.server.store.replace(item_id, encoded)
        self.send_json(200, {"id": item_id, "data": data})

    def delete_item(self, item_id):
        self.server.store.delete(item_id)
        self.send_json(204)

    def read_item(self):
        data = self.read_json()
        if not isinstance(data, dict):
            raise ApiError(400, "object_required")
        check_item_depth(data)
        return data, encode_json(data).decode("utf-8")

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
        self.send_json(code, {"error": "http_error"})

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

    def __init__(self, address, store):
        super().__init__(address, ApiHandler)
        self.store = store

    def handle_error(self, request, client_address):
        LOGGER.exception("%s Request handler failed", client_address[0])


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


def main():
    parser = argparse.ArgumentParser(description="Run a local JSON API.")
    parser.add_argument("--port", type=int, default=8000, help="listen port (default: 8000)")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="path to the SQLite database")
    parser.add_argument("--log-file", type=Path, default=DEFAULT_LOG, help="path to the rotating server log")
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--backup", type=Path, metavar="FILE", help="back up the database to a new file")
    action.add_argument("--restore", type=Path, metavar="FILE", help="restore a snapshot to a new --db file")
    action.add_argument("-i", "--interactive", action="store_true", help="start with an interactive console")
    args = parser.parse_args()
    if not 0 <= args.port <= 65535:
        parser.error("--port must be between 0 and 65535")

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

    log_path = args.log_file.resolve()
    database_files = {
        Path(str(database) + suffix).resolve() for suffix in ("", "-journal", "-wal", "-shm")
    }
    if database_files & log_files_for(log_path):
        parser.error("database and log file paths must be different")

    store = ItemStore(database)
    try:
        server = ApiServer(("127.0.0.1", args.port), store)
    except OSError as error:
        parser.exit(1, f"Could not start the server: {error}\n")

    with server:
        try:
            store.initialize()
        except sqlite3.Error as error:
            parser.exit(1, f"Could not initialize database ({args.db}): {error}\n")
        try:
            with request_logging(log_path, terminal=not args.interactive):
                print(f"Listening on http://127.0.0.1:{server.server_port}", flush=True)
                if args.interactive:
                    serve_interactive(server, log_path)
                else:
                    server.serve_forever()
        except OSError as error:
            parser.exit(1, f"Could not run the server: {error}\n")
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
