from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlparse
import json
import mimetypes
import os
import re
import sqlite3
import threading
import time


ROOT = Path(__file__).resolve().parent
PUBLIC_DIR = ROOT / "public"
DATA_DIR = Path(os.environ.get("DATA_DIR", ROOT / "data"))
DATABASE_PATH = Path(os.environ.get("DATABASE_PATH", DATA_DIR / "crowdline.sqlite3"))
HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "3000"))
RATE_LIMIT_PER_MINUTE = int(os.environ.get("RATE_LIMIT_PER_MINUTE", "120"))
WRITE_LIMIT_PER_MINUTE = int(os.environ.get("WRITE_LIMIT_PER_MINUTE", "45"))

ID_RE = re.compile(r"^[a-zA-Z0-9:_-]+$")
DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
DB_LOCK = threading.RLock()
RATE_LOCK = threading.Lock()
RATE_BUCKETS = {}


def now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def connect_db():
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


DB = connect_db()


def init_db():
    with DB_LOCK:
        DB.executescript(
            """
            CREATE TABLE IF NOT EXISTS kv_store (
              scope TEXT NOT NULL,
              player_id TEXT NOT NULL DEFAULT '',
              key TEXT NOT NULL,
              value TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              PRIMARY KEY (scope, player_id, key)
            );

            CREATE TABLE IF NOT EXISTS crowd_votes (
              round_id TEXT NOT NULL,
              player_id TEXT NOT NULL,
              item TEXT NOT NULL,
              position REAL NOT NULL CHECK (position >= 0 AND position <= 100),
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              PRIMARY KEY (round_id, player_id, item)
            );

            CREATE TABLE IF NOT EXISTS daily_results (
              day_key TEXT NOT NULL,
              player_id TEXT NOT NULL,
              name TEXT NOT NULL,
              score INTEGER NOT NULL CHECK (score >= 0 AND score <= 100),
              squares TEXT NOT NULL,
              created_at TEXT NOT NULL,
              PRIMARY KEY (day_key, player_id)
            );

            CREATE INDEX IF NOT EXISTS idx_crowd_votes_round
              ON crowd_votes (round_id);

            CREATE INDEX IF NOT EXISTS idx_daily_results_board
              ON daily_results (day_key, score DESC, created_at ASC);
            """
        )
        DB.commit()


def clean_id(value, max_length):
    text = str(value or "").strip()
    if not text or len(text) > max_length or not ID_RE.match(text):
        return ""
    return text


def clean_day_key(value):
    text = str(value or "").strip()
    return text if DAY_RE.match(text) else ""


def clean_player_name(value):
    text = str(value or "")
    for char in '<>&"\'':
        text = text.replace(char, "")
    text = "".join(ch for ch in text if ord(ch) >= 32 and ord(ch) != 127)
    return text.strip()[:14] or "Player"


def clean_squares(value):
    text = str(value or "")
    text = "".join(ch for ch in text if ord(ch) >= 32 and ord(ch) != 127)
    return text.strip()[:32]


def clean_score(value):
    try:
        score = round(float(value))
    except (TypeError, ValueError):
        return None
    return max(0, min(100, score))


def clean_position(value):
    try:
        position = float(value)
    except (TypeError, ValueError):
        return None
    return max(0, min(100, position))


def prune_rate_buckets(current_window):
    stale = [key for key in RATE_BUCKETS if key[1] < current_window - 2]
    for key in stale:
        RATE_BUCKETS.pop(key, None)


class Handler(BaseHTTPRequestHandler):
    server_version = "Crowdline/1.0"

    def do_GET(self):
        self.route()

    def do_POST(self):
        self.route()

    def do_PUT(self):
        self.route()

    def route(self):
        parsed = urlparse(self.path)
        try:
            if parsed.path.startswith("/api/"):
                if not self.allowed_by_rate_limit(parsed.path):
                    return
                self.handle_api(parsed.path)
            else:
                self.serve_static(parsed.path)
        except json.JSONDecodeError:
            self.send_error_json(400, "Invalid JSON")
        except ValueError as error:
            self.send_error_json(400, str(error))
        except Exception as error:
            print(error)
            self.send_error_json(500, "Server error")

    def client_key(self):
        forwarded = self.headers.get("X-Forwarded-For", "")
        if forwarded:
            return forwarded.split(",")[0].strip()
        return self.client_address[0]

    def allowed_by_rate_limit(self, path):
        if self.command == "GET" and path == "/api/health":
            return True

        minute = int(time.time() // 60)
        client = self.client_key()
        bucket_type = "write" if self.command in {"POST", "PUT"} else "read"
        limit = WRITE_LIMIT_PER_MINUTE if bucket_type == "write" else RATE_LIMIT_PER_MINUTE

        with RATE_LOCK:
            prune_rate_buckets(minute)
            key = (client, minute, bucket_type)
            RATE_BUCKETS[key] = RATE_BUCKETS.get(key, 0) + 1
            if RATE_BUCKETS[key] > limit:
                self.send_error_json(429, "Slow down a bit")
                return False
        return True

    def handle_api(self, path):
        parts = [unquote(part) for part in path.split("/") if part]

        if self.command == "GET" and len(parts) == 2 and parts[1] == "health":
            self.send_json(200, {"ok": True})
            return

        if len(parts) >= 4 and parts[1] == "store" and self.command in {"GET", "PUT"}:
            self.handle_store(parts)
            return

        if len(parts) == 3 and parts[1] == "crowd":
            self.handle_crowd(parts[2])
            return

        if len(parts) == 3 and parts[1] == "leaderboard":
            self.handle_leaderboard(parts[2])
            return

        self.send_error_json(404, "Not found")

    def handle_store(self, parts):
        scope = parts[2]
        if scope not in {"shared", "player"}:
            self.send_error_json(400, "Invalid storage scope")
            return

        is_shared = scope == "shared"
        player_id = "" if is_shared else clean_id(parts[3], 40)
        key = clean_id(parts[3] if is_shared else (parts[4] if len(parts) > 4 else ""), 100)
        if not key or (not is_shared and not player_id):
            self.send_error_json(400, "Invalid storage key")
            return

        with DB_LOCK:
            if self.command == "GET":
                row = DB.execute(
                    """
                    SELECT value FROM kv_store
                    WHERE scope = ? AND player_id = ? AND key = ?
                    """,
                    (scope, player_id, key),
                ).fetchone()
                if row is None:
                    self.send_error_json(404, "Not found")
                    return
                self.send_json(200, {"value": row["value"]})
                return

            body = self.read_json()
            value = str(body.get("value", ""))
            if len(value) > 20000:
                self.send_error_json(400, "Stored value is too large")
                return

            existing = DB.execute(
                """
                SELECT value FROM kv_store
                WHERE scope = ? AND player_id = ? AND key = ?
                """,
                (scope, player_id, key),
            ).fetchone()

            if scope == "player" and key.startswith("daily:") and existing is not None:
                self.send_json(200, {"key": key, "locked": True})
                return

            DB.execute(
                """
                INSERT INTO kv_store (scope, player_id, key, value, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(scope, player_id, key)
                DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
                """,
                (scope, player_id, key, value, now_iso()),
            )
            DB.commit()
            self.send_json(200, {"key": key})

    def handle_crowd(self, round_id_raw):
        round_id = clean_id(round_id_raw, 60)
        if not round_id:
            self.send_error_json(400, "Invalid round id")
            return

        with DB_LOCK:
            if self.command == "GET":
                rows = DB.execute(
                    """
                    SELECT item, SUM(position) AS sum, COUNT(*) AS n
                    FROM crowd_votes
                    WHERE round_id = ?
                    GROUP BY item
                    """,
                    (round_id,),
                ).fetchall()
                self.send_json(200, {
                    row["item"]: {"sum": row["sum"], "n": row["n"]}
                    for row in rows
                })
                return

            if self.command != "POST":
                self.send_error_json(405, "Method not allowed")
                return

            body = self.read_json()
            player_id = clean_id(body.get("pid"), 40)
            placements = body.get("placements")
            if not player_id or not isinstance(placements, dict):
                self.send_error_json(400, "Invalid placements")
                return

            saved = 0
            timestamp = now_iso()
            for item, raw_pos in placements.items():
                name = str(item or "").strip()[:80]
                position = clean_position(raw_pos)
                if not name or position is None:
                    continue
                DB.execute(
                    """
                    INSERT INTO crowd_votes
                      (round_id, player_id, item, position, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(round_id, player_id, item)
                    DO UPDATE SET position = excluded.position,
                                  updated_at = excluded.updated_at
                    """,
                    (round_id, player_id, name, position, timestamp, timestamp),
                )
                saved += 1

            DB.commit()
            self.send_json(200, {"ok": True, "saved": saved})

    def handle_leaderboard(self, day_key_raw):
        day_key = clean_day_key(day_key_raw)
        if not day_key:
            self.send_error_json(400, "Invalid leaderboard key")
            return

        with DB_LOCK:
            if self.command == "GET":
                self.send_json(200, self.leaderboard(day_key))
                return

            if self.command != "POST":
                self.send_error_json(405, "Method not allowed")
                return

            body = self.read_json()
            pid = clean_id(body.get("pid"), 40)
            score = clean_score(body.get("score"))
            if not pid or score is None:
                self.send_error_json(400, "Invalid leaderboard entry")
                return

            name = clean_player_name(body.get("name"))
            squares = clean_squares(body.get("squares"))
            timestamp = now_iso()
            DB.execute(
                """
                INSERT OR IGNORE INTO daily_results
                  (day_key, player_id, name, score, squares, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (day_key, pid, name, score, squares, timestamp),
            )

            value = json.dumps({"score": score, "squares": squares, "n": body.get("dailyNumber")}, ensure_ascii=False)
            DB.execute(
                """
                INSERT OR IGNORE INTO kv_store
                  (scope, player_id, key, value, updated_at)
                VALUES ('player', ?, ?, ?, ?)
                """,
                (pid, "daily:" + day_key, value, timestamp),
            )

            DB.commit()
            self.send_json(200, self.leaderboard(day_key))

    def leaderboard(self, day_key):
        rows = DB.execute(
            """
            SELECT player_id, name, score, squares
            FROM daily_results
            WHERE day_key = ?
            ORDER BY score DESC, created_at ASC
            LIMIT 50
            """,
            (day_key,),
        ).fetchall()
        return [
            {"pid": row["player_id"], "n": row["name"], "s": row["score"], "q": row["squares"]}
            for row in rows
        ]

    def serve_static(self, raw_path):
        request_path = unquote(raw_path)
        if request_path == "/":
            request_path = "/index.html"

        file_path = (PUBLIC_DIR / request_path.lstrip("/")).resolve()
        public_root = PUBLIC_DIR.resolve()
        if public_root not in file_path.parents and file_path != public_root:
            self.send_response(403)
            self.end_headers()
            self.wfile.write(b"Forbidden")
            return

        if not file_path.exists() or not file_path.is_file():
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Not found")
            return

        content_type = mimetypes.guess_type(str(file_path))[0] or "application/octet-stream"
        if content_type.startswith("text/") or content_type in {"application/javascript", "application/json"}:
            content_type += "; charset=utf-8"

        data = file_path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def read_json(self):
        length = int(self.headers.get("Content-Length", "0"))
        if length > 1024 * 1024:
            raise ValueError("Request body is too large")
        if length == 0:
            return {}
        raw = self.rfile.read(length).decode("utf-8")
        return json.loads(raw)

    def send_json(self, status, value):
        data = json.dumps(value, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def send_error_json(self, status, message):
        self.send_json(status, {"error": message})

    def log_message(self, fmt, *args):
        print("%s - %s" % (self.address_string(), fmt % args))


def main():
    init_db()
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    shown_host = "localhost" if HOST in {"0.0.0.0", "::"} else HOST
    print(f"Crowdline is running at http://{shown_host}:{PORT}")
    server.serve_forever()


if __name__ == "__main__":
    main()
