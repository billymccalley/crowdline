from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlparse
import datetime as dt
import json
import hashlib
import hmac
import mimetypes
import os
import re
import secrets
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
DAILY_EPOCH = dt.date(2026, 7, 11)

ID_RE = re.compile(r"^[a-zA-Z0-9:_-]+$")
DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
LOGIN_RE = re.compile(r"^[a-zA-Z0-9_ -]+$")
ANALYTICS_EVENTS = {
    "daily_start",
    "daily_complete",
    "practice_start",
    "signup",
    "share_copied",
}
FEEDBACK_CATEGORIES = {"bug", "confusing", "idea", "content", "account", "other"}
CURATION_LABELS = {"daily", "confusing", "too_easy", "too_hard", "funny", "needs_review", "retired"}
ADMIN_NAMES = {"billy"}
BLOCKED_NAME_PARTS = {
    "fuck",
    "fucker",
    "fuk",
    "shit",
    "bitch",
    "cunt",
    "nigger",
    "nigga",
    "faggot",
    "fagot",
    "slut",
    "whore",
    "kike",
    "retard",
    "chink",
    "gook",
    "spic",
    "wetback",
    "coon",
    "beaner",
    "raghead",
    "towelhead",
    "tranny",
    "dyke",
    "hitler",
    "nazi",
    "kkk",
}
NAME_FILTER_TRANS = str.maketrans({
    "0": "o",
    "1": "i",
    "!": "i",
    "3": "e",
    "4": "a",
    "@": "a",
    "5": "s",
    "$": "s",
    "7": "t",
    "+": "t",
    "8": "b",
})
DB_LOCK = threading.RLock()
RATE_LOCK = threading.Lock()
RATE_BUCKETS = {}
ROUND_IDS_CACHE = None
ROUND_DETAILS_CACHE = None


def now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def today_key():
    return dt.date.today().isoformat()


def max_world_today():
    return (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=14)).date()


def max_world_today_key():
    return max_world_today().isoformat()


def date_key_for_offset(days_ahead):
    return (dt.date.today() + dt.timedelta(days=days_ahead)).isoformat()


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

            CREATE TABLE IF NOT EXISTS users (
              id TEXT PRIMARY KEY,
              login_name TEXT NOT NULL UNIQUE,
              display_name TEXT NOT NULL,
              password_salt TEXT NOT NULL,
              password_hash TEXT NOT NULL,
              created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sessions (
              token_hash TEXT PRIMARY KEY,
              user_id TEXT NOT NULL,
              created_at TEXT NOT NULL,
              last_seen_at TEXT NOT NULL,
              FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
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

            CREATE TABLE IF NOT EXISTS analytics_events (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              event TEXT NOT NULL,
              day_key TEXT NOT NULL,
              user_id TEXT NOT NULL DEFAULT '',
              created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS reports (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              reporter_id TEXT NOT NULL DEFAULT '',
              target_type TEXT NOT NULL,
              target_value TEXT NOT NULL,
              reason TEXT NOT NULL,
              created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS feedback (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              user_id TEXT NOT NULL DEFAULT '',
              player_name TEXT NOT NULL DEFAULT '',
              category TEXT NOT NULL,
              message TEXT NOT NULL,
              page TEXT NOT NULL DEFAULT '',
              status TEXT NOT NULL DEFAULT 'open',
              created_at TEXT NOT NULL,
              closed_at TEXT NOT NULL DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS puzzle_curation (
              round_id TEXT PRIMARY KEY,
              label TEXT NOT NULL,
              note TEXT NOT NULL DEFAULT '',
              updated_by TEXT NOT NULL DEFAULT '',
              updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS daily_rounds (
              day_key TEXT PRIMARY KEY,
              daily_number INTEGER NOT NULL UNIQUE,
              round_id TEXT NOT NULL,
              created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_crowd_votes_round
              ON crowd_votes (round_id);

            CREATE INDEX IF NOT EXISTS idx_daily_results_board
              ON daily_results (day_key, score DESC, created_at ASC);

            CREATE INDEX IF NOT EXISTS idx_sessions_user
              ON sessions (user_id);

            CREATE INDEX IF NOT EXISTS idx_analytics_events_day
              ON analytics_events (day_key, event);

            CREATE INDEX IF NOT EXISTS idx_feedback_status
              ON feedback (status, created_at);

            CREATE INDEX IF NOT EXISTS idx_daily_rounds_round
              ON daily_rounds (round_id);
            """
        )
        cleanup_future_public_data()
        DB.commit()


def clean_id(value, max_length):
    text = str(value or "").strip()
    if not text or len(text) > max_length or not ID_RE.match(text):
        return ""
    return text


def clean_day_key(value):
    text = str(value or "").strip()
    return text if DAY_RE.match(text) else ""


def daily_number_for_key(day_key):
    try:
        year, month, day = [int(part) for part in str(day_key).split("-")]
        date_value = dt.date(year, month, day)
    except (TypeError, ValueError):
        return 0
    return (date_value - DAILY_EPOCH).days + 1


def date_for_key(day_key):
    try:
        year, month, day = [int(part) for part in str(day_key).split("-")]
        return dt.date(year, month, day)
    except (TypeError, ValueError):
        return None


def is_public_day_available(day_key):
    date_value = date_for_key(day_key)
    return bool(date_value and date_value <= max_world_today())


def cleanup_future_public_data():
    max_key = max_world_today_key()
    far_future_key = date_key_for_offset(31)
    for table in ("daily_results",):
        rows = DB.execute(f"SELECT DISTINCT day_key FROM {table}").fetchall()
        for row in rows:
            day_key = row["day_key"]
            if not date_for_key(day_key) or day_key > max_key:
                DB.execute(f"DELETE FROM {table} WHERE day_key = ?", (day_key,))

    rows = DB.execute("SELECT DISTINCT key FROM kv_store WHERE key LIKE 'daily:%' OR key LIKE 'archive:%'").fetchall()
    for row in rows:
        key = row["key"]
        day_key = key.split(":", 1)[1]
        if not date_for_key(day_key) or day_key > max_key:
            DB.execute("DELETE FROM kv_store WHERE key = ?", (key,))

    rows = DB.execute("SELECT day_key FROM daily_rounds").fetchall()
    for row in rows:
        day_key = row["day_key"]
        if not date_for_key(day_key) or day_key > far_future_key:
            DB.execute("DELETE FROM daily_rounds WHERE day_key = ?", (day_key,))


def stable_hash(value):
    h = 2166136261
    for char in str(value):
        h ^= ord(char)
        h = (h * 16777619) & 0xFFFFFFFF
    return h


def round_ids():
    global ROUND_IDS_CACHE
    if ROUND_IDS_CACHE is None:
        ROUND_IDS_CACHE = list(round_details().keys()) or ["edible"]
    return ROUND_IDS_CACHE


def round_details():
    global ROUND_DETAILS_CACHE
    if ROUND_DETAILS_CACHE is None:
        text = (PUBLIC_DIR / "index.html").read_text(encoding="utf-8")
        details = {}
        pattern = re.compile(
            r'\{\s*id:"([^"]+)",\s*a:"([^"]*)",\s*b:"([^"]*)",\s*items:\[(.*?)\]\s*\}',
            re.S,
        )
        item_pattern = re.compile(r'\["([^"]+)",\s*([0-9.]+)\]')
        for match in pattern.finditer(text):
            round_id = match.group(1)
            items = [
                {"name": item.group(1), "seed": float(item.group(2))}
                for item in item_pattern.finditer(match.group(4))
            ]
            if items:
                details[round_id] = {
                    "id": round_id,
                    "a": match.group(2),
                    "b": match.group(3),
                    "items": items,
                }
        ROUND_DETAILS_CACHE = details
    return ROUND_DETAILS_CACHE


def daily_label_rank(label):
    if label == "daily":
        return 0
    if label == "funny":
        return 1
    if label in {"too_easy", "too_hard"}:
        return 3
    if label in {"confusing", "needs_review"}:
        return 4
    return 2


def daily_schedule_ids():
    labels = {
        row["round_id"]: row["label"]
        for row in DB.execute("SELECT round_id, label FROM puzzle_curation").fetchall()
    }
    ids = [round_id for round_id in round_ids() if labels.get(round_id) != "retired"]
    if not ids:
        ids = round_ids()
    return sorted(
        ids,
        key=lambda round_id: (
            daily_label_rank(labels.get(round_id, "unmarked")),
            stable_hash("crowdline-daily:" + round_id),
        ),
    )


def used_daily_round_ids(except_day_key=""):
    if except_day_key:
        rows = DB.execute(
            "SELECT round_id FROM daily_rounds WHERE day_key != ?",
            (except_day_key,),
        ).fetchall()
    else:
        rows = DB.execute("SELECT round_id FROM daily_rounds").fetchall()
    return {row["round_id"] for row in rows}


def next_daily_round_id(daily_number, extra_used=None):
    schedule = daily_schedule_ids()
    if not schedule:
        return ""
    used = used_daily_round_ids()
    if extra_used:
        used.update(extra_used)
    for candidate in schedule:
        if candidate not in used:
            return candidate
    return ""


def locked_daily_round(day_key):
    day_key = clean_day_key(day_key)
    daily_number = daily_number_for_key(day_key)
    if not day_key or daily_number < 1:
        return None

    existing = DB.execute(
        "SELECT day_key, daily_number, round_id FROM daily_rounds WHERE day_key = ?",
        (day_key,),
    ).fetchone()
    if existing:
        return existing

    round_id = next_daily_round_id(daily_number)
    if not round_id:
        return None
    timestamp = now_iso()
    DB.execute(
        """
        INSERT INTO daily_rounds (day_key, daily_number, round_id, created_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(day_key) DO NOTHING
        """,
        (day_key, daily_number, round_id, timestamp),
    )
    row = DB.execute(
        "SELECT day_key, daily_number, round_id FROM daily_rounds WHERE day_key = ?",
        (day_key,),
    ).fetchone()
    return row


def clean_player_name(value):
    text = str(value or "")
    for char in '<>&"\'':
        text = text.replace(char, "")
    text = "".join(ch for ch in text if ord(ch) >= 32 and ord(ch) != 127)
    return text.strip()[:14] or "Player"


def normalized_name(value):
    text = str(value or "").lower().translate(NAME_FILTER_TRANS)
    return re.sub(r"[^a-z0-9]", "", text)


def is_blocked_name(value):
    compact = normalized_name(value)
    return any(part in compact for part in BLOCKED_NAME_PARTS)


def public_player_name(value):
    text = clean_player_name(value)
    return "Player" if is_blocked_name(text) else text


def clean_login_name(value):
    text = clean_player_name(value)
    if len(text) < 3 or not LOGIN_RE.match(text) or is_blocked_name(text):
        return ""
    return text


def password_hash(password, salt_hex=None):
    salt = bytes.fromhex(salt_hex) if salt_hex else secrets.token_bytes(16)
    hashed = hashlib.pbkdf2_hmac("sha256", str(password).encode("utf-8"), salt, 210000)
    return salt.hex(), hashed.hex()


def hash_token(token):
    return hashlib.sha256(str(token).encode("utf-8")).hexdigest()


def clean_squares(value):
    text = str(value or "")
    allowed = {"\U0001F7E9", "\U0001F7E8", "\U0001F7E7", "\U0001F7E5"}
    return "".join(ch for ch in text if ch in allowed)[:4]


def clean_score(value):
    try:
        score = round(float(value))
    except (TypeError, ValueError):
        return None
    return max(0, min(100, score))


def clean_nonnegative_int(value, max_value=1000000):
    try:
        number = int(float(value))
    except (TypeError, ValueError):
        return 0
    return max(0, min(max_value, number))


def clean_stat_record(value):
    if not isinstance(value, dict):
        return None
    axis = str(value.get("ax") or "").strip()[:160]
    score = clean_score(value.get("sc"))
    if not axis or score is None:
        return None
    return {"ax": axis, "sc": score}


def validate_stats_value(value):
    try:
        data = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    last_daily = clean_day_key(data.get("lastDaily")) if data.get("lastDaily") else ""
    if last_daily and not is_public_day_available(last_daily):
        last_daily = ""
    clean = {
        "g": clean_nonnegative_int(data.get("g")),
        "r": clean_nonnegative_int(data.get("r")),
        "s": clean_nonnegative_int(data.get("s"), 100000000),
        "hi": clean_stat_record(data.get("hi")),
        "lo": clean_stat_record(data.get("lo")),
        "d": clean_nonnegative_int(data.get("d")),
        "streak": clean_nonnegative_int(data.get("streak")),
        "bestStreak": clean_nonnegative_int(data.get("bestStreak")),
        "lastDaily": last_daily,
    }
    return json.dumps(clean, ensure_ascii=False)


def validate_result_value(value, key_prefix, day_key):
    if not is_public_day_available(day_key):
        return None
    try:
        data = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    score = clean_score(data.get("score"))
    if score is None:
        return None
    clean = {
        "score": score,
        "squares": clean_squares(data.get("squares")),
        "n": clean_nonnegative_int(data.get("n"), 100000),
    }
    if key_prefix == "daily":
        placements = clean_placements(data.get("placements"))
        if placements:
            clean["placements"] = placements
    return json.dumps(clean, ensure_ascii=False)


def validate_player_store_value(key, value):
    if len(value) > 5000:
        return None
    if key == "pref:muted":
        return value if value in {"0", "1"} else None
    if key == "pref:name":
        name = public_player_name(value)
        return name if name else None
    return None


def valid_store_key(scope, key):
    if scope == "player":
        if key in {"pref:muted", "pref:name", "stats"}:
            return True
        if key.startswith("daily:") or key.startswith("archive:"):
            day_key = key.split(":", 1)[1]
            return bool(clean_day_key(day_key) and is_public_day_available(day_key))
        return False
    if scope == "shared":
        if key.startswith("crowd:"):
            return bool(clean_id(key.split(":", 1)[1], 80))
        if key.startswith("lb:"):
            day_key = key.split(":", 1)[1]
            return bool(clean_day_key(day_key) and is_public_day_available(day_key))
    return False


def writable_player_store_key(key):
    return key in {"pref:muted", "pref:name"}


def clean_position(value):
    try:
        position = float(value)
    except (TypeError, ValueError):
        return None
    return max(0, min(100, position))


def clean_placements(value):
    if not isinstance(value, dict):
        return {}
    placements = {}
    for item, raw_pos in value.items():
        name = str(item or "").strip()[:80]
        position = clean_position(raw_pos)
        if name and position is not None:
            placements[name] = position
    return placements


def score_square(distance):
    if distance <= 8:
        return "\U0001F7E9"
    if distance <= 18:
        return "\U0001F7E8"
    if distance <= 32:
        return "\U0001F7E7"
    return "\U0001F7E5"


def calculate_round_result(round_id, player_id, placements):
    detail = round_details().get(round_id)
    if not detail:
        return None

    items = detail["items"]
    expected = {item["name"] for item in items}
    if any(name not in placements for name in expected):
        return None

    rows = DB.execute(
        """
        SELECT item, SUM(position) AS total, COUNT(*) AS count
        FROM crowd_votes
        WHERE round_id = ? AND player_id != ?
        GROUP BY item
        """,
        (round_id, player_id),
    ).fetchall()
    crowd = {row["item"]: row for row in rows}

    total_distance = 0
    squares = []
    for item in items:
        row = crowd.get(item["name"])
        crowd_sum = float(row["total"] or 0) if row else 0
        crowd_count = int(row["count"] or 0) if row else 0
        avg = (item["seed"] * 6 + crowd_sum) / (6 + crowd_count)
        distance = abs(avg - placements[item["name"]])
        total_distance += distance
        squares.append(score_square(distance))

    avg_distance = total_distance / len(items)
    score = round(max(0, 100 - avg_distance * 1.6))
    return {"score": max(0, min(100, score)), "squares": "".join(squares)}


def daily_result_value(player_id, day_key):
    row = DB.execute(
        """
        SELECT score, squares
        FROM daily_results
        WHERE player_id = ? AND day_key = ?
        """,
        (player_id, day_key),
    ).fetchone()
    if not row:
        return None
    return {
        "score": clean_score(row["score"]) or 0,
        "squares": clean_squares(row["squares"]),
        "n": daily_number_for_key(day_key),
    }


def player_stats_value(player_id):
    rows = DB.execute(
        """
        SELECT day_key, score
        FROM daily_results
        WHERE player_id = ?
        ORDER BY day_key ASC
        """,
        (player_id,),
    ).fetchall()
    if not rows:
        return {
            "g": 0,
            "r": 0,
            "s": 0,
            "hi": None,
            "lo": None,
            "d": 0,
            "streak": 0,
            "bestStreak": 0,
            "lastDaily": "",
        }

    total = 0
    high = None
    low = None
    best_streak = 0
    streak = 0
    previous_date = None
    for row in rows:
        score = clean_score(row["score"]) or 0
        day_key = row["day_key"]
        label = "Daily #" + str(daily_number_for_key(day_key))
        record = {"ax": label, "sc": score}
        total += score
        if high is None or score > high["sc"]:
            high = record
        if low is None or score < low["sc"]:
            low = record

        current_date = date_for_key(day_key)
        if previous_date and current_date == previous_date + dt.timedelta(days=1):
            streak += 1
        else:
            streak = 1
        best_streak = max(best_streak, streak)
        previous_date = current_date

    last_daily = rows[-1]["day_key"]
    return {
        "g": len(rows),
        "r": len(rows),
        "s": total,
        "hi": high,
        "lo": low,
        "d": len(rows),
        "streak": streak,
        "bestStreak": best_streak,
        "lastDaily": last_daily,
    }


def suspicious_perfect_entry(round_id, score, squares, placements):
    if score < 100:
        return False
    detail = round_details().get(round_id)
    if not detail:
        return False
    items = detail["items"]
    if clean_squares(squares) != "\U0001F7E9" * len(items):
        return True
    if not placements or any(item["name"] not in placements for item in items):
        return True

    exact_seed_hits = sum(
        1
        for item in items
        if abs(placements[item["name"]] - item["seed"]) <= 0.03
    )
    return exact_seed_hits == len(items)


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
            elif parsed.path == "/leaderboard":
                self.serve_leaderboard_page()
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

        if len(parts) >= 2 and parts[1] == "auth":
            self.handle_auth(parts)
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

        if len(parts) == 3 and parts[1] == "daily":
            self.handle_daily(parts[2])
            return

        if len(parts) >= 2 and parts[1] == "daily-admin":
            self.handle_daily_admin(parts)
            return

        if len(parts) >= 2 and parts[1] == "analytics":
            self.handle_analytics(parts)
            return

        if len(parts) >= 2 and parts[1] == "feedback":
            self.handle_feedback(parts)
            return

        if len(parts) >= 2 and parts[1] == "curation":
            self.handle_curation(parts)
            return

        if len(parts) == 2 and parts[1] == "report":
            self.handle_report()
            return

        self.send_error_json(404, "Not found")

    def bearer_token(self):
        header = self.headers.get("Authorization", "")
        if not header.lower().startswith("bearer "):
            return ""
        return header.split(" ", 1)[1].strip()

    def auth_user(self):
        token = self.bearer_token()
        if not token:
            return None
        row = DB.execute(
            """
            SELECT users.id, users.display_name, users.login_name
            FROM sessions
            JOIN users ON users.id = sessions.user_id
            WHERE sessions.token_hash = ?
            """,
            (hash_token(token),),
        ).fetchone()
        if row:
            DB.execute(
                "UPDATE sessions SET last_seen_at = ? WHERE token_hash = ?",
                (now_iso(), hash_token(token)),
            )
            DB.commit()
            return {
                "pid": row["id"],
                "name": row["display_name"],
                "isAdmin": row["login_name"] in ADMIN_NAMES,
            }
        return None

    def track_event(self, event, day_key="", user_id=""):
        if event not in ANALYTICS_EVENTS:
            return
        day_key = clean_day_key(day_key) or time.strftime("%Y-%m-%d", time.localtime())
        DB.execute(
            """
            INSERT INTO analytics_events (event, day_key, user_id, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (event, day_key, user_id or "", now_iso()),
        )

    def create_session(self, user_id):
        token = secrets.token_urlsafe(32)
        timestamp = now_iso()
        DB.execute(
            """
            INSERT INTO sessions (token_hash, user_id, created_at, last_seen_at)
            VALUES (?, ?, ?, ?)
            """,
            (hash_token(token), user_id, timestamp, timestamp),
        )
        return token

    def handle_auth(self, parts):
        action = parts[2] if len(parts) > 2 else ""
        with DB_LOCK:
            if self.command == "GET" and action == "me":
                user = self.auth_user()
                if not user:
                    self.send_error_json(401, "Not signed in")
                    return
                self.send_json(200, user)
                return

            if self.command == "POST" and action in {"signup", "login"}:
                body = self.read_json()
                name = clean_login_name(body.get("name"))
                password = str(body.get("password") or "")
                if not name or len(password) < 6:
                    self.send_error_json(400, "Use a name and a password with at least 6 characters")
                    return

                login_name = name.lower()
                if action == "signup":
                    existing = DB.execute(
                        "SELECT id FROM users WHERE login_name = ?",
                        (login_name,),
                    ).fetchone()
                    if existing:
                        self.send_error_json(409, "That name is already taken")
                        return

                    user_id = "u_" + secrets.token_urlsafe(12).replace("-", "_")
                    salt, hashed = password_hash(password)
                    DB.execute(
                        """
                        INSERT INTO users
                          (id, login_name, display_name, password_salt, password_hash, created_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (user_id, login_name, name, salt, hashed, now_iso()),
                    )
                    token = self.create_session(user_id)
                    self.track_event("signup", body.get("dayKey"), user_id)
                    DB.commit()
                    self.send_json(200, {"token": token, "pid": user_id, "name": name, "isAdmin": login_name in ADMIN_NAMES})
                    return

                row = DB.execute(
                    """
                    SELECT id, display_name, password_salt, password_hash
                    FROM users
                    WHERE login_name = ?
                    """,
                    (login_name,),
                ).fetchone()
                if not row:
                    self.send_error_json(401, "Name or password did not match")
                    return

                _, hashed = password_hash(password, row["password_salt"])
                if not hmac.compare_digest(hashed, row["password_hash"]):
                    self.send_error_json(401, "Name or password did not match")
                    return

                token = self.create_session(row["id"])
                DB.commit()
                self.send_json(200, {"token": token, "pid": row["id"], "name": row["display_name"], "isAdmin": login_name in ADMIN_NAMES})
                return

            if self.command == "POST" and action == "logout":
                token = self.bearer_token()
                if token:
                    DB.execute("DELETE FROM sessions WHERE token_hash = ?", (hash_token(token),))
                    DB.commit()
                self.send_json(200, {"ok": True})
                return

        self.send_error_json(404, "Not found")

    def handle_analytics(self, parts):
        with DB_LOCK:
            if self.command == "POST" and len(parts) == 2:
                body = self.read_json()
                event = str(body.get("event") or "")
                if event not in ANALYTICS_EVENTS:
                    self.send_error_json(400, "Invalid analytics event")
                    return
                user = self.auth_user()
                self.track_event(event, body.get("dayKey"), user["pid"] if user else "")
                DB.commit()
                self.send_json(200, {"ok": True})
                return

            if self.command == "GET" and len(parts) == 3 and parts[2] == "summary":
                user = self.auth_user()
                if not user or not user.get("isAdmin"):
                    self.send_error_json(403, "Analytics are private")
                    return

                rows = DB.execute(
                    """
                    SELECT day_key, event, COUNT(*) AS count
                    FROM analytics_events
                    GROUP BY day_key, event
                    ORDER BY day_key DESC, event ASC
                    LIMIT 120
                    """
                ).fetchall()
                totals = DB.execute(
                    """
                    SELECT event, COUNT(*) AS count
                    FROM analytics_events
                    GROUP BY event
                    ORDER BY event ASC
                    """
                ).fetchall()
                self.send_json(200, {
                    "totals": {row["event"]: row["count"] for row in totals},
                    "days": [
                        {"day": row["day_key"], "event": row["event"], "count": row["count"]}
                        for row in rows
                    ],
                })
                return

        self.send_error_json(404, "Not found")

    def handle_daily(self, day_key_raw):
        if self.command != "GET":
            self.send_error_json(405, "Method not allowed")
            return
        day_key = clean_day_key(day_key_raw)
        if not day_key or daily_number_for_key(day_key) < 1:
            self.send_error_json(400, "Invalid daily key")
            return
        if not is_public_day_available(day_key):
            self.send_error_json(403, "That Crowdline is not unlocked yet")
            return
        with DB_LOCK:
            row = locked_daily_round(day_key)
            if not row:
                self.send_error_json(400, "Invalid daily key")
                return
            DB.commit()
            self.send_json(200, {
                "dayKey": row["day_key"],
                "dailyNumber": row["daily_number"],
                "roundId": row["round_id"],
            })

    def daily_calendar_items(self, days):
        labels = {
            row["round_id"]: row["label"]
            for row in DB.execute("SELECT round_id, label FROM puzzle_curation").fetchall()
        }
        reserved = set()
        items = []
        for offset in range(1, days + 1):
            day_key = date_key_for_offset(offset)
            daily_number = daily_number_for_key(day_key)
            row = DB.execute(
                "SELECT day_key, daily_number, round_id FROM daily_rounds WHERE day_key = ?",
                (day_key,),
            ).fetchone()
            if row:
                round_id = row["round_id"]
                locked = True
            else:
                round_id = next_daily_round_id(daily_number, reserved)
                locked = False
            if round_id:
                reserved.add(round_id)
            items.append({
                "dayKey": day_key,
                "dailyNumber": daily_number,
                "roundId": round_id,
                "label": labels.get(round_id, "unmarked") if round_id else "",
                "locked": locked,
                "canEdit": day_key > today_key(),
            })
        return items

    def handle_daily_admin(self, parts):
        user = self.auth_user()
        if not user or not user.get("isAdmin"):
            self.send_error_json(403, "Daily calendar is private")
            return

        with DB_LOCK:
            if self.command == "GET" and len(parts) == 3 and parts[2] == "calendar":
                self.send_json(200, {"items": self.daily_calendar_items(8)})
                return

            if self.command != "POST" or len(parts) != 2:
                self.send_error_json(404, "Not found")
                return

            body = self.read_json()
            action = str(body.get("action") or "").strip().lower()
            day_key = clean_day_key(body.get("dayKey"))
            day_date = date_for_key(day_key)
            if not day_key or not day_date or daily_number_for_key(day_key) < 1:
                self.send_error_json(400, "Pick a valid future day")
                return
            if day_key <= today_key():
                self.send_error_json(400, "Today and past dailies are locked")
                return

            if action == "lock":
                existing = DB.execute(
                    "SELECT day_key, daily_number, round_id FROM daily_rounds WHERE day_key = ?",
                    (day_key,),
                ).fetchone()
                if existing:
                    self.send_json(200, {"ok": True, "items": self.daily_calendar_items(8)})
                    return

                round_id = clean_id(body.get("roundId"), 80) or next_daily_round_id(daily_number_for_key(day_key))
                if round_id not in round_ids():
                    self.send_error_json(400, "No unused daily-worthy rounds are available")
                    return
                label_row = DB.execute(
                    "SELECT label FROM puzzle_curation WHERE round_id = ?",
                    (round_id,),
                ).fetchone()
                if label_row and label_row["label"] == "retired":
                    self.send_error_json(400, "That round is retired")
                    return
                if round_id in used_daily_round_ids(except_day_key=day_key):
                    self.send_error_json(400, "That round is already locked for another day")
                    return

                DB.execute(
                    """
                    INSERT INTO daily_rounds (day_key, daily_number, round_id, created_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (day_key, daily_number_for_key(day_key), round_id, now_iso()),
                )
                DB.commit()
                self.send_json(200, {"ok": True, "items": self.daily_calendar_items(8)})
                return

            if action == "swap":
                round_id = clean_id(body.get("roundId"), 80)
                if round_id not in round_ids():
                    self.send_error_json(400, "Pick a real round")
                    return
                label_row = DB.execute(
                    "SELECT label FROM puzzle_curation WHERE round_id = ?",
                    (round_id,),
                ).fetchone()
                if label_row and label_row["label"] == "retired":
                    self.send_error_json(400, "That round is retired")
                    return
                used = used_daily_round_ids(except_day_key=day_key)
                if round_id in used:
                    self.send_error_json(400, "That round is already locked for another day")
                    return

                daily_number = daily_number_for_key(day_key)
                timestamp = now_iso()
                DB.execute(
                    """
                    INSERT INTO daily_rounds (day_key, daily_number, round_id, created_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(day_key)
                    DO UPDATE SET round_id = excluded.round_id
                    """,
                    (day_key, daily_number, round_id, timestamp),
                )
                DB.commit()
                self.send_json(200, {"ok": True, "items": self.daily_calendar_items(8)})
                return

        self.send_error_json(400, "Pick a daily calendar action")

    def handle_curation(self, parts):
        with DB_LOCK:
            if self.command == "GET" and len(parts) == 3 and parts[2] == "public":
                rows = DB.execute(
                    """
                    SELECT round_id, label
                    FROM puzzle_curation
                    ORDER BY round_id ASC
                    """
                ).fetchall()
                self.send_json(200, {
                    "items": [
                        {"roundId": row["round_id"], "label": row["label"]}
                        for row in rows
                    ]
                })
                return

            user = self.auth_user()
            if not user or not user.get("isAdmin"):
                self.send_error_json(403, "Curation is private")
                return

            if self.command == "GET" and len(parts) == 2:
                rows = DB.execute(
                    """
                    SELECT round_id, label, note, updated_by, updated_at
                    FROM puzzle_curation
                    ORDER BY updated_at DESC, round_id ASC
                    """
                ).fetchall()
                self.send_json(200, {
                    "items": [
                        {
                            "roundId": row["round_id"],
                            "label": row["label"],
                            "note": row["note"],
                            "updatedBy": row["updated_by"],
                            "updatedAt": row["updated_at"],
                        }
                        for row in rows
                    ]
                })
                return

            if self.command == "POST" and len(parts) == 2:
                body = self.read_json()
                round_id = clean_id(body.get("roundId"), 80)
                label = str(body.get("label") or "").strip().lower()
                note = str(body.get("note") or "").strip()[:240]
                if not round_id:
                    self.send_error_json(400, "Pick a round")
                    return
                if label in {"", "unmarked"}:
                    DB.execute("DELETE FROM puzzle_curation WHERE round_id = ?", (round_id,))
                    DB.commit()
                    self.send_json(200, {"ok": True})
                    return
                if label not in CURATION_LABELS:
                    self.send_error_json(400, "Pick a curation label")
                    return

                DB.execute(
                    """
                    INSERT INTO puzzle_curation
                      (round_id, label, note, updated_by, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(round_id)
                    DO UPDATE SET label = excluded.label,
                                  note = excluded.note,
                                  updated_by = excluded.updated_by,
                                  updated_at = excluded.updated_at
                    """,
                    (round_id, label, note, user["pid"], now_iso()),
                )
                DB.commit()
                self.send_json(200, {"ok": True})
                return

        self.send_error_json(404, "Not found")

    def handle_feedback(self, parts):
        with DB_LOCK:
            if self.command == "POST" and len(parts) == 2:
                body = self.read_json()
                category = str(body.get("category") or "other").strip().lower()
                message = str(body.get("message") or "").strip()
                page = str(body.get("page") or "").strip()[:160]
                if category not in FEEDBACK_CATEGORIES:
                    self.send_error_json(400, "Pick a feedback category")
                    return
                if len(message) < 3 or len(message) > 1200:
                    self.send_error_json(400, "Feedback should be 3 to 1200 characters")
                    return

                user = self.auth_user()
                timestamp = now_iso()
                DB.execute(
                    """
                    INSERT INTO feedback
                      (user_id, player_name, category, message, page, status, created_at)
                    VALUES (?, ?, ?, ?, ?, 'open', ?)
                    """,
                    (
                        user["pid"] if user else "",
                        user["name"] if user else "",
                        category,
                        message,
                        page,
                        timestamp,
                    ),
                )
                DB.commit()
                self.send_json(200, {"ok": True})
                return

            user = self.auth_user()
            if not user or not user.get("isAdmin"):
                self.send_error_json(403, "Feedback is private")
                return

            if self.command == "GET" and len(parts) == 2:
                rows = DB.execute(
                    """
                    SELECT id, user_id, player_name, category, message, page, status, created_at, closed_at
                    FROM feedback
                    ORDER BY created_at DESC
                    LIMIT 100
                    """
                ).fetchall()
                self.send_json(200, {
                    "items": [
                        {
                            "id": row["id"],
                            "userId": row["user_id"],
                            "playerName": row["player_name"],
                            "category": row["category"],
                            "message": row["message"],
                            "page": row["page"],
                            "status": row["status"],
                            "createdAt": row["created_at"],
                            "closedAt": row["closed_at"],
                        }
                        for row in rows
                    ]
                })
                return

            if self.command == "POST" and len(parts) == 4 and parts[3] == "close":
                try:
                    feedback_id = int(parts[2])
                except ValueError:
                    self.send_error_json(400, "Invalid feedback id")
                    return
                DB.execute(
                    "DELETE FROM feedback WHERE id = ?",
                    (feedback_id,),
                )
                DB.commit()
                self.send_json(200, {"ok": True})
                return

        self.send_error_json(404, "Not found")

    def handle_report(self):
        if self.command != "POST":
            self.send_error_json(405, "Method not allowed")
            return
        body = self.read_json()
        target_type = clean_id(body.get("type"), 30)
        target_value = str(body.get("value") or "").strip()[:120]
        reason = str(body.get("reason") or "reported").strip()[:200]
        if not target_type or not target_value:
            self.send_error_json(400, "Invalid report")
            return
        with DB_LOCK:
            user = self.auth_user()
            DB.execute(
                """
                INSERT INTO reports (reporter_id, target_type, target_value, reason, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (user["pid"] if user else "", target_type, target_value, reason, now_iso()),
            )
            DB.commit()
        self.send_json(200, {"ok": True})

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
        if not valid_store_key(scope, key):
            self.send_error_json(400, "Unsupported storage key")
            return

        with DB_LOCK:
            user = self.auth_user() if scope == "player" else None
            if scope == "player":
                if not user:
                    self.send_error_json(401, "Sign in to use server storage")
                    return
                if player_id != user["pid"]:
                    self.send_error_json(403, "Storage belongs to another player")
                    return

            if self.command == "GET":
                if scope == "player" and key == "stats":
                    self.send_json(200, {"value": json.dumps(player_stats_value(player_id), ensure_ascii=False)})
                    return
                if scope == "player" and (key.startswith("daily:") or key.startswith("archive:")):
                    day_key = key.split(":", 1)[1]
                    result = daily_result_value(player_id, day_key)
                    if not result:
                        self.send_json(404, {"value": None})
                        return
                    self.send_json(200, {"value": json.dumps(result, ensure_ascii=False)})
                    return
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

            if self.command != "PUT":
                self.send_error_json(405, "Method not allowed")
                return

            if scope == "shared":
                self.send_error_json(403, "Shared storage writes use dedicated endpoints")
                return
            if scope == "player" and not writable_player_store_key(key):
                self.send_error_json(403, "That value is managed by the game server")
                return

            body = self.read_json()
            value = str(body.get("value", ""))
            value = validate_player_store_value(key, value)
            if value is None:
                self.send_error_json(400, "Invalid stored value")
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
                data = {
                    row["item"]: {"sum": row["sum"], "n": row["n"], "samples": []}
                    for row in rows
                }
                sample_rows = DB.execute(
                    """
                    SELECT item, position
                    FROM crowd_votes
                    WHERE round_id = ?
                    ORDER BY updated_at DESC
                    """,
                    (round_id,),
                ).fetchall()
                for row in sample_rows:
                    item = row["item"]
                    if item not in data:
                        data[item] = {"sum": 0, "n": 0, "samples": []}
                    if len(data[item]["samples"]) < 80:
                        data[item]["samples"].append(row["position"])
                self.send_json(200, data)
                return

            if self.command != "POST":
                self.send_error_json(405, "Method not allowed")
                return

            body = self.read_json()
            player_id = clean_id(body.get("pid"), 40)
            user = self.auth_user()
            if user:
                player_id = user["pid"]
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
        if not day_key or daily_number_for_key(day_key) < 1:
            self.send_error_json(400, "Invalid leaderboard key")
            return
        if not is_public_day_available(day_key):
            self.send_error_json(403, "That leaderboard is not unlocked yet")
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
            user = self.auth_user()
            if not user:
                self.send_error_json(401, "Sign in to join the leaderboard")
                return
            pid = user["pid"]
            body["name"] = public_player_name(user["name"])
            if not pid:
                self.send_error_json(400, "Invalid leaderboard entry")
                return

            daily_row = locked_daily_round(day_key)
            if not daily_row:
                self.send_error_json(400, "Invalid daily")
                return
            round_id = daily_row["round_id"]
            body_round_id = clean_id(body.get("roundId"), 80)
            if body_round_id and body_round_id != round_id:
                self.send_error_json(400, "Wrong daily round")
                return
            placements = clean_placements(body.get("placements"))
            result = calculate_round_result(round_id, pid, placements)
            if not result:
                self.send_error_json(400, "Invalid placements")
                return
            score = result["score"]
            squares = result["squares"]

            name = public_player_name(body.get("name"))
            if suspicious_perfect_entry(round_id, score, squares, placements):
                DB.commit()
                self.send_json(200, self.leaderboard(day_key))
                return

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
            WHERE day_key = ? AND LOWER(name) != 'guest'
            ORDER BY score DESC, created_at ASC
            LIMIT 250
            """,
            (day_key,),
        ).fetchall()
        return [
            {"pid": row["player_id"], "n": public_player_name(row["name"]), "s": row["score"], "q": clean_squares(row["squares"])}
            for row in rows
        ]

    def serve_leaderboard_page(self):
        html = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Crowdline Leaderboard</title>
<style>
body{margin:0;background:#f4f6f5;color:#26323a;font:16px/1.5 "Trebuchet MS","Segoe UI",sans-serif;display:flex;justify-content:center;padding:34px 16px}
main{width:min(520px,100%)}
a{color:#37a578;font-weight:700;text-decoration:none}
h1{font-size:38px;line-height:1;margin:0 0 6px;letter-spacing:-1px}
.sub{color:#93a0aa;margin-bottom:22px}
.row{display:grid;grid-template-columns:44px 1fr auto 42px;gap:10px;align-items:center;background:#fff;border-radius:12px;padding:10px 14px;margin-bottom:8px;box-shadow:0 2px 8px rgba(38,50,58,.08)}
.row.me{background:#effaf5;outline:2px solid rgba(55,165,120,.55);box-shadow:0 5px 18px rgba(55,165,120,.22),0 2px 8px rgba(38,50,58,.08)}
.rank{font-weight:700;color:#93a0aa}.name{font-weight:700;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.sq{font-size:13px}.score{font-weight:700;text-align:right}
.empty{background:#fff;border-radius:12px;padding:18px;color:#93a0aa;text-align:center}
</style>
</head>
<body>
<main>
<a href="/">← Play Crowdline</a>
<h1>Today's leaderboard</h1>
<div class="sub" id="sub"></div>
<div id="board" class="empty">Loading...</div>
</main>
<script>
function dateKey(d){return d.getFullYear()+"-"+String(d.getMonth()+1).padStart(2,"0")+"-"+String(d.getDate()).padStart(2,"0")}
function esc(s){return String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;")}
function color(s){return "hsl("+Math.round(Math.max(0,Math.min(100,s))*1.2)+",62%,44%)"}
function authHeaders(){try{const t=localStorage.getItem("ss:auth:token")||"";return t?{Authorization:"Bearer "+t}:{}}catch(e){return {}}}
async function load(){
  const key=dateKey(new Date());
  document.getElementById("sub").textContent=key;
  const headers=authHeaders();
  const me=headers.Authorization?await fetch("/api/auth/me",{headers}).then(r=>r.ok?r.json():null).catch(()=>null):null;
  const board=await fetch("/api/leaderboard/"+encodeURIComponent(key)).then(r=>r.json()).catch(()=>[]);
  const el=document.getElementById("board");
  if(!board.length){el.className="empty";el.textContent="No finishes yet today.";return}
  el.className="";
  el.innerHTML=board.slice(0,25).map((e,i)=>'<div class="row'+(me&&e.pid===me.pid?' me':'')+'"><span class="rank">'+(i+1)+'.</span><span class="name">'+esc(e.n)+'</span><span class="sq">'+esc(e.q)+'</span><span class="score" style="color:'+color(e.s)+'">'+e.s+'</span></div>').join("");
}
load();
</script>
</body>
</html>"""
        data = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

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
