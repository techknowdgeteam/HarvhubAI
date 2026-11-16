#!/usr/bin/env python3
"""Programme training matcher and programme analytics writer.

Programme discovery is VPS-scoped: direct programme_vps bindings for this
machine's VPS IP are selected first, then active follower programmes belonging
to those VPS owners are added. A programme is processed only when it is in that
VPS scope AND either programme.advertisement=1 or its newest
signals_provider_interest.begin_test=1.

A successful Begin Test enables live signal marking. Pending orders use their
stable first-identification trade_time; triggered/closed trades use the entry
trigger timestamp and the other PHP-compatible entry/event fallbacks. Only
configured signals on or after signal_testing_started_at are marked
programme_analytics.live_trades=1. The worker also reconciles already-saved and
cache-reused rows so no qualifying pending order is missed. All other analytics
rows remain History (live_trades=0).

The worker retains the PHP-equivalent saved-configuration matcher, exact candle
scope, analytics batching/deduplication, and programme-owned trade cleanup. It
never modifies candle_records or creates broker orders.
"""
from __future__ import annotations

import argparse
import hashlib
import gzip
import json
import logging
import math
import os
import re
import signal
import socket
import struct
import sys
import time
import traceback
import tempfile
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

BRIDGE_HOST = "127.0.0.1"
BRIDGE_PORT = 5599
BRIDGE_OWNER = "programme_training"
MAX_FRAME = 64 * 1024 * 1024
HEADER = struct.Struct(">I")
MAX_ROOT_MATCH_STATES = 2500
CANDLE_PAGE_SIZE = 500  # programme_training.php PT_PAGE_SIZE
CHART_CANDLE_LIMIT = 2000  # programme_training.php PT_DEFAULT_AMOUNT
ANALYTICS_VALIDATE_PAGE_SIZE = 250
ANALYTICS_BATCH_SIZE = 20
PARENT_ANALYTICS_READ_BATCH_SIZE = 20
CONFIRMATION_ANALYTICS_READ_BATCH_SIZE = 20
PROGRESS_TABLE = "programme_training_progress"
MATCHER_CACHE_VERSION = "programme-training-family-cache-v8-account-rules-breakeven-restricted-trades"
LOCAL_FAMILY_CACHE_SCHEMA = "programme_training_local_family_cache_v1"
LOCAL_FAMILY_CACHE_DIRNAME = ".programme_training_cache"

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
)
log = logging.getLogger("programme_training")


class BridgeError(RuntimeError):
    pass


def sql_literal(value: Any) -> str:
    """Return a MySQL/MariaDB-safe SQL literal for mysqlremote's SQL-only bridge.

    The long-running mysqlremote bridge used by brokerdetails.py documents that
    it substitutes only ONE %s placeholder per request.  This worker needs
    multi-column filters and batched inserts, so every placeholder is rendered
    locally before the SQL is sent.  Text is encoded as a UTF-8 hex literal to
    avoid quote, backslash, and SQL-injection problems.
    """
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not (value == value and abs(value) != float("inf")):
            return "NULL"
        return repr(value)
    if isinstance(value, bytes):
        raw = value
    else:
        raw = str(value).encode("utf-8")
    return "CONVERT(X'" + raw.hex() + "' USING utf8mb4)"


def interpolate_sql(sql: str, params: Optional[Sequence[Any]] = None) -> str:
    """Replace every %s placeholder locally, requiring an exact parameter count."""
    values = list(params or [])
    count = sql.count("%s")
    if count != len(values):
        if count or values:
            raise BridgeError(
                f"SQL placeholder mismatch: query has {count} %s placeholder(s), "
                f"but {len(values)} parameter(s) were supplied"
            )
        return sql
    for value in values:
        sql = sql.replace("%s", sql_literal(value), 1)
    return sql


class BridgeClient:
    """Persistent mysqlremote bridge client, matching brokercharts.py framing."""
    def __init__(self, host: str = BRIDGE_HOST, port: int = BRIDGE_PORT,
                 owner: str = BRIDGE_OWNER) -> None:
        self.host, self.port, self.owner = host, int(port), owner
        self.sock: Optional[socket.socket] = None
        self.channel_id: Optional[int] = None

    def _recv_exact(self, count: int) -> bytes:
        assert self.sock is not None
        buf = bytearray()
        while len(buf) < count:
            part = self.sock.recv(count - len(buf))
            if not part:
                raise BridgeError("mysqlremote bridge closed the socket")
            buf.extend(part)
        return bytes(buf)

    def _connect(self) -> None:
        self.sock = socket.create_connection((self.host, self.port), timeout=15)
        self.sock.settimeout(120)

    def _close(self) -> None:
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                pass
        self.sock = None

    def request(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        if len(body) > MAX_FRAME:
            raise BridgeError(f"request exceeds bridge frame limit: {len(body)} bytes")
        frame = HEADER.pack(len(body)) + body
        last_error: Optional[Exception] = None
        for attempt in range(2):
            try:
                if self.sock is None:
                    self._connect()
                assert self.sock is not None
                self.sock.sendall(frame)
                (length,) = HEADER.unpack(self._recv_exact(HEADER.size))
                if not 0 < length <= MAX_FRAME:
                    raise BridgeError(f"invalid bridge response frame length: {length}")
                result = json.loads(self._recv_exact(length).decode("utf-8"))
                if not isinstance(result, dict):
                    raise BridgeError("bridge returned a non-object response")
                return result
            except Exception as exc:
                last_error = exc
                self._close()
                if attempt:
                    break
        raise BridgeError(f"bridge request failed: {last_error}")

    def acquire(self) -> int:
        if self.channel_id is not None:
            return self.channel_id
        reply = self.request({"op": "acquire_channel", "owner": self.owner})
        if reply.get("status") != "success":
            raise BridgeError(f"acquire_channel failed: {reply.get('message')}")
        self.channel_id = int(reply["channel_id"])
        log.info("Acquired mysqlremote channel %s", self.channel_id)
        return self.channel_id

    def query(self, sql: str, params: Optional[Sequence[Any]] = None,
              timeout: float = 90.0) -> Dict[str, Any]:
        # Do not send a parameter array to mysqlremote.  Its SQL bridge replaces
        # only one %s token per request; render all values safely on this side.
        rendered_sql = interpolate_sql(sql, params)
        request: Dict[str, Any] = {"sql": rendered_sql, "timeout": timeout}
        request["channel_id"] = self.acquire()
        reply = self.request(request)
        if reply.get("status") != "success":
            message = str(reply.get("message") or "SQL operation failed")
            query_id = str(reply.get("query_id") or "not supplied")
            status = str(reply.get("status") or "unknown")
            statement_results = reply.get("statement_results") or []
            failures = []
            for item in statement_results:
                if not isinstance(item, dict) or str(item.get("status") or "").lower() != "error":
                    continue
                failure = str(item.get("message") or "statement failed").strip()
                if failure and failure not in failures:
                    failures.append(failure)
            detail = "; ".join(failures[:3]) or "no statement-level explanation was returned"
            sql_preview = re.sub(r"\s+", " ", rendered_sql).strip()[:900]
            raise BridgeError(
                f"{message}; status={status}; query_id={query_id}; detail={detail}; SQL={sql_preview}"
            )
        return reply

    def fetch_all(self, sql: str, params: Optional[Sequence[Any]] = None,
                  timeout: float = 90.0) -> List[Dict[str, Any]]:
        result = self.query(sql, params, timeout)
        rows = result.get("results") or []
        return [normalise_row(r) for r in rows if isinstance(r, dict)]

    def execute(self, sql: str, params: Optional[Sequence[Any]] = None,
                timeout: float = 90.0) -> int:
        result = self.query(sql, params, timeout)
        return int(result.get("affected_rows") or 0)

    def close(self) -> None:
        if self.channel_id is not None:
            cid, self.channel_id = self.channel_id, None
            try:
                self.request({"op": "release_channel", "channel_id": cid})
            except Exception as exc:
                log.warning("Could not release bridge channel %s: %s", cid, exc)
        self._close()


MYSQL_NULL_SENTINELS = {"null", "none", "nan"}
DEFAULT_CANDLE_TYPE = "bearish_or_bullish"
VALID_CANDLE_TYPES = {"bearish", "bullish", DEFAULT_CANDLE_TYPE}


def normalise_db_value(value: Any) -> Any:
    """Convert mysqlremote's textual SQL-NULL sentinels to Python None.

    PDO in the PHP implementation returns SQL NULL as actual ``null``. The
    mysqlremote bridge can serialise the same database value as the strings
    ``"NULL"``/``"None"`` (and, on some paths, ``"nan"``). Those strings are
    truthy in Python and JavaScript, so a PHP expression such as
    ``row.ref_candle_type || DEFAULT_CANDLE_TYPE`` would incorrectly stop
    falling back when used unchanged in Python. Canonicalise the bridge value
    at the database boundary so the rest of the restoration code sees the
    same null value PHP receives.

    Empty strings and the string ``"0"`` are deliberately preserved. JavaScript
    treats both differently: an empty string is falsy, but the string "0" is
    truthy. Python's ordinary ``or`` then reproduces the JS ``||`` behavior for
    these values.
    """
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    if isinstance(value, str) and value.strip().lower() in MYSQL_NULL_SENTINELS:
        return None
    return value


def normalise_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Normalise result keys and bridge-encoded SQL NULL values like PDO."""
    return {
        str(key).strip().lower(): normalise_db_value(value)
        for key, value in row.items()
    }


def as_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    if value is None or str(value).strip().lower() in {"", "null", "none", "nan"}:
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError, OverflowError):
        return default


def as_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    if value is None or str(value).strip().lower() in {"", "null", "none", "nan"}:
        return default
    try:
        out = float(value)
        return out if out == out and abs(out) != float("inf") else default
    except (TypeError, ValueError, OverflowError):
        return default


def is_null_db_value(value: Any) -> bool:
    """Recognise SQL NULL sentinels without treating numeric zero as NULL."""
    if value is None:
        return True
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    return isinstance(value, str) and value.strip().lower() in {"", "null", "none", "nan"}


def is_empty_db_value(value: Any) -> bool:
    """Treat SQL NULL consistently across mysqlremote/PDO serialisation forms.

    Depending on the bridge path and driver, SQL NULL can arrive as None, an
    empty string, or the literal text "NULL"/"None".  Those text values must
    never be treated as populated configuration fields.
    """
    if value is None:
        return True
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8", errors="replace")
        except Exception:
            return True
    if isinstance(value, str):
        return value.strip().lower() in {"", "null", "none", "nan", "false", "0"}
    if isinstance(value, (int, float)):
        return value == 0
    return False


def has_db_value(value: Any) -> bool:
    return not is_empty_db_value(value)


def truthy(value: Any, default: bool = False) -> bool:
    if is_empty_db_value(value):
        return default if value is None else False
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def js_truthy(value: Any) -> bool:
    """JavaScript Boolean(value), used when mirroring ptRestoreSavedRows()."""
    if value is None or value is False:
        return False
    if isinstance(value, (int, float)) and value == 0:
        return False
    if isinstance(value, str) and value == "":
        return False
    return True


def js_truthy_db(value: Any) -> bool:
    """Apply JS truthiness after normalising mysqlremote's SQL-NULL sentinels.

    PDO supplies actual null for SQL NULL, while the bridge can serialise the same
    value as None/NULL/empty text. A literal string "0" is intentionally retained
    as JS-truthy because Boolean("0") is true in the PHP page's JavaScript.
    """
    if is_null_db_value(value):
        return False
    return js_truthy(value)


def parse_list(raw: Any) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, (list, tuple)):
        values = list(raw)
    else:
        text = str(raw).strip()
        if not text:
            return []
        try:
            decoded = json.loads(text)
            if isinstance(decoded, list):
                values = decoded
            elif isinstance(decoded, dict):
                values = list(decoded.values())
            else:
                values = [text]
        except (ValueError, TypeError):
            values = text.replace("[", "").replace("]", "").replace('"', "").replace("'", "").split(",")
    out: List[str] = []
    for v in values:
        if isinstance(v, (dict, list)):
            continue
        item = str(v).strip()
        if item and item not in out:
            out.append(item)
    return out


def tf_milliseconds(tf: str) -> Optional[int]:
    match = re.fullmatch(r"\s*(\d+)\s*([mhdw])\s*", str(tf or "").lower())
    if not match:
        aliases = {"mn1": 60_000, "mn5": 300_000, "mn15": 900_000,
                   "mn30": 1_800_000, "h1": 3_600_000, "h4": 14_400_000,
                   "d1": 86_400_000, "w1": 604_800_000}
        return aliases.get(str(tf or "").lower())
    number, unit = int(match.group(1)), match.group(2)
    return number * {"m": 60_000, "h": 3_600_000, "d": 86_400_000, "w": 604_800_000}[unit]


def parse_datetime(value: Any) -> Optional[datetime]:
    if value is None or str(value).strip() == "":
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        raw = str(value).strip().replace("T", " ")
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(raw)
        except ValueError:
            for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                try:
                    dt = datetime.strptime(raw[:26], fmt)
                    break
                except ValueError:
                    dt = None  # type: ignore[assignment]
            if dt is None:
                return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def dt_text(value: Any) -> Optional[str]:
    dt = parse_datetime(value)
    return dt.strftime("%Y-%m-%d %H:%M:%S") if dt else None


def safe_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


def decode_hex_text_or_plain(row: Dict[str, Any], hex_field: str, plain_field: str,
                             default: str = "") -> str:
    """Decode a text column returned as HEX(), while supporting legacy test/fake rows.

    mysqlremote serialises SQL result rows through PHP JSON. Selecting large or
    malformed UTF-8 TEXT/MEDIUMTEXT values directly can make that bridge fail
    even when LIMIT 1 is used. HEX() converts the database bytes to ASCII before
    the bridge serialises the result; this helper restores the original UTF-8 text.
    """
    encoded = row.get(hex_field)
    if encoded not in (None, ""):
        raw_hex = str(encoded).strip()
        if raw_hex.startswith("0x"):
            raw_hex = raw_hex[2:]
        try:
            return bytes.fromhex(raw_hex).decode("utf-8")
        except (ValueError, UnicodeDecodeError) as exc:
            raise BridgeError(f"Could not decode HEX() database field {hex_field}: {exc}") from exc
    plain = row.get(plain_field)
    if plain is None:
        return default
    return plain.decode("utf-8") if isinstance(plain, bytes) else str(plain)


def get_columns(db: BridgeClient, table: str) -> List[str]:
    if not re.fullmatch(r"[A-Za-z0-9_]+", table):
        raise ValueError("Invalid table identifier")
    rows = db.fetch_all(f"SHOW COLUMNS FROM `{table}`")
    return [str(r.get("field") or r.get("Field") or "") for r in rows if r.get("field") or r.get("Field")]


def table_exists(db: BridgeClient, table: str) -> bool:
    # information_schema is more consistently supported by mysqlremote than
    # SHOW TABLES LIKE (whose result-column name varies by server/version).
    rows = db.fetch_all(
        "SELECT `table_name` FROM `information_schema`.`tables` "
        "WHERE `table_schema`=DATABASE() AND `table_name`=%s LIMIT 1",
        [table], timeout=30)
    return bool(rows)


DEFAULT_TRADES_SETUP_PER_GROUP = 100


def load_trades_setup_per_group(db: BridgeClient) -> int:
    """Read the same group-size setting used by programme_training.php.

    The chart reads server_account.id=1.trades_setup_per_group. Keep the default
    at 100 only when the table/value is not available, exactly as the PHP page does.
    """
    try:
        if not table_exists(db, "server_account"):
            return DEFAULT_TRADES_SETUP_PER_GROUP
        rows = db.fetch_all(
            "SELECT `trades_setup_per_group` FROM `server_account` WHERE `id`=1 LIMIT 1",
            timeout=30,
        )
        configured = as_int(rows[0].get("trades_setup_per_group"), 0) if rows else 0
        # Mirror PHP exactly: only a positive server setting overrides the default.
        return configured if configured is not None and configured > 0 else DEFAULT_TRADES_SETUP_PER_GROUP
    except Exception as exc:
        log.warning(
            "Could not read server_account.trades_setup_per_group; using default %s: %s",
            DEFAULT_TRADES_SETUP_PER_GROUP, str(exc)[:600],
        )
        return DEFAULT_TRADES_SETUP_PER_GROUP


def split_family_ordinal(ordinal: Any, setup_group_limit: int) -> Tuple[int, int]:
    """Convert an internal 1-based family ordinal to (group_id, group-local number)."""
    limit = max(1, int(setup_group_limit or DEFAULT_TRADES_SETUP_PER_GROUP))
    number = max(1, as_int(ordinal, 1) or 1)
    return ((number - 1) // limit) + 1, ((number - 1) % limit) + 1


def family_ordinal_from_group(family_number: Any, setup_group_id: Any,
                              setup_group_limit: int) -> Optional[int]:
    """Restore the internal ordinal from a database/cache group-local identity."""
    number = as_int(family_number)
    group_id = as_int(setup_group_id, 1) or 1
    if number is None or number <= 0:
        return None
    limit = max(1, int(setup_group_limit or DEFAULT_TRADES_SETUP_PER_GROUP))
    return ((max(1, group_id) - 1) * limit) + number


def apply_setup_group_identity(family: Dict[str, Any], setup_group_limit: int) -> Dict[str, Any]:
    """Attach chart-equivalent group metadata without changing global family order.

    familyNumber remains the internal/global ordinal. groupFamilyNumber is the
    number persisted to programme_analytics.family_number; setupGroupId is stored
    separately in programme_analytics.setup_group_id.
    """
    limit = max(1, int(setup_group_limit or DEFAULT_TRADES_SETUP_PER_GROUP))
    ordinal = as_int(family.get("familyOrdinal"))
    if ordinal is None or ordinal <= 0:
        ordinal = as_int(family.get("familyNumber"), 0) or 0
    if ordinal <= 0:
        raise ValueError("A matched family must have a positive global family ordinal")
    group_id, group_number = split_family_ordinal(ordinal, limit)
    family["familyOrdinal"] = ordinal
    family["familyNumber"] = ordinal
    family["setupGroupId"] = group_id
    family["groupFamilyNumber"] = group_number
    family["_setupGroupLimit"] = limit
    return family


def family_group_identity(family: Dict[str, Any]) -> Tuple[int, int, int]:
    """Return (global ordinal, setup_group_id, group-local family_number)."""
    limit = max(1, as_int(family.get("_setupGroupLimit"), DEFAULT_TRADES_SETUP_PER_GROUP)
                or DEFAULT_TRADES_SETUP_PER_GROUP)
    apply_setup_group_identity(family, limit)
    return (
        int(family["familyOrdinal"]),
        int(family["setupGroupId"]),
        int(family["groupFamilyNumber"]),
    )


# --------------------------- Candle maths / matching ---------------------------

def candle_type(candle: Dict[str, Any]) -> str:
    op, cl = as_float(candle.get("open")), as_float(candle.get("close"))
    if op is None or cl is None:
        return "bearish_or_bullish"
    return "bullish" if cl > op else "bearish" if cl < op else "bearish_or_bullish"


def normalize_candle_type(value: Any) -> str:
    """Mirror programme_training.php normalizeCandleType().

    The PHP UI accepts only the three configured candle types and replaces an
    empty or invalid restored value with DEFAULT_CANDLE_TYPE. This also protects
    matcher entry points against a SQL NULL sentinel that slipped past a caller.
    """
    value = normalise_db_value(value)
    if isinstance(value, str) and value in VALID_CANDLE_TYPES:
        return value
    return DEFAULT_CANDLE_TYPE


def type_matches(candle: Dict[str, Any], wanted: Any) -> bool:
    """Mirror PHP ptTypeMatches(): blank/default accepts either type; other values are exact."""
    wanted = normalise_db_value(wanted)
    if not js_truthy_db(wanted):
        return True
    if wanted == DEFAULT_CANDLE_TYPE:
        return True
    return candle_type(candle) == wanted


def price(candle: Dict[str, Any], level: Any = "high") -> Optional[float]:
    op, cl = as_float(candle.get("open")), as_float(candle.get("close"))
    hi, lo = as_float(candle.get("high")), as_float(candle.get("low"))
    key = str(level or "high").lower().strip()
    if key == "open": return op
    if key == "close": return cl
    if key == "high": return hi
    if key == "low": return lo
    if key == "candle_center": return (hi + lo) / 2 if hi is not None and lo is not None else None
    if key == "body_center": return (op + cl) / 2 if op is not None and cl is not None else None
    if key == "high_wick_center": return (hi + max(op, cl)) / 2 if hi is not None and op is not None and cl is not None else None
    if key == "low_wick_center": return (lo + min(op, cl)) / 2 if lo is not None and op is not None and cl is not None else None
    return hi


def compare(a: Optional[float], operator: str, b: Optional[float]) -> bool:
    if a is None or b is None:
        return False
    return {
        "<": lambda: a < b, ">": lambda: a > b, "=": lambda: a == b,
        "==": lambda: a == b, "<=": lambda: a <= b, "=<": lambda: a <= b,
        ">=": lambda: a >= b, "<>": lambda: a != b, "!=": lambda: a != b,
    }.get(operator, lambda: a < b)()


VALID_PRICE_LEVELS = {"high", "low", "open", "close", "candle_center", "body_center", "high_wick_center", "low_wick_center"}


def price_parts(value: Any) -> List[str]:
    parts = [p.strip().lower() for p in re.split(r"\s+or\s+", str(value or "high"), flags=re.I)]
    kept = [p for p in parts if p in VALID_PRICE_LEVELS]
    return kept or ["high"]


def compare_levels(main: Dict[str, Any], main_level: Any, operator: str,
                   ref: Dict[str, Any], ref_level: Any) -> Optional[Dict[str, Any]]:
    for ml in price_parts(main_level):
        mv = price(main, ml)
        for rl in price_parts(ref_level):
            rv = price(ref, rl)
            if compare(mv, operator, rv):
                return {"matched": True, "mainLevel": ml, "referenceLevel": rl,
                        "mainValue": mv, "referenceValue": rv}
    return None


def any_first(*values: Any) -> bool:
    return any(str(v or "").strip().lower() == "any_first_candle" for v in values)


def effective_point(main: Dict[str, Any], ref: Dict[str, Any]) -> str:
    if any_first(main.get("offsetPosPoint"), ref.get("offsetPosPoint")):
        return "any_first_candle"
    return str(main.get("offsetPosPoint") or ref.get("offsetPosPoint") or "all").lower()


def offset_candidates(index: int, direction: Any, distance: Any, point: Any, length: int) -> List[int]:
    d = str(direction or "right").lower()
    dirs = [-1, 1] if d in {"left_and_right", "left and right", "both", "all"} else ([-1] if d == "left" else [1])
    n = as_float(distance, 1)
    if n is None:
        n = 1
    count = 0 if n == 0 else max(1, int(n))
    out: List[int] = []
    for sign in dirs:
        if count == 0:
            candidates = [index]
        elif str(point or "").lower() == "any_first_candle":
            candidates = range(index + sign, length if sign > 0 else -1, sign)
        else:
            candidates = [index + sign * count]
        for candidate in candidates:
            if 0 <= candidate < length and candidate not in out:
                out.append(candidate)
    return out


def resolve_new_group_reference(main_index: int, main: Dict[str, Any], ref: Dict[str, Any],
                                candles: Sequence[Dict[str, Any]],
                                diagnostics: Optional[Dict[str, Any]] = None) -> Optional[Tuple[int, Dict[str, Any]]]:
    point_type = str(ref.get("offsetPosPoint") or "all").lower()
    distance = max(1, as_int(ref.get("offsetPosition"), 1) or 1)
    direction = 1 if str(ref.get("position") or "left").lower() == "right" else -1
    if point_type == "any_first_candle":
        indices = range(main_index + direction, len(candles) if direction > 0 else -1, direction)
    elif point_type == "fixed":
        indices = [main_index + direction * distance]
    else:
        indices = range(main_index + direction, main_index + direction * (distance + 1), direction)
    valid: List[Tuple[int, Dict[str, Any]]] = []
    for idx in indices:
        if diagnostics is not None:
            diagnostics["reference_candidate_checks"] = diagnostics.get("reference_candidate_checks", 0) + 1
        if idx < 0 or idx >= len(candles):
            if point_type == "all":
                if diagnostics is not None:
                    diagnostics["reference_out_of_bounds"] = diagnostics.get("reference_out_of_bounds", 0) + 1
                return None
            if point_type == "fixed":
                if diagnostics is not None:
                    diagnostics["reference_out_of_bounds"] = diagnostics.get("reference_out_of_bounds", 0) + 1
                return None
            continue
        candle = candles[idx]
        if not type_matches(candle, ref.get("refCandleType")):
            if diagnostics is not None:
                diagnostics["reference_type_rejections"] = diagnostics.get("reference_type_rejections", 0) + 1
            if point_type == "all": return None
            continue
        if diagnostics is not None:
            diagnostics["reference_type_matches"] = diagnostics.get("reference_type_matches", 0) + 1
            diagnostics["price_comparisons"] = diagnostics.get("price_comparisons", 0) + 1
        lm = compare_levels(candles[main_index], main.get("priceLevel") or "high",
                            main.get("operator") or "<", candle, ref.get("priceLevel") or "high")
        if not lm:
            if diagnostics is not None:
                diagnostics["price_rejections"] = diagnostics.get("price_rejections", 0) + 1
            if point_type == "all": return None
            continue
        if diagnostics is not None:
            diagnostics["price_matches"] = diagnostics.get("price_matches", 0) + 1
        valid.append((idx, lm))
        if point_type in {"fixed", "random", "any_first_candle"}:
            break
    if point_type == "all":
        return valid[-1] if len(valid) == distance else None
    return valid[0] if valid else None


def family_identity_signature(root_id: int, timeframe: str, confirmation_root_id: int,
                              family_map: Dict[str, int], candles: Sequence[Dict[str, Any]]) -> str:
    pairs = []
    for identity in sorted(family_map, key=lambda x: str(x)):
        idx = as_int(family_map[identity], -1) or 0
        candle_id = candles[idx].get("id") if 0 <= idx < len(candles) else ""
        pairs.append(f"{identity}={candle_id}")
    return f"root={root_id};tf={timeframe.lower()};cr={confirmation_root_id};map={','.join(pairs)}"


# --------------------------- Saved configuration -----------------------------
def _row_has_reference_details(row: Dict[str, Any]) -> bool:
    """Legacy heuristic retained for diagnostics; PHP uses reference_id to route rows."""
    return any(has_db_value(row.get(field)) for field in (
        "reference_candle_name", "reference_mode", "ref_candle_type"
    )) or as_int(row.get("reference_id")) is not None


def is_drawing_only_row(row: Dict[str, Any]) -> bool:
    """Mirror ptRestoreSavedRows(): any truthy draw_from/draw_to makes a drawing row.

    PHP processes root-level metadata for the row, appends its drawing, and returns
    before creating/updating a matcher tree. It does this even when the same row also
    contains candle IDs/names. The previous Python heuristic incorrectly kept such
    mixed rows as matcher rows, which could create families the visible chart omitted.
    """
    return js_truthy_db(row.get("draw_from")) or js_truthy_db(row.get("draw_to"))


def _php_db_bool(value: Any) -> bool:
    """Exact ptDbBool(): only true, integer 1, or string '1' is enabled."""
    if value is True:
        return True
    if isinstance(value, bool):
        return False
    if isinstance(value, (int, float)):
        return value == 1
    return isinstance(value, str) and value == "1"


def _php_js_or(*values: Any) -> Any:
    """Return the first value JavaScript's `a || b` would select."""
    for value in values:
        if js_truthy_db(value):
            return value
    return values[-1] if values else None


def _php_parse_int_or(value: Any, fallback: Any) -> Any:
    """Mirror parseInt(value, 10) || fallback for restored IDs."""
    parsed = as_int(value)
    return parsed if parsed else fallback


def _php_parse_saved_timeframes(value: Any) -> List[str]:
    """Mirror ptParseSavedTimeframes(): preserve case and only accept JSON arrays."""
    raw = str(value or "").strip()
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        parsed = None
    if isinstance(parsed, list):
        values = parsed
    else:
        values = [part.strip() for part in raw.split(",")]
    result: List[str] = []
    for item in values:
        if item is None:
            text = "null"
        elif item is True:
            text = "true"
        elif item is False:
            text = "false"
        elif isinstance(item, dict):
            text = "[object Object]"
        elif isinstance(item, list):
            text = ",".join("" if part is None else str(part) for part in item)
        else:
            text = str(item)
        if text:
            result.append(text)
    return result


def _php_number(value: Any) -> Optional[float]:
    """Mirror Number(value) for restored numeric settings, including Number('')=0."""
    if isinstance(value, str) and value.strip() == "":
        return 0.0
    return as_float(value)


def _php_confirmation_root_settings(rows: List[Dict[str, Any]], rid: int,
                                    default_name_index: int = 1) -> Dict[str, Any]:
    """Replay the confirmation-root assignments in ptRestoreSavedRows() row by row.

    The browser creates the confirmation root on its first row and then mutates shared
    fields for every subsequent row. Most text/settings use JavaScript `||` and retain
    the previous non-empty value. useForTrades is different: ptDbBool() assigns it
    directly on every row, so the final row wins (including a blank/zero value).
    """
    default_name = f"Confirmation Root {default_name_index}"
    state: Dict[str, Any] = {
        "id": rid,
        "name": "",
        "timeframe": "",
        "timeframes": [],
        "expanded": False,
        "enabled": True,
        "trees": [],
        "useForTrades": False,
        "entryFrom": "",
        "orderType": "buy",
        "entryTriggerAfter": "",
        "entryPriceLevel": "high",
        "stoplossFrom": "",
        "stoplossPriceLevel": "high",
        "takeProfitFrom": "",
        "takeProfitType": "risk_reward",
        "takeProfitPriceLevel": "high",
        "riskReward": None,
        "anchorCandleId": None,
        "anchorCandleName": "",
        "anchorTime": "open_time",
        "isConfirmationRoot": True,
        "drawings": [],
    }

    for row in rows:
        state["enabled"] = True
        state["name"] = str(_php_js_or(row.get("root_name"), state.get("name"), default_name) or default_name)
        timeframe_value = _php_js_or(row.get("confirmation_timeframes"), row.get("timeframe"), "")
        state["timeframes"] = _php_parse_saved_timeframes(timeframe_value)
        state["timeframe"] = ",".join(state["timeframes"])
        state["anchorCandleId"] = as_int(_php_js_or(
            row.get("confirmation_anchor_candle_id"), state.get("anchorCandleId"), ""
        ))
        state["anchorCandleName"] = str(_php_js_or(
            row.get("confirmation_anchor_candle_name"), state.get("anchorCandleName"), ""
        ) or "")
        state["anchorTime"] = str(_php_js_or(
            row.get("confirmation_anchor_time"), state.get("anchorTime"), "open_time"
        ) or "open_time")

        # This direct assignment is deliberately NOT a first-row/template merge.
        state["useForTrades"] = _php_db_bool(row.get("use_for_trade"))

        state["entryFrom"] = str(_php_js_or(
            row.get("entry_at"), row.get("entry_from"), state.get("entryFrom"), ""
        ) or "")
        state["orderType"] = str(_php_js_or(
            row.get("order_type"), row.get("entry_order_type"), state.get("orderType"), "buy"
        ) or "buy")
        state["entryTriggerAfter"] = str(_php_js_or(
            row.get("entry_trigger_after"), state.get("entryTriggerAfter"), ""
        ) or "")
        state["entryPriceLevel"] = str(_php_js_or(
            row.get("entry_price_level"), state.get("entryPriceLevel"), "high"
        ) or "high")
        state["stoplossFrom"] = str(_php_js_or(
            row.get("stoploss_at"), row.get("stoploss_from"), state.get("stoplossFrom"), ""
        ) or "")
        state["stoplossPriceLevel"] = str(_php_js_or(
            row.get("stoploss_price_level"), state.get("stoplossPriceLevel"), "high"
        ) or "high")
        state["takeProfitFrom"] = str(_php_js_or(
            row.get("take_profit_at"), row.get("take_profit_from"), state.get("takeProfitFrom"), ""
        ) or "")
        requested_tp_type = re.sub(
            r"\s+", "_", str(row.get("take_profit_type") or "").lower()
        )
        state["takeProfitType"] = (
            "price_levels" if requested_tp_type == "price_levels"
            else str(state.get("takeProfitType") or "risk_reward")
        )
        state["takeProfitPriceLevel"] = str(_php_js_or(
            row.get("take_profit_price_level"), state.get("takeProfitPriceLevel"), "high"
        ) or "high")
        if row.get("risk_reward") is not None:
            # JS Number('') is 0; NULL alone preserves the previous property.
            raw_rr = row.get("risk_reward")
            if isinstance(raw_rr, str) and raw_rr.strip() == "":
                state["riskReward"] = 0.0
            else:
                state["riskReward"] = _php_number(raw_rr)

    return state


def assign_php_runtime_candle_ids(parent_rows: List[Dict[str, Any]],
                                  confirmation_rows: List[Dict[str, Any]]) -> None:
    """Reproduce PT_CFG.nextCandleId allocation from ptRestoreSavedRows().

    programme_training.php creates a main ID and a separate reference ID when
    restoring each tree. A new-offset reference can have reference_id=NULL in
    MySQL; the browser still allocates a distinct runtime ID for that candle.
    Reusing main_id for that reference collapses two identities (for example,
    Msnr RA and Msnr RB) and breaks later-tree links and confirmation anchors.

    The input order is deliberately parent rows first, then confirmation rows,
    matching JavaScript's rows.concat(confirmationRows).
    """
    next_candle_id = 1
    allocations: Dict[Tuple[str, int, int, int], Dict[str, int]] = {}
    ordered_rows: List[Tuple[Dict[str, Any], bool]] = (
        [(row, False) for row in parent_rows] +
        [(row, True) for row in confirmation_rows]
    )

    for row, is_confirmation in ordered_rows:
        # ptRestoreSavedRows() handles drawing rows before it creates/allocates a tree.
        if is_drawing_only_row(row):
            continue
        rid = (
            _php_parse_int_or(row.get("parent_root_id"), 1)
            if is_confirmation else _php_parse_int_or(row.get("root_id"), 1)
        )
        confirmation_id = (
            _php_parse_int_or(row.get("confirmation_root_id"), rid * 1000 + 1)
            if is_confirmation else 0
        )
        tree_id = _php_parse_int_or(row.get("tree_id"), 1)
        tree_key = ("confirmation" if is_confirmation else "root", rid, confirmation_id, tree_id)

        if tree_key not in allocations:
            saved_main_id = as_int(row.get("main_id"))
            if saved_main_id:
                main_id = saved_main_id
            else:
                main_id = next_candle_id
                next_candle_id += 1

            saved_reference_id = as_int(row.get("reference_id"))
            if saved_reference_id:
                reference_id = saved_reference_id
            else:
                reference_id = next_candle_id
                next_candle_id += 1

            next_candle_id = max(next_candle_id, main_id + 1, reference_id + 1)
            allocations[tree_key] = {"main_id": main_id, "reference_id": reference_id}

        # JavaScript updates the reference identity when it later encounters a
        # populated reference row. A NULL new-offset reference keeps the ID
        # allocated when the tree was first encountered.
        if as_int(row.get("reference_id")) and has_db_value(row.get("reference_candle_name")):
            allocations[tree_key]["reference_id"] = as_int(row.get("reference_id"), allocations[tree_key]["reference_id"]) or allocations[tree_key]["reference_id"]

    for row, is_confirmation in ordered_rows:
        if is_drawing_only_row(row):
            continue
        rid = (
            _php_parse_int_or(row.get("parent_root_id"), 1)
            if is_confirmation else _php_parse_int_or(row.get("root_id"), 1)
        )
        confirmation_id = (
            _php_parse_int_or(row.get("confirmation_root_id"), rid * 1000 + 1)
            if is_confirmation else 0
        )
        tree_id = _php_parse_int_or(row.get("tree_id"), 1)
        tree_key = ("confirmation" if is_confirmation else "root", rid, confirmation_id, tree_id)
        allocation = allocations[tree_key]
        row["__php_runtime_main_id"] = allocation["main_id"]
        row["__php_runtime_reference_id"] = allocation["reference_id"]
        row["__is_confirmation"] = is_confirmation


def make_tree(group: List[Dict[str, Any]], tree_index: int) -> Dict[str, Any]:
    """Replay the per-row tree restoration performed by PHP ptRestoreSavedRows().

    In PHP, a row is a reference row only when reference_id is neither NULL nor an
    empty string AND reference_candle_name is truthy. Every other row updates the
    main candle. This intentionally follows the visible chart's rule even for legacy
    new-offset rows whose reference_id is NULL or whose reference_mode is malformed.
    """
    group = [normalise_row(row) for row in group]
    group = sorted(group, key=lambda r: (as_int(r.get("config_id"), 0) or 0))
    if not group:
        raise ValueError("Cannot restore an empty programme-training tree")

    first = group[0]
    main_id = as_int(first.get("__php_runtime_main_id"), as_int(first.get("main_id"), 0)) or 0
    reference_id = as_int(first.get("__php_runtime_reference_id"), as_int(first.get("reference_id"), 0)) or 0
    first_position = str(first.get("position") or "")
    if tree_index > 0:
        initial_main_position = "right" if first_position == "right" else "left"
    else:
        initial_main_position = (
            first_position if first.get("__is_confirmation") and first_position in {"left", "right", "left_and_right"}
            else "left_and_right"
        )

    main = {
        "id": main_id,
        "name": str(first.get("main_candle_name") or ""),
        "candleText": str(first.get("candle_text") or ""),
        "textPosition": first.get("text_position") or "high",
        "textColor": first.get("text_color") or "purple",
        "mainCandleType": _php_js_or(first.get("main_candle_type"), DEFAULT_CANDLE_TYPE),
        "offsetCandle": first.get("offset_candle") or "",
        "position": initial_main_position,
        "offsetTo": _php_js_or(first.get("offset_to"), None),
        "offsetToName": first.get("offset_candle_name") or "",
        "offsetToKind": "",
        "offsetPosition": as_int(first.get("offset_position")),
        "offsetPosPoint": first.get("offset_position_point") or "all",
        "newGroupOffsetTo": as_int(first.get("main_new_group_offset_to")),
        "newGroupOffsetToName": "",
        "newGroupOffsetPosPoint": first.get("main_new_group_offset_pos_point") or "all",
        "newGroupDirection": first.get("main_new_group_direction") or "right",
        "priceLevel": first.get("price_level") or "high",
        "operator": first.get("operator") or "<",
        "selectedExistingId": as_int(first.get("offset_to")) if js_truthy_db(first.get("offset_to")) else None,
    }
    ref = {
        "id": reference_id,
        "name": str(first.get("reference_candle_name") or ""),
        "candleText": "",
        "textPosition": first.get("text_position") or "high",
        "textColor": first.get("text_color") or "purple",
        "refCandleType": _php_js_or(first.get("ref_candle_type"), DEFAULT_CANDLE_TYPE),
        "position": "right" if first.get("position") == "right" else "left",
        "offsetPosition": as_int(first.get("offset_position"), 1),
        "offsetPosPoint": first.get("offset_position_point") or "all",
        "priceLevel": first.get("price_level") or "high",
        "referenceMode": "existing",
        "selectedExistingId": as_int(first.get("reference_id")) if js_truthy_db(first.get("reference_id")) else None,
        "existingCandleId": as_int(first.get("reference_id")) if js_truthy_db(first.get("reference_id")) else None,
    }

    tree: Dict[str, Any] = {
        "id": as_int(first.get("tree_id"), 1) or 1,
        "regionSearchMode": first.get("region_search_mode") or "just_offset_existing_candles",
        "regionFromCandleId": as_int(first.get("region_from_candle_id")) if js_truthy_db(first.get("region_from_candle_id")) else None,
        "regionFromCandleName": first.get("region_from_candle_name") or "",
        "regionToCandleId": as_int(first.get("region_to_candle_id")) if js_truthy_db(first.get("region_to_candle_id")) else None,
        "regionToCandleName": first.get("region_to_candle_name") or "",
        "groupNecessity": first.get("group_necessity") or "required",
        "ifGroupFound": first.get("if_group_found") or "identify_and_trade",
        "main": main,
        "reference": ref,
        "offset": None,
    }

    for row in group:
        reference_id_value = row.get("reference_id")
        # PHP sets the tree's group-selection mode before its main/reference branch,
        # and only when reference_id is exactly NULL or an empty string.
        if reference_id_value is None or reference_id_value == "":
            region_mode = str(row.get("region_search_mode") or "")
            if tree_index > 0 and region_mode == "existing_candles_perimeter":
                tree["groupSelectionType"] = "existing_candles_window"
                tree["regionSearchMode"] = "existing_candles_perimeter"
            elif tree_index > 0 and (
                js_truthy_db(row.get("main_new_group_offset_to"))
                or not js_truthy_db(row.get("offset_to"))
            ):
                tree["groupSelectionType"] = "new_group"
            else:
                tree["groupSelectionType"] = "existing_group"

        is_reference_row = (
            reference_id_value is not None
            and reference_id_value != ""
            and js_truthy_db(row.get("reference_candle_name"))
        )

        if is_reference_row:
            ref["id"] = _php_parse_int_or(row.get("reference_id"), ref["id"])
            ref["name"] = str(row.get("reference_candle_name") or "")
            ref["refCandleType"] = _php_js_or(
                row.get("ref_candle_type"), row.get("offset_candle"), DEFAULT_CANDLE_TYPE
            )
            ref["offsetCandle"] = row.get("offset_candle") or row.get("ref_candle_type") or DEFAULT_CANDLE_TYPE
            mode_value = row.get("reference_mode")
            saved_is_new_offset = (
                str(mode_value) == "new_offset" if js_truthy_db(mode_value)
                else (js_truthy_db(row.get("offset_candle")) and js_truthy_db(row.get("offset_to")))
            )
            ref["referenceMode"] = "new_offset" if saved_is_new_offset else "existing"
            selected_reference = as_int(row.get("reference_id")) if not saved_is_new_offset else None
            ref["selectedExistingId"] = selected_reference
            ref["existingCandleId"] = selected_reference
            ref["position"] = "right" if row.get("position") == "right" else "left"
            ref["offsetPosition"] = as_int(row.get("offset_position"), 1)
            ref["offsetPosPoint"] = row.get("offset_position_point") or "all"
            ref["priceLevel"] = row.get("price_level") or "high"

            # PHP preserves an earlier non-empty annotation when a later duplicate
            # reference row contains NULL/blank candle_text.
            if row.get("candle_text") is not None and str(row.get("candle_text")).strip() != "":
                ref["candleText"] = str(row.get("candle_text"))
            ref["textPosition"] = row.get("text_position") or ref.get("textPosition") or "high"
            ref["textColor"] = row.get("text_color") or ref.get("textColor") or "purple"

            if row.get("offset_to") is not None and row.get("offset_to") != "":
                ref["offsetTo"] = _php_parse_int_or(row.get("offset_to"), main.get("id"))
                ref["offsetToName"] = row.get("offset_candle_name") or main.get("name") or ""

            if saved_is_new_offset:
                ref["offsetTo"] = _php_parse_int_or(row.get("offset_to"), main.get("id"))
                ref["offsetToName"] = row.get("offset_candle_name") or main.get("name") or ""
                tree["offset"] = ref
        else:
            # This branch intentionally also receives any populated reference-name
            # row whose reference_id is NULL/empty, exactly as the PHP page does.
            main["id"] = _php_parse_int_or(row.get("main_id"), main["id"])
            main["name"] = str(row.get("main_candle_name") or "")
            main["candleText"] = str(row.get("candle_text") or main.get("candleText") or "")
            main["textPosition"] = row.get("text_position") or main.get("textPosition") or "high"
            main["textColor"] = row.get("text_color") or main.get("textColor") or "purple"
            main["mainCandleType"] = _php_js_or(row.get("main_candle_type"), DEFAULT_CANDLE_TYPE)
            main["offsetTo"] = _php_js_or(row.get("offset_to"), None)
            main["selectedExistingId"] = as_int(row.get("offset_to")) if js_truthy_db(row.get("offset_to")) else None
            main["offsetToName"] = row.get("offset_candle_name") or ""
            main["offsetCandle"] = row.get("offset_candle") or ""
            main["position"] = row.get("position") or "all"
            main["offsetPosition"] = as_int(row.get("offset_position")) if row.get("offset_position") is not None else None
            main["offsetPosPoint"] = row.get("offset_position_point") or "all"
            main["priceLevel"] = row.get("price_level") or "high"
            main["operator"] = row.get("operator") or "<"

    if "groupSelectionType" not in tree:
        tree["groupSelectionType"] = "existing_group"

    return tree



def group_rows(rows: List[Dict[str, Any]], confirmation: bool = False) -> List[Dict[str, Any]]:
    """Group saved rows while following the PHP row-restore ordering and overwrites."""
    root_field = "confirmation_root_id" if confirmation else "root_id"
    groups: Dict[Any, List[Dict[str, Any]]] = {}
    skipped_drawings = 0
    missing_root = 0
    for raw_row in rows:
        row = normalise_row(raw_row)
        if is_drawing_only_row(row):
            skipped_drawings += 1
        # ptRestoreSavedRows() uses parseInt(value, 10) || 1 for parent IDs.
        # Confirmation roots additionally fall back to (parentRootId * 1000 + 1).
        if confirmation:
            parent_id = _php_parse_int_or(row.get("parent_root_id"), 1)
            rid = _php_parse_int_or(row.get("confirmation_root_id"), parent_id * 1000 + 1)
            group_key = (parent_id, rid)
        else:
            parent_id = 0
            rid = _php_parse_int_or(row.get("root_id"), 1)
            group_key = rid
        # Keep drawing rows in the root's restore stream. PHP applies the shared
        # confirmation settings before it branches to drawing handling and returns.
        groups.setdefault(group_key, []).append(row)

    if rows and not groups:
        sample = []
        for row in rows[:5]:
            sample.append(
                f"config={row.get('config_id') or 'n/a'}, root={row.get(root_field) or 'missing'}, "
                f"tree={row.get('tree_id') or 'n/a'}, main={row.get('main_candle_name') or 'n/a'}, "
                f"reference={row.get('reference_candle_name') or 'n/a'}"
            )
        log.warning(
            "Configuration rows could not be grouped (confirmation=%s): total=%s, "
            "missing_%s=%s, drawing_rows=%s. Sample rows: %s",
            confirmation, len(rows), root_field, missing_root, skipped_drawings,
            " | ".join(sample) if sample else "none"
        )

    roots: List[Dict[str, Any]] = []
    confirmation_default_name_counts: Dict[int, int] = {}
    for group_key, root_rows in sorted(groups.items()):
        if confirmation:
            parent_id, rid = group_key
        else:
            parent_id, rid = 0, group_key
        root_rows = sorted(root_rows, key=lambda r: (
            as_int(r.get("tree_id"), 0) or 0,
            as_int(r.get("config_id"), 0) or 0,
        ))
        matcher_rows = [row for row in root_rows if not is_drawing_only_row(row)]
        tree_ids = sorted({as_int(r.get("tree_id"), 1) or 1 for r in matcher_rows})
        trees: List[Dict[str, Any]] = []
        for idx, tid in enumerate(tree_ids):
            tree_rows = [
                r for r in matcher_rows
                if (as_int(r.get("tree_id"), 1) or 1) == tid
            ]
            tree = make_tree(tree_rows, idx)
            if not tree["main"]["name"] or not tree["reference"]["name"]:
                log.warning(
                    "Configuration root %s tree %s has incomplete main/reference names: "
                    "main=%r reference=%r row_count=%s; retaining it because PHP retains it "
                    "and the matcher must reject the whole family at this tree",
                    rid, tid, tree["main"].get("name"), tree["reference"].get("name"), len(tree_rows)
                )
            # Do not silently omit incomplete trees. PHP keeps them in root.trees;
            # its matcher clears the family chain when it reaches one. Omitting one
            # here would turn a required multi-tree setup into an easier partial match.
            trees.append(tree)

        template = root_rows[0]
        if confirmation:
            confirmation_default_name_counts[parent_id] = confirmation_default_name_counts.get(parent_id, 0) + 1
            root = _php_confirmation_root_settings(
                root_rows, rid, confirmation_default_name_counts[parent_id]
            )
            root["parentRootId"] = parent_id
            root["trees"] = trees
            root["applyToChart"] = True
            prior_trade_enabled = any(_php_db_bool(row.get("use_for_trade")) for row in root_rows[:-1])
            final_trade_enabled = _php_db_bool(root_rows[-1].get("use_for_trade"))
            if prior_trade_enabled and not final_trade_enabled:
                log.warning(
                    "PHP restoration parity: confirmation root %s (parent root %s) is DISABLED for trades "
                    "because its final saved row config_id=%s sets use_for_trade to %r. "
                    "The PHP page assigns this flag on every row, so the final row wins.",
                    rid, parent_id, root_rows[-1].get("config_id"), root_rows[-1].get("use_for_trade"),
                )
        else:
            timeframe_value = template.get("timeframe")
            normalized_timeframes = _php_parse_saved_timeframes(timeframe_value)
            root = {
                "id": rid,
                "name": str(template.get("root_name") or ""),
                "timeframe": ",".join(normalized_timeframes),
                "timeframes": normalized_timeframes,
                "expanded": False,
                "trees": trees,
                "enabled": True,
                "applyToChart": _php_db_bool(
                    template.get("apply_to_chart")
                    if template.get("apply_to_chart") is not None
                    else template.get("run_in_python")
                ),
                "useForTrades": _php_db_bool(template.get("use_for_trade")),
                "tradeFor": str(template.get("trade_for") or "root"),
                "entryFrom": str(template.get("entry_at") or template.get("entry_from") or ""),
                "orderType": str(template.get("order_type") or template.get("entry_order_type") or "buy"),
                "entryTriggerAfter": str(template.get("entry_trigger_after") or ""),
                "entryPriceLevel": str(template.get("entry_price_level") or "high"),
                "stoplossFrom": str(template.get("stoploss_at") or template.get("stoploss_from") or ""),
                "stoplossPriceLevel": str(template.get("stoploss_price_level") or "high"),
                "takeProfitFrom": str(template.get("take_profit_at") or template.get("take_profit_from") or ""),
                "takeProfitType": (
                    "price_levels"
                    if re.sub(r"\s+", "_", str(template.get("take_profit_type") or "").lower()) == "price_levels"
                    else "risk_reward"
                ),
                "takeProfitPriceLevel": str(template.get("take_profit_price_level") or "high"),
                "riskReward": _php_number(template.get("risk_reward")),
            }
        roots.append(root)
    return roots



def canonical_configuration(roots: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """Return stable JS-shaped config used by matching and parity diagnostics.

    This intentionally preserves the browser's runtime identities and field
    names; it is not a second interpretation of database rows.
    """
    def clean(value: Any) -> Any:
        if isinstance(value, dict):
            return {str(k): clean(v) for k, v in sorted(value.items(), key=lambda item: str(item[0]))}
        if isinstance(value, list):
            return [clean(v) for v in value]
        if isinstance(value, tuple):
            return [clean(v) for v in value]
        return value

    normalized = []
    for root in sorted(roots, key=lambda r: (as_int(r.get("id"), 0) or 0, str(r.get("name") or "").casefold())):
        trees = sorted(root.get("trees") or [], key=lambda t: as_int(t.get("id"), 0) or 0)
        normalized.append({
            "id": root.get("id"), "name": root.get("name"),
            "timeframes": list(root.get("timeframes") or []),
            "entryTriggerAfter": root.get("entryTriggerAfter"),
            "useForTrades": bool(root.get("useForTrades")),
            "tradeFor": root.get("tradeFor"),
            "entryFrom": root.get("entryFrom"), "orderType": root.get("orderType"),
            "entryPriceLevel": root.get("entryPriceLevel"),
            "stoplossFrom": root.get("stoplossFrom"),
            "stoplossPriceLevel": root.get("stoplossPriceLevel"),
            "takeProfitFrom": root.get("takeProfitFrom"),
            "takeProfitType": root.get("takeProfitType"),
            "takeProfitPriceLevel": root.get("takeProfitPriceLevel"),
            "riskReward": root.get("riskReward"),
            "trees": [{
                "id": tree.get("id"), "groupSelectionType": tree.get("groupSelectionType"),
                "regionFromCandleId": tree.get("regionFromCandleId"),
                "regionToCandleId": tree.get("regionToCandleId"),
                "groupNecessity": tree.get("groupNecessity"),
                "ifGroupFound": tree.get("ifGroupFound"),
                "main": tree.get("main"), "reference": tree.get("reference"),
            } for tree in trees]
        })
    return clean({"roots": normalized})


def audit_configuration_parity(roots: Sequence[Dict[str, Any]]) -> List[str]:
    """Report broken identity/anchor links before candle matching starts."""
    issues: List[str] = []
    for root in roots:
        trees = sorted(root.get("trees") or [], key=lambda t: as_int(t.get("id"), 0) or 0)
        known_keys = set()
        for tree_index, tree in enumerate(trees):
            main = tree.get("main") or {}
            ref = tree.get("reference") or {}
            if tree_index > 0 and tree.get("groupSelectionType") == "new_group":
                raw_anchor = main.get("newGroupOffsetTo")
                anchor = str(raw_anchor) if raw_anchor not in (None, "", 0, "0") else ""
                if not anchor:
                    issues.append(f"root {root.get('id')} tree {tree.get('id')}: new_group anchor is NULL/unset")
                elif anchor not in known_keys:
                    issues.append(f"root {root.get('id')} tree {tree.get('id')}: anchor identity {anchor} is not mapped by preceding trees")
            main_key = str(main.get("selectedExistingId") or main.get("offsetTo") or main.get("id"))
            ref_key = str(ref.get("id") if ref.get("referenceMode") == "new_offset" else
                          (ref.get("selectedExistingId") or ref.get("existingCandleId") or ref.get("id")))
            known_keys.update((main_key, ref_key))
    return issues


def load_programme_config(db: BridgeClient, user_id: int, programme_id: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    if not table_exists(db, "programme_training_configuration"):
        raise RuntimeError("programme_training_configuration table does not exist")
    # Load the complete saved rows, then sort defensively in Python. This avoids
    # assuming that older installations name their auto-increment key config_id.
    parent_rows = db.fetch_all(
        "SELECT * FROM `programme_training_configuration` WHERE `user_id`=%s AND `programme_id`=%s",
        [user_id, programme_id], timeout=120)
    parent_rows.sort(key=lambda r: (as_int(r.get("root_id"), 0) or 0,
                                    as_int(r.get("tree_id"), 0) or 0,
                                    as_int(r.get("config_id"), 0) or 0))
    confirmation_rows: List[Dict[str, Any]] = []
    if table_exists(db, "programme_training_confirmation_configuration"):
        confirmation_rows = db.fetch_all(
            "SELECT * FROM `programme_training_confirmation_configuration` WHERE `user_id`=%s AND `programme_id`=%s",
            [user_id, programme_id], timeout=120)
        confirmation_rows.sort(key=lambda r: (as_int(r.get("parent_root_id"), 0) or 0,
                                              as_int(r.get("confirmation_root_id"), 0) or 0,
                                              as_int(r.get("tree_id"), 0) or 0,
                                              as_int(r.get("config_id"), 0) or 0))
    # Match the browser restoration sequence: parents first, then confirmation
    # rows, while sharing one PT_CFG.nextCandleId allocator across both tables.
    assign_php_runtime_candle_ids(parent_rows, confirmation_rows)
    parent_roots = group_rows(parent_rows, confirmation=False)
    for issue in audit_configuration_parity(parent_roots):
        log.warning("Configuration parity audit: %s", issue)
    log.info(
        "Programme %s configuration: %s parent row(s), %s parent root(s), %s configured tree(s)",
        programme_id, len(parent_rows), len(parent_roots),
        sum(len(r.get("trees") or []) for r in parent_roots))
    # Confirmation table uses parent_root_id for its owning parent root. Normalize
    # that to root_id, while keeping confirmation_root_id as its own root identity.
    for row in confirmation_rows:
        row["parent_root_id"] = row.get("parent_root_id")
        row["root_id"] = as_int(row.get("parent_root_id"), as_int(row.get("root_id"), 0))
    # group_rows expects confirmation_root_id for the group ID.
    confirmation_roots = group_rows(confirmation_rows, confirmation=True)
    log.info(
        "Programme %s confirmation configuration: %s row(s), %s confirmation root(s), %s tree(s)",
        programme_id, len(confirmation_rows), len(confirmation_roots),
        sum(len(r.get("trees") or []) for r in confirmation_roots))
    by_parent: Dict[int, List[Dict[str, Any]]] = {}
    for cr in confirmation_roots:
        cr_id = cr["id"]
        parent_id = as_int(cr.get("parentRootId"))
        rows_for_cr = [
            r for r in confirmation_rows
            if as_int(r.get("confirmation_root_id")) == cr_id
            and as_int(r.get("parent_root_id")) == parent_id
        ]
        cr["parentRootId"] = parent_id
        cr["isConfirmationRoot"] = True
        cr["drawings"] = []
        by_parent.setdefault(cr["parentRootId"] or 0, []).append(cr)
    for root in parent_roots:
        root["confirmationRoots"] = by_parent.get(root["id"], [])
        root["confirmationRoot"] = (root["confirmationRoots"][0] if root["confirmationRoots"] else None)
        root["drawings"] = []
        # Match ptRestoreSavedRows()'s backward-compatible move of legacy
        # root-level trade fields to the first confirmation alternative.
        if (str(root.get("tradeFor") or "root") == "root_confirmation"
                and bool(root.get("useForTrades")) and root["confirmationRoots"]
                and not any(bool(cr.get("useForTrades")) for cr in root["confirmationRoots"])):
            first_cr = root["confirmationRoots"][0]
            for field in ("entryFrom", "orderType", "entryTriggerAfter", "entryPriceLevel",
                          "stoplossFrom", "stoplossPriceLevel", "takeProfitFrom", "takeProfitType",
                          "takeProfitPriceLevel", "riskReward"):
                value = root.get(field)
                if value not in (None, ""):
                    first_cr[field] = value
            first_cr["useForTrades"] = True
            root["useForTrades"] = False

    drawing_id = 1  # PT_CFG.nextOffsetId starts at 1 and advances in restore order.
    drawing_rows = [(row, False) for row in parent_rows] + [(row, True) for row in confirmation_rows]
    root_by_id = {int(root["id"]): root for root in parent_roots}
    for row, is_confirmation in drawing_rows:
        draw_from = row.get("draw_from")
        draw_to = row.get("draw_to")
        if not is_drawing_only_row(row):
            continue
        parent_id = (as_int(row.get("parent_root_id"), 1) or 1) if is_confirmation else (as_int(row.get("root_id"), 1) or 1)
        target_root = root_by_id.get(parent_id)
        if target_root is None:
            continue
        draw_tree_id = row.get("tree_id") or (str(draw_from or "").split("|")[0])
        target_root["drawings"].append({
            "id": drawing_id,
            "drawTreeId": str(draw_tree_id or ""),
            "drawFrom": draw_from or "",
            "drawFromPriceLevel": row.get("draw_from_price_level") or "high",
            "drawTo": draw_to or "",
            "drawToPriceLevel": row.get("draw_to_price_level") or "high",
            "drawingColor": row.get("drawing_color") or "blue",
            "drawFor": row.get("draw_for") or ("root_confirmation" if is_confirmation else "root"),
        })
        drawing_id += 1

    # Mirror ptRestoreSavedRows()'s backwards-compatible offset-alias repair.
    # PHP applies this to each parent root's drawings against that parent's trees
    # (including confirmation drawing rows, which are stored on the parent root).
    for root in parent_roots:
        trees_by_id = {str(tree.get("id")): tree for tree in (root.get("trees") or [])}

        def restore_offset_endpoint(value: Any) -> None:
            parts = str(value or "").split("|")
            if len(parts) < 3 or parts[1] != "offset":
                return
            tree = trees_by_id.get(str(parts[0]))
            if not tree or not tree.get("reference"):
                return
            reference = tree["reference"]
            if str(reference.get("id")) != str(parts[2]):
                return
            reference["referenceMode"] = "new_offset"
            reference["selectedExistingId"] = None
            reference["existingCandleId"] = None
            reference["offsetTo"] = (
                reference.get("offsetTo")
                or (tree.get("main") or {}).get("selectedExistingId")
                or (tree.get("main") or {}).get("id")
            )
            reference["offsetToName"] = (
                reference.get("offsetToName")
                or (tree.get("main") or {}).get("name")
                or ""
            )
            if not isinstance(reference.get("candleText"), str):
                reference["candleText"] = ""
            reference["textPosition"] = reference.get("textPosition") or "high"
            tree["offset"] = reference

        for drawing in root.get("drawings") or []:
            restore_offset_endpoint(drawing.get("drawFrom"))
            restore_offset_endpoint(drawing.get("drawTo"))

    return parent_roots, confirmation_roots, parent_rows, confirmation_rows


def candle_sequence_diagnostics(candles: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """Summarise the exact candle sequence handed to the matcher."""
    stamps = [parse_datetime(c.get("time") or c.get("open_time")) for c in candles]
    valid_stamps = [stamp for stamp in stamps if stamp is not None]
    monotonic = all(a <= b for a, b in zip(valid_stamps, valid_stamps[1:]))
    strict = all(a < b for a, b in zip(valid_stamps, valid_stamps[1:]))
    ids = [str(c.get("id")) for c in candles if c.get("id") is not None]
    times = [stamp.isoformat(sep=" ") for stamp in valid_stamps]
    missing_ohlc = sum(
        1 for c in candles
        if any(as_float(c.get(k)) is None for k in ("open", "high", "low", "close"))
    )
    invalid_ohlc = sum(
        1 for c in candles
        if as_float(c.get("high")) is not None and as_float(c.get("low")) is not None
        and as_float(c.get("high")) < as_float(c.get("low"))
    )
    return {
        "count": len(candles),
        "valid_timestamps": len(valid_stamps),
        "oldest": times[0] if times else None,
        "newest": times[-1] if times else None,
        "chronological_non_decreasing": monotonic,
        "chronological_strict": strict,
        "duplicate_ids": len(ids) - len(set(ids)),
        "duplicate_timestamps": len(times) - len(set(times)),
        "missing_ohlc_rows": missing_ohlc,
        "high_below_low_rows": invalid_ohlc,
    }


def _display_enum(value: Any, labels: Dict[str, str], fallback: str = "Not set") -> str:
    """Turn stored enum values into readable console labels."""
    raw = str(value or "").strip().lower()
    if not raw:
        return fallback
    return labels.get(raw, raw.replace("_", " ").replace("-", " ").title())


def _display_candle_name(candle: Dict[str, Any]) -> str:
    name = str(candle.get("name") or "Unnamed candle").strip()
    candle_id = as_int(candle.get("id"))
    identity = as_int(
        candle.get("selectedExistingId") or candle.get("existingCandleId") or candle.get("offsetTo")
    )
    if candle_id is not None:
        label = f"{name} (ID {candle_id})"
    else:
        label = name
    if identity is not None and identity != candle_id:
        label += f"; linked identity {identity}"
    return label


def _display_offset_rule(distance: Any, point: Any) -> str:
    count = as_int(distance)
    count_label = (f"{count} position" if count == 1 else f"{count} positions") if count is not None else "default distance"
    point_label = _display_enum(point, {
        "all": "all matching offsets",
        "fixed": "fixed offset",
        "random": "random matching offset",
        "any_first_candle": "first matching candle",
    })
    return f"{count_label}; {point_label}"


def _display_price_operator(operator: Any) -> str:
    return {
        "=": "equals", "==": "equals", "<>": "does not equal", "!=": "does not equal",
        "<": "is less than", ">": "is greater than", "<=": "is at most",
        "=<": "is at most", ">=": "is at least",
    }.get(str(operator or "<").strip(), "is less than")


def print_tree_configuration(root: Dict[str, Any]) -> None:
    """Print restored matcher settings as a readable, indented configuration report."""
    root_name = _root_display_name(root)
    root_id = root.get("id")
    timeframes = ", ".join(tf.upper() for tf in (root.get("timeframes") or [])) or "Not configured"
    route = _display_enum(root.get("tradeFor"), {
        "root": "Parent root",
        "root_confirmation": "Parent plus confirmation root",
    })
    print(f"\nROOT: {root_name} (ID {root_id})")
    print(f"  Parent timeframe(s): {timeframes}")
    print(f"  Trade routing: {route}")
    print(f"  Parent-root trade setup: {'Enabled' if bool(root.get('useForTrades')) else 'Disabled'}")
    trees = sorted(root.get("trees") or [], key=lambda t: as_int(t.get("id"), 0) or 0)
    print(f"  Configured trees: {len(trees)}")

    group_labels = {
        "existing_group": "Existing group",
        "new_group": "New group",
        "existing_candles_window": "Existing-candles window",
    }
    necessity_labels = {"required": "Required", "optional": "Optional"}
    action_labels = {
        "identify_and_trade": "Identify and trade",
        "identify_only": "Identify only",
        "trade_only": "Trade only",
    }
    position_labels = {
        "left": "Left", "right": "Right", "left_and_right": "Left and right",
        "both": "Left and right", "all": "Left and right",
    }
    candle_type_labels = {
        "bullish": "Bullish", "bearish": "Bearish",
        "bearish_or_bullish": "Bullish or bearish",
    }
    reference_mode_labels = {"existing": "Existing candle", "new_offset": "New offset"}
    price_labels = {
        "open": "Open", "close": "Close", "high": "High", "low": "Low",
        "candle_center": "Candle centre", "body_center": "Body centre",
        "high_wick_center": "High-wick centre", "low_wick_center": "Low-wick centre",
    }

    for tree in trees:
        main = tree.get("main") or {}
        ref = tree.get("reference") or {}
        tree_id = tree.get("id", "?")
        mode = _display_enum(tree.get("groupSelectionType"), group_labels)
        necessity = _display_enum(tree.get("groupNecessity"), necessity_labels)
        action = _display_enum(tree.get("ifGroupFound"), action_labels)
        print(f"\n  TREE {tree_id} — {mode}")
        print(f"    Requirement: {necessity}")
        print(f"    Action when found: {action}")

        print("    MAIN CANDLE")
        print(f"      Candle: {_display_candle_name(main)}")
        print(f"      Type: {_display_enum(main.get('mainCandleType'), candle_type_labels)}")
        print(f"      Search position: {_display_enum(main.get('position'), position_labels)}")
        print(f"      Offset rule: {_display_offset_rule(main.get('offsetPosition'), main.get('offsetPosPoint'))}")
        print(f"      Price level: {_display_enum(main.get('priceLevel'), price_labels)}")

        print("    REFERENCE CANDLE")
        print(f"      Candle: {_display_candle_name(ref)}")
        print(f"      Mode: {_display_enum(ref.get('referenceMode'), reference_mode_labels)}")
        print(f"      Type: {_display_enum(ref.get('refCandleType'), candle_type_labels)}")
        print(f"      Search position: {_display_enum(ref.get('position'), position_labels)}")
        print(f"      Offset rule: {_display_offset_rule(ref.get('offsetPosition'), ref.get('offsetPosPoint'))}")
        print(f"      Price level: {_display_enum(ref.get('priceLevel'), price_labels)}")

        main_level = _display_enum(main.get("priceLevel"), price_labels)
        ref_level = _display_enum(ref.get("priceLevel"), price_labels)
        relation = _display_price_operator(main.get("operator"))
        print(f"    PRICE RULE: Main {main_level.lower()} {relation} reference {ref_level.lower()}")

        anchor = as_int(main.get("newGroupOffsetTo"))
        if anchor is not None:
            print(f"    New-group anchor: candle identity {anchor}")
        direction = main.get("newGroupDirection")
        if direction and str(tree.get("groupSelectionType") or "").lower() == "new_group":
            print(f"    New-group search direction: {_display_enum(direction, position_labels)}")
        window_from = as_int(tree.get("regionFromCandleId"))
        window_to = as_int(tree.get("regionToCandleId"))
        if window_from is not None or window_to is not None:
            print(f"    Candle search window: {window_from if window_from is not None else 'start'} to {window_to if window_to is not None else 'end'}")

    for cr in (root.get("confirmationRoots") or []):
        cr_name = _root_display_name(cr, True)
        cr_id = cr.get("id")
        cr_tfs = ", ".join(tf.upper() for tf in (cr.get("timeframes") or [])) or "Not configured"
        anchor_id = cr.get("anchorCandleId")
        anchor_name = str(cr.get("anchorCandleName") or "Not configured")
        anchor_time = _display_enum(cr.get("anchorTime"), {
            "open_time": "Open time", "close_time": "Close time",
        })
        print(f"\n  CONFIRMATION ROOT: {cr_name} (ID {cr_id})")
        print(f"    Timeframe(s): {cr_tfs}")
        print(f"    Anchor candle: {anchor_name} (ID {anchor_id})")
        print(f"    Anchor time: {anchor_time}")
        print(f"    Configured trees: {len(cr.get('trees') or [])}")
        print(f"    Trade setup: {'Enabled' if bool(cr.get('useForTrades')) else 'Disabled'}")


def print_matcher_diagnostics(root: Dict[str, Any], candle_count: int) -> None:
    diagnostics = root.get("_matcherDiagnostics") or []
    if not diagnostics:
        print("  MATCHER DIAGNOSTICS: no tree diagnostics were produced (no trees or no candles)")
        return
    print(f"  MATCHER DIAGNOSTICS — {len(diagnostics)} tree(s), {candle_count:,} input candles")
    for item in diagnostics:
        print(
            f"    Tree {item.get('tree_id')}: {item.get('main_name') or 'Main'} "
            f"→ {item.get('reference_name') or 'Reference'}"
        )
        print(
            f"      Main candles: {item.get('main_candles_scanned', 0):,} scanned; "
            f"{item.get('main_type_matches', 0):,} passed type; "
            f"{item.get('main_type_rejections', 0):,} rejected"
        )
        print(
            f"      Reference candles: {item.get('reference_candidate_checks', 0):,} checked; "
            f"{item.get('reference_type_matches', 0):,} passed type; "
            f"{item.get('reference_type_rejections', 0):,} rejected; "
            f"{item.get('reference_out_of_bounds', 0):,} outside data"
        )
        print(
            f"      Price comparisons: {item.get('price_comparisons', 0):,}; "
            f"{item.get('price_matches', 0):,} passed; "
            f"{item.get('price_rejections', 0):,} rejected"
        )
        print(
            f"      Family states: {item.get('incoming_states', 0):,} received; "
            f"{item.get('candidate_states', 0):,} candidates; "
            f"{item.get('retained_states', 0):,} retained"
        )
        blocker = item.get("blocker") or "none"
        print(f"      Result: {item.get('status', 'evaluated')}; blocker: {blocker}")


# ---------------------------- Root family matcher ----------------------------
def match_root(root: Dict[str, Any], candles: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Match complete root families and retain a rejection trace for each tree."""
    trees = sorted(root.get("trees") or [], key=lambda t: int(t.get("id", 0)))
    if not candles or not trees:
        root["_matcherDiagnostics"] = [{
            "tree_id": tree.get("id"), "main_name": (tree.get("main") or {}).get("name"),
            "reference_name": (tree.get("reference") or {}).get("name"),
            "incoming_states": 0, "candidate_states": 0, "retained_states": 0,
            "status": "not evaluated", "blocker": "no candle rows" if not candles else "root has no trees",
        } for tree in trees]
        return []

    chain_states: List[Dict[str, Any]] = []
    processed_tree_count = 0
    tree_diagnostics: List[Dict[str, Any]] = []

    def inc(stats: Dict[str, Any], key: str, amount: int = 1) -> None:
        stats[key] = int(stats.get(key, 0)) + amount

    def main_type_pass(stats: Dict[str, Any], candle: Dict[str, Any], wanted: Any) -> bool:
        inc(stats, "main_candles_scanned")
        passed = type_matches(candle, wanted)
        if passed:
            inc(stats, "main_type_matches")
        else:
            inc(stats, "main_type_rejections")
        return passed

    def reference_type_pass(stats: Dict[str, Any], candle: Dict[str, Any], wanted: Any) -> bool:
        inc(stats, "reference_candidate_checks")
        passed = type_matches(candle, wanted)
        if passed:
            inc(stats, "reference_type_matches")
        else:
            inc(stats, "reference_type_rejections")
        return passed

    def compare_pass(stats: Dict[str, Any], main_candle: Dict[str, Any], main: Dict[str, Any],
                     ref_candle: Dict[str, Any], ref: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        inc(stats, "price_comparisons")
        result = compare_levels(
            main_candle, main.get("priceLevel") or "high", main.get("operator") or "<",
            ref_candle, ref.get("priceLevel") or "high",
        )
        if result:
            inc(stats, "price_matches")
        else:
            inc(stats, "price_rejections")
        return result

    for tree_index, tree in enumerate(trees):
        main, ref = tree.get("main") or {}, tree.get("reference") or {}
        stats: Dict[str, Any] = {
            "tree_id": tree.get("id"), "main_name": main.get("name"),
            "reference_name": ref.get("name"), "incoming_states": len(chain_states) if tree_index else len(candles),
            "main_candles_scanned": 0, "main_type_matches": 0, "main_type_rejections": 0,
            "reference_candidate_checks": 0, "reference_type_matches": 0,
            "reference_type_rejections": 0, "reference_out_of_bounds": 0,
            "price_comparisons": 0, "price_matches": 0, "price_rejections": 0,
            "window_invalid_states": 0, "identity_missing_states": 0,
            "preselected_reference_reuses": 0,
            "missing_anchor_states": 0, "anchor_not_configured": False,
            "new_group_reference_resolved": 0, "candidate_states": 0,
            "retained_states": 0, "group_selection_type": tree.get("groupSelectionType"),
        }
        if not main.get("name") or not ref.get("name"):
            chain_states = []
            stats.update({"status": "incomplete configuration", "blocker": "main or reference candle name is missing"})
            tree_diagnostics.append(stats)
            continue
        processed_tree_count += 1
        main_key = str(main.get("selectedExistingId") or main.get("offsetTo") or main.get("id"))
        is_new_ref = ref.get("referenceMode") == "new_offset"
        ref_key = str((ref.get("id") if is_new_ref else (ref.get("selectedExistingId") or ref.get("existingCandleId") or ref.get("id"))))
        next_states: List[Dict[str, Any]] = []
        n = len(candles)
        anchor_key_for_diagnostic: Optional[str] = None

        if tree_index == 0:
            for main_idx, main_candle in enumerate(candles):
                if len(next_states) >= MAX_ROOT_MATCH_STATES:
                    break
                if not main_type_pass(stats, main_candle, main.get("mainCandleType")):
                    continue
                candidates = offset_candidates(
                    main_idx, main.get("position") or ref.get("position") or "left_and_right",
                    main.get("offsetPosition") or ref.get("offsetPosition") or 1,
                    effective_point(main, ref), n,
                )
                for ref_idx in candidates:
                    if ref_idx < 0 or ref_idx >= n:
                        inc(stats, "reference_out_of_bounds")
                        continue
                    if not reference_type_pass(stats, candles[ref_idx], ref.get("refCandleType")):
                        continue
                    lm = compare_pass(stats, main_candle, main, candles[ref_idx], ref)
                    if not lm:
                        continue
                    fmap = {main_key: main_idx, ref_key: ref_idx}
                    cond = {
                        "rootId": root["id"], "treeId": tree["id"], "mainName": main["name"],
                        "referenceName": ref["name"], "ifGroupFound": tree.get("ifGroupFound") or "identify_and_trade",
                        "mainCandleId": candles[main_idx].get("id"), "referenceCandleId": candles[ref_idx].get("id"),
                        "offsetCandleId": candles[ref_idx].get("id") if is_new_ref else None,
                        "mainIdentity": main_key, "referenceIdentity": ref_key,
                        "operator": main.get("operator") or "<", "mainPriceLevel": lm["mainLevel"],
                        "referencePriceLevel": lm["referenceLevel"],
                        "configuredMainPriceLevel": main.get("priceLevel") or "high",
                        "configuredReferencePriceLevel": ref.get("priceLevel") or "high",
                        "mainValue": lm["mainValue"], "referenceValue": lm["referenceValue"],
                    }
                    next_states.append({"map": fmap, "mainIndex": main_idx, "refIndex": ref_idx, "conditions": [cond]})
                    if any_first(main.get("offsetPosPoint"), ref.get("offsetPosPoint")):
                        break
        else:
            for state in chain_states:
                if len(next_states) >= MAX_ROOT_MATCH_STATES:
                    break
                if tree.get("groupSelectionType") == "existing_candles_window":
                    from_key = str(tree.get("regionFromCandleId") or "")
                    to_key = str(tree.get("regionToCandleId") or "")
                    from_idx = as_int(state["map"].get(from_key), -1)
                    to_idx = as_int(state["map"].get(to_key), -1)
                    if from_idx < 0 or to_idx < 0 or from_idx == to_idx or from_idx >= n or to_idx >= n:
                        inc(stats, "window_invalid_states")
                        continue
                    step = 1 if to_idx > from_idx else -1
                    window_main = list(range(from_idx + step, to_idx, step))
                    if not window_main:
                        inc(stats, "window_invalid_states")
                        continue
                    if not is_new_ref:
                        existing_ref_idx = as_int(state["map"].get(ref_key), -1)
                        if existing_ref_idx < 0 or existing_ref_idx >= n or existing_ref_idx not in [from_idx, to_idx] + window_main:
                            inc(stats, "identity_missing_states")
                            continue
                        for mi in window_main:
                            if mi == existing_ref_idx:
                                continue
                            if not main_type_pass(stats, candles[mi], main.get("mainCandleType")):
                                continue
                            # PHP treats this as an already-selected existing reference
                            # identity. Unlike the ordinary existing-group branch, it does
                            # not re-run the current tree's reference-type predicate here.
                            inc(stats, "preselected_reference_reuses")
                            lm = compare_pass(stats, candles[mi], main, candles[existing_ref_idx], ref)
                            if not lm:
                                continue
                            new_map = dict(state["map"])
                            new_map[main_key] = mi
                            new_map[ref_key] = existing_ref_idx
                            cond = {
                                "rootId": root["id"], "treeId": tree["id"], "mainName": main["name"],
                                "referenceName": ref["name"], "ifGroupFound": tree.get("ifGroupFound") or "identify_and_trade",
                                "mainCandleId": candles[mi].get("id"), "referenceCandleId": candles[existing_ref_idx].get("id"),
                                "offsetCandleId": None, "mainIdentity": main_key, "referenceIdentity": ref_key,
                                "operator": main.get("operator") or "<", "mainPriceLevel": lm["mainLevel"],
                                "referencePriceLevel": lm["referenceLevel"],
                                "configuredMainPriceLevel": main.get("priceLevel") or "high",
                                "configuredReferencePriceLevel": ref.get("priceLevel") or "high",
                                "mainValue": lm["mainValue"], "referenceValue": lm["referenceValue"],
                                "groupSelectionType": "existing_candles_window",
                            }
                            next_states.append({"map": new_map, "mainIndex": mi, "refIndex": existing_ref_idx,
                                                "conditions": list(state["conditions"]) + [cond]})
                            break
                    else:
                        window_set = set(window_main)
                        for mi in window_main:
                            if len(next_states) >= MAX_ROOT_MATCH_STATES:
                                break
                            if not main_type_pass(stats, candles[mi], main.get("mainCandleType")):
                                continue
                            refs = offset_candidates(
                                mi, ref.get("position") or "left", ref.get("offsetPosition") or 1,
                                effective_point(main, ref), n,
                            )
                            before = len(next_states)
                            for ri in refs:
                                if ri not in window_set:
                                    continue
                                if ri < 0 or ri >= n:
                                    inc(stats, "reference_out_of_bounds")
                                    continue
                                if not reference_type_pass(stats, candles[ri], ref.get("refCandleType")):
                                    continue
                                lm = compare_pass(stats, candles[mi], main, candles[ri], ref)
                                if not lm:
                                    continue
                                new_map = dict(state["map"])
                                new_map[main_key] = mi
                                new_map[ref_key] = ri
                                cond = {
                                    "rootId": root["id"], "treeId": tree["id"], "mainName": main["name"],
                                    "referenceName": ref["name"], "ifGroupFound": tree.get("ifGroupFound") or "identify_and_trade",
                                    "mainCandleId": candles[mi].get("id"), "referenceCandleId": candles[ri].get("id"),
                                    "offsetCandleId": candles[ri].get("id"), "mainIdentity": main_key,
                                    "referenceIdentity": ref_key, "operator": main.get("operator") or "<",
                                    "mainPriceLevel": lm["mainLevel"], "referencePriceLevel": lm["referenceLevel"],
                                    "configuredMainPriceLevel": main.get("priceLevel") or "high",
                                    "configuredReferencePriceLevel": ref.get("priceLevel") or "high",
                                    "mainValue": lm["mainValue"], "referenceValue": lm["referenceValue"],
                                    "groupSelectionType": "existing_candles_window",
                                }
                                next_states.append({"map": new_map, "mainIndex": mi, "refIndex": ri,
                                                    "conditions": list(state["conditions"]) + [cond]})
                                break
                            if len(next_states) > before:
                                break
                elif tree.get("groupSelectionType") == "new_group":
                    raw_anchor = main.get("newGroupOffsetTo")
                    anchor_key = "" if raw_anchor is None or str(raw_anchor) == "" else str(raw_anchor)
                    anchor_key_for_diagnostic = anchor_key or None
                    if not anchor_key:
                        stats["anchor_not_configured"] = True
                        inc(stats, "missing_anchor_states")
                        continue
                    if anchor_key not in state["map"]:
                        inc(stats, "missing_anchor_states")
                        continue
                    anchor_idx = as_int(state["map"].get(anchor_key), -1)
                    if anchor_idx < 0 or anchor_idx >= n:
                        inc(stats, "missing_anchor_states")
                        continue
                    direction = -1 if str(main.get("newGroupDirection") or "right").lower() == "left" else 1
                    anchor_point = str(main.get("newGroupOffsetPosPoint") or "all").lower()
                    start = anchor_idx + direction
                    if anchor_point in {"fixed", "random"}:
                        end_exclusive = start + direction
                    else:
                        end_exclusive = n if direction > 0 else -1
                    mi = start
                    while (mi < end_exclusive if direction > 0 else mi > end_exclusive) and 0 <= mi < n:
                        if main_type_pass(stats, candles[mi], main.get("mainCandleType")):
                            resolved = resolve_new_group_reference(mi, main, ref, candles, stats)
                            if resolved:
                                inc(stats, "new_group_reference_resolved")
                                ri, lm = resolved
                                new_map = dict(state["map"])
                                new_map[main_key] = mi
                                new_map[ref_key] = ri
                                cond = {
                                    "rootId": root["id"], "treeId": tree["id"], "mainName": main["name"],
                                    "referenceName": ref["name"], "ifGroupFound": tree.get("ifGroupFound") or "identify_and_trade",
                                    "mainCandleId": candles[mi].get("id"), "referenceCandleId": candles[ri].get("id"),
                                    "offsetCandleId": None, "mainIdentity": main_key, "referenceIdentity": ref_key,
                                    "operator": main.get("operator") or "<", "mainPriceLevel": lm["mainLevel"],
                                    "referencePriceLevel": lm["referenceLevel"],
                                    "configuredMainPriceLevel": main.get("priceLevel") or "high",
                                    "configuredReferencePriceLevel": ref.get("priceLevel") or "high",
                                    "mainValue": lm["mainValue"], "referenceValue": lm["referenceValue"],
                                    "groupSelectionType": "new_group",
                                }
                                next_states.append({"map": new_map, "mainIndex": mi, "refIndex": ri,
                                                    "conditions": list(state["conditions"]) + [cond]})
                                if anchor_point == "any_first_candle":
                                    break
                        mi += direction
                else:
                    main_idx = as_int(state["map"].get(main_key), -1)
                    if main_idx < 0 or main_idx >= n:
                        inc(stats, "identity_missing_states")
                        continue
                    if not main_type_pass(stats, candles[main_idx], main.get("mainCandleType")):
                        continue
                    refs = (
                        offset_candidates(main_idx, ref.get("position") or "left", ref.get("offsetPosition") or 1,
                                          effective_point(main, ref), n)
                        if is_new_ref else
                        ([as_int(state["map"].get(ref_key), -1)] if ref_key in state["map"] else [])
                    )
                    if not refs:
                        inc(stats, "identity_missing_states")
                    for ri in refs:
                        if ri < 0 or ri >= n:
                            inc(stats, "reference_out_of_bounds")
                            continue
                        if not reference_type_pass(stats, candles[ri], ref.get("refCandleType")):
                            continue
                        lm = compare_pass(stats, candles[main_idx], main, candles[ri], ref)
                        if not lm:
                            continue
                        new_map = dict(state["map"])
                        if is_new_ref:
                            new_map[ref_key] = ri
                        cond = {
                            "rootId": root["id"], "treeId": tree["id"], "mainName": main["name"],
                            "referenceName": ref["name"], "ifGroupFound": tree.get("ifGroupFound") or "identify_and_trade",
                            "mainCandleId": candles[main_idx].get("id"), "referenceCandleId": candles[ri].get("id"),
                            "offsetCandleId": candles[ri].get("id") if is_new_ref else None,
                            "mainIdentity": main_key, "referenceIdentity": ref_key,
                            "operator": main.get("operator") or "<", "mainPriceLevel": lm["mainLevel"],
                            "referencePriceLevel": lm["referenceLevel"],
                            "configuredMainPriceLevel": main.get("priceLevel") or "high",
                            "configuredReferencePriceLevel": ref.get("priceLevel") or "high",
                            "mainValue": lm["mainValue"], "referenceValue": lm["referenceValue"],
                            "groupSelectionType": tree.get("groupSelectionType"),
                        }
                        next_states.append({"map": new_map, "mainIndex": main_idx, "refIndex": ri,
                                            "conditions": list(state["conditions"]) + [cond]})
                        if any_first(main.get("offsetPosPoint"), ref.get("offsetPosPoint")):
                            break

            if tree_index > 0 and tree.get("groupNecessity") == "not_important":
                next_states.extend(chain_states)

        stats["candidate_states"] = len(next_states)
        chain_states = next_states[:MAX_ROOT_MATCH_STATES]
        stats["retained_states"] = len(chain_states)
        stats["status"] = "evaluated"
        if tree_index == 0 and not stats["retained_states"]:
            if not stats["main_type_matches"]:
                stats["blocker"] = "no candle passed the configured main-candle type"
            elif not stats["reference_candidate_checks"]:
                stats["blocker"] = "main candles matched, but the configured reference offset produced no in-range candidate"
            elif not stats["reference_type_matches"]:
                stats["blocker"] = "reference candidates existed, but none passed the configured reference-candle type"
            elif not stats["price_matches"]:
                stats["blocker"] = "candle types matched, but no pair passed the configured price-level/operator comparison"
            else:
                stats["blocker"] = "initial matching states were removed or capped before retention"
        elif tree_index > 0 and not stats["retained_states"]:
            if stats.get("anchor_not_configured"):
                stats["blocker"] = "new_group is selected but its new-group anchor is not configured"
            elif stats.get("missing_anchor_states"):
                stats["blocker"] = "new_group anchor identity is absent from the preceding family map"
            elif stats.get("window_invalid_states"):
                stats["blocker"] = "existing-candles-window endpoints are missing, identical, reversed out of range, or contain no inner candle"
            elif stats.get("identity_missing_states"):
                stats["blocker"] = "an existing main/reference candle identity is not present in the preceding family map"
            elif not stats.get("main_type_matches"):
                stats["blocker"] = "incoming families did not contain a main candle passing this tree's type rule"
            elif not stats.get("reference_type_matches"):
                stats["blocker"] = "no selected reference candle passed this tree's reference type rule"
            elif not stats.get("price_matches"):
                stats["blocker"] = "no candidate passed this tree's price-level/operator comparison"
            else:
                stats["blocker"] = "no completed state survived this tree; inspect identity links and candidate counters"
        else:
            stats["blocker"] = "none; this tree retained candidate states"
        tree_diagnostics.append(stats)

    root["_matcherDiagnostics"] = tree_diagnostics
    required_trees = [t for i, t in enumerate(trees) if i == 0 or t.get("groupNecessity") != "not_important"]
    if not required_trees or processed_tree_count < len(required_trees):
        root["_matcherFinal"] = {"required_trees": len(required_trees), "processed_trees": processed_tree_count, "complete_families": 0}
        return []

    complete = []
    for state in chain_states:
        conditions = state.get("conditions") or []
        condition_ids = {str(c.get("treeId")) for c in conditions}
        if all(str(t["id"]) in condition_ids for t in required_trees):
            complete.append({
                "familyNumber": len(complete) + 1, "map": state["map"],
                "conditions": conditions,
                "noTrade": any(c.get("ifGroupFound") == "identify_but_dont_trade" for c in conditions),
            })
    root["_matcherFinal"] = {
        "required_trees": len(required_trees), "processed_trees": processed_tree_count,
        "final_chain_states": len(chain_states), "complete_families": len(complete),
    }
    if not complete and chain_states:
        root["_matcherFinal"]["blocker"] = "states remained, but one or more required tree IDs are missing from their conditions"
    elif not complete:
        root["_matcherFinal"]["blocker"] = "no chain states remained after the required tree sequence"
    else:
        root["_matcherFinal"]["blocker"] = "none"
    return complete


# ----------------------- Root confirmation matching --------------------------
def projection_map(higher: Sequence[Dict[str, Any]], lower: Sequence[Dict[str, Any]],
                   higher_tf: str, lower_tf: str) -> List[Dict[str, int]]:
    lower_duration = tf_milliseconds(lower_tf) or 0
    high_duration = tf_milliseconds(higher_tf) or 0
    lower_intervals = []
    for c in lower:
        start = parse_datetime(c.get("open_time") or c.get("time"))
        end = parse_datetime(c.get("close_time"))
        if not end and start and lower_duration:
            end = start + timedelta(milliseconds=lower_duration)
        lower_intervals.append((start, end))
    result: List[Dict[str, int]] = []
    cursor = 0
    for c in higher:
        start = parse_datetime(c.get("open_time") or c.get("time"))
        end = parse_datetime(c.get("close_time"))
        if not end and start and high_duration:
            end = start + timedelta(milliseconds=high_duration)
        if not start or not end or end <= start:
            result.append({"first": -1, "last": -1}); continue
        while cursor < len(lower_intervals):
            le = lower_intervals[cursor][1]
            if le and le <= start: cursor += 1
            else: break
        first = last = -1
        scan = cursor
        while scan < len(lower_intervals):
            ls, le = lower_intervals[scan]
            if ls and ls >= end: break
            if ls and le and ls < end and le > start:
                if first < 0: first = scan
                last = scan
            scan += 1
        result.append({"first": first, "last": last})
    return result


def confirmation_canonical(tree: Dict[str, Any], kind: str) -> str:
    c = tree.get("main" if kind == "main" else "reference") or {}
    if kind == "main": return str(c.get("selectedExistingId") or c.get("offsetTo") or c.get("id") or "")
    return str(c.get("id") if c.get("referenceMode") == "new_offset" else
               (c.get("selectedExistingId") or c.get("existingCandleId") or c.get("id") or ""))


def match_confirmation_at(cr: Dict[str, Any], anchor_idx: int, parent_family: Dict[str, Any],
                          lower: Sequence[Dict[str, Any]],
                          diagnostics: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    def add(key: str, amount: int = 1) -> None:
        if diagnostics is not None:
            diagnostics[key] = int(diagnostics.get(key, 0)) + amount

    def fail(reason: str) -> List[Dict[str, Any]]:
        add("failed_calls")
        if diagnostics is not None:
            reasons = diagnostics.setdefault("failure_reasons", {})
            reasons[reason] = int(reasons.get(reason, 0)) + 1
        return []

    trees = sorted(cr.get("trees") or [], key=lambda t: int(t.get("id", 0)))
    if not trees:
        return fail("confirmation root has no restored trees")
    if not (0 <= anchor_idx < len(lower)):
        return fail("projected anchor index is outside the lower-timeframe candle array")
    states: List[Dict[str, Any]] = []
    for ti, tree in enumerate(trees):
        main, ref = tree.get("main") or {}, tree.get("reference") or {}
        if not main.get("name") or not ref.get("name"):
            return fail(f"confirmation tree {tree.get('id')} is missing a main/reference name")
        main_key = confirmation_canonical(tree, "main")
        new_ref = ref.get("referenceMode") == "new_offset"
        ref_key = confirmation_canonical(tree, "reference")
        next_states: List[Dict[str, Any]] = []
        if ti == 0:
            mi = anchor_idx
            if type_matches(lower[mi], main.get("mainCandleType")):
                add("main_type_passes")
                refs = offset_candidates(mi, main.get("position") or ref.get("position") or "left_and_right",
                                         main.get("offsetPosition") or ref.get("offsetPosition") or 1,
                                         effective_point(main, ref), len(lower))
                for ri in refs:
                    add("reference_candidate_checks")
                    if not type_matches(lower[ri], ref.get("refCandleType")):
                        add("reference_type_rejections")
                        continue
                    add("reference_type_matches")
                    add("price_comparisons")
                    lm = compare_levels(lower[mi], main.get("priceLevel") or "high", main.get("operator") or "<",
                                        lower[ri], ref.get("priceLevel") or "high")
                    if not lm:
                        add("price_rejections")
                        continue
                    add("price_matches")
                    fmap = {main_key: mi, ref_key: ri}
                    cond = {"rootId": cr["id"], "treeId": tree["id"], "mainName": main["name"],
                            "referenceName": ref["name"], "ifGroupFound": tree.get("ifGroupFound") or "identify_and_trade",
                            "mainCandleId": lower[mi].get("id"), "referenceCandleId": lower[ri].get("id"),
                            "mainIdentity": main_key, "referenceIdentity": ref_key,
                            "mainPriceLevel": lm["mainLevel"], "referencePriceLevel": lm["referenceLevel"],
                            "mainValue": lm["mainValue"], "referenceValue": lm["referenceValue"]}
                    next_states.append({"map": fmap, "conditions": [cond]})
                    if any_first(main.get("offsetPosPoint"), ref.get("offsetPosPoint")):
                        break
            else:
                add("main_type_rejections")
        else:
            for state in states:
                mi = as_int(state["map"].get(main_key), -1)
                if mi < 0 or mi >= len(lower):
                    add("missing_identity_states")
                    continue
                if not type_matches(lower[mi], main.get("mainCandleType")):
                    add("main_type_rejections")
                    continue
                add("main_type_passes")
                refs = (offset_candidates(mi, ref.get("position") or "left", ref.get("offsetPosition") or 1,
                                          effective_point(main, ref), len(lower)) if new_ref else
                        ([as_int(state["map"].get(ref_key), -1)] if ref_key in state["map"] else []))
                if not refs:
                    add("missing_identity_states")
                for ri in refs:
                    add("reference_candidate_checks")
                    if ri < 0 or ri >= len(lower):
                        add("reference_out_of_bounds")
                        continue
                    if not type_matches(lower[ri], ref.get("refCandleType")):
                        add("reference_type_rejections")
                        continue
                    add("reference_type_matches")
                    add("price_comparisons")
                    lm = compare_levels(lower[mi], main.get("priceLevel") or "high", main.get("operator") or "<",
                                        lower[ri], ref.get("priceLevel") or "high")
                    if not lm:
                        add("price_rejections")
                        continue
                    add("price_matches")
                    fmap = dict(state["map"])
                    if new_ref:
                        fmap[ref_key] = ri
                    cond = {"rootId": cr["id"], "treeId": tree["id"], "mainName": main["name"],
                            "referenceName": ref["name"], "ifGroupFound": tree.get("ifGroupFound") or "identify_and_trade",
                            "mainCandleId": lower[mi].get("id"), "referenceCandleId": lower[ri].get("id"),
                            "mainIdentity": main_key, "referenceIdentity": ref_key,
                            "mainPriceLevel": lm["mainLevel"], "referencePriceLevel": lm["referenceLevel"],
                            "mainValue": lm["mainValue"], "referenceValue": lm["referenceValue"]}
                    next_states.append({"map": fmap, "conditions": list(state["conditions"]) + [cond]})
                    if any_first(main.get("offsetPosPoint"), ref.get("offsetPosPoint")):
                        break
        states = next_states
        if not states:
            return fail(f"confirmation tree {tree.get('id')} rejected the projected parent anchor")
    if any(len(st.get("conditions") or []) != len(trees) for st in states):
        return fail("not every confirmation tree condition was completed")
    add("successful_calls")
    return [{"map": st["map"], "anchorIndex": anchor_idx, "parentFamily": parent_family,
             "conditions": st["conditions"]} for st in states]



# ------------------------------- Trade engine ---------------------------------
def normalize_order(raw: Any) -> str:
    value = re.sub(r"[\s_]+", "-", str(raw or "buy").lower().strip())
    m = re.match(r"^(buy|sell)(?:-(limit|stop))?", value)
    return (m.group(1) + ("-" + m.group(2) if m.group(2) else "")) if m else "buy"


def find_entry_trigger(candles: Sequence[Dict[str, Any]], anchor_index: int,
                       anchor_price: float, order_type: str) -> Optional[int]:
    start = anchor_index + 1  # Entry-at candle can never trigger its own order.
    if start >= len(candles): return None
    order = normalize_order(order_type)
    use_high = order in {"sell-limit", "buy-stop"}
    use_low = order in {"buy-limit", "sell-stop"}
    if order in {"buy", "sell"}: return start
    anchor_id = str(candles[anchor_index].get("id")) if candles[anchor_index].get("id") is not None else None
    for idx in range(start, len(candles)):
        c = candles[idx]
        if anchor_id is not None and str(c.get("id")) == anchor_id: continue
        hi, lo = as_float(c.get("high")), as_float(c.get("low"))
        if use_high and hi is not None and hi >= anchor_price: return idx
        if use_low and lo is not None and lo <= anchor_price: return idx
    return None


def evaluate_outcome(candles: Sequence[Dict[str, Any]], trigger: Optional[int],
                     sl_price: Optional[float], tp_price: Optional[float],
                     order_type: str) -> Tuple[str, Optional[int], Optional[int]]:
    if trigger is None or sl_price is None or tp_price is None or not (0 <= trigger < len(candles)):
        return "pending entry", None, None
    is_long = normalize_order(order_type).startswith("buy")
    for idx in range(trigger, len(candles)):
        hi, lo = as_float(candles[idx].get("high")), as_float(candles[idx].get("low"))
        if hi is None or lo is None: continue
        stop_touched = lo <= sl_price if is_long else hi >= sl_price
        target_touched = hi >= tp_price if is_long else lo <= tp_price
        if stop_touched: return "stoploss hit", idx, None  # SL priority when both touch.
        if target_touched: return "take profit reached", None, idx
    return "in trade", None, None


WEEKDAY_INDEX = {
    "sunday": 0, "monday": 1, "tuesday": 2, "wednesday": 3,
    "thursday": 4, "friday": 5, "saturday": 6,
}


def _programme_zone(timezone_name: Any) -> ZoneInfo:
    name = str(timezone_name or "UTC").strip() or "UTC"
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError, TypeError):
        log.warning("Invalid programme timezone %r; Account Management rules will use UTC", name)
        return ZoneInfo("UTC")


def _programme_time_parts(raw_value: Any, timezone_name: Any) -> Optional[Dict[str, Any]]:
    """Mirror programme_training.php::ptProgrammeDateParts().

    Naive SQL timestamps are programme-local wall times. Offset-aware ISO values
    and Unix epochs are converted into the programme timezone before comparing
    date/day/time restrictions.
    """
    if raw_value is None or str(raw_value).strip() == "":
        return None
    zone = _programme_zone(timezone_name)
    dt: Optional[datetime] = None
    raw_text = str(raw_value).strip()

    if isinstance(raw_value, (int, float)) or re.fullmatch(r"-?\d{10,13}(?:\.\d+)?", raw_text):
        try:
            numeric = float(raw_value)
            epoch_seconds = numeric / 1000.0 if abs(numeric) >= 1e12 else numeric
            dt = datetime.fromtimestamp(epoch_seconds, tz=timezone.utc).astimezone(zone)
        except (TypeError, ValueError, OverflowError, OSError):
            return None
    else:
        normalized = re.sub(r"^(\d{4})\.(\d{1,2})\.(\d{1,2})(?=\s|$)", r"\1-\2-\3", raw_text)
        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(normalized.replace("T", " "))
        except ValueError:
            for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d"):
                try:
                    dt = datetime.strptime(normalized[:26], fmt)
                    break
                except ValueError:
                    dt = None
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=zone)
        else:
            dt = dt.astimezone(zone)

    if dt is None:
        return None
    # Python Monday=0; PHP/JS Sunday=0.
    weekday_index = (dt.weekday() + 1) % 7
    return {
        "date": dt.strftime("%Y-%m-%d"),
        "weekday": dt.strftime("%A"),
        "weekday_index": weekday_index,
        "hour": dt.hour,
        "minute": dt.minute,
        "second": dt.second,
        "time_minutes": (dt.hour * 60) + dt.minute + (dt.second / 60.0),
    }


def _rule_time_minutes(value: Any) -> Optional[float]:
    match = re.fullmatch(r"\s*(\d{1,2}):(\d{2})(?::(\d{2}))?\s*", str(value or "").strip())
    if not match:
        return None
    hour, minute, second = int(match.group(1)), int(match.group(2)), int(match.group(3) or 0)
    if hour > 23 or minute > 59 or second > 59:
        return None
    return hour * 60 + minute + (second / 60.0)


def parse_account_management_rr(value: Any) -> Optional[float]:
    """Parse Account Management RR fields exactly like the Training chart UI."""
    if value is None or str(value).strip() == "":
        return None
    raw = str(value).strip()
    pair = re.fullmatch(r"\s*([+-]?(?:\d+(?:\.\d*)?|\.\d+))\s*:\s*([+-]?(?:\d+(?:\.\d*)?|\.\d+))\s*", raw)
    try:
        if pair:
            left, right = float(pair.group(1)), float(pair.group(2))
            return (right / left) if math.isfinite(left) and left != 0 and math.isfinite(right) else None
        parsed = float(re.sub(r"R$", "", raw, flags=re.I).strip())
        return parsed if math.isfinite(parsed) else None
    except (ValueError, TypeError, ZeroDivisionError):
        return None


def evaluate_outcome_with_breakeven(
    candles: Sequence[Dict[str, Any]], trigger: Optional[int],
    sl_price: Optional[float], tp_price: Optional[float], order_type: str,
    entry_price: Optional[float], breakeven_rules: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    """Mirror the PHP chart's chronological Account Management breakeven engine.

    A threshold reached on candle N moves the stop for candle N+1 onward. If the
    active stop and TP both touch on one OHLC candle, the stop wins conservatively.
    """
    result: Dict[str, Any] = {
        "outcome": "pending entry", "sl_trigger_index": None, "tp_trigger_index": None,
        "breakeven_triggered": False, "breakeven_floating_profit_rr": None,
        "breakeven_stop_rr": None, "breakeven_stop_price": None,
        "breakeven_trigger_index": None, "max_floating_rr": 0.0,
    }
    if trigger is None or sl_price is None or tp_price is None or entry_price is None:
        return result
    if not (0 <= int(trigger) < len(candles)):
        return result
    original_stop = as_float(sl_price)
    target = as_float(tp_price)
    entry = as_float(entry_price)
    if original_stop is None or target is None or entry is None:
        return result
    risk_distance = abs(entry - original_stop)
    if not (risk_distance > 0 and math.isfinite(risk_distance)):
        return result

    normalized_rules: List[Tuple[float, float]] = []
    for rule in breakeven_rules or []:
        if not isinstance(rule, dict):
            continue
        floating_rr = parse_account_management_rr(rule.get("floating_profit_at_risk_reward"))
        stop_rr = parse_account_management_rr(rule.get("breakeven_at_risk_reward"))
        if floating_rr is None or stop_rr is None or floating_rr <= 0:
            continue
        normalized_rules.append((floating_rr, max(0.0, stop_rr)))
    normalized_rules.sort(key=lambda item: (item[0], item[1]))

    is_long = normalize_order(order_type).startswith("buy")
    result["outcome"] = "in trade"
    active_stop = original_stop
    next_rule_index = 0
    applied_rule: Optional[Tuple[float, float]] = None

    for idx in range(int(trigger), len(candles)):
        candle = candles[idx]
        if not candle:
            continue
        high, low = as_float(candle.get("high")), as_float(candle.get("low"))
        if high is None or low is None:
            continue

        favourable_move = (high - entry) if is_long else (entry - low)
        floating_rr = favourable_move / risk_distance
        if math.isfinite(floating_rr):
            result["max_floating_rr"] = max(float(result["max_floating_rr"]), floating_rr)

        stop_touched = low <= active_stop if is_long else high >= active_stop
        target_touched = high >= target if is_long else low <= target
        if stop_touched:
            result["outcome"] = "breakeven hit" if result["breakeven_triggered"] else "stoploss hit"
            result["sl_trigger_index"] = idx
            break
        if target_touched:
            # Record every BE threshold crossed on the TP bar, but keep TP as outcome.
            while next_rule_index < len(normalized_rules) and floating_rr >= normalized_rules[next_rule_index][0]:
                rule = normalized_rules[next_rule_index]
                next_rule_index += 1
                moved_stop = entry + (risk_distance * rule[1]) if is_long else entry - (risk_distance * rule[1])
                if applied_rule is None or (moved_stop > active_stop if is_long else moved_stop < active_stop):
                    active_stop = moved_stop
                    applied_rule = rule
                    result["breakeven_triggered"] = True
                    result["breakeven_floating_profit_rr"] = rule[0]
                    result["breakeven_stop_rr"] = rule[1]
                    result["breakeven_stop_price"] = active_stop
                    result["breakeven_trigger_index"] = idx
            result["outcome"] = "take profit reached"
            result["tp_trigger_index"] = idx
            break

        # Activate newly crossed thresholds only after completing this candle.
        while next_rule_index < len(normalized_rules) and floating_rr >= normalized_rules[next_rule_index][0]:
            rule = normalized_rules[next_rule_index]
            next_rule_index += 1
            moved_stop = entry + (risk_distance * rule[1]) if is_long else entry - (risk_distance * rule[1])
            if applied_rule is None or (moved_stop > active_stop if is_long else moved_stop < active_stop):
                active_stop = moved_stop
                applied_rule = rule
                result["breakeven_triggered"] = True
                result["breakeven_floating_profit_rr"] = rule[0]
                result["breakeven_stop_rr"] = rule[1]
                result["breakeven_stop_price"] = active_stop
                result["breakeven_trigger_index"] = idx
    return result


def _raw_candle_timestamp(candle: Optional[Dict[str, Any]]) -> Any:
    if not isinstance(candle, dict):
        return None
    return candle.get("time") if candle.get("time") is not None else (
        candle.get("open_time") if candle.get("open_time") is not None else candle.get("timestamp")
    )


def _source_main_candle_timestamp(
    parent_root: Dict[str, Any], family: Dict[str, Any],
    candles: Sequence[Dict[str, Any]], parent_candles: Optional[Sequence[Dict[str, Any]]],
) -> Any:
    """Resolve the parent/root main-candle identity used to identify a setup."""
    parent_family = family.get("parentFamily") if isinstance(family.get("parentFamily"), dict) else None
    source_map = (parent_family.get("map") or {}) if parent_family else (family.get("map") or {})
    source_candles = parent_candles if parent_family and parent_candles is not None else candles
    for tree in parent_root.get("trees") or []:
        main = tree.get("main") or {}
        identity = main.get("selectedExistingId") or main.get("offsetTo") or main.get("id")
        if identity is None:
            continue
        keys = [str(identity), "parent:" + str(identity)]
        for key in keys:
            index = as_int(source_map.get(key))
            if index is not None and 0 <= index < len(source_candles):
                return _raw_candle_timestamp(source_candles[index])
    return None


def apply_account_management_trade_rules(
    record: Dict[str, Any], candles: Sequence[Dict[str, Any]], trigger_index: Optional[int],
    entry_index: Optional[int], family: Dict[str, Any], parent_root: Dict[str, Any],
    parent_candles: Optional[Sequence[Dict[str, Any]]], account_rules: Dict[str, Any],
) -> Dict[str, Any]:
    """Stamp restricted-date/day classification using PHP's ordered time candidates."""
    timezone_name = str(account_rules.get("timezone") or "UTC")
    candidates: List[Dict[str, Any]] = []

    def add_time(raw: Any, basis: str) -> None:
        if raw is None or str(raw).strip() in {"", "—"}:
            return
        parts = _programme_time_parts(raw, timezone_name)
        if not parts:
            return
        key = f"{parts['date']}|{parts['hour']}:{parts['minute']}:{parts['second']}"
        if any(item["key"] == key for item in candidates):
            return
        candidates.append({"key": key, "raw": str(raw), "basis": basis, "parts": parts})

    # This order intentionally mirrors programme_training.php's
    # ptApplyProgrammeTradeRules() preference list. Prefer the original candle
    # value when its index is known: dt_text() stores offset-aware DB values in
    # UTC-naive DATETIME form, which must not then be reinterpreted as local wall time.
    if trigger_index is not None and 0 <= int(trigger_index) < len(candles):
        add_time(_raw_candle_timestamp(candles[int(trigger_index)]), "entry trigger")
    else:
        add_time(record.get("entry_trigger_time"), "entry trigger")
    add_time(record.get("entry_time"), "entry execution")
    if entry_index is not None and 0 <= int(entry_index) < len(candles):
        add_time(_raw_candle_timestamp(candles[int(entry_index)]), "Entry-at anchor candle")
    else:
        add_time(record.get("entry_anchor_time"), "Entry-at anchor")
    source_main_time = _source_main_candle_timestamp(parent_root, family, candles, parent_candles)
    add_time(source_main_time, "source-root identification candle")
    add_time(record.get("identification_time"), "identification candle")

    primary = next((item for item in candidates if item["basis"].startswith("entry trigger")), None)
    primary = primary or next((item for item in candidates if item["basis"].startswith("entry execution")), None)
    primary = primary or (candidates[0] if candidates else None)
    record["trade_entry_date"] = primary["parts"]["date"] if primary else None
    record["trade_entry_weekday"] = primary["parts"]["weekday"] if primary else None

    restricted_date_matches: List[Dict[str, Any]] = []
    for rule in account_rules.get("restricted_dates") or []:
        restricted_date = str((rule or {}).get("restricted_date") or "").strip()[:10]
        if not restricted_date:
            continue
        matched = next((item for item in candidates if item["parts"]["date"] == restricted_date), None)
        if matched:
            restricted_date_matches.append({
                "date": restricted_date, "matchedAt": matched["raw"], "matchBasis": matched["basis"],
            })

    restricted_day_matches: List[Dict[str, Any]] = []
    for rule in account_rules.get("restricted_days") or []:
        if not isinstance(rule, dict):
            continue
        configured_day = str(rule.get("day") or "").strip()
        configured_index = WEEKDAY_INDEX.get(configured_day.lower())
        from_minutes = _rule_time_minutes(rule.get("from_time"))
        to_minutes = _rule_time_minutes(rule.get("to_time"))
        if configured_index is None or from_minutes is None or to_minutes is None:
            continue
        matched = None
        for item in candidates:
            parts = item["parts"]
            weekday_index = int(parts["weekday_index"])
            time_minutes = float(parts["time_minutes"])
            if from_minutes == to_minutes:
                is_match = weekday_index == configured_index  # Equal times = full weekday.
            elif from_minutes < to_minutes:
                is_match = weekday_index == configured_index and from_minutes <= time_minutes <= to_minutes
            else:
                following_day = (configured_index + 1) % 7
                is_match = (
                    (weekday_index == configured_index and time_minutes >= from_minutes)
                    or (weekday_index == following_day and time_minutes <= to_minutes)
                )
            if is_match:
                matched = item
                break
        if matched:
            restricted_day_matches.append({
                "day": configured_day,
                "from_time": str(rule.get("from_time") or ""),
                "to_time": str(rule.get("to_time") or ""),
                "matchedDate": matched["parts"]["date"],
                "matchedAt": matched["raw"],
                "matchBasis": matched["basis"],
            })

    record["restricted_date_matches_json"] = safe_json(restricted_date_matches) if restricted_date_matches else None
    record["restricted_day_matches_json"] = safe_json(restricted_day_matches) if restricted_day_matches else None
    record["restricted_date_hit"] = int(bool(restricted_date_matches))
    record["restricted_day_hit"] = int(bool(restricted_day_matches))
    record["excluded_from_analytics"] = int(bool(restricted_date_matches or restricted_day_matches))
    record["analytics_exclusion_reason"] = "restricted trading window" if record["excluded_from_analytics"] else None
    return record


def trade_target_identity(root: Dict[str, Any], encoded: str, is_confirmation: bool) -> Optional[str]:
    if not encoded: return None
    parts = str(encoded).split("|")
    if len(parts) < 3: return None
    parent_target = parts[0] == "parent"
    source = root.get("parentTradeRoot") if parent_target else root
    tree_id = parts[1] if parent_target else parts[0]
    kind = parts[2] if parent_target else parts[1]
    fallback = parts[3] if parent_target and len(parts) > 3 else (parts[2] if len(parts) > 2 else "")
    if not source: source = root
    tree = next((t for t in source.get("trees", []) if str(t.get("id")) == str(tree_id)), None)
    if not tree: return None
    c = tree.get("main" if kind == "main" else "reference" if kind == "reference" else "offset") or tree.get("reference") or {}
    if kind == "main": identity = c.get("selectedExistingId") or c.get("offsetTo") or c.get("id") or fallback
    elif kind in {"reference", "offset"} and c.get("referenceMode") == "new_offset": identity = c.get("id") or fallback
    else: identity = c.get("selectedExistingId") or c.get("existingCandleId") or c.get("id") or fallback
    return ("parent:" + str(identity)) if parent_target else str(identity)


def resolve_parent_target(encoded: str, root: Dict[str, Any], family: Dict[str, Any],
                          parent_candles: Sequence[Dict[str, Any]], parent_tf: str,
                          lower_candles: Sequence[Dict[str, Any]], lower_tf: str,
                          proj: Sequence[Dict[str, int]], level: str) -> Optional[int]:
    if not encoded: return None
    parts = str(encoded).split("|")
    parent_target = parts[0] == "parent"
    if not parent_target:
        return as_int(family.get("map", {}).get(trade_target_identity(root, encoded, True)))
    ident = trade_target_identity(root, encoded, True)
    if not ident: return None
    parent_identity = ident[7:] if ident.startswith("parent:") else ident
    parent_index = as_int((family.get("parentFamily") or {}).get("map", {}).get(parent_identity))
    if parent_index is None or not (0 <= parent_index < len(proj)): return None
    piece = proj[parent_index]
    first, last = int(piece["first"]), int(piece["last"])
    if first < 0 or last < 0 or first >= len(lower_candles) or last >= len(lower_candles): return None
    normalized = str(level or "high").lower()
    if normalized == "open":
        return first
    if normalized == "close":
        return last
    # Match PHP ptProjectionPieceIndex(): only HIGH/LOW choose an extreme.
    # Every other derived level resolves to the middle lower-timeframe candle,
    # not the candle with the largest/smallest derived OHLC value.
    if normalized not in {"high", "low"}:
        return (first + last) // 2
    best, extreme = first, (float("inf") if normalized == "low" else -float("inf"))
    for idx in range(first, last + 1):
        c = lower_candles[idx]
        value = as_float(c.get("low" if normalized == "low" else "high"))
        if value is None:
            continue
        if (normalized == "low" and value < extreme) or (normalized == "high" and value > extreme):
            extreme, best = value, idx
    return best


def target_index(encoded: str, root: Dict[str, Any], family: Dict[str, Any],
                 candles: Sequence[Dict[str, Any]], parent_candles: Optional[Sequence[Dict[str, Any]]] = None,
                 parent_tf: str = "", lower_tf: str = "", proj: Optional[Sequence[Dict[str, int]]] = None,
                 level: str = "high", is_confirmation: bool = False) -> Optional[int]:
    if not encoded: return None
    key = trade_target_identity(root, encoded, is_confirmation)
    if key is None: return None
    if key.startswith("parent:"):
        if parent_candles is None or proj is None: return None
        return resolve_parent_target(encoded, root, family, parent_candles, parent_tf, candles, lower_tf, proj, level)
    return as_int((family.get("map") or {}).get(key))


def make_trade_record(parent_root: Dict[str, Any], trade_root: Dict[str, Any], family: Dict[str, Any],
                      candles: Sequence[Dict[str, Any]], timeframe: str, symbol: str,
                      programme_id: int, user_id: int, confirmation_root_id: int = 0,
                      confirmation_name: str = "", parent_candles: Optional[Sequence[Dict[str, Any]]] = None,
                      parent_tf: str = "", proj: Optional[Sequence[Dict[str, int]]] = None,
                      provider_rr: Optional[float] = None, provider_rr_type: str = "",
                      source_timeframe: Optional[str] = None,
                      trade_identified_at: Optional[str] = None,
                      setup_group_limit: Optional[int] = None,
                      account_rules: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    """Build one historical trade and stamp when this worker first identifies it.

    trade_time is deliberately different from the historical candle timestamps
    (entry_anchor_time/event_time): it records the scan's local wall-clock time.
    On later upserts, database persistence preserves the original non-NULL value.
    """
    if family.get("noTrade") or not trade_root.get("useForTrades"):
        return None

    if trade_identified_at is None:
        trade_identified_at = datetime.now().astimezone().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")

    entry_encoded = trade_root.get("entryFrom")
    sl_encoded = trade_root.get("stoplossFrom")
    tp_encoded = trade_root.get("takeProfitFrom")
    tp_type = str(trade_root.get("takeProfitType") or "risk_reward").strip().lower()
    if tp_type not in {"risk_reward", "price_levels"}:
        tp_type = "risk_reward"
    if not entry_encoded or not sl_encoded or (tp_type == "price_levels" and not tp_encoded):
        return None

    entry_level = str(trade_root.get("entryPriceLevel") or "high").lower().strip()
    sl_level = str(trade_root.get("stoplossPriceLevel") or "high").lower().strip()
    tp_level = str(trade_root.get("takeProfitPriceLevel") or "high").lower().strip()

    entry_idx = target_index(entry_encoded, trade_root, family, candles, parent_candles,
                             parent_tf, timeframe, proj, entry_level, confirmation_root_id > 0)
    sl_idx = target_index(sl_encoded, trade_root, family, candles, parent_candles,
                          parent_tf, timeframe, proj, sl_level, confirmation_root_id > 0)
    tp_idx = target_index(tp_encoded, trade_root, family, candles, parent_candles,
                          parent_tf, timeframe, proj, tp_level, confirmation_root_id > 0) if tp_type == "price_levels" else None
    if entry_idx is None or sl_idx is None or (tp_type == "price_levels" and tp_idx is None):
        return None
    if not (0 <= entry_idx < len(candles) and 0 <= sl_idx < len(candles)):
        return None

    entry_price = price(candles[entry_idx], entry_level)
    sl_price = price(candles[sl_idx], sl_level)
    if entry_price is None or sl_price is None:
        return None

    order = normalize_order(trade_root.get("orderType") or "buy")
    is_long = order.startswith("buy")
    risk_distance = abs(entry_price - sl_price)
    # The current PHP engine refuses to create a trade if its risk distance is zero.
    if not (risk_distance > 0):
        return None

    saved_rr = as_float(trade_root.get("riskReward"))
    has_provider_rr = provider_rr is not None and provider_rr > 0
    has_local_rr = saved_rr is not None and saved_rr > 0
    rr = provider_rr if has_provider_rr else (saved_rr if has_local_rr else 1.0)
    provision_rr_type = str(provider_rr_type or "fixed_risk_reward").strip().lower()
    if provision_rr_type not in {
        "fixed_risk_reward", "minimum_risk_reward", "custom_and_default_risk_reward"
    }:
        provision_rr_type = "fixed_risk_reward"

    provision_target = entry_price + (risk_distance * rr) if is_long else entry_price - (risk_distance * rr)
    custom_tp = price(candles[tp_idx], tp_level) if tp_type == "price_levels" else None
    configured_tp_price = provision_target if tp_type == "risk_reward" else custom_tp
    tp_price = configured_tp_price
    minimum_rr_applied = False

    if tp_type == "risk_reward":
        # Provider RR is authoritative; saved root RR is the fallback; 1:1 is legacy fallback.
        tp_price = provision_target
        configured_tp_price = tp_price
        effective_tp_type = "risk reward"
    else:
        actual_reward = None if custom_tp is None else (custom_tp - entry_price if is_long else entry_price - custom_tp)
        actual_rr = actual_reward / risk_distance if actual_reward is not None else None
        if has_provider_rr and provision_rr_type == "fixed_risk_reward":
            tp_price = provision_target
            effective_tp_type = "risk reward"
        elif has_provider_rr and provision_rr_type == "minimum_risk_reward":
            if actual_rr is None or actual_rr < rr:
                tp_price = provision_target
                minimum_rr_applied = True
                effective_tp_type = "risk reward"
            else:
                effective_tp_type = "price level"
        elif has_provider_rr and provision_rr_type == "custom_and_default_risk_reward":
            if actual_reward is None or actual_reward <= 0:
                tp_price = provision_target
                effective_tp_type = "risk reward"
            else:
                effective_tp_type = "price level"
        else:
            # Without a valid provision RR, custom price-level TP must provide at least 1R.
            if actual_reward is None or actual_reward < risk_distance:
                tp_price = entry_price + risk_distance if is_long else entry_price - risk_distance
                minimum_rr_applied = True
                effective_tp_type = "risk reward"
            else:
                effective_tp_type = "price level"

    if tp_price is None:
        return None

    trigger_idx = find_entry_trigger(candles, entry_idx, entry_price, order)
    if trigger_idx is not None and trigger_idx <= entry_idx:
        trigger_idx = None
    outcome_state = evaluate_outcome_with_breakeven(
        candles, trigger_idx, sl_price, tp_price, order, entry_price,
        (account_rules or {}).get("trades_breakeven") or [],
    )
    outcome = str(outcome_state["outcome"])
    sl_trigger = outcome_state.get("sl_trigger_index")
    tp_trigger = outcome_state.get("tp_trigger_index")
    breakeven_trigger = outcome_state.get("breakeven_trigger_index")
    reward_distance = abs(tp_price - entry_price)
    family_rr = reward_distance / risk_distance if risk_distance > 0 else None

    if sl_trigger is not None:
        event_idx = sl_trigger
    elif tp_trigger is not None:
        event_idx = tp_trigger
    elif trigger_idx is not None:
        event_idx = trigger_idx
    else:
        event_idx = entry_idx

    def candle_stamp(index: Optional[int]) -> Optional[str]:
        if index is None or index < 0 or index >= len(candles):
            return None
        candle = candles[index]
        return dt_text(candle.get("time") or candle.get("open_time"))

    effective_group_limit = (
        max(1, int(setup_group_limit)) if setup_group_limit is not None
        else max(1, as_int(family.get("_setupGroupLimit"), DEFAULT_TRADES_SETUP_PER_GROUP)
                 or DEFAULT_TRADES_SETUP_PER_GROUP)
    )
    apply_setup_group_identity(family, effective_group_limit)
    family_ordinal, setup_group_id, group_family_number = family_group_identity(family)
    signature = family_identity_signature(
        int(parent_root["id"]), timeframe, confirmation_root_id,
        family.get("map") or {}, candles,
    )
    family_map = {
        str(identity): candles[int(index)].get("id")
        for identity, index in (family.get("map") or {}).items()
        if as_int(index) is not None and 0 <= int(index) < len(candles)
    }

    record = {
        "programme_id": programme_id,
        "userid": user_id,
        "symbol": symbol,
        "timeframe": timeframe.lower(),
        "source_timeframe": (source_timeframe or timeframe).lower(),
        "root_id": int(parent_root["id"]),
        "root_name": parent_root.get("name") or f"Root {parent_root['id']}",
        "confirmation_root_id": int(confirmation_root_id),
        "confirmation_root_name": confirmation_name or None,
        "family_number": group_family_number,
        "setup_group_id": setup_group_id,
        # Internal-only ordinal: it is used by incremental matching/cache logic and
        # is deliberately omitted from ANALYTICS_COLUMNS/database storage.
        "family_ordinal": family_ordinal,
        "family_signature": signature,
        # Trade identity is based on the actual executed price triplet, not the
        # family candle map or any displayed setup/group/root numbering.
        "trade_identity_hash": None,
        "family_map_json": safe_json(family_map),
        "match_json": safe_json(family.get("conditions") or []),
        "matched_family": 1,
        "trade_configured": 1,
        "live_trades": 0,
        "order_type": order,
        "direction": "long" if is_long else "short",
        "entry_name": entry_encoded,
        "stoploss_name": sl_encoded,
        "take_profit_name": tp_encoded,
        "entry_anchor_time": candle_stamp(entry_idx),
        "entry_trigger_time": candle_stamp(trigger_idx),
        "stoploss_time": candle_stamp(sl_idx),
        "take_profit_time": candle_stamp(tp_idx),
        "stoploss_trigger_time": candle_stamp(sl_trigger),
        "take_profit_trigger_time": candle_stamp(tp_trigger),
        "event_time": candle_stamp(event_idx),
        "trade_time": trade_identified_at,
        "entry_price": entry_price,
        "stoploss_price": sl_price,
        "take_profit_price": tp_price,
        "configured_take_profit_price": configured_tp_price,
        "risk_reward": family_rr,
        "configured_risk_reward": rr if (has_provider_rr or has_local_rr) else None,
        "minimum_risk_reward_applied": int(minimum_rr_applied),
        "outcome": outcome,
        "stoploss_hit": int(outcome == "stoploss hit"),
        "profit_reached": int(outcome == "take profit reached"),
        "pending_entry": int(outcome == "pending entry"),
        "in_trade": int(outcome == "in trade"),
        "closed_trade": int(outcome in {"stoploss hit", "breakeven hit", "take profit reached"}),
        "trade_value_r": -1.0 if outcome == "stoploss hit" else (max(0.0, family_rr or 0.0) if outcome == "take profit reached" else 0.0),
        "effective_take_profit_type": effective_tp_type,
        "breakeven_triggered": int(bool(outcome_state.get("breakeven_triggered"))),
        "breakeven_floating_profit_rr": outcome_state.get("breakeven_floating_profit_rr"),
        "breakeven_stop_rr": outcome_state.get("breakeven_stop_rr"),
        "breakeven_stop_price": outcome_state.get("breakeven_stop_price"),
        "breakeven_trigger_time": candle_stamp(breakeven_trigger),
        "breakeven_stop_hit": int(outcome == "breakeven hit"),
        "max_floating_rr": float(outcome_state.get("max_floating_rr") or 0.0),
        "restricted_date_hit": 0,
        "restricted_day_hit": 0,
        "restricted_date_matches_json": None,
        "restricted_day_matches_json": None,
        "excluded_from_analytics": 0,
        "analytics_exclusion_reason": None,
    }
    apply_account_management_trade_rules(
        record, candles, trigger_idx, entry_idx, family, parent_root,
        parent_candles, account_rules or {"timezone": "UTC"},
    )
    record["trade_identity_hash"] = _trade_identity_hash(record)
    return record

def match_row(programme_id: int, user_id: int, symbol: str, timeframe: str,
              root: Dict[str, Any], family: Dict[str, Any], candles: Sequence[Dict[str, Any]],
              confirmation_root_id: int = 0, confirmation_name: str = "",
              source_timeframe: Optional[str] = None) -> Dict[str, Any]:
    family_ordinal, setup_group_id, group_family_number = family_group_identity(family)
    return {
        "programme_id": programme_id, "userid": user_id, "symbol": symbol,
        "timeframe": timeframe.lower(), "source_timeframe": (source_timeframe or timeframe).lower(),
        "root_id": int(root["id"]), "root_name": root.get("name") or f"Root {root['id']}",
        "confirmation_root_id": int(confirmation_root_id), "confirmation_root_name": confirmation_name or None,
        "family_number": group_family_number,
        "setup_group_id": setup_group_id,
        "family_ordinal": family_ordinal,
        "family_signature": family_identity_signature(int(root["id"]), timeframe, confirmation_root_id, family.get("map") or {}, candles),
        "trade_identity_hash": None,
        "family_map_json": safe_json({k: candles[int(v)].get("id") for k, v in (family.get("map") or {}).items() if 0 <= int(v) < len(candles)}),
        "match_json": safe_json(family.get("conditions") or []),
        "matched_family": 1, "trade_configured": 0, "live_trades": 0, "order_type": None,
        "direction": None, "entry_name": None, "stoploss_name": None, "take_profit_name": None,
        "entry_anchor_time": None, "entry_trigger_time": None, "stoploss_time": None,
        "take_profit_time": None, "stoploss_trigger_time": None, "take_profit_trigger_time": None,
        "event_time": dt_text(candles[max(family.get("map", {}).values(), default=0)].get("time")),
        "trade_time": None,
        "entry_price": None, "stoploss_price": None, "take_profit_price": None,
        "configured_take_profit_price": None, "risk_reward": None, "configured_risk_reward": None,
        "minimum_risk_reward_applied": 0,
        "outcome": "identify but don't trade" if family.get("noTrade") else "matched family",
        "stoploss_hit": 0, "profit_reached": 0, "pending_entry": 0, "in_trade": 0,
        "closed_trade": 0, "trade_value_r": 0.0,
        "restricted_date_hit": 0, "restricted_day_hit": 0,
        "restricted_date_matches_json": None, "restricted_day_matches_json": None,
        "excluded_from_analytics": 0, "analytics_exclusion_reason": None,
        "breakeven_triggered": 0, "breakeven_floating_profit_rr": None,
        "breakeven_stop_rr": None, "breakeven_stop_price": None,
        "breakeven_trigger_time": None, "breakeven_stop_hit": 0, "max_floating_rr": 0.0,
    }


# ------------------------------- VPS programme scope -------------------------------
def detect_vps_ip() -> Optional[str]:
    """Detect the same outbound-interface IP used by brokerdetails.py."""
    configured = os.environ.get("HARVHUB_VPS_IP", "").strip()
    if configured:
        return configured
    sock = None
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # UDP connect selects the route/interface; it does not send application data.
        sock.connect(("8.8.8.8", 80))
        ip = str(sock.getsockname()[0] or "").strip()
        return ip or None
    except OSError:
        try:
            ip = socket.gethostbyname(socket.gethostname())
            return ip if ip and not ip.startswith("127.") else None
        except OSError:
            return None
    finally:
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass


def _in_sql(values: Sequence[int]) -> Tuple[str, List[int]]:
    clean = sorted({int(value) for value in values if int(value) > 0})
    if not clean:
        return "", []
    return ",".join(["%s"] * len(clean)), clean


def discover_programmes(
    db: BridgeClient,
    programme_id: Optional[int] = None,
    vps_ip: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Return only eligible programmes attached to this VPS.

    VPS resolution intentionally happens before the programme/advertisement or
    Begin-Test checks. Owner rows match programme_vps.programme_vps_ip_address
    to the current VPS IP; active follower rows are then expanded from those
    owner programme IDs, matching brokerdetails.py's programme discovery.
    """
    local_ip = (vps_ip or detect_vps_ip() or "").strip()
    if not local_ip:
        log.error("VPS scope unavailable: could not determine this VPS IP; refusing an unscoped scan")
        return []
    if not table_exists(db, "programme_vps"):
        log.error("VPS scope unavailable: programme_vps table does not exist; refusing an unscoped scan")
        return []

    required_vps_columns = {
        "id", "programme_id", "user_id", "programme_vps_ip_address"
    }
    vps_columns = {column.lower() for column in get_columns(db, "programme_vps")}
    if not required_vps_columns.issubset(vps_columns):
        missing = sorted(required_vps_columns - vps_columns)
        raise RuntimeError("programme_vps is missing required VPS-binding column(s): " + ", ".join(missing))

    owner_rows = db.fetch_all(
        "SELECT `id`,`programme_id`,`user_id`,`programme_vps_ip_address` "
        "FROM `programme_vps` WHERE `programme_vps_ip_address`=%s ORDER BY `id` ASC",
        [local_ip], timeout=90,
    )
    owner_ids: List[int] = []
    owner_user_by_programme: Dict[int, int] = {}
    for row in owner_rows:
        pid = as_int(row.get("programme_id"), 0) or 0
        uid = as_int(row.get("user_id"), 0) or 0
        if pid <= 0 or uid <= 0:
            continue
        owner_ids.append(pid)
        owner_user_by_programme[pid] = uid

    programme_roles: Dict[int, set[str]] = {
        pid: {"owner"} for pid in owner_ids
    }
    follower_owner_by_programme: Dict[int, set[int]] = {}
    follower_rows: List[Dict[str, Any]] = []
    if owner_ids and table_exists(db, "programme_vps_hosts_followers"):
        marks, params = _in_sql(owner_ids)
        follower_rows = db.fetch_all(
            "SELECT `id`,`owner_programme_id`,`follower_programme_id`,`host_status` "
            "FROM `programme_vps_hosts_followers` "
            f"WHERE `owner_programme_id` IN ({marks}) AND `host_status`='active' "
            "ORDER BY `id` ASC",
            params, timeout=90,
        )
        for row in follower_rows:
            follower_pid = as_int(row.get("follower_programme_id"), 0) or 0
            owner_pid = as_int(row.get("owner_programme_id"), 0) or 0
            if follower_pid <= 0 or owner_pid not in owner_ids:
                continue
            programme_roles.setdefault(follower_pid, set()).add("active follower")
            follower_owner_by_programme.setdefault(follower_pid, set()).add(owner_pid)

    associated_ids = sorted(programme_roles)
    if not associated_ids:
        log.info("VPS scope: no programme bindings found for VPS IP %s", local_ip)
        return []

    marks, params = _in_sql(associated_ids)
    programme_rows = db.fetch_all(
        f"SELECT * FROM `programme` WHERE `id` IN ({marks}) ORDER BY `id` ASC",
        params, timeout=120,
    )
    if not programme_rows:
        log.info("VPS scope: %s binding(s) for %s, but no matching programme rows", len(associated_ids), local_ip)
        return []

    latest_interest: Dict[Tuple[int, int], Dict[str, Any]] = {}
    if table_exists(db, "signals_provider_interest"):
        marks, params = _in_sql(associated_ids)
        interest_rows = db.fetch_all(
            f"SELECT * FROM `signals_provider_interest` WHERE `programme_id` IN ({marks}) "
            "ORDER BY `id` DESC",
            params, timeout=120,
        )
        for row in interest_rows:
            pid = as_int(row.get("programme_id"), 0) or 0
            uid = as_int(row.get("user_id"), 0) or 0
            if pid <= 0 or uid <= 0:
                continue
            latest_interest.setdefault((pid, uid), row)

    eligible: List[Dict[str, Any]] = []
    skipped_in_vps_scope = 0
    for programme in programme_rows:
        pid = as_int(programme.get("id"), 0) or 0
        uid = as_int(programme.get("userid"), 0) or 0
        if pid <= 0 or uid <= 0 or pid not in programme_roles:
            continue
        if pid in owner_user_by_programme and owner_user_by_programme[pid] != uid:
            log.warning(
                "Ignoring mismatched VPS owner binding: programme=%s database_owner=%s vps_owner=%s",
                pid, uid, owner_user_by_programme[pid],
            )
            continue
        interest = latest_interest.get((pid, uid), {})
        begin_test = as_int(interest.get("begin_test"), 0) or 0
        advertisement = as_int(programme.get("advertisement"), 0) or 0

        programme["vps_associated"] = 1
        programme["vps_ip_address"] = local_ip
        programme["vps_role"] = ", ".join(sorted(programme_roles.get(pid, set())))
        programme["begin_test_interest_id"] = interest.get("id")
        programme["begin_test_flag"] = begin_test
        programme["begin_test_status"] = interest.get("interest_status")
        programme["begin_test_started_at"] = interest.get("signal_testing_started_at")
        programme["begin_test_interest"] = interest

        # VPS membership is necessary but not sufficient. Advertisement OR a
        # successful Begin Test is the only programme execution condition.
        if advertisement != 1 and begin_test != 1:
            skipped_in_vps_scope += 1
            continue
        eligible.append(programme)

    if programme_id is not None:
        eligible = [row for row in eligible if as_int(row.get("id"), 0) == int(programme_id)]
        if not eligible:
            log.warning(
                "Programme %s is not eligible on VPS %s: it must be VPS-associated and have "
                "advertisement=1 or the newest provision interest begin_test=1",
                programme_id, local_ip,
            )

    log.info(
        "VPS-first programme scope | VPS IP=%s | owner bindings=%s | active follower bindings=%s | "
        "associated programme rows=%s | skipped (neither advertised nor testing)=%s | eligible=%s%s",
        local_ip, len(owner_rows), len(follower_rows), len(programme_rows),
        skipped_in_vps_scope, len(eligible),
        f" | programme filter={programme_id}" if programme_id is not None else "",
    )
    return eligible


# ------------------------------- DB operations --------------------------------
ANALYTICS_COLUMNS = [
    "programme_id", "userid", "symbol", "timeframe", "source_timeframe", "root_id", "root_name",
    "confirmation_root_id", "confirmation_root_name", "family_number", "setup_group_id", "family_signature", "trade_identity_hash", "family_map_json",
    "match_json", "matched_family", "trade_configured", "live_trades", "order_type", "direction", "entry_name", "stoploss_name",
    "take_profit_name", "entry_anchor_time", "entry_trigger_time", "stoploss_time", "take_profit_time",
    "stoploss_trigger_time", "take_profit_trigger_time", "event_time", "trade_time", "entry_price", "stoploss_price",
    "take_profit_price", "configured_take_profit_price", "risk_reward", "configured_risk_reward",
    "minimum_risk_reward_applied", "outcome", "stoploss_hit", "profit_reached", "pending_entry", "in_trade",
    "closed_trade", "trade_value_r",
    "restricted_date_hit", "restricted_day_hit", "restricted_date_matches_json",
    "restricted_day_matches_json", "excluded_from_analytics", "analytics_exclusion_reason",
    "breakeven_triggered", "breakeven_floating_profit_rr", "breakeven_stop_rr",
    "breakeven_stop_price", "breakeven_trigger_time", "breakeven_stop_hit", "max_floating_rr",
]

ANALYTICS_ACCOUNT_RULE_COLUMN_DEFINITIONS = {
    "restricted_date_hit": "TINYINT(1) NOT NULL DEFAULT 0",
    "restricted_day_hit": "TINYINT(1) NOT NULL DEFAULT 0",
    "restricted_date_matches_json": "MEDIUMTEXT NULL DEFAULT NULL",
    "restricted_day_matches_json": "MEDIUMTEXT NULL DEFAULT NULL",
    "excluded_from_analytics": "TINYINT(1) NOT NULL DEFAULT 0",
    "analytics_exclusion_reason": "VARCHAR(120) NULL DEFAULT NULL",
    "breakeven_triggered": "TINYINT(1) NOT NULL DEFAULT 0",
    "breakeven_floating_profit_rr": "DECIMAL(20,8) NULL DEFAULT NULL",
    "breakeven_stop_rr": "DECIMAL(20,8) NULL DEFAULT NULL",
    "breakeven_stop_price": "DECIMAL(30,12) NULL DEFAULT NULL",
    "breakeven_trigger_time": "DATETIME NULL DEFAULT NULL",
    "breakeven_stop_hit": "TINYINT(1) NOT NULL DEFAULT 0",
    "max_floating_rr": "DECIMAL(20,8) NOT NULL DEFAULT 0",
}


def load_candles(db: BridgeClient, programme_id: int, user_id: int,
                 symbol: str, timeframe: str) -> List[Dict[str, Any]]:
    """Load exactly the latest PHP chart window for one programme pair.

    programme_training.php uses PT_PAGE_SIZE=500 and PT_DEFAULT_AMOUNT=2000.
    Its first page is newest-first; older pages use candle_time < oldest_time,
    then each page is reversed and prepended. Reproduce those same page sizes,
    exclusive cursor semantics, programme/user/symbol/timeframe filters, and
    final oldest-to-newest ordering. This intentionally does NOT read older DB
    candles after the PHP chart's 2,000-candle default window is full.
    """
    pages_desc: List[List[Dict[str, Any]]] = []
    before: Optional[str] = None
    loaded = 0

    while loaded < CHART_CANDLE_LIMIT:
        page_size = min(CANDLE_PAGE_SIZE, CHART_CANDLE_LIMIT - loaded)
        where_before = " AND `candle_time` < %s" if before else ""
        params: List[Any] = [programme_id, user_id, symbol, timeframe]
        if before:
            params.append(before)
        sql = (
            "SELECT id,candle_time,open_time,close_time,open,high,low,`close`, "
            "candle_center,body_center,high_wick_center,low_wick_center, "
            "candle_width_center,volume "
            "FROM `candle_records` WHERE `programmeid`=%s AND `userid`=%s "
            "AND `symbol`=%s AND `timeframe`=%s" + where_before +
            f" ORDER BY `candle_time` DESC LIMIT {int(page_size)}"
        )
        rows = db.fetch_all(sql, params, timeout=120)
        if not rows:
            break

        pages_desc.append(rows)
        loaded += len(rows)
        oldest = rows[-1].get("candle_time")
        if oldest is None or str(oldest).strip() == "":
            log.warning(
                "    %s %s: PHP-equivalent candle pagination stopped because the oldest row has no candle_time",
                symbol, timeframe.upper(),
            )
            break
        if len(rows) < page_size:
            break
        if before is not None and str(oldest) >= str(before):
            log.warning(
                "    %s %s: PHP-equivalent candle pagination made no progress at %s",
                symbol, timeframe.upper(), oldest,
            )
            break
        before = str(oldest)

    # PHP prepends each older, individually reversed page. This is equivalent
    # to reversing the page sequence and then reversing rows within each page.
    chronological_rows: List[Dict[str, Any]] = []
    for page in reversed(pages_desc):
        chronological_rows.extend(reversed(page))

    candles: List[Dict[str, Any]] = []
    for row in chronological_rows[:CHART_CANDLE_LIMIT]:
        candles.append({
            "id": as_int(row.get("id")),
            "time": row.get("candle_time"),
            "open_time": row.get("open_time"),
            "close_time": row.get("close_time"),
            "open": as_float(row.get("open")),
            "high": as_float(row.get("high")),
            "low": as_float(row.get("low")),
            "close": as_float(row.get("close")),
            "candle_center": as_float(row.get("candle_center")),
            "body_center": as_float(row.get("body_center")),
            "high_wick_center": as_float(row.get("high_wick_center")),
            "low_wick_center": as_float(row.get("low_wick_center")),
            "candle_width_center": as_float(row.get("candle_width_center")),
            "volume": as_float(row.get("volume")),
        })
    return candles


def _parse_family_map_candle_ids(raw_json: Any) -> Optional[set[str]]:
    """Return candle IDs in family_map_json; None means the map is unsafe to judge."""
    if raw_json is None:
        return None
    try:
        parsed = json.loads(str(raw_json))
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(parsed, dict) or not parsed:
        return None
    candle_ids: set[str] = set()
    for value in parsed.values():
        candle_id = as_int(value)
        if candle_id is None or candle_id <= 0:
            return None
        candle_ids.add(str(candle_id))
    return candle_ids or None


def _delete_mapped_programme_trades_for_stale_analytics(
    db: BridgeClient, programme_id: int, user_id: int, analytics_row_ids: Sequence[int]
) -> int:
    """Delete only programme-owned trade rows mapped to analytics rows being pruned.

    programme_trade_chart_identity is the link created by Programme Analytics.
    The investor_id IS NULL predicate is deliberate: investor and investor
    sub-account copies are not owned by this worker and must never be deleted.
    """
    if not analytics_row_ids:
        return 0
    if not table_exists(db, "programme_trade_chart_identity") or not table_exists(db, "programme_trades"):
        return 0

    trade_columns = {column.lower() for column in get_columns(db, "programme_trades")}
    identity_columns = {column.lower() for column in get_columns(db, "programme_trade_chart_identity")}
    required_trade = {"id", "userid", "programmeid", "investor_id"}
    required_identity = {
        "programme_trade_id", "userid", "programmeid", "root_id",
        "confirmation_root_id", "symbol", "timeframe", "source_timeframe", "family_signature",
    }
    if not required_trade.issubset(trade_columns) or not required_identity.issubset(identity_columns):
        log.warning(
            "    Stale mapped programme_trades cleanup skipped: required identity/ownership columns are unavailable."
        )
        return 0

    deleted = 0
    for offset in range(0, len(analytics_row_ids), 100):
        batch = [int(value) for value in analytics_row_ids[offset:offset + 100] if int(value) > 0]
        if not batch:
            continue
        placeholders = ",".join(["%s"] * len(batch))
        sql = (
            "DELETE ci, pt "
            "FROM `programme_trade_chart_identity` AS ci "
            "INNER JOIN `programme_trades` AS pt "
            "ON pt.`id`=ci.`programme_trade_id` "
            "AND pt.`userid`=ci.`userid` AND pt.`programmeid`=ci.`programmeid` "
            "INNER JOIN `programme_analytics` AS a ON "
            "a.`programme_id`=ci.`programmeid` AND a.`userid`=ci.`userid` "
            "AND a.`root_id` <=> ci.`root_id` "
            "AND COALESCE(a.`confirmation_root_id`,0)=COALESCE(ci.`confirmation_root_id`,0) "
            "AND UPPER(TRIM(a.`symbol`))=UPPER(TRIM(ci.`symbol`)) "
            "AND LOWER(TRIM(a.`timeframe`))=LOWER(TRIM(ci.`timeframe`)) "
            "AND a.`source_timeframe` <=> ci.`source_timeframe` "
            "AND a.`family_signature`=ci.`family_signature` "
            f"WHERE a.`id` IN ({placeholders}) "
            "AND a.`programme_id`=%s AND a.`userid`=%s "
            "AND ci.`programmeid`=%s AND ci.`userid`=%s "
            "AND pt.`programmeid`=%s AND pt.`userid`=%s AND pt.`investor_id` IS NULL"
        )
        if "tradeid" in trade_columns:
            sql += " AND pt.`tradeid` IS NULL"
        for owner_column in ("investor_sub_account_id", "sub_account_id", "accountmanagement_id", "investor_account_id"):
            if owner_column in trade_columns:
                sql += f" AND COALESCE(pt.`{owner_column}`,0)=0"
        params: List[Any] = batch + [programme_id, user_id, programme_id, user_id, programme_id, user_id]
        deleted += db.execute(sql, params, timeout=120)
    return max(0, int(deleted or 0))


def prune_analytics_rows_without_chart_candles(
    db: BridgeClient,
    programme_id: int,
    user_id: int,
    active_root_ids: Sequence[int],
    required_pairs: Sequence[Tuple[str, str]],
    current_candle_ids: Dict[Tuple[str, str], set[str]],
) -> Dict[str, int]:
    """Remove analytics families whose candle IDs are outside the PHP chart window.

    Only rows belonging to the current programme/owner, active chart roots, and
    candle pairs loaded successfully in this scan are considered. An unreadable
    or empty family map is retained (fail-safe); a readable map is stale when
    ANY referenced candle ID is no longer in that pair's current 2,000-candle
    chart window. Linked programme-level trades are deleted before analytics.
    """
    if not table_exists(db, "programme_analytics"):
        return {"analytics_removed": 0, "mapped_programme_trades_removed": 0}

    root_ids = sorted({int(value) for value in active_root_ids if int(value) > 0})
    pair_keys = {(str(symbol).casefold(), str(tf).casefold()) for symbol, tf in required_pairs}
    if not root_ids or not pair_keys:
        return {"analytics_removed": 0, "mapped_programme_trades_removed": 0}

    columns = {column.lower() for column in get_columns(db, "programme_analytics")}
    required_columns = {"id", "programme_id", "userid", "root_id", "symbol", "timeframe", "family_map_json"}
    missing = sorted(required_columns - columns)
    if missing:
        log.warning(
            "    Stale chart-family cleanup skipped because programme_analytics lacks: %s",
            ", ".join(missing),
        )
        return {"analytics_removed": 0, "mapped_programme_trades_removed": 0}

    root_placeholders = ",".join(["%s"] * len(root_ids))
    last_id = 0
    stale_rows: List[Dict[str, Any]] = []
    unreadable = 0
    while True:
        page = db.fetch_all(
            "SELECT `id`,`root_id`,`symbol`,`timeframe`,"
            "HEX(`family_map_json`) AS `family_map_json_hex` "
            "FROM `programme_analytics` WHERE `programme_id`=%s AND `userid`=%s "
            f"AND `root_id` IN ({root_placeholders}) AND `id`>%s "
            "ORDER BY `id` ASC LIMIT " + str(ANALYTICS_VALIDATE_PAGE_SIZE),
            [programme_id, user_id] + root_ids + [last_id], timeout=120,
        )
        if not page:
            break
        for row in page:
            row_id = as_int(row.get("id"), 0) or 0
            last_id = max(last_id, row_id)
            pair_key = (str(row.get("symbol") or "").casefold(), str(row.get("timeframe") or "").casefold())
            if pair_key not in pair_keys:
                continue
            raw_map = decode_hex_text_or_plain(
                row, "family_map_json_hex", "family_map_json", "{}"
            )
            map_ids = _parse_family_map_candle_ids(raw_map)
            if map_ids is None:
                unreadable += 1
                continue
            present_ids = current_candle_ids.get(pair_key, set())
            missing_ids = map_ids - present_ids
            if missing_ids:
                row["__stale_missing_ids"] = sorted(missing_ids)
                stale_rows.append(row)
        if len(page) < ANALYTICS_VALIDATE_PAGE_SIZE:
            break

    if unreadable:
        log.warning(
            "    Chart-window validation retained %s row(s) with empty/malformed family maps (fail-safe).",
            unreadable,
        )
    if not stale_rows:
        log.info("    Chart-window validation: no orphaned Programme Analytics candle maps found.")
        return {"analytics_removed": 0, "mapped_programme_trades_removed": 0}

    stale_ids = [int(row["id"]) for row in stale_rows]
    mapped_trade_rows_removed = _delete_mapped_programme_trades_for_stale_analytics(
        db, programme_id, user_id, stale_ids
    )
    analytics_removed = 0
    for offset in range(0, len(stale_ids), 250):
        batch = stale_ids[offset:offset + 250]
        placeholders = ",".join(["%s"] * len(batch))
        analytics_removed += db.execute(
            f"DELETE FROM `programme_analytics` WHERE `programme_id`=%s AND `userid`=%s AND `id` IN ({placeholders})",
            [programme_id, user_id] + batch, timeout=120,
        )

    log.warning(
        "    CHART-WINDOW CLEANUP: removed %s stale analytics row(s) and %s mapped programme-level trade/mapping row(s). "
        "Investor/sub-account trade rows were excluded.",
        max(0, int(analytics_removed or 0)), max(0, int(mapped_trade_rows_removed or 0)),
    )
    return {
        "analytics_removed": max(0, int(analytics_removed or 0)),
        "mapped_programme_trades_removed": max(0, int(mapped_trade_rows_removed or 0)),
    }



def _chart_family_scope_key(symbol: Any, timeframe: Any, root_id: Any,
                            confirmation_root_id: Any, source_timeframe: Any) -> Tuple[str, str, int, int, str]:
    """Canonical key for one exact programme-training chart family stream."""
    return (
        str(symbol or "").strip().casefold(),
        str(timeframe or "").strip().casefold(),
        as_int(root_id, 0) or 0,
        as_int(confirmation_root_id, 0) or 0,
        str(source_timeframe or "").strip().casefold(),
    )


def prune_analytics_rows_not_in_current_chart(
    db: BridgeClient,
    programme_id: int,
    user_id: int,
    validated_chart_scopes: Sequence[Tuple[str, str, int, int, str]],
    current_family_signatures: Dict[Tuple[str, str, int, int, str], set[str]],
) -> Dict[str, int]:
    """Remove programme analytics families that this exact chart scan no longer produces.

    This is stronger than a candle-ID presence check: a stored family is retained only
    when its exact (symbol, timeframe, root, confirmation, source timeframe,
    family_signature) is present in the current PHP-equivalent matcher output. It also
    removes families belonging to disabled/removed roots and obsolete timeframes.
    Deletions are ownership-scoped and remove only mapped programme-owned trade rows.
    """
    if not table_exists(db, "programme_analytics"):
        return {"analytics_removed": 0, "mapped_programme_trades_removed": 0}

    scopes = set(validated_chart_scopes)
    columns = {column.lower() for column in get_columns(db, "programme_analytics")}
    required = {
        "id", "programme_id", "userid", "root_id", "confirmation_root_id",
        "symbol", "timeframe", "source_timeframe", "family_signature",
    }
    missing = sorted(required - columns)
    if missing:
        log.warning(
            "    Exact chart-family cleanup skipped; programme_analytics lacks: %s",
            ", ".join(missing),
        )
        return {"analytics_removed": 0, "mapped_programme_trades_removed": 0}

    last_id = 0
    stale_ids: List[int] = []
    while True:
        page = db.fetch_all(
            "SELECT `id`,`root_id`,`confirmation_root_id`,`symbol`,`timeframe`,`source_timeframe`,"
            "HEX(`family_signature`) AS `family_signature_hex` "
            "FROM `programme_analytics` WHERE `programme_id`=%s AND `userid`=%s AND `id`>%s "
            "ORDER BY `id` ASC LIMIT " + str(ANALYTICS_VALIDATE_PAGE_SIZE),
            [programme_id, user_id, last_id], timeout=120,
        )
        if not page:
            break
        for row in page:
            row_id = as_int(row.get("id"), 0) or 0
            last_id = max(last_id, row_id)
            scope = _chart_family_scope_key(
                row.get("symbol"), row.get("timeframe"), row.get("root_id"),
                row.get("confirmation_root_id"), row.get("source_timeframe"),
            )
            # A row outside every validated chart stream is no longer part of the
            # active chart, including rows left by removed roots/timeframes.
            if scope not in scopes:
                stale_ids.append(row_id)
                continue
            signature = decode_hex_text_or_plain(
                row, "family_signature_hex", "family_signature", ""
            ).strip()
            if not signature or signature not in current_family_signatures.get(scope, set()):
                stale_ids.append(row_id)
        if len(page) < ANALYTICS_VALIDATE_PAGE_SIZE:
            break

    if not stale_ids:
        log.info("    Exact chart-family validation: every Programme Analytics row exists in the current chart output.")
        return {"analytics_removed": 0, "mapped_programme_trades_removed": 0}

    mapped_removed = _delete_mapped_programme_trades_for_stale_analytics(
        db, programme_id, user_id, stale_ids
    )
    analytics_removed = 0
    for offset in range(0, len(stale_ids), 250):
        batch = stale_ids[offset:offset + 250]
        placeholders = ",".join(["%s"] * len(batch))
        analytics_removed += db.execute(
            f"DELETE FROM `programme_analytics` WHERE `programme_id`=%s AND `userid`=%s AND `id` IN ({placeholders})",
            [programme_id, user_id] + batch, timeout=120,
        )

    log.warning(
        "    EXACT CHART CLEANUP: removed %s Programme Analytics row(s) and %s linked programme-owned trade/mapping row(s); "
        "investor/sub-account rows were excluded.",
        max(0, int(analytics_removed or 0)), max(0, int(mapped_removed or 0)),
    )
    return {
        "analytics_removed": max(0, int(analytics_removed or 0)),
        "mapped_programme_trades_removed": max(0, int(mapped_removed or 0)),
    }


def delete_orphaned_mapped_programme_trades(db: BridgeClient, programme_id: int, user_id: int) -> int:
    """Remove stale mapped programme trades whose Analytics family row is already gone."""
    if not table_exists(db, "programme_trade_chart_identity") or not table_exists(db, "programme_trades"):
        return 0
    if not table_exists(db, "programme_analytics"):
        return 0
    trade_columns = {column.lower() for column in get_columns(db, "programme_trades")}
    identity_columns = {column.lower() for column in get_columns(db, "programme_trade_chart_identity")}
    analytics_columns = {column.lower() for column in get_columns(db, "programme_analytics")}
    if not {"id", "userid", "programmeid", "investor_id"}.issubset(trade_columns):
        return 0
    if not {
        "programme_trade_id", "userid", "programmeid", "root_id",
        "confirmation_root_id", "symbol", "timeframe", "source_timeframe", "family_signature",
    }.issubset(identity_columns):
        return 0
    if not {
        "id", "programme_id", "userid", "root_id", "confirmation_root_id",
        "symbol", "timeframe", "source_timeframe", "family_signature",
    }.issubset(analytics_columns):
        return 0

    sql = (
        "DELETE ci,pt FROM `programme_trade_chart_identity` AS ci "
        "INNER JOIN `programme_trades` AS pt ON pt.`id`=ci.`programme_trade_id` "
        "AND pt.`userid`=ci.`userid` AND pt.`programmeid`=ci.`programmeid` "
        "LEFT JOIN `programme_analytics` AS a ON a.`programme_id`=ci.`programmeid` "
        "AND a.`userid`=ci.`userid` AND a.`root_id` <=> ci.`root_id` "
        "AND COALESCE(a.`confirmation_root_id`,0)=COALESCE(ci.`confirmation_root_id`,0) "
        "AND UPPER(TRIM(a.`symbol`))=UPPER(TRIM(ci.`symbol`)) "
        "AND LOWER(TRIM(a.`timeframe`))=LOWER(TRIM(ci.`timeframe`)) "
        "AND a.`source_timeframe` <=> ci.`source_timeframe` "
        "AND a.`family_signature`=ci.`family_signature` "
        "WHERE ci.`userid`=%s AND ci.`programmeid`=%s "
        "AND pt.`userid`=%s AND pt.`programmeid`=%s "
        "AND pt.`investor_id` IS NULL AND a.`id` IS NULL"
    )
    if "tradeid" in trade_columns:
        sql += " AND pt.`tradeid` IS NULL"
    for owner_column in ("investor_sub_account_id", "sub_account_id", "accountmanagement_id", "investor_account_id"):
        if owner_column in trade_columns:
            sql += f" AND COALESCE(pt.`{owner_column}`,0)=0"
    removed = max(0, int(db.execute(sql, [user_id, programme_id, user_id, programme_id], timeout=180) or 0))
    if removed:
        log.warning(
            "    ORPHANED PROGRAMME TRADE CLEANUP: removed %s mapped programme-owned trade/mapping row(s); "
            "investor/sub-account and broker-ticket rows were excluded.", removed,
        )
    return removed

def _is_unassigned_programme_trade_owner(value: Any) -> bool:
    """Return True only when an optional investor/sub-account owner is empty/zero.

    Unknown non-numeric values are deliberately treated as assigned so cleanup
    fails closed instead of risking an investor/sub-account record.
    """
    value = normalise_db_value(value)
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    try:
        number = Decimal(str(value).strip())
        return number.is_finite() and number == 0
    except (InvalidOperation, ValueError, TypeError):
        return False


def _programme_trade_price_identity(
    row: Dict[str, Any], *, analytics_row: bool
) -> Optional[Tuple[str, str, str, str, str, str]]:
    """Build the programme-trade E/SL/TP identity in Python, not SQL.

    Both sources are normalised with the same 12-decimal canonicalisation used
    by analytics deduplication. This avoids bridge incompatibility with SQL CAST,
    ROUND, NULL-safe equality, and correlated subqueries.
    """
    symbol = str(row.get("symbol") or "").strip().upper()
    timeframe = str(row.get("timeframe") or "").strip().lower()
    order_field = "order_type" if analytics_row else "ordertype"
    order_type = str(row.get(order_field) or "").strip().lower()
    entry_field = "entry_price" if analytics_row else "entry"
    stoploss_field = "stoploss_price" if analytics_row else "stoploss"
    takeprofit_field = "take_profit_price" if analytics_row else "takeprofit"
    entry = _canonical_trade_price(row.get(entry_field))
    stoploss = _canonical_trade_price(row.get(stoploss_field))
    takeprofit = _canonical_trade_price(row.get(takeprofit_field))
    if not symbol or not timeframe or not order_type:
        return None
    if entry is None or stoploss is None or takeprofit is None:
        return None
    return (symbol, timeframe, order_type, entry, stoploss, takeprofit)


def delete_unmapped_orphan_programme_trades(
    db: BridgeClient, programme_id: int, user_id: int
) -> int:
    """Delete only unlinked, canonical programme trades unsupported by Analytics.

    IMPORTANT: comparisons are performed in Python. The old implementation used
    a correlated NOT EXISTS with CAST/ROUND/NULL-safe comparisons, which this
    deployment's mysqlremote bridge rejects. SQL is now limited to simple scoped
    SELECTs and single-table DELETEs.

    Safety rules:
      * programme_trades.userid/programmeid must match this programme;
      * investor_id must be NULL;
      * optional sub-account/account ownership columns must be empty or zero;
      * broker-ticket rows (tradeid IS NOT NULL) are preserved;
      * rows with a chart-identity mapping are handled by mapped cleanup and skipped;
      * an Analytics trade with the same symbol/timeframe/order/E/SL/TP keeps the row.
    """
    if programme_id <= 0 or user_id <= 0:
        return 0
    if not table_exists(db, "programme_trades") or not table_exists(db, "programme_analytics"):
        return 0

    trade_columns = {column.lower() for column in get_columns(db, "programme_trades")}
    analytics_columns = {column.lower() for column in get_columns(db, "programme_analytics")}
    required_trade = {
        "id", "userid", "programmeid", "investor_id", "symbol", "timeframe",
        "ordertype", "entry", "stoploss", "takeprofit",
    }
    required_analytics = {
        "programme_id", "userid", "symbol", "timeframe", "order_type",
        "entry_price", "stoploss_price", "take_profit_price", "trade_configured",
    }
    missing_trade = sorted(required_trade - trade_columns)
    missing_analytics = sorted(required_analytics - analytics_columns)
    if missing_trade or missing_analytics:
        log.warning(
            "    Unmapped legacy trade cleanup skipped (fail-safe); missing programme_trades columns=%s; "
            "missing programme_analytics columns=%s",
            ", ".join(missing_trade) or "none",
            ", ".join(missing_analytics) or "none",
        )
        return 0

    # Read Analytics trade identities with a deliberately plain SELECT.
    # Do not add joins, correlated subqueries, CAST(), ROUND(), or <=> here.
    analytics_rows = db.fetch_all(
        "SELECT `symbol`,`timeframe`,`order_type`,`entry_price`,`stoploss_price`,"
        "`take_profit_price`,`trade_configured` "
        "FROM `programme_analytics` WHERE `programme_id`=%s AND `userid`=%s",
        [programme_id, user_id], timeout=180,
    )
    analytics_identities = set()
    for analytics_row in analytics_rows:
        if (as_int(analytics_row.get("trade_configured"), 0) or 0) != 1:
            continue
        identity = _programme_trade_price_identity(analytics_row, analytics_row=True)
        if identity is not None:
            analytics_identities.add(identity)

    # A separate simple SELECT supplies mapped IDs. Never infer that a trade is
    # legacy merely because one of its optional metadata columns is blank.
    mapped_ids = set()
    if table_exists(db, "programme_trade_chart_identity"):
        identity_columns = {
            column.lower() for column in get_columns(db, "programme_trade_chart_identity")
        }
        if {"programme_trade_id", "userid", "programmeid"}.issubset(identity_columns):
            mapped_rows = db.fetch_all(
                "SELECT `programme_trade_id` FROM `programme_trade_chart_identity` "
                "WHERE `userid`=%s AND `programmeid`=%s",
                [user_id, programme_id], timeout=90,
            )
            mapped_ids = {
                value for row in mapped_rows
                if (value := as_int(row.get("programme_trade_id"), 0)) and value > 0
            }

    # Select programme-owned rows in small ID-ordered pages. Only basic indexed
    # scope predicates are sent to mysqlremote; optional ownership is validated
    # below in Python and repeated in the DELETE as a concurrent-change guard.
    selected_columns = [
        "id", "investor_id", "symbol", "timeframe", "ordertype",
        "entry", "stoploss", "takeprofit",
    ]
    optional_owner_columns = [
        column for column in (
            "investor_sub_account_id", "sub_account_id",
            "accountmanagement_id", "investor_account_id",
        ) if column in trade_columns
    ]
    selected_columns.extend(optional_owner_columns)
    has_tradeid = "tradeid" in trade_columns
    if has_tradeid:
        selected_columns.append("tradeid")
    select_column_sql = ",".join(f"`{column}`" for column in selected_columns)

    page_size = 200
    last_id = 0
    removed = 0
    protected_owner_rows = 0
    protected_ticket_rows = 0
    protected_mapped_rows = 0

    while True:
        trade_rows = db.fetch_all(
            f"SELECT {select_column_sql} FROM `programme_trades` "
            "WHERE `userid`=%s AND `programmeid`=%s AND `investor_id` IS NULL "
            "AND `id`>%s ORDER BY `id` ASC LIMIT " + str(page_size),
            [user_id, programme_id, last_id], timeout=120,
        )
        if not trade_rows:
            break

        page_ids = []
        candidate_ids = []
        for trade_row in trade_rows:
            trade_id = as_int(trade_row.get("id"), 0) or 0
            if trade_id <= 0:
                continue
            page_ids.append(trade_id)
            last_id = max(last_id, trade_id)

            # The scoped SELECT already requires investor_id IS NULL. Check it
            # again against the returned row to fail closed on bridge anomalies.
            if normalise_db_value(trade_row.get("investor_id")) is not None:
                protected_owner_rows += 1
                continue
            if any(
                not _is_unassigned_programme_trade_owner(trade_row.get(column))
                for column in optional_owner_columns
            ):
                protected_owner_rows += 1
                continue
            if has_tradeid and normalise_db_value(trade_row.get("tradeid")) is not None:
                protected_ticket_rows += 1
                continue
            if trade_id in mapped_ids:
                protected_mapped_rows += 1
                continue

            identity = _programme_trade_price_identity(trade_row, analytics_row=False)
            if identity is None:
                # Do not delete incomplete/ambiguous legacy rows automatically.
                continue
            if identity not in analytics_identities:
                candidate_ids.append(trade_id)

        if candidate_ids:
            id_placeholders = ",".join(["%s"] * len(candidate_ids))
            delete_filters = [
                "`userid`=%s", "`programmeid`=%s", "`investor_id` IS NULL",
                f"`id` IN ({id_placeholders})",
            ]
            for column in optional_owner_columns:
                delete_filters.append(f"(`{column}` IS NULL OR `{column}`=0 OR `{column}`='')")
            if has_tradeid:
                delete_filters.append("`tradeid` IS NULL")
            delete_sql = "DELETE FROM `programme_trades` WHERE " + " AND ".join(delete_filters)
            delete_params: List[Any] = [user_id, programme_id] + candidate_ids
            removed += max(0, int(db.execute(delete_sql, delete_params, timeout=120) or 0))

        if len(trade_rows) < page_size:
            break

    if removed:
        log.warning(
            "    UNMAPPED LEGACY PROGRAMME TRADE CLEANUP: removed %s unlinked programme-owned row(s) "
            "without a matching configured Analytics trade.", removed,
        )
    else:
        log.info(
            "    Unmapped legacy programme trade cleanup: no eligible orphan rows found "
            "(protected sub-account rows=%s, broker-ticket rows=%s, mapped rows=%s).",
            protected_owner_rows, protected_ticket_rows, protected_mapped_rows,
        )
    return removed

def purge_programme_chart_rows_when_chart_is_empty(
    db: BridgeClient, programme_id: int, user_id: int, reason: str
) -> None:
    """Remove programme analytics/trade mappings when no chart scope can be processed."""
    try:
        result = prune_analytics_rows_not_in_current_chart(
            db, programme_id, user_id, [], {}
        )
        orphaned = delete_orphaned_mapped_programme_trades(db, programme_id, user_id)
        legacy_orphaned = delete_unmapped_orphan_programme_trades(db, programme_id, user_id)
        deduplicated = deduplicate_existing_programme_trades(db, programme_id, user_id)
        log.warning(
            "    EMPTY CHART CLEANUP (%s): removed %s analytics row(s), %s mapped/unmapped programme trade row(s), "
            "and deduplicated %s remaining programme-owned trade row(s).",
            reason, result["analytics_removed"],
            result["mapped_programme_trades_removed"] + orphaned + legacy_orphaned, deduplicated,
        )
    except Exception as exc:
        log.error("    Empty-chart cleanup could not complete (%s): %s", reason, str(exc)[:900])

def deduplicate_existing_programme_trades(
    db: BridgeClient, programme_id: int, user_id: int
) -> int:
    """Deduplicate programme-owned rows only; investor/sub-account copies are untouched.

    A programme-owned canonical row is exactly the row with investor_id IS NULL,
    matching the Programme Analytics PHP synchronizer. When optional sub-account
    columns exist, they must also be NULL/zero. Duplicate identity is programme,
    owner, symbol, timeframe, order type and entry/SL/TP prices. A mapped row is
    preferred over an unlinked row, then a closed/running/pending row, then newest ID.
    """
    if not table_exists(db, "programme_trades"):
        return 0
    columns = {column.lower() for column in get_columns(db, "programme_trades")}
    required = {
        "id", "userid", "programmeid", "investor_id", "symbol", "timeframe",
        "ordertype", "entry", "stoploss", "takeprofit", "orderstatus",
    }
    missing = sorted(required - columns)
    if missing:
        log.warning(
            "    programme_trades deduplication skipped to protect investor/sub-account data; missing columns: %s",
            ", ".join(missing),
        )
        return 0

    has_identity_map = table_exists(db, "programme_trade_chart_identity")
    identity_columns = {column.lower() for column in get_columns(db, "programme_trade_chart_identity")} if has_identity_map else set()
    has_valid_identity_map = has_identity_map and {"programme_trade_id", "userid", "programmeid"}.issubset(identity_columns)
    optional_owner_filters = []
    for column in ("investor_sub_account_id", "sub_account_id", "accountmanagement_id", "investor_account_id"):
        if column in columns:
            optional_owner_filters.append(f"COALESCE(d.`{column}`,0)=0")
            optional_owner_filters.append(f"COALESCE(k.`{column}`,0)=0")
    # A non-NULL broker ticket represents an actual broker-side execution. Keep
    # those rows intact even if their chart family has since fallen out of range.
    if "tradeid" in columns:
        optional_owner_filters.extend(["d.`tradeid` IS NULL", "k.`tradeid` IS NULL"])

    equality = (
        "UPPER(TRIM(COALESCE(d.`symbol`,'')))=UPPER(TRIM(COALESCE(k.`symbol`,''))) "
        "AND LOWER(TRIM(COALESCE(d.`timeframe`,'')))=LOWER(TRIM(COALESCE(k.`timeframe`,''))) "
        "AND LOWER(TRIM(COALESCE(d.`ordertype`,'')))=LOWER(TRIM(COALESCE(k.`ordertype`,''))) "
        "AND ROUND(CAST(d.`entry` AS DECIMAL(30,12)),12) <=> ROUND(CAST(k.`entry` AS DECIMAL(30,12)),12) "
        "AND ROUND(CAST(d.`stoploss` AS DECIMAL(30,12)),12) <=> ROUND(CAST(k.`stoploss` AS DECIMAL(30,12)),12) "
        "AND ROUND(CAST(d.`takeprofit` AS DECIMAL(30,12)),12) <=> ROUND(CAST(k.`takeprofit` AS DECIMAL(30,12)),12)"
    )
    status_d = (
        "(CASE WHEN LOWER(TRIM(COALESCE(d.`orderstatus`,'')))='closed' THEN 3 "
        "WHEN LOWER(TRIM(COALESCE(d.`orderstatus`,'')))='running' THEN 2 "
        "WHEN LOWER(TRIM(COALESCE(d.`orderstatus`,'')))='pending' THEN 1 ELSE 0 END)"
    )
    status_k = (
        "(CASE WHEN LOWER(TRIM(COALESCE(k.`orderstatus`,'')))='closed' THEN 3 "
        "WHEN LOWER(TRIM(COALESCE(k.`orderstatus`,'')))='running' THEN 2 "
        "WHEN LOWER(TRIM(COALESCE(k.`orderstatus`,'')))='pending' THEN 1 ELSE 0 END)"
    )

    join_map_d = join_map_k = ""
    prefer_map = ""
    delete_targets = "d"
    if has_valid_identity_map:
        join_map_d = (
            " LEFT JOIN `programme_trade_chart_identity` AS cd "
            "ON cd.`programme_trade_id`=d.`id` AND cd.`userid`=d.`userid` AND cd.`programmeid`=d.`programmeid` "
        )
        join_map_k = (
            " LEFT JOIN `programme_trade_chart_identity` AS ck "
            "ON ck.`programme_trade_id`=k.`id` AND ck.`userid`=k.`userid` AND ck.`programmeid`=k.`programmeid` "
        )
        mapped_d = "(CASE WHEN cd.`programme_trade_id` IS NULL THEN 0 ELSE 1 END)"
        mapped_k = "(CASE WHEN ck.`programme_trade_id` IS NULL THEN 0 ELSE 1 END)"
        prefer_map = (
            f"(({mapped_k} > {mapped_d}) OR "
            f"({mapped_k} = {mapped_d} AND "
            f"({status_k} > {status_d} OR ({status_k} = {status_d} AND k.`id` > d.`id`))))"
        )
        delete_targets = "d, cd"
    else:
        prefer_map = f"({status_k} > {status_d} OR ({status_k} = {status_d} AND k.`id` > d.`id`))"

    extra_filter = " AND ".join(optional_owner_filters)
    if extra_filter:
        extra_filter = " AND " + extra_filter
    sql = (
        f"DELETE {delete_targets} FROM `programme_trades` AS d "
        f"{join_map_d}"
        "INNER JOIN `programme_trades` AS k ON "
        "k.`userid`=d.`userid` AND k.`programmeid`=d.`programmeid` AND k.`id`<>d.`id` "
        f"AND {equality} "
        f"{join_map_k}"
        "WHERE d.`userid`=%s AND d.`programmeid`=%s "
        "AND k.`userid`=%s AND k.`programmeid`=%s "
        "AND d.`investor_id` IS NULL AND k.`investor_id` IS NULL "
        "AND d.`entry` IS NOT NULL AND d.`stoploss` IS NOT NULL AND d.`takeprofit` IS NOT NULL "
        "AND k.`entry` IS NOT NULL AND k.`stoploss` IS NOT NULL AND k.`takeprofit` IS NOT NULL "
        "AND UPPER(TRIM(COALESCE(d.`symbol`,'')))<>'' "
        "AND LOWER(TRIM(COALESCE(d.`timeframe`,'')))<>'' "
        "AND LOWER(TRIM(COALESCE(d.`ordertype`,'')))<>'' "
        f"AND {prefer_map}{extra_filter}"
    )
    removed = db.execute(sql, [user_id, programme_id, user_id, programme_id], timeout=180)
    removed = max(0, int(removed or 0))
    if removed:
        log.warning(
            "    PROGRAMME TRADES DEDUPLICATION: removed %s duplicate programme-owned row(s); "
            "investor/sub-account rows were not included.", removed,
        )
    else:
        log.info("    programme_trades deduplication: no duplicate programme-owned trade rows found.")
    return removed


def load_provider_risk_reward(db: BridgeClient, programme: Dict[str, Any]) -> Dict[str, Any]:
    """Mirror programme_training.php's exact latest Signals Provision lookup."""
    result = {"risk_reward": None, "risk_reward_type": "", "interest_status": ""}
    if not table_exists(db, "signals_provider_interest"):
        return result
    try:
        rows = db.fetch_all(
            "SELECT `risk_reward`,`risk_reward_type`,`interest_status` "
            "FROM `signals_provider_interest` WHERE `programme_id`=%s AND `user_id`=%s "
            "AND COALESCE(`accountmanagement_id`,0)=0 ORDER BY `id` DESC LIMIT 1",
            [programme.get("id"), programme.get("userid")], timeout=60)
        if not rows:
            return result
        row = rows[0]
        status = str(row.get("interest_status") or "").strip().lower()
        valid_statuses = {"interested", "pending", "active", "ongoing", "passed", "failed", "breached"}
        if status not in valid_statuses:
            return result
        rr = as_float(row.get("risk_reward"))
        rr = rr if rr is not None and rr > 0 else None
        rr_type = str(row.get("risk_reward_type") or "").strip().lower()
        valid_types = {"fixed_risk_reward", "minimum_risk_reward", "custom_and_default_risk_reward"}
        if rr_type not in valid_types:
            rr_type = ""
        return {"risk_reward": rr, "risk_reward_type": rr_type, "interest_status": status}
    except BridgeError as exc:
        log.warning("    Signals Provision risk/reward lookup failed; saved root RR will be used: %s", str(exc)[:1000])
        return result

def load_account_management_rules(db: BridgeClient, programme: Dict[str, Any]) -> Dict[str, Any]:
    """Load the exact per-programme Account Management rules used by Training.

    Scope is developerid + programme_id, matching programme_training.php. The
    programme timezone wins; legacy rule-table timezone values are a fallback.
    """
    user_id = as_int(programme.get("userid"), 0) or 0
    programme_id = as_int(programme.get("id"), 0) or 0
    result: Dict[str, Any] = {
        "timezone": str(programme.get("timezone") or "").strip(),
        "restricted_dates": [],
        "restricted_days": [],
        "trades_breakeven": [],
    }
    table_specs = {
        "restricted_dates": ("accountmanagement_restricted_dates", ["restricted_date"]),
        "restricted_days": ("accountmanagement_restricted_days", ["day", "from_time", "to_time"]),
        "trades_breakeven": ("accountmanagement_trades_breakeven", ["floating_profit_at_risk_reward", "breakeven_at_risk_reward"]),
    }
    if user_id <= 0 or programme_id <= 0:
        return result

    for key, (table, wanted_fields) in table_specs.items():
        if not table_exists(db, table):
            continue
        try:
            columns = {column.lower() for column in get_columns(db, table)}
            if not {"developerid", "programme_id"}.issubset(columns):
                log.warning("    Account Management rule table %s lacks developerid/programme_id; skipped", table)
                continue
            selected = [field for field in wanted_fields if field in columns]
            if not selected:
                continue
            has_timezone = "timezone" in columns
            if has_timezone:
                selected.append("timezone")
            order_by = " ORDER BY `id` ASC" if "id" in columns else ""
            sql = (
                "SELECT " + ",".join("`" + field + "`" for field in selected) +
                f" FROM `{table}` WHERE `developerid`=%s AND `programme_id`=%s" + order_by
            )
            rows = db.fetch_all(sql, [user_id, programme_id], timeout=60)
            cleaned_rows = []
            for row in rows:
                cleaned_rows.append({field: row.get(field) for field in wanted_fields if field in row})
                if not result["timezone"] and row.get("timezone"):
                    result["timezone"] = str(row.get("timezone")).strip()
            result[key] = cleaned_rows
        except BridgeError as exc:
            log.warning("    Account Management %s rule load failed: %s", key, str(exc)[:900])

    zone = _programme_zone(result.get("timezone"))
    result["timezone"] = getattr(zone, "key", "UTC")
    log.info(
        "Account Management rules loaded: timezone=%s, restricted dates=%s, restricted days=%s, breakeven thresholds=%s",
        result["timezone"], len(result["restricted_dates"]), len(result["restricted_days"]), len(result["trades_breakeven"]),
    )
    return result


def _analytics_identity(record: Dict[str, Any]) -> Dict[str, Any]:
    """Return the exact logical key defined by programme_analytics_schema.sql.

    `userid` is stored on each row and checked as an ownership/scope value, but
    it is deliberately NOT part of the table's UNIQUE family index. The supplied
    group-aware schema defines uq_programme_analytics_family_group as:
      programme_id, symbol, timeframe, source_timeframe, root_id,
      confirmation_root_id, setup_group_id, family_number.
    The upsert key must match that index exactly or the worker needlessly falls
    back to a SELECT-before-write path.
    """
    return {
        "programme_id": record.get("programme_id"),
        "symbol": record.get("symbol"),
        "timeframe": record.get("timeframe"),
        "source_timeframe": record.get("source_timeframe"),
        "root_id": record.get("root_id"),
        "confirmation_root_id": 0 if record.get("confirmation_root_id") in (None, "") else record.get("confirmation_root_id"),
        "setup_group_id": max(1, as_int(record.get("setup_group_id"), 1) or 1),
        "family_number": record.get("family_number"),
    }


# This list intentionally matches the group-aware UNIQUE index created/verified
# by ensure_analytics_group_schema(). Do not add userid unless the SQL index is
# changed at the same time; ownership is still enforced by the write fallback.
ANALYTICS_IDENTITY_COLUMNS = [
    "programme_id", "symbol", "timeframe", "source_timeframe", "root_id",
    "confirmation_root_id", "setup_group_id", "family_number",
]

# Configured trades have a second identity independent of this chart-family key.
# It is based on actual entry/stoploss/take-profit prices and deliberately ignores
# root/confirmation/group/family numbering. Match-only rows keep this field NULL.
TRADE_DEDUP_INDEX_COLUMNS = [
    "programme_id", "userid", "symbol", "timeframe", "trade_identity_hash",
]

TRADE_PRICE_DECIMAL_PLACES = 12
TRADE_PRICE_QUANTUM = Decimal("0.000000000001")


def _canonical_trade_price(value: Any) -> Optional[str]:
    """Return the same fixed-precision price text used by the SQL backfill."""
    if value is None or (isinstance(value, str) and value.strip() == ""):
        return None
    try:
        number = Decimal(str(value).strip())
        if not number.is_finite():
            return None
        rounded = number.quantize(TRADE_PRICE_QUANTUM, rounding=ROUND_HALF_UP)
        # MySQL casts DECIMAL(30,12) to a string with exactly twelve decimals.
        return format(rounded, f".{TRADE_PRICE_DECIMAL_PLACES}f")
    except (InvalidOperation, ValueError, TypeError):
        return None


def _trade_identity_payload(record: Dict[str, Any]) -> Optional[str]:
    """Build trade identity from programme/symbol/timeframe/order and E/SL/TP.

    Setup group, family number, root ID, confirmation ID, source timeframe and
    family candle-map signature are deliberately excluded. Two configured trades
    with the same entry, stop-loss and take-profit in the same programme market
    stream are the same analytics trade, even when different setups found them.
    """
    if (as_int(record.get("trade_configured"), 0) or 0) != 1:
        return None
    entry = _canonical_trade_price(record.get("entry_price"))
    stoploss = _canonical_trade_price(record.get("stoploss_price"))
    takeprofit = _canonical_trade_price(record.get("take_profit_price"))
    if entry is None or stoploss is None or takeprofit is None:
        return None

    programme_id = as_int(record.get("programme_id"))
    user_id = as_int(record.get("userid"))
    symbol = str(record.get("symbol") or "").strip().upper()
    timeframe = str(record.get("timeframe") or "").strip().lower()
    order_type = str(record.get("order_type") or "buy").strip().lower()
    if programme_id is None or user_id is None or not symbol or not timeframe:
        return None

    return "|".join((
        str(programme_id), str(user_id), symbol, timeframe, order_type,
        entry, stoploss, takeprofit,
    ))


def _trade_identity_hash(record: Any) -> Optional[str]:
    """Return SHA-256 of the actual trade's entry/stop-loss/take-profit identity."""
    if not isinstance(record, dict):
        return None
    payload = _trade_identity_payload(record)
    if not payload:
        return None
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _unique_index_column_sets(db: BridgeClient, table: str) -> List[set]:
    """Return unique-index column sets; failures are non-fatal for read/insert mode."""
    try:
        rows = db.fetch_all(f"SHOW INDEX FROM `{table}`", timeout=30)
    except Exception as exc:
        log.warning("    Could not inspect unique indexes on %s: %s", table, str(exc)[:800])
        return []
    grouped: Dict[str, List[Tuple[int, str]]] = {}
    for row in rows:
        if as_int(row.get("non_unique"), 1) != 0:
            continue
        key_name = str(row.get("key_name") or "")
        column_name = str(row.get("column_name") or "")
        if not key_name or not column_name:
            continue
        seq = as_int(row.get("seq_in_index"), 1) or 1
        grouped.setdefault(key_name, []).append((seq, column_name.lower()))
    return [
        {column for _seq, column in sorted(items)}
        for items in grouped.values()
        if items
    ]


def _has_exact_unique_identity(db: BridgeClient, table: str,
                               identity_columns: Sequence[str]) -> bool:
    target = {column.lower() for column in identity_columns}
    return any(columns == target for columns in _unique_index_column_sets(db, table))


def _unique_index_definitions(db: BridgeClient, table: str) -> Dict[str, set]:
    """Return unique indexes by name with their lower-case column sets."""
    rows = db.fetch_all(f"SHOW INDEX FROM `{table}`", timeout=30)
    grouped: Dict[str, List[Tuple[int, str]]] = {}
    for row in rows:
        if as_int(row.get("non_unique"), 1) != 0:
            continue
        name = str(row.get("key_name") or "").strip()
        column = str(row.get("column_name") or "").strip().lower()
        if not name or not column:
            continue
        sequence = as_int(row.get("seq_in_index"), 1) or 1
        grouped.setdefault(name, []).append((sequence, column))
    return {
        name: {column for _sequence, column in sorted(items)}
        for name, items in grouped.items() if items
    }


def _analytics_primary_key_column(db: BridgeClient, table: str) -> str:
    """Return the single-column primary key needed for safe duplicate deletion."""
    rows = db.fetch_all(f"SHOW INDEX FROM `{table}`", timeout=30)
    primary = sorted(
        ((as_int(row.get("seq_in_index"), 1) or 1, str(row.get("column_name") or "").strip())
         for row in rows
         if str(row.get("key_name") or "").strip().lower() == "primary"
         and row.get("column_name")),
        key=lambda item: item[0],
    )
    if len(primary) != 1:
        raise RuntimeError(
            f"Cannot safely remove duplicate rows from {table}: expected a single-column PRIMARY KEY, "
            f"found {len(primary)} primary-key column(s)."
        )
    column = primary[0][1]
    if not re.fullmatch(r"[A-Za-z0-9_]+", column):
        raise RuntimeError(f"Unsafe primary-key column returned for {table}: {column!r}")
    return column


def _trade_identity_sql_expression() -> str:
    """SQL equivalent of _trade_identity_payload() for historical-row backfill."""
    return (
        "SHA2(CONCAT_WS('|',"
        "CAST(`programme_id` AS CHAR),CAST(`userid` AS CHAR),"
        "UPPER(TRIM(`symbol`)),LOWER(TRIM(`timeframe`)),"
        "LOWER(TRIM(COALESCE(`order_type`, 'buy'))),"
        "CAST(CAST(ROUND(`entry_price`,12) AS DECIMAL(30,12)) AS CHAR),"
        "CAST(CAST(ROUND(`stoploss_price`,12) AS DECIMAL(30,12)) AS CHAR),"
        "CAST(CAST(ROUND(`take_profit_price`,12) AS DECIMAL(30,12)) AS CHAR)"
        "),256)"
    )


def backfill_trade_identity_hashes(db: BridgeClient, programme_id: int,
                                  user_id: int) -> int:
    """Rebuild every configured trade hash from its actual E/SL/TP price triplet.

    Match-only families and incomplete trade rows get NULL. The database hash uses
    the same fixed twelve-decimal canonical representation as Python.
    """
    expression = _trade_identity_sql_expression()
    sql = (
        "UPDATE `programme_analytics` SET `trade_identity_hash` = CASE "
        "WHEN COALESCE(`trade_configured`,0)=1 "
        "AND `entry_price` IS NOT NULL AND `stoploss_price` IS NOT NULL "
        "AND `take_profit_price` IS NOT NULL "
        f"THEN {expression} ELSE NULL END "
        "WHERE `programme_id`=%s AND `userid`=%s"
    )
    return db.execute(sql, [programme_id, user_id], timeout=180)


def _trade_status_rank_sql(alias: str) -> str:
    """SQL rank for canonical-row selection: closed > in-trade > pending > unknown."""
    if alias not in {"d", "k"}:
        raise ValueError("Unexpected SQL alias for trade status ranking")
    return (
        f"(CASE WHEN COALESCE({alias}.`closed_trade`,0)=1 THEN 3 "
        f"WHEN COALESCE({alias}.`in_trade`,0)=1 THEN 2 "
        f"WHEN COALESCE({alias}.`pending_entry`,0)=1 THEN 1 ELSE 0 END)"
    )


def deduplicate_existing_analytics_rows(db: BridgeClient, programme_id: int,
                                        user_id: int) -> Dict[str, int]:
    """Remove duplicate families and trades already saved in programme_analytics.

    Family rows are still collapsed by their family identity/signature. Configured
    trades are additionally collapsed by programme+owner+symbol+timeframe and the
    actual entry/stoploss/take-profit price triplet. Setup group, family number,
    root ID, confirmation ID and family_signature do not participate in trade
    identity. One canonical trade row remains, preferring configured/closed/newer.
    """
    if not table_exists(db, "programme_analytics"):
        return {"family_duplicates_removed": 0, "trade_duplicates_removed": 0}

    table_columns = {column.lower() for column in get_columns(db, "programme_analytics")}
    required = set(ANALYTICS_IDENTITY_COLUMNS) | {
        "userid", "trade_configured", "trade_identity_hash", "family_signature",
        "order_type", "entry_price", "stoploss_price", "take_profit_price",
        "closed_trade", "in_trade", "pending_entry",
    }
    missing = sorted(required - table_columns)
    if missing:
        raise RuntimeError(
            "Cannot deduplicate programme_analytics because required columns are missing: "
            + ", ".join(missing)
        )

    primary_key = _analytics_primary_key_column(db, "programme_analytics")
    if primary_key.lower() not in table_columns:
        raise RuntimeError(
            f"programme_analytics primary key {primary_key!r} was not returned by SHOW COLUMNS"
        )

    # Layer 1: exact stored analytics family key (group-aware chart identity).
    family_join = " AND ".join(
        f"d.`{column}` <=> k.`{column}`" for column in ANALYTICS_IDENTITY_COLUMNS
    ) + " AND d.`userid`=k.`userid`"
    status_k = _trade_status_rank_sql("k")
    status_d = _trade_status_rank_sql("d")
    family_preference = (
        "(COALESCE(k.`trade_configured`,0) > COALESCE(d.`trade_configured`,0) "
        "OR (COALESCE(k.`trade_configured`,0) = COALESCE(d.`trade_configured`,0) "
        f"AND {status_k} > {status_d}) "
        "OR (COALESCE(k.`trade_configured`,0) = COALESCE(d.`trade_configured`,0) "
        f"AND {status_k} = {status_d} AND k.`{primary_key}` > d.`{primary_key}`))"
    )
    family_delete_sql = (
        "DELETE d FROM `programme_analytics` AS d "
        "INNER JOIN `programme_analytics` AS k ON "
        f"{family_join} AND {family_preference} "
        "WHERE d.`programme_id`=%s AND d.`userid`=%s"
    )
    family_key_removed = db.execute(family_delete_sql, [programme_id, user_id], timeout=180)

    # Layer 2: same matched candle family under a different setup/group ordinal.
    # This remains a family deduplication rule, not the trade identity rule.
    family_scope_columns = [
        "programme_id", "userid", "symbol", "timeframe", "source_timeframe",
        "root_id", "confirmation_root_id", "family_signature",
    ]
    family_signature_join = " AND ".join(
        f"d.`{column}` <=> k.`{column}`" for column in family_scope_columns
    )
    family_signature_delete_sql = (
        "DELETE d FROM `programme_analytics` AS d "
        "INNER JOIN `programme_analytics` AS k ON "
        f"{family_signature_join} "
        "WHERE d.`programme_id`=%s AND d.`userid`=%s "
        "AND d.`family_signature` IS NOT NULL AND d.`family_signature`<>'' "
        "AND (COALESCE(k.`trade_configured`,0) > COALESCE(d.`trade_configured`,0) "
        "OR (COALESCE(k.`trade_configured`,0) = COALESCE(d.`trade_configured`,0) "
        f"AND {status_k} > {status_d}) "
        "OR (COALESCE(k.`trade_configured`,0) = COALESCE(d.`trade_configured`,0) "
        f"AND {status_k} = {status_d} AND k.`{primary_key}` > d.`{primary_key}`))"
    )
    family_signature_removed = db.execute(
        family_signature_delete_sql, [programme_id, user_id], timeout=180
    )

    # Layer 3: same actual trade prices, regardless of root, confirmation, group,
    # family number, family map or source timeframe. These are the user's requested
    # trade-level duplicate conditions.
    trade_scope_columns = [
        "programme_id", "userid", "symbol", "timeframe", "trade_identity_hash",
    ]
    trade_join = " AND ".join(
        f"d.`{column}` <=> k.`{column}`" for column in trade_scope_columns
    )
    trade_delete_sql = (
        "DELETE d FROM `programme_analytics` AS d "
        "INNER JOIN `programme_analytics` AS k ON "
        f"{trade_join} "
        "WHERE d.`programme_id`=%s AND d.`userid`=%s "
        "AND d.`trade_configured`=1 "
        "AND d.`trade_identity_hash` IS NOT NULL AND d.`trade_identity_hash`<>'' "
        f"AND ({status_k} > {status_d} "
        f"OR ({status_k} = {status_d} AND k.`{primary_key}` > d.`{primary_key}`))"
    )
    trade_removed = db.execute(trade_delete_sql, [programme_id, user_id], timeout=180)

    result = {
        "family_duplicates_removed": max(0, int(family_key_removed or 0))
        + max(0, int(family_signature_removed or 0)),
        "trade_duplicates_removed": max(0, int(trade_removed or 0)),
    }
    total_removed = result["family_duplicates_removed"] + result["trade_duplicates_removed"]
    if total_removed:
        log.warning(
            "    PROGRAMME ANALYTICS DEDUPLICATION: removed %s duplicate row(s) "
            "(family duplicates=%s, same entry+stoploss+take-profit trades=%s); "
            "one canonical row was retained per identity.",
            total_removed, result["family_duplicates_removed"], result["trade_duplicates_removed"],
        )
    else:
        log.info("    Programme Analytics deduplication: no existing family/trade duplicates found.")
    return result


def ensure_analytics_group_schema(db: BridgeClient, programme_id: int, user_id: int,
                                 setup_group_limit: int,
                                 cleanup_duplicates: bool = True) -> None:
    """Install the group key and the price-triplet trade identity key."""
    if not table_exists(db, "programme_analytics"):
        raise RuntimeError(
            "Required table programme_analytics does not exist; create the analytics schema first."
        )

    limit = max(1, int(setup_group_limit or DEFAULT_TRADES_SETUP_PER_GROUP))
    columns = {column.lower() for column in get_columns(db, "programme_analytics")}
    required_for_trade_key = {
        "programme_id", "userid", "symbol", "timeframe", "order_type",
        "trade_configured", "entry_price", "stoploss_price", "take_profit_price",
    }
    missing_trade_fields = sorted(required_for_trade_key - columns)
    if missing_trade_fields:
        raise RuntimeError(
            "programme_analytics is missing columns required for trade deduplication: "
            + ", ".join(missing_trade_fields)
        )
    if "family_number" not in columns:
        raise RuntimeError("programme_analytics is missing required column family_number")
    if "setup_group_id" not in columns:
        db.execute(
            "ALTER TABLE `programme_analytics` ADD COLUMN `setup_group_id` "
            "BIGINT(20) UNSIGNED NOT NULL DEFAULT 1 AFTER `family_number`",
            timeout=90,
        )
        columns.add("setup_group_id")
        log.info("    Added programme_analytics.setup_group_id")

    if "trade_identity_hash" not in columns:
        db.execute(
            "ALTER TABLE `programme_analytics` ADD COLUMN `trade_identity_hash` "
            "CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL DEFAULT NULL AFTER `family_signature`",
            timeout=90,
        )
        columns.add("trade_identity_hash")
        log.info("    Added programme_analytics.trade_identity_hash for entry/SL/TP deduplication")

    # Remove obsolete unique indexes created by the earlier family-signature
    # fingerprint implementation. Its root/group scope did not match the required
    # actual-trade identity. The replacement key deliberately omits those fields.
    wanted_trade = {column.lower() for column in TRADE_DEDUP_INDEX_COLUMNS}
    index_definitions = _unique_index_definitions(db, "programme_analytics")
    obsolete_trade_indexes = [
        name for name, index_columns in index_definitions.items()
        if name.lower() != "primary"
        and "trade_identity_hash" in index_columns
        and index_columns != wanted_trade
    ]
    for index_name in obsolete_trade_indexes:
        if not re.fullmatch(r"[A-Za-z0-9_$]+", index_name):
            raise RuntimeError(f"Unsafe trade deduplication index name returned by database: {index_name!r}")
        db.execute(f"ALTER TABLE `programme_analytics` DROP INDEX `{index_name}`", timeout=90)
        log.info("    Replaced obsolete trade-fingerprint index %s", index_name)

    # The old key allowed only one family_number per stream. That conflicts with
    # setup groups that each restart at family_number 1.
    old_identity = {column for column in ANALYTICS_IDENTITY_COLUMNS if column != "setup_group_id"}
    indexes = _unique_index_definitions(db, "programme_analytics")
    obsolete_family = [
        name for name, index_columns in indexes.items()
        if name.lower() != "primary"
        and "setup_group_id" not in index_columns
        and old_identity.issubset(index_columns)
    ]
    for index_name in obsolete_family:
        if not re.fullmatch(r"[A-Za-z0-9_$]+", index_name):
            raise RuntimeError(f"Unsafe unique index name returned by database: {index_name!r}")
        db.execute(f"ALTER TABLE `programme_analytics` DROP INDEX `{index_name}`", timeout=90)
        log.info("    Replaced obsolete programme_analytics family index %s", index_name)

    # Normalize/backfill setup groups and local family numbers. This is solely
    # the analytics display identity; it is not used for trade deduplication.
    db.execute(
        "UPDATE `programme_analytics` SET `setup_group_id`=1 "
        "WHERE `programme_id`=%s AND `userid`=%s "
        "AND (`setup_group_id` IS NULL OR `setup_group_id`<1)",
        [programme_id, user_id], timeout=90,
    )
    where_scope = "`programme_id`=%s AND `userid`=%s AND `family_number`>%s"
    db.execute(
        "UPDATE `programme_analytics` SET `setup_group_id` = GREATEST("
        "COALESCE(`setup_group_id`,1), FLOOR((`family_number`-1)/%s)+1) "
        f"WHERE {where_scope}",
        [limit, programme_id, user_id, limit], timeout=120,
    )
    db.execute(
        "UPDATE `programme_analytics` SET `family_number` = MOD(`family_number`-1,%s)+1 "
        f"WHERE {where_scope}",
        [limit, programme_id, user_id, limit], timeout=120,
    )

    # Rebuild the stored fingerprint from actual price columns before duplicate
    # cleanup. This replaces any older hashes derived from family_signature.
    affected = backfill_trade_identity_hashes(db, programme_id, user_id)
    log.info(
        "    Rebuilt %s analytics trade fingerprint row(s) from entry/stoploss/take-profit prices",
        max(0, int(affected or 0)),
    )

    if cleanup_duplicates:
        deduplicate_existing_analytics_rows(db, programme_id, user_id)

    # The family/group unique key continues to prevent duplicate chart-family rows.
    index_definitions = _unique_index_definitions(db, "programme_analytics")
    wanted_family = {column.lower() for column in ANALYTICS_IDENTITY_COLUMNS}
    if not any(index_columns == wanted_family for index_columns in index_definitions.values()):
        base_name = "uq_programme_analytics_family_group"
        candidate = base_name
        suffix = 2
        while candidate in index_definitions:
            candidate = f"{base_name}_{suffix}"
            suffix += 1
        try:
            db.execute(
                f"CREATE UNIQUE INDEX `{candidate}` ON `programme_analytics` "
                "(`programme_id`,`symbol`,`timeframe`,`source_timeframe`,`root_id`,"
                "`confirmation_root_id`,`setup_group_id`,`family_number`)",
                timeout=120,
            )
            log.info("    Created group-aware programme_analytics unique index %s", candidate)
        except BridgeError as exc:
            log.warning(
                "    Group-aware unique index could not be created; persistence will use "
                "the ownership-scoped compatibility path. Check for duplicate analytics identities: %s",
                str(exc)[:900],
            )

    # Enforce the requested trade identity independently of the family/group key.
    # A duplicate is the same programme/owner/symbol/timeframe/order and identical
    # entry_price + stoploss_price + take_profit_price, even across root/group IDs.
    trade_index_definitions = _unique_index_definitions(db, "programme_analytics")
    if not any(index_columns == wanted_trade for index_columns in trade_index_definitions.values()):
        trade_index_base = "uq_programme_analytics_trade_prices"
        trade_index_name = trade_index_base
        trade_suffix = 2
        while trade_index_name in trade_index_definitions:
            trade_index_name = f"{trade_index_base}_{trade_suffix}"
            trade_suffix += 1
        try:
            db.execute(
                f"CREATE UNIQUE INDEX `{trade_index_name}` ON `programme_analytics` "
                "(`programme_id`,`userid`,`symbol`,`timeframe`,`trade_identity_hash`)",
                timeout=120,
            )
            log.info("    Created entry/SL/TP trade unique index %s", trade_index_name)
        except BridgeError as exc:
            log.warning(
                "    Entry/SL/TP trade unique index could not be created; the per-row exact trade "
                "fingerprint check will be used. Check for remaining duplicate price triplets: %s",
                str(exc)[:900],
            )


def _identity_where(identity: Dict[str, Any], columns: Sequence[str]) -> Tuple[str, List[Any]]:
    """Build a simple equality predicate for the canonical analytics key.

    programme_analytics_schema.sql defines confirmation_root_id as NOT NULL
    DEFAULT 0, so COALESCE() is unnecessary here. Plain equality also keeps the
    fallback query compatible with simpler mysqlremote SQL executors.
    """
    parts: List[str] = []
    values: List[Any] = []
    for column in columns:
        value = identity.get(column)
        if column == "confirmation_root_id" and value in (None, ""):
            value = 0
        parts.append(f"`{column}`=%s")
        values.append(value)
    return " AND ".join(parts), values


def _trade_status_rank(record: Dict[str, Any]) -> int:
    if (as_int(record.get("closed_trade"), 0) or 0) == 1:
        return 3
    if (as_int(record.get("in_trade"), 0) or 0) == 1:
        return 2
    if (as_int(record.get("pending_entry"), 0) or 0) == 1:
        return 1
    return 0


def _refresh_analytics_row_in_place(
    db: BridgeClient,
    table: str,
    record: Dict[str, Any],
    columns: Sequence[str],
    immutable_columns: Sequence[str],
    where_sql: str,
    where_values: Sequence[Any],
    existing_trade_state: Dict[str, Any],
    label: str,
) -> str:
    """Refresh one existing family/trade row without downgrading its status."""
    existing_trade = as_int(existing_trade_state.get("trade_configured"), 0) or 0
    incoming_trade = as_int(record.get("trade_configured"), 0) or 0
    if existing_trade > incoming_trade:
        return "preserved"
    if existing_trade == incoming_trade == 1 and _trade_status_rank(record) < _trade_status_rank(existing_trade_state):
        return "preserved"

    protected = {str(column).lower() for column in immutable_columns} | {"userid"}
    update_columns = [column for column in columns if str(column).lower() not in protected]
    set_parts: List[str] = []
    update_values: List[Any] = []
    for column in update_columns:
        if str(column).lower() == "trade_time":
            set_parts.append("`trade_time`=COALESCE(`trade_time`,%s)")
        else:
            set_parts.append(f"`{column}`=%s")
        update_values.append(record.get(column))
    if not set_parts:
        return "preserved"

    update_values.extend(where_values)
    try:
        db.execute(
            f"UPDATE `{table}` SET {','.join(set_parts)} WHERE {where_sql}",
            update_values, timeout=120,
        )
        return "updated"
    except BridgeError as exc:
        log.warning(
            "    %s: existing analytics row could not be refreshed; preserving it: %s",
            label, str(exc)[:1000],
        )
        return "preserved"


def _insert_or_refresh_row(
    db: BridgeClient,
    table: str,
    record: Dict[str, Any],
    columns: Sequence[str],
    identity_columns: Sequence[str],
    has_unique_identity: bool,
    label: str,
) -> str:
    """Persist a family/trade once; refresh an existing row instead of inserting a copy.

    Exact family identities and family signatures can be refreshed in place. Configured
    trade identity is checked by same entry/SL/TP prices, independently of root/group
    numbering. A prior match-only row is promoted through its family signature, while a
    match-only rediscovery can never downgrade an existing configured trade.
    """
    values = [record.get(column) for column in columns]
    identity = {column: record.get(column) for column in identity_columns}
    where_sql, where_values = _identity_where(identity, identity_columns)
    insert_columns_sql = ",".join(f"`{column}`" for column in columns)
    insert_marks = "(" + ",".join(["%s"] * len(columns)) + ")"

    # First detect the canonical family row using the database's family identity.
    existing = db.fetch_all(
        f"SELECT `userid`,`trade_configured`,`closed_trade`,`in_trade`,`pending_entry` "
        f"FROM `{table}` WHERE {where_sql} LIMIT 1",
        where_values, timeout=60,
    )
    if existing:
        existing_owner = as_int(existing[0].get("userid"))
        current_owner = as_int(record.get("userid"))
        if existing_owner is not None and current_owner is not None and existing_owner != current_owner:
            return "preserved"
        return _refresh_analytics_row_in_place(
            db, table, record, columns, identity_columns, where_sql, where_values,
            existing[0], label,
        )

    # Preserve the family-level promotion path separately from trade identity:
    # if this exact candle family was previously saved as match-only under another
    # displayed setup/group number, upgrade that family row in place.
    family_signature = record.get("family_signature")
    if family_signature:
        family_scope = (
            "`programme_id`=%s AND `userid`=%s AND UPPER(`symbol`)=UPPER(%s) "
            "AND LOWER(`timeframe`)=LOWER(%s) AND LOWER(`source_timeframe`)=LOWER(%s) "
            "AND `root_id`=%s AND `confirmation_root_id`=%s AND `family_signature`=%s"
        )
        family_values = [
            record.get("programme_id"), record.get("userid"), record.get("symbol"),
            record.get("timeframe"), record.get("source_timeframe"), record.get("root_id"),
            record.get("confirmation_root_id") or 0, family_signature,
        ]
        existing_family = db.fetch_all(
            f"SELECT `userid`,`trade_configured`,`closed_trade`,`in_trade`,`pending_entry` "
            f"FROM `{table}` WHERE {family_scope} LIMIT 1",
            family_values, timeout=60,
        )
        if existing_family:
            existing_owner = as_int(existing_family[0].get("userid"))
            current_owner = as_int(record.get("userid"))
            if existing_owner is not None and current_owner is not None and existing_owner != current_owner:
                return "preserved"
            return _refresh_analytics_row_in_place(
                db, table, record, columns, identity_columns, family_scope,
                family_values, existing_family[0], label,
            )

    # If another family/configuration already created this exact E/SL/TP trade,
    # reuse its row regardless of root, confirmation, setup group or family number.
    trade_hash = record.get("trade_identity_hash")
    if trade_hash:
        trade_scope = (
            "`programme_id`=%s AND `userid`=%s AND UPPER(`symbol`)=UPPER(%s) "
            "AND LOWER(`timeframe`)=LOWER(%s) AND `trade_identity_hash`=%s"
        )
        trade_scope_values = [
            record.get("programme_id"), record.get("userid"), record.get("symbol"),
            record.get("timeframe"), trade_hash,
        ]
        repeated_trade = db.fetch_all(
            f"SELECT `userid`,`trade_configured`,`closed_trade`,`in_trade`,`pending_entry` "
            f"FROM `{table}` WHERE {trade_scope} LIMIT 1",
            trade_scope_values, timeout=60,
        )
        if repeated_trade:
            existing_owner = as_int(repeated_trade[0].get("userid"))
            current_owner = as_int(record.get("userid"))
            if existing_owner is not None and current_owner is not None and existing_owner != current_owner:
                return "preserved"
            return _refresh_analytics_row_in_place(
                db, table, record, columns, identity_columns, trade_scope,
                trade_scope_values, repeated_trade[0], label,
            )

    try:
        db.query(
            f"INSERT INTO `{table}` ({insert_columns_sql}) VALUES {insert_marks}",
            values, timeout=120,
        )
        return "inserted"
    except BridgeError:
        # A second process may have inserted the same family after our read.
        identity_after = db.fetch_all(
            f"SELECT 1 AS `found` FROM `{table}` WHERE {where_sql} LIMIT 1",
            where_values, timeout=60,
        )
        if identity_after:
            return "preserved"
        if trade_hash:
            trade_after = db.fetch_all(
                f"SELECT 1 AS `found` FROM `{table}` WHERE `programme_id`=%s AND `userid`=%s "
                "AND UPPER(`symbol`)=UPPER(%s) AND LOWER(`timeframe`)=LOWER(%s) "
                "AND `trade_identity_hash`=%s LIMIT 1",
                [record.get("programme_id"), record.get("userid"), record.get("symbol"),
                 record.get("timeframe"), trade_hash], timeout=60,
            )
            if trade_after:
                return "preserved"
        raise


def _batch_upsert_analytics(db: BridgeClient, records: Sequence[Dict[str, Any]],
                            columns: Sequence[str]) -> None:
    """Atomically insert new families or refresh a family/trade identity in place.

    A repeated entry/SL/TP fingerprint resolves to its existing row even across
    different setup groups and roots. Mutable values are refreshed only when the
    incoming record is at least as trade-configured as the saved record.
    """
    if not records:
        return
    insert_columns = ",".join(f"`{column}`" for column in columns)
    one_row = "(" + ",".join(["%s"] * len(columns)) + ")"
    values_sql = ",".join(one_row for _ in records)
    params: List[Any] = []
    for record in records:
        params.extend(record.get(column) for column in columns)

    mutable_columns = [
        column for column in columns
        if column.lower() not in ({column.lower() for column in ANALYTICS_IDENTITY_COLUMNS} | {"userid"})
    ]
    if "trade_configured" not in {column.lower() for column in mutable_columns}:
        raise RuntimeError("trade_configured must be included in analytics upsert columns")

    incoming_status_rank = (
        "(CASE WHEN COALESCE(VALUES(`closed_trade`),0)=1 THEN 3 "
        "WHEN COALESCE(VALUES(`in_trade`),0)=1 THEN 2 "
        "WHEN COALESCE(VALUES(`pending_entry`),0)=1 THEN 1 ELSE 0 END)"
    )
    saved_status_rank = (
        "(CASE WHEN COALESCE(`closed_trade`,0)=1 THEN 3 "
        "WHEN COALESCE(`in_trade`,0)=1 THEN 2 "
        "WHEN COALESCE(`pending_entry`,0)=1 THEN 1 ELSE 0 END)"
    )
    guard = (
        "(COALESCE(VALUES(`trade_configured`),0) > COALESCE(`trade_configured`,0) "
        "OR (COALESCE(VALUES(`trade_configured`),0) = COALESCE(`trade_configured`,0) "
        f"AND {incoming_status_rank} >= {saved_status_rank}))"
    )
    update_expressions: List[str] = []
    # Assign trade_configured last so the IF() guards above compare with the
    # pre-update value on MySQL/MariaDB's left-to-right ON DUPLICATE KEY semantics.
    for column in mutable_columns:
        lower = column.lower()
        if lower == "trade_configured":
            continue
        if lower == "trade_time":
            update_expressions.append(
                f"`{column}`=IF({guard},COALESCE(`{column}`,VALUES(`{column}`)),`{column}`)"
            )
        else:
            update_expressions.append(
                f"`{column}`=IF({guard},VALUES(`{column}`),`{column}`)"
            )
    update_expressions.append(
        "`trade_configured`=GREATEST(COALESCE(`trade_configured`,0),"
        "COALESCE(VALUES(`trade_configured`),0))"
    )
    update_sql = ",".join(update_expressions)
    sql = (
        f"INSERT INTO `programme_analytics` ({insert_columns}) VALUES {values_sql} "
        f"ON DUPLICATE KEY UPDATE {update_sql}"
    )
    db.query(sql, params, timeout=180)

def _print_write_progress(label: str, completed: int, total: int, width: int = 28) -> None:
    total = max(1, int(total))
    completed = max(0, min(int(completed), total))
    percent = int((completed * 100) / total)
    filled = int(width * completed / total)
    bar = "#" * filled + "-" * (width - filled)
    print(
        f"\r  {label}: {percent:3d}% [{bar}] {completed:,}/{total:,} records",
        end="",
        flush=True,
    )


def ensure_analytics_runtime_schema(db: BridgeClient, programme_id: int, user_id: int,
                                    setup_group_limit: int,
                                    cleanup_duplicates: bool = True) -> None:
    """Ensure analytics runtime columns and deduplication keys are installed."""
    ensure_analytics_group_schema(
        db, programme_id, user_id, setup_group_limit,
        cleanup_duplicates=cleanup_duplicates,
    )
    columns = {column.lower() for column in get_columns(db, "programme_analytics")}
    if "live_trades" not in columns:
        position = " AFTER `trade_configured`" if "trade_configured" in columns else ""
        db.execute(
            "ALTER TABLE `programme_analytics` ADD COLUMN `live_trades` "
            "TINYINT(1) NOT NULL DEFAULT 0" + position,
            timeout=90,
        )
        columns.add("live_trades")
    if "trade_time" not in columns:
        position = " AFTER `event_time`" if "event_time" in columns else ""
        db.execute(
            "ALTER TABLE `programme_analytics` ADD COLUMN `trade_time` DATETIME NULL DEFAULT NULL" + position,
            timeout=90,
        )
        columns.add("trade_time")

    # Additive-only fields make restricted-window and breakeven trades auditable
    # without creating duplicate analytics rows or altering the family identity.
    for column, definition in ANALYTICS_ACCOUNT_RULE_COLUMN_DEFINITIONS.items():
        if column.lower() not in columns:
            db.execute(
                f"ALTER TABLE `programme_analytics` ADD COLUMN `{column}` {definition}",
                timeout=90,
            )
            columns.add(column.lower())


def _parse_challenge_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw or raw.lower() in {"null", "none", "nan"}:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except (TypeError, ValueError, OverflowError):
        return None


def _is_at_or_after(value: Any, start_value: Any) -> bool:
    event_dt = _parse_challenge_datetime(value)
    start_dt = _parse_challenge_datetime(start_value)
    if event_dt is None or start_dt is None:
        return False
    if event_dt.tzinfo is not None and start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=timezone.utc)
    elif event_dt.tzinfo is None and start_dt.tzinfo is not None:
        event_dt = event_dt.replace(tzinfo=timezone.utc)
    if event_dt.tzinfo is not None:
        event_dt = event_dt.astimezone(timezone.utc)
        start_dt = start_dt.astimezone(timezone.utc)
    return event_dt >= start_dt


def _record_is_live_provision_trade(record: Dict[str, Any], live_since: Any) -> bool:
    """Classify a configured signal as live only from the active test start.

    Pending orders use trade_time first: the matcher writes that timestamp once
    when the signal is first identified, and the persistence layer preserves it
    on later refreshes. Once an order is triggered, entry_trigger_time becomes
    the authoritative entry time, matching the challenge event reader.
    """
    if (as_int(record.get("trade_configured"), 0) or 0) != 1:
        return False
    pending = (
        (as_int(record.get("pending_entry"), 0) or 0) == 1
        or str(record.get("outcome") or "").strip().lower().replace("_", " ") == "pending entry"
    )
    # Pending signals have not triggered, so their stable first-identification
    # time is the provision timestamp. Once triggered, use the actual entry time.
    time_keys = (
        ("trade_time", "entry_trigger_time", "entry_anchor_time", "event_time", "created_at")
        if pending else
        ("entry_trigger_time", "entry_anchor_time", "event_time", "trade_time", "created_at")
    )
    event_time = next(
        (record.get(key) for key in time_keys if record.get(key) not in (None, "")),
        None,
    )
    return _is_at_or_after(event_time, live_since)


def persist_pair(db: BridgeClient, programme_id: int, user_id: int, symbol: str, timeframe: str,
                 records: List[Dict[str, Any]],
                 setup_group_limit: int = DEFAULT_TRADES_SETUP_PER_GROUP,
                 live_since: Optional[str] = None,
                 live_enabled: bool = False) -> Dict[str, int]:
    """Persist group-aware analytics rows in bounded multi-row batches."""
    ensure_analytics_runtime_schema(db, programme_id, user_id, setup_group_limit, cleanup_duplicates=False)
    columns = get_columns(db, "programme_analytics")
    lower_columns = {str(column).lower() for column in columns}

    missing = [column for column in ANALYTICS_COLUMNS if column.lower() not in lower_columns]
    if missing:
        raise RuntimeError("programme_analytics is missing required column(s): " + ", ".join(missing))

    records = _deduplicate_records(records)
    for record in records:
        record["live_trades"] = int(
            bool(live_enabled) and bool(live_since)
            and _record_is_live_provision_trade(record, live_since)
        )

    counters = {"inserted": 0, "upserted": 0, "updated": 0, "preserved": 0}
    if not records:
        print(f"  {symbol} {timeframe.upper()}: no new analytics rows; saved families were reused and left unchanged")
        return counters

    unique_identity = _has_exact_unique_identity(db, "programme_analytics", ANALYTICS_IDENTITY_COLUMNS)
    unique_trade_identity = _has_exact_unique_identity(db, "programme_analytics", TRADE_DEDUP_INDEX_COLUMNS)
    if not unique_identity:
        log.warning(
            "    Expected analytics family unique index was not detected; using existing-row checks."
        )
    if not unique_trade_identity:
        log.warning(
            "    Entry/SL/TP trade unique index was not detected; using existing trade price checks."
        )

    total = len(records)
    completed = 0
    label = f"{symbol} {timeframe.upper()} database update"
    for offset in range(0, total, ANALYTICS_BATCH_SIZE):
        chunk = records[offset:offset + ANALYTICS_BATCH_SIZE]
        if unique_identity and unique_trade_identity:
            try:
                _batch_upsert_analytics(db, chunk, ANALYTICS_COLUMNS)
                counters["upserted"] += len(chunk)
                completed += len(chunk)
                _print_write_progress(label, completed, total)
                continue
            except BridgeError as exc:
                log.warning(
                    "\n  Batch of %s analytics rows was rejected; retrying this batch with individual atomic writes. %s",
                    len(chunk), str(exc)[:1200],
                )

        # Compatibility path. It is intentionally limited to a failed chunk so
        # normal operation remains batched and avoids hundreds of bridge calls.
        for record in chunk:
            status = _insert_or_refresh_row(
                db, "programme_analytics", record, ANALYTICS_COLUMNS,
                ANALYTICS_IDENTITY_COLUMNS, unique_identity,
                f"{symbol} {timeframe.upper()} group {record.get('setup_group_id')} "
                f"family {record.get('family_number')}",
            )
            counters[status] = counters.get(status, 0) + 1
            completed += 1
            _print_write_progress(label, completed, total)

    print()
    return counters

def mark_existing_live_provision_analytics(
    db: BridgeClient, programme_id: int, user_id: int, live_since: str
) -> int:
    """Reconcile already-saved analytics signals against the active test start.

    This catches pending orders reused from the local family cache or carried
    forward from a previous scan. Pending orders use their stable trade_time
    (first identified timestamp); triggered trades use their actual entry time.
    The challenge page reads programme_analytics.live_trades, so this is the
    authoritative live flag consumed by the provision monitor.
    """
    if not live_since or not table_exists(db, "programme_analytics"):
        return 0
    columns = {column.lower() for column in get_columns(db, "programme_analytics")}
    required = {"programme_id", "userid", "trade_configured", "live_trades"}
    if not required.issubset(columns):
        log.warning(
            "    Existing live-signal reconciliation skipped; missing analytics columns: %s",
            ", ".join(sorted(required - columns)),
        )
        return 0

    pending_parts = []
    if "pending_entry" in columns:
        pending_parts.append("COALESCE(`pending_entry`,0)=1")
    if "outcome" in columns:
        pending_parts.append("LOWER(REPLACE(COALESCE(`outcome`,''),'_',' '))='pending entry'")
    pending_expr = "(" + " OR ".join(pending_parts) + ")" if pending_parts else "(1=0)"

    def coalesce_expr(candidates: Sequence[str]) -> str:
        available = [f"`{name}`" for name in candidates if name in columns]
        if not available:
            return "NULL"
        return available[0] if len(available) == 1 else "COALESCE(" + ",".join(available) + ")"

    pending_time = coalesce_expr((
        "trade_time", "entry_trigger_time", "entry_anchor_time", "event_time", "created_at"
    ))
    triggered_time = coalesce_expr((
        "entry_trigger_time", "entry_anchor_time", "event_time", "trade_time", "created_at"
    ))
    if pending_time == "NULL" or triggered_time == "NULL":
        log.warning("    Existing live-signal reconciliation skipped; no usable analytics timestamps exist")
        return 0

    matched_filter = " AND COALESCE(`matched_family`,1)<>0" if "matched_family" in columns else ""
    sql = (
        "UPDATE `programme_analytics` SET `live_trades` = CASE "
        f"WHEN {pending_expr} THEN CASE WHEN {pending_time}>=%s THEN 1 ELSE 0 END "
        f"ELSE CASE WHEN {triggered_time}>=%s THEN 1 ELSE 0 END END "
        "WHERE `programme_id`=%s AND `userid`=%s AND COALESCE(`trade_configured`,0)=1"
        + matched_filter
    )
    try:
        changed = db.execute(sql, [live_since, live_since, programme_id, user_id], timeout=120)
        log.info(
            "    Existing Analytics live flags reconciled from %s (%s row(s) affected)",
            live_since, changed,
        )
        return max(0, int(changed or 0))
    except Exception as exc:
        log.error("    Existing Analytics live-flag reconciliation failed: %s", str(exc)[:900])
        raise


def print_persistent_analytics_summary(db: BridgeClient, programme_id: int, user_id: int) -> None:
    """Show what the PHP dashboard can already read before this scan writes anything."""
    print("SAVED ANALYTICS BASELINE (before this scan):")
    try:
        rows = db.fetch_all(
            "SELECT `symbol`,`timeframe`,`root_id`,COALESCE(`confirmation_root_id`,0) AS `confirmation_root_id`, "
            "COUNT(*) AS `analytics_rows`,COALESCE(SUM(`matched_family`),0) AS `matched_families`, "
            "COALESCE(SUM(`trade_configured`),0) AS `configured_trades` "
            "FROM `programme_analytics` WHERE `programme_id`=%s AND `userid`=%s "
            "GROUP BY `symbol`,`timeframe`,`root_id`,COALESCE(`confirmation_root_id`,0) "
            "ORDER BY `symbol`,`timeframe`,`root_id`,COALESCE(`confirmation_root_id`,0)",
            [programme_id, user_id], timeout=90,
        )
    except Exception as exc:
        print(f"  Could not read the pre-existing analytics baseline: {str(exc)[:800]}")
        return
    if not rows:
        print("  No pre-existing programme_analytics rows for this programme/user.")
        return
    total_rows = total_families = total_trades = 0
    table_rows: List[Tuple[str, str, str, str, int, int, int]] = []
    for row in rows:
        count = as_int(row.get("analytics_rows"), 0) or 0
        families = as_int(row.get("matched_families"), 0) or 0
        trades = as_int(row.get("configured_trades"), 0) or 0
        total_rows += count
        total_families += families
        total_trades += trades
        confirmation_id = as_int(row.get("confirmation_root_id"), 0) or 0
        scope_name = f"Confirmation {confirmation_id}" if confirmation_id else "Main root"
        table_rows.append((
            str(row.get("symbol") or ""), str(row.get("timeframe") or "").upper(),
            str(row.get("root_id") or ""), scope_name, count, families, trades,
        ))

    headers = ("SYMBOL", "TF", "ROOT", "SCOPE", "ROWS", "FAMILIES", "TRADES")
    rendered = [headers] + [
        (symbol, timeframe, root_id, scope, f"{count:,}", f"{families:,}", f"{trades:,}")
        for symbol, timeframe, root_id, scope, count, families, trades in table_rows
    ]
    widths = [max(len(str(row[index])) for row in rendered) for index in range(len(headers))]
    print("  " + "  ".join(str(value).ljust(widths[index]) for index, value in enumerate(headers)))
    print("  " + "  ".join("-" * width for width in widths))
    for values in rendered[1:]:
        print("  " + "  ".join(
            str(value).rjust(widths[index]) if index >= 4 else str(value).ljust(widths[index])
            for index, value in enumerate(values)
        ))
    print("  " + "-" * (sum(widths) + 2 * (len(widths) - 1)))
    print(f"  STORED TOTALS: {total_rows:,} analytics rows | {total_families:,} matched families | {total_trades:,} configured trades")
    print("  Note: these are the saved totals visible to the PHP analytics page, not the current scan's match count.")


def _timeframe_order_key(tf: str) -> Tuple[int, str]:
    """Higher timeframes first, with a stable lexical tie-breaker."""
    duration = tf_milliseconds(tf)
    return (-(duration if duration is not None else 0), str(tf).lower())


def _trade_outcome_counts(records: Sequence[Dict[str, Any]]) -> Dict[str, int]:
    return {
        "families": len(records),
        "trades": sum(1 for r in records if int(r.get("trade_configured") or 0) == 1),
        "stoploss": sum(1 for r in records if r.get("outcome") == "stoploss hit"),
        "breakeven": sum(1 for r in records if r.get("outcome") == "breakeven hit"),
        "breakeven_triggered": sum(1 for r in records if int(r.get("breakeven_triggered") or 0) == 1),
        "restricted_dates": sum(1 for r in records if int(r.get("restricted_date_hit") or 0) == 1),
        "restricted_days": sum(1 for r in records if int(r.get("restricted_day_hit") or 0) == 1),
        "profit": sum(1 for r in records if r.get("outcome") == "take profit reached"),
        "pending": sum(1 for r in records if r.get("outcome") == "pending entry"),
        "in_trade": sum(1 for r in records if r.get("outcome") == "in trade"),
    }


def _root_display_name(root: Dict[str, Any], confirmation: bool = False) -> str:
    name = str(root.get("name") or "").strip()
    if name:
        return name
    prefix = "Confirmation " if confirmation else "Root "
    return prefix + str(root.get("id") or "?")


def _snapshot_write(path: Path, payload: Dict[str, Any]) -> None:
    """Write a complete, readable JSON snapshot atomically."""
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, default=str)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _trade_record_preference(record: Dict[str, Any]) -> Tuple[int, int, int, int, str, str]:
    """Prefer configured, closed/current rows when records share the same E/SL/TP."""
    configured = as_int(record.get("trade_configured"), 0) or 0
    closed = as_int(record.get("closed_trade"), 0) or 0
    in_trade = as_int(record.get("in_trade"), 0) or 0
    pending = as_int(record.get("pending_entry"), 0) or 0
    return (
        configured,
        closed,
        in_trade,
        pending,
        str(record.get("event_time") or ""),
        str(record.get("trade_time") or ""),
    )


def _deduplicate_records(records: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Deduplicate family rows, then configured trades by exact E/SL/TP identity.

    The first pass keeps one row per chart family identity. The second pass removes
    configured trades with the same actual entry_price + stoploss_price +
    take_profit_price, independent of root/confirmation/group/setup numbering.
    Match-only rows have no trade fingerprint and remain family records.
    """
    by_family_identity: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
    for record in records:
        identity = tuple(record.get(column) for column in ANALYTICS_IDENTITY_COLUMNS)
        current = by_family_identity.get(identity)
        if current is None:
            by_family_identity[identity] = record
        else:
            current_trade = as_int(current.get("trade_configured"), 0) or 0
            new_trade = as_int(record.get("trade_configured"), 0) or 0
            if new_trade > current_trade or (
                new_trade == current_trade
                and _trade_record_preference(record) >= _trade_record_preference(current)
            ):
                by_family_identity[identity] = record

    # Same candle family under a different displayed setup number is one family.
    by_family_signature: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
    without_signature: List[Dict[str, Any]] = []
    for record in by_family_identity.values():
        signature = record.get("family_signature")
        if not signature:
            without_signature.append(record)
            continue
        scope = (
            record.get("programme_id"), record.get("userid"),
            str(record.get("symbol") or "").upper(),
            str(record.get("timeframe") or "").lower(),
            str(record.get("source_timeframe") or "").lower(),
            record.get("root_id"), record.get("confirmation_root_id") or 0,
            str(signature),
        )
        current = by_family_signature.get(scope)
        if current is None:
            by_family_signature[scope] = record
        elif (
            (as_int(record.get("trade_configured"), 0) or 0)
            > (as_int(current.get("trade_configured"), 0) or 0)
            or _trade_record_preference(record) >= _trade_record_preference(current)
        ):
            by_family_signature[scope] = record

    # Finally deduplicate actual trades on price triplet. This scope intentionally
    # has no root/group/family/source timeframe dimensions.
    by_trade_identity: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
    match_only_or_incomplete: List[Dict[str, Any]] = []
    for record in list(by_family_signature.values()) + without_signature:
        trade_hash = _trade_identity_hash(record)
        record["trade_identity_hash"] = trade_hash
        if not trade_hash:
            match_only_or_incomplete.append(record)
            continue
        identity = (
            record.get("programme_id"), record.get("userid"),
            str(record.get("symbol") or "").upper(),
            str(record.get("timeframe") or "").lower(), trade_hash,
        )
        current = by_trade_identity.get(identity)
        if current is None or _trade_record_preference(record) >= _trade_record_preference(current):
            by_trade_identity[identity] = record

    return list(by_trade_identity.values()) + match_only_or_incomplete



# ----------------------- Incremental family checkpoints -----------------------
def _sha256_payload(value: Any) -> str:
    return hashlib.sha256(safe_json(value).encode("utf-8")).hexdigest()


def candle_prefix_hash(candles: Sequence[Dict[str, Any]], end_index: Optional[int] = None) -> str:
    """Hash the exact historical candle prefix used by a checkpoint.

    A later appended candle does not change an earlier prefix. A changed, deleted,
    or backfilled historical candle does, forcing a safe full recomputation.
    """
    if end_index is None:
        end_index = len(candles) - 1
    digest = hashlib.sha256()
    if end_index < 0:
        return digest.hexdigest()
    for candle in candles[:end_index + 1]:
        fields = (
            candle.get("id"), candle.get("time"), candle.get("open_time"),
            candle.get("close_time"), candle.get("open"), candle.get("high"),
            candle.get("low"), candle.get("close"),
        )
        digest.update(safe_json(fields).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def _progress_scope_key(row: Dict[str, Any]) -> Tuple[str, int, int, str, str]:
    return (
        str(row.get("symbol") or ""),
        as_int(row.get("root_id"), 0) or 0,
        as_int(row.get("confirmation_root_id"), 0) or 0,
        str(row.get("source_timeframe") or "").lower(),
        str(row.get("timeframe") or "").lower(),
    )


def load_training_progress(db: BridgeClient, programme_id: int, user_id: int) -> Dict[Tuple[str, int, int, str, str], Dict[str, Any]]:
    """Load checkpoints once per programme. Missing table means first-run/full scan."""
    try:
        if not table_exists(db, PROGRESS_TABLE):
            return {}
        rows = db.fetch_all(
            f"SELECT * FROM `{PROGRESS_TABLE}` WHERE `programme_id`=%s AND `userid`=%s",
            [programme_id, user_id], timeout=60,
        )
    except Exception as exc:
        log.warning("Incremental checkpoint read unavailable; this scan will rebuild matches: %s", str(exc)[:800])
        return {}
    return {_progress_scope_key(row): row for row in rows}


def inspect_progress(row: Optional[Dict[str, Any]], config_hash: str,
                     candles: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """Check whether a saved checkpoint still represents an unchanged prefix."""
    result = {"valid": False, "appended": False, "last_index": -1, "reason": "no checkpoint"}
    if not row:
        return result
    if str(row.get("config_hash") or "") != config_hash:
        result["reason"] = "configuration changed"
        return result

    saved_id = row.get("last_candle_id")
    if saved_id in (None, ""):
        if candles:
            index = -1
        else:
            index = -1
    else:
        saved_id_text = str(saved_id)
        index = next((i for i, candle in enumerate(candles)
                      if candle.get("id") is not None and str(candle.get("id")) == saved_id_text), -2)
        if index == -2:
            result["reason"] = "checkpoint candle is no longer present"
            return result

    expected_hash = str(row.get("prefix_hash") or "")
    actual_hash = candle_prefix_hash(candles, index)
    if not expected_hash or expected_hash != actual_hash:
        result["reason"] = "historical candle prefix changed"
        return result

    result.update({
        "valid": True,
        "last_index": index,
        "appended": index < len(candles) - 1,
        "reason": "new candles appended" if index < len(candles) - 1 else "no candle changes",
        "last_family_number": as_int(row.get("last_parent_family_number"), 0) or 0,
        "processed_family_count": as_int(row.get("processed_family_count"), 0) or 0,
        "saved_match_count": as_int(row.get("saved_match_count")),
    })
    return result


def build_progress_row(programme_id: int, user_id: int, symbol: str, root_id: int,
                       confirmation_root_id: int, source_timeframe: str, timeframe: str,
                       config_hash: str, candles: Sequence[Dict[str, Any]],
                       last_family_number: int, processed_family_count: int,
                       saved_match_count: Optional[int] = None) -> Dict[str, Any]:
    last = candles[-1] if candles else {}
    last_stamp = dt_text(last.get("time") or last.get("open_time")) if last else None
    return {
        "programme_id": programme_id,
        "userid": user_id,
        "symbol": symbol,
        "root_id": root_id,
        "confirmation_root_id": confirmation_root_id,
        "source_timeframe": source_timeframe.lower(),
        "timeframe": timeframe.lower(),
        "config_hash": config_hash,
        "last_candle_id": str(last.get("id")) if last.get("id") is not None else None,
        "last_candle_time": last_stamp,
        "prefix_hash": candle_prefix_hash(candles),
        "last_parent_family_number": max(0, int(last_family_number)),
        "processed_family_count": max(0, int(processed_family_count)),
        "saved_match_count": max(0, int(
            processed_family_count if saved_match_count is None else saved_match_count
        )),
    }



# ---------------------- Local persistent matcher cache ----------------------
def local_parent_scope_key(symbol: str, root_id: int, parent_tf: str) -> str:
    return f"parent|{str(symbol).upper()}|{int(root_id)}|{str(parent_tf).lower()}"


def local_confirmation_scope_key(symbol: str, root_id: int, parent_tf: str, child_tf: str) -> str:
    return (
        f"confirmation|{str(symbol).upper()}|{int(root_id)}|"
        f"{str(parent_tf).lower()}|{str(child_tf).lower()}"
    )


def local_family_cache_path(programme_id: int, user_id: int) -> Path:
    """Place the durable cache beside the worker so Windows/XAMPP runs reuse it."""
    base = Path(__file__).resolve().parent / LOCAL_FAMILY_CACHE_DIRNAME
    return base / f"programme_{int(programme_id)}_user_{int(user_id)}.json.gz"


def load_local_family_cache(programme_id: int, user_id: int) -> Tuple[Dict[str, Any], Path, bool]:
    """Load the local compressed cache. Missing/corrupt files cause a safe full rebuild."""
    path = local_family_cache_path(programme_id, user_id)
    empty = {
        "schema": LOCAL_FAMILY_CACHE_SCHEMA,
        "programme_id": int(programme_id),
        "user_id": int(user_id),
        "scopes": {},
    }
    if not path.exists():
        return empty, path, False
    try:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            payload = json.load(handle)
        if not isinstance(payload, dict):
            raise ValueError("cache root is not an object")
        if payload.get("schema") != LOCAL_FAMILY_CACHE_SCHEMA:
            raise ValueError("cache schema version does not match this worker")
        if as_int(payload.get("programme_id")) != int(programme_id):
            raise ValueError("cache programme ID does not match")
        if as_int(payload.get("user_id")) != int(user_id):
            raise ValueError("cache owner ID does not match")
        scopes = payload.get("scopes")
        if not isinstance(scopes, dict):
            raise ValueError("cache scopes are not an object")
        payload["scopes"] = scopes
        return payload, path, True
    except Exception as exc:
        log.warning(
            "Local family cache could not be loaded (%s). A full candle-history rebuild will be used; "
            "the existing Programme Analytics rows will not be deleted. Reason: %s",
            path, str(exc)[:600],
        )
        return empty, path, False


def save_local_family_cache(path: Path, payload: Dict[str, Any]) -> None:
    """Atomically persist the reusable matcher cache after DB analytics writes succeed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8")
    with temporary.open("wb") as raw_handle:
        with gzip.GzipFile(fileobj=raw_handle, mode="wb", mtime=0) as compressed:
            compressed.write(encoded)
        raw_handle.flush()
        os.fsync(raw_handle.fileno())
    os.replace(temporary, path)


def local_cache_progress_index(scopes: Dict[str, Any]) -> Dict[Tuple[str, int, int, str, str], Dict[str, Any]]:
    """Build checkpoint lookup from the local cache; DB progress is a write-only mirror."""
    result: Dict[Tuple[str, int, int, str, str], Dict[str, Any]] = {}
    for entry in scopes.values():
        if not isinstance(entry, dict):
            continue
        progress = entry.get("progress")
        if isinstance(progress, dict):
            result[_progress_scope_key(progress)] = progress
    return result


def set_local_cache_scope(scopes: Dict[str, Any], key: str, entry: Dict[str, Any]) -> bool:
    """Replace one scope only when its contents actually changed."""
    if scopes.get(key) == entry:
        return False
    scopes[key] = entry
    return True


def serialize_parent_family_for_local_cache(
    family: Dict[str, Any], candles: Sequence[Dict[str, Any]], root_id: int, timeframe: str
) -> Dict[str, Any]:
    """Store parent map references as candle-record IDs, never array offsets."""
    map_ids: Dict[str, Any] = {}
    id_to_index = {
        str(candle.get("id")): index
        for index, candle in enumerate(candles)
        if candle.get("id") is not None
    }
    ids = [str(candle.get("id")) for candle in candles if candle.get("id") is not None]
    if len(id_to_index) != len(ids):
        raise ValueError("candle data contains duplicate IDs; local family maps cannot be safely keyed by ID")
    for identity, raw_index in (family.get("map") or {}).items():
        index = as_int(raw_index, -1)
        if index is None or index < 0 or index >= len(candles):
            raise ValueError(f"parent family {family.get('familyNumber')} has an invalid candle index")
        candle_id = candles[index].get("id")
        if candle_id is None:
            raise ValueError(f"parent family {family.get('familyNumber')} references a candle without an ID")
        map_ids[str(identity)] = str(candle_id)
    if not map_ids:
        raise ValueError(f"parent family {family.get('familyNumber')} has an empty candle map")
    mapped_indices = {identity: id_to_index[str(candle_id)] for identity, candle_id in map_ids.items()}
    signature = family_identity_signature(root_id, timeframe, 0, mapped_indices, candles)
    conditions = family.get("conditions") or []
    if not isinstance(conditions, list):
        raise ValueError(f"parent family {family.get('familyNumber')} conditions are not a list")
    # Round-trip to ensure the local file contains only JSON-safe matcher data.
    conditions = json.loads(safe_json(conditions))
    ordinal, setup_group_id, group_family_number = family_group_identity(family)
    return {
        # Local cache retains the global ordinal; database rows store group-local
        # family_number plus setup_group_id.
        "family_number": ordinal,
        "setup_group_id": setup_group_id,
        "group_family_number": group_family_number,
        "family_signature": signature,
        "family_map_candle_ids": map_ids,
        "conditions": conditions,
        "no_trade": bool(family.get("noTrade")),
        "trade_configured": int(bool(family.get("savedTradeConfigured"))),
        "outcome": str(family.get("savedOutcome") or ""),
    }


def restore_parent_families_from_local_cache(
    entry: Dict[str, Any], candles: Sequence[Dict[str, Any]], root_id: int, timeframe: str,
    setup_group_limit: int = DEFAULT_TRADES_SETUP_PER_GROUP,
) -> Tuple[List[Dict[str, Any]], int]:
    """Rehydrate parent anchors from local IDs, checking candle presence and signatures."""
    entries = entry.get("families")
    progress = entry.get("progress") or {}
    if not isinstance(entries, list):
        raise ValueError("local parent cache has no family list")
    id_to_index = {
        str(candle.get("id")): index
        for index, candle in enumerate(candles)
        if candle.get("id") is not None
    }
    candle_ids = [str(candle.get("id")) for candle in candles if candle.get("id") is not None]
    if len(id_to_index) != len(candle_ids):
        raise ValueError("current parent candle data contains duplicate candle IDs")
    families: List[Dict[str, Any]] = []
    trade_count = 0
    for saved in entries:
        if not isinstance(saved, dict):
            raise ValueError("local parent cache contains a malformed family")
        family_number = as_int(saved.get("family_number"))
        raw_map = saved.get("family_map_candle_ids")
        conditions = saved.get("conditions")
        if family_number is None or family_number <= 0 or not isinstance(raw_map, dict) or not isinstance(conditions, list):
            raise ValueError("local parent cache contains an invalid family number/map/condition list")
        mapped: Dict[str, int] = {}
        for identity, candle_id in raw_map.items():
            index = id_to_index.get(str(candle_id))
            if index is None:
                raise ValueError(f"parent family {family_number} refers to missing candle ID {candle_id}")
            mapped[str(identity)] = index
        if not mapped:
            raise ValueError(f"parent family {family_number} has an empty saved map")
        actual_signature = family_identity_signature(root_id, timeframe, 0, mapped, candles)
        saved_signature = str(saved.get("family_signature") or "")
        if not saved_signature or saved_signature != actual_signature:
            raise ValueError(f"parent family {family_number} signature does not match current candles")
        configured = int(bool(as_int(saved.get("trade_configured"), 0)))
        outcome = str(saved.get("outcome") or "").strip().lower()
        restored_family = {
            "familyNumber": family_number,
            "familyOrdinal": family_number,
            "map": mapped,
            "conditions": conditions,
            "noTrade": bool(saved.get("no_trade")),
            "savedTradeConfigured": bool(configured),
            "savedOutcome": outcome,
        }
        apply_setup_group_identity(restored_family, setup_group_limit)
        families.append(restored_family)
        trade_count += configured

    families.sort(key=lambda item: int(item["familyNumber"]))
    numbers = [int(item["familyNumber"]) for item in families]
    expected_count = max(0, as_int(progress.get("processed_family_count"), len(families)) or 0)
    expected_highwater = max(0, as_int(progress.get("last_parent_family_number"), 0) or 0)
    if len(numbers) != expected_count:
        raise ValueError(f"local parent cache contains {len(numbers)} families; checkpoint expects {expected_count}")
    if numbers != list(range(1, expected_highwater + 1)):
        if expected_highwater == 0 and not numbers:
            pass
        else:
            raise ValueError("local parent family numbers are not consecutive through the checkpoint high-water mark")
    return families, trade_count


LOCAL_CONFIRMATION_CACHE_FIELDS = (
    "family_number", "setup_group_id", "family_ordinal", "confirmation_root_id",
    "trade_configured", "outcome", "family_signature", "family_map_json", "match_json",
)


def confirmation_row_for_local_cache(
    record: Dict[str, Any],
    setup_group_limit: int = DEFAULT_TRADES_SETUP_PER_GROUP,
) -> Dict[str, Any]:
    """Normalize cache rows while preserving their global parent-family ordinal.

    Older local caches stored the ungrouped, globally increasing family_number.
    New cache rows store group-local family_number, setup_group_id and the internal
    family_ordinal. This accepts both layouts and always returns the current group
    split for the configured server_account limit.
    """
    row = {field: record.get(field) for field in LOCAL_CONFIRMATION_CACHE_FIELDS}
    raw_family_number = as_int(record.get("family_number"))
    explicit_ordinal = as_int(record.get("family_ordinal"))
    raw_group_id = as_int(record.get("setup_group_id"))
    if explicit_ordinal is not None and explicit_ordinal > 0:
        ordinal = explicit_ordinal
    elif raw_group_id is not None:
        ordinal = family_ordinal_from_group(raw_family_number, raw_group_id, setup_group_limit)
    else:
        # Compatibility with pre-group local caches: their family_number was
        # the global ordinal, not the new group-local display number.
        ordinal = raw_family_number
    if ordinal is None or ordinal <= 0:
        raise ValueError("confirmation analytics record is missing its family/root identity")
    group_id, local_number = split_family_ordinal(ordinal, setup_group_limit)
    row["family_number"] = local_number
    row["setup_group_id"] = group_id
    row["family_ordinal"] = ordinal
    row["confirmation_root_id"] = as_int(row.get("confirmation_root_id"))
    row["trade_configured"] = int(bool(as_int(row.get("trade_configured"), 0)))
    row["outcome"] = str(row.get("outcome") or "")
    row["family_signature"] = str(row.get("family_signature") or "")
    row["family_map_json"] = str(row.get("family_map_json") or "{}")
    row["match_json"] = str(row.get("match_json") or "[]")
    if row["confirmation_root_id"] is None:
        raise ValueError("confirmation analytics record is missing its confirmation root identity")
    parsed_map = json.loads(row["family_map_json"])
    parsed_conditions = json.loads(row["match_json"])
    if not isinstance(parsed_map, dict) or not isinstance(parsed_conditions, list):
        raise ValueError("confirmation analytics record contains an invalid family map/condition list")
    return row


def restore_confirmation_rows_from_local_cache(
    entry: Dict[str, Any], lower_candles: Sequence[Dict[str, Any]], root_id: int, child_tf: str,
    setup_group_limit: int = DEFAULT_TRADES_SETUP_PER_GROUP,
) -> List[Dict[str, Any]]:
    """Validate local confirmation maps against current lower-TF candle IDs."""
    rows = entry.get("rows")
    if not isinstance(rows, list):
        raise ValueError("local confirmation cache has no row list")
    id_to_index = {
        str(candle.get("id")): index
        for index, candle in enumerate(lower_candles)
        if candle.get("id") is not None
    }
    candle_ids = [str(candle.get("id")) for candle in lower_candles if candle.get("id") is not None]
    if len(id_to_index) != len(candle_ids):
        raise ValueError("current confirmation candle data contains duplicate candle IDs")
    restored: List[Dict[str, Any]] = []
    seen = set()
    for saved in rows:
        if not isinstance(saved, dict):
            raise ValueError("local confirmation cache contains a malformed row")
        row = confirmation_row_for_local_cache(saved, setup_group_limit)
        try:
            raw_map = json.loads(row["family_map_json"] or "{}")
            conditions = json.loads(row["match_json"] or "[]")
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError("local confirmation cache contains invalid JSON") from exc
        mapped: Dict[str, int] = {}
        for identity, candle_id in raw_map.items():
            index = id_to_index.get(str(candle_id))
            if index is None:
                raise ValueError(f"confirmation family {row['family_number']} refers to missing candle ID {candle_id}")
            mapped[str(identity)] = index
        if not mapped or not isinstance(conditions, list):
            raise ValueError(f"confirmation family {row['family_number']} has an invalid map/condition list")
        actual_signature = family_identity_signature(
            root_id, child_tf, int(row["confirmation_root_id"]), mapped, lower_candles
        )
        if row["family_signature"] and row["family_signature"] != actual_signature:
            raise ValueError(f"confirmation family {row['family_number']} signature does not match current candles")
        identity = (int(row["confirmation_root_id"]), int(row["family_ordinal"]))
        if identity in seen:
            raise ValueError(f"duplicate confirmation cache identity {identity}")
        seen.add(identity)
        restored.append(row)
    restored.sort(key=lambda item: (int(item["confirmation_root_id"]), int(item["family_ordinal"])))
    return restored


def confirmation_cache_summary(rows: Sequence[Dict[str, Any]]) -> Dict[int, Dict[str, int]]:
    summary: Dict[int, Dict[str, int]] = {}
    for row in rows:
        confirmation_id = as_int(row.get("confirmation_root_id"))
        if confirmation_id is None:
            continue
        item = summary.setdefault(confirmation_id, {"family_count": 0, "trade_count": 0})
        item["family_count"] += 1
        item["trade_count"] += int(bool(as_int(row.get("trade_configured"), 0)))
    return summary


def append_progress_if_changed(
    progress_index: Dict[Tuple[str, int, int, str, str], Dict[str, Any]],
    pending_updates: List[Dict[str, Any]],
    row: Dict[str, Any],
) -> None:
    """Avoid writing an unchanged checkpoint every idle polling cycle."""
    previous = progress_index.get(_progress_scope_key(row))
    if previous is None:
        pending_updates.append(row)
        return
    numeric_fields = ("last_parent_family_number", "processed_family_count", "saved_match_count")
    text_fields = (
        "config_hash", "last_candle_id", "last_candle_time", "prefix_hash",
        "source_timeframe", "timeframe",
    )
    for field in numeric_fields:
        if (as_int(previous.get(field), 0) or 0) != (as_int(row.get(field), 0) or 0):
            pending_updates.append(row)
            return
    for field in text_fields:
        old_value = previous.get(field)
        new_value = row.get(field)
        if str(old_value or "") != str(new_value or ""):
            pending_updates.append(row)
            return

def _provider_trade_configuration(provider_data: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Values that can change historical trade calculations without changing candles."""
    provider_data = provider_data or {}
    account_rules = provider_data.get("account_management_rules") or {}
    def clean_rule_rows(key: str, fields: Sequence[str]) -> List[Dict[str, str]]:
        cleaned: List[Dict[str, str]] = []
        for row in account_rules.get(key) or []:
            if not isinstance(row, dict):
                continue
            cleaned.append({field: str(row.get(field) if row.get(field) is not None else "") for field in fields})
        return cleaned
    return {
        "risk_reward": as_float(provider_data.get("risk_reward")),
        "risk_reward_type": str(provider_data.get("risk_reward_type") or "").strip().lower(),
        "interest_status": str(provider_data.get("interest_status") or "").strip().lower(),
        "account_management_rules": {
            "timezone": str(account_rules.get("timezone") or "UTC"),
            "restricted_dates": clean_rule_rows("restricted_dates", ["restricted_date"]),
            "restricted_days": clean_rule_rows("restricted_days", ["day", "from_time", "to_time"]),
            "trades_breakeven": clean_rule_rows("trades_breakeven", ["floating_profit_at_risk_reward", "breakeven_at_risk_reward"]),
        },
    }


def root_configuration_hash(root: Dict[str, Any],
                            provider_data: Optional[Dict[str, Any]] = None,
                            setup_group_limit: int = DEFAULT_TRADES_SETUP_PER_GROUP) -> str:
    return _sha256_payload({
        "matcher_cache_version": MATCHER_CACHE_VERSION,
        "setup_group_limit": max(1, int(setup_group_limit or DEFAULT_TRADES_SETUP_PER_GROUP)),
        "root_configuration": canonical_configuration([root]),
        "provider_trade_configuration": _provider_trade_configuration(provider_data),
    })


def confirmation_configuration_hash(root: Dict[str, Any], confirmations: Sequence[Dict[str, Any]],
                                    child_tf: str,
                                    provider_data: Optional[Dict[str, Any]] = None,
                                    setup_group_limit: int = DEFAULT_TRADES_SETUP_PER_GROUP) -> str:
    configured = []
    for confirmation in confirmations:
        configured.append({
            "id": confirmation.get("id"),
            "name": confirmation.get("name"),
            "timeframes": list(confirmation.get("timeframes") or []),
            "anchor_candle_id": confirmation.get("anchorCandleId"),
            "anchor_candle_name": confirmation.get("anchorCandleName"),
            "anchor_time": confirmation.get("anchorTime"),
            "configuration": canonical_configuration([confirmation]),
        })
    return _sha256_payload({
        "matcher_cache_version": MATCHER_CACHE_VERSION,
        "setup_group_limit": max(1, int(setup_group_limit or DEFAULT_TRADES_SETUP_PER_GROUP)),
        "parent_configuration": canonical_configuration([root]),
        "child_timeframe": child_tf.lower(),
        "confirmation_alternatives": configured,
        "provider_trade_configuration": _provider_trade_configuration(provider_data),
    })


def fetch_existing_parent_families(db: BridgeClient, programme_id: int, user_id: int,
                                   symbol: str, timeframe: str, root_id: int,
                                   candles: Sequence[Dict[str, Any]],
                                   max_family_number: Optional[int] = None,
                                   setup_group_limit: int = DEFAULT_TRADES_SETUP_PER_GROUP) -> Tuple[List[Dict[str, Any]], int]:
    """Rehydrate saved parent families in group order using bounded keyset reads.

    The database stores a group-local family_number. This helper reconstructs the
    internal global ordinal as ((setup_group_id - 1) * group_limit) + family_number
    so Group 2 / Setup 1 follows Group 1 / Setup N rather than colliding with it.
    Legacy tables without setup_group_id are still read as globally numbered rows.
    """
    target_ordinal = max(0, int(max_family_number)) if max_family_number is not None else None
    if target_ordinal == 0:
        return [], 0

    columns = {column.lower() for column in get_columns(db, "programme_analytics")}
    has_group_column = "setup_group_id" in columns
    limit = max(1, int(setup_group_limit or DEFAULT_TRADES_SETUP_PER_GROUP))
    if has_group_column:
        ordinal_expr = f"(((GREATEST(1,COALESCE(`setup_group_id`,1))-1)*{limit})+`family_number`)"
        group_expr = "GREATEST(1,COALESCE(`setup_group_id`,1))"
    else:
        ordinal_expr = "`family_number`"
        group_expr = "1"

    id_to_index = {
        str(candle.get("id")): index
        for index, candle in enumerate(candles)
        if candle.get("id") is not None
    }
    families: List[Dict[str, Any]] = []
    trade_count = 0
    last_ordinal = 0
    page_size = PARENT_ANALYTICS_READ_BATCH_SIZE

    while True:
        upper_limit_sql = f" AND {ordinal_expr}<=%s" if target_ordinal is not None else ""
        sql = (
            f"SELECT `family_number`,{group_expr} AS `setup_group_id`,"
            f"{ordinal_expr} AS `family_ordinal`,"
            "HEX(`family_signature`) AS `family_signature_hex`,"
            "HEX(`family_map_json`) AS `family_map_json_hex`,"
            "HEX(`match_json`) AS `match_json_hex`,`trade_configured`,"
            "HEX(`outcome`) AS `outcome_hex` "
            "FROM `programme_analytics` WHERE `programme_id`=%s AND `userid`=%s "
            "AND `symbol`=%s AND `timeframe`=%s AND `source_timeframe`=%s "
            "AND `root_id`=%s AND `confirmation_root_id`=0 AND `matched_family`=1 "
            f"AND {ordinal_expr}>%s" + upper_limit_sql +
            f" ORDER BY {group_expr} ASC,`family_number` ASC LIMIT {int(page_size)}"
        )
        params: List[Any] = [programme_id, user_id, symbol, timeframe, timeframe, root_id, last_ordinal]
        if target_ordinal is not None:
            params.append(target_ordinal)

        try:
            page = db.fetch_all(sql, params, timeout=90)
        except BridgeError as exc:
            if page_size > 1:
                smaller = max(1, page_size // 2)
                log.warning(
                    "  PARENT CACHE: group-aware read failed at %s rows; retrying with %s-row pages "
                    "(root %s, next family ordinal after %s): %s",
                    page_size, smaller, root_id, last_ordinal, str(exc)[:500],
                )
                page_size = smaller
                continue
            probe_sql = (
                f"SELECT {ordinal_expr} AS `family_ordinal` FROM `programme_analytics` "
                "WHERE `programme_id`=%s AND `userid`=%s AND `symbol`=%s "
                "AND `timeframe`=%s AND `source_timeframe`=%s AND `root_id`=%s "
                "AND `confirmation_root_id`=0 AND `matched_family`=1 "
                f"AND {ordinal_expr}>%s"
                + (f" AND {ordinal_expr}<=%s" if target_ordinal is not None else "")
                + f" ORDER BY {ordinal_expr} ASC LIMIT 1"
            )
            probe_params: List[Any] = [programme_id, user_id, symbol, timeframe, timeframe, root_id, last_ordinal]
            if target_ordinal is not None:
                probe_params.append(target_ordinal)
            try:
                probe = db.fetch_all(probe_sql, probe_params, timeout=45)
                detail = "scalar group-aware lookup succeeded" if probe else "scalar group-aware lookup returned no row"
            except BridgeError as probe_exc:
                detail = "scalar group-aware lookup also failed: " + str(probe_exc)[:500]
            raise BridgeError(
                f"Could not read saved parent analytics for {symbol} {timeframe.upper()}, root {root_id}, "
                f"family ordinal > {last_ordinal}, even one row at a time. {detail}. Original error: {exc}"
            ) from exc

        if not page:
            break
        page_ordinals = [as_int(row.get("family_ordinal")) for row in page]
        if any(value is None or value <= 0 for value in page_ordinals):
            raise BridgeError(f"Saved parent analytics for root {root_id} contains an invalid family ordinal")
        numeric_ordinals = [int(value) for value in page_ordinals if value is not None]
        next_ordinal = max(numeric_ordinals)
        if next_ordinal <= last_ordinal or len(set(numeric_ordinals)) != len(numeric_ordinals):
            raise BridgeError(f"Parent analytics group pagination did not advance cleanly for root {root_id}")

        for row in page:
            ordinal = as_int(row.get("family_ordinal"))
            try:
                signature_text = decode_hex_text_or_plain(row, "family_signature_hex", "family_signature", "")
                map_text = decode_hex_text_or_plain(row, "family_map_json_hex", "family_map_json", "{}")
                match_text = decode_hex_text_or_plain(row, "match_json_hex", "match_json", "[]")
                outcome_text = decode_hex_text_or_plain(row, "outcome_hex", "outcome", "")
                raw_map = json.loads(map_text or "{}")
                conditions = json.loads(match_text or "[]")
            except (TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError, BridgeError) as exc:
                log.warning("  PARENT CACHE: family ordinal %s has invalid saved JSON: %s", ordinal, str(exc)[:500])
                return [], 0
            if ordinal is None or not isinstance(raw_map, dict) or not isinstance(conditions, list):
                return [], 0

            mapped: Dict[str, int] = {}
            for identity, candle_id in raw_map.items():
                index = id_to_index.get(str(candle_id))
                if index is None:
                    log.warning(
                        "  PARENT CACHE: family ordinal %s references missing candle ID %s; rebuild required",
                        ordinal, candle_id,
                    )
                    return [], 0
                mapped[str(identity)] = index
            if not mapped:
                return [], 0

            signature = family_identity_signature(root_id, timeframe, 0, mapped, candles)
            if signature_text and signature_text != signature:
                log.warning("  PARENT CACHE: family ordinal %s signature no longer matches its map", ordinal)
                return [], 0

            configured = int(as_int(row.get("trade_configured"), 0) or 0)
            family = {
                "familyNumber": ordinal,
                "familyOrdinal": ordinal,
                "map": mapped,
                "conditions": conditions,
                "noTrade": any(
                    isinstance(condition, dict)
                    and condition.get("ifGroupFound") == "identify_but_dont_trade"
                    for condition in conditions
                ),
                "savedTradeConfigured": bool(configured),
                "savedOutcome": outcome_text.strip().lower(),
            }
            apply_setup_group_identity(family, limit)
            families.append(family)
            trade_count += configured

        last_ordinal = next_ordinal
        if target_ordinal is not None and last_ordinal >= target_ordinal:
            break
        if len(page) < page_size:
            break

    families.sort(key=lambda item: int(item["familyNumber"]))
    ordinals = [int(item["familyNumber"]) for item in families]
    if ordinals and ordinals != list(range(1, max(ordinals) + 1)):
        log.warning("  PARENT CACHE: saved family ordinals are not consecutive; a full rebuild is required")
        return [], 0
    if target_ordinal is not None and ordinals and ordinals[-1] != target_ordinal:
        log.warning("  PARENT CACHE: requested through ordinal %s but retrieved through %s", target_ordinal, ordinals[-1])
        return [], 0
    return families, trade_count


def stabilize_parent_family_numbers(root: Dict[str, Any], timeframe: str,
                                     candidates: Sequence[Dict[str, Any]],
                                     candles: Sequence[Dict[str, Any]],
                                     existing: Sequence[Dict[str, Any]],
                                     setup_group_limit: int = DEFAULT_TRADES_SETUP_PER_GROUP) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Preserve every saved parent family and allocate numbers only to new signatures.

    On an append-only candle update, a complete historical parent family remains a
    valid projection anchor. Retain its cached map/conditions directly rather than
    allowing a fresh scan's state cap or enumeration order to renumber/drop it.
    The matcher output is used only to discover additional family signatures.
    """
    old_by_signature: Dict[str, Dict[str, Any]] = {}
    all_families: List[Dict[str, Any]] = []
    for family in existing:
        signature = family_identity_signature(
            int(root["id"]), timeframe, 0, family.get("map") or {}, candles
        )
        if signature in old_by_signature:
            continue
        saved_family = dict(family)
        old_by_signature[signature] = saved_family
        all_families.append(saved_family)

    max_number = max(
        (int(family.get("familyNumber") or 0) for family in all_families),
        default=0,
    )
    new_families: List[Dict[str, Any]] = []
    seen = set(old_by_signature)
    for candidate in candidates:
        signature = family_identity_signature(
            int(root["id"]), timeframe, 0, candidate.get("map") or {}, candles
        )
        if signature in seen:
            continue
        seen.add(signature)
        max_number += 1
        family = dict(candidate)
        family["familyNumber"] = max_number
        family["familyOrdinal"] = max_number
        apply_setup_group_identity(family, setup_group_limit)
        all_families.append(family)
        new_families.append(family)

    all_families.sort(key=lambda item: int(item.get("familyNumber") or 0))
    for family in all_families:
        apply_setup_group_identity(family, setup_group_limit)
    return all_families, new_families


def fetch_existing_confirmation_summary(db: BridgeClient, programme_id: int, user_id: int,
                                         symbol: str, child_tf: str, parent_tf: str,
                                         root_id: int, confirmation_ids: Sequence[int]) -> Dict[int, Dict[str, int]]:
    """Read compact saved-row counts without downloading every cached JSON map.

    On an unchanged lower-timeframe history, the matcher only needs the saved
    count for reporting/checkpoint continuity. Successful cached families do not
    need to be rehydrated because no old parent family is scheduled for matching.
    Keeping this query aggregate-only avoids transferring large match/map JSON.
    """
    ids = sorted({int(value) for value in confirmation_ids if int(value) > 0})
    summaries: Dict[int, Dict[str, int]] = {}
    for confirmation_id in ids:
        rows = db.fetch_all(
            "SELECT COUNT(*) AS `saved_rows`, "
            "COALESCE(SUM(COALESCE(`trade_configured`,0)),0) AS `configured_trades` "
            "FROM `programme_analytics` WHERE `programme_id`=%s AND `userid`=%s "
            "AND `symbol`=%s AND `timeframe`=%s AND `source_timeframe`=%s "
            "AND `root_id`=%s AND `confirmation_root_id`=%s AND `matched_family`=1",
            [programme_id, user_id, symbol, child_tf, parent_tf, root_id, confirmation_id],
            timeout=60,
        )
        row = rows[0] if rows else {}
        summaries[confirmation_id] = {
            "family_count": max(0, as_int(row.get("saved_rows"), 0) or 0),
            "trade_count": max(0, as_int(row.get("configured_trades"), 0) or 0),
        }
    return summaries


def fetch_existing_confirmation_rows(db: BridgeClient, programme_id: int, user_id: int,
                                      symbol: str, child_tf: str, parent_tf: str,
                                      root_id: int, confirmation_ids: Sequence[int],
                                      setup_group_limit: int = DEFAULT_TRADES_SETUP_PER_GROUP) -> List[Dict[str, Any]]:
    """Read confirmation maps in bounded pages using the parent setup's group identity."""
    ids = sorted({int(value) for value in confirmation_ids if int(value) > 0})
    if not ids:
        return []

    columns = {column.lower() for column in get_columns(db, "programme_analytics")}
    has_group_column = "setup_group_id" in columns
    limit = max(1, int(setup_group_limit or DEFAULT_TRADES_SETUP_PER_GROUP))
    if has_group_column:
        ordinal_expr = f"(((GREATEST(1,COALESCE(`setup_group_id`,1))-1)*{limit})+`family_number`)"
        group_expr = "GREATEST(1,COALESCE(`setup_group_id`,1))"
    else:
        ordinal_expr = "`family_number`"
        group_expr = "1"

    selected: List[Dict[str, Any]] = []
    for confirmation_id in ids:
        last_ordinal = 0
        page_size = CONFIRMATION_ANALYTICS_READ_BATCH_SIZE
        while True:
            sql = (
                f"SELECT `family_number`,{group_expr} AS `setup_group_id`,"
                f"{ordinal_expr} AS `family_ordinal`,`confirmation_root_id`,`trade_configured`,"
                "HEX(`outcome`) AS `outcome_hex`,HEX(`family_signature`) AS `family_signature_hex`,"
                "HEX(`family_map_json`) AS `family_map_json_hex`,HEX(`match_json`) AS `match_json_hex` "
                "FROM `programme_analytics` WHERE `programme_id`=%s AND `userid`=%s "
                "AND `symbol`=%s AND `timeframe`=%s AND `source_timeframe`=%s "
                "AND `root_id`=%s AND `confirmation_root_id`=%s AND `matched_family`=1 "
                f"AND {ordinal_expr}>%s ORDER BY {group_expr} ASC,`family_number` ASC "
                f"LIMIT {int(page_size)}"
            )
            params = [programme_id, user_id, symbol, child_tf, parent_tf, root_id, confirmation_id, last_ordinal]
            try:
                page = db.fetch_all(sql, params, timeout=90)
            except BridgeError as exc:
                if page_size > 1:
                    smaller = max(1, page_size // 2)
                    log.warning(
                        "    Confirmation cache read for root %s failed at %s rows; retrying with %s-row pages: %s",
                        confirmation_id, page_size, smaller, str(exc)[:500],
                    )
                    page_size = smaller
                    continue
                raise BridgeError(
                    f"Could not read saved confirmation analytics for confirmation root {confirmation_id}, "
                    f"family ordinal > {last_ordinal}: {exc}"
                ) from exc
            if not page:
                break
            ordinals = [as_int(row.get("family_ordinal")) for row in page]
            if any(value is None or value <= 0 for value in ordinals):
                raise BridgeError(
                    f"Saved confirmation analytics for root {confirmation_id} contains an invalid family ordinal"
                )
            numeric_ordinals = [int(value) for value in ordinals if value is not None]
            next_ordinal = max(numeric_ordinals)
            if next_ordinal <= last_ordinal or len(set(numeric_ordinals)) != len(numeric_ordinals):
                raise BridgeError(
                    f"Confirmation analytics group pagination did not advance for root {confirmation_id}"
                )
            for cached_row in page:
                cached_row["outcome"] = decode_hex_text_or_plain(cached_row, "outcome_hex", "outcome", "")
                cached_row["family_signature"] = decode_hex_text_or_plain(
                    cached_row, "family_signature_hex", "family_signature", ""
                )
                cached_row["family_map_json"] = decode_hex_text_or_plain(
                    cached_row, "family_map_json_hex", "family_map_json", "{}"
                )
                cached_row["match_json"] = decode_hex_text_or_plain(
                    cached_row, "match_json_hex", "match_json", "[]"
                )
                selected.append(cached_row)
            last_ordinal = next_ordinal
            if len(page) < page_size:
                break
    selected.sort(key=lambda row: (
        as_int(row.get("confirmation_root_id"), 0) or 0,
        as_int(row.get("family_ordinal"), 0) or 0,
    ))
    return selected


def rehydrate_saved_confirmation_family(saved_row: Dict[str, Any],
                                       parent_family: Dict[str, Any],
                                       lower_candles: Sequence[Dict[str, Any]],
                                       root_id: int, child_tf: str,
                                       setup_group_limit: int = DEFAULT_TRADES_SETUP_PER_GROUP) -> Dict[str, Any]:
    """Restore one saved child-timeframe family map from candle IDs to indices."""
    try:
        raw_map = json.loads(str(saved_row.get("family_map_json") or "{}"))
        conditions = json.loads(str(saved_row.get("match_json") or "[]"))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("saved confirmation family JSON is invalid") from exc
    if not isinstance(raw_map, dict) or not isinstance(conditions, list):
        raise ValueError("saved confirmation family map/conditions have an unexpected shape")

    id_to_index = {
        str(candle.get("id")): index
        for index, candle in enumerate(lower_candles)
        if candle.get("id") is not None
    }
    mapped: Dict[str, int] = {}
    for identity, candle_id in raw_map.items():
        index = id_to_index.get(str(candle_id))
        if index is None:
            raise ValueError("a saved confirmation candle is no longer present in the lower-timeframe data")
        mapped[str(identity)] = index
    if not mapped:
        raise ValueError("saved confirmation family has an empty candle map")

    family_ordinal = as_int(saved_row.get("family_ordinal"))
    if family_ordinal is None:
        family_ordinal = family_ordinal_from_group(
            saved_row.get("family_number"), saved_row.get("setup_group_id"), setup_group_limit
        )
    confirmation_id = as_int(saved_row.get("confirmation_root_id"))
    if family_ordinal is None or confirmation_id is None:
        raise ValueError("saved confirmation family is missing its family or confirmation identity")
    group_id, group_family_number = split_family_ordinal(family_ordinal, setup_group_limit)
    signature = family_identity_signature(root_id, child_tf, confirmation_id, mapped, lower_candles)
    saved_signature = str(saved_row.get("family_signature") or "")
    if saved_signature and saved_signature != signature:
        raise ValueError("saved confirmation family signature no longer matches its candle map")

    no_trade = bool(parent_family.get("noTrade")) or any(
        isinstance(condition, dict) and condition.get("ifGroupFound") == "identify_but_dont_trade"
        for condition in conditions
    )
    restored_family = {
        "familyNumber": family_ordinal,
        "familyOrdinal": family_ordinal,
        "setupGroupId": group_id,
        "groupFamilyNumber": group_family_number,
        "_setupGroupLimit": max(1, int(setup_group_limit)),
        "map": mapped,
        "conditions": conditions,
        "parentFamily": parent_family,
        "confirmationRootId": confirmation_id,
        "noTrade": no_trade,
    }
    return restored_family


def refresh_open_confirmation_trade_records(
    saved_rows: Sequence[Dict[str, Any]],
    root: Dict[str, Any],
    confirmation_roots: Sequence[Dict[str, Any]],
    parent_families: Sequence[Dict[str, Any]],
    parent_candles: Sequence[Dict[str, Any]],
    lower_candles: Sequence[Dict[str, Any]],
    parent_tf: str,
    child_tf: str,
    symbol: str,
    programme_id: int,
    user_id: int,
    projection: Sequence[Dict[str, int]],
    provider_rr: Optional[float],
    provider_rr_type: str,
    trade_identified_at: Optional[str] = None,
    setup_group_limit: int = DEFAULT_TRADES_SETUP_PER_GROUP,
    account_rules: Optional[Dict[str, Any]] = None,
) -> Dict[int, List[Dict[str, Any]]]:
    """Refresh open/pending trade outcomes without re-running confirmation matching.

    A confirmed family is a stable match when only later candles are appended, but
    its historical trade outcome can move from pending/in-trade to closed. Rebuild
    just those trade calculations using their saved child family maps and the full
    parent family projection anchor. Successful confirmation matcher calls remain
    skipped.
    """
    if str(root.get("tradeFor") or "root") != "root_confirmation":
        return {}

    roots_by_id = {int(cr.get("id") or 0): cr for cr in confirmation_roots}
    parents_by_number = {
        int(family.get("familyNumber") or 0): family
        for family in parent_families
        if int(family.get("familyNumber") or 0) > 0
    }
    refreshed: Dict[int, List[Dict[str, Any]]] = {}
    for saved_row in saved_rows:
        if as_int(saved_row.get("trade_configured"), 0) != 1:
            continue
        outcome = str(saved_row.get("outcome") or "").strip().lower()
        confirmation_id = as_int(saved_row.get("confirmation_root_id"), 0) or 0
        cr = roots_by_id.get(confirmation_id)
        if not cr or not cr.get("useForTrades"):
            continue
        # A later lower-timeframe candle can mature open trades. If any trade
        # target is inherited from the parent timeframe, a changing projection
        # window can also alter its mapped target level, so refresh that trade
        # even when its prior outcome was already closed.
        uses_parent_target = any(
            str(cr.get(field) or "").strip().lower().startswith("parent|")
            for field in ("entryFrom", "stoplossFrom", "takeProfitFrom")
        )
        if outcome not in {"in trade", "pending entry", "matched family", ""} and not uses_parent_target:
            continue

        family_ordinal = as_int(saved_row.get("family_ordinal"), 0) or 0
        parent_family = parents_by_number.get(family_ordinal)
        if parent_family is None:
            raise ValueError("an open confirmation trade has no matching cached parent family")
        child_family = rehydrate_saved_confirmation_family(
            saved_row, parent_family, lower_candles, int(root.get("id") or 0), child_tf,
            setup_group_limit,
        )
        confirmation_name = _root_display_name(cr, True)
        trade_root = dict(cr)
        trade_root["parentTradeRoot"] = root
        trade_root["tradeFor"] = "root"
        trade_root["useForTrades"] = True

        refreshed_trade = make_trade_record(
            root, trade_root, child_family, lower_candles, child_tf, symbol,
            programme_id, user_id, confirmation_id, confirmation_name,
            parent_candles, parent_tf, projection,
            provider_rr=provider_rr, provider_rr_type=provider_rr_type,
            source_timeframe=parent_tf, trade_identified_at=trade_identified_at,
            setup_group_limit=setup_group_limit, account_rules=account_rules,
        )
        refreshed_record = refreshed_trade or match_row(
            programme_id, user_id, symbol, child_tf, root, child_family,
            lower_candles, confirmation_id, confirmation_name,
            source_timeframe=parent_tf,
        )
        refreshed_record["live_trades"] = 0
        refreshed.setdefault(confirmation_id, []).append(refreshed_record)
    return refreshed



def select_confirmation_families_to_process(
    parent_families: Sequence[Dict[str, Any]],
    progress_status: Dict[str, Any],
    existing_confirmation_by_family: Dict[int, Any],
    force_full: bool = False,
) -> Tuple[List[Dict[str, Any]], str]:
    """Select safe confirmation work without discarding parent projection anchors.

    A successful previous confirmation is stable when only later lower-timeframe
    candles are appended: any newly found match completes later and cannot replace
    the earlier successful winner. Previously unsuccessful anchors are retried on
    lower-timeframe growth because forward/any-first searches may now find a candle.
    """
    if force_full or not progress_status.get("valid"):
        return list(parent_families), "full confirmation pass"
    old_highwater = int(progress_status.get("last_family_number") or 0)
    if progress_status.get("appended"):
        selected = [
            family for family in parent_families
            if int(family.get("familyNumber") or 0) > old_highwater
            or int(family.get("familyNumber") or 0) not in existing_confirmation_by_family
        ]
        return selected, "new lower-timeframe candles; retrying previous misses and new parent families"
    selected = [
        family for family in parent_families
        if int(family.get("familyNumber") or 0) > old_highwater
    ]
    return selected, "unchanged lower-timeframe history; processing only new parent families"

def persist_training_progress(db: BridgeClient, rows: Sequence[Dict[str, Any]]) -> None:
    """Save checkpoints only after every analytics pair has persisted successfully."""
    if not rows:
        return
    db.query(
        f"CREATE TABLE IF NOT EXISTS `{PROGRESS_TABLE}` ("
        "`programme_id` BIGINT UNSIGNED NOT NULL,"
        "`userid` BIGINT UNSIGNED NOT NULL,"
        "`symbol` VARCHAR(64) NOT NULL,"
        "`root_id` BIGINT UNSIGNED NOT NULL,"
        "`confirmation_root_id` BIGINT UNSIGNED NOT NULL DEFAULT 0,"
        "`source_timeframe` VARCHAR(16) NOT NULL,"
        "`timeframe` VARCHAR(16) NOT NULL,"
        "`config_hash` CHAR(64) NOT NULL,"
        "`last_candle_id` VARCHAR(64) NULL DEFAULT NULL,"
        "`last_candle_time` DATETIME NULL DEFAULT NULL,"
        "`prefix_hash` CHAR(64) NOT NULL,"
        "`last_parent_family_number` BIGINT UNSIGNED NOT NULL DEFAULT 0,"
        "`processed_family_count` BIGINT UNSIGNED NOT NULL DEFAULT 0,"
        "`saved_match_count` BIGINT UNSIGNED NOT NULL DEFAULT 0,"
        "`updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
        "PRIMARY KEY (`programme_id`,`userid`,`symbol`,`root_id`,`confirmation_root_id`,`source_timeframe`,`timeframe`)"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci",
        timeout=120,
    )
    progress_columns = {column.lower() for column in get_columns(db, PROGRESS_TABLE)}
    if "saved_match_count" not in progress_columns:
        # Additive compatibility migration, deliberately performed only in the
        # final persistence stage after programme_analytics rows have been saved.
        db.execute(
            f"ALTER TABLE `{PROGRESS_TABLE}` ADD COLUMN `saved_match_count` "
            "BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER `processed_family_count`",
            timeout=90,
        )
    columns = [
        "programme_id", "userid", "symbol", "root_id", "confirmation_root_id",
        "source_timeframe", "timeframe", "config_hash", "last_candle_id",
        "last_candle_time", "prefix_hash", "last_parent_family_number",
        "processed_family_count", "saved_match_count",
    ]
    marks = "(" + ",".join(["%s"] * len(columns)) + ")"
    update_columns = columns[7:]
    update_sql = ",".join(f"`{column}`=VALUES(`{column}`)" for column in update_columns)
    for offset in range(0, len(rows), 50):
        chunk = rows[offset:offset + 50]
        params: List[Any] = []
        for row in chunk:
            params.extend(row.get(column) for column in columns)
        values_sql = ",".join(marks for _ in chunk)
        sql = (
            f"INSERT INTO `{PROGRESS_TABLE}` ({','.join(f'`{c}`' for c in columns)}) "
            f"VALUES {values_sql} ON DUPLICATE KEY UPDATE {update_sql}"
        )
        db.query(sql, params, timeout=120)


def print_confirmation_diagnostics(name: str, trace: Dict[str, Any]) -> None:
    matcher = trace.get("matcher") or {}
    reasons = matcher.get("failure_reasons") or {}
    reason_text = "; ".join(
        f"{reason}: {count}"
        for reason, count in sorted(reasons.items(), key=lambda item: (-int(item[1]), str(item[0])))[:4]
    ) or "none"
    blockers = trace.get("setup_blockers") or []
    blocker_text = "; ".join(str(item) for item in blockers) or "none"
    print(f"  CONFIRMATION DIAGNOSTICS — {name}:")
    print(
        f"    Parent families checked: {trace.get('parent_families_checked', 0):,}; "
        f"anchors mapped: {trace.get('parent_family_anchor_mapped', 0):,}; "
        f"projection windows missing: {trace.get('projection_window_missing', 0):,}"
    )
    print(
        f"    Matcher calls: {trace.get('matcher_calls', 0):,}; "
        f"successful: {trace.get('matcher_successes', 0):,}; "
        f"failed: {matcher.get('failed_calls', 0):,}"
    )
    print(
        f"    Price comparisons: {matcher.get('price_comparisons', 0):,}; "
        f"price matches: {matcher.get('price_matches', 0):,}; "
        f"price rejections: {matcher.get('price_rejections', 0):,}"
    )
    print(f"    Main-type rejections: {matcher.get('main_type_rejections', 0):,}; reference-type rejections: {matcher.get('reference_type_rejections', 0):,}")
    print(f"    Main failure reasons: {reason_text}")
    print(f"    Setup blockers: {blocker_text}")

def process_programme(db: BridgeClient, programme: Dict[str, Any]) -> Dict[str, int]:
    """Run the complete PHP-equivalent historical matching chain for one programme."""
    programme_id = int(programme.get("id") or 0)
    user_id = int(programme.get("userid") or 0)
    programme_name = str(programme.get("program_name") or f"Programme {programme_id}")
    advertised = (as_int(programme.get("advertisement"), 0) or 0) == 1
    begin_test = (as_int(programme.get("begin_test_flag"), 0) or 0) == 1
    if (as_int(programme.get("vps_associated"), 0) or 0) != 1:
        log.warning("Programme %s skipped: no confirmed association with this VPS", programme_id)
        return {"pairs": 0, "families": 0, "trades": 0, "candles": 0, "skipped": 1}
    if not (advertised or begin_test):
        log.info("Programme %s skipped: advertisement=0 and begin_test=0", programme_id)
        return {"pairs": 0, "families": 0, "trades": 0, "candles": 0, "skipped": 1}
    live_since = str(programme.get("begin_test_started_at") or "").strip() if begin_test else ""
    live_enabled = bool(begin_test and live_since)
    setup_group_limit = load_trades_setup_per_group(db)
    if table_exists(db, "programme_analytics"):
        ensure_analytics_runtime_schema(db, programme_id, user_id, setup_group_limit)
    else:
        log.warning("programme_analytics is not installed; analytics writes will fail until its schema is installed")
    # programme_trades can contain investor copies and sub-account rows. The
    # deduplicator is intentionally restricted to investor_id IS NULL rows.
    deduplicate_existing_programme_trades(db, programme_id, user_id)
    # One timestamp for the whole programme scan: it marks when this run first
    # identified each configured trade, not the historical candle entry time.
    trade_identified_at = datetime.now().astimezone().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")

    print("\n" + "=" * 82)
    print(f"PROGRAMME: {programme_name} (ID {programme_id})")
    print("=" * 82)
    print(f"Owner user ID: {user_id}")
    print(f"Trade setup group size: {setup_group_limit} setups per group (server_account.trades_setup_per_group)")
    begin_status = str(programme.get("begin_test_status") or "not started").strip().lower()
    begin_started = programme.get("begin_test_started_at") or "not recorded"
    begin_interest_id = programme.get("begin_test_interest_id") or "none"
    print(f"VPS association: {programme.get('vps_role') or 'confirmed'} | VPS IP: {programme.get('vps_ip_address') or 'unknown'}")
    print(f"Advertisement: {'1 (advertised)' if advertised else '0 (not advertised)'}")
    print(
        f"Begin Test: {'1 (active)' if begin_test else '0 (not active)'} "
        f"(interest #{begin_interest_id}, status={begin_status}, started_at={begin_started})"
    )
    print(f"Live signal marking: {'enabled from ' + live_since if live_enabled else 'disabled (History only)'}")

    print("fetching saved programme training configuration..")
    roots, confirmation_roots, parent_rows, confirmation_rows = load_programme_config(
        db, user_id, programme_id
    )
    roots = sorted(roots, key=lambda r: (int(r.get("id") or 0), str(r.get("name") or "").casefold()))

    # The browser uses applyToChart as the root-level execution gate. Confirmation
    # alternatives inherit that parent gate; a disabled parent contributes nothing.
    print("\nROOT EXECUTION STATUS")
    for root in roots:
        enabled = bool(root.get("applyToChart"))
        status = "INCLUDED" if enabled else "SKIPPED"
        reason = "apply to chart is enabled" if enabled else "apply to chart is disabled"
        print(f"  { _root_display_name(root) } (root {root.get('id')}): {status} — {reason}")
    active_roots = [root for root in roots if bool(root.get("applyToChart"))]
    if not active_roots:
        print(f"{programme_name}: no roots have apply_to_chart enabled. Skipping candle fetch and analytics.")
        purge_programme_chart_rows_when_chart_is_empty(
            db, programme_id, user_id, "no active apply_to_chart roots"
        )
        return {"pairs": 0, "families": 0, "trades": 0, "candles": 0}

    print("\nACTIVE ROOT CONFIGURATION")
    print("The sections below show the saved settings used by the matcher.")
    for active_root in active_roots:
        print_tree_configuration(active_root)

    symbols = sorted(set(parse_list(programme.get("selected_symbols"))), key=str.casefold)
    if not symbols:
        print(f"{programme_name}: selected_symbols is empty. No candle pairs can be processed.")
        purge_programme_chart_rows_when_chart_is_empty(
            db, programme_id, user_id, "programme has no selected symbols"
        )
        return {"pairs": 0, "families": 0, "trades": 0, "candles": 0}

    # Build only exact pairs requested by enabled parent roots and their confirmation
    # alternatives. programme.selected_timeframes is deliberately not a source of
    # timeframes here; each saved root/confirmation configuration is authoritative.
    required_pairs: set[Tuple[str, str]] = set()
    for root in active_roots:
        parent_tfs = sorted({str(tf).strip().lower() for tf in (root.get("timeframes") or []) if str(tf).strip()}, key=_timeframe_order_key)
        for symbol in symbols:
            for tf in parent_tfs:
                required_pairs.add((symbol, tf))
            for cr in root.get("confirmationRoots", []) or []:
                for tf in (cr.get("timeframes") or []):
                    child_tf = str(tf).strip().lower()
                    if child_tf:
                        required_pairs.add((symbol, child_tf))

    if not required_pairs:
        print(f"{programme_name}: enabled roots have no configured parent or confirmation timeframes.")
        purge_programme_chart_rows_when_chart_is_empty(
            db, programme_id, user_id, "no configured chart timeframes"
        )
        return {"pairs": 0, "families": 0, "trades": 0, "candles": 0}

    required_pairs_ordered = sorted(
        required_pairs,
        key=lambda pair: (pair[0].casefold(), _timeframe_order_key(pair[1]))
    )
    print_persistent_analytics_summary(db, programme_id, user_id)
    required_timeframes = sorted({tf for _symbol, tf in required_pairs}, key=_timeframe_order_key)
    print("fetching candle records..")
    print(f"Symbols: {', '.join(symbols)}")
    print(f"Configured timeframes only: {', '.join(tf.upper() for tf in required_timeframes)}")

    fetched_candles: Dict[str, Dict[str, List[Dict[str, Any]]]] = {symbol: {} for symbol in symbols}
    total_candles = 0
    for symbol, timeframe in required_pairs_ordered:
        candles = load_candles(db, programme_id, user_id, symbol, timeframe)
        fetched_candles.setdefault(symbol, {})[timeframe] = candles
        total_candles += len(candles)
        diag = candle_sequence_diagnostics(candles)
        print(f"  CANDLE DATA — {symbol} {timeframe.upper()}")
        print(f"    Loaded records: {len(candles):,}")
        print(f"    Historical range: {diag['oldest'] or 'not available'} → {diag['newest'] or 'not available'}")
        sequence_label = "chronological and strictly ascending" if diag["chronological_strict"] else (
            "chronological with repeated times" if diag["chronological_non_decreasing"] else "not in chronological order"
        )
        print(f"    Sequence: {sequence_label}")
        print(
            f"    Integrity checks: duplicate candle IDs={diag['duplicate_ids']:,}; "
            f"duplicate timestamps={diag['duplicate_timestamps']:,}; "
            f"missing OHLC={diag['missing_ohlc_rows']:,}; "
            f"high below low={diag['high_below_low_rows']:,}"
        )
        if diag["valid_timestamps"] != len(candles):
            print(f"    DATA WARNING: {len(candles) - diag['valid_timestamps']} candle(s) have no parseable time/open_time")
        if not diag["chronological_non_decreasing"]:
            print("    DATA WARNING: candle timestamps are not ascending; PHP chart matching receives oldest-to-newest candles")

    current_candle_ids: Dict[Tuple[str, str], set[str]] = {}
    for pair_symbol, pair_timeframe in required_pairs_ordered:
        pair_candles = fetched_candles.get(pair_symbol, {}).get(pair_timeframe, [])
        current_candle_ids[(pair_symbol.casefold(), pair_timeframe.casefold())] = {
            str(candle.get("id")) for candle in pair_candles if candle.get("id") is not None
        }
    print(
        f"\nCHART-WINDOW VALIDATION: matching is restricted to the PHP chart default "
        f"of {CHART_CANDLE_LIMIT:,} candles per symbol/timeframe."
    )
    chart_cleanup = prune_analytics_rows_without_chart_candles(
        db, programme_id, user_id,
        [int(root.get("id") or 0) for root in active_roots],
        required_pairs_ordered,
        current_candle_ids,
    )
    print(
        f"  Stale analytics rows removed: {chart_cleanup['analytics_removed']:,}; "
        f"mapped programme-level trade rows removed: {chart_cleanup['mapped_programme_trades_removed']:,}."
    )
    # Run again after orphan cleanup so only the canonical programme-owned
    # trade row remains. Investor/sub-account rows remain outside the SQL scope.
    deduplicate_existing_programme_trades(db, programme_id, user_id)

    provider = load_provider_risk_reward(db, programme)
    # Read the same programme-scoped Account Management rules used by the PHP chart.
    # They are inputs to historical calculations only; this worker never edits those tables.
    provider["account_management_rules"] = load_account_management_rules(db, programme)
    print("Preparing the internal matching snapshot...")
    snapshot_payload = {
        "schema": "programme_training_runtime_snapshot_v1",
        "programme": {
            "id": programme_id,
            "userid": user_id,
            "program_name": programme_name,
            "selected_symbols": programme.get("selected_symbols"),
            "begin_test_interest_id": programme.get("begin_test_interest_id"),
            "begin_test_status": programme.get("begin_test_status"),
            "begin_test_started_at": programme.get("begin_test_started_at"),
            "begin_test_flag": int(begin_test),
            "advertisement": int(advertised),
            "vps_ip_address": programme.get("vps_ip_address"),
            "vps_role": programme.get("vps_role"),
        },
        "scope": {
            "programme_id": programme_id,
            "user_id": user_id,
            "symbols": symbols,
            "required_timeframes": required_timeframes,
            "required_pairs": [{"symbol": symbol, "timeframe": tf} for symbol, tf in required_pairs_ordered],
            "programme_filter": "VPS-associated AND (programme.advertisement = 1 OR newest signals_provider_interest.begin_test = 1)",
            "live_trades_filter": "configured trade at/after signal_testing_started_at, only when begin_test = 1",
            "root_filter": "apply_to_chart = 1 only",
        },
        "configuration": {
            "roots": roots,
            "confirmation_roots": confirmation_roots,
            "parent_rows": parent_rows,
            "confirmation_rows": confirmation_rows,
            "restoration_engine": "programme_training.php ptRestoreSavedRows() compatible",
        },
        "provider_risk_reward": provider,
        "candles": fetched_candles,
    }

    totals = {"pairs": 0, "families": 0, "trades": 0, "candles": total_candles}
    with tempfile.TemporaryDirectory(prefix=f"programme_training_{programme_id}_") as temp_dir:
        snapshot_path = Path(temp_dir) / "programme_training_runtime.json"
        result_path = Path(temp_dir) / "programme_analytics_result.json"
        _snapshot_write(snapshot_path, snapshot_payload)
        print(f"  Internal matching snapshot prepared ({snapshot_path.stat().st_size:,} bytes)")
        print("  Loading the snapshot into the in-memory matching store...")
        with snapshot_path.open("r", encoding="utf-8") as handle:
            snapshot = json.load(handle)

        # From this point onward all matching inputs come only from the JSON snapshot.
        runtime_roots = snapshot["configuration"]["roots"]
        runtime_roots = [root for root in runtime_roots if bool(root.get("applyToChart"))]
        candle_cache: Dict[str, Dict[str, List[Dict[str, Any]]]] = snapshot["candles"]
        provider_data = snapshot.get("provider_risk_reward") or {}
        account_rules = provider_data.get("account_management_rules") or {"timezone": "UTC"}
        provider_rr = as_float(provider_data.get("risk_reward"))
        provider_rr_type = str(provider_data.get("risk_reward_type") or "")
        records_by_pair: Dict[Tuple[str, str], List[Dict[str, Any]]] = {
            pair: [] for pair in required_pairs_ordered
        }
        local_cache_state, local_cache_path, local_cache_loaded = load_local_family_cache(
            programme_id, user_id
        )
        local_cache_scopes: Dict[str, Any] = local_cache_state.setdefault("scopes", {})
        # The local cache is authoritative for match maps/checkpoints. The DB
        # progress table is retained as a write-only mirror; the worker no longer
        # SELECTs family JSON or raw family rows from programme_analytics.
        progress_index = local_cache_progress_index(local_cache_scopes)
        progress_updates: List[Dict[str, Any]] = []
        local_cache_dirty = False
        if local_cache_loaded:
            print(f"Local matcher cache: loaded {len(local_cache_scopes):,} saved scope(s) from {local_cache_path}")
        else:
            print("Local matcher cache: not found or invalid; this scan will build it after successful analytics persistence")
        pairs_with_matches = set()
        # Every scope below is validated against the exact family signatures
        # produced by this run. Empty sets are meaningful: the chart currently
        # has no matched family in that root/timeframe stream.
        validated_chart_scopes: set[Tuple[str, str, int, int, str]] = set()
        current_family_signatures: Dict[Tuple[str, str, int, int, str], set[str]] = {}

        print("Matching mode: incremental; saved family maps will be reused when candle data is unchanged")
        print("Starting parent-family matching and confirmation projection...")
        for root in runtime_roots:
            root_name = _root_display_name(root)
            parent_tfs = sorted(
                {str(tf).strip().lower() for tf in (root.get("timeframes") or []) if str(tf).strip()},
                key=_timeframe_order_key,
            )
            confirmation_roots_for_parent = root.get("confirmationRoots", []) or []
            trade_for = str(root.get("tradeFor") or "root")
            # A parent-timeframe trade exists only when trade_for=root and the
            # parent root itself has its trade toggle enabled. Confirmation trades
            # are reported separately against each selected confirmation root.
            parent_trade_setup = (trade_for == "root" and bool(root.get("useForTrades")))

            # ROOT MATCHING: process all parent timeframes before any confirmation projection.
            # Every root family is either freshly matched or rehydrated from its saved
            # family_map_json. Rehydrated maps are essential: confirmation projection
            # still receives every parent anchor, including families already processed.
            parent_results: List[Dict[str, Any]] = []
            for symbol in symbols:
                for parent_tf in parent_tfs:
                    parent_candles = candle_cache.get(symbol, {}).get(parent_tf) or []
                    print(f"\n{programme_name} {symbol} {root_name} {parent_tf.upper()}:")
                    root_hash = root_configuration_hash(root, provider_data, setup_group_limit)
                    root_key = (symbol, int(root.get("id") or 0), 0, parent_tf, parent_tf)
                    parent_cache_key = local_parent_scope_key(
                        symbol, int(root.get("id") or 0), parent_tf
                    )
                    local_parent_entry = local_cache_scopes.get(parent_cache_key)
                    stored_root_progress = (
                        local_parent_entry.get("progress")
                        if isinstance(local_parent_entry, dict) else None
                    )
                    root_status = inspect_progress(stored_root_progress, root_hash, parent_candles)
                    if not isinstance(local_parent_entry, dict):
                        root_status.update({"valid": False, "reason": "local parent-family cache not found"})
                    cached_families: List[Dict[str, Any]] = []
                    cached_parent_trades = 0
                    cache_complete = False
                    if root_status["valid"]:
                        try:
                            cached_families, cached_parent_trades = restore_parent_families_from_local_cache(
                                local_parent_entry, parent_candles,
                                int(root.get("id") or 0), parent_tf, setup_group_limit,
                            )
                            cache_complete = (
                                len(cached_families) == int(root_status.get("processed_family_count") or 0)
                                and max((int(f.get("familyNumber") or 0) for f in cached_families), default=0)
                                == int(root_status.get("last_family_number") or 0)
                            )
                            if not cache_complete:
                                log.warning(
                                    "  PARENT CACHE: local family count does not match its checkpoint; "
                                    "a complete parent rebuild is required."
                                )
                        except (ValueError, TypeError, KeyError) as exc:
                            cached_families = []
                            cached_parent_trades = 0
                            cache_complete = False
                            root_status["valid"] = False
                            root_status["reason"] = "local parent-family cache failed validation"
                            log.warning(
                                "  PARENT CACHE: local family cache failed validation; rebuilding from candles. %s",
                                str(exc)[:700],
                            )
                    if not cache_complete and root_status.get("valid"):
                        root_status["valid"] = False
                        root_status["reason"] = "local parent-family cache is incomplete"

                    parent_force_full = not (root_status["valid"] and cache_complete)
                    new_parent_families: List[Dict[str, Any]] = []
                    if not parent_candles:
                        families = []
                        new_parent_families = []
                        print(f"  {root_name}: no {parent_tf.upper()} candles; 0 matched families")
                    elif not parent_force_full and not root_status["appended"]:
                        families = cached_families
                        print(
                            f"  MATCHING CACHE: reused {len(families):,} previously matched families; "
                            "candle history and configuration are unchanged, so the root matcher was skipped"
                        )
                    else:
                        if parent_force_full:
                            print(
                                f"  MATCHING MODE: full rebuild ({root_status.get('reason', 'checkpoint unavailable')}); "
                                "validating the complete parent-family history"
                            )
                        else:
                            print(
                                f"  MATCHING MODE: new candle data detected; rescanning to catch newly completed "
                                "families while preserving existing family numbers"
                            )
                        candidates = match_root(root, parent_candles)
                        if parent_force_full:
                            families = candidates
                            new_parent_families = list(candidates)
                        else:
                            families, new_parent_families = stabilize_parent_family_numbers(
                                root, parent_tf, candidates, parent_candles, cached_families,
                                setup_group_limit,
                            )
                        print_matcher_diagnostics(root, len(parent_candles))
                        final_trace = root.get("_matcherFinal") or {}
                        print(
                            f"  MATCHER RESULT: required trees={final_trace.get('required_trees', 0)}, "
                            f"processed trees={final_trace.get('processed_trees', 0)}, "
                            f"complete families={len(families):,}; "
                            f"new families={len(new_parent_families):,}; "
                            f"blocker={final_trace.get('blocker', 'not recorded')}"
                        )

                    parent_scope = _chart_family_scope_key(
                        symbol, parent_tf, root.get("id"), 0, parent_tf
                    )
                    validated_chart_scopes.add(parent_scope)
                    parent_signatures = current_family_signatures.setdefault(parent_scope, set())
                    for matched_family in families:
                        apply_setup_group_identity(matched_family, setup_group_limit)
                        parent_signatures.add(family_identity_signature(
                            int(root.get("id") or 0), parent_tf, 0,
                            matched_family.get("map") or {}, parent_candles,
                        ))
                    family_count = len(families)
                    totals["families"] += family_count
                    max_family_number = max(
                        (int(family.get("familyNumber") or 0) for family in families), default=0
                    )
                    refresh_existing_parent_trades = (
                        parent_trade_setup and not parent_force_full
                        and bool(root_status.get("appended")) and cache_complete
                    )
                    refreshable_parent_families = [
                        family for family in cached_families
                        if family.get("savedTradeConfigured")
                        and family.get("savedOutcome") in {"in trade", "pending entry", "matched family", ""}
                    ] if refresh_existing_parent_trades else []
                    parent_families_to_write = list(new_parent_families)
                    if refreshable_parent_families:
                        new_signatures = {
                            family_identity_signature(int(root["id"]), parent_tf, 0, family.get("map") or {}, parent_candles)
                            for family in parent_families_to_write
                        }
                        for family in refreshable_parent_families:
                            signature = family_identity_signature(
                                int(root["id"]), parent_tf, 0, family.get("map") or {}, parent_candles
                            )
                            if signature not in new_signatures:
                                parent_families_to_write.append(family)
                                new_signatures.add(signature)

                    refreshed_parent_trade_count = len(refreshable_parent_families)
                    reused_parent_trades = (
                        max(0, cached_parent_trades - refreshed_parent_trade_count)
                        if not parent_force_full else 0
                    )
                    new_trade_count = 0
                    parent_records: List[Dict[str, Any]] = []
                    for family in parent_families_to_write:
                        base = match_row(
                            programme_id, user_id, symbol, parent_tf,
                            root, family, parent_candles,
                            source_timeframe=parent_tf,
                        )
                        trade = None
                        if parent_trade_setup:
                            trade = make_trade_record(
                                root, root, family, parent_candles, parent_tf,
                                symbol, programme_id, user_id,
                                provider_rr=provider_rr,
                                provider_rr_type=provider_rr_type,
                                source_timeframe=parent_tf,
                                trade_identified_at=trade_identified_at,
                                setup_group_limit=setup_group_limit,
                                account_rules=account_rules,
                            )
                        record = trade if trade else base
                        record["live_trades"] = 0
                        parent_records.append(record)
                        if trade:
                            new_trade_count += 1
                    if parent_records:
                        records_by_pair.setdefault((symbol, parent_tf), []).extend(parent_records)
                    if refreshed_parent_trade_count:
                        print(
                            f"  TRADE OUTCOME REFRESH: recalculated {refreshed_parent_trade_count:,} "
                            "previously open/pending parent trades using the newly appended candles"
                        )
                    if family_count:
                        pairs_with_matches.add((symbol, parent_tf))
                    if parent_trade_setup:
                        totals["trades"] += reused_parent_trades + new_trade_count
                        print(
                            f"  TRADE SETUP: enabled for this root; configured trades="
                            f"{reused_parent_trades + new_trade_count:,}"
                        )
                    else:
                        print("  TRADE SETUP: disabled for the parent root; its families remain available to confirmations")
                    print(
                        f"  ROOT FAMILY TOTAL: {family_count:,} matched; "
                        f"{len(families) - len(new_parent_families):,} previously saved; "
                        f"{len(new_parent_families):,} new/changed"
                    )

                    # Update trade-state metadata on each family from rows recalculated
                    # during this scan. Cached, untouched families retain their prior values.
                    parent_records_by_family = {
                        int(record.get("family_ordinal") or 0): record
                        for record in parent_records
                        if int(record.get("family_ordinal") or 0) > 0
                    }
                    for family in families:
                        saved_record = parent_records_by_family.get(int(family.get("familyNumber") or 0))
                        if saved_record is not None:
                            family["savedTradeConfigured"] = bool(as_int(saved_record.get("trade_configured"), 0))
                            family["savedOutcome"] = str(saved_record.get("outcome") or "")

                    parent_progress_row = build_progress_row(
                        programme_id, user_id, symbol, int(root.get("id") or 0), 0,
                        parent_tf, parent_tf, root_hash, parent_candles,
                        max_family_number, family_count,
                    )
                    append_progress_if_changed(progress_index, progress_updates, parent_progress_row)
                    parent_cache_entry = {
                        "scope_type": "parent",
                        "config_hash": root_hash,
                        "progress": parent_progress_row,
                        "families": [
                            serialize_parent_family_for_local_cache(
                                family, parent_candles, int(root.get("id") or 0), parent_tf
                            )
                            for family in families
                        ],
                    }
                    if set_local_cache_scope(local_cache_scopes, parent_cache_key, parent_cache_entry):
                        local_cache_dirty = True

                    parent_results.append({
                        "symbol": symbol,
                        "timeframe": parent_tf,
                        "candles": parent_candles,
                        "families": families,
                        "new_families": new_parent_families,
                        "force_confirmation_full": parent_force_full,
                        "previous_family_highwater": (
                            int(root_status.get("last_family_number") or 0)
                            if root_status["valid"] and cache_complete else 0
                        ),
                    })

            # CONFIRMATION MATCHING: each confirmation alternative is evaluated on
            # its own selected lower timeframe, anchored to each matched parent family.
            if not confirmation_roots_for_parent:
                continue

            for parent_result in parent_results:
                symbol = parent_result["symbol"]
                parent_tf = parent_result["timeframe"]
                parent_candles = parent_result["candles"]
                parent_families = parent_result["families"]
                parent_force_confirmation_full = bool(parent_result["force_confirmation_full"])
                child_timeframes = sorted(
                    {str(tf).strip().lower()
                     for cr in confirmation_roots_for_parent
                     for tf in (cr.get("timeframes") or [])
                     if str(tf).strip()},
                    key=_timeframe_order_key,
                )
                for child_tf in child_timeframes:
                    lower_candles = candle_cache.get(symbol, {}).get(child_tf) or []
                    configured_for_child: List[Tuple[int, Dict[str, Any]]] = [
                        (order, cr) for order, cr in enumerate(confirmation_roots_for_parent)
                        if child_tf in {str(tf).strip().lower() for tf in (cr.get("timeframes") or [])}
                    ]
                    if not configured_for_child:
                        continue

                    for _scope_order, scope_cr in configured_for_child:
                        confirmation_scope = _chart_family_scope_key(
                            symbol, child_tf, root.get("id"), scope_cr.get("id"), parent_tf
                        )
                        validated_chart_scopes.add(confirmation_scope)
                        current_family_signatures.setdefault(confirmation_scope, set())

                    confirmation_ids = [int(cr.get("id") or 0) for _order, cr in configured_for_child]
                    confirmation_hash = confirmation_configuration_hash(
                        root, [cr for _order, cr in configured_for_child], child_tf,
                        provider_data, setup_group_limit,
                    )
                    confirmation_key = (symbol, int(root.get("id") or 0), 0, parent_tf, child_tf)
                    confirmation_cache_key = local_confirmation_scope_key(
                        symbol, int(root.get("id") or 0), parent_tf, child_tf
                    )
                    local_confirmation_entry = local_cache_scopes.get(confirmation_cache_key)
                    stored_confirmation_progress = (
                        local_confirmation_entry.get("progress")
                        if isinstance(local_confirmation_entry, dict) else None
                    )
                    confirmation_status = inspect_progress(
                        stored_confirmation_progress, confirmation_hash, lower_candles
                    )
                    if not isinstance(local_confirmation_entry, dict):
                        confirmation_status.update({
                            "valid": False,
                            "reason": "local confirmation-family cache not found",
                        })
                    existing_confirmation_rows: List[Dict[str, Any]] = []
                    existing_confirmation_counts: Dict[int, Dict[str, int]] = {}
                    existing_confirmation_by_family: Dict[int, List[Dict[str, Any]]] = {}
                    existing_by_confirmation: Dict[int, Dict[int, bool]] = {}
                    saved_confirmation_rows_loaded = False
                    saved_confirmation_total = 0

                    # Reusable match maps now come from the local compressed cache,
                    # never from raw programme_analytics SELECTs. The table remains
                    # the durable dashboard output and is only written at the end.
                    if (not parent_force_confirmation_full
                            and confirmation_status.get("valid")
                            and isinstance(local_confirmation_entry, dict)):
                        try:
                            existing_confirmation_rows = restore_confirmation_rows_from_local_cache(
                                local_confirmation_entry, lower_candles,
                                int(root.get("id") or 0), child_tf, setup_group_limit,
                            )
                            saved_confirmation_rows_loaded = True
                            existing_confirmation_counts = confirmation_cache_summary(existing_confirmation_rows)
                        except (ValueError, TypeError, KeyError) as exc:
                            confirmation_status["valid"] = False
                            confirmation_status["reason"] = "local confirmation cache failed validation"
                            existing_confirmation_rows = []
                            existing_confirmation_counts = {}
                            print(
                                "  CONFIRMATION CACHE WARNING: local cached confirmation maps failed validation; "
                                "all parent families will be re-evaluated. " + str(exc)[:700]
                            )

                    if lower_candles and parent_families and parent_force_confirmation_full:
                        print(
                            "  CONFIRMATION CACHE READ: skipped because a full parent/confirmation "
                            "rebuild is already required"
                        )
                    elif lower_candles and parent_families and not confirmation_status.get("valid"):
                        print(
                            "  CONFIRMATION CACHE READ: no valid local cache; every parent family "
                            "will be re-evaluated"
                        )
                    elif confirmation_status.get("valid"):
                        print(
                            f"  CONFIRMATION CACHE: restored {len(existing_confirmation_rows):,} "
                            "saved match record(s) from the local cache"
                        )

                    for saved_row in existing_confirmation_rows:
                        saved_family_ordinal = as_int(saved_row.get("family_ordinal"))
                        saved_confirmation_id = as_int(saved_row.get("confirmation_root_id"))
                        if saved_family_ordinal is None or saved_confirmation_id is None:
                            continue
                        existing_confirmation_by_family.setdefault(saved_family_ordinal, []).append(saved_row)
                        existing_by_confirmation.setdefault(saved_confirmation_id, {})[saved_family_ordinal] = bool(
                            as_int(saved_row.get("trade_configured"), 0)
                        )

                    saved_confirmation_total = sum(
                        int(item.get("family_count") or 0)
                        for item in existing_confirmation_counts.values()
                    )
                    expected_saved_count = confirmation_status.get("saved_match_count")
                    if confirmation_status.get("valid") and expected_saved_count is not None:
                        actual_saved_count = len(existing_confirmation_rows)
                        if actual_saved_count != int(expected_saved_count):
                            confirmation_status["valid"] = False
                            confirmation_status["reason"] = (
                                "local confirmation cache row count does not match its checkpoint"
                            )
                            existing_confirmation_rows = []
                            existing_confirmation_counts = {}
                            existing_confirmation_by_family = {}
                            existing_by_confirmation = {}
                            saved_confirmation_total = 0
                            saved_confirmation_rows_loaded = False
                            print(
                                "  CONFIRMATION CACHE WARNING: local cache row count differs from its checkpoint; "
                                "performing a full confirmation pass"
                            )

                    families_to_project, confirmation_mode = select_confirmation_families_to_process(
                        parent_families, confirmation_status, existing_confirmation_by_family,
                        force_full=parent_force_confirmation_full,
                    )

                    for _order, cr in configured_for_child:
                        cr_name = _root_display_name(cr, True)
                        print(f"\n{programme_name} {symbol} {root_name} {cr_name} {child_tf.upper()}:")
                        print(f"  {cr_name} projection process on {root_name}")
                        if bool(cr.get("useForTrades")) and trade_for == "root_confirmation":
                            print("  trade is setup for this confirmation")
                        else:
                            print("  trade is not setup for this confirmation")

                    cached_confirmation_family_count = (
                        len(existing_confirmation_by_family)
                        if saved_confirmation_rows_loaded
                        else saved_confirmation_total
                    )
                    print(
                        f"  CONFIRMATION CACHE: {confirmation_mode}; "
                        f"{cached_confirmation_family_count:,} parent families already have a saved confirmation; "
                        f"{len(families_to_project):,} family/families scheduled for projection matching"
                    )

                    # A confirmation family can only exist when its parent root
                    # first produced a complete family to anchor the projection.
                    if not parent_candles or not parent_families:
                        parent_reason = (
                            f"parent matcher produced 0 complete {parent_tf.upper()} families"
                            if parent_candles else f"no {parent_tf.upper()} parent candle records were loaded"
                        )
                        print(
                            f"  CONFIRMATION BLOCKED: {parent_reason}; "
                            f"lower_timeframe_candles={len(lower_candles)}. "
                            "The 1H confirmation matcher is not called until a parent family exists."
                        )
                        for _order, cr in configured_for_child:
                            cr_name = _root_display_name(cr, True)
                            print(
                                f"  {cr_name}: parent_families={len(parent_families)}, "
                                f"anchor_id={cr.get('anchorCandleId')}, "
                                f"anchor_name={cr.get('anchorCandleName')!r}, "
                                f"confirmation_trees={len(cr.get('trees') or [])}; "
                                f"result=0 (blocked before projection/matching)"
                            )
                            if bool(cr.get("useForTrades")) and trade_for == "root_confirmation":
                                print("  Configured trades: 0 (no confirmation families matched)")
                        continue

                    parent_ms = tf_milliseconds(parent_tf)
                    child_ms = tf_milliseconds(child_tf)
                    if parent_ms is None or child_ms is None or parent_ms <= child_ms:
                        for _order, cr in configured_for_child:
                            print(f"  {_root_display_name(cr, True)} has 0 matched families (confirmation timeframe must be lower than {parent_tf.upper()})")
                            if bool(cr.get("useForTrades")) and trade_for == "root_confirmation":
                                print("  Configured trades: 0 (no confirmation families matched)")
                        continue
                    if not lower_candles:
                        for _order, cr in configured_for_child:
                            print(f"  {_root_display_name(cr, True)} has 0 matched families (no {child_tf.upper()} candle records)")
                            if bool(cr.get("useForTrades")) and trade_for == "root_confirmation":
                                print("  Configured trades: 0 (no confirmation families matched)")
                        continue

                    projection = projection_map(parent_candles, lower_candles, parent_tf, child_tf)
                    valid_projection_pieces = sum(
                        1 for piece in projection
                        if piece.get("first", -1) >= 0 and piece.get("last", -1) >= 0
                    )
                    refreshed_confirmation_records: Dict[int, List[Dict[str, Any]]] = {}
                    if (confirmation_status.get("valid") and confirmation_status.get("appended")
                            and not parent_force_confirmation_full):
                        try:
                            refreshed_confirmation_records = refresh_open_confirmation_trade_records(
                                existing_confirmation_rows, root,
                                [cr for _order, cr in configured_for_child],
                                parent_families, parent_candles, lower_candles,
                                parent_tf, child_tf, symbol, programme_id, user_id,
                                projection, provider_rr, provider_rr_type,
                                trade_identified_at=trade_identified_at,
                                setup_group_limit=setup_group_limit,
                                account_rules=account_rules,
                            )
                            refreshed_total = sum(len(items) for items in refreshed_confirmation_records.values())
                            if refreshed_total:
                                print(
                                    f"  TRADE OUTCOME REFRESH: recalculated {refreshed_total:,} "
                                    "previously open/pending confirmation trades; saved family matches were reused"
                                )
                        except ValueError as exc:
                            confirmation_status["valid"] = False
                            confirmation_status["reason"] = str(exc)
                            families_to_project, confirmation_mode = select_confirmation_families_to_process(
                                parent_families, confirmation_status, existing_confirmation_by_family
                            )
                            refreshed_confirmation_records = {}
                            print(
                                "  CONFIRMATION CACHE WARNING: saved trade-family maps could not be restored; "
                                "performing a full confirmation pass instead. " + str(exc)
                            )
                    eligible: List[Tuple[int, Dict[str, Any]]] = []
                    confirmation_trace: Dict[int, Dict[str, Any]] = {}
                    for cr_order, cr in configured_for_child:
                        cr_id = int(cr.get("id") or 0)
                        confirmation_trace[cr_id] = {
                            "parent_families_checked": 0, "parent_family_anchor_mapped": 0,
                            "invalid_parent_indices": 0, "projection_window_missing": 0,
                            "anchor_timestamp_invalid": 0, "lower_anchor_not_found": 0,
                            "matcher_calls": 0, "matcher_successes": 0,
                            "matcher": {}, "setup_blockers": [],
                        }
                        if not cr.get("trees"):
                            confirmation_trace[cr_id]["setup_blockers"].append("no restored confirmation trees")
                            print(f"  {_root_display_name(cr, True)} cannot be projected: no restored confirmation trees")
                            continue
                        if cr.get("anchorCandleId") in (None, "", 0, "0"):
                            confirmation_trace[cr_id]["setup_blockers"].append("confirmation anchor ID is missing/zero")
                            print(f"  {_root_display_name(cr, True)} cannot be projected: missing confirmation anchor ID")
                            continue
                        eligible.append((cr_order, cr))
                        print(
                            f"  PROJECTION SETUP {_root_display_name(cr, True)}: "
                            f"anchor_id={cr.get('anchorCandleId')}, anchor_name={cr.get('anchorCandleName')!r}, "
                            f"anchor_time={cr.get('anchorTime')}, parent_families={len(parent_families)}, "
                            f"projection_pieces={valid_projection_pieces}/{len(projection)}"
                        )

                    candidates_by_parent: Dict[int, List[Tuple[int, int, int, Dict[str, Any], Dict[str, Any]]]] = {}
                    for cr_order, cr in eligible:
                        cr_id = int(cr.get("id") or 0)
                        trace = confirmation_trace[cr_id]
                        anchor_key = str(cr.get("anchorCandleId"))
                        for parent_family in families_to_project:
                            trace["parent_families_checked"] += 1
                            parent_map = parent_family.get("map") or {}
                            if anchor_key not in parent_map:
                                if "anchor identity absent from parent family map" not in trace["setup_blockers"]:
                                    trace["setup_blockers"].append("anchor identity absent from parent family map")
                                continue
                            trace["parent_family_anchor_mapped"] += 1
                            parent_idx = as_int(parent_map.get(anchor_key), -1)
                            if parent_idx is None or not (0 <= parent_idx < len(projection)):
                                trace["invalid_parent_indices"] += 1
                                continue
                            piece = projection[parent_idx]
                            if piece.get("first", -1) < 0 or piece.get("last", -1) < 0:
                                trace["projection_window_missing"] += 1
                                continue

                            parent_candle = parent_candles[parent_idx]
                            if str(cr.get("anchorTime") or "open_time").lower() == "close_time":
                                anchor_stamp = parent_candle.get("close_time") or parent_candle.get("time")
                            else:
                                anchor_stamp = parent_candle.get("open_time") or parent_candle.get("time")
                            anchor_dt = parse_datetime(anchor_stamp)
                            if not anchor_dt:
                                trace["anchor_timestamp_invalid"] += 1
                                continue

                            anchor_idx = -1
                            for lower_idx, lower_candle in enumerate(lower_candles):
                                lower_dt = parse_datetime(lower_candle.get("open_time") or lower_candle.get("time"))
                                if lower_dt and lower_dt >= anchor_dt:
                                    anchor_idx = lower_idx
                                    break
                            if anchor_idx < 0:
                                trace["lower_anchor_not_found"] += 1
                                continue

                            trace["matcher_calls"] += 1
                            one_match_diagnostic: Dict[str, Any] = {}
                            found = match_confirmation_at(
                                cr, anchor_idx, parent_family, lower_candles,
                                diagnostics=one_match_diagnostic,
                            )
                            for key in (
                                "successful_calls", "failed_calls", "main_type_passes", "main_type_rejections",
                                "reference_candidate_checks", "reference_type_matches", "reference_type_rejections",
                                "reference_out_of_bounds", "price_comparisons", "price_matches", "price_rejections",
                                "missing_identity_states",
                            ):
                                trace["matcher"][key] = int(trace["matcher"].get(key, 0)) + int(one_match_diagnostic.get(key, 0))
                            if one_match_diagnostic.get("failure_reasons"):
                                reason_counts = trace["matcher"].setdefault("failure_reasons", {})
                                for reason, count in one_match_diagnostic["failure_reasons"].items():
                                    reason_counts[reason] = int(reason_counts.get(reason, 0)) + int(count)
                            if not found:
                                continue
                            trace["matcher_successes"] += 1
                            chosen = min(
                                found,
                                key=lambda state: (
                                    max(state.get("map", {}).values()) if state.get("map") else anchor_idx,
                                    min(state.get("map", {}).values()) if state.get("map") else anchor_idx,
                                ),
                            )
                            child_family = dict(chosen)
                            child_family.update({
                                "confirmationRootId": cr["id"],
                                "confirmationRoot": cr,
                                "confirmationRootName": _root_display_name(cr, True),
                                "parentFamily": parent_family,
                                "familyNumber": parent_family.get("familyNumber"),
                                "familyOrdinal": parent_family.get("familyOrdinal", parent_family.get("familyNumber")),
                                "setupGroupId": parent_family.get("setupGroupId"),
                                "groupFamilyNumber": parent_family.get("groupFamilyNumber"),
                                "_setupGroupLimit": setup_group_limit,
                                "noTrade": bool(parent_family.get("noTrade")) or any(
                                    condition.get("ifGroupFound") == "identify_but_dont_trade"
                                    for condition in (chosen.get("conditions") or [])
                                ),
                            })
                            indices = [as_int(value) for value in (child_family.get("map") or {}).values()]
                            indices = [value for value in indices if value is not None]
                            occurrence = min(indices) if indices else anchor_idx
                            completion = max(indices) if indices else anchor_idx
                            family_key = int(parent_family.get("familyNumber") or 0)
                            candidates_by_parent.setdefault(family_key, []).append(
                                (completion, occurrence, cr_order, cr, child_family)
                            )

                    for _cr_order, cr in configured_for_child:
                        cr_id = int(cr.get("id") or 0)
                        trace = confirmation_trace.get(cr_id) or {}
                        matcher = trace.get("matcher") or {}
                        print_confirmation_diagnostics(_root_display_name(cr, True), trace)

                    confirmation_records: Dict[int, List[Dict[str, Any]]] = {
                        int(cr.get("id") or 0): [] for _order, cr in configured_for_child
                    }
                    confirmation_trade_counts: Dict[int, int] = {
                        int(cr.get("id") or 0): 0 for _order, cr in configured_for_child
                    }
                    for parent_family in families_to_project:
                        family_key = int(parent_family.get("familyNumber") or 0)
                        alternatives = candidates_by_parent.get(family_key) or []
                        if not alternatives:
                            continue
                        _completion, _occurrence, _cr_order, winning_cr, child_family = min(
                            alternatives, key=lambda item: (item[0], item[1], item[2])
                        )
                        cr_name = _root_display_name(winning_cr, True)
                        base = match_row(
                            programme_id, user_id, symbol, child_tf, root, child_family,
                            lower_candles, int(winning_cr["id"]), cr_name,
                            source_timeframe=parent_tf,
                        )
                        trade = None
                        if trade_for == "root_confirmation" and winning_cr.get("useForTrades"):
                            trade_root = dict(winning_cr)
                            trade_root["parentTradeRoot"] = root
                            trade_root["tradeFor"] = "root"
                            trade_root["useForTrades"] = True
                            trade = make_trade_record(
                                root, trade_root, child_family, lower_candles, child_tf,
                                symbol, programme_id, user_id, int(winning_cr["id"]), cr_name,
                                parent_candles, parent_tf, projection,
                                provider_rr=provider_rr,
                                provider_rr_type=provider_rr_type,
                                source_timeframe=parent_tf,
                                trade_identified_at=trade_identified_at,
                                setup_group_limit=setup_group_limit,
                                account_rules=account_rules,
                            )
                        final_record = trade if trade else base
                        final_record["live_trades"] = 0
                        records_by_pair.setdefault((symbol, child_tf), []).append(final_record)
                        confirmation_records.setdefault(int(winning_cr.get("id") or 0), []).append(final_record)
                        if trade:
                            confirmation_trade_counts[int(winning_cr.get("id") or 0)] = confirmation_trade_counts.get(int(winning_cr.get("id") or 0), 0) + 1

                    for cr_id, refreshed_records in refreshed_confirmation_records.items():
                        if refreshed_records:
                            records_by_pair.setdefault((symbol, child_tf), []).extend(refreshed_records)

                    for _cr_order, cr in configured_for_child:
                        cr_id = int(cr.get("id") or 0)
                        cr_name = _root_display_name(cr, True)
                        cached_by_family = existing_by_confirmation.get(cr_id, {})
                        saved_counts = existing_confirmation_counts.get(
                            cr_id, {"family_count": 0, "trade_count": 0}
                        )
                        new_records_for_cr = confirmation_records.get(cr_id, [])
                        new_family_numbers = {
                            int(record.get("family_ordinal") or 0)
                            for record in new_records_for_cr
                            if int(record.get("family_ordinal") or 0) > 0
                        }
                        full_confirmation_rebuild = parent_force_confirmation_full or not confirmation_status["valid"]
                        if full_confirmation_rebuild:
                            saved_family_count = 0
                            total_family_count = len(new_family_numbers)
                            saved_trade_count = 0
                        else:
                            saved_family_count = (
                                len(cached_by_family) if saved_confirmation_rows_loaded
                                else int(saved_counts.get("family_count") or 0)
                            )
                            total_family_count = saved_family_count + len(new_family_numbers)
                            saved_trade_count = (
                                sum(1 for value in cached_by_family.values() if value)
                                if saved_confirmation_rows_loaded
                                else int(saved_counts.get("trade_count") or 0)
                            )
                        trade_count = saved_trade_count + confirmation_trade_counts.get(cr_id, 0)
                        totals["families"] += total_family_count
                        totals["trades"] += trade_count
                        print(
                            f"  {cr_name}: {total_family_count:,} matched families "
                            f"({saved_family_count:,} saved, {len(new_records_for_cr):,} newly evaluated matches)"
                        )
                        if bool(cr.get("useForTrades")) and trade_for == "root_confirmation":
                            print(f"    Configured trades: {trade_count:,}")
                    if parent_families:
                        pairs_with_matches.update(
                            (symbol, child_tf) for _order, cr in configured_for_child
                            if int((existing_confirmation_counts.get(int(cr.get("id") or 0)) or {}).get("family_count", 0)) > 0
                            or existing_by_confirmation.get(int(cr.get("id") or 0))
                            or confirmation_records.get(int(cr.get("id") or 0))
                        )

                    saved_confirmation_identities = {
                        (
                            int(as_int(row.get("confirmation_root_id"), 0) or 0),
                            int(as_int(row.get("family_ordinal"), 0) or 0),
                        )
                        for row in existing_confirmation_rows
                        if as_int(row.get("confirmation_root_id"), 0) and as_int(row.get("family_ordinal"), 0)
                    }
                    new_confirmation_identities = {
                        (
                            int(as_int(record.get("confirmation_root_id"), 0) or 0),
                            int(as_int(record.get("family_ordinal"), 0) or 0),
                        )
                        for cr_records in confirmation_records.values()
                        for record in cr_records
                        if as_int(record.get("confirmation_root_id"), 0) and as_int(record.get("family_ordinal"), 0)
                    }

                    if parent_force_confirmation_full or not confirmation_status.get("valid"):
                        # A full pass re-evaluates every parent family. The new
                        # matches alone define the rebuilt checkpoint count.
                        saved_confirmation_count = len(new_confirmation_identities)
                    elif (not confirmation_status.get("appended")
                            and not saved_confirmation_rows_loaded):
                        # Existing successful identities are represented by the
                        # last verified checkpoint count. On an unchanged child
                        # timeframe, only higher/new parent families are matched,
                        # so their new identities can be safely added to this count.
                        previous_identity_count = max(
                            0, as_int(confirmation_status.get("saved_match_count"), 0) or 0
                        )
                        saved_confirmation_count = previous_identity_count + len(new_confirmation_identities)
                    else:
                        saved_confirmation_identities.update(new_confirmation_identities)
                        saved_confirmation_count = len(saved_confirmation_identities)

                    confirmation_progress_row = build_progress_row(
                        programme_id, user_id, symbol, int(root.get("id") or 0), 0,
                        parent_tf, child_tf, confirmation_hash, lower_candles,
                        max((int(family.get("familyNumber") or 0) for family in parent_families), default=0),
                        len(parent_families),
                        saved_match_count=saved_confirmation_count,
                    )
                    append_progress_if_changed(progress_index, progress_updates, confirmation_progress_row)

                    # On full rebuild replace the local confirmation scope. On an
                    # append-only pass, merge old successful matches with new matches
                    # and refreshed trade outcomes so no prior parent projection is lost.
                    full_confirmation_rebuild = (
                        parent_force_confirmation_full or not confirmation_status.get("valid")
                    )
                    merged_confirmation_rows: Dict[Tuple[int, int], Dict[str, Any]] = {}
                    if not full_confirmation_rebuild:
                        for saved_row in existing_confirmation_rows:
                            cached_row = confirmation_row_for_local_cache(saved_row, setup_group_limit)
                            identity = (
                                int(cached_row["confirmation_root_id"]),
                                int(cached_row["family_ordinal"]),
                            )
                            merged_confirmation_rows[identity] = cached_row
                    for cr_records in confirmation_records.values():
                        for record in cr_records:
                            cached_row = confirmation_row_for_local_cache(record, setup_group_limit)
                            identity = (
                                int(cached_row["confirmation_root_id"]),
                                int(cached_row["family_ordinal"]),
                            )
                            merged_confirmation_rows[identity] = cached_row
                    for refreshed_records in refreshed_confirmation_records.values():
                        for record in refreshed_records:
                            cached_row = confirmation_row_for_local_cache(record, setup_group_limit)
                            identity = (
                                int(cached_row["confirmation_root_id"]),
                                int(cached_row["family_ordinal"]),
                            )
                            merged_confirmation_rows[identity] = cached_row
                    for cached_row in merged_confirmation_rows.values():
                        confirmation_scope = _chart_family_scope_key(
                            symbol, child_tf, root.get("id"),
                            cached_row.get("confirmation_root_id"), parent_tf,
                        )
                        validated_chart_scopes.add(confirmation_scope)
                        signature = str(cached_row.get("family_signature") or "").strip()
                        if signature:
                            current_family_signatures.setdefault(confirmation_scope, set()).add(signature)
                    confirmation_cache_entry = {
                        "scope_type": "confirmation",
                        "config_hash": confirmation_hash,
                        "progress": confirmation_progress_row,
                        "rows": [
                            merged_confirmation_rows[key]
                            for key in sorted(merged_confirmation_rows)
                        ],
                    }
                    if set_local_cache_scope(local_cache_scopes, confirmation_cache_key, confirmation_cache_entry):
                        local_cache_dirty = True

        # Remove any previously saved Analytics family that this exact chart scan
        # did not reproduce. This catches removed candles AND still-present candles
        # whose old setup no longer matches the active chart configuration. Run it
        # before persistence so an obsolete family cannot be refreshed/reinserted.
        exact_chart_cleanup = prune_analytics_rows_not_in_current_chart(
            db, programme_id, user_id,
            sorted(validated_chart_scopes), current_family_signatures,
        )
        orphaned_trade_rows = delete_orphaned_mapped_programme_trades(db, programme_id, user_id)
        legacy_orphaned_trade_rows = delete_unmapped_orphan_programme_trades(db, programme_id, user_id)
        print(
            f"\nEXACT CHART MATCH VALIDATION: removed {exact_chart_cleanup['analytics_removed']:,} "
            f"non-chart Programme Analytics row(s), "
            f"{exact_chart_cleanup['mapped_programme_trades_removed'] + orphaned_trade_rows + legacy_orphaned_trade_rows:,} "
            "mapped/unmapped programme-owned trade row(s); investor/sub-account rows were untouched."
        )

        # Create a separate result JSON before writing to programme_analytics.
        result_rows = []
        for (symbol, timeframe), records in records_by_pair.items():
            final_records = _deduplicate_records(records)
            result_rows.append({"symbol": symbol, "timeframe": timeframe, "records": final_records})
        result_payload = {
            "schema": "programme_training_analytics_result_v1",
            "programme_id": programme_id,
            "user_id": user_id,
            "live_trades": int(live_enabled),
            "live_since": live_since or None,
            "trades_setup_per_group": setup_group_limit,
            "results": result_rows,
        }
        _snapshot_write(result_path, result_payload)
        with result_path.open("r", encoding="utf-8") as handle:
            result_from_json = json.load(handle)

        print("\nFINAL STAGE: updating Programme Analytics in bounded database batches")
        print("All matching and confirmation projections are complete; database writes are now the final processing stage.")
        for pair_result in result_from_json.get("results", []):
            symbol = str(pair_result.get("symbol") or "")
            timeframe = str(pair_result.get("timeframe") or "").lower()
            records = pair_result.get("records") or []
            if not records:
                # Still run the schema guard. This ensures trade_time/live_trades
                # are present even on a scan where every family came from cache.
                persist_pair(
                    db, programme_id, user_id, symbol, timeframe, [], setup_group_limit,
                    live_since=live_since, live_enabled=live_enabled,
                )
                print(f"  {symbol} {timeframe.upper()}: no new records to write; prior family results were reused")
                continue
            persistence = persist_pair(
                db, programme_id, user_id, symbol, timeframe, records, setup_group_limit,
                live_since=live_since, live_enabled=live_enabled,
            )
            saved = sum(persistence.values())
            print(
                f"  {symbol} {timeframe.upper()}: {saved:,} new/changed analytics record(s) written "
                f"(batched upserts={persistence.get('upserted', 0):,}, "
                f"compatibility inserts={persistence.get('inserted', 0):,}, "
                f"updated={persistence.get('updated', 0):,}, preserved={persistence.get('preserved', 0):,})"
            )

        if live_enabled:
            mark_existing_live_provision_analytics(
                db, programme_id, user_id, live_since
            )

        print("\nFINAL DEDUPLICATION: Programme Analytics and programme-owned Programme Trades")
        if table_exists(db, "programme_analytics"):
            final_analytics_dedup = deduplicate_existing_analytics_rows(db, programme_id, user_id)
            print(
                f"  Programme Analytics duplicate family rows removed: "
                f"{final_analytics_dedup['family_duplicates_removed']:,}; duplicate configured trades removed: "
                f"{final_analytics_dedup['trade_duplicates_removed']:,}."
            )
        final_programme_trade_duplicates = deduplicate_existing_programme_trades(
            db, programme_id, user_id
        )
        print(f"  programme_trades duplicate programme-owned rows removed: {final_programme_trade_duplicates:,}.")

        # Checkpoints are deliberately saved only after every analytics pair write
        # succeeds. If persistence fails midway, the next run will safely retry.
        print("  Saving incremental processing checkpoints...")
        try:
            persist_training_progress(db, progress_updates)
            print(f"  Database checkpoint rows changed: {len(progress_updates):,} root/timeframe scope(s)")
            if not progress_updates:
                print("  Database checkpoint mirror was unchanged; no rewrite was necessary.")
        except Exception as exc:
            # The local compressed cache is the matcher source of truth. A DB
            # checkpoint mirror failure must not discard successfully persisted
            # analytics or force the worker back onto programme_analytics SELECTs.
            print(
                "  CHECKPOINT MIRROR WARNING: database checkpoint mirror could not be updated; "
                "the local cache will still be saved. " + str(exc)[:900]
            )

        if local_cache_dirty:
            try:
                save_local_family_cache(local_cache_path, local_cache_state)
                compressed_size = local_cache_path.stat().st_size if local_cache_path.exists() else 0
                print(
                    f"  Local matcher cache saved: {len(local_cache_scopes):,} scope(s), "
                    f"{compressed_size:,} compressed bytes ({local_cache_path})"
                )
            except OSError as exc:
                print(
                    "  LOCAL CACHE WARNING: analytics were saved, but the local matcher cache "
                    "could not be written. The next scan will safely rebuild from candles. " + str(exc)[:700]
                )
        else:
            print("  Local matcher cache unchanged; no local cache rewrite was necessary.")

    totals["pairs"] = len(pairs_with_matches)
    print("Temporary runtime and result snapshots cleaned up")
    print(
        f"{programme_name} complete: {totals['pairs']} analytics pair(s), "
        f"{totals['families']} matched family record(s), {totals['trades']} configured trade record(s), "
        f"{totals['candles']:,} candles fetched"
    )
    print(
        f"Analytics live flag: {1 if live_enabled else 0} "
        f"({'Begin-Test signals from ' + live_since if live_enabled else 'History mode'})"
    )
    print(f"Setup grouping: family_number restarts at 1 for each setup_group_id (limit {setup_group_limit})")
    return totals

def main() -> int:
    loop = False
    parser = argparse.ArgumentParser(
        description="Programme Training: VPS-scoped matcher and programme analytics writer"
    )
    parser.add_argument("--once", action="store_true", help="run one complete scan and exit")
    parser.add_argument("--interval", type=int, default=60, help="seconds between complete scans (default: 60)")
    parser.add_argument(
        "--programme-id", type=int, default=None,
        help="optionally limit to one programme; it must still be linked to this VPS and be advertised or Begin-Test active",
    )
    parser.add_argument("--vps-ip", default=None, help="override VPS IP detection (must match programme_vps.programme_vps_ip_address)")
    parser.add_argument("--bridge-host", default=BRIDGE_HOST)
    parser.add_argument("--bridge-port", type=int, default=BRIDGE_PORT)
    args = parser.parse_args()
    if args.once:
        loop = False
    if args.interval < 5:
        parser.error("--interval must be at least 5 seconds")

    print("Programme Training scope: direct VPS owner programmes + active VPS follower programmes")
    print("Execution gate after VPS filtering: advertisement=1 OR newest signals_provider_interest.begin_test=1")
    print("Live analytics gate: configured signals on/after signal_testing_started_at while begin_test=1")
    print("Root filter: apply_to_chart = 1 only; disabled roots and their confirmations are skipped")
    print("Configuration restoration: PHP ptRestoreSavedRows row order, drawing-row skips, strict flags and last-row confirmation settings")
    print(f"Continuous loop: {'enabled' if loop else 'disabled'} (set loop=False in main or use --once for one scan)")
    print(f"Database batch size: {ANALYTICS_BATCH_SIZE} analytics records per write")
    print(f"Chart candle scope: latest {CHART_CANDLE_LIMIT:,} candles per symbol/timeframe (PHP PT_DEFAULT_AMOUNT; pages of {CANDLE_PAGE_SIZE})")
    print("Analytics deduplication: programme/owner/symbol/timeframe/order + identical entry/SL/TP prices; group/root IDs ignored")
    print("programme_trades deduplication: programme-owned investor_id IS NULL rows only; investor/sub-account copies are excluded")
    print("Legacy orphan cleanup: Python-side trade-price comparison; no correlated SQL subqueries or SQL price casts")
    print("Stale chart cleanup: Analytics rows must match current PHP-window candle IDs and exact matcher family signatures")
    print("Inactive/removed roots and obsolete timeframe families are pruned from this programme's Analytics scope")
    print("Family cache: local compressed file; no raw family-row SELECTs from programme_analytics")
    db = BridgeClient(args.bridge_host, args.bridge_port, BRIDGE_OWNER)
    stopping = False

    def stop(_signum: int, _frame: Any) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, stop)

    try:
        while not stopping:
            started = time.monotonic()
            programmes = discover_programmes(db, args.programme_id, args.vps_ip)
            totals = {"programmes": 0, "pairs": 0, "families": 0, "trades": 0, "candles": 0, "skipped": 0}
            if not programmes:
                if args.programme_id is None:
                    print("No VPS-associated programmes are advertised or have Begin Test active; nothing to scan.")
                else:
                    print(f"Programme {args.programme_id} is not eligible on this VPS; nothing to scan.")
            for programme in programmes:
                if stopping:
                    break
                try:
                    result = process_programme(db, programme)
                    totals["programmes"] += 1
                    for key in ("pairs", "families", "trades", "candles", "skipped"):
                        totals[key] += int(result.get(key, 0))
                except Exception:
                    log.error("Programme %s failed:\n%s", programme.get("id"), traceback.format_exc())

            print("\n" + "-" * 82)
            print("PROGRAMME TRAINING SCAN COMPLETE")
            print(f"Programmes processed : {totals['programmes']}")
            print(f"Programmes skipped   : {totals['skipped']}")
            print(f"Analytics pairs      : {totals['pairs']}")
            print(f"Matched families     : {totals['families']}")
            print(f"Trade records        : {totals['trades']}")
            print(f"Candles fetched      : {totals['candles']:,}")
            print("Programme scope      : VPS-associated AND (advertisement=1 OR begin_test=1)")
            print("Live trade scope     : post-test-start configured signals only when begin_test=1")
            print("-" * 82)

            if not loop:
                break
            elapsed = time.monotonic() - started
            sleep_for = max(1, args.interval - int(elapsed))
            for _ in range(sleep_for):
                if stopping:
                    break
                time.sleep(1)
    except BridgeError as exc:
        log.error("Database bridge unavailable: %s", exc)
        return 2
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
