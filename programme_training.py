#!/usr/bin/env python3
"""Python counterpart of programme_training.php's saved-configuration matcher.

Only programmes with a successful Begin Test recorded in
signals_provider_interest.begin_test = 1 are considered. VPS/IP scoping is
intentionally disabled for this stage and can be integrated later. Only roots
with apply_to_chart enabled are processed. The worker fetches the exact configured
symbol/timeframe candle scopes, restores the saved configuration into the same
JS-shaped runtime model, writes both into temporary JSON, reloads that JSON, and
then runs root/tree matching, confirmation projection and historical trade
calculations without chart rendering.

Results are written to programme_analytics with live_trades=0 (History). Trade identification time is stored in trade_time. It never
modifies candle_records, creates broker orders, or writes to programme_trades.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
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
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

BRIDGE_HOST = "127.0.0.1"
BRIDGE_PORT = 5599
BRIDGE_OWNER = "programme_training"
MAX_FRAME = 64 * 1024 * 1024
HEADER = struct.Struct(">I")
MAX_ROOT_MATCH_STATES = 2500
CANDLE_PAGE_SIZE = 2000
ANALYTICS_BATCH_SIZE = 20
PARENT_ANALYTICS_READ_BATCH_SIZE = 20
CONFIRMATION_ANALYTICS_READ_BATCH_SIZE = 20
PROGRESS_TABLE = "programme_training_progress"
MATCHER_CACHE_VERSION = "programme-training-family-cache-v2"

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
    w = normalize_candle_type(wanted)
    return w == DEFAULT_CANDLE_TYPE or candle_type(candle) == w


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
    """Identify a saved reference row, including a newly-created offset.

    PHP deliberately stores reference_id=NULL for a new-offset reference, so
    reference_id alone is NOT a reliable main/reference discriminator. The
    reference name and reference_mode are authoritative in that case.
    """
    return any(has_db_value(row.get(field)) for field in (
        "reference_candle_name", "reference_mode", "ref_candle_type"
    )) or as_int(row.get("reference_id")) is not None


def is_drawing_only_row(row: Dict[str, Any]) -> bool:
    """Return True only for actual drawing rows, not textual SQL NULLs.

    programme_training.php stores drawings in the same table as matcher rows.
    Its normal root/tree rows also have draw_from/draw_to columns, normally NULL.
    The mysqlremote bridge may serialise those NULLs as strings, so a simple
    ``if row.get('draw_from')`` incorrectly drops every matcher row.
    """
    has_drawing = has_db_value(row.get("draw_from")) or has_db_value(row.get("draw_to"))
    if not has_drawing:
        return False
    has_matcher_identity = any(has_db_value(row.get(field)) for field in (
        "main_candle_name", "reference_candle_name", "main_id", "reference_id"
    ))
    return not has_matcher_identity


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
        rid = (as_int(row.get("parent_root_id"), 1) or 1) if is_confirmation else (as_int(row.get("root_id"), 1) or 1)
        confirmation_id = as_int(row.get("confirmation_root_id"), 0) or 0 if is_confirmation else 0
        tree_id = as_int(row.get("tree_id"), 1) or 1
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
        rid = (as_int(row.get("parent_root_id"), 1) or 1) if is_confirmation else (as_int(row.get("root_id"), 1) or 1)
        confirmation_id = (as_int(row.get("confirmation_root_id"), 0) or 0) if is_confirmation else 0
        tree_id = as_int(row.get("tree_id"), 1) or 1
        tree_key = ("confirmation" if is_confirmation else "root", rid, confirmation_id, tree_id)
        allocation = allocations[tree_key]
        row["__php_runtime_main_id"] = allocation["main_id"]
        row["__php_runtime_reference_id"] = allocation["reference_id"]
        row["__is_confirmation"] = is_confirmation


def make_tree(group: List[Dict[str, Any]], tree_index: int) -> Dict[str, Any]:
    # Direct unit-test/legacy callers may bypass BridgeClient.fetch_all(). Apply
    # the same SQL-NULL boundary conversion here so every restoration path gets
    # PDO-equivalent values, not truthy strings such as "NULL".
    group = [normalise_row(row) for row in group]

    # The PHP saver writes two records per tree. The main row has no
    # reference-specific fields. The reference row may have reference_id=NULL
    # for referenceMode='new_offset', so do not identify it using reference_id
    # alone (or by comparing raw NULL representations).
    main_row = next((r for r in group if (
        has_db_value(r.get("main_candle_name")) and not _row_has_reference_details(r)
    )), None)
    ref_row = next((r for r in group if _row_has_reference_details(r)), None)

    # Older rows may not have reference_mode populated. Fall back to explicit
    # reference name/type data on a distinct record.
    if main_row is not None and ref_row is None:
        ref_row = next((r for r in group if r is not main_row and (
            has_db_value(r.get("reference_candle_name")) or
            has_db_value(r.get("ref_candle_type")) or
            as_int(r.get("reference_id")) is not None
        )), None)
    # Permit a legacy denormalised row that contains both candle names.
    if main_row is None and len(group) == 1:
        only = group[0]
        if has_db_value(only.get("main_candle_name")) and has_db_value(only.get("reference_candle_name")):
            main_row = ref_row = only
    if main_row is None:
        main_row = next((r for r in group if has_db_value(r.get("main_candle_name"))), group[0])
    if ref_row is None:
        ref_row = next((r for r in group if r is not main_row and has_db_value(r.get("reference_candle_name"))), main_row)

    # These are the same runtime identities created by ptRestoreSavedRows().
    # Never use main_id as the fallback identity for a NULL new-offset reference.
    main_id = as_int(main_row.get("__php_runtime_main_id"), as_int(main_row.get("main_id"), 0)) or 0
    ref_id = as_int(ref_row.get("__php_runtime_reference_id"), as_int(ref_row.get("reference_id"), 0)) or 0
    offset_to = as_int(main_row.get("offset_to"))
    selected_main = offset_to
    ref_mode_value = normalise_db_value(ref_row.get("reference_mode"))
    # Match programme_training.php's savedIsNewOffset expression: a populated
    # reference_mode wins; otherwise both offset_candle and offset_to are tested
    # with JavaScript truthiness (not Python's treatment of string "0").
    if js_truthy_db(ref_mode_value):
        ref_mode = str(ref_mode_value).lower()
    else:
        ref_mode = (
            "new_offset"
            if js_truthy_db(ref_row.get("offset_candle")) and js_truthy_db(ref_row.get("offset_to"))
            else "existing"
        )
    is_new_ref = ref_mode == "new_offset"
    selected_ref = None if is_new_ref else as_int(ref_row.get("reference_id"))
    region_mode_value = main_row.get("region_search_mode")
    region_mode = str(region_mode_value if has_db_value(region_mode_value) else "").strip().lower()
    # Match programme_training.php ptRestoreSavedRows() exactly. The browser
    # does not decide this from region_search_mode='just_offset_existing_candles'
    # alone: for later trees it selects new_group when the saved new-group anchor
    # is JS-truthy OR the main row's offset_to is JS-falsy. Existing-candles-window
    # is the one mode selected directly by region_search_mode.
    if tree_index > 0 and region_mode == "existing_candles_perimeter":
        group_type = "existing_candles_window"
    elif tree_index > 0 and (
        js_truthy_db(main_row.get("main_new_group_offset_to"))
        or not js_truthy_db(main_row.get("offset_to"))
    ):
        group_type = "new_group"
    else:
        group_type = "existing_group"
    main = {
        "id": main_id, "name": str(main_row.get("main_candle_name") or ""),
        "candleText": str(main_row.get("candle_text") or ""),
        "textPosition": main_row.get("text_position") or "high",
        "textColor": main_row.get("text_color") or "purple",
        "mainCandleType": normalize_candle_type(main_row.get("main_candle_type") or DEFAULT_CANDLE_TYPE),
        "position": main_row.get("position") or ("left_and_right" if tree_index == 0 else "left"),
        "offsetTo": offset_to, "offsetToName": str(main_row.get("offset_candle_name") or ""),
        "offsetToKind": "", "offsetCandle": main_row.get("offset_candle") or "",
        "selectedExistingId": selected_main,
        "offsetPosition": as_int(main_row.get("offset_position")),
        "offsetPosPoint": main_row.get("offset_position_point") or "all",
        "newGroupOffsetTo": as_int(main_row.get("main_new_group_offset_to")),
        "newGroupOffsetToName": "",
        "newGroupOffsetPosPoint": main_row.get("main_new_group_offset_pos_point") or "all",
        "newGroupDirection": main_row.get("main_new_group_direction") or "right",
        "priceLevel": main_row.get("price_level") or "high",
        "operator": main_row.get("operator") or "<",
    }
    ref = {
        "id": ref_id, "name": str(ref_row.get("reference_candle_name") or ""),
        "refCandleType": normalize_candle_type(
            ref_row.get("ref_candle_type") or ref_row.get("offset_candle") or DEFAULT_CANDLE_TYPE
        ),
        "position": "right" if str(ref_row.get("position") or "left") == "right" else "left",
        "offsetTo": as_int(ref_row.get("offset_to")),
        "offsetToName": str(ref_row.get("offset_candle_name") or ""),
        # PHP uses `row.offset_position != null ? parseInt(...) : 1`, so an
        # explicitly saved numeric zero must remain zero rather than being
        # replaced by the default through Python's `or` operator.
        "offsetPosition": as_int(ref_row.get("offset_position"), 1),
        "offsetPosPoint": ref_row.get("offset_position_point") or "all",
        "priceLevel": ref_row.get("price_level") or "high",
        "candleText": str(ref_row.get("candle_text") or ""),
        "textPosition": ref_row.get("text_position") or "high",
        "textColor": ref_row.get("text_color") or "purple",
        "referenceMode": ref_mode,
        "selectedExistingId": selected_ref,
        "existingCandleId": selected_ref,
    }
    return {
        "id": as_int(main_row.get("tree_id"), 1) or 1,
        "groupSelectionType": group_type,
        "regionFromCandleId": as_int(main_row.get("region_from_candle_id")),
        "regionToCandleId": as_int(main_row.get("region_to_candle_id")),
        "groupNecessity": main_row.get("group_necessity") or "required",
        "ifGroupFound": main_row.get("if_group_found") or "identify_and_trade",
        "main": main, "reference": ref,
        "offset": ref if is_new_ref else None,
    }


def group_rows(rows: List[Dict[str, Any]], confirmation: bool = False) -> List[Dict[str, Any]]:
    root_field = "confirmation_root_id" if confirmation else "root_id"
    groups: Dict[int, List[Dict[str, Any]]] = {}
    skipped_drawings = 0
    missing_root = 0
    for row in rows:
        if is_drawing_only_row(row):
            skipped_drawings += 1
            continue
        rid = as_int(row.get(root_field))
        if rid is None:
            missing_root += 1
            continue
        groups.setdefault(rid, []).append(row)

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
            "missing_%s=%s, drawing_only=%s. Sample rows: %s",
            confirmation, len(rows), root_field, missing_root, skipped_drawings,
            " | ".join(sample) if sample else "none"
        )

    roots: List[Dict[str, Any]] = []
    for rid, root_rows in sorted(groups.items()):
        root_rows = sorted(root_rows, key=lambda r: (as_int(r.get("tree_id"), 0) or 0,
                                                     as_int(r.get("config_id"), 0) or 0))
        tree_ids = sorted({as_int(r.get("tree_id"), 1) or 1 for r in root_rows})
        trees: List[Dict[str, Any]] = []
        for idx, tid in enumerate(tree_ids):
            tree_rows = [r for r in root_rows if (as_int(r.get("tree_id"), 1) or 1) == tid]
            tree = make_tree(tree_rows, idx)
            if tree["main"]["name"] and tree["reference"]["name"]:
                trees.append(tree)
            else:
                log.warning(
                    "Configuration root %s tree %s has incomplete main/reference names: "
                    "main=%r reference=%r row_count=%s",
                    rid, tid, tree["main"].get("name"), tree["reference"].get("name"), len(tree_rows)
                )
        template = root_rows[0]
        timeframe_value = (template.get("confirmation_timeframes") or template.get("timeframe")) if confirmation else template.get("timeframe")
        normalized_timeframes = [x.lower() for x in parse_list(timeframe_value)]
        root = {
            "id": rid,
            "name": str(template.get("root_name") or (("Confirmation Root " if confirmation else "Root ") + str(rid))),
            "timeframe": ",".join(normalized_timeframes),
            "timeframes": normalized_timeframes,
            "expanded": False,
            "trees": trees,
            "enabled": True,
            "applyToChart": truthy(template.get("apply_to_chart"), True) if not is_null_db_value(template.get("apply_to_chart")) else truthy(template.get("run_in_python"), True),
            "useForTrades": truthy(template.get("use_for_trade")),
            "tradeFor": str(template.get("trade_for") or "root").lower(),
            "entryFrom": str(template.get("entry_at") or template.get("entry_from") or ""),
            "orderType": str(template.get("order_type") or template.get("entry_order_type") or "buy"),
            "entryPriceLevel": str(template.get("entry_price_level") or "high"),
            "stoplossFrom": str(template.get("stoploss_at") or template.get("stoploss_from") or ""),
            "stoplossPriceLevel": str(template.get("stoploss_price_level") or "high"),
            "takeProfitFrom": str(template.get("take_profit_at") or template.get("take_profit_from") or ""),
            "takeProfitType": "risk_reward" if str(template.get("take_profit_type") or "risk_reward").lower().replace(" ", "_") != "price_levels" else "price_levels",
            "takeProfitPriceLevel": str(template.get("take_profit_price_level") or "high"),
            "riskReward": as_float(template.get("risk_reward")),
            "anchorCandleId": as_int(template.get("confirmation_anchor_candle_id")),
            "anchorCandleName": str(template.get("confirmation_anchor_candle_name") or ""),
            "anchorTime": str(template.get("confirmation_anchor_time") or "open_time"),
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
        rows_for_cr = [r for r in confirmation_rows if as_int(r.get("confirmation_root_id")) == cr_id]
        cr["parentRootId"] = as_int(rows_for_cr[0].get("parent_root_id")) if rows_for_cr else None
        cr["isConfirmationRoot"] = True
        cr["drawings"] = []
        by_parent.setdefault(cr["parentRootId"] or 0, []).append(cr)
    for root in parent_roots:
        root["confirmationRoots"] = by_parent.get(root["id"], [])
        root["confirmationRoot"] = (root["confirmationRoots"][0] if root["confirmationRoots"] else None)
        root["drawings"] = []
        # Match ptRestoreSavedRows()'s backward-compatible move of legacy
        # root-level trade fields to the first confirmation alternative.
        if (str(root.get("tradeFor") or "root").lower() == "root_confirmation"
                and bool(root.get("useForTrades")) and root["confirmationRoots"]
                and not any(bool(cr.get("useForTrades")) for cr in root["confirmationRoots"])):
            first_cr = root["confirmationRoots"][0]
            for field in ("entryFrom", "orderType", "entryPriceLevel", "stoplossFrom",
                          "stoplossPriceLevel", "takeProfitFrom", "takeProfitType",
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
        if not (has_db_value(draw_from) or has_db_value(draw_to)):
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
    if normalized == "open": return first
    if normalized == "close": return last
    best, extreme = first, (float("inf") if normalized == "low" else -float("inf"))
    for idx in range(first, last + 1):
        c = lower_candles[idx]
        value = as_float(c.get("low")) if normalized == "low" else as_float(c.get("high")) if normalized == "high" else price(c, normalized)
        if value is None: continue
        if (normalized == "low" and value < extreme) or (normalized != "low" and value > extreme):
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
                      trade_identified_at: Optional[str] = None) -> Optional[Dict[str, Any]]:
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
    outcome, sl_trigger, tp_trigger = evaluate_outcome(candles, trigger_idx, sl_price, tp_price, order)
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

    signature = family_identity_signature(
        int(parent_root["id"]), timeframe, confirmation_root_id,
        family.get("map") or {}, candles,
    )
    family_map = {
        str(identity): candles[int(index)].get("id")
        for identity, index in (family.get("map") or {}).items()
        if as_int(index) is not None and 0 <= int(index) < len(candles)
    }

    return {
        "programme_id": programme_id,
        "userid": user_id,
        "symbol": symbol,
        "timeframe": timeframe.lower(),
        "source_timeframe": (source_timeframe or timeframe).lower(),
        "root_id": int(parent_root["id"]),
        "root_name": parent_root.get("name") or f"Root {parent_root['id']}",
        "confirmation_root_id": int(confirmation_root_id),
        "confirmation_root_name": confirmation_name or None,
        "family_number": int(family.get("familyNumber") or 0),
        "family_signature": signature,
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
        "closed_trade": int(outcome in {"stoploss hit", "take profit reached"}),
        "trade_value_r": -1.0 if outcome == "stoploss hit" else (max(0.0, family_rr or 0.0) if outcome == "take profit reached" else 0.0),
        "effective_take_profit_type": effective_tp_type,
    }

def match_row(programme_id: int, user_id: int, symbol: str, timeframe: str,
              root: Dict[str, Any], family: Dict[str, Any], candles: Sequence[Dict[str, Any]],
              confirmation_root_id: int = 0, confirmation_name: str = "",
              source_timeframe: Optional[str] = None) -> Dict[str, Any]:
    return {
        "programme_id": programme_id, "userid": user_id, "symbol": symbol,
        "timeframe": timeframe.lower(), "source_timeframe": (source_timeframe or timeframe).lower(),
        "root_id": int(root["id"]), "root_name": root.get("name") or f"Root {root['id']}",
        "confirmation_root_id": int(confirmation_root_id), "confirmation_root_name": confirmation_name or None,
        "family_number": int(family.get("familyNumber") or 0),
        "family_signature": family_identity_signature(int(root["id"]), timeframe, confirmation_root_id, family.get("map") or {}, candles),
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
    }


# ------------------------------- Begin Test scope -------------------------------
def discover_programmes(db: BridgeClient,
                       programme_id: Optional[int] = None) -> List[Dict[str, Any]]:
    """Return programmes whose Signals Provision has actually begun testing.

    signals_provision_request.php::spc_startProvisionTest() is the source of
    truth: a successful Begin Test writes begin_test=1, interest_status='ongoing'
    and signal_testing_started_at. The PHP deliberately leaves begin_test=1
    when a test later passes or fails; cancelling an interest resets it to 0.

    Therefore this worker includes every programme with a persisted
    begin_test=1 record (ongoing, failed/breached or passed). It does not use
    programme_vps, follower bindings, or this computer's IP. A programme-id
    argument only narrows this already Begin-Test-qualified set.
    """
    if not table_exists(db, "signals_provider_interest"):
        raise RuntimeError(
            "signals_provider_interest table does not exist; the worker cannot "
            "verify which programmes have begun their provision test."
        )

    sql = (
        "SELECT p.*, "
        "spi.id AS begin_test_interest_id, "
        "spi.begin_test AS begin_test_flag, "
        "spi.interest_status AS begin_test_status, "
        "spi.signal_testing_started_at AS begin_test_started_at "
        "FROM `programme` p "
        "INNER JOIN `signals_provider_interest` spi "
        "  ON spi.programme_id = p.id AND spi.user_id = p.userid "
        "WHERE COALESCE(spi.begin_test, 0) = 1 "
        "ORDER BY p.id ASC, spi.id DESC"
    )
    rows = db.fetch_all(sql, timeout=120)

    # A programme may have more than one provision-interest row. Retain the
    # newest row that actually began a test, matching the PHP's latest-interest
    # ordering while ensuring one Python processing pass per programme.
    programmes_by_id: Dict[int, Dict[str, Any]] = {}
    for row in rows:
        pid = as_int(row.get("id"))
        if pid is None or pid <= 0:
            continue
        programmes_by_id.setdefault(pid, row)

    programmes = [programmes_by_id[pid] for pid in sorted(programmes_by_id)]
    if programme_id is not None:
        programmes = [
            row for row in programmes
            if as_int(row.get("id")) == int(programme_id)
        ]
        if not programmes:
            log.warning(
                "Programme %s has no matching signals_provider_interest row "
                "with begin_test=1; skipped",
                programme_id,
            )

    log.info(
        "Begin Test scope: %s started interest record(s), %s unique programme(s)%s",
        len(rows), len(programmes),
        f" (programme filter {programme_id})" if programme_id is not None else "",
    )
    return programmes


# ------------------------------- DB operations --------------------------------
ANALYTICS_COLUMNS = [
    "programme_id", "userid", "symbol", "timeframe", "source_timeframe", "root_id", "root_name",
    "confirmation_root_id", "confirmation_root_name", "family_number", "family_signature", "family_map_json",
    "match_json", "matched_family", "trade_configured", "live_trades", "order_type", "direction", "entry_name", "stoploss_name",
    "take_profit_name", "entry_anchor_time", "entry_trigger_time", "stoploss_time", "take_profit_time",
    "stoploss_trigger_time", "take_profit_trigger_time", "event_time", "trade_time", "entry_price", "stoploss_price",
    "take_profit_price", "configured_take_profit_price", "risk_reward", "configured_risk_reward",
    "minimum_risk_reward_applied", "outcome", "stoploss_hit", "profit_reached", "pending_entry", "in_trade",
    "closed_trade", "trade_value_r",
]


def load_candles(db: BridgeClient, programme_id: int, user_id: int,
                 symbol: str, timeframe: str) -> List[Dict[str, Any]]:
    """Fetch every stored candle for one exact pair, in PHP chart order.

    programme_training.php fetches newest-first pages of at most 2,000 rows,
    reverses each page, and prepends older pages. This performs the same
    programme/user/symbol/timeframe scoping and pagination. The completed list
    is chronological, oldest candle first, exactly as PT_CANDLES is for matching.
    """
    pages_desc: List[List[Dict[str, Any]]] = []
    before: Optional[str] = None
    page_number = 0
    while True:
        page_number += 1
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
            f" ORDER BY `candle_time` DESC LIMIT {CANDLE_PAGE_SIZE}"
        )
        rows = db.fetch_all(sql, params, timeout=120)
        if not rows:
            break
        pages_desc.append(rows)
        oldest = rows[-1].get("candle_time")
        if oldest is None or str(oldest).strip() == "":
            log.warning("    %s %s: pagination stopped because the oldest candle has no candle_time", symbol, timeframe.upper())
            break
        if len(rows) < CANDLE_PAGE_SIZE:
            break
        if before is not None and str(oldest) >= str(before):
            log.warning("    %s %s: pagination made no progress at %s", symbol, timeframe.upper(), oldest)
            break
        before = str(oldest)

    # Pages were collected newest-to-oldest and rows within each page are DESC.
    chronological_rows: List[Dict[str, Any]] = []
    for page in reversed(pages_desc):
        chronological_rows.extend(reversed(page))

    candles: List[Dict[str, Any]] = []
    for row in chronological_rows:
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

def _analytics_identity(record: Dict[str, Any]) -> Dict[str, Any]:
    """Return the exact logical key defined by programme_analytics_schema.sql.

    `userid` is stored on each row and checked as an ownership/scope value, but
    it is deliberately NOT part of the table's UNIQUE family index. The supplied
    schema defines uq_programme_analytics_family as:
      programme_id, symbol, timeframe, source_timeframe, root_id,
      confirmation_root_id, family_number.
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
        "family_number": record.get("family_number"),
    }


# This list intentionally matches uq_programme_analytics_family in the supplied
# programme_analytics_schema.sql. Do not add userid unless the SQL unique index
# is changed at the same time.
ANALYTICS_IDENTITY_COLUMNS = [
    "programme_id", "symbol", "timeframe", "source_timeframe", "root_id",
    "confirmation_root_id", "family_number",
]


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


def _insert_or_refresh_row(
    db: BridgeClient,
    table: str,
    record: Dict[str, Any],
    columns: Sequence[str],
    identity_columns: Sequence[str],
    has_unique_identity: bool,
    label: str,
) -> str:
    """Write one analytics identity using the table's real unique key first.

    The current programme_analytics schema has an exact UNIQUE family index.
    Prefer one atomic INSERT ... ON DUPLICATE KEY UPDATE statement: it both
    inserts new families and refreshes existing ones without a preliminary
    SELECT, which is important because the user's mysqlremote channel currently
    fails on that SELECT form.

    `userid` is inserted for the PHP ownership filter but is not in the schema's
    unique key and must not be overwritten by a duplicate-key refresh. If a
    deployment lacks the expected unique index or rejects the upsert syntax, the
    legacy SELECT/update/insert fallback remains available and never deletes rows.
    """
    values = [record.get(column) for column in columns]
    identity = {column: record.get(column) for column in identity_columns}
    where_sql, where_values = _identity_where(identity, identity_columns)
    insert_columns_sql = ",".join(f"`{column}`" for column in columns)
    insert_marks = "(" + ",".join(["%s"] * len(columns)) + ")"

    # Keep the database ownership field immutable on duplicate-key refresh.
    protected_columns = {column.lower() for column in identity_columns} | {"userid"}
    update_columns = [column for column in columns if column.lower() not in protected_columns]

    if has_unique_identity:
        update_expressions = []
        for column in update_columns:
            if column.lower() == "trade_time":
                # Keep the original discovery timestamp when a cached trade is
                # refreshed later as its pending/running/closed outcome matures.
                update_expressions.append("`trade_time`=COALESCE(`trade_time`,VALUES(`trade_time`))")
            else:
                update_expressions.append(f"`{column}`=VALUES(`{column}`)")
        update_sql = ",".join(update_expressions)
        if not update_sql:
            # Defensive only: the analytics row always has mutable outcome/data columns.
            raise RuntimeError("No mutable programme_analytics columns available for upsert")
        upsert_sql = (
            f"INSERT INTO `{table}` ({insert_columns_sql}) VALUES {insert_marks} "
            f"ON DUPLICATE KEY UPDATE {update_sql}"
        )
        try:
            db.query(upsert_sql, values, timeout=120)
            return "upserted"
        except BridgeError as exc:
            log.warning(
                "    %s: atomic analytics upsert was rejected; attempting compatibility fallback. %s",
                label, str(exc)[:1400],
            )

    # Compatibility fallback for installations whose unique index differs from
    # programme_analytics_schema.sql or whose bridge cannot execute upserts.
    # Add userid as an independent ownership filter, not as part of the unique key.
    scope_where = where_sql + " AND `userid`=%s"
    scope_values = where_values + [record.get("userid")]
    existing = db.fetch_all(
        f"SELECT 1 AS `found` FROM `{table}` WHERE {scope_where} LIMIT 1",
        scope_values, timeout=60,
    )
    if existing:
        set_parts: List[str] = []
        update_values: List[Any] = []
        for column in update_columns:
            if column.lower() == "trade_time":
                set_parts.append("`trade_time`=COALESCE(`trade_time`,%s)")
            else:
                set_parts.append(f"`{column}`=%s")
            update_values.append(record.get(column))
        set_sql = ",".join(set_parts)
        if not set_sql:
            return "preserved"
        update_values.extend(scope_values)
        try:
            db.execute(
                f"UPDATE `{table}` SET {set_sql} WHERE {scope_where}",
                update_values, timeout=120,
            )
            return "updated"
        except BridgeError as exc:
            log.warning(
                "    %s: bridge does not permit refreshing this existing row; preserving history: %s",
                label, str(exc)[:1200],
            )
            return "preserved"

    db.query(
        f"INSERT INTO `{table}` ({insert_columns_sql}) VALUES {insert_marks}",
        values, timeout=120,
    )
    return "inserted"

def _batch_upsert_analytics(db: BridgeClient, records: Sequence[Dict[str, Any]],
                            columns: Sequence[str]) -> None:
    """Write a small batch atomically using the programme analytics family key."""
    if not records:
        return
    insert_columns = ",".join(f"`{column}`" for column in columns)
    one_row = "(" + ",".join(["%s"] * len(columns)) + ")"
    values_sql = ",".join(one_row for _ in records)
    params: List[Any] = []
    for record in records:
        params.extend(record.get(column) for column in columns)
    protected = {column.lower() for column in ANALYTICS_IDENTITY_COLUMNS} | {"userid"}
    update_columns = [column for column in columns if column.lower() not in protected]
    update_expressions = []
    for column in update_columns:
        if column.lower() == "trade_time":
            update_expressions.append("`trade_time`=COALESCE(`trade_time`,VALUES(`trade_time`))")
        else:
            update_expressions.append(f"`{column}`=VALUES(`{column}`)")
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


def persist_pair(db: BridgeClient, programme_id: int, user_id: int, symbol: str, timeframe: str,
                 records: List[Dict[str, Any]]) -> Dict[str, int]:
    """Persist only the new/changed family rows, in bounded multi-row batches."""
    if not table_exists(db, "programme_analytics"):
        raise RuntimeError("Required table programme_analytics does not exist; create the analytics schema first.")

    columns = get_columns(db, "programme_analytics")
    lower_columns = {str(column).lower() for column in columns}
    if "live_trades" not in lower_columns:
        position = " AFTER `trade_configured`" if "trade_configured" in lower_columns else ""
        db.execute(
            "ALTER TABLE `programme_analytics` ADD COLUMN `live_trades` TINYINT(1) NOT NULL DEFAULT 0" + position,
            timeout=90,
        )
        columns = get_columns(db, "programme_analytics")
        lower_columns = {str(column).lower() for column in columns}

    if "trade_time" not in lower_columns:
        position = " AFTER `event_time`" if "event_time" in lower_columns else ""
        db.execute(
            "ALTER TABLE `programme_analytics` ADD COLUMN `trade_time` DATETIME NULL DEFAULT NULL" + position,
            timeout=90,
        )
        columns = get_columns(db, "programme_analytics")
        lower_columns = {str(column).lower() for column in columns}

    missing = [column for column in ANALYTICS_COLUMNS if column.lower() not in lower_columns]
    if missing:
        raise RuntimeError("programme_analytics is missing required column(s): " + ", ".join(missing))

    records = _deduplicate_records(records)
    for record in records:
        record["live_trades"] = 0

    counters = {"inserted": 0, "upserted": 0, "updated": 0, "preserved": 0}
    if not records:
        print(f"  {symbol} {timeframe.upper()}: no new analytics rows; saved families were reused and left unchanged")
        return counters

    unique_identity = _has_exact_unique_identity(db, "programme_analytics", ANALYTICS_IDENTITY_COLUMNS)
    if not unique_identity:
        log.warning(
            "    Expected analytics unique index was not detected; using compatibility row writes."
        )

    total = len(records)
    completed = 0
    label = f"{symbol} {timeframe.upper()} database update"
    for offset in range(0, total, ANALYTICS_BATCH_SIZE):
        chunk = records[offset:offset + ANALYTICS_BATCH_SIZE]
        if unique_identity:
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
                f"{symbol} {timeframe.upper()} family {record.get('family_number')}",
            )
            counters[status] = counters.get(status, 0) + 1
            completed += 1
            _print_write_progress(label, completed, total)

    print()
    return counters

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


def _deduplicate_records(records: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deduplicated: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
    for record in records:
        identity = (
            record.get("root_id"), record.get("confirmation_root_id"),
            record.get("source_timeframe"), record.get("family_number"),
        )
        deduplicated[identity] = record
    return list(deduplicated.values())



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
    return {
        "risk_reward": as_float(provider_data.get("risk_reward")),
        "risk_reward_type": str(provider_data.get("risk_reward_type") or "").strip().lower(),
        "interest_status": str(provider_data.get("interest_status") or "").strip().lower(),
    }


def root_configuration_hash(root: Dict[str, Any],
                            provider_data: Optional[Dict[str, Any]] = None) -> str:
    return _sha256_payload({
        "matcher_cache_version": MATCHER_CACHE_VERSION,
        "root_configuration": canonical_configuration([root]),
        "provider_trade_configuration": _provider_trade_configuration(provider_data),
    })


def confirmation_configuration_hash(root: Dict[str, Any], confirmations: Sequence[Dict[str, Any]],
                                    child_tf: str,
                                    provider_data: Optional[Dict[str, Any]] = None) -> str:
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
        "parent_configuration": canonical_configuration([root]),
        "child_timeframe": child_tf.lower(),
        "confirmation_alternatives": configured,
        "provider_trade_configuration": _provider_trade_configuration(provider_data),
    })


def fetch_existing_parent_families(db: BridgeClient, programme_id: int, user_id: int,
                                   symbol: str, timeframe: str, root_id: int,
                                   candles: Sequence[Dict[str, Any]],
                                   max_family_number: Optional[int] = None) -> Tuple[List[Dict[str, Any]], int]:
    """Rehydrate saved parent families using bounded, keyset-paginated reads.

    The previous implementation selected every family_map_json and match_json
    record in one mysqlremote response. A root with 1,000+ families can make that
    response too large for the hosting PHP bridge to execute or serialise.

    This version reads at most PARENT_ANALYTICS_READ_BATCH_SIZE rows per request,
    advancing by family_number. If a page fails, it retries that page with smaller
    limits (20, 10, 5, 2, then 1). A one-row failure is raised to the caller, which
    invalidates the cache and performs a complete parent match. Parent maps are
    never silently omitted: all successfully restored families are returned only
    after the full requested range has been read and validated.

    family_map_json stores candle_records IDs, not array positions. They are mapped
    back to the current candle array, preserving the exact parent anchors required
    by confirmation projection.
    """
    target_family_number = (
        max(0, int(max_family_number)) if max_family_number is not None else None
    )
    if target_family_number == 0:
        return [], 0

    id_to_index = {
        str(candle.get("id")): index
        for index, candle in enumerate(candles)
        if candle.get("id") is not None
    }
    families: List[Dict[str, Any]] = []
    trade_count = 0
    last_family_number = 0
    page_size = PARENT_ANALYTICS_READ_BATCH_SIZE

    while True:
        upper_limit_sql = " AND `family_number`<=%s" if target_family_number is not None else ""
        sql = (
            "SELECT `family_number`,HEX(`family_signature`) AS `family_signature_hex`,"
            "HEX(`family_map_json`) AS `family_map_json_hex`,"
            "HEX(`match_json`) AS `match_json_hex`,`trade_configured`,"
            "HEX(`outcome`) AS `outcome_hex` "
            "FROM `programme_analytics` WHERE `programme_id`=%s AND `userid`=%s "
            "AND `symbol`=%s AND `timeframe`=%s AND `source_timeframe`=%s "
            "AND `root_id`=%s AND `confirmation_root_id`=0 AND `matched_family`=1 "
            "AND `family_number`>%s"
            + upper_limit_sql
            + " ORDER BY `family_number` ASC "
            + f"LIMIT {int(page_size)}"
        )
        params: List[Any] = [
            programme_id, user_id, symbol, timeframe, timeframe, root_id,
            last_family_number,
        ]
        if target_family_number is not None:
            params.append(target_family_number)

        try:
            page = db.fetch_all(sql, params, timeout=90)
        except BridgeError as exc:
            if page_size > 1:
                smaller_page_size = max(1, page_size // 2)
                log.warning(
                    "  PARENT CACHE: read failed at %s rows per request; "
                    "retrying with %s-row pages (root %s, next family after %s).",
                    page_size, smaller_page_size, root_id, last_family_number,
                )
                page_size = smaller_page_size
                continue
            probe_sql = (
                "SELECT `family_number` FROM `programme_analytics` "
                "WHERE `programme_id`=%s AND `userid`=%s AND `symbol`=%s "
                "AND `timeframe`=%s AND `source_timeframe`=%s AND `root_id`=%s "
                "AND `confirmation_root_id`=0 AND `matched_family`=1 "
                "AND `family_number`>%s "
                + ("AND `family_number`<=%s " if target_family_number is not None else "")
                + "ORDER BY `family_number` ASC LIMIT 1"
            )
            probe_params = [programme_id, user_id, symbol, timeframe, timeframe, root_id, last_family_number]
            if target_family_number is not None:
                probe_params.append(target_family_number)
            try:
                scalar_probe = db.fetch_all(probe_sql, probe_params, timeout=45)
                probe_result = (
                    "scalar family_number lookup succeeded; the bridge failure is likely in "
                    "the selected analytics text columns or their serialization"
                    if scalar_probe else
                    "scalar family_number lookup returned no matching row; verify the exact programme/user/symbol/timeframe scope"
                )
            except BridgeError as probe_exc:
                probe_result = "scalar family_number lookup also failed: " + str(probe_exc)[:500]
            raise BridgeError(
                "Could not read saved parent analytics for "
                f"{symbol} {timeframe.upper()}, root {root_id}, "
                f"family_number > {last_family_number}, even one row at a time. "
                f"Diagnostic: {probe_result}. Original error: {exc}"
            ) from exc

        if not page:
            break

        page_numbers = [as_int(row.get("family_number")) for row in page]
        if any(number is None for number in page_numbers):
            raise BridgeError(
                f"Saved parent analytics for root {root_id} contains a row without family_number"
            )
        numeric_page_numbers = [int(number) for number in page_numbers if number is not None]
        next_family_number = max(numeric_page_numbers)
        if next_family_number <= last_family_number:
            raise BridgeError(
                f"Parent analytics pagination did not advance for root {root_id} "
                f"after family {last_family_number}"
            )
        if len(set(numeric_page_numbers)) != len(numeric_page_numbers):
            raise BridgeError(
                f"Parent analytics page for root {root_id} contains duplicate family numbers"
            )

        for row in page:
            family_number = as_int(row.get("family_number"))
            try:
                family_signature_text = decode_hex_text_or_plain(
                    row, "family_signature_hex", "family_signature", ""
                )
                family_map_text = decode_hex_text_or_plain(
                    row, "family_map_json_hex", "family_map_json", "{}"
                )
                match_text = decode_hex_text_or_plain(
                    row, "match_json_hex", "match_json", "[]"
                )
                outcome_text = decode_hex_text_or_plain(
                    row, "outcome_hex", "outcome", ""
                )
                raw_map = json.loads(family_map_text or "{}")
                conditions = json.loads(match_text or "[]")
            except (TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError, BridgeError):
                log.warning(
                    "  PARENT CACHE: family %s has invalid saved map/condition JSON; "
                    "a full parent rebuild will be required.", family_number,
                )
                return [], 0
            if family_number is None or not isinstance(raw_map, dict) or not isinstance(conditions, list):
                log.warning(
                    "  PARENT CACHE: family %s has an invalid saved data shape; "
                    "a full parent rebuild will be required.", family_number,
                )
                return [], 0

            mapped: Dict[str, int] = {}
            for identity, candle_id in raw_map.items():
                index = id_to_index.get(str(candle_id))
                if index is None:
                    log.warning(
                        "  PARENT CACHE: family %s references candle ID %s which is absent "
                        "from the loaded history; a full parent rebuild will be required.",
                        family_number, candle_id,
                    )
                    return [], 0
                mapped[str(identity)] = index
            if not mapped:
                log.warning(
                    "  PARENT CACHE: family %s has an empty candle map; "
                    "a full parent rebuild will be required.", family_number,
                )
                return [], 0

            signature = family_identity_signature(root_id, timeframe, 0, mapped, candles)
            saved_signature = family_signature_text
            if saved_signature and saved_signature != signature:
                log.warning(
                    "  PARENT CACHE: family %s signature no longer matches its candle map; "
                    "a full parent rebuild will be required.", family_number,
                )
                return [], 0

            configured = int(as_int(row.get("trade_configured"), 0) or 0)
            families.append({
                "familyNumber": family_number,
                "map": mapped,
                "conditions": conditions,
                "noTrade": any(
                    isinstance(condition, dict)
                    and condition.get("ifGroupFound") == "identify_but_dont_trade"
                    for condition in conditions
                ),
                "savedTradeConfigured": bool(configured),
                "savedOutcome": outcome_text.strip().lower(),
            })
            trade_count += configured

        last_family_number = next_family_number
        if target_family_number is not None and last_family_number >= target_family_number:
            break
        if len(page) < page_size:
            break

    families.sort(key=lambda item: int(item["familyNumber"]))
    numbers = [int(item["familyNumber"]) for item in families]
    if numbers and numbers != list(range(1, max(numbers) + 1)):
        log.warning(
            "  PARENT CACHE: saved family numbers are not consecutive (received %s families, "
            "highest number %s); a full parent rebuild will be required.",
            len(numbers), max(numbers),
        )
        return [], 0
    if target_family_number is not None and numbers and numbers[-1] != target_family_number:
        log.warning(
            "  PARENT CACHE: requested families through %s but retrieved only through %s; "
            "a full parent rebuild will be required.",
            target_family_number, numbers[-1],
        )
        return [], 0
    return families, trade_count


def stabilize_parent_family_numbers(root: Dict[str, Any], timeframe: str,
                                     candidates: Sequence[Dict[str, Any]],
                                     candles: Sequence[Dict[str, Any]],
                                     existing: Sequence[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
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
        all_families.append(family)
        new_families.append(family)

    all_families.sort(key=lambda item: int(item.get("familyNumber") or 0))
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
                                      root_id: int, confirmation_ids: Sequence[int]) -> List[Dict[str, Any]]:
    """Read confirmation cache maps in bounded, keyset-paginated batches.

    The former one-shot SELECT used ``confirmation_root_id IN (...)`` and tried
    to return every JSON map/condition in a single mysqlremote response. Some
    hosting bridge executions return a generic partial failure for that large
    result. This version queries one confirmation root at a time and pages by
    family_number. A failed page is retried with progressively smaller batches.
    """
    ids = sorted({int(value) for value in confirmation_ids if int(value) > 0})
    if not ids:
        return []

    selected: List[Dict[str, Any]] = []
    for confirmation_id in ids:
        last_family_number = -1
        page_size = CONFIRMATION_ANALYTICS_READ_BATCH_SIZE
        while True:
            sql = (
                "SELECT `family_number`,`confirmation_root_id`,`trade_configured`,"
                "HEX(`outcome`) AS `outcome_hex`,HEX(`family_signature`) AS `family_signature_hex`,"
                "HEX(`family_map_json`) AS `family_map_json_hex`,"
                "HEX(`match_json`) AS `match_json_hex` "
                "FROM `programme_analytics` WHERE `programme_id`=%s AND `userid`=%s "
                "AND `symbol`=%s AND `timeframe`=%s AND `source_timeframe`=%s "
                "AND `root_id`=%s AND `confirmation_root_id`=%s AND `matched_family`=1 "
                "AND `family_number`>%s ORDER BY `family_number` ASC "
                f"LIMIT {int(page_size)}"
            )
            params = [
                programme_id, user_id, symbol, child_tf, parent_tf, root_id,
                confirmation_id, last_family_number,
            ]
            try:
                page = db.fetch_all(sql, params, timeout=90)
            except BridgeError as exc:
                if page_size > 1:
                    smaller = max(1, page_size // 2)
                    log.warning(
                        "    Confirmation cache read for root %s failed at %s rows; "
                        "retrying with %s-row pages: %s",
                        confirmation_id, page_size, smaller, str(exc)[:500],
                    )
                    page_size = smaller
                    continue
                raise BridgeError(
                    f"Could not read saved confirmation analytics for confirmation root "
                    f"{confirmation_id}, family_number > {last_family_number}, even one row at a time: {exc}"
                ) from exc

            if not page:
                break

            page_numbers = [as_int(row.get("family_number")) for row in page]
            if any(number is None for number in page_numbers):
                raise BridgeError(
                    f"Saved confirmation analytics for root {confirmation_id} contains a row without family_number"
                )
            next_family_number = max(int(number) for number in page_numbers if number is not None)
            if next_family_number <= last_family_number:
                raise BridgeError(
                    f"Confirmation analytics pagination did not advance for root {confirmation_id} "
                    f"after family {last_family_number}"
                )

            for cached_row in page:
                cached_row["outcome"] = decode_hex_text_or_plain(
                    cached_row, "outcome_hex", "outcome", ""
                )
                cached_row["family_signature"] = decode_hex_text_or_plain(
                    cached_row, "family_signature_hex", "family_signature", ""
                )
                cached_row["family_map_json"] = decode_hex_text_or_plain(
                    cached_row, "family_map_json_hex", "family_map_json", "{}"
                )
                cached_row["match_json"] = decode_hex_text_or_plain(
                    cached_row, "match_json_hex", "match_json", "[]"
                )
            selected.extend(page)
            last_family_number = next_family_number
            if len(page) < page_size:
                break

    selected.sort(key=lambda row: (
        as_int(row.get("confirmation_root_id"), 0) or 0,
        as_int(row.get("family_number"), 0) or 0,
    ))
    return selected



def rehydrate_saved_confirmation_family(saved_row: Dict[str, Any],
                                       parent_family: Dict[str, Any],
                                       lower_candles: Sequence[Dict[str, Any]],
                                       root_id: int, child_tf: str) -> Dict[str, Any]:
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

    family_number = as_int(saved_row.get("family_number"))
    confirmation_id = as_int(saved_row.get("confirmation_root_id"))
    if family_number is None or confirmation_id is None:
        raise ValueError("saved confirmation family is missing its family or confirmation identity")
    signature = family_identity_signature(root_id, child_tf, confirmation_id, mapped, lower_candles)
    saved_signature = str(saved_row.get("family_signature") or "")
    if saved_signature and saved_signature != signature:
        raise ValueError("saved confirmation family signature no longer matches its candle map")

    no_trade = bool(parent_family.get("noTrade")) or any(
        isinstance(condition, dict) and condition.get("ifGroupFound") == "identify_but_dont_trade"
        for condition in conditions
    )
    return {
        "familyNumber": family_number,
        "map": mapped,
        "conditions": conditions,
        "parentFamily": parent_family,
        "confirmationRootId": confirmation_id,
        "noTrade": no_trade,
    }


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
) -> Dict[int, List[Dict[str, Any]]]:
    """Refresh open/pending trade outcomes without re-running confirmation matching.

    A confirmed family is a stable match when only later candles are appended, but
    its historical trade outcome can move from pending/in-trade to closed. Rebuild
    just those trade calculations using their saved child family maps and the full
    parent family projection anchor. Successful confirmation matcher calls remain
    skipped.
    """
    if str(root.get("tradeFor") or "root").lower() != "root_confirmation":
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

        family_number = as_int(saved_row.get("family_number"), 0) or 0
        parent_family = parents_by_number.get(family_number)
        if parent_family is None:
            raise ValueError("an open confirmation trade has no matching cached parent family")
        child_family = rehydrate_saved_confirmation_family(
            saved_row, parent_family, lower_candles, int(root.get("id") or 0), child_tf
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
    # One timestamp for the whole programme scan: it marks when this run first
    # identified each configured trade, not the historical candle entry time.
    trade_identified_at = datetime.now().astimezone().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")

    print("\n" + "=" * 82)
    print(f"PROGRAMME: {programme_name} (ID {programme_id})")
    print("=" * 82)
    print(f"Owner user ID: {user_id}")
    begin_status = str(programme.get("begin_test_status") or "unknown").strip().lower()
    begin_started = programme.get("begin_test_started_at") or "not recorded"
    begin_interest_id = programme.get("begin_test_interest_id") or "unknown"
    print(
        f"{programme_name} Begin Test = yes "
        f"(interest #{begin_interest_id}, status={begin_status}, started_at={begin_started})"
    )

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
        return {"pairs": 0, "families": 0, "trades": 0, "candles": 0}

    print("\nACTIVE ROOT CONFIGURATION")
    print("The sections below show the saved settings used by the matcher.")
    for active_root in active_roots:
        print_tree_configuration(active_root)

    symbols = sorted(set(parse_list(programme.get("selected_symbols"))), key=str.casefold)
    if not symbols:
        print(f"{programme_name}: selected_symbols is empty. No candle pairs can be processed.")
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

    provider = load_provider_risk_reward(db, programme)
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
        },
        "scope": {
            "programme_id": programme_id,
            "user_id": user_id,
            "symbols": symbols,
            "required_timeframes": required_timeframes,
            "required_pairs": [{"symbol": symbol, "timeframe": tf} for symbol, tf in required_pairs_ordered],
            "programme_filter": "signals_provider_interest.begin_test = 1",
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
        provider_rr = as_float(provider_data.get("risk_reward"))
        provider_rr_type = str(provider_data.get("risk_reward_type") or "")
        records_by_pair: Dict[Tuple[str, str], List[Dict[str, Any]]] = {
            pair: [] for pair in required_pairs_ordered
        }
        progress_index = load_training_progress(db, programme_id, user_id)
        progress_updates: List[Dict[str, Any]] = []
        pairs_with_matches = set()

        print("Matching mode: incremental; saved family maps will be reused when candle data is unchanged")
        print("Starting parent-family matching and confirmation projection...")
        for root in runtime_roots:
            root_name = _root_display_name(root)
            parent_tfs = sorted(
                {str(tf).strip().lower() for tf in (root.get("timeframes") or []) if str(tf).strip()},
                key=_timeframe_order_key,
            )
            confirmation_roots_for_parent = root.get("confirmationRoots", []) or []
            trade_for = str(root.get("tradeFor") or "root").strip().lower()
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
                    root_hash = root_configuration_hash(root, provider_data)
                    root_key = (symbol, int(root.get("id") or 0), 0, parent_tf, parent_tf)
                    stored_root_progress = progress_index.get(root_key)
                    root_status = inspect_progress(stored_root_progress, root_hash, parent_candles)
                    cached_families: List[Dict[str, Any]] = []
                    cached_parent_trades = 0
                    cache_complete = False
                    if root_status["valid"]:
                        try:
                            cached_families, cached_parent_trades = fetch_existing_parent_families(
                                db, programme_id, user_id, symbol, parent_tf,
                                int(root.get("id") or 0), parent_candles,
                                max_family_number=int(root_status.get("last_family_number") or 0),
                            )
                            cache_complete = (
                                len(cached_families) == int(root_status.get("processed_family_count") or 0)
                                and (max((int(f.get("familyNumber") or 0) for f in cached_families), default=0)
                                     == int(root_status.get("last_family_number") or 0))
                            )
                            if not cache_complete:
                                log.warning(
                                    "  PARENT CACHE: retrieved family count does not match the checkpoint; "
                                    "the parent matcher will perform a full rebuild."
                                )
                        except BridgeError as exc:
                            # Do not terminate the programme because a cached JSON read
                            # failed. A full match reconstructs the parent families from
                            # the loaded candles, keeping every resulting parent anchor
                            # available to the confirmation projection stage.
                            cached_families = []
                            cached_parent_trades = 0
                            cache_complete = False
                            log.warning(
                                "  PARENT CACHE: database read failed after page-size retries. "
                                "Invalidating this cache and rebuilding the parent families "
                                "from candle history. Details: %s",
                                str(exc)[:900],
                            )

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
                                root, parent_tf, candidates, parent_candles, cached_families
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

                    append_progress_if_changed(
                        progress_index, progress_updates,
                        build_progress_row(
                            programme_id, user_id, symbol, int(root.get("id") or 0), 0,
                            parent_tf, parent_tf, root_hash, parent_candles,
                            max_family_number, family_count,
                        ),
                    )
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

                    confirmation_ids = [int(cr.get("id") or 0) for _order, cr in configured_for_child]
                    confirmation_hash = confirmation_configuration_hash(
                        root, [cr for _order, cr in configured_for_child], child_tf, provider_data
                    )
                    confirmation_key = (symbol, int(root.get("id") or 0), 0, parent_tf, child_tf)
                    stored_confirmation_progress = progress_index.get(confirmation_key)
                    confirmation_status = inspect_progress(
                        stored_confirmation_progress, confirmation_hash, lower_candles
                    )
                    existing_confirmation_rows: List[Dict[str, Any]] = []
                    existing_confirmation_counts: Dict[int, Dict[str, int]] = {}
                    existing_confirmation_by_family: Dict[int, List[Dict[str, Any]]] = {}
                    existing_by_confirmation: Dict[int, Dict[int, bool]] = {}
                    saved_confirmation_rows_loaded = False
                    saved_confirmation_total = 0

                    # A full rebuild already schedules every parent family for
                    # confirmation matching. Reading old confirmation JSON maps
                    # here is unnecessary and can make the mysqlremote response
                    # fail before the rebuild starts. Only read maps when the
                    # lower timeframe has appended candles and cached successes
                    # must be preserved/refreshed. With unchanged candles, use a
                    # compact count-only query instead.
                    can_use_confirmation_cache = (
                        bool(lower_candles) and bool(parent_families)
                        and bool(confirmation_status.get("valid"))
                        and not parent_force_confirmation_full
                    )
                    needs_saved_maps = can_use_confirmation_cache and bool(confirmation_status.get("appended"))
                    needs_saved_summary = can_use_confirmation_cache and not bool(confirmation_status.get("appended"))

                    if (lower_candles and parent_families and parent_force_confirmation_full):
                        print(
                            "  CONFIRMATION CACHE READ: skipped because a full parent/confirmation "
                            "rebuild is already required"
                        )
                    elif (lower_candles and parent_families and not confirmation_status.get("valid")):
                        print(
                            "  CONFIRMATION CACHE READ: skipped because no valid confirmation "
                            "checkpoint exists; every parent family will be re-evaluated"
                        )

                    if needs_saved_maps:
                        try:
                            existing_confirmation_rows = fetch_existing_confirmation_rows(
                                db, programme_id, user_id, symbol, child_tf, parent_tf,
                                int(root.get("id") or 0), confirmation_ids,
                            )
                            saved_confirmation_rows_loaded = True
                        except BridgeError as exc:
                            confirmation_status["valid"] = False
                            confirmation_status["reason"] = "saved confirmation maps could not be read safely"
                            existing_confirmation_rows = []
                            print(
                                "  CONFIRMATION CACHE WARNING: saved confirmation maps could not be read; "
                                "all parent families will be re-evaluated. " + str(exc)[:700]
                            )

                    elif needs_saved_summary:
                        try:
                            existing_confirmation_counts = fetch_existing_confirmation_summary(
                                db, programme_id, user_id, symbol, child_tf, parent_tf,
                                int(root.get("id") or 0), confirmation_ids,
                            )
                        except BridgeError as exc:
                            confirmation_status["valid"] = False
                            confirmation_status["reason"] = "saved confirmation summary could not be read safely"
                            existing_confirmation_counts = {}
                            print(
                                "  CONFIRMATION CACHE WARNING: saved confirmation summary could not be read; "
                                "all parent families will be re-evaluated. " + str(exc)[:700]
                            )

                    for saved_row in existing_confirmation_rows:
                        saved_family_number = as_int(saved_row.get("family_number"))
                        saved_confirmation_id = as_int(saved_row.get("confirmation_root_id"))
                        if saved_family_number is None or saved_confirmation_id is None:
                            continue
                        existing_confirmation_by_family.setdefault(saved_family_number, []).append(saved_row)
                        existing_by_confirmation.setdefault(saved_confirmation_id, {})[saved_family_number] = bool(
                            as_int(saved_row.get("trade_configured"), 0)
                        )
                        counts = existing_confirmation_counts.setdefault(
                            saved_confirmation_id, {"family_count": 0, "trade_count": 0}
                        )
                        counts["family_count"] += 1
                        counts["trade_count"] += int(bool(as_int(saved_row.get("trade_configured"), 0)))

                    saved_confirmation_total = sum(
                        int(item.get("family_count") or 0)
                        for item in existing_confirmation_counts.values()
                    )
                    expected_saved_count = confirmation_status.get("saved_match_count")
                    if confirmation_status.get("valid") and expected_saved_count is not None:
                        actual_saved_count = (
                            len(existing_confirmation_rows) if saved_confirmation_rows_loaded
                            else saved_confirmation_total if needs_saved_summary
                            else int(expected_saved_count)
                        )
                        if actual_saved_count != int(expected_saved_count):
                            confirmation_status["valid"] = False
                            confirmation_status["reason"] = (
                                "saved confirmation analytics rows are incomplete or changed"
                            )
                            existing_confirmation_rows = []
                            existing_confirmation_counts = {}
                            existing_confirmation_by_family = {}
                            existing_by_confirmation = {}
                            saved_confirmation_total = 0
                            saved_confirmation_rows_loaded = False

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
                            int(record.get("family_number") or 0)
                            for record in new_records_for_cr
                            if int(record.get("family_number") or 0) > 0
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
                            int(as_int(row.get("family_number"), 0) or 0),
                        )
                        for row in existing_confirmation_rows
                        if as_int(row.get("confirmation_root_id"), 0) and as_int(row.get("family_number"), 0)
                    }
                    new_confirmation_identities = {
                        (
                            int(as_int(record.get("confirmation_root_id"), 0) or 0),
                            int(as_int(record.get("family_number"), 0) or 0),
                        )
                        for cr_records in confirmation_records.values()
                        for record in cr_records
                        if as_int(record.get("confirmation_root_id"), 0) and as_int(record.get("family_number"), 0)
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

                    append_progress_if_changed(
                        progress_index, progress_updates,
                        build_progress_row(
                            programme_id, user_id, symbol, int(root.get("id") or 0), 0,
                            parent_tf, child_tf, confirmation_hash, lower_candles,
                            max((int(family.get("familyNumber") or 0) for family in parent_families), default=0),
                            len(parent_families),
                            saved_match_count=saved_confirmation_count,
                        ),
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
            "live_trades": 0,
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
                print(f"  {symbol} {timeframe.upper()}: no new records to write; prior family results were reused")
                continue
            persistence = persist_pair(db, programme_id, user_id, symbol, timeframe, records)
            saved = sum(persistence.values())
            print(
                f"  {symbol} {timeframe.upper()}: {saved:,} new/changed analytics record(s) written "
                f"(batched upserts={persistence.get('upserted', 0):,}, "
                f"compatibility inserts={persistence.get('inserted', 0):,}, "
                f"updated={persistence.get('updated', 0):,}, preserved={persistence.get('preserved', 0):,})"
            )

        # Checkpoints are deliberately saved only after every analytics pair write
        # succeeds. If persistence fails midway, the next run will safely retry.
        print("  Saving incremental processing checkpoints...")
        persist_training_progress(db, progress_updates)
        print(f"  Checkpoint rows changed: {len(progress_updates):,} root/timeframe scope(s)")
        if not progress_updates:
            print("  Existing checkpoints were unchanged; no checkpoint rewrite was necessary.")

    totals["pairs"] = len(pairs_with_matches)
    print("Temporary runtime and result snapshots cleaned up")
    print(
        f"{programme_name} complete: {totals['pairs']} analytics pair(s), "
        f"{totals['families']} matched family record(s), {totals['trades']} configured trade record(s), "
        f"{totals['candles']:,} candles fetched"
    )
    print("History mode: every analytics record is saved with live_trades = 0")
    return totals

def main() -> int:
    loop = False
    parser = argparse.ArgumentParser(
        description="Programme Training: Begin-Test historical matcher and programme analytics writer"
    )
    parser.add_argument("--once", action="store_true", help="run one complete scan and exit")
    parser.add_argument("--interval", type=int, default=60, help="seconds between complete scans (default: 60)")
    parser.add_argument(
        "--programme-id", type=int, default=None,
        help="optionally limit to one programme, but only when signals_provider_interest.begin_test=1",
    )
    parser.add_argument("--bridge-host", default=BRIDGE_HOST)
    parser.add_argument("--bridge-port", type=int, default=BRIDGE_PORT)
    args = parser.parse_args()
    if args.once:
        loop = False
    if args.interval < 5:
        parser.error("--interval must be at least 5 seconds")

    print("Programme Training scope: all programmes with signals_provider_interest.begin_test = 1")
    print("VPS/IP limitation: disabled for now; programme discovery is based on Begin Test only")
    print("Root filter: apply_to_chart = 1 only; disabled roots and their confirmations are skipped")
    print(f"Continuous loop: {'enabled' if loop else 'disabled'} (set loop=False in main or use --once for one scan)")
    print(f"Database batch size: {ANALYTICS_BATCH_SIZE} analytics records per write")
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
            programmes = discover_programmes(db, args.programme_id)
            totals = {"programmes": 0, "pairs": 0, "families": 0, "trades": 0, "candles": 0}
            if not programmes:
                if args.programme_id is None:
                    print("No programmes have signals_provider_interest.begin_test = 1; nothing to scan.")
                else:
                    print(f"Programme {args.programme_id} has not begun a provision test; nothing to scan.")
            for programme in programmes:
                if stopping:
                    break
                try:
                    result = process_programme(db, programme)
                    totals["programmes"] += 1
                    for key in ("pairs", "families", "trades", "candles"):
                        totals[key] += int(result.get(key, 0))
                except Exception:
                    log.error("Programme %s failed:\n%s", programme.get("id"), traceback.format_exc())

            print("\n" + "-" * 82)
            print("PROGRAMME TRAINING SCAN COMPLETE")
            print(f"Programmes processed : {totals['programmes']}")
            print(f"Analytics pairs      : {totals['pairs']}")
            print(f"Matched families     : {totals['families']}")
            print(f"Trade records        : {totals['trades']}")
            print(f"Candles fetched      : {totals['candles']:,}")
            print("Live flag            : 0 (History)")
            print("Programme scope      : Begin Test flag only (no VPS/IP restriction)")
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
