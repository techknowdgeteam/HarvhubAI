#!/usr/bin/env python3
"""Python counterpart of programme_training.php's saved-configuration matcher.

Only programmes assigned to this computer's VPS IP are considered, including
active follower programmes hosted by a directly assigned owner programme. Only
roots with apply_to_chart enabled are processed. Begin Test does not control
programme discovery: when enabled, only newly detected pending-entry orders are
written to programme_analytics with live_trades=1; every other analytics record
is History (live_trades=0). When Begin Test is disabled, all records are History.

The worker fetches the exact configured symbol/timeframe candle scopes, restores
the saved configuration into the same JS-shaped runtime model, writes both into
temporary JSON, reloads that JSON, and then runs root/tree matching,
confirmation projection and historical trade calculations without chart
rendering. It never modifies candle_records, creates broker orders, or writes to
programme_trades.
"""
from __future__ import annotations

import argparse
import hashlib
import ipaddress
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
import urllib.request
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
            # The bridge often returns the generic "Some statements failed".
            # Preserve its per-statement diagnostics and the exact SQL so the
            # actual MySQL error (unknown column/table, permissions, etc.) is
            # visible instead of being swallowed by main().
            detail_text = json.dumps(reply, ensure_ascii=False, default=str)
            message = str(reply.get("message") or "SQL operation failed")
            sql_preview = re.sub(r"\\s+", " ", rendered_sql).strip()[:1600]
            raise BridgeError(
                f"{message}; bridge_reply={detail_text[:5000]}; SQL={sql_preview}"
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


def normalise_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {str(k).strip().lower(): v for k, v in row.items()}


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


def type_matches(candle: Dict[str, Any], wanted: Any) -> bool:
    w = str(wanted or "bearish_or_bullish").strip().lower()
    return not w or w == "bearish_or_bullish" or candle_type(candle) == w


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
    ref_mode_value = ref_row.get("reference_mode")
    ref_mode = str(ref_mode_value if has_db_value(ref_mode_value) else (
        "new_offset" if has_db_value(ref_row.get("offset_candle")) and has_db_value(ref_row.get("offset_to")) else "existing"
    )).lower()
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
        "mainCandleType": main_row.get("main_candle_type") or "bearish_or_bullish",
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
        "refCandleType": ref_row.get("ref_candle_type") or ref_row.get("offset_candle") or "bearish_or_bullish",
        "position": "right" if str(ref_row.get("position") or "left") == "right" else "left",
        "offsetTo": as_int(ref_row.get("offset_to")),
        "offsetToName": str(ref_row.get("offset_candle_name") or ""),
        "offsetPosition": as_int(ref_row.get("offset_position"), 1) or 1,
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
        sample = [{
            "config_id": r.get("config_id"), root_field: r.get(root_field),
            "root_id": r.get("root_id"), "tree_id": r.get("tree_id"),
            "main_candle_name": r.get("main_candle_name"),
            "reference_id": r.get("reference_id"),
            "reference_candle_name": r.get("reference_candle_name"),
            "reference_mode": r.get("reference_mode"),
            "draw_from": r.get("draw_from"), "draw_to": r.get("draw_to"),
        } for r in rows[:5]]
        log.warning(
            "Configuration rows could not be grouped (confirmation=%s): total=%s, "
            "missing_%s=%s, drawing_only=%s, sample=%s",
            confirmation, len(rows), root_field, missing_root, skipped_drawings,
            json.dumps(sample, ensure_ascii=False, default=str)
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


def print_tree_configuration(root: Dict[str, Any]) -> None:
    """Print the restored identities and links that the matcher actually uses."""
    root_name = _root_display_name(root)
    print(
        f"  CONFIG {root_name} (root_id={root.get('id')}): "
        f"timeframes={','.join(root.get('timeframes') or []) or '(none)'}, "
        f"trade_for={root.get('tradeFor')}, use_for_trades={bool(root.get('useForTrades'))}, "
        f"trees={len(root.get('trees') or [])}"
    )
    for index, tree in enumerate(sorted(root.get("trees") or [], key=lambda t: as_int(t.get("id"), 0) or 0)):
        main, ref = tree.get("main") or {}, tree.get("reference") or {}
        print(
            f"    TREE {tree.get('id')} [{index}] mode={tree.get('groupSelectionType')} "
            f"necessity={tree.get('groupNecessity')} action={tree.get('ifGroupFound')} | "
            f"MAIN {main.get('name')!r} id={main.get('id')} identity="
            f"{main.get('selectedExistingId') or main.get('offsetTo') or main.get('id')} "
            f"type={main.get('mainCandleType')} position={main.get('position')} "
            f"offset={main.get('offsetPosition')} point={main.get('offsetPosPoint')} "
            f"level={main.get('priceLevel')} op={main.get('operator')} "
            f"new_group_anchor={main.get('newGroupOffsetTo')} "
            f"direction={main.get('newGroupDirection')} "
            f"window=({tree.get('regionFromCandleId')},{tree.get('regionToCandleId')}) | "
            f"REF {ref.get('name')!r} id={ref.get('id')} identity="
            f"{ref.get('selectedExistingId') or ref.get('existingCandleId') or ref.get('id')} "
            f"mode={ref.get('referenceMode')} type={ref.get('refCandleType')} "
            f"position={ref.get('position')} offset={ref.get('offsetPosition')} "
            f"point={ref.get('offsetPosPoint')} level={ref.get('priceLevel')}"
        )
    for cr in (root.get("confirmationRoots") or []):
        print(
            f"    CONFIRMATION {cr.get('name')!r} id={cr.get('id')} "
            f"timeframes={','.join(cr.get('timeframes') or []) or '(none)'} "
            f"anchor_id={cr.get('anchorCandleId')} anchor_name={cr.get('anchorCandleName')!r} "
            f"anchor_time={cr.get('anchorTime')} trees={len(cr.get('trees') or [])} "
            f"use_for_trades={bool(cr.get('useForTrades'))}, trade_for={root.get('tradeFor')}"
        )


def print_matcher_diagnostics(root: Dict[str, Any], candle_count: int) -> None:
    diagnostics = root.get("_matcherDiagnostics") or []
    if not diagnostics:
        print("  MATCHER TRACE: no tree diagnostics were produced (root has no trees or no candles)")
        return
    print(f"  MATCHER TRACE: {len(diagnostics)} tree(s), input candles={candle_count}, cap={MAX_ROOT_MATCH_STATES} states")
    for item in diagnostics:
        fields = (
            f"incoming={item.get('incoming_states', 0)}, "
            f"main_scan={item.get('main_candles_scanned', 0)}, "
            f"main_type_ok={item.get('main_type_matches', 0)}, "
            f"ref_checks={item.get('reference_candidate_checks', 0)}, "
            f"ref_type_ok={item.get('reference_type_matches', 0)}, "
            f"ref_type_rejected={item.get('reference_type_rejections', 0)}, "
            f"ref_oob={item.get('reference_out_of_bounds', 0)}, "
            f"price_checks={item.get('price_comparisons', 0)}, "
            f"price_ok={item.get('price_matches', 0)}, "
            f"price_rejected={item.get('price_rejections', 0)}, "
            f"new_group_ref_ok={item.get('new_group_reference_resolved', 0)}, "
            f"anchor_missing={item.get('missing_anchor_states', 0)}, "
            f"anchor_unset={item.get('anchor_not_configured', False)}, "
            f"window_invalid={item.get('window_invalid_states', 0)}, "
            f"identity_missing={item.get('identity_missing_states', 0)}, "
            f"preselected_ref_reused={item.get('preselected_reference_reuses', 0)}, "
            f"output={item.get('retained_states', 0)}"
        )
        print(
            f"    TREE {item.get('tree_id')} {item.get('main_name')} -> {item.get('reference_name')}: "
            f"{fields}; blocker={item.get('blocker') or 'no specific blocker recorded'}"
        )


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
                      source_timeframe: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Build one historical trade exactly from the matched family and saved targets."""
    if family.get("noTrade") or not trade_root.get("useForTrades"):
        return None

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
        "entry_price": None, "stoploss_price": None, "take_profit_price": None,
        "configured_take_profit_price": None, "risk_reward": None, "configured_risk_reward": None,
        "minimum_risk_reward_applied": 0,
        "outcome": "identify but don't trade" if family.get("noTrade") else "matched family",
        "stoploss_hit": 0, "profit_reached": 0, "pending_entry": 0, "in_trade": 0,
        "closed_trade": 0, "trade_value_r": 0.0,
    }


# ------------------------------- VPS/IP scope ---------------------------------
def detect_vps_ip() -> Optional[str]:
    """Return this machine's public IP, preferring the configured override.

    programme_vps.programme_vps_ip_address stores the public VPS address, so a
    private interface address is not a safe substitute. If detection fails the
    caller must fail closed rather than scanning programmes without an IP scope.
    """
    configured = str(os.environ.get("HARVHUB_VPS_IP") or "").strip()
    if configured:
        try:
            return str(ipaddress.ip_address(configured))
        except ValueError:
            log.warning("HARVHUB_VPS_IP is not a valid IP address: %r", configured)

    endpoints = (
        "https://api.ipify.org",
        "https://checkip.amazonaws.com",
        "https://icanhazip.com",
    )
    for url in endpoints:
        try:
            request = urllib.request.Request(
                url,
                headers={"User-Agent": "HarvHubAI-ProgrammeTraining/1.0"},
            )
            with urllib.request.urlopen(request, timeout=8) as response:
                candidate = response.read(128).decode("ascii", errors="ignore").strip()
            return str(ipaddress.ip_address(candidate))
        except Exception as exc:
            log.warning("Public IP detection failed via %s: %s", url, str(exc)[:240])
    return None


def _in_sql(values: Iterable[int]) -> Tuple[str, List[int]]:
    clean = sorted({int(value) for value in values if int(value) > 0})
    if not clean:
        return "", []
    return ",".join(["%s"] * len(clean)), clean


# ------------------------------- VPS programme scope ---------------------------
def discover_programmes(db: BridgeClient,
                       programme_id: Optional[int] = None,
                       vps_ip: Optional[str] = None) -> List[Dict[str, Any]]:
    """Return programmes bound to this VPS IP, plus active hosted followers.

    The newest Signals Provider interest row is attached only as a status flag;
    Begin Test never restricts which VPS-associated programmes are processed.
    If the IP or required VPS schema cannot be verified, discovery fails closed.
    """
    local_ip = str(vps_ip or detect_vps_ip() or "").strip()
    if not local_ip:
        log.error("VPS scope unavailable: public IP could not be detected; refusing an unscoped programme scan")
        return []
    try:
        local_ip = str(ipaddress.ip_address(local_ip))
    except ValueError:
        log.error("VPS scope unavailable: invalid VPS IP %r; refusing an unscoped programme scan", local_ip)
        return []

    if not table_exists(db, "programme_vps"):
        log.error("VPS scope unavailable: programme_vps table does not exist; refusing an unscoped programme scan")
        return []

    vps_columns = {name.lower() for name in get_columns(db, "programme_vps")}
    required = {"programme_id", "user_id", "programme_vps_ip_address"}
    if not required.issubset(vps_columns):
        log.error(
            "VPS scope unavailable: programme_vps is missing required column(s): %s",
            ", ".join(sorted(required - vps_columns)),
        )
        return []

    owner_rows = db.fetch_all(
        "SELECT `programme_id`,`user_id` FROM `programme_vps` "
        "WHERE `programme_vps_ip_address`=%s",
        [local_ip], timeout=90,
    )
    owner_user_by_programme: Dict[int, int] = {}
    for row in owner_rows:
        pid = as_int(row.get("programme_id"))
        uid = as_int(row.get("user_id"))
        if pid and pid > 0 and uid and uid > 0:
            owner_user_by_programme[pid] = uid

    roles: Dict[int, set[str]] = {pid: {"owner"} for pid in owner_user_by_programme}
    follower_owner: Dict[int, set[int]] = {}
    if owner_user_by_programme and table_exists(db, "programme_vps_hosts_followers"):
        follower_columns = {name.lower() for name in get_columns(db, "programme_vps_hosts_followers")}
        follower_required = {"owner_programme_id", "follower_programme_id", "host_status"}
        if follower_required.issubset(follower_columns):
            marks, params = _in_sql(owner_user_by_programme.keys())
            follower_rows = db.fetch_all(
                "SELECT `owner_programme_id`,`follower_programme_id`,`host_status` "
                "FROM `programme_vps_hosts_followers` "
                f"WHERE `owner_programme_id` IN ({marks}) AND `host_status`='active'",
                params, timeout=90,
            )
            for row in follower_rows:
                owner_pid = as_int(row.get("owner_programme_id"))
                follower_pid = as_int(row.get("follower_programme_id"))
                if owner_pid in owner_user_by_programme and follower_pid and follower_pid > 0:
                    roles.setdefault(follower_pid, set()).add("active follower")
                    follower_owner.setdefault(follower_pid, set()).add(owner_pid)

    associated_ids = sorted(roles)
    if not associated_ids:
        log.info("VPS %s has no direct programme or active follower bindings", local_ip)
        return []

    marks, params = _in_sql(associated_ids)
    programme_rows = db.fetch_all(
        f"SELECT * FROM `programme` WHERE `id` IN ({marks}) ORDER BY `id` ASC",
        params, timeout=120,
    )

    latest_interest: Dict[Tuple[int, int], Dict[str, Any]] = {}
    if table_exists(db, "signals_provider_interest"):
        interest_rows = db.fetch_all(
            f"SELECT * FROM `signals_provider_interest` WHERE `programme_id` IN ({marks}) ORDER BY `id` DESC",
            params, timeout=120,
        )
        for row in interest_rows:
            pid = as_int(row.get("programme_id"))
            uid = as_int(row.get("user_id"))
            if pid and uid:
                latest_interest.setdefault((pid, uid), row)

    output: List[Dict[str, Any]] = []
    for programme in programme_rows:
        pid = as_int(programme.get("id"))
        uid = as_int(programme.get("userid"))
        if not pid or not uid or pid not in roles:
            continue
        # A direct VPS owner binding must still match programme.userid. Active
        # followers are independently owned and are allowed through their host.
        if "owner" in roles[pid] and owner_user_by_programme.get(pid) != uid:
            log.warning(
                "Ignoring mismatched VPS owner binding: programme=%s database_owner=%s vps_owner=%s",
                pid, uid, owner_user_by_programme.get(pid),
            )
            roles[pid].discard("owner")
            if not roles[pid]:
                continue

        interest = latest_interest.get((pid, uid), {})
        programme["vps_associated"] = 1
        programme["vps_ip_address"] = local_ip
        programme["vps_role"] = ", ".join(sorted(roles[pid]))
        programme["vps_host_programme_ids"] = sorted(follower_owner.get(pid, set()))
        programme["begin_test_interest_id"] = interest.get("id")
        programme["begin_test_flag"] = as_int(interest.get("begin_test"), 0) or 0
        programme["begin_test_status"] = interest.get("interest_status") or "not started"
        programme["begin_test_started_at"] = interest.get("signal_testing_started_at")
        output.append(programme)

    if programme_id is not None:
        output = [item for item in output if as_int(item.get("id")) == int(programme_id)]

    log.info(
        "VPS scope resolved: IP=%s | direct owner bindings=%s | associated programmes=%s%s",
        local_ip, len(owner_user_by_programme), len(output),
        f" (programme filter {programme_id})" if programme_id is not None else "",
    )
    return output

# ------------------------------- DB operations --------------------------------
ANALYTICS_COLUMNS = [
    "programme_id", "userid", "symbol", "timeframe", "source_timeframe", "root_id", "root_name",
    "confirmation_root_id", "confirmation_root_name", "family_number", "family_signature", "family_map_json",
    "match_json", "matched_family", "trade_configured", "live_trades", "order_type", "direction", "entry_name", "stoploss_name",
    "take_profit_name", "entry_anchor_time", "entry_trigger_time", "stoploss_time", "take_profit_time",
    "stoploss_trigger_time", "take_profit_trigger_time", "event_time", "entry_price", "stoploss_price",
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
    """Stable identity used for scope-safe analytics reads/upserts."""
    return {
        "programme_id": record.get("programme_id"),
        "userid": record.get("userid"),
        "symbol": record.get("symbol"),
        "timeframe": record.get("timeframe"),
        "root_id": record.get("root_id"),
        "confirmation_root_id": record.get("confirmation_root_id") or 0,
        "source_timeframe": record.get("source_timeframe"),
        "family_number": record.get("family_number"),
    }


ANALYTICS_IDENTITY_COLUMNS = [
    "programme_id", "userid", "symbol", "timeframe", "root_id",
    "confirmation_root_id", "source_timeframe", "family_number",
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
    """Build an analytics identity predicate with NULL/0 confirmation parity.

    Older analytics rows may store a root-only confirmation ID as NULL while
    current rows use 0. They represent the same logical identity.
    """
    parts: List[str] = []
    values: List[Any] = []
    for column in columns:
        if column == "confirmation_root_id":
            parts.append("COALESCE(`confirmation_root_id`,0)=%s")
            value = identity.get(column)
            values.append(0 if value in (None, "") else value)
        else:
            parts.append(f"`{column}`=%s")
            values.append(identity.get(column))
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
    """Insert or refresh one analytics identity without deleting history.

    First locate an existing logical identity (including legacy NULL confirmation
    IDs), then update it. Only a genuinely new identity is inserted. This avoids
    duplicate root-only rows when an older row uses confirmation_root_id=NULL.
    """
    values = [record.get(column) for column in columns]
    identity = {column: record.get(column) for column in identity_columns}
    where_sql, where_values = _identity_where(identity, identity_columns)
    insert_columns_sql = ",".join(f"`{column}`" for column in columns)
    insert_marks = "(" + ",".join(["%s"] * len(columns)) + ")"
    update_columns = [column for column in columns if column not in identity_columns]

    existing = db.fetch_all(
        f"SELECT 1 AS `found` FROM `{table}` WHERE {where_sql} LIMIT 1",
        where_values, timeout=60,
    )
    if existing:
        set_sql = ",".join(f"`{column}`=%s" for column in update_columns)
        update_values = [record.get(column) for column in update_columns] + where_values
        try:
            db.execute(
                f"UPDATE `{table}` SET {set_sql} WHERE {where_sql}",
                update_values, timeout=120,
            )
            return "updated"
        except BridgeError as exc:
            log.warning(
                "    %s: bridge does not permit refreshing this existing row; preserving history: %s",
                label, str(exc)[:900],
            )
            return "preserved"

    if has_unique_identity:
        update_sql = ",".join(f"`{column}`=VALUES(`{column}`)" for column in update_columns)
        upsert_sql = (
            f"INSERT INTO `{table}` ({insert_columns_sql}) VALUES {insert_marks} "
            f"ON DUPLICATE KEY UPDATE {update_sql}"
        )
        try:
            db.query(upsert_sql, values, timeout=120)
            return "upserted"
        except BridgeError as exc:
            log.warning(
                "    %s: insert/upsert form rejected; trying insert-only for this new identity: %s",
                label, str(exc)[:900],
            )

    db.query(
        f"INSERT INTO `{table}` ({insert_columns_sql}) VALUES {insert_marks}",
        values, timeout=120,
    )
    return "inserted"

def persist_pair(db: BridgeClient, programme_id: int, user_id: int, symbol: str, timeframe: str,
                 records: List[Dict[str, Any]]) -> Dict[str, int]:
    """Persist analytics using each record's already-decided live/history flag."""
    if not table_exists(db, "programme_analytics"):
        raise RuntimeError("Required table programme_analytics does not exist; create the analytics schema first.")

    columns = get_columns(db, "programme_analytics")
    lower_columns = {str(column).lower() for column in columns}
    if "live_trades" not in lower_columns:
        position = " AFTER `trade_configured`" if "trade_configured" in lower_columns else ""
        try:
            db.execute(
                "ALTER TABLE `programme_analytics` ADD COLUMN `live_trades` TINYINT(1) NOT NULL DEFAULT 0" + position,
                timeout=90,
            )
            columns = get_columns(db, "programme_analytics")
            lower_columns = {str(column).lower() for column in columns}
        except BridgeError as exc:
            raise RuntimeError(
                "programme_analytics.live_trades is missing and could not be added. "
                "Run the programme_analytics_live_trades.sql migration. " + str(exc)
            ) from exc

    missing = [column for column in ANALYTICS_COLUMNS if column.lower() not in lower_columns]
    if missing:
        raise RuntimeError("programme_analytics is missing required column(s): " + ", ".join(missing))

    counters = {"inserted": 0, "upserted": 0, "updated": 0, "preserved": 0}
    unique_identity = _has_exact_unique_identity(db, "programme_analytics", ANALYTICS_IDENTITY_COLUMNS)
    for record in _deduplicate_records(records):
        record["live_trades"] = 1 if as_int(record.get("live_trades"), 0) == 1 else 0
        status = _insert_or_refresh_row(
            db, "programme_analytics", record, ANALYTICS_COLUMNS,
            ANALYTICS_IDENTITY_COLUMNS, unique_identity,
            f"{symbol} {timeframe.upper()} root {record.get('root_id')} family {record.get('family_number')}",
        )
        counters[status] = counters.get(status, 0) + 1

    if not records:
        log.info("    %s %s: no matched families to write; existing historical analytics were not deleted", symbol, timeframe.upper())
    return counters


def reset_programme_live_trades(db: BridgeClient, programme_id: int, user_id: int) -> int:
    """Set all existing analytics for this programme to History before recalculation."""
    if not table_exists(db, "programme_analytics"):
        return 0
    columns = {name.lower() for name in get_columns(db, "programme_analytics")}
    if "live_trades" not in columns:
        # persist_pair() adds the column if needed; its default is History (0).
        return 0
    return db.execute(
        "UPDATE `programme_analytics` SET `live_trades`=0 "
        "WHERE `programme_id`=%s AND `userid`=%s",
        [programme_id, user_id], timeout=120,
    )


def print_persistent_analytics_summary(db: BridgeClient, programme_id: int, user_id: int) -> None:
    """Show what the PHP dashboard can already read before this scan writes anything."""
    print("Persistent Programme Analytics already stored (read-only baseline):")
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
    for row in rows:
        count = as_int(row.get("analytics_rows"), 0) or 0
        families = as_int(row.get("matched_families"), 0) or 0
        trades = as_int(row.get("configured_trades"), 0) or 0
        total_rows += count
        total_families += families
        total_trades += trades
        confirmation_id = as_int(row.get("confirmation_root_id"), 0) or 0
        scope_name = f"confirmation_root={confirmation_id}" if confirmation_id else "main_root"
        print(
            f"  {row.get('symbol')} {str(row.get('timeframe') or '').upper()} "
            f"root={row.get('root_id')} {scope_name}: rows={count}, "
            f"matched_families={families}, configured_trades={trades}"
        )
    print(
        f"  STORED TOTALS: analytics_rows={total_rows}, "
        f"matched_families={total_families}, configured_trades={total_trades}"
    )
    print("  These stored totals are what the PHP analytics page reads; they are not the current scan's match count.")


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


def apply_live_trade_policy(
    records_by_pair: Dict[Tuple[str, str], List[Dict[str, Any]]],
    begin_test_enabled: bool,
) -> Tuple[int, int]:
    """Mark only configured pending-entry orders Live during an active Begin Test."""
    live_pending_count = 0
    history_trade_count = 0
    for pair_records in records_by_pair.values():
        for record in pair_records:
            is_pending_order = (
                as_int(record.get("trade_configured"), 0) == 1
                and (
                    as_int(record.get("pending_entry"), 0) == 1
                    or str(record.get("outcome") or "").strip().lower() == "pending entry"
                )
            )
            is_live = bool(begin_test_enabled and is_pending_order)
            record["live_trades"] = 1 if is_live else 0
            if is_live:
                live_pending_count += 1
            elif as_int(record.get("trade_configured"), 0) == 1:
                history_trade_count += 1
    return live_pending_count, history_trade_count


def process_programme(db: BridgeClient, programme: Dict[str, Any]) -> Dict[str, int]:
    """Run the complete PHP-equivalent historical matching chain for one programme."""
    programme_id = int(programme.get("id") or 0)
    user_id = int(programme.get("userid") or 0)
    programme_name = str(programme.get("program_name") or f"Programme {programme_id}")

    print("\n" + "=" * 82)
    print(f"PROGRAMME: {programme_name} (ID {programme_id})")
    print("=" * 82)
    print(f"Owner user ID: {user_id}")
    begin_test_enabled = truthy(programme.get("begin_test_flag"), False)
    begin_status = str(programme.get("begin_test_status") or "not started").strip().lower()
    begin_started = programme.get("begin_test_started_at") or "not recorded"
    begin_interest_id = programme.get("begin_test_interest_id") or "none"
    print(f"VPS IP: {programme.get('vps_ip_address') or 'unknown'} | VPS role: {programme.get('vps_role') or 'unknown'}")
    print(
        f"{programme_name} Begin Test = {'enabled' if begin_test_enabled else 'disabled'} "
        f"(interest #{begin_interest_id}, status={begin_status}, started_at={begin_started})"
    )
    demoted = reset_programme_live_trades(db, programme_id, user_id)
    print(f"Existing analytics reset to History before recalculation: {demoted} row(s)")

    print("fetching saved programme training configuration..")
    roots, confirmation_roots, parent_rows, confirmation_rows = load_programme_config(
        db, user_id, programme_id
    )
    roots = sorted(roots, key=lambda r: (int(r.get("id") or 0), str(r.get("name") or "").casefold()))

    # The browser uses applyToChart as the root-level execution gate. Confirmation
    # alternatives inherit that parent gate; a disabled parent contributes nothing.
    for root in roots:
        enabled = bool(root.get("applyToChart"))
        print(f"{programme_name} | { _root_display_name(root) } apply to chart = {'yes' if enabled else 'no'}")
    active_roots = [root for root in roots if bool(root.get("applyToChart"))]
    if not active_roots:
        print(f"{programme_name}: no roots have apply_to_chart enabled. Skipping candle fetch and analytics.")
        return {"pairs": 0, "families": 0, "trades": 0, "candles": 0}

    print("Restored active-root configuration trace (identities below are the ones consumed by Python):")
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
        print(
            f"  {symbol} {timeframe.upper()}: fetched {len(candles):,} candle record(s); "
            f"oldest={diag['oldest'] or 'n/a'}, newest={diag['newest'] or 'n/a'}, "
            f"chronological={diag['chronological_non_decreasing']}, "
            f"strict={diag['chronological_strict']}, duplicate_ids={diag['duplicate_ids']}, "
            f"duplicate_times={diag['duplicate_timestamps']}, missing_OHLC={diag['missing_ohlc_rows']}, "
            f"high_below_low={diag['high_below_low_rows']}"
        )
        if diag["valid_timestamps"] != len(candles):
            print(f"    DATA WARNING: {len(candles) - diag['valid_timestamps']} candle(s) have no parseable time/open_time")
        if not diag["chronological_non_decreasing"]:
            print("    DATA WARNING: candle timestamps are not ascending; PHP chart matching receives oldest-to-newest candles")

    provider = load_provider_risk_reward(db, programme)
    print("converting and building into json")
    snapshot_payload = {
        "schema": "programme_training_runtime_snapshot_v1",
        "programme": {
            "id": programme_id,
            "userid": user_id,
            "program_name": programme_name,
            "selected_symbols": programme.get("selected_symbols"),
            "begin_test_interest_id": programme.get("begin_test_interest_id"),
            "begin_test_flag": int(begin_test_enabled),
            "begin_test_status": programme.get("begin_test_status"),
            "begin_test_started_at": programme.get("begin_test_started_at"),
            "vps_ip_address": programme.get("vps_ip_address"),
            "vps_role": programme.get("vps_role"),
        },
        "scope": {
            "programme_id": programme_id,
            "user_id": user_id,
            "symbols": symbols,
            "required_timeframes": required_timeframes,
            "required_pairs": [{"symbol": symbol, "timeframe": tf} for symbol, tf in required_pairs_ordered],
            "programme_filter": "programme_vps IP match (direct owner or active follower)",
            "begin_test_policy": "only pending_entry trade records are live when Begin Test is enabled",
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
        print(f"  Temporary runtime JSON created: {snapshot_path.name} ({snapshot_path.stat().st_size:,} bytes)")
        print("  Reloading temporary JSON as the Python in-memory configuration and candle store..")
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

        print("configuration matching engine at the background..")
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
            parent_results: List[Tuple[str, str, List[Dict[str, Any]], List[Dict[str, Any]]]] = []
            for symbol in symbols:
                for parent_tf in parent_tfs:
                    parent_candles = candle_cache.get(symbol, {}).get(parent_tf) or []
                    print(f"\n{programme_name} {symbol} {root_name} {parent_tf.upper()}:")
                    if not parent_candles:
                        print(f"  {root_name} has 0 matched families (no candle records for {symbol} {parent_tf.upper()})")
                        print(f"  trade is {'setup' if parent_trade_setup else 'not setup'} for {root_name}")
                        if parent_trade_setup:
                            print("  found 0 trades")
                        parent_results.append((symbol, parent_tf, parent_candles, []))
                        continue

                    families = match_root(root, parent_candles)
                    print(f"  {root_name} has {len(families)} matched families")
                    print_matcher_diagnostics(root, len(parent_candles))
                    final_trace = root.get("_matcherFinal") or {}
                    print(
                        f"  MATCHER FINAL: required_trees={final_trace.get('required_trees', 0)}, "
                        f"processed_trees={final_trace.get('processed_trees', 0)}, "
                        f"final_chain_states={final_trace.get('final_chain_states', 0)}, "
                        f"complete_families={final_trace.get('complete_families', 0)}, "
                        f"blocker={final_trace.get('blocker', 'not recorded')}"
                    )
                    print(f"  trade is {'setup' if parent_trade_setup else 'not setup'} for {root_name}")
                    trade_count = 0
                    parent_records: List[Dict[str, Any]] = []
                    for family in families:
                        base = match_row(
                            programme_id, user_id, symbol, parent_tf,
                            root, family, parent_candles,
                            source_timeframe=parent_tf,
                        )
                        trade = None
                        if trade_for == "root" and root.get("useForTrades"):
                            trade = make_trade_record(
                                root, root, family, parent_candles, parent_tf,
                                symbol, programme_id, user_id,
                                provider_rr=provider_rr,
                                provider_rr_type=provider_rr_type,
                                source_timeframe=parent_tf,
                            )
                        record = trade if trade else base
                        parent_records.append(record)
                        if trade:
                            trade_count += 1
                    records_by_pair.setdefault((symbol, parent_tf), []).extend(parent_records)
                    if parent_trade_setup:
                        print(f"  found {trade_count} trades")
                        if root.get("useForTrades") and trade_count < len(families):
                            print("  some matched families did not produce trades because they were identify-only or their trade targets were not resolvable")
                    parent_results.append((symbol, parent_tf, parent_candles, families))

            # CONFIRMATION MATCHING: each confirmation alternative is evaluated on
            # its own selected lower timeframe, anchored to each matched parent family.
            if not confirmation_roots_for_parent:
                continue

            for symbol, parent_tf, parent_candles, parent_families in parent_results:
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
                    for _order, cr in configured_for_child:
                        cr_name = _root_display_name(cr, True)
                        print(f"\n{programme_name} {symbol} {root_name} {cr_name} {child_tf.upper()}:")
                        print(f"  {cr_name} projection process on {root_name}")
                        if bool(cr.get("useForTrades")) and trade_for == "root_confirmation":
                            print("  trade is setup for this confirmation")
                        else:
                            print("  trade is not setup for this confirmation")

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
                                print("  found 0 trades, check analytics in programme analytics")
                        continue

                    parent_ms = tf_milliseconds(parent_tf)
                    child_ms = tf_milliseconds(child_tf)
                    if parent_ms is None or child_ms is None or parent_ms <= child_ms:
                        for _order, cr in configured_for_child:
                            print(f"  {_root_display_name(cr, True)} has 0 matched families (confirmation timeframe must be lower than {parent_tf.upper()})")
                            if bool(cr.get("useForTrades")) and trade_for == "root_confirmation":
                                print("  found 0 trades, check analytics in programme analytics")
                        continue
                    if not lower_candles:
                        for _order, cr in configured_for_child:
                            print(f"  {_root_display_name(cr, True)} has 0 matched families (no {child_tf.upper()} candle records)")
                            if bool(cr.get("useForTrades")) and trade_for == "root_confirmation":
                                print("  found 0 trades, check analytics in programme analytics")
                        continue

                    projection = projection_map(parent_candles, lower_candles, parent_tf, child_tf)
                    valid_projection_pieces = sum(
                        1 for piece in projection
                        if piece.get("first", -1) >= 0 and piece.get("last", -1) >= 0
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
                        for parent_family in parent_families:
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
                        print(
                            f"  CONFIRMATION TRACE {_root_display_name(cr, True)}: "
                            f"parent_families_checked={trace.get('parent_families_checked', 0)}, "
                            f"anchor_mapped={trace.get('parent_family_anchor_mapped', 0)}, "
                            f"invalid_parent_index={trace.get('invalid_parent_indices', 0)}, "
                            f"no_projection_window={trace.get('projection_window_missing', 0)}, "
                            f"bad_anchor_time={trace.get('anchor_timestamp_invalid', 0)}, "
                            f"no_lower_anchor={trace.get('lower_anchor_not_found', 0)}, "
                            f"matcher_calls={trace.get('matcher_calls', 0)}, "
                            f"matcher_successes={trace.get('matcher_successes', 0)}, "
                            f"matcher_detail={json.dumps(matcher, ensure_ascii=False, default=str)}, "
                            f"setup_blockers={json.dumps(trace.get('setup_blockers', []), ensure_ascii=False)}"
                        )

                    confirmation_records: Dict[int, List[Dict[str, Any]]] = {
                        int(cr.get("id") or 0): [] for _order, cr in configured_for_child
                    }
                    confirmation_trade_counts: Dict[int, int] = {
                        int(cr.get("id") or 0): 0 for _order, cr in configured_for_child
                    }
                    for parent_family in parent_families:
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
                            )
                        final_record = trade if trade else base
                        records_by_pair.setdefault((symbol, child_tf), []).append(final_record)
                        confirmation_records.setdefault(int(winning_cr.get("id") or 0), []).append(final_record)
                        if trade:
                            confirmation_trade_counts[int(winning_cr.get("id") or 0)] = confirmation_trade_counts.get(int(winning_cr.get("id") or 0), 0) + 1

                    for _cr_order, cr in configured_for_child:
                        cr_id = int(cr.get("id") or 0)
                        cr_name = _root_display_name(cr, True)
                        cr_records = confirmation_records.get(cr_id, [])
                        trade_count = confirmation_trade_counts.get(cr_id, 0)
                        print(f"  {cr_name} has {len(cr_records)} matched families")
                        if bool(cr.get("useForTrades")) and trade_for == "root_confirmation":
                            print(f"  found {trade_count} trades, check analytics in programme analytics")

        # Only an untriggered pending-entry order is Live, and only while Begin
        # Test is enabled. Matched families, active positions, and completed
        # trades are always History. This is deliberately applied after matching
        # so both parent and confirmation trade records follow the same rule.
        live_pending_count, history_trade_count = apply_live_trade_policy(
            records_by_pair, begin_test_enabled
        )

        print(
            f"Live/History policy: Begin Test={'ON' if begin_test_enabled else 'OFF'} | "
            f"pending orders set Live={live_pending_count} | "
            f"trade records set History={history_trade_count}"
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
            "begin_test_enabled": bool(begin_test_enabled),
            "live_trade_policy": "pending_entry_only_when_begin_test_enabled",
            "results": result_rows,
        }
        _snapshot_write(result_path, result_payload)
        with result_path.open("r", encoding="utf-8") as handle:
            result_from_json = json.load(handle)

        print("updating analytics to programme analytics")
        for pair_result in result_from_json.get("results", []):
            symbol = str(pair_result.get("symbol") or "")
            timeframe = str(pair_result.get("timeframe") or "").lower()
            records = pair_result.get("records") or []
            if not records:
                print(f"  {symbol} {timeframe.upper()}: 0 analytics rows from this run; no new row inserted")
                continue
            persistence = persist_pair(db, programme_id, user_id, symbol, timeframe, records)
            saved = sum(persistence.values())
            totals["pairs"] += 1
            totals["families"] += sum(int(record.get("matched_family") or 0) for record in records)
            totals["trades"] += sum(int(record.get("trade_configured") or 0) for record in records)
            print(
                f"  {symbol} {timeframe.upper()}: analytics rows processed={saved} "
                f"(inserted={persistence.get('inserted', 0)}, upserted={persistence.get('upserted', 0)}, "
                f"updated={persistence.get('updated', 0)}, preserved={persistence.get('preserved', 0)})"
            )

    print("Temporary JSON files cleaned up")
    print(
        f"{programme_name} complete: {totals['pairs']} analytics pair(s), "
        f"{totals['families']} matched family record(s), {totals['trades']} configured trade record(s), "
        f"{totals['candles']:,} candles fetched"
    )
    print("Analytics status: only pending-entry orders are Live when Begin Test is enabled; all others are History")
    return totals

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Programme Training: VPS-scoped chart matcher with Begin-Test pending-order status"
    )
    parser.add_argument("--execution-type", choices=("loop", "once"), default="loop", help="manager execution mode")
    parser.add_argument("--once", action="store_true", help="run one complete scan and exit (legacy alias)")
    parser.add_argument("--interval", type=int, default=60, help="seconds between complete scans (default: 60)")
    parser.add_argument(
        "--programme-id", type=int, default=None,
        help="optionally limit to one programme already assigned to this VPS IP",
    )
    parser.add_argument(
        "--vps-ip", default=None,
        help="override detected public VPS IP (must match programme_vps.programme_vps_ip_address)",
    )
    parser.add_argument("--bridge-host", default=BRIDGE_HOST)
    parser.add_argument("--bridge-port", type=int, default=BRIDGE_PORT)
    args = parser.parse_args()
    if args.interval < 5:
        parser.error("--interval must be at least 5 seconds")
    run_as_loop = args.execution_type == "loop" and not args.once

    print(f"Programme Training starting | mode={'LOOP' if run_as_loop else 'ONCE'} | interval={args.interval}s", flush=True)
    print("Programme Training scope: programmes bound to this VPS IP (direct owner + active followers)")
    print("Begin Test policy: enabled = pending-entry orders Live; disabled = all trades History")
    print("Root filter: apply_to_chart = 1 only; disabled roots and their confirmations are skipped")
    db = BridgeClient(args.bridge_host, args.bridge_port, BRIDGE_OWNER)
    stopping = False

    def stop(_signum: int, _frame: Any) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, stop)

    try:
        local_vps_ip = args.vps_ip or detect_vps_ip()
        if local_vps_ip:
            print(f"Detected VPS public IP: {local_vps_ip}", flush=True)
        else:
            print("VPS public IP could not be detected. Programme discovery will fail closed; no unscoped programmes will run.", flush=True)

        while not stopping:
            started = time.monotonic()
            print("\n" + "=" * 82, flush=True)
            print(f"PROGRAMME TRAINING SCAN STARTED | cycle={int(started)} | mode={'LOOP' if run_as_loop else 'ONCE'}", flush=True)
            print(f"Discovering programmes assigned to VPS IP {local_vps_ip or '(unavailable)'}...", flush=True)
            programmes = discover_programmes(db, args.programme_id, local_vps_ip)
            print(f"Programme discovery returned {len(programmes)} VPS-associated candidate programme(s).", flush=True)
            totals = {"programmes": 0, "pairs": 0, "families": 0, "trades": 0, "candles": 0}
            if not programmes:
                if not local_vps_ip:
                    print("No scan performed because the VPS IP is unavailable; refusing to process programmes without an IP match.")
                elif args.programme_id is None:
                    print(f"No programmes are assigned to VPS IP {local_vps_ip}; nothing to scan.")
                else:
                    print(f"Programme {args.programme_id} is not assigned to VPS IP {local_vps_ip}; nothing to scan.")
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
            print("Live flag            : pending-entry orders only, and only when Begin Test is enabled")
            print(f"Programme scope      : VPS IP match ({local_vps_ip or 'unavailable'}); direct owner + active followers")
            print("-" * 82)

            if not run_as_loop:
                print("Programme Training one-shot scan finished; exiting.", flush=True)
                break
            elapsed = time.monotonic() - started
            sleep_for = max(1, args.interval - int(elapsed))
            print(f"Next Programme Training scan in {sleep_for}s.", flush=True)
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
