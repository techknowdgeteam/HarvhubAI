#!/usr/bin/env python3
"""Python counterpart of programme_training.php's saved-configuration matcher.

All programmes are eligible for this temporary test run; the VPS/computer-IP
assignment guard is disabled. Use --programme-id to limit a test to one programme.
Only roots with apply_to_chart enabled are processed. Begin Test does not control
programme discovery: when enabled, only newly detected pending-entry orders are
written to programme_analytics with live_trades=1; every other analytics record
is History (live_trades=0). When Begin Test is disabled, all records are History.

The worker fetches the exact configured symbol/timeframe candle scopes, restores
the saved configuration into the same JS-shaped runtime model, writes both into
temporary JSON, reloads that JSON, and then runs root/tree matching,
confirmation projection and historical trade calculations without chart
rendering. Each scan snapshots the existing analytics setup groups and compares
newly calculated records with the existing database content. Exact programme trade
and analytics matches are left untouched (no INSERT or UPDATE); changed stable
identities are updated in place; only missing identities are inserted. Programme
trades are synchronized and verified before analytics writes. Obsolete analytics
rows are deleted only after every expected analytics identity and payload has been
read back and verified. programme_trades rows are never deleted; investor copies
and broker trade IDs are not repurposed. The worker never modifies candle_records
or creates broker orders.

IMPORTANT: PHP/PDO PARITY FOR MYSQL NULL VALUES
------------------------------------------------
MySQL Remote can serialize an actual SQL NULL as the literal text "NULL" in a
row returned to Python. PHP PDO exposes that same database value as an actual
null. Therefore every MySQL row must be normalized at the database boundary
before configuration restoration or matching begins: the Python worker converts
the SQL-NULL text sentinel to Python None, just as PHP PDO does.

Do not solve this mismatch with matcher-specific workarounds, additional
fallback rules, or independent Python-only matching behavior. The Python
configuration restoration and matcher must follow programme_training.php's
saved-row restoration, defaults, identity handling, and matching decisions.
Only the SQL-NULL sentinel is converted here; legitimate empty strings, "0",
"false", and other stored text values are not rewritten by this conversion.

IMPORTANT: MYSQL REMOTE SQL TEXT LITERAL FORMAT
------------------------------------------------
For SQL text values sent through this worker's MySQL Remote/PHP query path,
use the charset-introduced hexadecimal form `_utf8mb4 X'...'`. The form
`CONVERT(X'...' USING utf8mb4)` was tested through the same query interface
and failed, while `_utf8mb4 X'...'` succeeded. Keep this encoding centralized
in sql_literal(); do not introduce a separate Python-only encoding per query.
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
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP, localcontext
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

BRIDGE_HOST = "127.0.0.1"
BRIDGE_PORT = 5599
BRIDGE_OWNER = "programme_training"
MAX_FRAME = 64 * 1024 * 1024
HEADER = struct.Struct(">I")
MAX_ROOT_MATCH_STATES = 2500

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
    # Match the verified MySQL Remote/PHP query-interface representation.
    # CONVERT(X'...' USING utf8mb4) fails through that endpoint, while the
    # equivalent _utf8mb4 X'...' character-set introducer succeeds.
    return "_utf8mb4 X'" + raw.hex() + "'"


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


def _bridge_sql_is_read(sql: Any) -> bool:
    """Only retry operations whose repeated execution cannot change data."""
    text = str(sql or '').strip()
    while True:
        previous = text
        text = text.lstrip()
        if text.startswith('--'):
            newline = text.find('\n')
            text = text[newline + 1:] if newline >= 0 else ''
        elif text.startswith('/*'):
            close = text.find('*/')
            text = text[close + 2:] if close >= 0 else ''
        if text == previous:
            break
    if not text:
        return False
    first = text.split(None, 1)[0].upper()
    return first in {'SELECT', 'SHOW', 'DESCRIBE', 'DESC', 'EXPLAIN', 'WITH'}


def _bridge_error_is_transient(message: Any) -> bool:
    """Recognise transient browser/PHP/bridge errors, not SQL schema errors."""
    value = str(message or '').lower()
    markers = (
        "unexpected token '<'", 'not valid json', 'failed to fetch',
        'browser not available', 'channel window missing',
        'window missing', 'unknown channel_id', 'channel limit reached',
        'failed to spawn dom window', 'query input failed',
        'timed out', 'timeout (last=', 'cannot talk to bridge',
        'mysqlremote bridge closed', 'connection refused',
        'connection reset', 'forcibly closed', 'temporarily unavailable',
        'non-json response', '[empty response]', 'empty response',
        'text/html; charset=utf-8', 'upstream returned html',
        'signal is aborted without reason', 'aborterror', 'the operation was aborted',
        'request timed out', 'database request timed out',
    )
    return any(marker in value for marker in markers)


def format_sql_for_console(sql: str) -> str:
    """Format SQL diagnostics as readable multi-line text for the console."""
    compact = re.sub(r"\s+", " ", str(sql or "")).strip()
    keywords = r"\s+(FROM|WHERE|GROUP BY|ORDER BY|HAVING|LIMIT|AND|OR)\s+"
    return re.sub(
        keywords,
        lambda match: "\n    " + match.group(1).upper() + " ",
        compact,
        flags=re.IGNORECASE,
    )


class BridgeClient:
    """Persistent mysqlremote client with safe read retries and channel renewal.

    A SELECT/SHOW request can be repeated after a broken bridge connection or
    an upstream HTML-instead-of-JSON response. Writes are never replayed after
    an ambiguous response, because they may already have reached MySQL.
    """
    def __init__(self, host: str = BRIDGE_HOST, port: int = BRIDGE_PORT,
                 owner: str = BRIDGE_OWNER) -> None:
        self.host, self.port, self.owner = host, int(port), owner
        self.sock: Optional[socket.socket] = None
        self.channel_id: Optional[int] = None
        self._request_lock = __import__('threading').RLock()

    def _recv_exact(self, count: int) -> bytes:
        assert self.sock is not None
        buf = bytearray()
        while len(buf) < count:
            part = self.sock.recv(count - len(buf))
            if not part:
                raise BridgeError('mysqlremote bridge closed the socket')
            buf.extend(part)
        return bytes(buf)

    def _connect(self, response_timeout: float = 120.0) -> None:
        self.sock = socket.create_connection((self.host, self.port), timeout=15)
        self.sock.settimeout(max(120.0, float(response_timeout)))

    def _close(self) -> None:
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                pass
        self.sock = None

    def request(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        body = json.dumps(payload, ensure_ascii=False, default=str).encode('utf-8')
        if len(body) > MAX_FRAME:
            raise BridgeError(f'bridge request exceeds frame limit: {len(body)} bytes')
        frame = HEADER.pack(len(body)) + body
        safe_to_replay = ('sql' not in payload) or _bridge_sql_is_read(payload.get('sql'))
        try:
            operation_timeout = float(payload.get('timeout', 90.0))
        except (TypeError, ValueError):
            operation_timeout = 90.0
        # The socket must outlive the browser/PHP query timeout. Never replay writes.
        response_timeout = max(120.0, min(600.0, operation_timeout + 60.0))
        last_error: Optional[Exception] = None
        with self._request_lock:
            for attempt in range(2):
                sent = False
                try:
                    if self.sock is None:
                        self._connect(response_timeout)
                    assert self.sock is not None
                    self.sock.settimeout(response_timeout)
                    self.sock.sendall(frame)
                    sent = True
                    (length,) = HEADER.unpack(self._recv_exact(HEADER.size))
                    if not 0 < length <= MAX_FRAME:
                        raise BridgeError(f'invalid bridge response frame length: {length}')
                    result = json.loads(self._recv_exact(length).decode('utf-8'))
                    if not isinstance(result, dict):
                        raise BridgeError('bridge returned a non-object response')
                    return result
                except Exception as exc:
                    last_error = exc
                    self._close()
                    # An interrupted write may already have completed remotely.
                    if sent and not safe_to_replay:
                        break
                    if attempt:
                        break
        raise BridgeError(f'bridge request failed: {last_error}')

    def acquire(self) -> int:
        if self.channel_id is not None:
            return self.channel_id
        reply = self.request({'op': 'acquire_channel', 'owner': self.owner})
        if reply.get('status') != 'success':
            raise BridgeError(f"acquire_channel failed: {reply.get('message')}")
        self.channel_id = int(reply['channel_id'])
        log.info('Acquired mysqlremote channel %s for %s', self.channel_id, self.owner)
        return self.channel_id

    def invalidate_channel(self) -> None:
        """Release a questionable DOM window and force a fresh channel next time."""
        channel_id, self.channel_id = self.channel_id, None
        if channel_id is not None:
            try:
                self.request({'op': 'release_channel', 'channel_id': channel_id})
            except Exception:
                pass
        self._close()

    def query(self, sql: str, params: Optional[Sequence[Any]] = None,
              timeout: float = 90.0) -> Dict[str, Any]:
        rendered_sql = interpolate_sql(sql, params)
        is_read = _bridge_sql_is_read(rendered_sql)
        delay = 1.0
        last_log = 0.0
        attempt = 0
        while True:
            attempt += 1
            try:
                request: Dict[str, Any] = {
                    'sql': rendered_sql, 'timeout': timeout,
                    'channel_id': self.acquire(),
                }
                reply = self.request(request)
            except BridgeError as exc:
                if not is_read:
                    raise
                if time.monotonic() - last_log >= 15:
                    log.warning(
                        'MySQL Remote read is temporarily unavailable (%s); '
                        'keeping %s alive and retrying in %.1fs',
                        str(exc)[:700], self.owner, delay,
                    )
                    last_log = time.monotonic()
                self.invalidate_channel()
                time.sleep(delay)
                delay = min(10.0, delay * 1.6)
                continue

            if reply.get('status') == 'success':
                return reply

            message = str(reply.get('message') or 'SQL operation failed')
            if 'unknown channel_id' in message.lower():
                # mysqlremote rejects an unknown channel before dispatching SQL,
                # so renewing and replaying this request is safe even for writes.
                self.invalidate_channel()
                continue
            if reply.get('transient') or _bridge_error_is_transient(message):
                if not is_read:
                    raise BridgeError(
                        'Transient upstream bridge response while executing a write; '
                        'the write outcome is uncertain, so it was NOT replayed. '
                        f'{message}; SQL={rendered_sql[:1200]}'
                    )
                if time.monotonic() - last_log >= 15:
                    log.warning(
                        'MySQL Remote/PHP query endpoint returned a transient error '
                        '(%s); renewing the browser channel and retrying read query '
                        'attempt %s', message[:500], attempt,
                    )
                    last_log = time.monotonic()
                self.invalidate_channel()
                time.sleep(delay)
                delay = min(10.0, delay * 1.6)
                continue

            statement_errors = []
            for index, statement in enumerate(reply.get('statement_results') or [], start=1):
                if not isinstance(statement, dict):
                    continue
                statement_status = str(statement.get('status') or '').strip().lower()
                if statement_status not in {'success', 'ok'}:
                    statement_message = str(statement.get('message') or 'statement failed').strip()
                    statement_errors.append(f'Statement {index}: {statement_message}')

            query_id = reply.get('query_id')
            error_lines = [f'Database query failed: {message}']
            if query_id:
                error_lines.append(f'Query ID: {query_id}')
            error_lines.extend(statement_errors)
            error_lines.append('SQL sent to MySQL Remote:')
            error_lines.extend('    ' + line for line in format_sql_for_console(rendered_sql).splitlines())
            raise BridgeError('\n'.join(error_lines))

    def fetch_all(self, sql: str, params: Optional[Sequence[Any]] = None,
                  timeout: float = 90.0) -> List[Dict[str, Any]]:
        result = self.query(sql, params, timeout)
        rows = result.get('results') or []
        return [normalise_row(row) for row in rows if isinstance(row, dict)]

    def execute(self, sql: str, params: Optional[Sequence[Any]] = None,
                timeout: float = 90.0) -> int:
        result = self.query(sql, params, timeout)
        return int(result.get('affected_rows') or 0)

    def close(self) -> None:
        self.invalidate_channel()


def normalise_mysql_value(value: Any) -> Any:
    """Convert MySQL Remote's serialized SQL NULL sentinel to Python None.

    PHP PDO returns SQL NULL as null. Depending on the MySQL Remote response
    path, the same value can reach this worker as the string "NULL" instead.
    Normalize that representation once, at the database boundary, so every
    downstream configuration and matcher function sees the same value PHP
    sees. Do not collapse empty strings, numeric zero, the text "0", or other
    ordinary strings into NULL here.
    """
    if isinstance(value, str) and value.strip().casefold() == "null":
        return None
    if isinstance(value, bytes):
        # The bridge normally returns JSON strings, but support byte-valued
        # adapters without changing the contents of any non-NULL byte value.
        try:
            if value.decode("utf-8", errors="strict").strip().casefold() == "null":
                return None
        except UnicodeDecodeError:
            pass
    if isinstance(value, list):
        return [normalise_mysql_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(normalise_mysql_value(item) for item in value)
    if isinstance(value, dict):
        return {key: normalise_mysql_value(item) for key, item in value.items()}
    return value


def normalise_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Match PHP PDO row values before any saved configuration is restored."""
    return {
        str(key).strip().lower(): normalise_mysql_value(value)
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
    """Parse SQL/ISO timestamps and the epoch values accepted by the chart."""
    if value is None or str(value).strip() == "":
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        raw = str(value).strip()
        if re.fullmatch(r"-?\d{10,13}(?:\.\d+)?", raw):
            try:
                numeric = float(raw)
                seconds = numeric / 1000.0 if abs(numeric) >= 1e12 else numeric
                dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
            except (ValueError, TypeError, OverflowError, OSError):
                return None
        else:
            normalized = re.sub(r"^(\d{4})\.(\d{1,2})\.(\d{1,2})(?=\s|$)", r"\1-\2-\3", raw)
            normalized = normalized.replace("T", " ")
            if normalized.endswith("Z"):
                normalized = normalized[:-1] + "+00:00"
            try:
                dt = datetime.fromisoformat(normalized)
            except ValueError:
                for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                    try:
                        dt = datetime.strptime(normalized[:26], fmt)
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


DEFAULT_TRADES_SETUP_PER_GROUP = 100


def load_trades_setup_per_group(db: BridgeClient) -> int:
    """Read setup-group size from the normalized server_operational_settings table."""
    try:
        rows = db.fetch_all(
            "SELECT `trades_setup_per_group` "
            "FROM `server_operational_settings` WHERE `id`=1 LIMIT 1",
            timeout=30,
        )
        configured = as_int(rows[0].get("trades_setup_per_group"), 0) if rows else 0
        return configured if configured is not None and configured > 0 else DEFAULT_TRADES_SETUP_PER_GROUP
    except Exception as exc:
        log.warning(
            "Could not read server_operational_settings.trades_setup_per_group; "
            "using default %s: %s",
            DEFAULT_TRADES_SETUP_PER_GROUP, str(exc)[:600],
        )
        return DEFAULT_TRADES_SETUP_PER_GROUP


def split_family_ordinal(ordinal: Any, setup_group_limit: int) -> Tuple[int, int]:
    """Map a global 1-based family ordinal to (setup_group_id, local family_number)."""
    limit = max(1, int(setup_group_limit or DEFAULT_TRADES_SETUP_PER_GROUP))
    number = max(1, as_int(ordinal, 1) or 1)
    return ((number - 1) // limit) + 1, ((number - 1) % limit) + 1


def family_group_identity(family: Dict[str, Any], setup_group_limit: int) -> Tuple[int, int, int]:
    """Return the global ordinal, chart setup_group_id, and group-local family number."""
    ordinal = as_int(family.get("familyNumber"), 0) or 0
    if ordinal <= 0:
        raise ValueError("A matched family must have a positive familyNumber before analytics persistence")
    group_id, local_number = split_family_ordinal(ordinal, setup_group_limit)
    return ordinal, group_id, local_number


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
            # Match PHP ptRestoreSavedRows(): fall back only for SQL NULL / missing.
            # Empty strings and numeric/string zero remain explicitly disabled.
            "applyToChart": (
                (lambda flag: flag is True or flag == 1 or flag == "1")(
                    template.get("apply_to_chart") if template.get("apply_to_chart") is not None
                    else template.get("run_in_python")
                )
            ),
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
    """Print the restored identities and links in a readable hierarchy."""
    root_name = _root_display_name(root)
    print(f"  Root: {root_name} (ID {root.get('id')})")
    print(f"    Timeframes: {', '.join(tf.upper() for tf in (root.get('timeframes') or [])) or '(none)'}")
    print(f"    Trade target: {root.get('tradeFor') or 'root'} | Trade enabled: {'yes' if root.get('useForTrades') else 'no'}")
    trees = sorted(root.get('trees') or [], key=lambda t: as_int(t.get('id'), 0) or 0)
    print(f"    Configured trees: {len(trees)}")
    for index, tree in enumerate(trees, start=1):
        main, ref = tree.get('main') or {}, tree.get('reference') or {}
        main_identity = main.get('selectedExistingId') or main.get('offsetTo') or main.get('id')
        ref_identity = ref.get('selectedExistingId') or ref.get('existingCandleId') or ref.get('id')
        print(
            f"      Tree {tree.get('id')} ({index}/{len(trees)}) | "
            f"mode={tree.get('groupSelectionType')} | necessity={tree.get('groupNecessity')} | "
            f"action={tree.get('ifGroupFound')}"
        )
        print(
            f"        Main: {main.get('name')!r} | ID={main.get('id')} | "
            f"identity={main_identity} | type={main.get('mainCandleType')}"
        )
        print(
            f"          position={main.get('position')} | offset={main.get('offsetPosition')} | "
            f"point={main.get('offsetPosPoint')} | price={main.get('priceLevel')} {main.get('operator')}"
        )
        print(
            f"        Reference: {ref.get('name')!r} | ID={ref.get('id')} | "
            f"identity={ref_identity} | mode={ref.get('referenceMode')} | "
            f"type={ref.get('refCandleType')}"
        )
        print(
            f"          position={ref.get('position')} | offset={ref.get('offsetPosition')} | "
            f"point={ref.get('offsetPosPoint')} | price={ref.get('priceLevel')}"
        )
    for cr in (root.get("confirmationRoots") or []):
        print(
            f"    Confirmation: {_root_display_name(cr, True)} (ID {cr.get('id')}) | "
            f"timeframes={', '.join(tf.upper() for tf in (cr.get('timeframes') or [])) or '(none)'}"
        )
        print(
            f"      Anchor: ID={cr.get('anchorCandleId')} | "
            f"name={cr.get('anchorCandleName')!r} | time={cr.get('anchorTime')} | "
            f"trees={len(cr.get('trees') or [])} | trade_enabled={'yes' if cr.get('useForTrades') else 'no'}"
        )


def print_matcher_diagnostics(root: Dict[str, Any], candle_count: int) -> None:
    """Print matcher counters as short, grouped diagnostic lines."""
    diagnostics = root.get("_matcherDiagnostics") or []
    if not diagnostics:
        print("  Matcher diagnostics: none produced (no configured trees or no candle input).")
        return
    print(f"  Matcher diagnostics: {len(diagnostics)} tree(s) | input candles={candle_count:,} | state cap={MAX_ROOT_MATCH_STATES:,}")
    for item in diagnostics:
        print(f"    Tree {item.get('tree_id')}: {item.get('main_name')} -> {item.get('reference_name')}")
        print(
            f"      Input/main: incoming={item.get('incoming_states', 0):,} | "
            f"main scanned={item.get('main_candles_scanned', 0):,} | "
            f"main type matches={item.get('main_type_matches', 0):,}"
        )
        print(
            f"      Reference: checks={item.get('reference_candidate_checks', 0):,} | "
            f"type matches={item.get('reference_type_matches', 0):,} | "
            f"type rejected={item.get('reference_type_rejections', 0):,} | "
            f"out of bounds={item.get('reference_out_of_bounds', 0):,}"
        )
        print(
            f"      Price: comparisons={item.get('price_comparisons', 0):,} | "
            f"matches={item.get('price_matches', 0):,} | "
            f"rejected={item.get('price_rejections', 0):,}"
        )
        print(
            f"      Result: retained={item.get('retained_states', 0):,} | "
            f"missing identity={item.get('identity_missing_states', 0):,} | "
            f"missing anchor={item.get('missing_anchor_states', 0):,} | "
            f"invalid window={item.get('window_invalid_states', 0):,}"
        )
        print(f"      Blocker: {item.get('blocker') or 'none'}")


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
                      account_rules: Optional[Dict[str, Any]] = None,
                      setup_group_limit: int = DEFAULT_TRADES_SETUP_PER_GROUP) -> Optional[Dict[str, Any]]:
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
    trade_state = evaluate_trade_outcome_with_breakeven(
        candles, trigger_idx, sl_price, tp_price, order, entry_price,
        (account_rules or {}).get("trades_breakeven") or [],
    )
    outcome = str(trade_state.get("outcome") or "pending entry")
    sl_trigger = trade_state.get("sl_trigger")
    tp_trigger = trade_state.get("tp_trigger")
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

    family_ordinal, setup_group_id, group_family_number = family_group_identity(
        family, setup_group_limit
    )
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
        "family_ordinal": family_ordinal,  # transient global ordinal used during staged refresh
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
        "profit_reached": int(outcome == "take profit reached" or (outcome == "breakeven" and (as_float(trade_state.get("breakeven_stop_rr"), 0.0) or 0.0) >= 0.01)),
        "pending_entry": int(outcome == "pending entry"),
        "in_trade": int(outcome == "in trade"),
        "closed_trade": int(outcome in {"stoploss hit", "take profit reached", "breakeven", "breakeven stop hit", "breakeven hit"}),
        "trade_value_r": -1.0 if outcome == "stoploss hit" else (
            max(0.0, family_rr or 0.0) if outcome == "take profit reached" else
            max(0.0, as_float(trade_state.get("breakeven_stop_rr"), 0.0) or 0.0)
            if outcome in {"breakeven", "breakeven hit"} and (as_float(trade_state.get("breakeven_stop_rr"), 0.0) or 0.0) >= 0.01 else 0.0
        ),
        "restricted_date_hit": 0,
        "restricted_day_hit": 0,
        "restricted_date_matches_json": "[]",
        "restricted_day_matches_json": "[]",
        "excluded_from_analytics": 0,
        "analytics_exclusion_reason": None,
        "breakeven_triggered": int(bool(trade_state.get("breakeven_triggered"))),
        "breakeven_floating_profit_rr": trade_state.get("breakeven_floating_profit_rr"),
        "breakeven_stop_rr": trade_state.get("breakeven_stop_rr"),
        "breakeven_stop_price": trade_state.get("breakeven_stop_price"),
        "breakeven_trigger_time": candle_stamp(trade_state.get("breakeven_trigger_index")),
        "breakeven_stop_hit": int(outcome in {"breakeven", "breakeven stop hit", "breakeven hit"}),
        "max_floating_rr": as_float(trade_state.get("max_floating_rr"), 0.0) or 0.0,
        "effective_take_profit_type": effective_tp_type,
    }
    source_main_time = _source_main_candle_time(parent_root, family, candles, parent_candles)
    apply_account_management_trade_rules(
        record, candles, trigger_idx, entry_idx, source_main_time, account_rules,
    )
    return record

def match_row(programme_id: int, user_id: int, symbol: str, timeframe: str,
              root: Dict[str, Any], family: Dict[str, Any], candles: Sequence[Dict[str, Any]],
              confirmation_root_id: int = 0, confirmation_name: str = "",
              source_timeframe: Optional[str] = None,
              setup_group_limit: int = DEFAULT_TRADES_SETUP_PER_GROUP) -> Dict[str, Any]:
    family_ordinal, setup_group_id, group_family_number = family_group_identity(
        family, setup_group_limit
    )
    return {
        "programme_id": programme_id, "userid": user_id, "symbol": symbol,
        "timeframe": timeframe.lower(), "source_timeframe": (source_timeframe or timeframe).lower(),
        "root_id": int(root["id"]), "root_name": root.get("name") or f"Root {root['id']}",
        "confirmation_root_id": int(confirmation_root_id), "confirmation_root_name": confirmation_name or None,
        "family_number": group_family_number,
        "setup_group_id": setup_group_id,
        "family_ordinal": family_ordinal,  # transient global ordinal used during staged refresh
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
        "restricted_date_hit": 0, "restricted_day_hit": 0,
        "restricted_date_matches_json": "[]", "restricted_day_matches_json": "[]",
        "excluded_from_analytics": 0, "analytics_exclusion_reason": None,
        "breakeven_triggered": 0, "breakeven_floating_profit_rr": None,
        "breakeven_stop_rr": None, "breakeven_stop_price": None,
        "breakeven_trigger_time": None, "breakeven_stop_hit": 0, "max_floating_rr": 0.0,
    }


# ------------------------------- Programme discovery ---------------------------
def _in_sql(values: Iterable[int]) -> Tuple[str, List[int]]:
    clean = sorted({int(value) for value in values if int(value) > 0})
    if not clean:
        return "", []
    return ",".join(["%s"] * len(clean)), clean


def discover_programmes(
    db: BridgeClient,
    programme_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Discover programmes without the temporary VPS/computer-IP guard.

    This test mode deliberately does not read or depend on programme_vps or
    programme_vps_hosts_followers. The optional programme_id argument remains
    available to narrow the test to one programme. Begin Test is attached only
    as a status flag and never controls programme discovery.
    """
    if programme_id is not None:
        programme_rows = db.fetch_all(
            "SELECT * FROM `programme` WHERE `id`=%s ORDER BY `id` ASC",
            [int(programme_id)], timeout=120,
        )
    else:
        programme_rows = db.fetch_all(
            "SELECT * FROM `programme` ORDER BY `id` ASC",
            timeout=180,
        )

    valid_programmes: List[Dict[str, Any]] = []
    programme_ids: List[int] = []
    programme_owners: Dict[int, int] = {}
    for programme in programme_rows:
        pid = as_int(programme.get("id"))
        uid = as_int(programme.get("userid"))
        if not pid or pid <= 0 or not uid or uid <= 0:
            log.warning(
                "Skipping programme discovery row with invalid id/userid: id=%r userid=%r",
                programme.get("id"), programme.get("userid"),
            )
            continue
        valid_programmes.append(programme)
        programme_ids.append(pid)
        programme_owners[pid] = uid

    latest_interest: Dict[Tuple[int, int], Dict[str, Any]] = {}
    if programme_ids and table_exists(db, "signals_provider_interest"):
        marks, params = _in_sql(programme_ids)
        interest_rows = db.fetch_all(
            "SELECT * FROM `signals_provider_interest` "
            f"WHERE `programme_id` IN ({marks}) ORDER BY `id` DESC",
            params, timeout=180,
        )
        for row in interest_rows:
            pid = as_int(row.get("programme_id"))
            uid = as_int(row.get("user_id"))
            if pid and uid and (pid, uid) not in latest_interest:
                latest_interest[(pid, uid)] = row

    for programme in valid_programmes:
        pid = as_int(programme.get("id")) or 0
        uid = as_int(programme.get("userid")) or 0
        interest = latest_interest.get((pid, uid), {})
        programme["begin_test_interest_id"] = interest.get("id")
        programme["begin_test_flag"] = as_int(interest.get("begin_test"), 0) or 0
        programme["begin_test_status"] = interest.get("interest_status") or "not started"
        programme["begin_test_started_at"] = interest.get("signal_testing_started_at")

    log.info(
        "Unrestricted test programme discovery: programmes=%s%s",
        len(valid_programmes),
        f" (programme filter {programme_id})" if programme_id is not None else "",
    )
    return valid_programmes

# ------------------------------- DB operations --------------------------------
ANALYTICS_COLUMNS = [
    "programme_id", "userid", "symbol", "timeframe", "source_timeframe", "root_id", "root_name",
    "confirmation_root_id", "confirmation_root_name", "family_number", "setup_group_id", "rebuild_batch_token", "family_signature", "family_map_json",
    "match_json", "matched_family", "trade_configured", "live_trades", "order_type", "direction", "entry_name", "stoploss_name",
    "take_profit_name", "entry_anchor_time", "entry_trigger_time", "stoploss_time", "take_profit_time",
    "stoploss_trigger_time", "take_profit_trigger_time", "event_time", "entry_price", "stoploss_price",
    "take_profit_price", "configured_take_profit_price", "risk_reward", "configured_risk_reward",
    "minimum_risk_reward_applied", "outcome", "stoploss_hit", "profit_reached", "pending_entry", "in_trade",
    "closed_trade", "trade_value_r", "restricted_date_hit", "restricted_day_hit",
    "restricted_date_matches_json", "restricted_day_matches_json", "excluded_from_analytics",
    "analytics_exclusion_reason", "breakeven_triggered", "breakeven_floating_profit_rr",
    "breakeven_stop_rr", "breakeven_stop_price", "breakeven_trigger_time",
    "breakeven_stop_hit", "max_floating_rr",
]

ANALYTICS_ADDITIVE_COLUMNS = {
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
                 symbol: str, timeframe: str, total_limit: int) -> List[Dict[str, Any]]:
    """Fetch only the newest configured number of candles for one exact pair.

    Rows are requested newest-first from candle_records, then reversed so the
    completed list is chronological, oldest candle first, matching PT_CANDLES.
    The total is supplied by server_account.chart_total_candles; there is no
    local default or hardcoded processing cap.
    """
    if total_limit <= 0:
        return []
    sql = (
        "SELECT id,candle_time,open_time,close_time,open,high,low,`close`, "
        "candle_center,body_center,high_wick_center,low_wick_center, "
        "candle_width_center,volume "
        "FROM `candle_records` WHERE `programmeid`=%s AND `userid`=%s "
        "AND `symbol`=%s AND `timeframe`=%s "
        f"ORDER BY `candle_time` DESC LIMIT {int(total_limit)}"
    )
    newest_first = db.fetch_all(
        sql, [programme_id, user_id, symbol, timeframe], timeout=120
    )
    candles: List[Dict[str, Any]] = []
    for row in reversed(newest_first):
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

def resolve_account_management_developer_id(db: BridgeClient, user_id: int) -> int:
    """Resolve Account Management ownership by HarvHub email, with legacy ID fallback.

    Account Management tables key rows by developers.id. It is unsafe to assume
    developers.id always equals harvhub.id, so mirror the Signals Dashboard lookup.
    """
    if user_id <= 0:
        return user_id
    try:
        if not table_exists(db, "harvhub") or not table_exists(db, "developers"):
            return user_id
        harvhub_columns = {str(column).lower() for column in get_columns(db, "harvhub")}
        developer_columns = {str(column).lower() for column in get_columns(db, "developers")}
        if not {"id", "email"}.issubset(harvhub_columns) or not {"id", "email"}.issubset(developer_columns):
            return user_id
        owner_rows = db.fetch_all(
            "SELECT `email` FROM `harvhub` WHERE `id`=%s LIMIT 1", [user_id], timeout=30
        )
        email = str(owner_rows[0].get("email") or "").strip() if owner_rows else ""
        if not email:
            return user_id
        developer_rows = db.fetch_all(
            "SELECT `id` FROM `developers` WHERE LOWER(`email`)=LOWER(%s) LIMIT 1",
            [email], timeout=30,
        )
        developer_id = as_int(developer_rows[0].get("id"), 0) if developer_rows else 0
        return developer_id if developer_id and developer_id > 0 else user_id
    except Exception as exc:
        log.warning("Account Management developer lookup failed; using legacy HarvHub ID %s: %s", user_id, str(exc)[:500])
        return user_id


def load_account_management_rules(db: BridgeClient, programme: Dict[str, Any]) -> Dict[str, Any]:
    """Load the same programme-scoped Account Management rules as programme_training.php."""
    programme_id = as_int(programme.get("id"), 0) or 0
    user_id = as_int(programme.get("userid"), 0) or 0
    developer_id = resolve_account_management_developer_id(db, user_id)
    timezone_name = str(programme.get("timezone") or "").strip()
    rules: Dict[str, Any] = {
        "timezone": timezone_name or "UTC",
        "restricted_dates": [],
        "restricted_days": [],
        "trades_breakeven": [],
    }
    rules["developer_id"] = developer_id
    if programme_id <= 0 or developer_id <= 0:
        return rules

    table_specs = {
        "restricted_dates": ("accountmanagement_restricted_dates", ["restricted_date"]),
        "restricted_days": ("accountmanagement_restricted_days", ["day", "from_time", "to_time"]),
        "trades_breakeven": ("accountmanagement_trades_breakeven", ["floating_profit_at_risk_reward", "breakeven_at_risk_reward"]),
    }
    for rule_key, (table, wanted_fields) in table_specs.items():
        try:
            if not table_exists(db, table):
                continue
            columns = {str(column).lower() for column in get_columns(db, table)}
            if not {"developerid", "programme_id"}.issubset(columns):
                log.warning("Account Management table %s is missing developerid/programme_id; skipping %s rules", table, rule_key)
                continue
            selected = [field for field in wanted_fields if field.lower() in columns]
            if selected:
                fields_sql = ",".join(f"`{field}`" for field in selected)
                rules[rule_key] = db.fetch_all(
                    f"SELECT {fields_sql} FROM `{table}` WHERE `developerid`=%s AND `programme_id`=%s ORDER BY `id` ASC",
                    [developer_id, programme_id], timeout=60,
                )
            if not timezone_name and "timezone" in columns:
                timezone_rows = db.fetch_all(
                    f"SELECT `timezone` FROM `{table}` WHERE `developerid`=%s AND `programme_id`=%s "
                    "AND `timezone` IS NOT NULL AND `timezone`<>'' ORDER BY `id` ASC LIMIT 1",
                    [developer_id, programme_id], timeout=30,
                )
                if timezone_rows:
                    candidate = str(timezone_rows[0].get("timezone") or "").strip()
                    if candidate:
                        timezone_name = candidate
        except Exception as exc:
            log.warning("Account Management %s rules could not be loaded: %s", rule_key, str(exc)[:700])

    try:
        ZoneInfo(timezone_name or "UTC")
        rules["timezone"] = timezone_name or "UTC"
    except (ZoneInfoNotFoundError, ValueError):
        log.warning("Invalid programme timezone %r; restricted-window rules will use UTC", timezone_name)
        rules["timezone"] = "UTC"
    return rules


def parse_account_risk_reward(value: Any) -> Optional[float]:
    """Mirror the browser's ptParseRiskReward() for Account Management values."""
    if value is None or str(value).strip() == "":
        return None
    raw = str(value).strip()
    pair = re.fullmatch(r"\s*([+-]?(?:\d+(?:\.\d*)?|\.\d+))\s*:\s*([+-]?(?:\d+(?:\.\d*)?|\.\d+))\s*", raw)
    if pair:
        left, right = as_float(pair.group(1)), as_float(pair.group(2))
        return right / left if left not in (None, 0) and right is not None else None
    return as_float(re.sub(r"R$", "", raw, flags=re.IGNORECASE).strip())


def evaluate_trade_outcome_with_breakeven(
    candles: Sequence[Dict[str, Any]], trigger: Optional[int],
    sl_price: Optional[float], tp_price: Optional[float], order_type: str,
    entry_price: Optional[float], breakeven_rules: Optional[Sequence[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Mirror programme_training.php ptEvaluateTradeOutcome() candle by candle."""
    result: Dict[str, Any] = {
        "outcome": "pending entry", "sl_trigger": None, "tp_trigger": None,
        "breakeven_triggered": False, "breakeven_floating_profit_rr": None,
        "breakeven_stop_rr": None, "breakeven_stop_price": None,
        "breakeven_trigger_index": None, "max_floating_rr": 0.0,
    }
    if trigger is None or sl_price is None or tp_price is None or entry_price is None:
        return result
    if not (0 <= int(trigger) < len(candles)):
        return result
    risk_distance = abs(float(entry_price) - float(sl_price))
    if not (risk_distance > 0):
        return result

    normalized_rules: List[Dict[str, float]] = []
    for rule in breakeven_rules or []:
        floating = parse_account_risk_reward(rule.get("floating_profit_at_risk_reward"))
        stop_rr = parse_account_risk_reward(rule.get("breakeven_at_risk_reward"))
        if floating is None or stop_rr is None or floating <= 0:
            continue
        normalized_rules.append({"floating_rr": floating, "stop_rr": max(0.0, stop_rr)})
    normalized_rules.sort(key=lambda rule: (rule["floating_rr"], rule["stop_rr"]))

    is_long = normalize_order(order_type).startswith("buy")
    active_stop = float(sl_price)
    next_rule_index = 0
    applied_rule: Optional[Dict[str, float]] = None
    result["outcome"] = "in trade"

    for index in range(int(trigger), len(candles)):
        candle = candles[index]
        high, low = as_float(candle.get("high")), as_float(candle.get("low"))
        if high is None or low is None:
            continue
        favourable_move = (high - float(entry_price)) if is_long else (float(entry_price) - low)
        floating_rr = favourable_move / risk_distance
        if floating_rr == floating_rr and abs(floating_rr) != float("inf"):
            result["max_floating_rr"] = max(float(result["max_floating_rr"]), floating_rr)

        # Only rules activated on completed earlier bars govern this bar.
        stop_touched = low <= active_stop if is_long else high >= active_stop
        target_touched = high >= float(tp_price) if is_long else low <= float(tp_price)
        if stop_touched:
            if result["breakeven_triggered"]:
                # A locked stop at >= +0.01R is a profitable breakeven exit.
                # A true 0R stop is separately identified as a breakeven stop hit.
                locked_rr = as_float(result.get("breakeven_stop_rr"), 0.0) or 0.0
                result["outcome"] = "breakeven" if locked_rr >= 0.01 else "breakeven stop hit"
            else:
                result["outcome"] = "stoploss hit"
            result["sl_trigger"] = index
            break
        if target_touched:
            # The TP candle can also cross a BE threshold; retain that audit data.
            while next_rule_index < len(normalized_rules) and floating_rr >= normalized_rules[next_rule_index]["floating_rr"]:
                rule = normalized_rules[next_rule_index]
                next_rule_index += 1
                moved_stop = float(entry_price) + (risk_distance * rule["stop_rr"] if is_long else -risk_distance * rule["stop_rr"])
                if applied_rule is None or (moved_stop > active_stop if is_long else moved_stop < active_stop):
                    active_stop = moved_stop
                    applied_rule = rule
                    result["breakeven_triggered"] = True
                    result["breakeven_floating_profit_rr"] = rule["floating_rr"]
                    result["breakeven_stop_rr"] = rule["stop_rr"]
                    result["breakeven_stop_price"] = active_stop
                    result["breakeven_trigger_index"] = index
            result["outcome"] = "take profit reached"
            result["tp_trigger"] = index
            break

        # Newly reached levels activate only for subsequent candles.
        while next_rule_index < len(normalized_rules) and floating_rr >= normalized_rules[next_rule_index]["floating_rr"]:
            rule = normalized_rules[next_rule_index]
            next_rule_index += 1
            moved_stop = float(entry_price) + (risk_distance * rule["stop_rr"] if is_long else -risk_distance * rule["stop_rr"])
            if applied_rule is None or (moved_stop > active_stop if is_long else moved_stop < active_stop):
                active_stop = moved_stop
                applied_rule = rule
                result["breakeven_triggered"] = True
                result["breakeven_floating_profit_rr"] = rule["floating_rr"]
                result["breakeven_stop_rr"] = rule["stop_rr"]
                result["breakeven_stop_price"] = active_stop
                result["breakeven_trigger_index"] = index
    return result


def _programme_datetime_parts(raw_value: Any, timezone_name: str) -> Optional[Dict[str, Any]]:
    """Mirror ptProgrammeDateParts(): naive SQL times are programme-local."""
    if raw_value is None or str(raw_value).strip() == "":
        return None
    try:
        zone = ZoneInfo(timezone_name or "UTC")
    except (ZoneInfoNotFoundError, ValueError):
        zone = timezone.utc

    raw_text = str(raw_value).strip()
    try:
        if re.fullmatch(r"-?\d{10,13}(?:\.\d+)?", raw_text):
            numeric = float(raw_text)
            epoch_seconds = numeric / 1000.0 if abs(numeric) >= 1e12 else numeric
            dt = datetime.fromtimestamp(epoch_seconds, tz=zone)
        else:
            normalized = re.sub(r"^(\d{4})\.(\d{1,2})\.(\d{1,2})(?=\s|$)", r"\1-\2-\3", raw_text)
            naive_match = re.fullmatch(
                r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})(?:[ T](\d{1,2}):(\d{2})(?::(\d{2})(?:\.\d+)?)?)?",
                normalized,
            )
            if naive_match:
                year, month, day, hour, minute, second = naive_match.groups()
                # A timezone-less SQL/terminal timestamp is already programme-local.
                dt = datetime(int(year), int(month), int(day), int(hour or 0), int(minute or 0), int(second or 0))
            else:
                iso_text = normalized.replace("T", " ")
                if iso_text.endswith("Z"):
                    iso_text = iso_text[:-1] + "+00:00"
                dt = datetime.fromisoformat(iso_text)
                if dt.tzinfo is not None:
                    dt = dt.astimezone(zone).replace(tzinfo=None)
    except (ValueError, TypeError, OverflowError, OSError):
        return None
    return {
        "date": dt.strftime("%Y-%m-%d"),
        "weekday_index": (dt.weekday() + 1) % 7,  # JS: Sunday=0
        "time_minutes": dt.hour * 60 + dt.minute + dt.second / 60.0,
    }


def _rule_time_minutes(value: Any) -> Optional[float]:
    match = re.fullmatch(r"\s*(\d{1,2}):(\d{2})(?::(\d{2}))?\s*", str(value or ""))
    if not match:
        return None
    hour, minute, second = int(match.group(1)), int(match.group(2)), int(match.group(3) or 0)
    if hour > 23 or minute > 59 or second > 59:
        return None
    return hour * 60 + minute + second / 60.0


def _source_main_candle_time(
    parent_root: Dict[str, Any], family: Dict[str, Any],
    candles: Sequence[Dict[str, Any]], parent_candles: Optional[Sequence[Dict[str, Any]]],
) -> Optional[Any]:
    """Resolve the same original parent-root main candle used by chart rule checks."""
    source_family = family.get("parentFamily") or family
    source_candles = parent_candles if parent_candles is not None else candles
    trees = sorted(parent_root.get("trees") or [], key=lambda tree: as_int(tree.get("id"), 0) or 0)
    if not trees:
        return None
    tree = trees[0]
    main = tree.get("main") or {}
    identity = main.get("selectedExistingId") or main.get("offsetTo") or main.get("id")
    conditions = source_family.get("conditions") or []
    for condition in conditions:
        if (str(condition.get("treeId")) == str(tree.get("id"))
                and str(condition.get("rootId", parent_root.get("id"))) == str(parent_root.get("id"))
                and condition.get("mainCandleId") is not None):
            candle_id = str(condition.get("mainCandleId"))
            for candle in source_candles:
                if candle.get("id") is not None and str(candle.get("id")) == candle_id:
                    return candle.get("time") or candle.get("open_time") or candle.get("timestamp")
    if identity not in (None, ""):
        index = as_int((source_family.get("map") or {}).get(str(identity)))
        if index is not None and 0 <= index < len(source_candles):
            candle = source_candles[index]
            return candle.get("time") or candle.get("open_time") or candle.get("timestamp")
    return None


def apply_account_management_trade_rules(
    record: Dict[str, Any], candles: Sequence[Dict[str, Any]],
    trigger_index: Optional[int], entry_index: Optional[int],
    source_main_candle_time: Any, rules: Optional[Dict[str, Any]],
) -> None:
    """Apply programme_training.php restricted-date/day matching to a trade row."""
    rule_data = rules or {}
    timezone_name = str(rule_data.get("timezone") or "UTC")
    candidates: List[Dict[str, Any]] = []

    def add_time(raw: Any, basis: str) -> None:
        parts = _programme_datetime_parts(raw, timezone_name)
        if parts is None:
            return
        key = parts["date"] + "|" + str(parts["time_minutes"])
        if any(item["key"] == key for item in candidates):
            return
        candidates.append({"key": key, "raw": str(raw), "basis": basis, "parts": parts})

    def add_candle_index(index: Optional[int], basis: str) -> None:
        if index is None:
            return
        numeric_index = as_int(index)
        if numeric_index is None or not (0 <= numeric_index < len(candles)):
            return
        candle = candles[numeric_index]
        add_time(candle.get("time") or candle.get("open_time") or candle.get("timestamp"), basis)

    # Candle-native values are authoritative. SQL-normalized record timestamps
    # are fallbacks only, because converting an offset timestamp to DATETIME can
    # otherwise discard its original programme-timezone context.
    add_candle_index(trigger_index, "entry trigger")
    if not candidates:
        add_time(record.get("entry_trigger_time"), "entry trigger")
    add_candle_index(entry_index, "Entry-at anchor")
    if len(candidates) < 2:
        add_time(record.get("entry_anchor_time"), "Entry-at anchor")
    add_time(source_main_candle_time, "source-root identification candle")

    primary = next((item for item in candidates if item["basis"].startswith("entry trigger")), None)
    if primary is None:
        primary = next((item for item in candidates if item["basis"].startswith("entry execution")), None)
    if primary is None and candidates:
        primary = candidates[0]
    record["trade_entry_date"] = primary["parts"]["date"] if primary else None

    restricted_date_matches: List[Dict[str, Any]] = []
    for rule in rule_data.get("restricted_dates") or []:
        restricted_date = str(rule.get("restricted_date") or "").strip()[:10]
        if not restricted_date:
            continue
        match = next((item for item in candidates if item["parts"]["date"] == restricted_date), None)
        if match:
            restricted_date_matches.append({
                "date": restricted_date, "matchedAt": match["raw"], "matchBasis": match["basis"],
            })

    weekday_indexes = {"sunday": 0, "monday": 1, "tuesday": 2, "wednesday": 3,
                       "thursday": 4, "friday": 5, "saturday": 6}
    restricted_day_matches: List[Dict[str, Any]] = []
    for rule in rule_data.get("restricted_days") or []:
        configured_day = str(rule.get("day") or "").strip()
        configured_index = weekday_indexes.get(configured_day.lower())
        from_minutes = _rule_time_minutes(rule.get("from_time"))
        to_minutes = _rule_time_minutes(rule.get("to_time"))
        if configured_index is None or from_minutes is None or to_minutes is None:
            continue
        matched_time = None
        for candidate in candidates:
            parts = candidate["parts"]
            weekday = parts["weekday_index"]
            minute = parts["time_minutes"]
            if from_minutes == to_minutes:
                matched = weekday == configured_index
            elif from_minutes < to_minutes:
                matched = weekday == configured_index and from_minutes <= minute <= to_minutes
            else:
                following_day = (configured_index + 1) % 7
                matched = ((weekday == configured_index and minute >= from_minutes)
                           or (weekday == following_day and minute <= to_minutes))
            if matched:
                matched_time = candidate
                break
        if matched_time:
            restricted_day_matches.append({
                "day": configured_day,
                "from_time": str(rule.get("from_time") or ""),
                "to_time": str(rule.get("to_time") or ""),
                "matchedDate": matched_time["parts"]["date"],
                "matchedAt": matched_time["raw"],
                "matchBasis": matched_time["basis"],
            })

    record["restricted_date_hit"] = int(bool(restricted_date_matches))
    record["restricted_day_hit"] = int(bool(restricted_day_matches))
    record["restricted_date_matches_json"] = safe_json(restricted_date_matches)
    record["restricted_day_matches_json"] = safe_json(restricted_day_matches)
    record["excluded_from_analytics"] = int(bool(restricted_date_matches or restricted_day_matches))
    record["analytics_exclusion_reason"] = "restricted trading window" if record["excluded_from_analytics"] else None


def _analytics_identity(record: Dict[str, Any]) -> Dict[str, Any]:
    """Stable chart-family identity, including its setup group."""
    return {
        "programme_id": record.get("programme_id"),
        "symbol": record.get("symbol"),
        "timeframe": record.get("timeframe"),
        "source_timeframe": record.get("source_timeframe"),
        "root_id": record.get("root_id"),
        "confirmation_root_id": record.get("confirmation_root_id") or 0,
        "setup_group_id": record.get("setup_group_id"),
        "family_number": record.get("family_number"),
    }


ANALYTICS_IDENTITY_COLUMNS = [
    "programme_id", "symbol", "timeframe", "source_timeframe", "root_id",
    "confirmation_root_id", "setup_group_id", "family_number",
]


ANALYTICS_INTEGER_COLUMNS = {
    "programme_id", "userid", "root_id", "confirmation_root_id", "family_number",
    "setup_group_id", "matched_family", "trade_configured", "live_trades",
    "minimum_risk_reward_applied", "stoploss_hit", "profit_reached", "pending_entry",
    "in_trade", "closed_trade", "restricted_date_hit", "restricted_day_hit",
    "excluded_from_analytics", "breakeven_triggered", "breakeven_stop_hit",
}
ANALYTICS_DECIMAL_COLUMNS = {
    "entry_price", "stoploss_price", "take_profit_price", "configured_take_profit_price",
    "risk_reward", "configured_risk_reward", "trade_value_r",
    "breakeven_floating_profit_rr", "breakeven_stop_rr", "breakeven_stop_price",
    "max_floating_rr",
}
# Populated from information_schema for the active programme_analytics table.
# Comparisons must use the precision MariaDB actually stores, not Python's
# binary-float representation of the calculation result.
ANALYTICS_DECIMAL_SCALES: Dict[str, int] = {}
ANALYTICS_NUMERIC_TYPES: Dict[str, str] = {}
ANALYTICS_NUMERIC_PRECISIONS: Dict[str, Optional[int]] = {}
ANALYTICS_JSON_COLUMNS = {
    "family_map_json", "match_json", "restricted_date_matches_json",
    "restricted_day_matches_json",
}
ANALYTICS_TIME_COLUMNS = {
    "entry_anchor_time", "entry_trigger_time", "stoploss_time", "take_profit_time",
    "stoploss_trigger_time", "take_profit_trigger_time", "event_time",
    "breakeven_trigger_time",
}


def _analytics_identity_key(record: Dict[str, Any]) -> Tuple[Any, ...]:
    """Canonical, user-scoped chart-family key matching PHP's group identity."""
    source_tf = record.get("source_timeframe")
    if isinstance(source_tf, bytes):
        source_tf = source_tf.decode("utf-8", errors="replace")
    source_tf = None if source_tf is None else str(source_tf).strip().casefold()
    symbol = str(record.get("symbol") or "").strip().casefold()
    timeframe = str(record.get("timeframe") or "").strip().casefold()
    return (
        as_int(record.get("userid"), 0) or 0,
        as_int(record.get("programme_id"), 0) or 0,
        symbol,
        timeframe,
        source_tf,
        as_int(record.get("root_id"), 0) or 0,
        as_int(record.get("confirmation_root_id"), 0) or 0,
        as_int(record.get("setup_group_id"), 1) or 1,
        as_int(record.get("family_number"), 0) or 0,
    )


def load_analytics_decimal_metadata(db: BridgeClient) -> Dict[str, int]:
    """Load real DECIMAL scales from MariaDB before any analytics comparison.

    Python may calculate values such as 0.9497499999999998 while a DECIMAL(20,10)
    column stores 0.9497500000. We must compare at the database column scale.
    The metadata is refreshed for each programme run so schema changes are picked
    up without hardcoded precision assumptions.
    """
    global ANALYTICS_DECIMAL_SCALES, ANALYTICS_NUMERIC_TYPES, ANALYTICS_NUMERIC_PRECISIONS

    wanted = sorted(ANALYTICS_DECIMAL_COLUMNS)
    placeholders = ",".join(["%s"] * len(wanted))
    rows = db.fetch_all(
        "SELECT `COLUMN_NAME` AS `column_name`, `DATA_TYPE` AS `data_type`, "
        "`NUMERIC_PRECISION` AS `numeric_precision`, `NUMERIC_SCALE` AS `numeric_scale` "
        "FROM `information_schema`.`COLUMNS` "
        "WHERE `TABLE_SCHEMA`=DATABASE() AND `TABLE_NAME`='programme_analytics' "
        f"AND `COLUMN_NAME` IN ({placeholders})",
        wanted,
        timeout=60,
    )

    numeric_types: Dict[str, str] = {}
    numeric_precisions: Dict[str, Optional[int]] = {}
    decimal_scales: Dict[str, int] = {}
    for row in rows:
        column = str(row.get("column_name") or "").strip().lower()
        data_type = str(row.get("data_type") or "").strip().lower()
        if not column:
            continue
        numeric_types[column] = data_type
        numeric_precisions[column] = as_int(row.get("numeric_precision"), None)
        if data_type in {"decimal", "numeric"}:
            scale = as_int(row.get("numeric_scale"), None)
            if scale is None or scale < 0:
                raise RuntimeError(
                    f"MariaDB returned no valid NUMERIC_SCALE for programme_analytics.{column}; "
                    "refusing unsafe floating-point comparison."
                )
            decimal_scales[column] = scale

    returned = set(numeric_types)
    missing = sorted(set(wanted) - returned)
    if missing:
        raise RuntimeError(
            "Could not load numeric column metadata for programme_analytics: "
            + ", ".join(missing)
            + ". Refusing analytics reconciliation because decimal precision is unknown."
        )

    ANALYTICS_DECIMAL_SCALES = decimal_scales
    ANALYTICS_NUMERIC_TYPES = numeric_types
    ANALYTICS_NUMERIC_PRECISIONS = numeric_precisions

    decimal_summary = ", ".join(
        f"{column}={scale}" for column, scale in sorted(decimal_scales.items())
    )
    non_decimal = sorted(
        column for column, data_type in numeric_types.items()
        if data_type not in {"decimal", "numeric"}
    )
    print(
        "  programme_analytics numeric precision loaded from information_schema: "
        f"DECIMAL scales [{decimal_summary or 'none'}]"
        + (f" | non-DECIMAL numeric columns [{', '.join(non_decimal)}]" if non_decimal else "")
    )
    return dict(decimal_scales)


def _canonical_analytics_value(column: str, value: Any) -> Any:
    """Compare values at the actual MariaDB column precision.

    DECIMAL values are quantized to information_schema.NUMERIC_SCALE using
    MariaDB-compatible half-away-from-zero rounding (ROUND_HALF_UP). This removes
    harmless Python float tails while still detecting differences at stored scale.
    """
    value = normalise_mysql_value(value)
    if value is None:
        return 0 if column == "confirmation_root_id" else None
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    if column in ANALYTICS_INTEGER_COLUMNS:
        parsed = as_int(value)
        return parsed if parsed is not None else str(value).strip()
    if column in ANALYTICS_DECIMAL_COLUMNS:
        try:
            number = Decimal(str(value).strip())
            if not number.is_finite():
                return str(value).strip()
            scale = ANALYTICS_DECIMAL_SCALES.get(column)
            if scale is not None:
                quantum = Decimal(1).scaleb(-scale)
                precision = ANALYTICS_NUMERIC_PRECISIONS.get(column) or 0
                digits = len(number.as_tuple().digits)
                with localcontext() as context:
                    context.prec = max(38, precision + scale + 4, digits + scale + 4)
                    return number.quantize(quantum, rounding=ROUND_HALF_UP)
            # FLOAT/DOUBLE columns do not expose a DECIMAL scale. Keep them
            # numeric and normalize exponent/trailing-zero representation.
            return number.normalize()
        except (InvalidOperation, ValueError, TypeError):
            return str(value).strip()
    if column in ANALYTICS_JSON_COLUMNS:
        if isinstance(value, (dict, list)):
            parsed = value
        else:
            raw = str(value).strip()
            try:
                parsed = json.loads(raw)
            except (ValueError, TypeError):
                return raw
        return json.dumps(parsed, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    if column in ANALYTICS_TIME_COLUMNS:
        normalized = dt_text(value)
        return normalized if normalized is not None else str(value).strip()
    if column in {"symbol", "timeframe", "source_timeframe"}:
        return str(value).strip().casefold()
    return str(value) if not isinstance(value, (int, float, bool)) else value


def analytics_records_equal(existing: Dict[str, Any], expected: Dict[str, Any]) -> bool:
    """True only when every stored analytics field already equals this scan's result."""
    return all(
        _canonical_analytics_value(column, existing.get(column))
        == _canonical_analytics_value(column, expected.get(column))
        for column in ANALYTICS_COLUMNS
    )


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
    """Return unique indexes by name and their lower-case column sets."""
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


def prepare_analytics_rebuild_state(
    db: BridgeClient, programme_id: int, user_id: int, setup_group_limit: int
) -> Dict[str, Any]:
    """Capture every old analytics row and its group identity without deleting it.

    The snapshot is used for incremental reconciliation: exact matches are left
    untouched, changed identities are updated in place, and genuinely new
    identities are inserted. Only old rows absent from the verified result set
    are eligible for deletion at the end.
    """
    if not table_exists(db, "programme_analytics"):
        raise RuntimeError("Required table programme_analytics does not exist; refusing to start a fresh rebuild.")

    limit = max(1, int(setup_group_limit or DEFAULT_TRADES_SETUP_PER_GROUP))
    columns = {column.lower() for column in get_columns(db, "programme_analytics")}
    if "family_number" not in columns:
        raise RuntimeError("programme_analytics is missing required column family_number")

    group_column_added = False
    if "setup_group_id" not in columns:
        db.execute(
            "ALTER TABLE `programme_analytics` ADD COLUMN `setup_group_id` "
            "BIGINT(20) UNSIGNED NOT NULL DEFAULT 1 AFTER `family_number`",
            timeout=90,
        )
        columns.add("setup_group_id")
        group_column_added = True
        print("  Added programme_analytics.setup_group_id for chart-compatible setup grouping.")

    if "rebuild_batch_token" not in columns:
        db.execute(
            "ALTER TABLE `programme_analytics` ADD COLUMN `rebuild_batch_token` "
            "VARCHAR(64) NULL DEFAULT NULL AFTER `setup_group_id`",
            timeout=90,
        )
        columns.add("rebuild_batch_token")
        print("  Added nullable programme_analytics.rebuild_batch_token for interrupted-refresh cleanup.")

    for column, definition in ANALYTICS_ADDITIVE_COLUMNS.items():
        if column.lower() not in columns:
            db.execute(
                f"ALTER TABLE `programme_analytics` ADD COLUMN `{column}` {definition}",
                timeout=90,
            )
            columns.add(column.lower())
            print(f"  Added programme_analytics.{column} compatibility column.")
    if "live_trades" not in columns:
        position = " AFTER `trade_configured`" if "trade_configured" in columns else ""
        db.execute(
            "ALTER TABLE `programme_analytics` ADD COLUMN `live_trades` TINYINT(1) NOT NULL DEFAULT 0" + position,
            timeout=90,
        )
        columns.add("live_trades")
        print("  Added programme_analytics.live_trades compatibility column.")
    missing_columns = [column for column in ANALYTICS_COLUMNS if column.lower() not in columns]
    if missing_columns:
        raise RuntimeError(
            "programme_analytics is missing required column(s): " + ", ".join(missing_columns)
        )
    if "id" not in columns:
        raise RuntimeError(
            "programme_analytics.id is required for safe row-level incremental cleanup; no rows were changed."
        )

    # Resolve actual precision before loading/comparing existing analytics.
    # If metadata cannot be read, stop before writing rather than misclassifying
    # exact database values as changed or accepting genuinely different values.
    load_analytics_decimal_metadata(db)

    # Remove only obsolete family unique indexes that omit setup_group_id. They
    # would reject a valid family_number restart in a later group.
    old_identity = {column for column in ANALYTICS_IDENTITY_COLUMNS if column != "setup_group_id"}
    index_definitions = _unique_index_definitions(db, "programme_analytics")
    for index_name, index_columns in index_definitions.items():
        if (
            index_name.lower() != "primary"
            and "setup_group_id" not in index_columns
            and old_identity.issubset(index_columns)
        ):
            if not re.fullmatch(r"[A-Za-z0-9_$]+", index_name):
                raise RuntimeError(f"Unsafe analytics index name returned by database: {index_name!r}")
            db.execute(f"ALTER TABLE `programme_analytics` DROP INDEX `{index_name}`", timeout=90)
            print(f"  Replaced legacy analytics family index: {index_name}")

    # Legacy rows with globally increasing family_number need one-time conversion
    # into the same group/local numbering used by PHP. Rows carrying an old batch
    # token are not rewritten here; the later comparison will retain them only if
    # their identity/payload belongs to the current calculated dataset.
    legacy_rows = 0
    if group_column_added:
        legacy_rows = 1
    else:
        check_rows = db.fetch_all(
            "SELECT COUNT(*) AS `legacy_rows` FROM `programme_analytics` "
            "WHERE `programme_id`=%s AND `userid`=%s AND `rebuild_batch_token` IS NULL "
            "AND `family_number`>%s",
            [programme_id, user_id, limit], timeout=60,
        )
        legacy_rows = as_int(check_rows[0].get("legacy_rows"), 0) if check_rows else 0
    if legacy_rows:
        db.execute(
            "UPDATE `programme_analytics` SET `setup_group_id`=FLOOR((`family_number`-1)/%s)+1 "
            "WHERE `programme_id`=%s AND `userid`=%s AND `rebuild_batch_token` IS NULL "
            "AND `family_number`>%s",
            [limit, programme_id, user_id, limit], timeout=120,
        )
        db.execute(
            "UPDATE `programme_analytics` SET `family_number`=MOD(`family_number`-1,%s)+1 "
            "WHERE `programme_id`=%s AND `userid`=%s AND `rebuild_batch_token` IS NULL "
            "AND `family_number`>%s",
            [limit, programme_id, user_id, limit], timeout=120,
        )
        print("  Migrated legacy global family numbers into setup_group_id + group-local family_number.")

    # Ensure the unique identity includes the setup group, as the PHP analytics
    # contract does. Fail closed if it cannot be installed; do not delete any rows.
    index_definitions = _unique_index_definitions(db, "programme_analytics")
    wanted = {column.lower() for column in ANALYTICS_IDENTITY_COLUMNS}
    if not any(index_columns == wanted for index_columns in index_definitions.values()):
        base_name = "uq_programme_analytics_family_group"
        candidate = base_name
        suffix = 2
        while candidate in index_definitions:
            candidate = f"{base_name}_{suffix}"
            suffix += 1
        db.execute(
            f"CREATE UNIQUE INDEX `{candidate}` ON `programme_analytics` "
            "(`programme_id`,`symbol`,`timeframe`,`source_timeframe`,`root_id`,"
            "`confirmation_root_id`,`setup_group_id`,`family_number`)",
            timeout=120,
        )
        print(f"  Verified group-aware analytics identity index: {candidate}")

    group_rows = db.fetch_all(
        "SELECT `symbol`,`timeframe`,`root_id`,COALESCE(`confirmation_root_id`,0) AS `confirmation_root_id`, "
        "COALESCE(`source_timeframe`,'') AS `source_timeframe`,`setup_group_id`,COUNT(*) AS `row_count`, "
        "MIN(`family_number`) AS `min_family_number`,MAX(`family_number`) AS `max_family_number` "
        "FROM `programme_analytics` WHERE `programme_id`=%s AND `userid`=%s "
        "GROUP BY `symbol`,`timeframe`,`root_id`,COALESCE(`confirmation_root_id`,0),"
        "COALESCE(`source_timeframe`,''),`setup_group_id` "
        "ORDER BY `symbol`,`timeframe`,`root_id`,COALESCE(`confirmation_root_id`,0),`setup_group_id`",
        [programme_id, user_id], timeout=90,
    )
    highwater_rows = db.fetch_all(
        "SELECT COUNT(*) AS `row_count`,COALESCE(MAX(`setup_group_id`),0) AS `max_group_id`, "
        "COALESCE(MAX((`setup_group_id`-1)*%s+`family_number`),0) AS `max_global_ordinal`, "
        "COALESCE(MAX(`family_number`),0) AS `max_family_number`, "
        "SUM(CASE WHEN `rebuild_batch_token` IS NOT NULL THEN 1 ELSE 0 END) AS `leftover_staged_rows` "
        "FROM `programme_analytics` WHERE `programme_id`=%s AND `userid`=%s",
        [limit, programme_id, user_id], timeout=60,
    )
    highwater = highwater_rows[0] if highwater_rows else {}
    old_row_count = as_int(highwater.get("row_count"), 0) or 0
    old_max_group_id = as_int(highwater.get("max_group_id"), 0) or 0
    old_max_global_ordinal = as_int(highwater.get("max_global_ordinal"), 0) or 0
    old_max_family_number = as_int(highwater.get("max_family_number"), 0) or 0
    leftover_staged_rows = as_int(highwater.get("leftover_staged_rows"), 0) or 0

    # Capture each original row, including its primary key and complete analytics
    # payload. This lets the next scan retain exact matches without writing them.
    existing_select_columns = ["id"] + ANALYTICS_COLUMNS
    existing_records = db.fetch_all(
        "SELECT " + ",".join(f"`{column}`" for column in existing_select_columns) +
        " FROM `programme_analytics` WHERE `programme_id`=%s AND `userid`=%s ORDER BY `id`",
        [programme_id, user_id], timeout=120,
    )
    if len(existing_records) != old_row_count:
        raise RuntimeError(
            "Could not capture a consistent programme_analytics snapshot: "
            f"count query returned {old_row_count:,}, row snapshot returned {len(existing_records):,}. "
            "No analytics rows will be deleted."
        )
    existing_by_identity: Dict[Tuple[Any, ...], List[Dict[str, Any]]] = {}
    for existing_record in existing_records:
        existing_by_identity.setdefault(_analytics_identity_key(existing_record), []).append(existing_record)

    print("\nEXISTING ANALYTICS GROUP SNAPSHOT (captured before incremental reconciliation)")
    if not group_rows:
        print("  No existing rows; the first fresh dataset will start at setup group 1, family 1.")
    else:
        print("  Symbol/TF | Root | Confirmation | Group | Rows | Family range")
        for row in group_rows:
            print(
                f"  {str(row.get('symbol') or '')} {str(row.get('timeframe') or '').upper()} | "
                f"{row.get('root_id')} | {row.get('confirmation_root_id')} | "
                f"{row.get('setup_group_id')} | {as_int(row.get('row_count'),0) or 0:,} | "
                f"{row.get('min_family_number')}–{row.get('max_family_number')}"
            )
        print(f"  Existing analytics rows: {old_row_count:,}; highest group: {old_max_group_id:,}; "
              f"highest global family ordinal: {old_max_global_ordinal:,}.")
    if leftover_staged_rows:
        print(f"  Rows carrying a leftover refresh marker: {leftover_staged_rows:,}; "
              "they will be compared with this scan and removed only if obsolete after verification.")

    return {
        "setup_group_limit": limit,
        "existing_groups": group_rows,
        "existing_rows": old_row_count,
        "old_max_group_id": old_max_group_id,
        "old_max_global_ordinal": old_max_global_ordinal,
        "existing_records": existing_records,
        "existing_by_identity": existing_by_identity,
        "kept_existing_ids": set(),
        "desired_records": {},
    }


ANALYTICS_INSERT_BATCH_SIZE = 50
ANALYTICS_UPDATE_BATCH_SIZE = 20


def _analytics_bulk_parallel_limit(db: BridgeClient) -> int:
    """Read the Selenium bridge's bounded async-channel execution limit."""
    cached = getattr(db, "_analytics_bulk_parallel_limit", None)
    if cached is not None:
        return int(cached)
    limit = 1
    try:
        status = db.request({"op": "bridge_status"})
        transport = str(status.get("transport") or "").lower()
        safe_selenium_channels = (
            transport == "selenium_webdriver"
            and status.get("webdriver_io_serialized") is True
        )
        if safe_selenium_channels:
            configured = as_int(status.get("max_parallel_sql_channels"), 1) or 1
            limit = max(1, min(4, configured))
    except Exception:
        # Older bridge versions remain compatible but are restricted to one
        # request at a time because they do not advertise safe channel dispatch.
        limit = 1
    setattr(db, "_analytics_bulk_parallel_limit", limit)
    return limit


def _run_analytics_bulk_jobs(
    db: BridgeClient, jobs: Sequence[Tuple[str, Sequence[Any]]], label: str,
) -> None:
    """Run independent SQL batches across bounded Selenium DOM channels.

    Each worker owns one persistent mysqlremote channel/window. Selenium
    commands are serialized by the bridge, while each window's JavaScript fetch
    continues asynchronously, allowing up to the bridge's configured number of
    independent PHP/MySQL requests to be in flight at once.
    """
    if not jobs:
        return

    worker_count = min(_analytics_bulk_parallel_limit(db), len(jobs))
    if worker_count <= 1:
        print(
            f"    {label.replace('_', ' ').title()}: {len(jobs)} SQL batch(es) "
            "through one persistent Selenium channel"
        )
        for batch_number, (sql, params) in enumerate(jobs, start=1):
            reply = db.query(sql, params, timeout=180)
            affected = as_int(reply.get("affected_rows"), 0) or 0
            query_id = reply.get("query_id") or "not returned"
            print(
                f"      {label.replace('_', ' ')} batch {batch_number:,}/{len(jobs):,} "
                f"SQL succeeded | query_id={query_id} | affected_rows={affected:,}"
            )
        return

    # Assign multiple batches to each worker so a Selenium DOM window is
    # created once and reused, rather than opening/closing a window per batch.
    worker_jobs: List[List[Tuple[int, Tuple[str, Sequence[Any]]]]] = [
        [] for _ in range(worker_count)
    ]
    for index, job in enumerate(jobs):
        worker_jobs[index % worker_count].append((index + 1, job))

    print(
        f"    {label.replace('_', ' ').title()}: {len(jobs)} SQL batch(es) "
        f"across {worker_count} Selenium channels (browser fetches async)"
    )

    def run_worker(worker_index: int, assigned: Sequence[Tuple[int, Tuple[str, Sequence[Any]]]]) -> None:
        client = BridgeClient(
            host=db.host,
            port=db.port,
            owner=f"{db.owner}_{label}_{worker_index + 1}",
        )
        try:
            total = len(assigned)
            for local_index, (_global_index, (sql, params)) in enumerate(assigned, start=1):
                reply = client.query(sql, params, timeout=180)
                affected = as_int(reply.get("affected_rows"), 0) or 0
                query_id = reply.get("query_id") or "not returned"
                print(
                    f"      {label.replace('_', ' ')} channel {worker_index + 1}: "
                    f"batch {local_index:,}/{total:,} SQL succeeded | "
                    f"query_id={query_id} | affected_rows={affected:,}"
                )
        finally:
            client.close()

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = [
            executor.submit(run_worker, worker_index, assigned)
            for worker_index, assigned in enumerate(worker_jobs)
            if assigned
        ]
        # Resolve all workers. A failure stops verification/deletion; already
        # committed rows are detected by the next scan's stable identity check.
        for future in futures:
            future.result()


def _flush_analytics_insert_batches(
    db: BridgeClient, pending: Sequence[Dict[str, Any]], columns: Sequence[str],
    programme_id: int, user_id: int,
) -> int:
    """Insert new analytics identities with multi-row INSERT batches via Selenium."""
    del programme_id, user_id  # already embedded in each prepared record
    jobs: List[Tuple[str, Sequence[Any]]] = []
    insert_columns_sql = ",".join(f"`{column}`" for column in columns)
    row_marks = "(" + ",".join(["%s"] * len(columns)) + ")"
    for offset in range(0, len(pending), ANALYTICS_INSERT_BATCH_SIZE):
        chunk = pending[offset:offset + ANALYTICS_INSERT_BATCH_SIZE]
        values_sql = ",".join(row_marks for _ in chunk)
        params: List[Any] = []
        for record in chunk:
            params.extend(record.get(column) for column in columns)
        sql = (
            f"INSERT INTO `programme_analytics` ({insert_columns_sql}) "
            f"VALUES {values_sql}"
        )
        jobs.append((sql, params))
    _run_analytics_bulk_jobs(db, jobs, "analytics_insert")
    print(f"    Analytics bulk insert complete: {len(pending):,} new row(s)")
    return len(pending)

def _flush_analytics_update_batches(
    db: BridgeClient,
    pending: Sequence[Tuple[int, Dict[str, Any], Sequence[str]]],
    columns: Sequence[str], programme_id: int, user_id: int,
) -> int:
    """Update changed analytics rows in CASE/WHEN batches via Selenium."""
    jobs: List[Tuple[str, Sequence[Any]]] = []
    excluded = {"programme_id", "userid"}
    for offset in range(0, len(pending), ANALYTICS_UPDATE_BATCH_SIZE):
        chunk = pending[offset:offset + ANALYTICS_UPDATE_BATCH_SIZE]
        set_parts: List[str] = []
        params: List[Any] = []
        changed_by_row = {
            row_id: set(changed_columns)
            for row_id, _record, changed_columns in chunk
        }
        rows_by_id = {row_id: record for row_id, record, _changed in chunk}

        for column in columns:
            if column in excluded:
                continue
            pairs = [
                (row_id, rows_by_id[row_id].get(column))
                for row_id, changed in changed_by_row.items()
                if column in changed
            ]
            if not pairs:
                continue
            case_sql = "CASE `id` " + " ".join("WHEN %s THEN %s" for _ in pairs)
            case_sql += f" ELSE `{column}` END"
            set_parts.append(f"`{column}`={case_sql}")
            for row_id, value in pairs:
                params.extend([row_id, value])

        if not set_parts:
            continue

        ids = [row_id for row_id, _record, _changed in chunk]
        where_marks = ",".join(["%s"] * len(ids))
        params.extend(ids)
        params.extend([programme_id, user_id])
        sql = (
            "UPDATE `programme_analytics` SET " + ",".join(set_parts) +
            f" WHERE `id` IN ({where_marks}) "
            "AND `programme_id`=%s AND `userid`=%s"
        )
        jobs.append((sql, params))

    _run_analytics_bulk_jobs(db, jobs, "analytics_update")
    print(f"    Analytics bulk update complete: {len(pending):,} changed row(s)")
    return len(pending)

def persist_pair(
    db: BridgeClient, programme_id: int, user_id: int, symbol: str, timeframe: str,
    records: List[Dict[str, Any]], rebuild_state: Dict[str, Any],
    setup_group_limit: int = DEFAULT_TRADES_SETUP_PER_GROUP,
) -> Dict[str, int]:
    """Incrementally reconcile analytics, preserving exact rows and batching writes.

    Identical identity + identical payload => keep the exact old row untouched.
    Existing identity + changed payload => update the same row in a CASE batch.
    Missing identity => insert only that row in a multi-row INSERT batch.
    Old rows are deleted only after the complete desired data has been verified.
    """
    counters = {"inserted": 0, "updated": 0, "unchanged": 0}
    existing_by_identity = rebuild_state["existing_by_identity"]
    kept_ids = rebuild_state["kept_existing_ids"]
    desired_records = rebuild_state["desired_records"]
    columns = ANALYTICS_COLUMNS
    pending_inserts: List[Dict[str, Any]] = []
    pending_updates: List[Tuple[int, Dict[str, Any], Sequence[str]]] = []

    for source_record in _deduplicate_records(records):
        record = {column: source_record.get(column) for column in columns}
        record["programme_id"] = int(programme_id)
        record["userid"] = int(user_id)
        record["symbol"] = str(record.get("symbol") or symbol)
        record["timeframe"] = str(record.get("timeframe") or timeframe).lower()
        record["live_trades"] = 1 if as_int(record.get("live_trades"), 0) == 1 else 0
        record["rebuild_batch_token"] = None

        group_id = as_int(record.get("setup_group_id"), 0) or 0
        family_number = as_int(record.get("family_number"), 0) or 0
        if group_id <= 0 or family_number <= 0 or family_number > max(1, int(setup_group_limit)):
            ordinal = as_int(source_record.get("family_ordinal"), 0) or 0
            if ordinal <= 0:
                raise RuntimeError(
                    f"{symbol} {timeframe.upper()}: analytics row has invalid setup-group identity "
                    f"(group={group_id}, family={family_number}, ordinal={ordinal})."
                )
            group_id = ((ordinal - 1) // max(1, int(setup_group_limit))) + 1
            family_number = ((ordinal - 1) % max(1, int(setup_group_limit))) + 1
            record["setup_group_id"] = group_id
            record["family_number"] = family_number

        key = _analytics_identity_key(record)
        if key in desired_records:
            if analytics_records_equal(desired_records[key], record):
                counters["unchanged"] += 1
                continue
            raise RuntimeError(
                f"Duplicate calculated analytics identity in {symbol} {timeframe.upper()}: {key!r}"
            )
        desired_records[key] = dict(record)

        candidates = existing_by_identity.get(key, [])
        identical = next(
            (row for row in candidates if analytics_records_equal(row, record)), None
        )
        if identical is not None:
            old_id = as_int(identical.get("id"), 0) or 0
            if old_id <= 0:
                raise RuntimeError("Existing identical analytics row has no valid primary key id.")
            kept_ids.add(old_id)
            counters["unchanged"] += 1
            continue

        if candidates:
            target = min(
                candidates,
                key=lambda row: as_int(row.get("id"), 2**63 - 1) or 2**63 - 1,
            )
            old_id = as_int(target.get("id"), 0) or 0
            if old_id <= 0:
                raise RuntimeError(
                    "Existing analytics row has no valid primary key id; refusing duplicate insert."
                )
            changed_columns = [
                column for column in columns
                if column not in {"programme_id", "userid"}
                and _canonical_analytics_value(column, target.get(column))
                != _canonical_analytics_value(column, record.get(column))
            ]
            if changed_columns:
                pending_updates.append((old_id, dict(record), changed_columns))
                counters["updated"] += 1
            else:
                counters["unchanged"] += 1
            kept_ids.add(old_id)
            target.update(record)
            target["id"] = old_id
        else:
            pending_inserts.append(dict(record))
            counters["inserted"] += 1

    # Each bulk statement is submitted as one SQL operation through the
    # Selenium-controlled PHP interface. Writes are never replayed
    # automatically after an ambiguous browser/transport failure; the next
    # scan safely recognises any rows that did reach MySQL.
    if pending_updates:
        _flush_analytics_update_batches(
            db, pending_updates, columns, programme_id, user_id
        )
    if pending_inserts:
        _flush_analytics_insert_batches(
            db, pending_inserts, columns, programme_id, user_id
        )

    return counters

def _analytics_diagnostic_value(value: Any, max_length: int = 180) -> str:
    """Compact, unambiguous representation of a database value for console diagnostics."""
    value = normalise_mysql_value(value)
    if value is None:
        rendered = "NULL"
    elif isinstance(value, (dict, list, tuple)):
        try:
            rendered = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
        except Exception:
            rendered = repr(value)
    elif isinstance(value, str):
        rendered = repr(value)
    else:
        rendered = repr(value)
    if len(rendered) > max_length:
        rendered = rendered[:max_length - 3] + "..."
    return rendered


def _analytics_record_differences(
    actual: Dict[str, Any], expected: Dict[str, Any]
) -> List[Tuple[str, Any, Any]]:
    """Return field-level (column, expected, actual) differences after canonicalization."""
    differences: List[Tuple[str, Any, Any]] = []
    for column in ANALYTICS_COLUMNS:
        expected_value = expected.get(column)
        actual_value = actual.get(column)
        if _canonical_analytics_value(column, actual_value) != _canonical_analytics_value(column, expected_value):
            differences.append((column, expected_value, actual_value))
    return differences


def verify_incremental_analytics_dataset(
    db: BridgeClient, programme_id: int, user_id: int,
    desired_records: Dict[Tuple[Any, ...], Dict[str, Any]],
) -> int:
    """Verify every desired analytics identity and print exact field differences."""
    columns = ["id"] + ANALYTICS_COLUMNS
    current_rows = db.fetch_all(
        "SELECT " + ",".join(f"`{column}`" for column in columns) +
        " FROM `programme_analytics` WHERE `programme_id`=%s AND `userid`=%s ORDER BY `id`",
        [programme_id, user_id], timeout=120,
    )
    current_by_identity: Dict[Tuple[Any, ...], List[Dict[str, Any]]] = {}
    for row in current_rows:
        current_by_identity.setdefault(_analytics_identity_key(row), []).append(row)

    missing: List[Tuple[Any, ...]] = []
    mismatched: List[Tuple[Any, ...]] = []
    mismatch_column_counts: Dict[str, int] = {}
    mismatch_column_examples: Dict[str, Tuple[Tuple[Any, ...], int, Any, Any]] = {}
    mismatch_samples: List[Tuple[Tuple[Any, ...], int, List[Tuple[str, Any, Any]], int]] = []
    duplicate_candidate_identities = 0

    for key, expected in desired_records.items():
        candidates = current_by_identity.get(key, [])
        if not candidates:
            missing.append(key)
            continue
        if len(candidates) > 1:
            duplicate_candidate_identities += 1
        candidate_differences = [
            (row, _analytics_record_differences(row, expected)) for row in candidates
        ]
        exact = next((row for row, differences in candidate_differences if not differences), None)
        if exact is not None:
            continue

        mismatched.append(key)
        # Pick the closest persisted candidate so diagnostics describe the
        # smallest actionable difference, even when legacy duplicate identities exist.
        best_row, best_differences = min(
            candidate_differences,
            key=lambda item: (len(item[1]), as_int(item[0].get("id"), 2**63 - 1) or 2**63 - 1),
        )
        row_id = as_int(best_row.get("id"), 0) or 0
        mismatch_samples.append((key, row_id, best_differences, len(candidates)))
        for column, expected_value, actual_value in best_differences:
            mismatch_column_counts[column] = mismatch_column_counts.get(column, 0) + 1
            mismatch_column_examples.setdefault(
                column, (key, row_id, expected_value, actual_value)
            )

    print(
        f"  Incremental analytics verification: expected={len(desired_records):,} | "
        f"database rows read={len(current_rows):,} | identities found={len(current_by_identity):,} | "
        f"missing={len(missing):,} | payload mismatches={len(mismatched):,}"
    )
    if duplicate_candidate_identities:
        print(
            f"  Verification note: {duplicate_candidate_identities:,} desired identity/identities "
            "have multiple database rows; closest candidate shown in diagnostics."
        )

    if missing or mismatched:
        print("  ANALYTICS VERIFICATION DIAGNOSTICS — exact stored-vs-expected differences")
        if missing:
            print(f"    Missing identity count: {len(missing):,}")
            for key in missing[:10]:
                print(f"      MISSING identity={key!r}")
            if len(missing) > 10:
                print(f"      ... {len(missing) - 10:,} additional missing identities omitted")

        if mismatched:
            print(
                f"    Mismatched identity count: {len(mismatched):,}; "
                f"distinct mismatched columns: {len(mismatch_column_counts):,}"
            )
            for column, count in sorted(
                mismatch_column_counts.items(), key=lambda item: (-item[1], item[0])
            ):
                key, row_id, expected_value, actual_value = mismatch_column_examples[column]
                print(
                    f"      COLUMN `{column}` differs in {count:,}/{len(mismatched):,} rows | "
                    f"sample row_id={row_id} | expected={_analytics_diagnostic_value(expected_value)} | "
                    f"database={_analytics_diagnostic_value(actual_value)} | identity={key!r}"
                )

            print("    Representative mismatched records:")
            for key, row_id, differences, candidate_count in mismatch_samples[:5]:
                print(
                    f"      row_id={row_id} | identity={key!r} | "
                    f"same-identity database rows={candidate_count} | "
                    f"different columns={len(differences)}"
                )
                for column, expected_value, actual_value in differences[:20]:
                    print(
                        f"        `{column}`: expected={_analytics_diagnostic_value(expected_value)}; "
                        f"database={_analytics_diagnostic_value(actual_value)}"
                    )
                if len(differences) > 20:
                    print(f"        ... {len(differences) - 20:,} additional differing columns omitted")
            if len(mismatch_samples) > 5:
                print(f"      ... {len(mismatch_samples) - 5:,} additional mismatched records omitted")

        raise RuntimeError(
            "Incremental analytics verification failed. Existing analytics have NOT been deleted. "
            f"Missing={len(missing):,}, mismatched={len(mismatched):,}. "
            "See ANALYTICS VERIFICATION DIAGNOSTICS above for exact columns and values."
        )
    return len(desired_records)


def delete_obsolete_analytics_rows(
    db: BridgeClient, programme_id: int, user_id: int,
    existing_records: Sequence[Dict[str, Any]], kept_existing_ids: set,
) -> int:
    """Delete only captured old rows not represented by the fully verified dataset."""
    obsolete_ids = sorted({
        as_int(row.get("id"), 0) or 0
        for row in existing_records
        if (as_int(row.get("id"), 0) or 0) > 0
        and (as_int(row.get("id"), 0) or 0) not in kept_existing_ids
    })
    if not obsolete_ids:
        print("  Obsolete old analytics rows: 0 — every existing row is still represented; nothing to delete.")
        return 0

    deleted = 0
    for offset in range(0, len(obsolete_ids), 100):
        chunk = obsolete_ids[offset:offset + 100]
        marks = ",".join(["%s"] * len(chunk))
        deleted += db.execute(
            "DELETE FROM `programme_analytics` WHERE `programme_id`=%s AND `userid`=%s "
            f"AND `id` IN ({marks})",
            [programme_id, user_id, *chunk], timeout=120,
        )
    remaining_count = 0
    for offset in range(0, len(obsolete_ids), 100):
        chunk = obsolete_ids[offset:offset + 100]
        marks = ",".join(["%s"] * len(chunk))
        remaining = db.fetch_all(
            "SELECT COUNT(*) AS `remaining_obsolete` FROM `programme_analytics` "
            f"WHERE `programme_id`=%s AND `userid`=%s AND `id` IN ({marks})",
            [programme_id, user_id, *chunk], timeout=90,
        )
        remaining_count += as_int(remaining[0].get("remaining_obsolete"), 0) if remaining else 0
    if (remaining_count or 0) != 0:
        raise RuntimeError(
            f"Obsolete analytics cleanup incomplete: {remaining_count} old row(s) remain. "
            "All verified current analytics rows have been preserved."
        )
    return deleted


def verify_final_analytics_row_count(
    db: BridgeClient, programme_id: int, user_id: int, expected_rows: int,
) -> int:
    rows = db.fetch_all(
        "SELECT COUNT(*) AS `row_count` FROM `programme_analytics` "
        "WHERE `programme_id`=%s AND `userid`=%s",
        [programme_id, user_id], timeout=90,
    )
    actual = as_int(rows[0].get("row_count"), 0) if rows else 0
    actual = max(0, actual or 0)
    if actual != expected_rows:
        raise RuntimeError(
            f"Final incremental analytics count mismatch: expected={expected_rows:,}, found={actual:,}."
        )
    print(f"  Final analytics verification: {actual:,} row(s) match the current calculated dataset.")
    return actual



# ---------------------- Programme Trades first-stage sync -------------------
PROGRAMME_TRADE_REQUEST_COLUMNS = {
    "order_request_status": "VARCHAR(64) NULL DEFAULT NULL",
    "order_request_filled": "TINYINT(1) NOT NULL DEFAULT 0",
    "order_request_filled_failure_reason": "VARCHAR(500) NULL DEFAULT NULL",
}


def _first_record_value(record: Dict[str, Any], names: Sequence[str]) -> Any:
    """PHP pa_firstRowValue parity: zero and False are valid populated values."""
    for name in names:
        if name in record and record[name] is not None and record[name] != "":
            return record[name]
    return None


def _resolve_programme_trade_timeframe(
    record: Dict[str, Any], roots: Sequence[Dict[str, Any]]
) -> str:
    """Mirror programme_analytics.php pa_resolveChartTimeframe()."""
    root_id = as_int(record.get("root_id"), 0) or 0
    confirmation_id = as_int(record.get("confirmation_root_id"), 0) or 0
    root = next((item for item in roots if (as_int(item.get("id"), 0) or 0) == root_id), None)
    configured: List[str] = []
    if confirmation_id > 0 and root:
        for confirmation in root.get("confirmationRoots") or []:
            if (as_int(confirmation.get("id"), 0) or 0) == confirmation_id:
                configured = [str(value).strip().lower() for value in (confirmation.get("timeframes") or [])]
                break
    elif root:
        configured = [str(value).strip().lower() for value in (root.get("timeframes") or [])]

    candidates = [
        str(record.get("timeframe") or "").strip().lower(),
        str(record.get("source_timeframe") or "").strip().lower(),
    ]
    for candidate in candidates:
        if candidate and candidate in configured:
            return candidate
    if confirmation_id > 0:
        return candidates[1] or candidates[0]
    return candidates[0] or candidates[1]


def _schema_column_exists(db: BridgeClient, table: str, column: str) -> bool:
    rows = db.fetch_all(
        "SELECT COUNT(*) AS `n` FROM `information_schema`.`COLUMNS` "
        "WHERE `TABLE_SCHEMA`=DATABASE() AND `TABLE_NAME`=" + sql_literal(table) +
        " AND `COLUMN_NAME`=" + sql_literal(column),
        timeout=60,
    )
    return bool(rows and (as_int(rows[0].get("n"), 0) or 0) > 0)


def _schema_index_columns(db: BridgeClient, table: str, index_name: str) -> Optional[Tuple[str, ...]]:
    rows = db.fetch_all(f"SHOW INDEX FROM `{table}`", timeout=90)
    found = [
        row for row in rows
        if str(row.get("key_name") or "").strip().lower() == index_name.lower()
    ]
    if not found:
        return None
    found.sort(key=lambda row: as_int(row.get("seq_in_index"), 0) or 0)
    return tuple(str(row.get("column_name") or "").strip().lower() for row in found)


def _execute_ddl_and_verify(
    db: BridgeClient,
    sql: str,
    description: str,
    verifier,
    timeout: float = 240,
) -> None:
    """Run DDL once, then verify server state even if Selenium lost its response.

    A browser AbortController can lose the HTTP response after MariaDB commits
    DDL. Do not blindly replay that write: read the resulting schema and accept
    it only when the requested change is demonstrably present.
    """
    write_error: Optional[Exception] = None
    try:
        db.execute(sql, timeout=timeout)
    except Exception as exc:
        write_error = exc

    try:
        applied = bool(verifier())
    except Exception as verify_exc:
        if write_error is not None:
            raise RuntimeError(
                f"{description}: DDL response was lost and schema verification also failed: "
                f"{verify_exc}. Original DDL error: {write_error}"
            ) from write_error
        raise RuntimeError(f"{description}: could not verify database schema: {verify_exc}") from verify_exc

    if applied:
        if write_error is not None:
            print(
                f"  Verified {description}: MariaDB applied the change despite the lost "
                "Selenium/PHP response; the DDL was NOT replayed."
            )
        return

    if write_error is not None:
        raise RuntimeError(
            f"{description}: DDL failed and the requested schema change is not present. "
            f"The write was not replayed. Original error: {write_error}"
        ) from write_error
    raise RuntimeError(f"{description}: SQL returned but the expected schema change is absent.")


def ensure_programme_trade_sync_schema(db: BridgeClient) -> None:
    """Ensure trade-sync schema, tolerating a lost DDL acknowledgement safely."""
    if not table_exists(db, "programme_trades"):
        raise RuntimeError(
            "Required table programme_trades does not exist; refusing to continue to analytics writes."
        )

    # One SHOW COLUMNS lookup replaces repeated per-column metadata requests.
    trade_columns = {column.lower() for column in get_columns(db, "programme_trades")}
    additions = [
        ("contract_id", "BIGINT(20) UNSIGNED NOT NULL DEFAULT 1", " AFTER `programmeid`"),
        ("setup_group_id", "BIGINT(20) UNSIGNED NOT NULL DEFAULT 1", " AFTER `contract_id`"),
        ("chart_identity_hash", "CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL DEFAULT NULL", " AFTER `tradeid`"),
        ("live", "TINYINT(1) NOT NULL DEFAULT 0 COMMENT '1=Live, 0=History'", " AFTER `tradeid`"),
        ("trade_time", "DATETIME NULL DEFAULT NULL", " AFTER `live`"),
    ]
    for column, definition, position in additions:
        if column not in trade_columns:
            _execute_ddl_and_verify(
                db,
                f"ALTER TABLE `programme_trades` ADD COLUMN `{column}` {definition}{position}",
                f"programme_trades.{column}",
                lambda column=column: _schema_column_exists(db, "programme_trades", column),
                timeout=240,
            )
            trade_columns.add(column)
            print(f"  Added/verified programme_trades.{column} compatibility column.")

    for column, definition in PROGRAMME_TRADE_REQUEST_COLUMNS.items():
        if column not in trade_columns:
            _execute_ddl_and_verify(
                db,
                f"ALTER TABLE `programme_trades` ADD COLUMN `{column}` {definition}",
                f"programme_trades.{column}",
                lambda column=column: _schema_column_exists(db, "programme_trades", column),
                timeout=240,
            )
            trade_columns.add(column)
            print(f"  Added/verified programme_trades.{column} compatibility column.")

    required_trade_columns = {
        "id", "userid", "programmeid", "contract_id", "setup_group_id", "investor_id",
        "tradeid", "chart_identity_hash", "symbol", "ordertype", "orderstatus", "timeframe", "entry", "stoploss",
        "takeprofit", "trade_risk_reward", "pnl", "pnl_risk_reward", "trade_status", "live",
        "trade_time", "order_request_status", "order_request_filled",
        "order_request_filled_failure_reason",
    }
    actual_trade_columns = trade_columns
    missing = sorted(required_trade_columns - actual_trade_columns)
    if missing:
        raise RuntimeError(
            "programme_trades is missing required PHP-compatible column(s): " + ", ".join(missing)
        )

    # Optional lookup index: verify post-write state if the Selenium response is lost.
    try:
        expected_contract_index = ("userid", "programmeid", "contract_id")
        actual_contract_index = _schema_index_columns(db, "programme_trades", "idx_programme_trades_contract")
        if actual_contract_index is None:
            _execute_ddl_and_verify(
                db,
                "ALTER TABLE `programme_trades` ADD KEY `idx_programme_trades_contract` "
                "(`userid`,`programmeid`,`contract_id`)",
                "programme_trades.idx_programme_trades_contract",
                lambda: _schema_index_columns(db, "programme_trades", "idx_programme_trades_contract")
                    == expected_contract_index,
                timeout=240,
            )
        elif actual_contract_index != expected_contract_index:
            raise RuntimeError(f"existing contract index has unexpected columns: {actual_contract_index}")
    except Exception as exc:
        log.warning("  Programme trades contract lookup index warning: %s", str(exc).splitlines()[0][:500])

    # Required unique index for safe batch insert + exact generated-ID lookup.
    expected_hash_index = ("userid", "programmeid", "chart_identity_hash")
    actual_hash_index = _schema_index_columns(db, "programme_trades", "uniq_programme_trades_chart_hash")
    if actual_hash_index is None:
        _execute_ddl_and_verify(
            db,
            "ALTER TABLE `programme_trades` ADD UNIQUE KEY `uniq_programme_trades_chart_hash` "
            "(`userid`,`programmeid`,`chart_identity_hash`)",
            "programme_trades.uniq_programme_trades_chart_hash",
            lambda: _schema_index_columns(db, "programme_trades", "uniq_programme_trades_chart_hash")
                == expected_hash_index,
            timeout=300,
        )
    elif actual_hash_index != expected_hash_index:
        raise RuntimeError(
            "programme_trades.uniq_programme_trades_chart_hash exists with unexpected columns: "
            f"{actual_hash_index}; expected {expected_hash_index}."
        )

    identity_table_sql = (
        "CREATE TABLE IF NOT EXISTS `programme_trade_chart_identity` ("
        "`programme_trade_id` BIGINT(20) UNSIGNED NOT NULL,"
        "`userid` BIGINT(20) UNSIGNED NOT NULL,"
        "`programmeid` BIGINT(20) UNSIGNED NOT NULL,"
        "`contract_id` BIGINT(20) UNSIGNED NOT NULL DEFAULT 1,"
        "`identity_hash` CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,"
        "`chart_trade_id` VARCHAR(255) NULL DEFAULT NULL,"
        "`root_id` BIGINT(20) UNSIGNED NULL DEFAULT NULL,"
        "`confirmation_root_id` BIGINT(20) UNSIGNED NULL DEFAULT NULL,"
        "`family_number` BIGINT(20) UNSIGNED NULL DEFAULT NULL,"
        "`setup_group_id` BIGINT(20) UNSIGNED NOT NULL DEFAULT 1,"
        "`family_signature` TEXT NULL,"
        "`symbol` VARCHAR(64) NOT NULL,"
        "`timeframe` VARCHAR(32) NOT NULL,"
        "`source_timeframe` VARCHAR(32) NULL DEFAULT NULL,"
        "`entry_anchor_time` VARCHAR(40) NULL DEFAULT NULL,"
        "`entry_trigger_time` VARCHAR(40) NULL DEFAULT NULL,"
        "`outcome` VARCHAR(64) NULL DEFAULT NULL,"
        "`created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,"
        "`updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
        "PRIMARY KEY (`programme_trade_id`),"
        "UNIQUE KEY `uniq_programme_trade_chart_identity` (`userid`,`programmeid`,`identity_hash`),"
        "KEY `idx_programme_trade_chart_root` (`userid`,`programmeid`,`root_id`,`confirmation_root_id`),"
        "KEY `idx_programme_trade_chart_contract` (`userid`,`programmeid`,`contract_id`)"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
    )
    if not table_exists(db, "programme_trade_chart_identity"):
        _execute_ddl_and_verify(
            db,
            identity_table_sql,
            "programme_trade_chart_identity table",
            lambda: table_exists(db, "programme_trade_chart_identity"),
            timeout=300,
        )

    identity_columns = {column.lower() for column in get_columns(db, "programme_trade_chart_identity")}
    if "setup_group_id" not in identity_columns:
        _execute_ddl_and_verify(
            db,
            "ALTER TABLE `programme_trade_chart_identity` ADD COLUMN `setup_group_id` "
            "BIGINT(20) UNSIGNED NOT NULL DEFAULT 1 AFTER `family_number`",
            "programme_trade_chart_identity.setup_group_id",
            lambda: _schema_column_exists(db, "programme_trade_chart_identity", "setup_group_id"),
            timeout=240,
        )
        identity_columns.add("setup_group_id")

    required_identity_columns = {
        "programme_trade_id", "userid", "programmeid", "contract_id", "identity_hash",
        "chart_trade_id", "root_id", "confirmation_root_id", "family_number", "setup_group_id",
        "family_signature", "symbol", "timeframe", "source_timeframe", "entry_anchor_time",
        "entry_trigger_time", "outcome",
    }
    missing_identity = sorted(required_identity_columns - identity_columns)
    if missing_identity:
        raise RuntimeError(
            "programme_trade_chart_identity is missing required column(s): "
            + ", ".join(missing_identity)
        )


def _latest_programme_contract_id(
    db: BridgeClient, programme_id: int, user_id: int
) -> int:
    latest = 0
    if table_exists(db, "programme"):
        programme_columns = {column.lower() for column in get_columns(db, "programme")}
        if "contract_id" in programme_columns:
            rows = db.fetch_all(
                "SELECT `contract_id` FROM `programme` WHERE `id`=%s AND `userid`=%s LIMIT 1",
                [programme_id, user_id], timeout=45,
            )
            if rows:
                latest = max(latest, as_int(rows[0].get("contract_id"), 0) or 0)
    rows = db.fetch_all(
        "SELECT COALESCE(MAX(`contract_id`),0) AS `latest_contract_id` "
        "FROM `programme_trades` WHERE `userid`=%s AND `programmeid`=%s",
        [user_id, programme_id], timeout=60,
    )
    if rows:
        latest = max(latest, as_int(rows[0].get("latest_contract_id"), 0) or 0)
    return latest if latest > 0 else 1


def _normalise_trade_outcome_for_programme_trade(outcome: Any) -> str:
    value = str(outcome or "").strip().lower()
    value = re.sub(r"[\s_-]+", " ", value)
    return value


def _build_programme_trade_sync_row(
    record: Dict[str, Any], roots: Sequence[Dict[str, Any]], contract_id: int
) -> Optional[Dict[str, Any]]:
    """Prepare one canonical chart trade using the PHP pa_syncProgrammeTrades rules."""
    if as_int(record.get("trade_configured"), 0) != 1:
        return None
    if "matched_family" in record and as_int(record.get("matched_family"), 0) == 0:
        return None

    symbol = str(record.get("symbol") or "").strip()
    timeframe = _resolve_programme_trade_timeframe(record, roots)
    if not symbol or not timeframe:
        return None

    root_id = as_int(record.get("root_id"), 0) or 0
    confirmation_raw = record.get("confirmation_root_id")
    confirmation_id = 0 if confirmation_raw in (None, "") else (as_int(confirmation_raw, 0) or 0)
    confirmation_db_id = confirmation_id if confirmation_id > 0 else None
    family_number = as_int(record.get("family_number"))
    setup_group_id = max(1, as_int(record.get("setup_group_id"), 1) or 1)
    is_identifiable = root_id > 0 and family_number is not None and family_number > 0
    if is_identifiable:
        trade_kind = "confirmation" if confirmation_id > 0 else "root"
        chart_trade_id = "|".join((
            trade_kind, str(root_id), str(confirmation_id), timeframe,
            str(setup_group_id), str(family_number),
        ))
        identity_seed = symbol + "|" + chart_trade_id
    else:
        chart_trade_id = None
        identity_seed = "unidentified|" + symbol + "|" + timeframe + "|" + str(
            record.get("id") or hashlib.sha256(safe_json(record).encode("utf-8")).hexdigest()
        )
    identity_hash = hashlib.sha256(identity_seed.encode("utf-8")).hexdigest()

    outcome = _normalise_trade_outcome_for_programme_trade(record.get("outcome"))
    pnl = _first_record_value(record, ("pnl", "profit_loss", "profitandloss"))
    pnl = as_float(pnl) if pnl is not None else None
    pnl_risk_reward = _first_record_value(record, ("pnl_risk_reward",))
    live = 1 if as_int(record.get("live_trades"), 0) == 1 else 0
    trade_time = _first_record_value(record, (
        "trade_time", "entry_trigger_time", "entry_anchor_time", "event_time", "open_time", "created_at"
    ))

    flat_outcomes = {
        "closed flat", "breakeven", "break even", "breakeven hit",
        "closed at breakeven", "closed breakeven", "breakeven stop hit",
    }
    profit_outcomes = {"closed in profit", "closed profit", "profit", "profitable"}
    loss_outcomes = {"closed in loss", "closed loss", "loss", "losing"}
    terminal_outcomes = {
        "stoploss hit", "take profit reached", "closed", "position closed",
        "manual close", "manually closed",
    } | flat_outcomes | profit_outcomes | loss_outcomes

    if outcome in flat_outcomes and pnl is None:
        pnl = 0.0

    if outcome == "in trade":
        order_status = "running"
    elif outcome == "pending entry":
        order_status = "pending"
    elif outcome in terminal_outcomes:
        order_status = "closed"
    else:
        return None

    order_type = str(record.get("order_type") or "").strip().lower()
    allowed_order_types = {"buy", "sell", "buy-stop", "sell-stop", "buy-limit", "sell-limit"}
    if order_type not in allowed_order_types:
        direction = str(record.get("direction") or "").strip().lower()
        order_type = "sell" if direction == "short" else "buy"

    entry = _first_record_value(record, ("entry", "entry_price", "entry_trigger_price"))
    stoploss = _first_record_value(record, ("stoploss", "stoploss_price"))
    takeprofit = _first_record_value(record, ("takeprofit", "take_profit_price"))
    risk_reward = _first_record_value(record, ("trade_risk_reward", "risk_reward"))
    if pnl_risk_reward is None:
        if outcome == "take profit reached" or outcome in profit_outcomes:
            pnl_risk_reward = risk_reward
        elif outcome == "stoploss hit" or outcome in loss_outcomes:
            pnl_risk_reward = -1
        elif outcome in flat_outcomes:
            pnl_risk_reward = 0

    return {
        "userid": int(record.get("userid") or 0),
        "programmeid": int(record.get("programme_id") or 0),
        "contract_id": int(contract_id),
        "setup_group_id": int(setup_group_id),
        "symbol": symbol,
        "ordertype": order_type,
        "orderstatus": order_status,
        "timeframe": timeframe,
        "entry": entry,
        "stoploss": stoploss,
        "takeprofit": takeprofit,
        "trade_risk_reward": risk_reward,
        "pnl": pnl,
        "pnl_risk_reward": pnl_risk_reward,
        "trade_status": order_status,
        "live": live,
        "trade_time": dt_text(trade_time) if trade_time is not None else None,
        "order_request_status": record.get("order_request_status"),
        "order_request_filled": 1 if as_int(record.get("order_request_filled"), 0) == 1 else 0,
        "order_request_filled_failure_reason": record.get("order_request_filled_failure_reason"),
        "identity_hash": identity_hash,
        "chart_trade_id": chart_trade_id,
        "root_id": root_id if root_id > 0 else None,
        "confirmation_root_id": confirmation_db_id,
        "family_number": family_number,
        "family_signature": record.get("family_signature"),
        "source_timeframe": record.get("source_timeframe"),
        "entry_anchor_time": record.get("entry_anchor_time"),
        "entry_trigger_time": record.get("entry_trigger_time"),
        "outcome": str(record.get("outcome") or "") or None,
    }


def _canonical_trade_sync_value(column: str, value: Any) -> Any:
    value = normalise_mysql_value(value)
    if value is None:
        return None
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    if column in {"contract_id", "setup_group_id", "live"}:
        parsed = as_int(value)
        return parsed if parsed is not None else str(value).strip()
    if column in {"entry", "stoploss", "takeprofit", "trade_risk_reward", "pnl", "pnl_risk_reward"}:
        try:
            number = Decimal(str(value).strip())
            return number.normalize() if number.is_finite() else str(value).strip()
        except (InvalidOperation, ValueError, TypeError):
            return str(value).strip()
    if column == "trade_time":
        return dt_text(value) or str(value).strip()
    if column in {"symbol", "timeframe", "ordertype", "orderstatus", "trade_status"}:
        return str(value).strip().casefold()
    return str(value)


def programme_trade_row_matches(existing: Dict[str, Any], expected: Dict[str, Any]) -> bool:
    """Avoid an UPDATE when the canonical programme-owned trade is already exact."""
    columns = (
        "contract_id", "setup_group_id", "symbol", "ordertype", "orderstatus", "timeframe",
        "entry", "stoploss", "takeprofit", "trade_risk_reward", "pnl", "pnl_risk_reward",
        "trade_status", "live", "trade_time",
    )
    return all(
        _canonical_trade_sync_value(column, existing.get(column))
        == _canonical_trade_sync_value(column, expected.get(column))
        for column in columns
    )


def programme_trade_identity_row_matches(existing: Dict[str, Any], expected: Dict[str, Any]) -> bool:
    """Avoid rewriting an unchanged programme_trade_chart_identity mapping."""
    columns = (
        "contract_id", "identity_hash", "chart_trade_id", "root_id", "confirmation_root_id",
        "family_number", "setup_group_id", "family_signature", "symbol", "timeframe",
        "source_timeframe", "entry_anchor_time", "entry_trigger_time", "outcome",
    )
    integer_columns = {"contract_id", "root_id", "confirmation_root_id", "family_number", "setup_group_id"}
    casefold_columns = {"symbol", "timeframe"}
    for column in columns:
        left = normalise_mysql_value(existing.get(column))
        right = normalise_mysql_value(expected.get(column))
        if column in integer_columns:
            left = as_int(left) if left is not None else None
            right = as_int(right) if right is not None else None
        elif column in casefold_columns:
            left = None if left is None else str(left).strip().casefold()
            right = None if right is None else str(right).strip().casefold()
        else:
            left = None if left is None else str(left)
            right = None if right is None else str(right)
        if left != right:
            return False
    return True


def _trade_identity_fallback_key(row: Dict[str, Any], prefix: str = "") -> Tuple[Any, ...]:
    """Stable legacy family key used only when exactly one old mapping matches."""
    def value(name: str) -> Any:
        return normalise_mysql_value(row.get(prefix + name))

    return (
        as_int(value("root_id"), 0) or 0,
        as_int(value("confirmation_root_id"), 0) or 0,
        str(value("symbol") or "").strip().casefold(),
        str(value("timeframe") or "").strip().casefold(),
        None if value("source_timeframe") is None else str(value("source_timeframe")),
        None if value("family_signature") is None else str(value("family_signature")),
    )


def _legacy_trade_core_matches(existing: Dict[str, Any], expected: Dict[str, Any]) -> bool:
    """Only adopt a legacy, unlinked row when its full old core signature matches."""
    columns = ("symbol", "timeframe", "entry", "stoploss", "takeprofit", "ordertype", "orderstatus")
    return all(
        _canonical_trade_sync_value(column, existing.get(column))
        == _canonical_trade_sync_value(column, expected.get(column))
        for column in columns
    )


def _chunks(items: Sequence[Any], size: int) -> Iterable[Sequence[Any]]:
    for offset in range(0, len(items), size):
        yield items[offset:offset + size]


def _bulk_values_sql(rows: Sequence[Dict[str, Any]], columns: Sequence[str]) -> str:
    """Build a safe multi-row VALUES list using the bridge's verified SQL literals."""
    return ",".join(
        "(" + ",".join(sql_literal(row.get(column)) for column in columns) + ")"
        for row in rows
    )


def _bulk_case_update(
    db: BridgeClient,
    programme_id: int,
    user_id: int,
    rows: Sequence[Tuple[int, Dict[str, Any]]],
    columns: Sequence[str],
    batch_size: int = 250,
) -> int:
    """CASE-update batches; read back by primary key if the browser ACK is lost."""
    affected = 0
    for chunk in _chunks(list(rows), batch_size):
        ids = [int(row_id) for row_id, _ in chunk]
        assignments = []
        for column in columns:
            cases = " ".join(
                f"WHEN {int(row_id)} THEN {sql_literal(values.get(column))}"
                for row_id, values in chunk
            )
            assignments.append(f"`{column}`=CASE `id` {cases} ELSE `{column}` END")
        sql = (
            "UPDATE `programme_trades` SET " + ",".join(assignments)
            + f" WHERE `userid`={int(user_id)} AND `programmeid`={int(programme_id)}"
            + " AND `investor_id` IS NULL AND `id` IN ("
            + ",".join(str(value) for value in ids) + ")"
        )
        try:
            affected += db.execute(sql, timeout=240)
        except Exception as write_exc:
            select_columns = ",".join(f"`{column}`" for column in columns)
            check_rows = db.fetch_all(
                f"SELECT `id`,{select_columns} FROM `programme_trades` "
                f"WHERE `userid`={int(user_id)} AND `programmeid`={int(programme_id)} "
                "AND `investor_id` IS NULL AND `id` IN (" + ",".join(str(value) for value in ids) + ")",
                timeout=120,
            )
            by_id = {as_int(row.get("id"), 0): row for row in check_rows}
            all_applied = True
            for row_id, expected_values in chunk:
                actual = by_id.get(int(row_id))
                if actual is None or any(
                    _canonical_trade_sync_value(column, actual.get(column))
                    != _canonical_trade_sync_value(column, expected_values.get(column))
                    for column in columns
                ):
                    all_applied = False
                    break
            if not all_applied:
                raise RuntimeError(
                    f"programme_trades CASE UPDATE batch outcome is uncertain: Selenium/PHP "
                    f"reported {write_exc}; read-back did not verify all {len(chunk)} target rows. "
                    "The write was NOT replayed."
                ) from write_exc
            print(
                f"  Verified programme_trades UPDATE batch ({len(chunk)} rows) from MySQL "
                "after the Selenium response was lost; the UPDATE was not replayed."
            )
            affected += len(chunk)
    return affected


def _fetch_trade_rows_by_hashes(
    db: BridgeClient, programme_id: int, user_id: int, hashes: Sequence[str]
) -> Dict[str, Dict[str, Any]]:
    if not hashes:
        return {}
    literals = ",".join(sql_literal(value) for value in hashes)
    rows = db.fetch_all(
        "SELECT `id`,`chart_identity_hash`,`contract_id`,`setup_group_id`,`symbol`,`ordertype`,"
        "`orderstatus`,`timeframe`,`entry`,`stoploss`,`takeprofit`,`trade_risk_reward`,`pnl`,"
        "`pnl_risk_reward`,`trade_status`,`live`,`trade_time` FROM `programme_trades` "
        f"WHERE `userid`={int(user_id)} AND `programmeid`={int(programme_id)} "
        "AND `investor_id` IS NULL AND `chart_identity_hash` IN (" + literals + ")",
        timeout=150,
    )
    return {
        str(row.get("chart_identity_hash")): row
        for row in rows if row.get("chart_identity_hash") is not None
    }


def _fetch_trade_identity_rows(
    db: BridgeClient, programme_id: int, user_id: int, hashes: Sequence[str]
) -> Dict[str, Dict[str, Any]]:
    if not hashes:
        return {}
    literals = ",".join(sql_literal(value) for value in hashes)
    rows = db.fetch_all(
        "SELECT `programme_trade_id`,`userid`,`programmeid`,`contract_id`,`identity_hash`,"
        "`chart_trade_id`,`root_id`,`confirmation_root_id`,`family_number`,`setup_group_id`,"
        "`family_signature`,`symbol`,`timeframe`,`source_timeframe`,`entry_anchor_time`,"
        "`entry_trigger_time`,`outcome` FROM `programme_trade_chart_identity` "
        f"WHERE `userid`={int(user_id)} AND `programmeid`={int(programme_id)} "
        "AND `identity_hash` IN (" + literals + ")",
        timeout=150,
    )
    return {
        str(row.get("identity_hash")): row
        for row in rows if row.get("identity_hash") is not None
    }


def sync_programme_trades_first(
    db: BridgeClient,
    programme_id: int,
    user_id: int,
    records: Sequence[Dict[str, Any]],
    roots: Sequence[Dict[str, Any]],
) -> Dict[str, int]:
    """Synchronize canonical programme trades using bulk inserts and bulk updates.

    The helper identity table remains authoritative for PHP/dashboard compatibility.
    programme_trades.chart_identity_hash is an additive correlation key that lets
    one multi-row INSERT be mapped back to its generated IDs without assuming that
    auto-increment IDs are consecutive or relying on LAST_INSERT_ID() arithmetic.
    Investor copies and broker trade IDs are never touched, and no trade rows are
    deleted.
    """
    ensure_programme_trade_sync_schema(db)
    contract_id = _latest_programme_contract_id(db, programme_id, user_id)

    prepared: Dict[str, Dict[str, Any]] = {}
    for source_record in records:
        trade = _build_programme_trade_sync_row(source_record, roots, contract_id)
        if trade is not None:
            prepared[str(trade["identity_hash"])] = trade

    counters = {
        "inserted": 0, "updated": 0, "verified": 0, "skipped": 0,
        "mapping_updated": 0, "batches": 0,
    }
    if not prepared:
        print("  No configured trade rows were eligible for programme_trades synchronization.")
        return counters

    # Load all canonical programme-owned trades and their helper mappings in one read.
    # This replaces several SELECT/INSERT/UPDATE requests for every single trade.
    existing_rows = db.fetch_all(
        "SELECT t.`id`,t.`contract_id`,t.`setup_group_id`,t.`symbol`,t.`ordertype`,t.`orderstatus`,"
        "t.`timeframe`,t.`entry`,t.`stoploss`,t.`takeprofit`,t.`trade_risk_reward`,t.`pnl`,"
        "t.`pnl_risk_reward`,t.`trade_status`,t.`live`,t.`trade_time`,t.`chart_identity_hash`,"
        "ci.`identity_hash` AS `mapped_identity_hash`,ci.`contract_id` AS `mapped_contract_id`,"
        "ci.`chart_trade_id` AS `mapped_chart_trade_id`,ci.`root_id` AS `mapped_root_id`,"
        "ci.`confirmation_root_id` AS `mapped_confirmation_root_id`,"
        "ci.`family_number` AS `mapped_family_number`,ci.`setup_group_id` AS `mapped_setup_group_id`,"
        "ci.`family_signature` AS `mapped_family_signature`,ci.`symbol` AS `mapped_symbol`,"
        "ci.`timeframe` AS `mapped_timeframe`,ci.`source_timeframe` AS `mapped_source_timeframe`,"
        "ci.`entry_anchor_time` AS `mapped_entry_anchor_time`,"
        "ci.`entry_trigger_time` AS `mapped_entry_trigger_time`,ci.`outcome` AS `mapped_outcome` "
        "FROM `programme_trades` t LEFT JOIN `programme_trade_chart_identity` ci "
        "ON ci.`programme_trade_id`=t.`id` AND ci.`userid`=t.`userid` "
        "AND ci.`programmeid`=t.`programmeid` "
        f"WHERE t.`userid`={int(user_id)} AND t.`programmeid`={int(programme_id)} "
        "AND t.`investor_id` IS NULL ORDER BY t.`id` ASC",
        timeout=180,
    )

    # The helper identity table is the authoritative identity source for an
    # already-mapped trade. chart_identity_hash is only an auxiliary correlation
    # column for bulk INSERT ID retrieval. Older runs can leave the two hashes
    # different on the same row (for example, after an identity-hash migration).
    # Indexing BOTH values as equal aliases lets one physical trade row match two
    # current chart identities, which caused the "matched to more than one chart
    # identity" abort. Prefer mapped_identity_hash, and only use chart_identity_hash
    # for rows that do not yet have a helper mapping.
    mapped_by_hash: Dict[str, Dict[str, Any]] = {}
    chart_only_by_hash: Dict[str, Dict[str, Any]] = {}
    fallback_by_signature: Dict[Tuple[Any, ...], List[Dict[str, Any]]] = {}
    legacy_unlinked: List[Dict[str, Any]] = []
    stale_chart_hash_rows: List[Tuple[int, Dict[str, Any]]] = []

    def _add_unique_identity(
        index: Dict[str, Dict[str, Any]], identity_hash: str,
        row: Dict[str, Any], source_label: str,
    ) -> None:
        row_id = as_int(row.get("id"), 0) or 0
        prior = index.get(identity_hash)
        if prior is not None and as_int(prior.get("id"), 0) != row_id:
            raise RuntimeError(
                f"Duplicate canonical programme_trades {source_label} identity detected "
                f"for identity_hash={identity_hash}: row IDs "
                f"{as_int(prior.get('id'), 0)} and {row_id}. "
                "No trade rows were inserted, updated, or deleted."
            )
        index[identity_hash] = row

    for row in existing_rows:
        row_id = as_int(row.get("id"), 0) or 0
        if row_id <= 0:
            continue
        map_value = normalise_mysql_value(row.get("mapped_identity_hash"))
        chart_value = normalise_mysql_value(row.get("chart_identity_hash"))
        map_hash = str(map_value).strip() if map_value is not None else ""
        chart_hash = str(chart_value).strip() if chart_value is not None else ""

        if map_hash:
            _add_unique_identity(mapped_by_hash, map_hash, row, "helper-mapped")
            if chart_hash and chart_hash != map_hash:
                # Do not treat this stale auxiliary value as a second identity.
                # Clear it below so a genuinely new chart identity can be inserted
                # without colliding with the old row's unique chart-hash index.
                stale_chart_hash_rows.append((row_id, row))
            key = _trade_identity_fallback_key(row, "mapped_")
            if key[-1] is not None:
                fallback_by_signature.setdefault(key, []).append(row)
        elif chart_hash:
            _add_unique_identity(chart_only_by_hash, chart_hash, row, "chart-only")
        else:
            legacy_unlinked.append(row)

    # Repair stale auxiliary hashes before deciding which current identities are
    # new. This changes no business trade fields and never deletes a trade row.
    # The helper identity table remains authoritative for these rows.
    if stale_chart_hash_rows:
        stale_updates = [
            (row_id, {"chart_identity_hash": None})
            for row_id, _row in stale_chart_hash_rows
        ]
        repaired = _bulk_case_update(
            db, programme_id, user_id, stale_updates,
            ("chart_identity_hash",), batch_size=500,
        )
        for _row_id, row in stale_chart_hash_rows:
            row["chart_identity_hash"] = None
        print(
            f"  Reconciled {len(stale_chart_hash_rows):,} stale programme_trades chart-hash "
            "alias(es) using the authoritative programme_trade_chart_identity mapping."
        )

    # Mapped identities always win over a chart-only legacy row carrying the same
    # hash. Such a legacy row is not silently adopted as a second copy.
    existing_by_hash: Dict[str, Dict[str, Any]] = dict(mapped_by_hash)
    for identity_hash, row in chart_only_by_hash.items():
        existing_by_hash.setdefault(identity_hash, row)

    used_trade_ids: set[int] = set()
    resolved: Dict[str, Tuple[int, Dict[str, Any], Optional[Dict[str, Any]]]] = {}
    changed_trade_updates: List[Tuple[int, Dict[str, Any]]] = []
    hash_only_updates: List[Tuple[int, Dict[str, Any]]] = []
    new_trades: List[Tuple[str, Dict[str, Any]]] = []

    for identity_hash, trade in prepared.items():
        existing = existing_by_hash.get(identity_hash)

        # Preserve the older family-signature migration fallback, but only when unique.
        if existing is None and trade.get("family_signature"):
            key = _trade_identity_fallback_key(trade)
            candidates = [
                row for row in fallback_by_signature.get(key, [])
                if (as_int(row.get("id"), 0) or 0) not in used_trade_ids
            ]
            if len(candidates) == 1:
                existing = candidates[0]

        # Preserve safe legacy adoption: only one unlinked programme-owned row
        # with the full old core signature can be adopted.
        if (
            existing is None
            and trade.get("chart_trade_id") is not None
            and trade.get("entry") is not None
        ):
            candidates = [
                row for row in legacy_unlinked
                if (as_int(row.get("id"), 0) or 0) not in used_trade_ids
                and _legacy_trade_core_matches(row, trade)
            ]
            if len(candidates) == 1:
                existing = candidates[0]

        if existing is None:
            new_trades.append((identity_hash, trade))
            continue

        trade_id = as_int(existing.get("id"), 0) or 0
        if trade_id <= 0 or trade_id in used_trade_ids:
            raise RuntimeError(
                "A canonical programme_trades row was matched to more than one chart identity; "
                "refusing an ambiguous trade mapping."
            )
        used_trade_ids.add(trade_id)
        map_hash = normalise_mysql_value(existing.get("mapped_identity_hash"))
        map_meta = None
        if map_hash is not None:
            map_meta = {
                "contract_id": existing.get("mapped_contract_id"),
                "identity_hash": map_hash,
                "chart_trade_id": existing.get("mapped_chart_trade_id"),
                "root_id": existing.get("mapped_root_id"),
                "confirmation_root_id": existing.get("mapped_confirmation_root_id"),
                "family_number": existing.get("mapped_family_number"),
                "setup_group_id": existing.get("mapped_setup_group_id"),
                "family_signature": existing.get("mapped_family_signature"),
                "symbol": existing.get("mapped_symbol"),
                "timeframe": existing.get("mapped_timeframe"),
                "source_timeframe": existing.get("mapped_source_timeframe"),
                "entry_anchor_time": existing.get("mapped_entry_anchor_time"),
                "entry_trigger_time": existing.get("mapped_entry_trigger_time"),
                "outcome": existing.get("mapped_outcome"),
            }

        trade_changed = not programme_trade_row_matches(existing, trade)
        stored_chart_hash = normalise_mysql_value(existing.get("chart_identity_hash"))
        stored_map_hash = normalise_mysql_value(existing.get("mapped_identity_hash"))
        update_values = dict(trade)
        # When the helper mapping already owns this exact identity, preserve the
        # auxiliary hash value after stale-alias cleanup (which may have set it
        # to NULL). Do not recreate the conflicting alias during a business-field
        # update. Legacy/fallback adoptions receive the current identity hash.
        if stored_map_hash is not None and str(stored_map_hash).strip() == identity_hash:
            update_values["chart_identity_hash"] = stored_chart_hash
        else:
            update_values["chart_identity_hash"] = identity_hash
        if trade_changed:
            changed_trade_updates.append((trade_id, update_values))
            counters["updated"] += 1
        elif stored_map_hash is not None and str(stored_map_hash).strip() == identity_hash:
            # The helper mapping already owns this identity. Keep the auxiliary
            # chart hash cleared when it was stale; do not reintroduce an alias.
            counters["skipped"] += 1
        elif stored_chart_hash is None or str(stored_chart_hash).strip() != identity_hash:
            # Legacy/fallback adoption: attach the current hash only when this row
            # did not already have this identity in the authoritative helper table.
            hash_only_updates.append((trade_id, {"chart_identity_hash": identity_hash}))
            counters["skipped"] += 1
        else:
            counters["skipped"] += 1

        resolved[identity_hash] = (trade_id, trade, map_meta)

    # Bulk changed-row update; unchanged business rows are not rewritten.
    trade_update_columns = (
        "contract_id", "setup_group_id", "symbol", "ordertype", "orderstatus", "timeframe",
        "entry", "stoploss", "takeprofit", "trade_risk_reward", "pnl", "pnl_risk_reward",
        "trade_status", "live", "trade_time", "chart_identity_hash",
    )
    for chunk in _chunks(changed_trade_updates, 250):
        _bulk_case_update(db, programme_id, user_id, chunk, trade_update_columns, batch_size=250)
        counters["batches"] += 1
    for chunk in _chunks(hash_only_updates, 500):
        _bulk_case_update(db, programme_id, user_id, chunk, ("chart_identity_hash",), batch_size=500)
        counters["batches"] += 1

    # Insert new canonical rows in multi-row INSERT statements. chart_identity_hash
    # makes their generated IDs retrievable exactly, without unsafe ID arithmetic.
    insert_columns = (
        "userid", "programmeid", "contract_id", "setup_group_id", "investor_id", "tradeid",
        "chart_identity_hash", "symbol", "ordertype", "orderstatus", "timeframe", "entry",
        "stoploss", "takeprofit", "trade_risk_reward", "pnl", "pnl_risk_reward",
        "trade_status", "live", "trade_time", "order_request_status", "order_request_filled",
        "order_request_filled_failure_reason",
    )
    for chunk in _chunks(new_trades, 500):
        insert_rows = []
        for identity_hash, trade in chunk:
            insert_rows.append({
                "userid": user_id,
                "programmeid": programme_id,
                "contract_id": trade["contract_id"],
                "setup_group_id": trade["setup_group_id"],
                "investor_id": None,
                "tradeid": None,
                "chart_identity_hash": identity_hash,
                "symbol": trade["symbol"],
                "ordertype": trade["ordertype"],
                "orderstatus": trade["orderstatus"],
                "timeframe": trade["timeframe"],
                "entry": trade.get("entry"),
                "stoploss": trade.get("stoploss"),
                "takeprofit": trade.get("takeprofit"),
                "trade_risk_reward": trade.get("trade_risk_reward"),
                "pnl": trade.get("pnl"),
                "pnl_risk_reward": trade.get("pnl_risk_reward"),
                "trade_status": trade["trade_status"],
                "live": trade["live"],
                "trade_time": trade.get("trade_time"),
                "order_request_status": trade.get("order_request_status"),
                "order_request_filled": trade.get("order_request_filled", 0),
                "order_request_filled_failure_reason": trade.get("order_request_filled_failure_reason"),
            })
        sql = (
            "INSERT INTO `programme_trades` ("
            + ",".join(f"`{column}`" for column in insert_columns)
            + ") VALUES " + _bulk_values_sql(insert_rows, insert_columns)
        )
        chunk_hashes = [identity_hash for identity_hash, _ in chunk]
        try:
            db.execute(sql, timeout=300)
        except Exception as write_exc:
            # A single InnoDB multi-row INSERT is atomic, but the browser can lose
            # the response after MariaDB committed it. Read the durable identities;
            # never blindly submit the batch a second time.
            persisted = _fetch_trade_rows_by_hashes(db, programme_id, user_id, chunk_hashes)
            fully_applied = len(persisted) == len(chunk)
            if fully_applied:
                for identity_hash, expected_trade in chunk:
                    actual_trade = persisted.get(identity_hash)
                    if actual_trade is None or not programme_trade_row_matches(actual_trade, expected_trade):
                        fully_applied = False
                        break
            if not fully_applied:
                raise RuntimeError(
                    f"programme_trades multi-row INSERT response was lost; MySQL read-back "
                    f"verified {len(persisted):,}/{len(chunk):,} inserted trades. "
                    "The batch was NOT replayed to avoid duplicates. Original error: "
                    f"{write_exc}"
                ) from write_exc
            print(
                f"  Verified programme_trades INSERT batch ({len(chunk)} rows) from MySQL "
                "after the Selenium response was lost; the INSERT was not replayed."
            )
        counters["inserted"] += len(chunk)
        counters["batches"] += 1

    # Read the generated IDs for all new trades by their unique correlation hashes.
    new_hashes = [identity_hash for identity_hash, _ in new_trades]
    for hash_chunk in _chunks(new_hashes, 500):
        literals = ",".join(sql_literal(value) for value in hash_chunk)
        rows = db.fetch_all(
            "SELECT `id`,`chart_identity_hash` FROM `programme_trades` "
            f"WHERE `userid`={int(user_id)} AND `programmeid`={int(programme_id)} "
            "AND `investor_id` IS NULL AND `chart_identity_hash` IN (" + literals + ")",
            timeout=120,
        )
        ids_by_hash = {
            str(row.get("chart_identity_hash")): as_int(row.get("id"), 0) or 0
            for row in rows
        }
        for identity_hash in hash_chunk:
            trade_id = ids_by_hash.get(identity_hash, 0)
            if trade_id <= 0:
                raise RuntimeError(
                    "Bulk programme_trades INSERT completed but its generated row could not be "
                    f"retrieved by chart_identity_hash={identity_hash}. Analytics insertion is blocked."
                )
            trade = prepared[identity_hash]
            resolved[identity_hash] = (trade_id, trade, None)

    # Prepare and batch-upsert only missing or changed helper mappings.
    identity_columns = (
        "programme_trade_id", "userid", "programmeid", "contract_id", "identity_hash", "chart_trade_id",
        "root_id", "confirmation_root_id", "family_number", "setup_group_id", "family_signature",
        "symbol", "timeframe", "source_timeframe", "entry_anchor_time", "entry_trigger_time", "outcome",
    )
    identity_upserts: List[Dict[str, Any]] = []
    for identity_hash, (trade_id, trade, existing_meta) in resolved.items():
        identity_values = {
            "programme_trade_id": trade_id,
            "userid": user_id,
            "programmeid": programme_id,
            "contract_id": trade["contract_id"],
            "identity_hash": identity_hash,
            "chart_trade_id": trade.get("chart_trade_id"),
            "root_id": trade.get("root_id"),
            "confirmation_root_id": trade.get("confirmation_root_id"),
            "family_number": trade.get("family_number"),
            "setup_group_id": trade["setup_group_id"],
            "family_signature": trade.get("family_signature"),
            "symbol": trade["symbol"],
            "timeframe": trade["timeframe"],
            "source_timeframe": trade.get("source_timeframe"),
            "entry_anchor_time": trade.get("entry_anchor_time"),
            "entry_trigger_time": trade.get("entry_trigger_time"),
            "outcome": trade.get("outcome"),
        }
        if existing_meta is None or not programme_trade_identity_row_matches(existing_meta, identity_values):
            identity_upserts.append(identity_values)

    for chunk in _chunks(identity_upserts, 500):
        sql = (
            "INSERT INTO `programme_trade_chart_identity` ("
            + ",".join(f"`{column}`" for column in identity_columns)
            + ") VALUES " + _bulk_values_sql(chunk, identity_columns)
            + " ON DUPLICATE KEY UPDATE "
            + ",".join(
                f"`{column}`=VALUES(`{column}`)"
                for column in identity_columns if column != "programme_trade_id"
            )
            + ",`programme_trade_id`=VALUES(`programme_trade_id`)"
        )
        chunk_hashes = [str(row["identity_hash"]) for row in chunk]
        try:
            db.execute(sql, timeout=300)
        except Exception as write_exc:
            persisted_mappings = _fetch_trade_identity_rows(db, programme_id, user_id, chunk_hashes)
            fully_applied = len(persisted_mappings) == len(chunk)
            if fully_applied:
                for expected_values in chunk:
                    identity_hash = str(expected_values["identity_hash"])
                    actual_values = persisted_mappings.get(identity_hash)
                    if actual_values is None:
                        fully_applied = False
                        break
                    if (
                        as_int(actual_values.get("programme_trade_id"), 0)
                        != as_int(expected_values.get("programme_trade_id"), 0)
                        or as_int(actual_values.get("userid"), 0) != user_id
                        or as_int(actual_values.get("programmeid"), 0) != programme_id
                        or not programme_trade_identity_row_matches(actual_values, expected_values)
                    ):
                        fully_applied = False
                        break
            if not fully_applied:
                raise RuntimeError(
                    f"programme_trade_chart_identity upsert response was lost; MySQL read-back "
                    f"verified {len(persisted_mappings):,}/{len(chunk):,} mapping rows. "
                    "The batch was NOT replayed. Original error: "
                    f"{write_exc}"
                ) from write_exc
            print(
                f"  Verified programme_trade_chart_identity upsert batch ({len(chunk)} rows) "
                "after the Selenium response was lost; it was not replayed."
            )
        counters["mapping_updated"] += len(chunk)
        counters["batches"] += 1

    # Verify every expected trade hash in bounded IN lists, after all batches finish.
    expected_hashes = list(prepared.keys())
    verified_hashes = 0
    for hash_chunk in _chunks(expected_hashes, 500):
        literals = ",".join(sql_literal(value) for value in hash_chunk)
        check = db.fetch_all(
            "SELECT COUNT(DISTINCT ci.`identity_hash`) AS `verified_count` "
            "FROM `programme_trade_chart_identity` ci "
            "INNER JOIN `programme_trades` pt ON pt.`id`=ci.`programme_trade_id` "
            "AND pt.`userid`=ci.`userid` AND pt.`programmeid`=ci.`programmeid` "
            "WHERE ci.`userid`=" + str(int(user_id))
            + " AND ci.`programmeid`=" + str(int(programme_id))
            + " AND pt.`investor_id` IS NULL AND ci.`identity_hash` IN (" + literals + ")",
            timeout=150,
        )
        verified_hashes += as_int(check[0].get("verified_count"), 0) if check else 0

    counters["verified"] = verified_hashes
    if counters["verified"] != len(expected_hashes):
        raise RuntimeError(
            "Programme Trades bulk verification failed: "
            f"expected {len(expected_hashes):,} unique chart trades, verified {counters['verified']:,}. "
            "The worker has stopped before programme_analytics writes; no programme_trades rows were deleted. "
            "Inspect programme_trades.chart_identity_hash and programme_trade_chart_identity for this programme."
        )

    print(
        "  Programme trades bulk sync: "
        f"inserted={counters['inserted']:,} | updated={counters['updated']:,} | "
        f"unchanged={counters['skipped']:,} | mapping rows written={counters['mapping_updated']:,} | "
        f"SQL batches={counters['batches']:,} | verified={counters['verified']:,}"
    )
    return counters

def print_persistent_analytics_summary(db: BridgeClient, programme_id: int, user_id: int) -> None:
    """Show what the PHP dashboard can already read before this scan writes anything."""
    print("\nSTORED ANALYTICS BASELINE (before this scan; read-only)")
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
        print(f"  Baseline could not be read: {str(exc).splitlines()[0][:300]}")
        return
    if not rows:
        print("  No existing programme_analytics rows were found for this programme/user.")
        return
    total_rows = total_families = total_trades = 0
    print("  Scope                         Rows     Families     Trades")
    print("  -----------------------------------------------------------")
    for row in rows:
        count = as_int(row.get("analytics_rows"), 0) or 0
        families = as_int(row.get("matched_families"), 0) or 0
        trades = as_int(row.get("configured_trades"), 0) or 0
        total_rows += count
        total_families += families
        total_trades += trades
        confirmation_id = as_int(row.get("confirmation_root_id"), 0) or 0
        scope_name = f"confirmation #{confirmation_id}" if confirmation_id else "main root"
        scope = f"{row.get('symbol')} {str(row.get('timeframe') or '').upper()} | root {row.get('root_id')} | {scope_name}"
        print(f"  {scope:<43} {count:>5,} {families:>12,} {trades:>10,}")
    print("  -----------------------------------------------------------")
    print(f"  {'TOTAL':<43} {total_rows:>5,} {total_families:>12,} {total_trades:>10,}")
    print("  Note: these are already-stored dashboard records, not matches from the current scan.")



def _timeframe_order_key(tf: str) -> Tuple[int, str]:
    """Higher timeframes first, with a stable lexical tie-breaker."""
    duration = tf_milliseconds(tf)
    return (-(duration if duration is not None else 0), str(tf).lower())


def _trade_outcome_counts(records: Sequence[Dict[str, Any]]) -> Dict[str, int]:
    return {
        "families": len(records),
        "trades": sum(1 for r in records if int(r.get("trade_configured") or 0) == 1),
        "stoploss": sum(1 for r in records if r.get("outcome") == "stoploss hit"),
        "profit": sum(1 for r in records if r.get("outcome") in {"take profit reached", "breakeven"}),
        "take_profit": sum(1 for r in records if r.get("outcome") == "take profit reached"),
        "breakeven_profit": sum(1 for r in records if r.get("outcome") == "breakeven"),
        "breakeven_stop_hit": sum(1 for r in records if r.get("outcome") in {"breakeven stop hit", "breakeven hit"}),
        "restricted_date": sum(1 for r in records if int(r.get("restricted_date_hit") or 0) == 1),
        "restricted_day": sum(1 for r in records if int(r.get("restricted_day_hit") or 0) == 1),
        "excluded": sum(1 for r in records if int(r.get("excluded_from_analytics") or 0) == 1),
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
            record.get("source_timeframe"), record.get("setup_group_id"), record.get("family_number"),
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


def process_programme(
    db: BridgeClient,
    programme: Dict[str, Any],
    progress: Optional[Dict[str, Any]] = None,
) -> Dict[str, int]:
    """Run the complete PHP-equivalent historical matching chain for one programme.

    `progress` is updated as each stage completes so the scan summary can still
    report useful partial results when database persistence fails.
    """
    programme_id = int(programme.get("id") or 0)
    user_id = int(programme.get("userid") or 0)
    programme_name = str(programme.get("program_name") or f"Programme {programme_id}")
    progress = progress if progress is not None else {}
    progress.update({
        "programme_id": programme_id, "programme_name": programme_name,
        "status": "RUNNING", "stage": "starting", "reason": "",
        "pairs": 0, "families": 0, "trades": 0, "candles": 0,
        "analytics_rows_deleted": 0, "analytics_rows_saved": 0,
        "analytics_rows_inserted": 0, "analytics_rows_updated": 0,
        "analytics_rows_unchanged": 0,
        "programme_trade_rows_inserted": 0, "programme_trade_rows_updated": 0,
        "programme_trade_rows_unchanged": 0, "programme_trade_rows_verified": 0,
        "analytics_rows_before": 0, "analytics_groups_before": 0,
    })

    print("\n" + "=" * 82)
    print(f"PROGRAMME: {programme_name} (ID {programme_id})")
    print("=" * 82)
    print(f"Owner user ID: {user_id}")
    progress["stage"] = "reading chart candle limit"
    try:
        chart_setting_rows = db.fetch_all(
            "SELECT `chart_total_candles` FROM `server_account` WHERE `id`=1 LIMIT 1",
            timeout=30,
        )
        configured_chart_total_candles = as_int(chart_setting_rows[0].get("chart_total_candles")) if chart_setting_rows else None
        # This database value is the one and only total-history ceiling used by
        # the PHP chart, higher-timeframe projection and Python matcher.
        chart_total_candles = configured_chart_total_candles
    except Exception as exc:
        progress.update({"status": "FAILED", "reason": str(exc).splitlines()[0][:300]})
        log.error("%s: could not read server_account.chart_total_candles; skipping programme: %s", programme_name, exc)
        return {"pairs": 0, "families": 0, "trades": 0, "candles": 0, "analytics_rows_saved": 0}
    if chart_total_candles is None or chart_total_candles < 1:
        reason = "server_account.chart_total_candles is empty or invalid"
        progress.update({"status": "SKIPPED", "reason": reason, "stage": "chart setting validation"})
        print(f"{programme_name}: {reason}. Skipping candle fetch and analytics; no fallback is applied.")
        return {"pairs": 0, "families": 0, "trades": 0, "candles": 0, "analytics_rows_saved": 0}
    print(
        f"Candle window per configured symbol/timeframe: {chart_total_candles:,} "
        "(server_account.chart_total_candles; no additional hardcoded history ceiling)"
    )
    setup_group_limit = load_trades_setup_per_group(db)
    print(f"Trade setup group size: {setup_group_limit:,} setups per group (server_operational_settings.trades_setup_per_group)")
    begin_test_enabled = truthy(programme.get("begin_test_flag"), False)
    begin_status = str(programme.get("begin_test_status") or "not started").strip().lower()
    begin_started = programme.get("begin_test_started_at") or "not recorded"
    begin_interest_id = programme.get("begin_test_interest_id") or "none"
    print(
        f"{programme_name} Begin Test = {'enabled' if begin_test_enabled else 'disabled'} "
        f"(interest #{begin_interest_id}, status={begin_status}, started_at={begin_started})"
    )
    print(
        "Refresh order: upsert/verify programme-owned programme_trades first; keep identical analytics "
        "rows untouched; update changed identities; insert only missing identities; verify the complete "
        "dataset; then delete only obsolete analytics rows. programme_trades rows are never deleted."
    )

    progress["stage"] = "loading saved programme configuration"
    print("\nSAVED PROGRAMME CONFIGURATION")
    roots, confirmation_roots, parent_rows, confirmation_rows = load_programme_config(
        db, user_id, programme_id
    )
    roots = sorted(roots, key=lambda r: (int(r.get("id") or 0), str(r.get("name") or "").casefold()))

    # The browser uses applyToChart as the root-level execution gate. Confirmation
    # alternatives inherit that parent gate; a disabled parent contributes nothing.
    for root in roots:
        enabled = bool(root.get("applyToChart"))
        print(
            f"{programme_name} | root_id={as_int(root.get('id'), 0) or 0} | "
            f"{_root_display_name(root)} | apply_to_chart={'1' if enabled else '0'}"
        )
    active_roots = [root for root in roots if bool(root.get("applyToChart"))]
    print(
        "Applied-to-chart root scope: "
        + (", ".join(
            f"{as_int(root.get('id'), 0) or 0}:{_root_display_name(root)}"
            for root in active_roots
        ) if active_roots else "NONE")
    )
    if not active_roots:
        reason = "no roots have apply_to_chart enabled"
        progress.update({"status": "SKIPPED", "reason": reason, "stage": "root eligibility"})
        print(f"{programme_name}: {reason}. Skipping candle fetch and analytics.")
        return {"pairs": 0, "families": 0, "trades": 0, "candles": 0, "analytics_rows_saved": 0}

    print("\nRESTORED ACTIVE-ROOT CONFIGURATION (identities consumed by Python)")
    for active_root in active_roots:
        print_tree_configuration(active_root)

    symbols = sorted(set(parse_list(programme.get("selected_symbols"))), key=str.casefold)
    if not symbols:
        reason = "selected_symbols is empty"
        progress.update({"status": "SKIPPED", "reason": reason, "stage": "symbol selection"})
        print(f"{programme_name}: {reason}. No candle pairs can be processed.")
        return {"pairs": 0, "families": 0, "trades": 0, "candles": 0, "analytics_rows_saved": 0}

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
        reason = "enabled roots have no configured parent or confirmation timeframes"
        progress.update({"status": "SKIPPED", "reason": reason, "stage": "timeframe selection"})
        print(f"{programme_name}: {reason}.")
        return {"pairs": 0, "families": 0, "trades": 0, "candles": 0, "analytics_rows_saved": 0}

    required_pairs_ordered = sorted(
        required_pairs,
        key=lambda pair: (pair[0].casefold(), _timeframe_order_key(pair[1]))
    )
    print_persistent_analytics_summary(db, programme_id, user_id)
    progress["stage"] = "capturing existing analytics group layout"
    rebuild_state = prepare_analytics_rebuild_state(
        db, programme_id, user_id, setup_group_limit
    )
    progress.update({
        "analytics_rows_before": rebuild_state["existing_rows"],
        "analytics_groups_before": len(rebuild_state["existing_groups"]),
    })
    required_timeframes = sorted({tf for _symbol, tf in required_pairs}, key=_timeframe_order_key)
    progress["stage"] = "fetching configured candle records"
    print("\nCANDLE INPUTS")
    print(f"  Symbols: {', '.join(symbols)}")
    print(f"  Configured timeframes: {', '.join(tf.upper() for tf in required_timeframes)}")

    fetched_candles: Dict[str, Dict[str, List[Dict[str, Any]]]] = {symbol: {} for symbol in symbols}
    total_candles = 0
    for symbol, timeframe in required_pairs_ordered:
        candles = load_candles(db, programme_id, user_id, symbol, timeframe, chart_total_candles)
        fetched_candles.setdefault(symbol, {})[timeframe] = candles
        total_candles += len(candles)
        diag = candle_sequence_diagnostics(candles)
        print(f"  {symbol} {timeframe.upper()}: {len(candles):,} candle(s)")
        print(f"    Time range: {diag['oldest'] or 'n/a'}  ->  {diag['newest'] or 'n/a'}")
        print(
            f"    Validation: chronological={'yes' if diag['chronological_non_decreasing'] else 'NO'} | "
            f"strict={'yes' if diag['chronological_strict'] else 'no'} | "
            f"duplicate IDs={diag['duplicate_ids']} | duplicate times={diag['duplicate_timestamps']} | "
            f"missing OHLC={diag['missing_ohlc_rows']} | high below low={diag['high_below_low_rows']}"
        )
        if diag["valid_timestamps"] != len(candles):
            print(f"    DATA WARNING: {len(candles) - diag['valid_timestamps']} candle(s) have no parseable time/open_time")
        if not diag["chronological_non_decreasing"]:
            print("    DATA WARNING: candle timestamps are not ascending; PHP chart matching receives oldest-to-newest candles")

    progress["candles"] = total_candles
    progress["stage"] = "loading trade and account-management rules"
    provider = load_provider_risk_reward(db, programme)
    account_rules = load_account_management_rules(db, programme)
    print(
        "Account Management rules loaded: "
        f"timezone={account_rules.get('timezone') or 'UTC'}, "
        f"developer_id={account_rules.get('developer_id') or user_id}, "
        f"restricted_dates={len(account_rules.get('restricted_dates') or [])}, "
        f"restricted_days={len(account_rules.get('restricted_days') or [])}, "
        f"breakeven_rules={len(account_rules.get('trades_breakeven') or [])}"
    )
    progress["stage"] = "building runtime snapshot"
    print("\nRUNTIME SNAPSHOT")
    snapshot_payload = {
        "schema": "programme_training_runtime_snapshot_v1",
        "programme": {
            "id": programme_id,
            "userid": user_id,
            "program_name": programme_name,
            "selected_symbols": programme.get("selected_symbols"),
            "chart_total_candles": chart_total_candles,
            "begin_test_interest_id": programme.get("begin_test_interest_id"),
            "begin_test_flag": int(begin_test_enabled),
            "begin_test_status": programme.get("begin_test_status"),
            "begin_test_started_at": programme.get("begin_test_started_at"),
        },
        "scope": {
            "programme_id": programme_id,
            "user_id": user_id,
            "symbols": symbols,
            "required_timeframes": required_timeframes,
            "chart_total_candles": chart_total_candles,
            "required_pairs": [{"symbol": symbol, "timeframe": tf} for symbol, tf in required_pairs_ordered],
            "programme_filter": "all programmes (temporary VPS/computer-IP guard disabled); optionally narrowed by --programme-id",
            "begin_test_policy": "only pending_entry trade records are live when Begin Test is enabled",
            "root_filter": "only first saved root rows with apply_to_chart=1 (PHP ptDbBool semantics); confirmation roots inherit the parent root filter",
            "applied_root_ids": [as_int(root.get("id"), 0) or 0 for root in active_roots],
            "applied_root_names": [_root_display_name(root) for root in active_roots],
        },
        "configuration": {
            "roots": roots,
            "confirmation_roots": confirmation_roots,
            "parent_rows": parent_rows,
            "confirmation_rows": confirmation_rows,
            "restoration_engine": "programme_training.php ptRestoreSavedRows() compatible",
        },
        "provider_risk_reward": provider,
        "account_management_rules": account_rules,
        "candles": fetched_candles,
    }

    totals = {"pairs": 0, "families": 0, "trades": 0, "candles": total_candles}
    with tempfile.TemporaryDirectory(prefix=f"programme_training_{programme_id}_") as temp_dir:
        snapshot_path = Path(temp_dir) / "programme_training_runtime.json"
        result_path = Path(temp_dir) / "programme_analytics_result.json"
        _snapshot_write(snapshot_path, snapshot_payload)
        print(f"  Created: {snapshot_path.name} ({snapshot_path.stat().st_size:,} bytes)")
        print("  Reloading the saved snapshot as the matching input.")
        with snapshot_path.open("r", encoding="utf-8") as handle:
            snapshot = json.load(handle)

        # From this point onward all matching inputs come only from the JSON snapshot.
        runtime_roots = snapshot["configuration"]["roots"]
        runtime_roots = [root for root in runtime_roots if bool(root.get("applyToChart"))]
        candle_cache: Dict[str, Dict[str, List[Dict[str, Any]]]] = snapshot["candles"]
        provider_data = snapshot.get("provider_risk_reward") or {}
        provider_rr = as_float(provider_data.get("risk_reward"))
        provider_rr_type = str(provider_data.get("risk_reward_type") or "")
        account_rule_data = snapshot.get("account_management_rules") or {}
        records_by_pair: Dict[Tuple[str, str], List[Dict[str, Any]]] = {
            pair: [] for pair in required_pairs_ordered
        }

        progress["stage"] = "matching root families and confirmations"
        print("\nMATCHING RESULTS")
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
                            setup_group_limit=setup_group_limit,
                        )
                        trade = None
                        if trade_for == "root" and root.get("useForTrades"):
                            trade = make_trade_record(
                                root, root, family, parent_candles, parent_tf,
                                symbol, programme_id, user_id,
                                provider_rr=provider_rr,
                                provider_rr_type=provider_rr_type,
                                source_timeframe=parent_tf,
                                account_rules=account_rule_data,
                                setup_group_limit=setup_group_limit,
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
                        print(f"  Confirmation diagnostics: {_root_display_name(cr, True)}")
                        print(
                            f"    Projection: parent checked={trace.get('parent_families_checked', 0):,} | "
                            f"anchor mapped={trace.get('parent_family_anchor_mapped', 0):,} | "
                            f"invalid parent index={trace.get('invalid_parent_indices', 0):,}"
                        )
                        print(
                            f"    Window/anchor: missing projection window={trace.get('projection_window_missing', 0):,} | "
                            f"invalid anchor time={trace.get('anchor_timestamp_invalid', 0):,} | "
                            f"lower anchor not found={trace.get('lower_anchor_not_found', 0):,}"
                        )
                        print(
                            f"    Matcher: calls={trace.get('matcher_calls', 0):,} | "
                            f"successful calls={trace.get('matcher_successes', 0):,} | "
                            f"failed calls={matcher.get('failed_calls', 0):,}"
                        )
                        if matcher:
                            print(
                                f"    Candle checks: main type pass={matcher.get('main_type_passes', 0):,} | "
                                f"reference checks={matcher.get('reference_candidate_checks', 0):,} | "
                                f"reference type matches={matcher.get('reference_type_matches', 0):,} | "
                                f"price matches={matcher.get('price_matches', 0):,} | "
                                f"price rejected={matcher.get('price_rejections', 0):,}"
                            )
                            for reason, count in sorted((matcher.get('failure_reasons') or {}).items()):
                                print(f"      Failure reason: {reason} ({count:,})")
                        for blocker in (trace.get('setup_blockers') or []):
                            print(f"    Setup blocker: {blocker}")

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
                            setup_group_limit=setup_group_limit,
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
                                account_rules=account_rule_data,
                                setup_group_limit=setup_group_limit,
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

        print("\nLIVE / HISTORY POLICY")
        print(f"  Begin Test: {'ON' if begin_test_enabled else 'OFF'}")
        print(f"  Pending-entry orders marked Live: {live_pending_count:,}")
        print(f"  Configured trade records marked History: {history_trade_count:,}")

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

        # Capture every calculated pair before the first DB write. This is
        # important because a persistence error on the first pair must not hide
        # matches already calculated for later pairs in the same programme.
        for pair_result in result_from_json.get("results", []):
            pair_records = pair_result.get("records") or []
            if not pair_records:
                continue
            totals["pairs"] += 1
            totals["families"] += sum(int(record.get("matched_family") or 0) for record in pair_records)
            totals["trades"] += sum(int(record.get("trade_configured") or 0) for record in pair_records)
        progress.update(totals)

        print("\nCALCULATED ANALYTICS SUMMARY (before database writes)")
        print("  Pair             Matched families    Configured trades")
        print("  ------------------------------------------------------")
        for pair_result in result_from_json.get("results", []):
            pair_symbol = str(pair_result.get("symbol") or "")
            pair_timeframe = str(pair_result.get("timeframe") or "").upper()
            pair_records = pair_result.get("records") or []
            if not pair_records:
                continue
            pair_families = sum(int(record.get("matched_family") or 0) for record in pair_records)
            pair_trades = sum(int(record.get("trade_configured") or 0) for record in pair_records)
            print(f"  {pair_symbol + ' ' + pair_timeframe:<16} {pair_families:>16,} {pair_trades:>20,}")
        print("  ------------------------------------------------------")
        print(f"  {'TOTAL':<16} {totals['families']:>16,} {totals['trades']:>20,}")
        print(f"  Analytics pairs: {totals['pairs']:,}")

        # PHP parity: synchronize canonical programme-owned chart trades FIRST.
        # This stage may insert new rows or refresh existing investor_id IS NULL
        # rows, but it never deletes programme_trades or alters investor copies.
        all_calculated_records = [
            record
            for pair_result in result_from_json.get("results", [])
            for record in (pair_result.get("records") or [])
        ]
        progress["stage"] = "syncing and verifying programme_trades before analytics insertion"
        configured_trade_rows = sum(
            1 for record in all_calculated_records
            if as_int(record.get("trade_configured"), 0) == 1
            and as_int(record.get("matched_family"), 1) != 0
        )
        print("\nPROGRAMME TRADES — FIRST DATABASE WRITE STAGE")
        print(f"  Calculated analytics records supplied: {len(all_calculated_records):,}")
        print(f"  Configured trade candidates: {configured_trade_rows:,}")
        print("  Existing programme-owned trades: updated only when their stored fields changed; exact matches are skipped.")
        print("  New programme-owned trades: multi-row INSERT batches of up to 500, correlated by chart_identity_hash.")
        print("  Investor copies and broker trade IDs: preserved; no trade rows are deleted.")
        trade_sync = sync_programme_trades_first(
            db, programme_id, user_id, all_calculated_records, active_roots
        )
        totals["programme_trade_rows_inserted"] = trade_sync["inserted"]
        totals["programme_trade_rows_updated"] = trade_sync["updated"]
        totals["programme_trade_rows_verified"] = trade_sync["verified"]
        totals["programme_trade_rows_unchanged"] = trade_sync["skipped"]
        progress.update(totals)
        print(
            f"  Programme Trades verified: {trade_sync['verified']:,} | "
            f"inserted={trade_sync['inserted']:,} | updated={trade_sync['updated']:,} | "
            f"unchanged/skipped={trade_sync['skipped']:,}"
        )
        print("  FIRST STAGE COMPLETE — proceeding to Programme Analytics insertion.")

        # Incremental analytics reconciliation. Existing exact matches remain in
        # place and are not written again; changed identities update in place;
        # only identities missing from the old snapshot are inserted.
        expected_records: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
        for pair_result in result_from_json.get("results", []):
            symbol = str(pair_result.get("symbol") or "")
            timeframe = str(pair_result.get("timeframe") or "").lower()
            for record in _deduplicate_records(pair_result.get("records") or []):
                prepared = {column: record.get(column) for column in ANALYTICS_COLUMNS}
                prepared["programme_id"] = programme_id
                prepared["userid"] = user_id
                prepared["symbol"] = str(prepared.get("symbol") or symbol)
                prepared["timeframe"] = str(prepared.get("timeframe") or timeframe).lower()
                prepared["rebuild_batch_token"] = None
                key = _analytics_identity_key(prepared)
                if key in expected_records:
                    raise RuntimeError(f"Duplicate calculated analytics identity across pairs: {key!r}")
                expected_records[key] = prepared

        expected_new_rows = len(expected_records)
        if expected_new_rows == 0 and int(rebuild_state.get("existing_rows", 0)) > 0:
            raise RuntimeError(
                "Fresh calculation produced zero analytics identities while previous analytics exist. "
                "The existing programme_analytics dataset is preserved; nothing will be deleted."
            )

        progress["stage"] = "incrementally reconciling programme_analytics"
        print("\nANALYTICS INCREMENTAL RECONCILIATION")
        print(f"  Existing rows captured: {rebuild_state['existing_rows']:,}")
        print(f"  Newly calculated identities: {expected_new_rows:,}")
        print("  Identical existing rows: retained without INSERT, UPDATE, or DELETE.")
        print("  Changed existing rows: updated in place; new identities: inserted only once.")

        for pair_result in result_from_json.get("results", []):
            symbol = str(pair_result.get("symbol") or "")
            timeframe = str(pair_result.get("timeframe") or "").lower()
            records = pair_result.get("records") or []
            if not records:
                print(f"  {symbol} {timeframe.upper()}: no calculated records; no analytics writes needed")
                continue
            pair_counts = _trade_outcome_counts(records)
            family_count = sum(int(record.get("matched_family") or 0) for record in records)
            trade_count = sum(int(record.get("trade_configured") or 0) for record in records)
            print(f"  {symbol} {timeframe.upper()}: reconciling {len(_deduplicate_records(records)):,} family record(s)")
            print(f"    Matched family records: {family_count:,} | configured trade records: {trade_count:,}")
            progress["stage"] = f"incrementally reconciling analytics for {symbol} {timeframe.upper()}"
            persistence = persist_pair(
                db, programme_id, user_id, symbol, timeframe, records,
                rebuild_state=rebuild_state,
                setup_group_limit=setup_group_limit,
            )
            totals["analytics_rows_inserted"] = totals.get("analytics_rows_inserted", 0) + persistence["inserted"]
            totals["analytics_rows_updated"] = totals.get("analytics_rows_updated", 0) + persistence["updated"]
            totals["analytics_rows_unchanged"] = totals.get("analytics_rows_unchanged", 0) + persistence["unchanged"]
            totals["analytics_rows_saved"] = (
                totals["analytics_rows_inserted"] + totals["analytics_rows_updated"] +
                totals["analytics_rows_unchanged"]
            )
            progress.update(totals)
            print(
                f"    Actions: inserted={persistence['inserted']:,} | "
                f"updated={persistence['updated']:,} | "
                f"unchanged/skipped={persistence['unchanged']:,} | deleted=0 (deletion deferred)"
            )

        # No old row is deleted until every expected identity and full payload
        # has been read back successfully from MySQL Remote.
        progress["stage"] = "verifying reconciled programme_analytics dataset"
        verified_rows = verify_incremental_analytics_dataset(
            db, programme_id, user_id, rebuild_state["desired_records"]
        )
        if verified_rows != expected_new_rows:
            raise RuntimeError(
                f"Analytics verification count mismatch: expected={expected_new_rows:,}, verified={verified_rows:,}. "
                "Obsolete old rows were not deleted."
            )

        old_records = rebuild_state["existing_records"]
        keep_old_ids = rebuild_state["kept_existing_ids"]
        obsolete_count = sum(
            1 for row in old_records
            if (as_int(row.get("id"), 0) or 0) > 0
            and (as_int(row.get("id"), 0) or 0) not in keep_old_ids
        )
        print("\nANALYTICS INCREMENTAL COMMIT")
        print(f"  Current identities verified: {verified_rows:,}")
        print(f"  Existing identical/updated rows retained: {len(keep_old_ids):,}")
        print(f"  New rows inserted: {totals['analytics_rows_inserted']:,}")
        print(f"  Changed existing rows updated: {totals['analytics_rows_updated']:,}")
        print(f"  Obsolete captured rows eligible for deletion: {obsolete_count:,}")
        progress["stage"] = "deleting obsolete analytics only after full verification"
        deleted_rows = delete_obsolete_analytics_rows(
            db, programme_id, user_id, old_records, keep_old_ids
        )
        totals["analytics_rows_deleted"] = deleted_rows
        final_rows = verify_final_analytics_row_count(
            db, programme_id, user_id, expected_new_rows
        )
        totals["analytics_rows_saved"] = final_rows
        progress.update(totals)
        print(
            f"  Cleanup complete: deleted obsolete={deleted_rows:,}; "
            f"unchanged preserved={totals['analytics_rows_unchanged']:,}; "
            f"new inserted={totals['analytics_rows_inserted']:,}; "
            f"changed updated={totals['analytics_rows_updated']:,}."
        )
        print("  programme_trades: first-stage upsert/verification completed; no trade rows were deleted.")

    print("\nTemporary runtime/result JSON files cleaned up.")
    progress.update(totals)
    progress.update({"status": "COMPLETED", "stage": "complete", "reason": ""})
    print(
        f"{programme_name}: matching and persistence completed | "
        f"pairs={totals['pairs']} | families={totals['families']:,} | "
        f"trades={totals['trades']:,} | programme_trades inserted/updated/unchanged="
        f"{totals.get('programme_trade_rows_inserted', 0):,}/{totals.get('programme_trade_rows_updated', 0):,}/"
        f"{totals.get('programme_trade_rows_unchanged', 0):,} | "
        f"analytics inserted/updated/unchanged="
        f"{totals.get('analytics_rows_inserted', 0):,}/"
        f"{totals.get('analytics_rows_updated', 0):,}/"
        f"{totals.get('analytics_rows_unchanged', 0):,} | "
        f"obsolete analytics deleted={totals.get('analytics_rows_deleted', 0):,} | "
        f"final analytics rows={totals.get('analytics_rows_saved', 0):,} | candles={totals['candles']:,}"
    )
    print("Analytics policy: pending-entry orders are Live only when Begin Test is enabled; all other records are History.")
    return totals

def print_scan_summary(
    scan_number: int,
    elapsed_seconds: float,
    status: str,
    programmes_discovered: int,
    totals: Dict[str, int],
    programme_reports: Sequence[Dict[str, Any]],
    failure: Optional[str] = None,
) -> None:
    """Always print a readable end-of-cycle summary, including failed cycles."""
    print("\n" + "=" * 82, flush=True)
    print(f"PROGRAMME TRAINING SCAN SUMMARY | scan={scan_number} | status={status}", flush=True)
    print("-" * 82, flush=True)
    print(f"Elapsed time                 : {elapsed_seconds:.1f}s", flush=True)
    print(f"Programmes discovered        : {programmes_discovered}", flush=True)
    print(f"Programmes attempted         : {totals.get('programmes_attempted', 0)}", flush=True)
    print(f"Programmes completed         : {totals.get('programmes_completed', 0)}", flush=True)
    print(f"Programmes skipped           : {totals.get('programmes_skipped', 0)}", flush=True)
    print(f"Programmes failed/blocked    : {totals.get('programmes_failed', 0)}", flush=True)
    print("", flush=True)
    print("CALCULATED RESULTS", flush=True)
    print(f"  Analytics pairs processed  : {totals.get('pairs', 0):,}", flush=True)
    print(f"  Matched family records      : {totals.get('families', 0):,}", flush=True)
    print(f"  Configured trade records    : {totals.get('trades', 0):,}", flush=True)
    print(f"  Candle records fetched      : {totals.get('candles', 0):,}", flush=True)
    print("", flush=True)
    print("DATABASE REFRESH RESULTS", flush=True)
    print(f"  New analytics rows inserted: {totals.get('analytics_rows_inserted', 0):,}", flush=True)
    print(f"  Existing rows updated       : {totals.get('analytics_rows_updated', 0):,}", flush=True)
    print(f"  Existing rows unchanged     : {totals.get('analytics_rows_unchanged', 0):,} (no DB write)", flush=True)
    print(f"  Obsolete analytics deleted  : {totals.get('analytics_rows_deleted', 0):,} (after verification)", flush=True)
    print(f"  Final analytics rows        : {totals.get('analytics_rows_saved', 0):,}", flush=True)
    print(
        "  programme_trades inserted  : "
        f"{totals.get('programme_trade_rows_inserted', 0):,} | updated: "
        f"{totals.get('programme_trade_rows_updated', 0):,} | unchanged: "
        f"{totals.get('programme_trade_rows_unchanged', 0):,} | verified: "
        f"{totals.get('programme_trade_rows_verified', 0):,} (no deletes)", flush=True
    )
    print("", flush=True)
    if programme_reports:
        print("PROGRAMME BREAKDOWN", flush=True)
        for report in programme_reports:
            name = report.get("programme_name") or f"Programme {report.get('programme_id', '?')}"
            print(
                f"  {name} (ID {report.get('programme_id', '?')}) | "
                f"{report.get('status', 'UNKNOWN')} | stage: {report.get('stage', 'unknown')}",
                flush=True,
            )
            print(
                f"    pairs={report.get('pairs', 0):,} | "
                f"families={report.get('families', 0):,} | "
                f"trades={report.get('trades', 0):,} | "
                f"candles={report.get('candles', 0):,} | "
                f"inserted/updated/unchanged={report.get('analytics_rows_inserted', 0):,}/"
                f"{report.get('analytics_rows_updated', 0):,}/{report.get('analytics_rows_unchanged', 0):,} | "
                f"obsolete deleted={report.get('analytics_rows_deleted', 0):,} | "
                f"final rows={report.get('analytics_rows_saved', 0):,}",
                flush=True,
            )
            reason = report.get("reason") or report.get("error")
            if reason:
                first_line = str(reason).splitlines()[0]
                print(f"    Note: {first_line[:300]}", flush=True)
    if failure:
        print("", flush=True)
        print("SCAN BLOCKER", flush=True)
        first_line = str(failure).splitlines()[0]
        print(f"  {first_line[:500]}", flush=True)
    print("-" * 82, flush=True)
    print(
        "Begin Test policy: only pending-entry orders can be Live while Begin Test is ON; "
        "otherwise all trade records are History.",
        flush=True,
    )
    print("Programme scope: all programmes (temporary VPS/computer-IP guard disabled).", flush=True)
    print("=" * 82, flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(
        description='Programme Training Test: unrestricted programme discovery with chart-applied root filtering'
    )
    parser.add_argument('--execution-type', choices=('loop', 'once'), default='loop', help='manager execution mode')
    parser.add_argument('--once', action='store_true', help='run one complete scan and exit (legacy alias)')
    parser.add_argument('--interval', type=int, default=10, help='seconds between complete scans (clamped to 5–10; default: 10)')
    parser.add_argument('--programme-id', type=int, default=None,
                        help='optionally limit processing to one programme (no VPS/computer-IP check)')
    parser.add_argument('--bridge-host', default=BRIDGE_HOST)
    parser.add_argument('--bridge-port', type=int, default=BRIDGE_PORT)
    args = parser.parse_args()
    args.interval = min(10, max(5, int(args.interval)))
    run_as_loop = args.execution_type == 'loop' and not args.once

    print("\n" + "=" * 82, flush=True)
    print(f"PROGRAMME TRAINING | MODE: {'LOOP' if run_as_loop else 'ONCE'} | SCAN INTERVAL: {args.interval}s", flush=True)
    print("Scope: all programmes; VPS/computer-IP assignment guard is temporarily disabled", flush=True)
    print("Begin Test: only pending-entry orders are Live while enabled; otherwise History.", flush=True)
    print("Root filter: apply_to_chart = 1 only; disabled roots and their confirmations are skipped.", flush=True)
    print("Persistence order: programme_trades upsert + verify FIRST; then compare analytics, skip identical rows, insert missing/update changed, verify all, and delete only obsolete analytics.", flush=True)
    print("programme_trades: bulk INSERT batches up to 500 rows; changed rows use CASE UPDATE batches; NEVER delete trade rows.", flush=True)
    print("MySQL Remote: safe reads may retry; ambiguous writes are never replayed automatically.", flush=True)
    print("=" * 82, flush=True)

    db = BridgeClient(args.bridge_host, args.bridge_port, BRIDGE_OWNER)
    stopping = False

    def stop(_signum: int, _frame: Any) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, stop)
    if hasattr(signal, 'SIGTERM'):
        signal.signal(signal.SIGTERM, stop)

    try:
        print("VPS/computer-IP guard: DISABLED for testing; programme discovery is unrestricted.", flush=True)
        if args.programme_id is not None:
            print(f"Programme filter: only programme ID {args.programme_id}.", flush=True)

        scan_number = 0
        while not stopping:
            scan_number += 1
            started = time.monotonic()
            programmes_discovered = 0
            programme_reports: List[Dict[str, Any]] = []
            scan_totals: Dict[str, int] = {
                "programmes_attempted": 0, "programmes_completed": 0,
                "programmes_skipped": 0, "programmes_failed": 0,
                "pairs": 0, "families": 0, "trades": 0, "candles": 0,
                "analytics_rows_deleted": 0, "analytics_rows_saved": 0,
                "analytics_rows_inserted": 0, "analytics_rows_updated": 0,
                "analytics_rows_unchanged": 0,
                "programme_trade_rows_inserted": 0, "programme_trade_rows_updated": 0,
                "programme_trade_rows_unchanged": 0, "programme_trade_rows_verified": 0,
            }
            scan_status = "COMPLETE"
            scan_failure: Optional[str] = None

            print("\n" + "#" * 82, flush=True)
            print(f"SCAN {scan_number} START | local time: {datetime.now().astimezone().isoformat(timespec='seconds')}", flush=True)
            print("#" * 82, flush=True)
            try:
                print("[1/3] Discovering programmes (VPS/computer-IP guard disabled)...", flush=True)
                programmes = discover_programmes(db, args.programme_id)
                programmes_discovered = len(programmes)
                print(f"      Found {programmes_discovered} programme row(s) for apply_to_chart evaluation.", flush=True)

                if not programmes:
                    if args.programme_id is None:
                        print("      No programme rows were found.", flush=True)
                    else:
                        print(f"      Programme ID {args.programme_id} was not found.", flush=True)

                print("[2/3] Processing enabled programme roots...", flush=True)
                for programme in programmes:
                    if stopping:
                        scan_status = "INTERRUPTED"
                        break
                    report: Dict[str, Any] = {}
                    try:
                        process_programme(db, programme, report)
                        if report.get("status") == "RUNNING":
                            report["status"] = "COMPLETED"
                            report["stage"] = "complete"
                        elif report.get("status") == "FAILED":
                            scan_status = "PARTIAL - PROGRAMME ERROR"
                            if not scan_failure:
                                scan_failure = str(report.get("reason") or "A programme stage failed.")
                    except BridgeError as exc:
                        report["status"] = "DATABASE BLOCKED"
                        report["error"] = str(exc)
                        scan_status = "INCOMPLETE - DATABASE ERROR"
                        scan_failure = str(exc)
                        log.error(
                            "Database operation failed for programme %s during %s:\n%s",
                            programme.get('id'), report.get('stage', 'unknown stage'), str(exc),
                        )
                    except Exception as exc:
                        report["status"] = "FAILED"
                        report["error"] = str(exc)
                        scan_status = "PARTIAL - PROGRAMME ERROR"
                        log.error(
                            "Programme %s failed during %s:\n%s",
                            programme.get('id'), report.get('stage', 'unknown stage'), traceback.format_exc(),
                        )
                    finally:
                        scan_totals["programmes_attempted"] += 1
                        if report.get("status") == "COMPLETED":
                            scan_totals["programmes_completed"] += 1
                        elif report.get("status") == "SKIPPED":
                            scan_totals["programmes_skipped"] += 1
                        else:
                            scan_totals["programmes_failed"] += 1
                        for key in (
                            "pairs", "families", "trades", "candles",
                            "analytics_rows_deleted", "analytics_rows_saved",
                            "analytics_rows_inserted", "analytics_rows_updated", "analytics_rows_unchanged",
                            "programme_trade_rows_inserted", "programme_trade_rows_updated",
                            "programme_trade_rows_unchanged", "programme_trade_rows_verified",
                        ):
                            scan_totals[key] += int(report.get(key, 0) or 0)
                        programme_reports.append(report.copy())

                    if report.get("status") == "DATABASE BLOCKED":
                        # Do not continue issuing database writes after a SQL/bridge failure.
                        break

                print("[3/3] Preparing scan summary...", flush=True)

            except BridgeError as exc:
                scan_status = "INCOMPLETE - DATABASE ERROR"
                scan_failure = str(exc)
                log.error("Programme discovery failed because MySQL Remote was unavailable:\n%s", str(exc))
            except Exception as exc:
                scan_status = "INCOMPLETE - UNEXPECTED ERROR"
                scan_failure = str(exc)
                log.error("Unexpected scan failure:\n%s", traceback.format_exc())
            finally:
                print_scan_summary(
                    scan_number=scan_number,
                    elapsed_seconds=time.monotonic() - started,
                    status=scan_status,
                    programmes_discovered=programmes_discovered,
                    totals=scan_totals,
                    programme_reports=programme_reports,
                    failure=scan_failure,
                )

            if not run_as_loop:
                print("One-shot scan finished.", flush=True)
                if scan_status.startswith("INCOMPLETE") or scan_status.startswith("PARTIAL"):
                    return 2
                break

            if scan_status.startswith("INCOMPLETE"):
                db.close()
                db = BridgeClient(args.bridge_host, args.bridge_port, BRIDGE_OWNER)
                print("The worker remains active. It will retry on the next scan.", flush=True)

            sleep_for = args.interval
            print(f"Next scan starts in {sleep_for} second(s).", flush=True)
            for _ in range(sleep_for):
                if stopping:
                    break
                time.sleep(1)
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
