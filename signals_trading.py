
"""
 signals_trading.py
Programme-scoped trade pipeline orchestrator.

Runs programme-requested MT5 Breakeven / Close actions first, then the
restricted-timerange stage + recent-highest-balance target stage and the
existing investor trading pipeline. Programme requests are independent of
programme_training_configuration and retry until MT5 confirms the action.

ARCHITECTURE
------------
* Same bridge pattern as brokerdetails.py: this file is a TCP CLIENT of
  the long-running mysqlremote.py bridge. It does NOT launch Chrome.
* The DATABASE is the source of truth. No local folder scanning, no JSON
  reads, no JSON writes. EVER.
* Every worker process (one per investor) acquires its OWN dedicated
  channel from the bridge. Channels are isolated.
* Helpers (db_query, _BridgeClient, channel mgmt, time helpers) live at
  MODULE scope so all future functions in this file can share one channel
  per worker.
* Normal investor-side reconciliation remains subaccount-aware and uses the
  investor copy's tradeid/magic plus chart identity and programme_analytics groups.
  This is part of the investor's own execution pipeline, not programme authority.
* The programme's explicit manual request path selects MT5 tickets primarily
  by `magic == programme_trades.tradeid`, then verifies exact symbol and BUY/SELL
  direction. All child tickets sharing that magic are managed; volume and prices
  are not used as identity fields.

RESTRICTED TIMERANGE SEMANTICS
------------------------------
Restriction is PROGRAMME-SCOPED, not investor-scoped.

Two stages, evaluated in order:

  STAGE 1 — Restricted Dates
    Query `accountmanagement_restricted_dates` for the programme.
    Convert "now" to the programme's timezone, format as YYYY-MM-DD.
    If that exact date appears in the list -> FULL-DAY LOCKOUT.
    No further checks are performed for that programme.

  STAGE 2 — Restricted Days
    Only reached when Stage 1 finds nothing for today.
    Query `accountmanagement_restricted_days` for the programme and
    evaluate the window for today's weekday using the strict rule
    below.

WINDOW RULE (strict, Stage 2 only)
----------------------------------
`from_time` and `to_time` define an inclusive span. `now` is RESTRICTED
if it lies between `from_time` and `to_time` inclusive. Anything outside
that span is FREE.

  * 12:00 AM -> 12:00 PM  restricts 12:00 AM..11:59 AM. Noon onward = free.
  * 12:00 AM -> 12:00 AM  restricts the WHOLE day.
  * 12:00 AM -> 2:00 AM   restricts 12:00 AM..2:00 AM. 2:01 AM onward = free.
  * Cross-midnight windows (e.g. 10:00 PM -> 2:00 AM) are handled.

A "NULL" timezone (or empty) is NOT an error — it means the programme has
not configured a timezone, and the restriction check is skipped for that
programme.

ACTIVE INVESTOR DETECTION
-------------------------
VPS eligibility and investor entitlement are separate:

* The programme VPS IP identifies the owner programme and its active
  programme-VPS followers. Account-VPS tables are not consulted here.
* An investor/subaccount is eligible only when a `programme_revenue_deals`
  row associates that exact pair with the programme AND the matching
  `harvhub.id` + `harvhub.sub_account_id` row has
  `account_contract_status = 'contract_active'`. This mirrors the PHP investor
  synchronization rule exactly.
* The programme VPS IP scopes which programmes can run on this computer; it
  does not replace or weaken the active investor eligibility check.
* `programme_trades` remains the sole signal-copy and manual-request authority.
  An active investor worker can run its own programme account-management stages
  even before a new inherited signal copy exists.
* Programme-owned `programme_trades` rows (`investor_id IS NULL`) remain the
  canonical signal and manual-request source. Investor copies are the
  account-specific signal records.

PROGRAMME AUTHORITY
-------------------
Programme master trades do not control investor sizing or account-management
stages. The investor pipeline continues to use the programme's own
`accountmanagement_*` configuration against that investor's MT5 account.
The programme has a narrow execution authority only through the explicit
`order_request_status` manual-request fields (`order closing requested`,
`position closing requested`, `breakeven requested`). Those requests are
matched by canonical MT5 magic (`programme_trades.tradeid`) and verified by
symbol/direction. They may affect all child tickets for that signal; volume is
used only when MT5 requires it to send a close, never to identify a ticket.

PIPELINE ORDER AND GATES
------------------------
Programme-requested Breakeven / Close commands run immediately after each
investor MT5 account is connected and its login/server identity is verified.
They run before both normal trading gates and do not depend on programme
training configuration. Failed actions retain a reason and retry each cycle.

The normal automated trading stages then use the two short-circuits below:

  GATE 1 — Restricted timerange
    If the investor's programme is currently restricted (restricted
    date / full restricted day / inside restricted time window), skip
    all normal automated trading stages, including recent_highest_balance_revenue_target_db.
    Programme-requested Breakeven / Close commands have already run.

  GATE 2 — Revenue Target alarm
    recent_highest_balance_revenue_target_db() populates the module-global
    `recent_highest_alert`. If `is_triggered` is True after that stage
    runs (target met, or pre-pending alarm fired), skip subsequent normal
    automated trading stages for this cycle. Programme-requested manual
    actions have already been processed before either gate.
"""

import os
import sys
import time
import json
import socket
import struct
import threading
import traceback
import multiprocessing as mp
from datetime import datetime, timedelta

try:
    import pytz
    from zoneinfo import ZoneInfo
except Exception:
    pytz = None
    ZoneInfo = None

# ---------------------------------------------------------------------------
# MT5 import guard — everything MT5-related must go through mt5_available().
# ---------------------------------------------------------------------------
try:
    import MetaTrader5 as mt5
except Exception:
    mt5 = None


# ==============================================================================
#  REBRANDED MT5 PATHS — must match brokerdetails.py exactly
# ==============================================================================
DEFAULT_MT5_PATH     = r"C:\xampp\htdocs\harvhubAI\mt5\MetaTrader 5"
MT5_DESTINATION_PATH = r"C:\xampp\htdocs\harvhubAI\mt5"


# ==============================================================================
#  BRIDGE CLIENT — same protocol as brokerdetails.py, one channel per worker
# ==============================================================================
BRIDGE_HOST = "127.0.0.1"
BRIDGE_PORT = 5599

BRIDGE_OWNER = "signals_trading"

_HEADER = struct.Struct(">I")
MAX_FRAME = 64 * 1024 * 1024

_MT5_IPC_LOCK = threading.RLock()


class _BridgeError(Exception):
    pass


def _bridge_sql_is_read(sql):
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
    return bool(text) and text.split(None, 1)[0].upper() in {
        'SELECT', 'SHOW', 'DESCRIBE', 'DESC', 'EXPLAIN', 'WITH'
    }


def _bridge_error_is_transient(message):
    value = str(message or '').lower()
    return any(marker in value for marker in (
        "unexpected token '<'", 'not valid json', 'failed to fetch',
        'browser not available', 'channel window missing', 'window missing',
        'unknown channel_id', 'channel limit reached', 'failed to spawn dom window',
        'query input failed', 'timed out', 'timeout (last=',
        'cannot talk to bridge', 'bridge closed connection',
        'connection refused', 'connection reset', 'temporarily unavailable',
    ))


class _BridgeClient:
    """Channel-aware bridge client with safe idempotent-read retry behaviour."""

    def __init__(self, host=BRIDGE_HOST, port=BRIDGE_PORT, owner=BRIDGE_OWNER):
        self.host = host
        self.port = int(port)
        self.owner = f'{owner}:{os.getpid()}'
        self.channel_id = None
        self._channel_lock = threading.Lock()
        self._sock = None
        self._sock_lock = threading.RLock()

    @staticmethod
    def _recv_exact(sock, count):
        data = bytearray()
        while len(data) < count:
            chunk = sock.recv(count - len(data))
            if not chunk:
                return None
            data.extend(chunk)
        return bytes(data)

    def _connect(self):
        sock = socket.create_connection((self.host, self.port), timeout=15)
        sock.settimeout(120)
        return sock

    def _close_sock_locked(self):
        if self._sock is not None:
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None

    def _send_request(self, req):
        payload = json.dumps(req, ensure_ascii=False, default=str).encode('utf-8')
        if len(payload) > MAX_FRAME:
            raise _BridgeError(f'bridge request exceeds frame limit: {len(payload)} bytes')
        frame = _HEADER.pack(len(payload)) + payload
        safe_to_replay = ('sql' not in req) or _bridge_sql_is_read(req.get('sql'))
        last_error = None
        with self._sock_lock:
            for attempt in range(2):
                sent = False
                try:
                    if self._sock is None:
                        self._sock = self._connect()
                    self._sock.sendall(frame)
                    sent = True
                    header = self._recv_exact(self._sock, _HEADER.size)
                    if header is None:
                        raise _BridgeError('bridge closed connection (no header)')
                    (length,) = _HEADER.unpack(header)
                    if length <= 0 or length > MAX_FRAME:
                        raise _BridgeError(f'invalid bridge response frame length: {length}')
                    body = self._recv_exact(self._sock, length)
                    if body is None:
                        raise _BridgeError('bridge closed connection (no body)')
                    result = json.loads(body.decode('utf-8'))
                    if not isinstance(result, dict):
                        raise _BridgeError('bridge returned a non-object response')
                    return result
                except Exception as exc:
                    last_error = exc
                    self._close_sock_locked()
                    if sent and not safe_to_replay:
                        break
                    if attempt:
                        break
        raise _BridgeError(f'cannot talk to bridge {self.host}:{self.port}: {last_error}')

    def acquire_channel(self):
        with self._channel_lock:
            if self.channel_id is not None:
                return self.channel_id
            response = self._send_request({'op': 'acquire_channel', 'owner': self.owner})
            if response.get('status') != 'success':
                raise _BridgeError(f"acquire_channel failed: {response.get('message')}")
            self.channel_id = int(response['channel_id'])
            print(f'[bridge] {self.owner}: acquired channel {self.channel_id}', flush=True)
            return self.channel_id

    def release_channel(self):
        with self._channel_lock:
            cid, self.channel_id = self.channel_id, None
        if cid is not None:
            try:
                self._send_request({'op': 'release_channel', 'channel_id': cid})
                print(f'[bridge] {self.owner}: released channel {cid}', flush=True)
            except Exception as exc:
                print(f'[bridge] {self.owner}: release channel {cid} warning: {exc}', flush=True)
        with self._sock_lock:
            self._close_sock_locked()

    def put(self, request, timeout=None):
        req = dict(request)
        has_sql = 'sql' in req
        is_read = _bridge_sql_is_read(req.get('sql')) if has_sql else True
        delay = 1.0
        last_log = 0.0
        attempt = 0
        while True:
            attempt += 1
            try:
                if has_sql:
                    req['channel_id'] = self.acquire_channel()
                response = self._send_request(req)
            except _BridgeError as exc:
                if not is_read:
                    raise
                if time.monotonic() - last_log >= 15:
                    print(f'[bridge] {self.owner}: read request unavailable ({exc}); waiting {delay:.1f}s and reconnecting', flush=True)
                    last_log = time.monotonic()
                self.release_channel()
                time.sleep(delay)
                delay = min(10.0, delay * 1.6)
                continue

            message = str((response or {}).get('message') or '')
            if (response or {}).get('status') == 'success':
                return response
            if 'unknown channel_id' in message.lower():
                # This response is generated before SQL execution. Renewal and
                # replay are safe for reads and writes alike.
                self.release_channel()
                if attempt < 3:
                    continue
            elif ((response or {}).get('transient') or _bridge_error_is_transient(message)) and is_read:
                if time.monotonic() - last_log >= 15:
                    print(f'[bridge] {self.owner}: PHP query endpoint returned a transient HTML/JSON error; renewing channel and retrying read #{attempt}', flush=True)
                    last_log = time.monotonic()
                self.release_channel()
                time.sleep(delay)
                delay = min(10.0, delay * 1.6)
                continue
            elif ((response or {}).get('transient') or _bridge_error_is_transient(message)) and not is_read:
                response['message'] = (
                    'Transient upstream response during a write; result is uncertain and '
                    'the client deliberately did not replay it. ' + message
                )
            return response

    def get(self, timeout=None, block=True):
        raise _BridgeError('request/response mode is active; get() is not supported')


# ==============================================================================
#  MODULE-SCOPE HELPERS (shared by all future functions)
# ==============================================================================
_db = None
recent_highest_alert = None
recovery_generator_alert = None   # populated by recovery_generator_system_db()


def _worker_init():
    """Each worker process gets its own bridge client -> its own channel."""
    global _db
    _db = _BridgeClient()


def db_query(sql, params=None, timeout=20.0, quiet=False):
    global _db
    if _db is None:
        _db = _BridgeClient()

    start = time.perf_counter()
    try:
        result = _db.put(
            {'sql': sql, 'params': params, 'timeout': timeout},
        )
    except _BridgeError as e:
        return {
            'status': 'error',
            'message': f'bridge error: {e}',
            'results': [], 'affected_rows': 0,
            'query_id': None, 'raw_sql': sql,
            'elapsed': time.perf_counter() - start,
        }

    if not quiet and result.get('status') != 'success':
        print(f"      [SQL FAIL] {result.get('message')}")
        print(f"      [SQL WAS ] {sql.strip()[:200]}")
        print(f"      [PARAMS  ] {params}")

    return result

def db_query_many(statements, params_list=None, timeout=60.0, quiet=False):
    """
    Send multiple SQL statements to the bridge in ONE request.

    The PHP (phpmyadmin_tablesfetch.php) already splits ';'-separated
    SQL, schedules it dependency-safely, executes it, and returns a
    per-statement results array. The template renders one .stmt-block
    per statement. The bridge (mysqlremote.py) preserves that structure
    in resp['statement_results'] when the SQL contains more than one
    statement.

    Returns:
        {
          'status':  'success' | 'partial' | 'error',
          'message': ...,
          'statements': [
              {'status': 'success'|'empty'|'error'|'skipped',
               'sql': <original>,
               'type': 'SELECT'|'INSERT'|...,
               'rows': [...],         # for read queries
               'affected_rows': int,  # for write queries
               'message': '...'},
              ...
          ],
          'elapsed': float,
        }

    On any failure, returns the same shape with status='error' and
    statements=[] so callers can treat it uniformly.
    """
    if not statements:
        return {
            'status': 'success', 'message': 'no statements',
            'statements': [], 'elapsed': 0.0,
        }

    joined = ";\n".join(s.strip().rstrip(';') for s in statements if s and s.strip())
    if not joined:
        return {
            'status': 'success', 'message': 'no executable statements',
            'statements': [], 'elapsed': 0.0,
        }

    # The bridge only substitutes %s once per request (see _substitute_params).
    # So a batch request must not rely on parameter binding. Callers that
    # need params should inline them before calling.
    if params_list is not None:
        raise ValueError(
            "db_query_many does not support parameters. Inline the values "
            "into the statement strings, or call db_query per statement."
        )

    if _db is None:
        _worker_init()

    start = time.perf_counter()
    try:
        result = _db.put({'sql': joined, 'timeout': timeout})
    except _BridgeError as e:
        return {
            'status': 'error',
            'message': f'bridge error: {e}',
            'statements': [], 'elapsed': time.perf_counter() - start,
        }

    elapsed = time.perf_counter() - start

    if not quiet and result.get('status') != 'success':
        print(f"      [SQL-MANY FAIL] {result.get('message')}")
        print(f"      [SQL-MANY WAS ] {joined[:300]}")

    stmts = result.get('statement_results')
    if stmts is None:
        # Bridge didn't preserve per-statement structure (older bridge).
        # Fall back to treating the whole response as one statement.
        stmts = [{
            'status': result.get('status', 'error'),
            'sql': joined,
            'type': 'OTHER',
            'rows': result.get('results') or [],
            'affected_rows': result.get('affected_rows') or 0,
            'message': result.get('message', ''),
        }]

    return {
        'status': result.get('status', 'error'),
        'message': result.get('message', ''),
        'statements': stmts,
        'elapsed': elapsed,
    }


# ---------------------------------------------------------------------------
# Programme-scoped notification PDO adapter (single instance per worker)
# ---------------------------------------------------------------------------
try:
    import notification_service as _notif_service
    _NOTIF_PDO = _notif_service.make_pdo(db_query)
except Exception:
    _notif_service = None
    _NOTIF_PDO = None


def _is_ok(res):
    return bool(res) and res.get('status') == 'success'


def db_fetch_all(sql, params=None, timeout=20.0, quiet=True):
    """SELECT ... -> list[dict]. Empty list on failure."""
    res = db_query(sql, params, timeout=timeout, quiet=quiet)
    if not _is_ok(res):
        return []
    return [_normalize_row(r) for r in (res.get('results') or [])]


def db_fetch_one(sql, params=None, timeout=20.0, quiet=True):
    """SELECT ... LIMIT 1 -> dict or None."""
    rows = db_fetch_all(sql, params, timeout=timeout, quiet=quiet)
    return rows[0] if rows else None


def db_fetch_scalar(sql, params=None, timeout=20.0, quiet=True, default=None):
    """SELECT one-col one-row -> value or default."""
    row = db_fetch_one(sql, params, timeout=timeout, quiet=quiet)
    if not row:
        return default
    for v in row.values():
        return v
    return default


def db_execute(sql, params=None, timeout=20.0, quiet=True):
    """INSERT/UPDATE/DELETE -> (ok: bool, affected: int, message: str)."""
    res = db_query(sql, params, timeout=timeout, quiet=quiet)
    if not _is_ok(res):
        return False, 0, res.get('message', 'error')
    return True, int(res.get('affected_rows') or 0), res.get('message', '')


def _pick_first_key(row, *candidate_keys):
    if not isinstance(row, dict):
        return None
    lowered = {(k.strip().lower() if isinstance(k, str) else k): v
               for k, v in row.items()}
    for key in candidate_keys:
        if key is None:
            continue
        lk = key.strip().lower()
        if lk in lowered and lowered[lk] not in (None, '', 'NULL'):
            return lowered[lk]
    return None


def _raw_first_key(row, *candidate_keys):
    if not isinstance(row, dict):
        return None
    lowered = {(k.strip().lower() if isinstance(k, str) else k): v
               for k, v in row.items()}
    for key in candidate_keys:
        if key is None:
            continue
        lk = key.strip().lower()
        if lk in lowered:
            return lowered[lk]
    return None


def _coerce_int(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    try:
        s = str(value).strip()
        if not s or s.lower() in ('null', 'none', 'nan'):
            return None
        if '.' in s:
            return int(float(s))
        return int(s)
    except (ValueError, TypeError):
        return None


def _coerce_float(value, default=None):
    if value is None:
        return default
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return float(value)
    try:
        s = str(value).strip()
        if not s or s.lower() in ('null', 'none', 'nan'):
            return default
        return float(s)
    except (ValueError, TypeError):
        return default


def _normalize_row(row):
    if row is None:
        return None
    if isinstance(row, dict):
        return {
            (k.strip().lower() if isinstance(k, str) else k): v
            for k, v in row.items()
        }
    return None


def _is_nullish(value):
    """True if the value is effectively unset (None, '', 'NULL', 'null')."""
    if value is None:
        return True
    s = str(value).strip().lower()
    return s in ('', 'null', 'none', 'nan')


def to_bool(value, default=False):
    """Parse booleans from strings/numbers/bools."""
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "on", "enabled")
    return default


# ---------------------------------------------------------------------------
# Programme trade ownership + live-source eligibility.
# PHP stores programme_trades.userid as the PROGRAMME OWNER (programme.userid)
# and stores investor/subaccount ownership in separate investor_* columns.
# ---------------------------------------------------------------------------
_PROGRAMME_USERID_CACHE = {}
_DB_TABLE_COLUMNS_CACHE = {}


def resolve_programme_userid(programme_id, job=None):
    """Return the owner userid for a programme; never substitute investor_id."""
    if isinstance(job, dict):
        supplied = _coerce_int(job.get('programme_userid'))
        if supplied and supplied > 0:
            _PROGRAMME_USERID_CACHE[int(programme_id)] = supplied
            return supplied
    pid = _coerce_int(programme_id)
    if not pid or pid <= 0:
        return None
    cached = _PROGRAMME_USERID_CACHE.get(pid)
    if cached and cached > 0:
        return cached
    row = db_fetch_one(
        "SELECT userid FROM programme WHERE id = %s LIMIT 1",
        [pid], timeout=10.0, quiet=True,
    )
    owner_id = _coerce_int(_raw_first_key(row or {}, 'userid'))
    if owner_id and owner_id > 0:
        _PROGRAMME_USERID_CACHE[pid] = owner_id
        return owner_id
    return None


def _db_table_columns(table_name):
    """Cached, allowlisted schema introspection for known HarvHub tables."""
    allowed = {
        'programme_trades', 'programme_trade_chart_identity',
        'programme_analytics', 'programme', 'harvhub',
        'server_operational_settings',
    }
    table = str(table_name or '').strip().lower()
    if table not in allowed:
        return set()
    if table in _DB_TABLE_COLUMNS_CACHE:
        return _DB_TABLE_COLUMNS_CACHE[table]
    rows = db_fetch_all(f"SHOW COLUMNS FROM `{table}`", timeout=10.0, quiet=True)
    cols = set()
    for row in rows:
        name = _raw_first_key(row, 'Field')
        if name:
            cols.add(str(name).strip().lower())
    _DB_TABLE_COLUMNS_CACHE[table] = cols
    return cols


def _parse_lagos_datetime(value):
    """Parse DB/ISO timestamps into naive Africa/Lagos datetimes."""
    from datetime import date, timezone
    try:
        from zoneinfo import ZoneInfo
        lagos_tz = ZoneInfo('Africa/Lagos')
    except Exception:
        lagos_tz = timezone(timedelta(hours=1))

    if value is None or str(value).strip() == '':
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, date):
        dt = datetime(value.year, value.month, value.day)
    else:
        raw = str(value).strip()
        if raw.isdigit() and len(raw) in (10, 13):
            try:
                epoch = float(raw) / (1000.0 if len(raw) == 13 else 1.0)
                dt = datetime.fromtimestamp(epoch, tz=lagos_tz)
            except Exception:
                dt = None
        else:
            dt = None
            iso = raw.replace('Z', '+00:00')
            try:
                dt = datetime.fromisoformat(iso)
            except Exception:
                pass
            if dt is None:
                for fmt in (
                    '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d',
                    '%Y/%m/%d %H:%M:%S', '%Y/%m/%d %H:%M', '%Y/%m/%d',
                    '%B %d, %Y %H:%M:%S', '%B %d, %Y',
                    '%d-%m-%Y %H:%M:%S', '%d-%m-%Y',
                ):
                    try:
                        dt = datetime.strptime(raw, fmt)
                        break
                    except Exception:
                        continue
    if dt is None:
        return None
    try:
        if dt.tzinfo is not None:
            return dt.astimezone(lagos_tz).replace(tzinfo=None)
    except Exception:
        pass
    return dt.replace(tzinfo=None)



def _strict_db_rows(sql, params=None, timeout=15.0):
    """Return (rows, error). Unlike db_fetch_all, preserve query failures."""
    res = db_query(sql, params, timeout=timeout, quiet=True)
    if not _is_ok(res):
        return None, (res or {}).get('message', 'database query failed')
    return [_normalize_row(r) for r in (res.get('results') or [])], None


# Must remain synchronized with programme_training_test.py. The limit is read
# from the normalized operational settings table, not server_account.
DEFAULT_TRADES_SETUP_PER_GROUP = 100
_TRADES_SETUP_PER_GROUP_CACHE = None


def load_trades_setup_per_group(force_refresh=False):
    """Read the configured maximum group-local family_number value."""
    global _TRADES_SETUP_PER_GROUP_CACHE
    if _TRADES_SETUP_PER_GROUP_CACHE is not None and not force_refresh:
        return _TRADES_SETUP_PER_GROUP_CACHE

    rows, error = _strict_db_rows(
        "SELECT `trades_setup_per_group` FROM `server_operational_settings` "
        "WHERE `id` = 1 LIMIT 1",
        timeout=20.0,
    )
    configured = _coerce_int(_raw_first_key(rows[0], 'trades_setup_per_group')) if rows else None
    if configured is not None and configured > 0:
        _TRADES_SETUP_PER_GROUP_CACHE = configured
        print(
            f"[SETUP-GROUPS] trades_setup_per_group={configured:,} "
            "(server_operational_settings.id=1)",
            flush=True,
        )
    else:
        _TRADES_SETUP_PER_GROUP_CACHE = DEFAULT_TRADES_SETUP_PER_GROUP
        detail = f"; database read: {error}" if error else ''
        print(
            f"[SETUP-GROUPS] setting unavailable/invalid; using default "
            f"{DEFAULT_TRADES_SETUP_PER_GROUP}{detail}",
            flush=True,
        )
    return _TRADES_SETUP_PER_GROUP_CACHE


def _valid_setup_group_identity(row, setup_group_limit=None):
    """A valid chart setup has a positive group ID and a local family number in range."""
    limit = setup_group_limit or load_trades_setup_per_group()
    group_id = _coerce_int(_raw_first_key(row or {}, 'setup_group_id'))
    family_number = _coerce_int(_raw_first_key(row or {}, 'family_number'))
    return bool(
        group_id is not None and group_id > 0
        and family_number is not None and 1 <= family_number <= int(limit)
    )


def _identity_text(value):
    if _is_nullish(value):
        return ''
    return str(value).strip().lower()


def _identity_int_or_zero(value):
    if _is_nullish(value):
        return 0
    val = _coerce_int(value)
    return val if val is not None else str(value).strip()


def fetch_subaccount_magic_catalog(investor_id, sub_account_id):
    """Map MT5 magic values only from this exact subaccount's inherited programme_trades copies.

    programme_revenue_deals is deliberately not an ownership/authorization
    source for this trading process. A programme/subaccount relationship is
    evidenced by the investor-specific programme_trades rows themselves.
    Canonical master rows are included only for programme IDs already evidenced
    by at least one inherited copy for this exact investor/subaccount; this is
    used solely to detect reused magic numbers safely.
    """
    inv_id = _coerce_int(investor_id)
    sub_id = _coerce_int(sub_account_id)
    empty = {
        'by_magic': {}, 'all_magic': set(), 'ambiguous': set(),
        'programme_ids': set(), 'ok': False, 'reason': None
    }
    if not inv_id or inv_id <= 0 or not sub_id or sub_id <= 0:
        empty['reason'] = 'missing investor/subaccount identifiers'
        return empty

    pt_cols = _db_table_columns('programme_trades')
    required = {
        'programmeid', 'investor_id', 'investor_sub_account_id', 'tradeid'
    }
    if not required.issubset(pt_cols):
        empty['reason'] = 'programme_trades is missing programme/subaccount/tradeid columns'
        return empty

    copy_sql = (
        "SELECT programmeid, tradeid FROM programme_trades "
        f"WHERE investor_id = {int(inv_id)} "
        f"AND investor_sub_account_id = {int(sub_id)} "
        "AND tradeid IS NOT NULL"
    )
    copy_rows, err = _strict_db_rows(copy_sql, None, timeout=15.0)
    if copy_rows is None:
        empty['reason'] = f'could not read inherited programme-trades copies: {err}'
        return empty

    by_magic = {}
    programme_ids = set()
    for row in copy_rows:
        magic = _coerce_int(_raw_first_key(row, 'tradeid'))
        pid = _coerce_int(_raw_first_key(row, 'programmeid'))
        if magic is None or magic <= 0 or pid is None or pid <= 0:
            continue
        programme_ids.add(pid)
        by_magic.setdefault(magic, set()).add(pid)

    # Also inspect canonical master trade IDs for the programmes already
    # evidenced by this account's inherited copies. Do not import unrelated
    # revenue-deal or account-VPS programme relationships.
    if programme_ids:
        programme_id_sql = ','.join(str(int(pid)) for pid in sorted(programme_ids))
        canonical_sql = (
            "SELECT programmeid, tradeid FROM programme_trades "
            "WHERE investor_id IS NULL AND tradeid IS NOT NULL "
            f"AND programmeid IN ({programme_id_sql})"
        )
        canonical_rows, err = _strict_db_rows(
            canonical_sql, None, timeout=20.0
        )
        if canonical_rows is None:
            empty['reason'] = f'could not read canonical programme magic catalogue: {err}'
            return empty
        for row in canonical_rows:
            magic = _coerce_int(_raw_first_key(row, 'tradeid'))
            pid = _coerce_int(_raw_first_key(row, 'programmeid'))
            if magic is None or magic <= 0 or pid is None or pid <= 0:
                continue
            by_magic.setdefault(magic, set()).add(pid)

    ambiguous = {magic for magic, pids in by_magic.items() if len(pids) > 1}
    return {
        'by_magic': by_magic,
        'all_magic': set(by_magic),
        'ambiguous': ambiguous,
        'programme_ids': programme_ids,
        'ok': True,
        'reason': None,
    }

def fetch_programme_analytics_magic_groups(
    programme_id, programme_userid, require_live=False,
):
    """Resolve canonical programme tradeids through chart identity + analytics groups.

    The mapping chain is:
      programme_trades canonical row -> programme_trade_chart_identity
      -> programme_analytics (root/confirmation/family/symbol/timeframe).

    SQL is intentionally kept to simple table-scoped SELECTs. Identity matching
    happens in Python so it does not depend on correlated subqueries, SQL casts,
    or bridge-specific JOIN/EXISTS handling.
    """
    pid = _coerce_int(programme_id)
    owner_id = _coerce_int(programme_userid)
    empty = {'by_magic': {}, 'ambiguous': set(), 'ok': False,
             'reason': None, 'canonical_tradeids': set(), 'groups_matched': 0}
    if not pid or pid <= 0 or not owner_id or owner_id <= 0:
        empty['reason'] = 'missing programme id or programme owner userid'
        return empty

    pt_cols = _db_table_columns('programme_trades')
    ci_cols = _db_table_columns('programme_trade_chart_identity')
    pa_cols = _db_table_columns('programme_analytics')
    required_pt = {
        'id', 'userid', 'programmeid', 'investor_id', 'tradeid', 'setup_group_id', 'symbol'
    }
    required_ci = {
        'programme_trade_id', 'userid', 'programmeid', 'root_id',
        'confirmation_root_id', 'family_number', 'setup_group_id', 'symbol',
        'timeframe', 'source_timeframe'
    }
    required_pa = {
        'userid', 'programme_id', 'root_id', 'confirmation_root_id',
        'family_number', 'setup_group_id', 'symbol', 'timeframe'
    }
    if not required_pt.issubset(pt_cols):
        empty['reason'] = 'programme_trades schema is missing canonical trade identity columns'
        return empty
    if not required_ci.issubset(ci_cols):
        empty['reason'] = 'programme_trade_chart_identity is missing chart group identity columns'
        return empty
    if not required_pa.issubset(pa_cols):
        empty['reason'] = 'programme_analytics is missing analytics group identity columns'
        return empty
    if require_live and not {'live_trades', 'trade_configured'}.issubset(pa_cols):
        empty['reason'] = 'programme_analytics is missing live_trades/trade_configured flags'
        return empty

    setup_group_limit = load_trades_setup_per_group()
    pa_select = [
        'root_id', 'confirmation_root_id', 'family_number',
        'setup_group_id', 'symbol', 'timeframe'
    ]
    for col in ('source_timeframe', 'live_trades', 'trade_configured', 'matched_family'):
        if col in pa_cols:
            pa_select.append(col)
    ci_select = [
        'programme_trade_id', 'root_id', 'confirmation_root_id',
        'family_number', 'setup_group_id', 'symbol', 'timeframe', 'source_timeframe'
    ]
    pt_select = ['id', 'tradeid', 'setup_group_id', 'symbol']
    for col in ('timeframe', 'ordertype', 'contract_id'):
        if col in pt_cols:
            pt_select.append(col)

    pa_sql = (
        f"SELECT {', '.join(f'`{c}`' for c in pa_select)} "
        "FROM programme_analytics WHERE userid = %s AND programme_id = %s"
    )
    ci_sql = (
        f"SELECT {', '.join(f'`{c}`' for c in ci_select)} "
        "FROM programme_trade_chart_identity WHERE userid = %s AND programmeid = %s"
    )
    pt_sql = (
        f"SELECT {', '.join(f'`{c}`' for c in pt_select)} "
        "FROM programme_trades WHERE userid = %s AND programmeid = %s "
        "AND investor_id IS NULL"
    )
    pa_rows, err = _strict_db_rows(pa_sql, [owner_id, pid], timeout=20.0)
    if pa_rows is None:
        empty['reason'] = f'could not read programme_analytics groups: {err}'
        return empty
    ci_rows, err = _strict_db_rows(ci_sql, [owner_id, pid], timeout=20.0)
    if ci_rows is None:
        empty['reason'] = f'could not read programme_trade_chart_identity: {err}'
        return empty
    master_rows, err = _strict_db_rows(pt_sql, [owner_id, pid], timeout=20.0)
    if master_rows is None:
        empty['reason'] = f'could not read canonical programme_trades rows: {err}'
        return empty

    master_by_id = {}
    master_tid_counts = {}
    for row in master_rows:
        row_id = _coerce_int(_raw_first_key(row, 'id'))
        tid = _coerce_int(_raw_first_key(row, 'tradeid'))
        if row_id is None or tid is None or tid <= 0:
            continue
        master_by_id[row_id] = row
        master_tid_counts[tid] = master_tid_counts.get(tid, 0) + 1

    def _group_match(ci, pa):
        # family_number is now group-local and may repeat in another group.
        # Therefore setup_group_id is a mandatory part of the group identity.
        if not _valid_setup_group_identity(ci, setup_group_limit):
            return False
        if not _valid_setup_group_identity(pa, setup_group_limit):
            return False
        ci_group_id = _coerce_int(_raw_first_key(ci, 'setup_group_id'))
        pa_group_id = _coerce_int(_raw_first_key(pa, 'setup_group_id'))
        if ci_group_id is None or ci_group_id != pa_group_id:
            return False

        ci_root = _identity_int_or_zero(_raw_first_key(ci, 'root_id'))
        pa_root = _identity_int_or_zero(_raw_first_key(pa, 'root_id'))
        if ci_root in (0, '') or ci_root != pa_root:
            return False
        if _identity_int_or_zero(_raw_first_key(ci, 'confirmation_root_id')) != \
           _identity_int_or_zero(_raw_first_key(pa, 'confirmation_root_id')):
            return False
        if _identity_int_or_zero(_raw_first_key(ci, 'family_number')) != \
           _identity_int_or_zero(_raw_first_key(pa, 'family_number')):
            return False
        ci_symbol = _identity_text(_raw_first_key(ci, 'symbol'))
        pa_symbol = _identity_text(_raw_first_key(pa, 'symbol'))
        if not ci_symbol or ci_symbol != pa_symbol:
            return False
        ci_tfs = {
            _identity_text(_raw_first_key(ci, 'timeframe')),
            _identity_text(_raw_first_key(ci, 'source_timeframe')),
        } - {''}
        pa_tfs = {_identity_text(_raw_first_key(pa, 'timeframe'))}
        if 'source_timeframe' in pa_cols:
            pa_tfs.add(_identity_text(_raw_first_key(pa, 'source_timeframe')))
        pa_tfs.discard('')
        if not ci_tfs.intersection(pa_tfs):
            return False
        if 'matched_family' in pa_cols:
            mf = _raw_first_key(pa, 'matched_family')
            if not _is_nullish(mf):
                mf_int = _coerce_int(mf)
                if (mf_int == 0) or (mf_int is None and not to_bool(mf, default=True)):
                    return False
        if require_live:
            if not to_bool(_raw_first_key(pa, 'live_trades'), default=False):
                return False
            if not to_bool(_raw_first_key(pa, 'trade_configured'), default=False):
                return False
        return True

    groups_by_tid = {}
    for ci in ci_rows:
        canonical_id = _coerce_int(_raw_first_key(ci, 'programme_trade_id'))
        master = master_by_id.get(canonical_id)
        if not master:
            continue
        ci_symbol = _identity_text(_raw_first_key(ci, 'symbol'))
        master_symbol = _identity_text(_raw_first_key(master, 'symbol'))
        if not ci_symbol or not master_symbol or ci_symbol != master_symbol:
            continue
        ci_group_id = _coerce_int(_raw_first_key(ci, 'setup_group_id'))
        master_group_id = _coerce_int(_raw_first_key(master, 'setup_group_id'))
        if ci_group_id is None or ci_group_id <= 0 or ci_group_id != master_group_id:
            continue
        matching_pa = [pa for pa in pa_rows if _group_match(ci, pa)]
        if not matching_pa:
            continue
        tid = _coerce_int(_raw_first_key(master, 'tradeid'))
        if tid is None or tid <= 0:
            continue
        pa = matching_pa[0]
        ci_group = {
            'root_id': _raw_first_key(ci, 'root_id'),
            'confirmation_root_id': _raw_first_key(ci, 'confirmation_root_id'),
            'family_number': _raw_first_key(ci, 'family_number'),
            'setup_group_id': _raw_first_key(ci, 'setup_group_id'),
            'symbol': _raw_first_key(ci, 'symbol'),
            'timeframe': _raw_first_key(ci, 'timeframe'),
            'source_timeframe': _raw_first_key(ci, 'source_timeframe'),
            'live_trades': _raw_first_key(pa, 'live_trades'),
            'trade_configured': _raw_first_key(pa, 'trade_configured'),
        }
        identity_key = (
            str(ci_group['root_id']), str(ci_group['confirmation_root_id']),
            str(ci_group['setup_group_id']), str(ci_group['family_number']),
            _identity_text(ci_group['symbol']),
            tuple(sorted({_identity_text(ci_group['timeframe']),
                          _identity_text(ci_group['source_timeframe'])} - {''})),
        )
        groups_by_tid.setdefault(tid, []).append((identity_key, ci_group, master))

    out = {}
    ambiguous = set()
    for tid, candidates in groups_by_tid.items():
        identities = {c[0] for c in candidates}
        if len(identities) != 1 or master_tid_counts.get(tid, 0) != 1:
            ambiguous.add(tid)
            continue
        _, group, master = candidates[0]
        out[tid] = {'group': group, 'canonical_row': master}

    return {
        'by_magic': out,
        'ambiguous': ambiguous,
        'ok': True,
        'reason': None,
        'canonical_tradeids': set(out),
        'groups_matched': len(out),
    }


def fetch_investor_programme_analytics_magic_groups(
    programme_id, programme_userid, investor_id, sub_account_id,
    require_live=False,
):
    """Return subaccount copies whose magic resolves to a chart/analytics group.

    When require_live=True, the matching programme_analytics row must also
    have live_trades=1 and trade_configured=1. Copy symbol and exact owner
    identity remain mandatory; timeframe matching is resolved at the
    canonical chart-identity/analytics layer.
    """
    groups = fetch_programme_analytics_magic_groups(
        programme_id, programme_userid, require_live=require_live,
    )
    result = {'by_magic': {}, 'ambiguous': set(), 'ok': groups.get('ok', False),
              'reason': groups.get('reason'), 'groups_matched': 0}
    if not groups.get('ok'):
        return result

    inv_id = _coerce_int(investor_id)
    sub_id = _coerce_int(sub_account_id)
    if not inv_id or inv_id <= 0 or not sub_id or sub_id <= 0:
        result.update(ok=False, reason='missing investor/subaccount identifiers')
        return result

    pt_cols = _db_table_columns('programme_trades')
    if 'setup_group_id' not in pt_cols:
        result.update(ok=False, reason='programme_trades is missing setup_group_id')
        return result
    select_cols = ['id', 'tradeid', 'setup_group_id', 'symbol']
    for col in ('timeframe', 'ordertype', 'orderstatus', 'trade_time'):
        if col in pt_cols:
            select_cols.append(col)
    sql = (
        f"SELECT {', '.join(f'`{c}`' for c in select_cols)} "
        "FROM programme_trades WHERE userid = %s AND programmeid = %s "
        "AND investor_id = %s AND investor_sub_account_id = %s "
        "AND tradeid IS NOT NULL"
    )
    rows, err = _strict_db_rows(
        sql, [_coerce_int(programme_userid), _coerce_int(programme_id), inv_id, sub_id],
        timeout=20.0,
    )
    if rows is None:
        result.update(ok=False, reason=f'could not read investor programme_trades copies: {err}')
        return result

    rows_by_tid = {}
    for row in rows:
        tid = _coerce_int(_raw_first_key(row, 'tradeid'))
        if tid is None or tid <= 0 or tid not in groups['by_magic']:
            continue
        # Count every copy with this magic before validating its group. A
        # second row in a different setup group is still an ambiguity; it must
        # not disappear merely because only one copy matches the canonical ID.
        rows_by_tid.setdefault(tid, []).append(row)

    for tid, copies in rows_by_tid.items():
        if len(copies) != 1:
            result['ambiguous'].add(tid)
            continue
        copy_row = copies[0]
        group_rec = groups['by_magic'][tid]
        copy_symbol = _identity_text(_raw_first_key(copy_row, 'symbol'))
        group_symbol = _identity_text(_raw_first_key(group_rec['group'], 'symbol'))
        copy_group_id = _coerce_int(_raw_first_key(copy_row, 'setup_group_id'))
        canonical_group_id = _coerce_int(_raw_first_key(group_rec['group'], 'setup_group_id'))
        if (
            not copy_symbol or copy_symbol != group_symbol
            or copy_group_id is None or copy_group_id <= 0
            or copy_group_id != canonical_group_id
        ):
            result['ambiguous'].add(tid)
            continue
        result['by_magic'][tid] = {
            'row': copies[0],
            'group': groups['by_magic'][tid]['group'],
            'canonical_row': groups['by_magic'][tid]['canonical_row'],
        }

    result['ambiguous'].update(groups.get('ambiguous', set()))
    result['groups_matched'] = len(result['by_magic'])
    return result


def verify_mt5_subaccount_identity(investor_id, sub_account_id):
    """Fail closed unless the connected MT5 login/server match this harvhub subaccount."""
    row = db_fetch_one(
        "SELECT login, server FROM harvhub "
        "WHERE id = %s AND sub_account_id = %s LIMIT 1",
        [investor_id, sub_account_id], timeout=10.0, quiet=True,
    ) or {}
    expected_login = _raw_first_key(row, 'login')
    expected_server = _raw_first_key(row, 'server')
    account = mt5_account_info()
    if not row or _is_nullish(expected_login) or _is_nullish(expected_server) or account is None:
        print(f"[MT5-IDENTITY] cannot verify investor={investor_id} sub={sub_account_id}: missing expected/connected identity")
        return False

    actual_login = getattr(account, 'login', None)
    actual_server = getattr(account, 'server', None)
    def _login_key(value):
        n = _coerce_int(value)
        return str(n) if n is not None else str(value or '').strip()

    login_ok = _login_key(actual_login) == _login_key(expected_login)
    server_ok = str(actual_server or '').strip().lower() == str(expected_server or '').strip().lower()
    if not (login_ok and server_ok):
        print(
            f"[MT5-IDENTITY] MISMATCH investor={investor_id} sub={sub_account_id}: "
            f"expected login={expected_login!r} server={expected_server!r}; "
            f"connected login={actual_login!r} server={actual_server!r}. "
            "All MT5 trading stages are blocked for this job."
        )
        return False
    print(f"[MT5-IDENTITY] verified investor={investor_id} sub={sub_account_id} "
          f"login={actual_login} server={actual_server}")
    return True

def fetch_live_investor_programme_trades(
    programme_id, programme_userid, investor_id, sub_account_id,
    execution_start_date, pending_only=True,
):
    """Fetch only this investor's copies of mapped, configured LIVE source trades.

    The canonical programme-level row is joined through programme_trade_chart_identity
    to programme_analytics.live_trades=1. The timestamp is taken from the new
    canonical programme_trades.trade_time first, then the investor copy, then
    the chart identity trigger/anchor timestamp. created_at is deliberately NOT a
    fallback because sync time is not signal time.

    Returns (eligible_rows, counters). Missing schemas or execution dates fail
    closed, so no unverified trade can reach the placer.
    """
    meta = {
        'skipped_old_contract': 0,
        'skipped_missing_trade_time': 0,
        'skipped_future_trade_time': 0,
        'skipped_ambiguous_magic': 0,
        'skipped_unmapped_group': 0,
        'skipped_duplicate_join_rows': 0,
        'ambiguous_tradeids': [],
        'unmapped_tradeids': [],
        'reason': None,
    }
    pid = _coerce_int(programme_id)
    owner_id = _coerce_int(programme_userid)
    inv_id = _coerce_int(investor_id)
    sub_id = _coerce_int(sub_account_id)
    if not all((pid and pid > 0, owner_id and owner_id > 0,
                inv_id and inv_id > 0, sub_id and sub_id > 0)):
        meta['reason'] = 'missing programme owner / investor / subaccount identifiers'
        return [], meta

    start_dt = _parse_lagos_datetime(execution_start_date)
    if start_dt is None:
        meta['reason'] = 'harvhub.execution_start_date is missing or invalid'
        return [], meta

    pt_cols = _db_table_columns('programme_trades')
    ci_cols = _db_table_columns('programme_trade_chart_identity')
    pa_cols = _db_table_columns('programme_analytics')

    required_pt = {
        'id', 'userid', 'programmeid', 'investor_id', 'investor_sub_account_id',
        'tradeid', 'setup_group_id', 'orderstatus'
    }
    required_ci = {
        'programme_trade_id', 'userid', 'programmeid', 'root_id',
        'confirmation_root_id', 'family_number', 'setup_group_id', 'symbol',
        'timeframe', 'source_timeframe', 'entry_trigger_time', 'entry_anchor_time'
    }
    required_pa = {
        'userid', 'programme_id', 'root_id', 'confirmation_root_id',
        'family_number', 'setup_group_id', 'symbol', 'timeframe',
        'live_trades', 'trade_configured'
    }
    if not required_pt.issubset(pt_cols):
        meta['reason'] = 'programme_trades schema is missing required ownership/status columns'
        return [], meta
    if not required_ci.issubset(ci_cols):
        meta['reason'] = 'programme_trade_chart_identity is missing required chart identity columns'
        return [], meta
    if not required_pa.issubset(pa_cols):
        meta['reason'] = 'programme_analytics is missing required live-trade columns'
        return [], meta

    time_sources = []
    if 'trade_time' in pt_cols:
        time_sources.append("NULLIF(p.trade_time, '0000-00-00 00:00:00')")
        time_sources.append("NULLIF(t.trade_time, '0000-00-00 00:00:00')")
    time_sources.extend([
        "NULLIF(ci.entry_trigger_time, '')",
        "NULLIF(ci.entry_anchor_time, '')",
    ])
    effective_time_sql = 'COALESCE(' + ', '.join(time_sources) + ')'

    tf_match = [
        "LOWER(TRIM(COALESCE(a.timeframe,''))) = LOWER(TRIM(COALESCE(ci.timeframe,'')))",
        "LOWER(TRIM(COALESCE(a.timeframe,''))) = LOWER(TRIM(COALESCE(ci.source_timeframe,'')))",
    ]
    if 'source_timeframe' in pa_cols:
        tf_match.extend([
            "LOWER(TRIM(COALESCE(a.source_timeframe,''))) = LOWER(TRIM(COALESCE(ci.timeframe,'')))",
            "LOWER(TRIM(COALESCE(a.source_timeframe,''))) = LOWER(TRIM(COALESCE(ci.source_timeframe,'')))",
        ])
    live_predicates = [
        'a.userid = t.userid',
        'a.programme_id = t.programmeid',
        'a.setup_group_id = ci.setup_group_id',
        'p.setup_group_id = ci.setup_group_id',
        't.setup_group_id = p.setup_group_id',
        'a.live_trades = 1',
        'a.trade_configured = 1',
        'a.root_id = ci.root_id',
        'COALESCE(a.confirmation_root_id, 0) = COALESCE(ci.confirmation_root_id, 0)',
        'COALESCE(a.family_number, 0) = COALESCE(ci.family_number, 0)',
        'UPPER(TRIM(a.symbol)) = UPPER(TRIM(ci.symbol))',
        '(' + ' OR '.join(tf_match) + ')',
    ]
    if 'matched_family' in pa_cols:
        live_predicates.append('COALESCE(a.matched_family, 1) <> 0')
    if 'contract_id' in pt_cols:
        live_predicates.append('p.contract_id = t.contract_id')
    # Do not gate on the programme master's orderstatus. Older Python versions
    # mirrored one investor's running/closed state back to the master row, which
    # could incorrectly suppress the same still-live signal for other investors.
    # The authoritative source gate is programme_analytics.live_trades=1.

    if pending_only:
        status_sql = "AND (LOWER(COALESCE(t.orderstatus,'')) = 'pending' OR t.orderstatus IS NULL OR t.orderstatus = '')"
    else:
        # RR/BE reconciliation may inspect pending and running trades, but must
        # never pick up failed or closed investor rows.
        status_sql = "AND (LOWER(COALESCE(t.orderstatus,'')) IN ('pending','running') OR t.orderstatus IS NULL OR t.orderstatus = '')"

    # A manual programme request owns the matching trade until it is cancelled.
    # Exclude it from every normal placement/modification stage so a just-deleted
    # pending order cannot be placed again, and an automated SL stage cannot
    # overwrite a user-requested breakeven stop. The canonical programme request
    # is checked first, so it still protects the row if copy mirroring is delayed.
    manual_request_sql = ''
    if 'order_request_status' in pt_cols:
        manual_request_sql = """
           AND LOWER(REPLACE(REPLACE(TRIM(COALESCE(p.order_request_status, '')), '_', ' '), '-', ' ')) NOT IN (
                   'position closing requested',
                   'order closing requested',
                   'breakeven requested'
               )
        """

    sql = f"""
        SELECT t.*, {effective_time_sql} AS _effective_trade_time
          FROM programme_trades t
          INNER JOIN programme_trades p
                  ON p.userid = t.userid
                 AND p.programmeid = t.programmeid
                 AND p.tradeid = t.tradeid
                 AND p.investor_id IS NULL
          INNER JOIN programme_trade_chart_identity ci
                  ON ci.programme_trade_id = p.id
                 AND ci.userid = p.userid
                 AND ci.programmeid = p.programmeid
         WHERE t.userid = %s
           AND t.programmeid = %s
           AND t.investor_id = %s
           AND t.investor_sub_account_id = %s
           {status_sql}
           {manual_request_sql}
           AND EXISTS (
                SELECT 1
                  FROM programme_analytics a
                 WHERE {' AND '.join(live_predicates)}
           )
         ORDER BY t.id ASC
    """
    rows, sql_error = _strict_db_rows(
        sql, [owner_id, pid, inv_id, sub_id], timeout=25.0,
    )
    if rows is None:
        meta['reason'] = f'could not read live investor programme trades: {sql_error}'
        return [], meta

    # Resolve the exact investor-copy row through the canonical programme row,
    # chart identity, and programme_analytics group. This is stricter than
    # accepting a bare tradeid copied into programme_trades.
    group_resolution = fetch_investor_programme_analytics_magic_groups(
        pid, owner_id, inv_id, sub_id, require_live=True,
    )
    if not group_resolution.get('ok'):
        meta['reason'] = group_resolution.get('reason') or 'programme analytics group mapping unavailable'
        return [], meta

    # MT5 magic contains only tradeid, not programme_id. Catalogue every
    # programme associated with this same subaccount, including canonical
    # trades whose investor copy has not yet appeared. Reused magic is unsafe.
    magic_catalog = fetch_subaccount_magic_catalog(inv_id, sub_id)
    if not magic_catalog.get('ok'):
        meta['reason'] = magic_catalog.get('reason') or 'subaccount magic catalogue unavailable'
        return [], meta

    ambiguous_magic = (
        set(magic_catalog.get('ambiguous') or set())
        | set(group_resolution.get('ambiguous') or set())
    )
    mapped = group_resolution.get('by_magic') or {}
    rows_by_id = {}
    unmapped_tids = set()
    queried_tids = {
        tid for tid in (_coerce_int(_raw_first_key(r, 'tradeid')) for r in rows)
        if tid is not None and tid > 0
    }
    queried_row_ids = {
        row_id for row_id in (_coerce_int(_raw_first_key(r, 'id')) for r in rows)
        if row_id is not None and row_id > 0
    }
    for row in rows:
        tid = _coerce_int(_raw_first_key(row, 'tradeid'))
        row_id = _coerce_int(_raw_first_key(row, 'id'))
        if tid is None or row_id is None or tid <= 0:
            continue
        group_rec = mapped.get(tid)
        if tid in ambiguous_magic or group_rec is None:
            unmapped_tids.add(tid)
            continue
        # Match the exact investor-copy record, not simply another row with
        # the same magic number in the same subaccount/programme.
        mapped_row_id = _coerce_int(_raw_first_key(group_rec.get('row') or {}, 'id'))
        if mapped_row_id != row_id:
            unmapped_tids.add(tid)
            continue
        rows_by_id[row_id] = row

    before = len(rows)
    rows = sorted(rows_by_id.values(), key=lambda r: _coerce_int(_raw_first_key(r, 'id')) or 0)
    # Count both cross-programme collisions and same-programme group/copy
    # ambiguities, including those excluded from the mapped dictionary.
    skipped_ambiguous_tids = queried_tids.intersection(ambiguous_magic)
    skipped_unmapped_tids = unmapped_tids - ambiguous_magic
    meta['skipped_ambiguous_magic'] = len(skipped_ambiguous_tids)
    meta['skipped_unmapped_group'] = len(skipped_unmapped_tids)
    if skipped_ambiguous_tids:
        meta['ambiguous_tradeids'] = sorted(skipped_ambiguous_tids)[:50]
    if skipped_unmapped_tids:
        meta['unmapped_tradeids'] = sorted(skipped_unmapped_tids)[:50]
    meta['skipped_duplicate_join_rows'] = max(0, before - len(queried_row_ids))

    from datetime import timezone
    try:
        from zoneinfo import ZoneInfo
        now_dt = datetime.now(ZoneInfo('Africa/Lagos')).replace(tzinfo=None)
    except Exception:
        now_dt = datetime.now(timezone(timedelta(hours=1))).replace(tzinfo=None)

    eligible = []
    for row in rows:
        trade_dt = _parse_lagos_datetime(_raw_first_key(row, '_effective_trade_time'))
        if trade_dt is None:
            meta['skipped_missing_trade_time'] += 1
            continue
        if trade_dt < start_dt:
            meta['skipped_old_contract'] += 1
            continue
        if trade_dt > now_dt:
            meta['skipped_future_trade_time'] += 1
            continue
        row['_effective_trade_datetime'] = trade_dt
        eligible.append(row)
    return eligible, meta


def to_float(value, default=None):
    """Safe float parser (handles numbers, strings, one-element lists)."""
    if value is None:
        return default
    if isinstance(value, (list, tuple, set)):
        for item in value:
            result = to_float(item, default=None)
            if result is not None:
                return result
        return default
    if isinstance(value, bool):
        return default
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip()
        if v == "":
            return default
        v = v.replace('%', '').strip()
        try:
            return float(v)
        except ValueError:
            return default
    return default


def parse_pct(value, default=0.0):
    f = to_float(value, default=None)
    if f is None:
        return default
    return float(f)


def format_money(v, decimals=2):
    try:
        return f"${float(v):,.{decimals}f}"
    except (TypeError, ValueError):
        return "$0.00"


def format_datetime(dt):
    if not dt:
        return "—"
    try:
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(dt)


def extract_volume_from_signal(order_data):
    """Return the first `*_volume` field as float, or plain `volume`."""
    if not isinstance(order_data, dict):
        return None
    for key, value in order_data.items():
        if key.endswith('_volume'):
            try:
                return float(value)
            except (ValueError, TypeError):
                continue
    if 'volume' in order_data:
        try:
            return float(order_data['volume'])
        except (ValueError, TypeError):
            return None
    return None


def get_volume_field_from_order(order):
    """Return (key, value) for the first `_volume` field, else `volume`."""
    if not isinstance(order, dict):
        return None, None
    for key, value in order.items():
        if '_volume' in key.lower() and isinstance(value, (int, float)):
            return key, value
    if 'volume' in order and isinstance(order['volume'], (int, float)):
        return 'volume', order['volume']
    return None, None


def extract_rr_from_comment(comment):
    """Parse 'R<number>' from a trade comment. Returns float or 0."""
    import re as _re
    if not comment or not isinstance(comment, str):
        return 0
    m = _re.search(r'R([0-9]+(?:\.[0-9]+)?)', comment)
    if m:
        try:
            return float(m.group(1))
        except (ValueError, TypeError):
            return 0
    return 0


# ---------- time / timezone helpers ----------
def parse_time_string(time_str):
    """Parse '9:00 am', '17:30', '9:00:00', '09:00' -> (hour, minute)."""
    if not time_str:
        return None, None

    s = str(time_str).strip().lower()
    is_pm = 'pm' in s
    is_am = 'am' in s
    clean = s.replace('pm', '').replace('am', '').replace(' ', '').strip()

    try:
        if ':' in clean:
            parts = clean.split(':')
            hour = int(parts[0])
            minute = int(parts[1]) if len(parts) > 1 else 0
        else:
            hour = int(clean)
            minute = 0
    except (ValueError, TypeError):
        return None, None

    if is_pm and hour != 12:
        hour += 12
    elif is_am and hour == 12:
        hour = 0

    return hour, minute


def to_12hr(hour, minute):
    period = "AM" if hour < 12 else "PM"
    h12 = hour % 12
    if h12 == 0:
        h12 = 12
    return f"{h12}:{minute:02d} {period}"


def get_current_time_in_timezone(timezone_str):
    """
    Convert 'now' to the given IANA timezone.
    Returns (datetime, resolved: bool). resolved=False means the timezone
    was null/unusable and local time was returned as a fallback.
    """
    if _is_nullish(timezone_str):
        return datetime.now(), False

    tz_name = str(timezone_str).strip()

    if ZoneInfo is not None:
        try:
            return datetime.now(ZoneInfo(tz_name)), True
        except Exception:
            pass

    if pytz is not None:
        try:
            return datetime.now(pytz.timezone(tz_name)), True
        except Exception:
            pass

    print(f"  ⚠️ Unknown timezone '{tz_name}' — falling back to local time")
    return datetime.now(), False


def is_time_in_window(now_h, now_m, sh, sm, eh, em):
    """
    True if (now_h, now_m) is INSIDE the inclusive span [sh:sm .. eh:em].
    Handles cross-midnight windows. An empty/locked window (from == to)
    means the WHOLE day is restricted.
    """
    now_min = now_h * 60 + now_m
    start_min = sh * 60 + sm
    end_min = eh * 60 + em

    # from == to -> full day restriction
    if start_min == end_min:
        return True

    # cross-midnight window (e.g. 22:00 -> 02:00)
    if end_min < start_min:
        return now_min >= start_min or now_min <= end_min

    # normal span (inclusive on both ends)
    return start_min <= now_min <= end_min


def is_full_day_window(from_h, from_m, to_h, to_m):
    """
    A day-row is a FULL-DAY restriction when its span covers the whole day.
    Canonical shapes:
      * 00:00 -> 00:00  (literal 12am to 12am)
      * 00:00 -> 23:59  (00:00 through the last minute)
    """
    f_min = from_h * 60 + from_m
    t_min = to_h * 60 + to_m
    if f_min == 0 and t_min == 0:
        return True
    if f_min == 0 and to_h == 23 and to_m == 59:
        return True
    return False


def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return None


# ==============================================================================
#  REBRANDED TERMINAL PATH RESOLUTION — mirrors brokerdetails.py exactly
# ==============================================================================
def _sanitize_folder_component(value):
    import re as _re
    if value is None:
        return ''
    s = str(value).strip()
    s = _re.sub(r'[<>:"/\\|?*]', '_', s)
    s = s.replace(' ', '_')
    return s


def build_mt5_terminal_path_sa(server, login, user_id, sub_account_id):
    """
    Same formula as brokerdetails.build_mt5_terminal_path_sa.
    Returns (folder_name, folder_abs, terminal_exe).
    """
    s   = _sanitize_folder_component(server)
    l   = _sanitize_folder_component(login)
    u   = _sanitize_folder_component(user_id)
    aid = _sanitize_folder_component(sub_account_id)
    folder_name  = f"{s}_{l}_U{u}_SA{aid}"
    folder_abs   = os.path.join(MT5_DESTINATION_PATH, folder_name)
    terminal_exe = os.path.join(folder_abs, "terminal64.exe")
    return folder_name, folder_abs, terminal_exe


def resolve_investor_terminal_path(inv_db_id, sub_id):
    """
    Return the correct terminal64.exe path for this investor sub-account.

    Priority (DB Terminal_path is IGNORED — it historically contained
    garbage serialized values):
      1. Rebuild from server+login using brokerdetails' formula
         — IF the rebuilt folder + exe exist on disk
      2. None (caller MUST skip the investor — no default fallback)

    Returns the resolved path string, or None when:
      * no harvhub row exists for (investor, sub)
      * login/server are missing/NULL
      * the rebuilt terminal64.exe does not exist on disk
    """
    inv_id_int = _coerce_int(inv_db_id)
    sub_id_int = _coerce_int(sub_id)
    if not inv_id_int or not sub_id_int:
        print(f"[MT5-PATH] invalid investor/subaccount identifiers: {inv_db_id}/{sub_id}")
        return None
    row = db_fetch_one(
        f"SELECT login, server FROM harvhub "
        f"WHERE id = {int(inv_id_int)} AND sub_account_id = {int(sub_id_int)} LIMIT 1",
        None, timeout=10.0, quiet=True,
    )
    if not row:
        print(f"[MT5-PATH] no harvhub row for investor={inv_db_id} "
              f"sub={sub_id} — skip")
        return None

    login_v  = _raw_first_key(row, 'login')
    server_v = _raw_first_key(row, 'server')

    if not login_v or not server_v:
        print(f"[MT5-PATH] no login/server in harvhub for "
              f"investor={inv_db_id} sub={sub_id} — skip")
        return None

    # Rebuild using brokerdetails formula — the ONLY source of truth
    _, folder_abs, terminal_exe = build_mt5_terminal_path_sa(
        server_v, login_v, inv_db_id, sub_id
    )

    if os.path.isfile(terminal_exe):
        print(f"[MT5-PATH] resolved (rebuilt) = {terminal_exe}")
        # Cosmetic write-back — the column is no longer read, but keeping
        # it in sync helps anyone inspecting the DB manually.
        try:
            db_execute(
                "UPDATE harvhub SET Terminal_path = " + _pt_sql_literal(terminal_exe) +
                f" WHERE id = {int(inv_id_int)} AND sub_account_id = {int(sub_id_int)}",
                None, timeout=10.0, quiet=True,
            )
        except Exception:
            pass
        return terminal_exe

    print(f"[MT5-PATH] rebuilt path missing on disk: {terminal_exe} — skip")
    return None

# ==============================================================================
#  MT5 — thin, locked wrappers (available to every future stage)
# ==============================================================================
def mt5_available():
    return mt5 is not None


def mt5_connect(terminal_path=None, timeout_ms=10000):
    """
    Initialize MT5. Idempotent — safe to call multiple times.

    Refuses to run without an explicit terminal_path. This prevents the
    silent fallback to mt5.initialize() with no path, which is what
    produced the -10003 "IPC initialize failed, Process create failed"
    noise when a bogus path was passed in.
    """
    with _MT5_IPC_LOCK:
        if not mt5_available():
            return False
        if not terminal_path:
            print("  ⚠️ mt5_connect called without a terminal_path — refusing")
            return False
        try:
            mt5.shutdown()
        except Exception:
            pass
        try:
            return bool(mt5.initialize(
                path=terminal_path, timeout=timeout_ms, portable=True,
            ))
        except Exception as e:
            print(f"  ⚠️ mt5.initialize failed: {e}")
            return False

def mt5_disconnect():
    with _MT5_IPC_LOCK:
        if not mt5_available():
            return
        try:
            mt5.shutdown()
        except Exception:
            pass


def mt5_account_info():
    with _MT5_IPC_LOCK:
        if not mt5_available():
            return None
        try:
            return mt5.account_info()
        except Exception:
            return None


def mt5_terminal_info():
    with _MT5_IPC_LOCK:
        if not mt5_available():
            return None
        try:
            return mt5.terminal_info()
        except Exception:
            return None


def mt5_last_error():
    with _MT5_IPC_LOCK:
        if not mt5_available():
            return None
        try:
            return mt5.last_error()
        except Exception:
            return None


def mt5_balance():
    acc = mt5_account_info()
    if acc is None:
        return None
    try:
        return float(acc.balance)
    except Exception:
        return None


def mt5_symbol_info(symbol):
    with _MT5_IPC_LOCK:
        if not mt5_available():
            return None
        try:
            return mt5.symbol_info(symbol)
        except Exception:
            return None


def mt5_symbol_tick(symbol):
    with _MT5_IPC_LOCK:
        if not mt5_available():
            return None
        try:
            return mt5.symbol_info_tick(symbol)
        except Exception:
            return None


def mt5_symbol_select(symbol, enable=True):
    with _MT5_IPC_LOCK:
        if not mt5_available():
            return False
        try:
            return bool(mt5.symbol_select(symbol, enable))
        except Exception:
            return False


def mt5_symbols_get():
    with _MT5_IPC_LOCK:
        if not mt5_available():
            return []
        try:
            return mt5.symbols_get() or []
        except Exception:
            return []


def mt5_orders_get(**kwargs):
    with _MT5_IPC_LOCK:
        if not mt5_available():
            return []
        try:
            return mt5.orders_get(**kwargs) or []
        except Exception:
            return []


def mt5_positions_get(**kwargs):
    with _MT5_IPC_LOCK:
        if not mt5_available():
            return []
        try:
            return mt5.positions_get(**kwargs) or []
        except Exception:
            return []


def mt5_history_deals(start_naive, end_naive):
    with _MT5_IPC_LOCK:
        if not mt5_available():
            return []
        try:
            return mt5.history_deals_get(start_naive, end_naive) or []
        except Exception:
            return []


def mt5_history_orders(start_naive, end_naive):
    with _MT5_IPC_LOCK:
        if not mt5_available():
            return []
        try:
            return mt5.history_orders_get(start_naive, end_naive) or []
        except Exception:
            return []


def mt5_order_send(request):
    with _MT5_IPC_LOCK:
        if not mt5_available():
            return None
        try:
            return mt5.order_send(request)
        except Exception:
            return None


def mt5_order_calc_profit(order_type, symbol, volume, price_open, price_close):
    with _MT5_IPC_LOCK:
        if not mt5_available():
            return None
        try:
            return mt5.order_calc_profit(
                order_type, symbol, volume, price_open, price_close
            )
        except Exception:
            return None


def mt5_order_calc_margin(order_type, symbol, volume, price):
    with _MT5_IPC_LOCK:
        if not mt5_available():
            return None
        try:
            return mt5.order_calc_margin(order_type, symbol, volume, price)
        except Exception:
            return None


# ==============================================================================
#  DB-DRIVEN DISCOVERY: investors associated with this VPS
# ==============================================================================
def fetch_vps_programmes(local_ip):
    """
    Return {programme_id, userid} for every programme hosted on this VPS:
      - the owner programme in programme_vps;
      - active follower programmes in programme_vps_hosts_followers.

    IMPORTANT: This is deliberately programme-VPS scoped. The separate
    account VPS tables (`vps`, `vps_hosts_followers`) are not imported here.
    The SQL uses one IP placeholder because the remote bridge supports one
    bound placeholder per request.
    """
    sql = """
        SELECT DISTINCT
               CASE
                   WHEN programme_scope.scope_type = 0 THEN v.programme_id
                   ELSE f.follower_programme_id
               END AS programme_id,
               p.userid AS userid
          FROM programme_vps v
          CROSS JOIN (
                SELECT 0 AS scope_type
                UNION ALL
                SELECT 1 AS scope_type
          ) AS programme_scope
          LEFT JOIN programme_vps_hosts_followers f
                 ON programme_scope.scope_type = 1
                AND f.owner_programme_id = v.programme_id
                AND f.host_status = 'active'
          LEFT JOIN programme p
                 ON p.id = CASE
                               WHEN programme_scope.scope_type = 0
                                   THEN v.programme_id
                               ELSE f.follower_programme_id
                           END
         WHERE v.programme_vps_ip_address = %s
           AND (
                programme_scope.scope_type = 0
                OR f.id IS NOT NULL
           )
    """
    print(f"[fetch_vps_programmes] querying programme VPS bindings for ip={local_ip!r}")
    result = db_query(sql, [local_ip], timeout=20.0, quiet=True)
    if not _is_ok(result):
        print(
            "[fetch_vps_programmes] DATABASE QUERY FAILED: "
            f"status={result.get('status')!r}; "
            f"message={result.get('message') or 'No database error message supplied'}"
        )
        return []

    rows = [_normalize_row(r) for r in (result.get('results') or [])]
    out = []
    seen = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        pid = _coerce_int(_pick_first_key(row, 'programme_id'))
        uid = _coerce_int(_pick_first_key(row, 'userid'))
        if pid is None or pid <= 0 or pid in seen:
            continue
        seen.add(pid)
        out.append({'programme_id': pid, 'userid': uid})

    print(f"[fetch_vps_programmes] programme VPS bindings returned: {len(out)}")
    return out


def fetch_active_investors_for_programme(programme_id):
    """Return the exact active investor/subaccount pairs used by the PHP sync.

    Eligibility requires a programme_revenue_deals association AND the exact
    harvhub (id, sub_account_id) row to have account_contract_status='contract_active'.
    A LIVE programme_trades copy alone is not enough to make a subaccount active.
    """
    pid = _coerce_int(programme_id)
    if pid is None or pid <= 0:
        return []

    sql = f"""
        SELECT DISTINCT
               d.investor_id,
               d.investor_sub_account_id AS sub_account_id
          FROM programme_revenue_deals d
          INNER JOIN harvhub h
                  ON h.id = d.investor_id
                 AND h.sub_account_id = d.investor_sub_account_id
         WHERE d.programme_id = {int(pid)}
           AND d.investor_id > 0
           AND d.investor_sub_account_id > 0
           AND h.account_contract_status = 'contract_active'
         ORDER BY d.investor_id ASC, d.investor_sub_account_id ASC
    """
    rows, error = _strict_db_rows(sql, None, timeout=15.0)
    if rows is None:
        print(f"[fetch_active_investors_for_programme] programme={pid} query failed: {error}")
        return []

    out = []
    seen = set()
    for row in rows:
        inv = _coerce_int(_pick_first_key(row, 'investor_id'))
        sub = _coerce_int(_pick_first_key(row, 'sub_account_id'))
        if inv is None or inv <= 0 or sub is None or sub <= 0:
            continue
        key = (inv, sub)
        if key in seen:
            continue
        seen.add(key)
        out.append({'investor_id': inv, 'sub_account_id': sub})
    return out

def fetch_vps_investor_jobs(local_ip):
    """Discover active investor jobs for programmes hosted on this computer.

    Two independent checks are intentionally combined, but not conflated:
      1. programme_vps + active programme_vps_hosts_followers restrict the
         programme IDs to this computer's programme-VPS allocation.
      2. programme_revenue_deals + exact harvhub subaccount +
         account_contract_status='contract_active' define PHP-equivalent
         investor eligibility.

    programme_trades remains the signal-copy/request authority. A worker may
    still need to run investor-owned account-management stages before a new
    signal copy exists, so job discovery must not require a LIVE copy row.
    Account-VPS tables are never read here.
    """
    if not local_ip:
        print("[fetch_vps_investor_jobs] No local IP supplied; no jobs discovered.")
        return []

    programme_bindings = fetch_vps_programmes(local_ip)
    programme_map = {}
    for item in programme_bindings or []:
        pid = _coerce_int(item.get('programme_id'))
        uid = _coerce_int(item.get('userid'))
        if pid and pid > 0:
            programme_map[pid] = uid

    programme_ids = sorted(programme_map)
    print(
        f"[fetch_vps_investor_jobs] programme-VPS scope for ip={local_ip!r}: "
        f"{programme_ids}"
    )
    if not programme_ids:
        print(
            "[fetch_vps_investor_jobs] No programme owner/active-follower binding "
            "exists for this IP. Account-VPS bindings are independent and are ignored."
        )
        return []

    # IDs originate from the database and are converted to positive integers.
    # Inlineing these validated numeric IDs avoids repeated IP placeholders in
    # the bridge, which supports a single bound placeholder per request.
    programme_id_sql = ",".join(str(int(pid)) for pid in programme_ids)
    sql = f"""
        SELECT DISTINCT
               d.programme_id AS programme_id,
               p.program_name AS programme_name,
               p.userid AS programme_userid,
               d.investor_id AS investor_id,
               d.investor_sub_account_id AS sub_account_id
          FROM programme_revenue_deals d
          INNER JOIN programme p
                  ON p.id = d.programme_id
          INNER JOIN harvhub h
                  ON h.id = d.investor_id
                 AND h.sub_account_id = d.investor_sub_account_id
         WHERE d.programme_id IN ({programme_id_sql})
           AND d.investor_id > 0
           AND d.investor_sub_account_id > 0
           AND h.account_contract_status = 'contract_active'
         ORDER BY d.programme_id ASC,
                  d.investor_id ASC,
                  d.investor_sub_account_id ASC
    """

    print(
        "[fetch_vps_investor_jobs] Applying PHP-equivalent active eligibility: "
        "programme_revenue_deals + exact harvhub subaccount + contract_active."
    )
    result = db_query(sql, None, timeout=25.0, quiet=True)
    if not _is_ok(result):
        print(
            "[fetch_vps_investor_jobs] DATABASE QUERY FAILED: "
            f"status={result.get('status')!r}; "
            f"message={result.get('message') or 'No database error message supplied'}"
        )
        return []

    rows = [_normalize_row(r) for r in (result.get('results') or [])]
    jobs = []
    seen = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        pid = _coerce_int(_pick_first_key(row, 'programme_id'))
        inv = _coerce_int(_pick_first_key(row, 'investor_id'))
        sub = _coerce_int(_pick_first_key(row, 'sub_account_id'))
        if not all((pid and pid > 0, inv and inv > 0, sub and sub > 0)):
            continue

        programme_userid = _coerce_int(_pick_first_key(row, 'programme_userid'))
        expected_owner = programme_map.get(pid)
        if expected_owner and programme_userid and programme_userid != expected_owner:
            print(
                f"[fetch_vps_investor_jobs] Skipping inconsistent owner mapping "
                f"for programme={pid}: VPS owner={expected_owner}, row owner={programme_userid}"
            )
            continue
        if not programme_userid:
            programme_userid = expected_owner
        if not programme_userid or programme_userid <= 0:
            print(
                f"[fetch_vps_investor_jobs] Skipping programme={pid}; "
                "programme owner userid could not be resolved."
            )
            continue

        key = (inv, sub, pid)
        if key in seen:
            continue
        seen.add(key)
        jobs.append({
            'investor_id': inv,
            'sub_account_id': sub,
            'programme_id': pid,
            'programme_name': str(_pick_first_key(row, 'programme_name') or '').strip()
                              or f'programme_{pid}',
            'programme_userid': programme_userid,
        })

    if jobs:
        print(
            f"[fetch_vps_investor_jobs] Active programme jobs: {len(jobs)} "
            f"across {len({j['programme_id'] for j in jobs})} VPS-scoped programme(s)."
        )
        for job in jobs:
            print(
                "  [JOB] programme={programme_id} investor={investor_id} "
                "subaccount={sub_account_id}".format(**job)
            )
    else:
        print(
            "[fetch_vps_investor_jobs] Programme VPS binding(s) exist, but no "
            "investor/subaccount passed the PHP contract_active eligibility rule."
        )
    return jobs


def is_active_investor_for_programme(investor_id, sub_account_id, programme_id):
    """Recheck the same active-deal rule as PHP immediately before processing."""
    inv = _coerce_int(investor_id)
    sub = _coerce_int(sub_account_id)
    pid = _coerce_int(programme_id)
    if not all((inv and inv > 0, sub and sub > 0, pid and pid > 0)):
        return False, 'invalid investor/subaccount/programme identifiers'
    sql = f"""
        SELECT 1 AS eligible
          FROM programme_revenue_deals d
          INNER JOIN harvhub h
                  ON h.id = d.investor_id
                 AND h.sub_account_id = d.investor_sub_account_id
         WHERE d.programme_id = {int(pid)}
           AND d.investor_id = {int(inv)}
           AND d.investor_sub_account_id = {int(sub)}
           AND h.account_contract_status = 'contract_active'
         LIMIT 1
    """
    rows, error = _strict_db_rows(sql, None, timeout=12.0)
    if rows is None:
        return False, f'active-contract eligibility query failed: {error}'
    if not rows:
        return False, 'no matching programme revenue-deal with contract_active status'
    return True, None

def fetch_programmes_metadata_batch(programme_ids):
    """
    Return {programme_id: {'programme_name': str, 'timezone': str|None}}
    for every id, in ONE query.
    """
    if not programme_ids:
        return {}
    ids = [int(p) for p in programme_ids if p]
    if not ids:
        return {}
    placeholders = ','.join(['%s'] * len(ids))
    sql = (
        f"SELECT id, program_name, timezone FROM programme "
        f"WHERE id IN ({placeholders})"
    )
    rows = db_fetch_all(sql, ids, timeout=15.0, quiet=True)
    out = {}
    for row in rows:
        pid = _coerce_int(_pick_first_key(row, 'id'))
        if pid is None:
            continue
        name = (_pick_first_key(row, 'program_name') or '').strip()
        tz_raw = _raw_first_key(row, 'timezone')
        tz = None if _is_nullish(tz_raw) else str(tz_raw).strip()
        out[pid] = {'programme_name': name, 'timezone': tz}
    return out

def fetch_programme_restrictions_batch(programme_ids):
    """
    Return {programme_id: {'dates': [...], 'days': [...]}} for every id.

    Two queries total regardless of how many programmes are passed.
    """
    if not programme_ids:
        return {}
    ids = [int(p) for p in programme_ids if p]
    if not ids:
        return {}
    placeholders = ','.join(['%s'] * len(ids))

    out = {pid: {'dates': [], 'days': []} for pid in ids}

    dates_sql = (
        f"SELECT programme_id, restricted_date "
        f"FROM accountmanagement_restricted_dates "
        f"WHERE programme_id IN ({placeholders}) "
        f"ORDER BY programme_id ASC, restricted_date ASC"
    )
    for row in db_fetch_all(dates_sql, ids, timeout=15.0, quiet=True):
        pid = _coerce_int(_pick_first_key(row, 'programme_id'))
        val = _pick_first_key(row, 'restricted_date')
        if pid is None or val is None:
            continue
        s = str(val).strip()
        if s:
            out[pid]['dates'].append(s[:10])

    days_sql = (
        f"SELECT programme_id, day, from_time, to_time "
        f"FROM accountmanagement_restricted_days "
        f"WHERE programme_id IN ({placeholders}) "
        f"ORDER BY programme_id ASC, id ASC"
    )
    for row in db_fetch_all(days_sql, ids, timeout=15.0, quiet=True):
        pid = _coerce_int(_pick_first_key(row, 'programme_id'))
        if pid is None:
            continue
        out[pid]['days'].append({
            'day':       (_pick_first_key(row, 'day') or '').strip(),
            'from_time': str(_pick_first_key(row, 'from_time') or '').strip(),
            'to_time':   str(_pick_first_key(row, 'to_time') or '').strip(),
        })
    return out

# ==============================================================================
#  PROGRAMME RESTRICTION LOADERS
# ==============================================================================
def fetch_programme_timezone(programme_id):
    """
    Return the programme's timezone string, or None if NULL/empty.
    """
    tz_res = db_query(
        "SELECT timezone FROM programme WHERE id = %s LIMIT 1",
        [programme_id], timeout=10.0, quiet=True,
    )
    if _is_ok(tz_res) and tz_res.get('results'):
        row = tz_res['results'][0]
        for k, v in row.items():
            if isinstance(k, str) and k.strip().lower() == 'timezone':
                if _is_nullish(v):
                    return None
                return str(v).strip()
    return None


def fetch_programme_restricted_dates(programme_id):
    """
    Return list of dates (as YYYY-MM-DD strings) from
    accountmanagement_restricted_dates for this programme.
    """
    out = []
    res = db_query(
        "SELECT restricted_date "
        "FROM accountmanagement_restricted_dates "
        "WHERE programme_id = %s ORDER BY restricted_date ASC",
        [programme_id], timeout=15.0, quiet=True,
    )
    if not _is_ok(res):
        return out

    for raw in res.get('results') or []:
        row = _normalize_row(raw)
        val = _pick_first_key(row, 'restricted_date')
        if val is None:
            continue
        s = str(val).strip()
        if s:
            out.append(s[:10])  # normalise to YYYY-MM-DD
    return out


def fetch_programme_restricted_days(programme_id):
    """
    Return list of {day, from_time, to_time} rows from
    accountmanagement_restricted_days for this programme.
    """
    rows = []
    r_res = db_query(
        "SELECT day, from_time, to_time "
        "FROM accountmanagement_restricted_days "
        "WHERE programme_id = %s ORDER BY id ASC",
        [programme_id], timeout=15.0, quiet=True,
    )
    if _is_ok(r_res):
        for raw in r_res.get('results') or []:
            row = _normalize_row(raw)
            rows.append({
                'day':       (_pick_first_key(row, 'day') or '').strip(),
                'from_time': str(_pick_first_key(row, 'from_time') or '').strip(),
                'to_time':   str(_pick_first_key(row, 'to_time') or '').strip(),
            })
    return rows


def fetch_investor_identity(investor_id):
    res = db_query(
        "SELECT id, fullname, username, email "
        "FROM harvhub WHERE id = %s LIMIT 1",
        [investor_id], timeout=10.0, quiet=True,
    )
    if _is_ok(res) and res.get('results'):
        row = _normalize_row(res['results'][0])
        return {
            'id':       _coerce_int(_pick_first_key(row, 'id')) or investor_id,
            'fullname': (_pick_first_key(row, 'fullname') or '').strip(),
            'username': (_pick_first_key(row, 'username') or '').strip(),
            'email':    (_pick_first_key(row, 'email') or '').strip(),
        }
    return {'id': investor_id, 'fullname': '', 'username': '', 'email': ''}


def fetch_programme_name(programme_id):
    res = db_query(
        "SELECT program_name FROM programme WHERE id = %s LIMIT 1",
        [programme_id], timeout=10.0, quiet=True,
    )
    if _is_ok(res) and res.get('results'):
        for v in res['results'][0].values():
            return '' if v is None else str(v).strip()
    return ''

# ==============================================================================
#  CHECK AND RECORD UNAUTHORIZED ACTIONS — DB version (tradeid-based)
#
#  Investor-scoped, MT5-facing.
#
#  A trade (pending order, open position, or closed deal) is AUTHORIZED for
#  this programme only when its magic/tradeid resolves through the exact
#  investor-subaccount copy -> canonical programme_trades row ->
#  programme_trade_chart_identity -> matching programme_analytics group.
#
#  MT5 magic itself contains only an integer, not programme_id. Since tradeid
#  is a per-programme counter, the subaccount catalogue also checks all
#  programmes linked to this exact MT5 account. Cross-programme collisions
#  are marked ambiguous and are never modified or attributed by guessing.
#
#  Nothing here touches JSON files.
# ==============================================================================
def check_and_record_unauthorized_actions_db(inv_db_id=None, jobs=None):
    """
    Reads:
        * programme_trades        -> this investor's authorized tradeid set
        * harvhub                 -> anchor (broker_balance, execution_start_date)
                                     + identity
        * balance_log             -> last known closing balance for continuity
        * MT5                     -> live orders / positions / history deals

    Writes:
        * balance_log             -> ONE ROW PER LAGOS DAY from execution_start_date
                                     through today (BATCHED — one round-trip)
        * contract_notifications  -> INVESTOR-SIDE alert (per investor, per event)
        * programme_notifications -> PROGRAMME-OWNER-SIDE aggregated alert

    Investor-scoped. Skip via `jobs=` to avoid re-running discovery.
    """
    global unauthorized_alert
    from datetime import datetime, timedelta, timezone
    import time as _time

    # ------------------------------------------------------------------
    # LAGOS TIMEZONE
    # ------------------------------------------------------------------
    try:
        from zoneinfo import ZoneInfo
        LAGOS_TZ = ZoneInfo("Africa/Lagos")
        _LAGOS_TZ_SOURCE = "zoneinfo"
    except Exception:
        LAGOS_TZ = timezone(timedelta(hours=1))
        _LAGOS_TZ_SOURCE = "fixed_utc+1"

    def lagos_now():
        return datetime.now(LAGOS_TZ)

    def lagos_now_str():
        return lagos_now().strftime("%Y-%m-%d %H:%M:%S")

    def lagos_today_str():
        return lagos_now().strftime("%Y-%m-%d")

    def lagos_from_timestamp(ts):
        if not ts:
            return None
        try:
            ts_float = float(ts)
        except (TypeError, ValueError):
            return None
        if ts_float > 1e11:
            ts_float = ts_float / 1000.0
        try:
            return datetime.fromtimestamp(ts_float, tz=timezone.utc).astimezone(LAGOS_TZ)
        except Exception:
            return None

    def lagos_date_str(dt):
        if dt is None:
            return None
        try:
            return dt.astimezone(LAGOS_TZ).strftime("%Y-%m-%d")
        except Exception:
            return None

    def lagos_date_add(date_str, days):
        try:
            base = datetime.strptime(date_str, "%Y-%m-%d")
            return (base + timedelta(days=days)).strftime("%Y-%m-%d")
        except Exception:
            return None

    def lagos_daterange(start_str, end_str):
        cur = start_str
        guard = 0
        while cur is not None and cur <= end_str and guard < 4000:
            yield cur
            cur = lagos_date_add(cur, 1)
            guard += 1

    unauthorized_alert = {
        'is_triggered': False,
        'investor_id': inv_db_id if inv_db_id else 'all',
        'sub_account_id': None,
        'programme_id': None,
        'investor_authorized_tradeids': set(),
        'unauthorized_trades': [],
        'unauthorized_orders': [],
        'unauthorized_positions': [],
        'unauthorized_pnl_total': 0.0,
        'unaccounted_deficit': 0.0,
        'unauthorized_types': [],
        'application_status': 'approved',
        'errors': [],
        'timestamp': lagos_now_str(),
    }

    print(f"\n{'='*10} 🔍 AUTHORIZED-ACTIONS AUDIT (DB) {'='*10}")
    print(f" 🕒 Lagos time now: {lagos_now().strftime('%Y-%m-%d %H:%M:%S %Z')} (source: {_LAGOS_TZ_SOURCE})")

    stats = {
        'investor_id': inv_db_id if inv_db_id else 'all',
        'investors_processed': 0,
        'investors_with_unauthorized': 0,
        'unauthorized_orders_found': 0,
        'unauthorized_orders_cancelled': 0,
        'unauthorized_positions_found': 0,
        'unauthorized_positions_closed': 0,
        'unauthorized_trades_found': 0,
        'unaccounted_deficits_found': 0,
        'balance_log_rows_written': 0,
        'notifications_queued': 0,
        'errors': [],
        'processing_success': False,
        'details': [],
    }

    # ==================================================================
    # PROGRAMME-LEVEL VIOLATOR AGGREGATION
    # ==================================================================
    programme_violators = {}

    def _register_programme_violator(
        programme_id, programme_name,
        investor_id, sub_account_id,
        total_unauth_pnl, unauth_types,
    ):
        if not programme_id:
            return
        entry = programme_violators.setdefault(programme_id, {
            'programme_name': programme_name or f"programme {programme_id}",
            'violators': [],
        })
        if not entry['programme_name'] and programme_name:
            entry['programme_name'] = programme_name
        entry['violators'].append({
            'investor_id': investor_id,
            'sub_account_id': sub_account_id,
            'pnl': float(total_unauth_pnl or 0.0),
            'types': list(unauth_types or []),
        })

    # ==================================================================
    # SECTION 1 — PROGRAMME-SCOPED READERS
    # ==================================================================
    def programme_fetch_owner_identity(programme_id):
        prog_row = db_fetch_one(
            "SELECT program_name, userid FROM programme WHERE id = %s LIMIT 1",
            [programme_id], timeout=10.0, quiet=True,
        )
        if not prog_row:
            return '', '', None

        programme_name = (
            _pick_first_key(prog_row, 'program_name') or ''
        ).strip()
        owner_userid = _coerce_int(_raw_first_key(prog_row, 'userid'))

        owner_email = ''
        if owner_userid is not None and owner_userid > 0:
            main_row = db_fetch_one(
                """
                SELECT email FROM harvhub
                 WHERE id = %s AND is_main_account = 1
                 ORDER BY id ASC
                 LIMIT 1
                """,
                [owner_userid], timeout=10.0, quiet=True,
            )
            if main_row:
                owner_email = (
                    _pick_first_key(main_row, 'email') or ''
                ).strip().lower()

            if not owner_email:
                any_row = db_fetch_one(
                    """
                    SELECT email FROM harvhub
                     WHERE id = %s
                       AND email IS NOT NULL
                       AND email <> ''
                     ORDER BY is_main_account DESC, id ASC
                     LIMIT 1
                    """,
                    [owner_userid], timeout=10.0, quiet=True,
                )
                if any_row:
                    owner_email = (
                        _pick_first_key(any_row, 'email') or ''
                    ).strip().lower()

        if not owner_email:
            print(f"      [NOTIF] ⚠️ Could not resolve owner email for "
                  f"programme={programme_id} (userid={owner_userid})")

        return owner_email, programme_name, owner_userid

    # ==================================================================
    # SECTION 2 — INVESTOR-SCOPED READERS
    # ==================================================================
    def investor_fetch_identity(investor_id, sub_account_id):
        row = db_fetch_one(
            """
            SELECT id, sub_account_id, main_account_id,
                   fullname, username, email
              FROM harvhub
             WHERE id = %s AND sub_account_id = %s
             LIMIT 1
            """,
            [investor_id, sub_account_id], timeout=10.0, quiet=True,
        )
        return row or {}

    def investor_fetch_anchor(investor_id, sub_account_id):
        return db_fetch_one(
            """
            SELECT id, sub_account_id, broker_balance, execution_start_date
              FROM harvhub
             WHERE id = %s AND sub_account_id = %s
             LIMIT 1
            """,
            [investor_id, sub_account_id], timeout=10.0, quiet=True,
        ) or {}

    def investor_fetch_authorized_tradeids(investor_id, sub_account_id, programme_id, programme_userid):
        # Authorization requires a chain through canonical programme_trades,
        # chart identity, and the matching programme_analytics group. A bare
        # investor copy with a matching integer magic is not enough.
        grouped = fetch_investor_programme_analytics_magic_groups(
            programme_id, programme_userid, investor_id, sub_account_id,
        )
        if not grouped.get('ok'):
            return set(), grouped
        catalog = fetch_subaccount_magic_catalog(investor_id, sub_account_id)
        if not catalog.get('ok'):
            grouped['ok'] = False
            grouped['reason'] = catalog.get('reason')
            return set(), grouped
        ambiguous = set(catalog.get('ambiguous') or set()) | set(grouped.get('ambiguous') or set())
        authorized = set(grouped.get('by_magic') or {}) - ambiguous
        grouped['ambiguous'] = ambiguous
        grouped['catalog'] = catalog
        return authorized, grouped

    # ==================================================================
    # SECTION 3 — MT5 READS
    # ==================================================================
    def mt5_history_deals_for_investor(execution_start_dt):
        if execution_start_dt is None:
            execution_start_dt = lagos_now() - timedelta(days=30)
        try:
            deals = mt5_history_deals(execution_start_dt, lagos_now())
        except Exception:
            deals = []
        return deals or []

    def mt5_open_positions():
        try:
            return mt5_positions_get() or []
        except Exception:
            return []

    def mt5_pending_orders():
        try:
            return mt5_orders_get() or []
        except Exception:
            return []

    # ==================================================================
    # SECTION 3b — MT5 ENFORCEMENT
    # ==================================================================
    def mt5_cancel_pending_order(order):
        ticket = getattr(order, 'ticket', None)
        if ticket is None:
            return False, "no_ticket"
        try:
            req = {"action": mt5.TRADE_ACTION_REMOVE, "order": int(ticket)}
            res = mt5_order_send(req)
            if res and res.retcode == mt5.TRADE_RETCODE_DONE:
                return True, "cancelled"
            if res:
                return False, f"retcode={res.retcode} comment={getattr(res, 'comment', '')}"
            return False, f"no_response last_error={mt5_last_error()}"
        except Exception as e:
            return False, f"exception: {e}"

    def mt5_close_position(pos):
        ticket = getattr(pos, 'ticket', None)
        symbol = getattr(pos, 'symbol', None)
        volume = getattr(pos, 'volume', None)
        if ticket is None or not symbol or not volume:
            return False, "missing_position_fields", None

        tick = mt5_symbol_tick(symbol)
        if tick is None:
            return False, f"no_tick_for_{symbol}", None

        if getattr(pos, 'type', None) == 0:
            close_type = mt5.ORDER_TYPE_SELL if mt5 is not None else 1
            close_price = tick.bid
        else:
            close_type = mt5.ORDER_TYPE_BUY if mt5 is not None else 0
            close_price = tick.ask

        attempts = [
            {
                "action": mt5.TRADE_ACTION_DEAL,
                "symbol": symbol,
                "volume": float(volume),
                "type": close_type,
                "position": int(ticket),
                "price": float(close_price),
                "deviation": 20,
                "magic": int(getattr(pos, 'magic', 0) or 0),
                "comment": "UNAUTH-CLOSE",
                "type_time": mt5.ORDER_TIME_GTC,
                "type_filling": mt5.ORDER_FILLING_IOC,
            },
            {
                "action": mt5.TRADE_ACTION_DEAL,
                "symbol": symbol,
                "volume": float(volume),
                "type": close_type,
                "position": int(ticket),
                "price": float(close_price),
                "deviation": 50,
                "magic": int(getattr(pos, 'magic', 0) or 0),
                "comment": "UNAUTH-CLOSE-RETRY",
                "type_filling": mt5.ORDER_FILLING_RETURN,
            },
        ]
        last_msg = "no_attempt"
        for req in attempts:
            try:
                res = mt5_order_send(req)
                if res and res.retcode == mt5.TRADE_RETCODE_DONE:
                    return True, "closed", float(close_price)
                if res:
                    last_msg = f"retcode={res.retcode} comment={getattr(res, 'comment', '')}"
                else:
                    last_msg = f"no_response last_error={mt5_last_error()}"
            except Exception as e:
                last_msg = f"exception: {e}"
        return False, last_msg, None

    # ==================================================================
    # SECTION 4 — NOTIFICATION HOOKS
    # ==================================================================
    def notify_investor_unauthorized(investor_id, sub_account_id,
                                     investor_email, main_account_id):
        if not investor_email or '@' not in investor_email:
            print(f"      [NOTIF] investor email invalid — skip")
            return 0

        try:
            from notification_service import (
                record_contract_notification,
            )
        except Exception as e:
            print(f"      [NOTIF] notification_service unavailable: {e}")
            return 0

        message = (
            "We detected unauthorized actions on your account. "
            "This means trades were placed outside your contract trades. "
            "Unfortunately your contract has been cancelled."
        )

        event_id = f"{int(_time.time() * 1000)}"
        try:
            nid = record_contract_notification(
                _NOTIF_PDO,
                {
                    'user_email': investor_email,
                    'sub_account_id': int(sub_account_id),
                    'main_account_id': int(main_account_id or 0),
                    'notification_key': (
                        f'contract-unauth-investor-{investor_id}-'
                        f'{sub_account_id}-{event_id}'
                    ),
                    'title': 'Unauthorized Actions Detected',
                    'message': message,
                    'type': 'danger',
                    'section': 'Unauthorized',
                    'action_tab': 'activity',
                    'force': False,
                    'send_email': True,
                },
            )
            if nid:
                print(f"      [NOTIF] ✅ investor {investor_email} notified (id={nid})")
                return 1
            print(f"      [NOTIF] investor {investor_email} — record returned None")
            return 0
        except Exception as e:
            print(f"      [NOTIF] investor notify failed: {e}")
            return 0

    def notify_programme_owner_violators(
        programme_id, programme_name, owner_email, violators,
    ):
        if not owner_email or '@' not in owner_email:
            print(f"      [NOTIF] owner email missing/invalid for "
                  f"programme={programme_id} — skip owner notify")
            return 0
        if not violators:
            return 0

        try:
            from notification_service import (
                record_programme_notification,
            )
        except Exception as e:
            print(f"      [NOTIF] notification_service unavailable: {e}")
            return 0

        count = len(violators)
        violator_word = "violator" if count == 1 else "violators"
        investor_word = "investor" if count == 1 else "investors"
        have_word = "has" if count == 1 else "have"

        profit_violators = [v for v in violators if (v.get('pnl') or 0.0) > 0.01]
        noprofit_violators = [v for v in violators if (v.get('pnl') or 0.0) <= 0.01]

        profit_count = len(profit_violators)
        noprofit_count = len(noprofit_violators)

        base = (
            f"We detected {count} of your {programme_name} {investor_word} "
            f"{have_word} violated the programme trades thus the "
            f"{violator_word} {have_word} been paused and can no longer "
            f"proceed with this contract."
        )

        clauses = []

        if profit_count > 0:
            profit_violator_word = (
                "violator" if profit_count == 1 else "violators"
            )
            clauses.append(
                f"Profit percentage made within the periods will be paid "
                f"for the {profit_count} {profit_violator_word} that "
                f"recorded profit."
            )

        if noprofit_count > 0:
            noprofit_violator_word = (
                "violator" if noprofit_count == 1 else "violators"
            )
            clauses.append(
                f"Unfortunately no profit has been made for the "
                f"{noprofit_count} {noprofit_violator_word} that did not "
                f"record profit, so no profit percentage will be paid."
            )

        if not clauses:
            clauses.append(
                "No profit percentage is payable to the violator or violators."
            )

        message = base + " " + " ".join(clauses)

        event_id = f"{int(_time.time() * 1000)}"
        try:
            nid = record_programme_notification(
                _NOTIF_PDO,
                int(programme_id),
                owner_email,
                {
                    'notification_key': (
                        f'prog-unauth-owner-{programme_id}-{event_id}'
                    ),
                    'title': 'Programme Contract Violation Detected',
                    'message': message,
                    'type': 'danger',
                    'section': 'Unauthorized',
                    'action_tab': 'programme_investors',
                    'force': False,
                    'send_email': True,
                },
            )
            if nid:
                print(f"      [NOTIF] ✅ owner {owner_email} notified (id={nid})")
                return 1
            print(f"      [NOTIF] owner {owner_email} — record returned None")
            return 0
        except Exception as e:
            print(f"      [NOTIF] owner notify failed: {e}")
            return 0

    # ==================================================================
    # SECTION 5 — PER-INVESTOR PIPELINE
    # ==================================================================
    def process_investor(investor_id, sub_account_id, programme_id, programme_name):
        detail = {
            'investor_id': investor_id,
            'sub_account_id': sub_account_id,
            'programme_id': programme_id,
            'investor_email': None,
            'owner_email': None,
            'main_account_id': 0,
            'authorized_tradeid_count': 0,
            'unauthorized_orders': 0,
            'unauthorized_orders_cancelled': 0,
            'unauthorized_positions': 0,
            'unauthorized_positions_closed': 0,
            'unauthorized_trades': 0,
            'unauthorized_pnl_total': 0.0,
            'unaccounted_deficit': 0.0,
            'unauthorized_types': [],
            'balance_log_action': None,
            'balance_log_days_written': 0,
            'notifications_sent': 0,
            'application_status': 'approved',
            'error': None,
        }

        identity = investor_fetch_identity(investor_id, sub_account_id)
        investor_email = (_pick_first_key(identity, 'email') or '').strip().lower()
        main_account_id = _coerce_int(
            _raw_first_key(identity, 'main_account_id')
        ) or 0
        fullname = _pick_first_key(identity, 'fullname') or f"user{investor_id}"
        detail['investor_email'] = investor_email
        detail['main_account_id'] = main_account_id

        owner_email, prog_name, owner_userid = programme_fetch_owner_identity(programme_id)
        if not programme_name:
            programme_name = prog_name
        detail['owner_email'] = owner_email

        print(f"\n{'─'*72}")
        print(f" 🔍 Investor {investor_id} (sub {sub_account_id}) | "
              f"programme '{programme_name}' (id={programme_id})")
        print(f"{'─'*72}")
        print(f"  │ Investor email      : {investor_email or '—'}")
        print(f"  │ Programme owner     : {owner_email or '—'}"
              f"{f' (userid={owner_userid})' if owner_userid else ''}")

        authorized_tradeids, group_resolution = investor_fetch_authorized_tradeids(
            investor_id, sub_account_id, programme_id, owner_userid)
        detail['authorized_tradeid_count'] = len(authorized_tradeids)
        if not group_resolution.get('ok'):
            reason = group_resolution.get('reason') or 'programme analytics group mapping unavailable'
            detail['error'] = f'magic/group validation blocked: {reason}'
            detail['mapping_blocked'] = True
            print(f"  │ ⛔ Magic/group validation unavailable: {reason}")
            print(f"  │ ⛔ No MT5 orders or positions will be modified for this subaccount job")
            return detail

        magic_catalog = group_resolution.get('catalog') or {}
        programmes_by_magic = magic_catalog.get('by_magic') or {}
        ambiguous_magics = set(group_resolution.get('ambiguous') or set())
        current_programme_magic_rows = {
            tid for tid, programme_ids in programmes_by_magic.items()
            if programme_id in programme_ids
        }
        other_programme_magics = {
            tid for tid, programme_ids in programmes_by_magic.items()
            if programme_id not in programme_ids
        }
        unresolved_current_magics = current_programme_magic_rows - authorized_tradeids
        detail['group_mapped_tradeid_count'] = len(authorized_tradeids)
        detail['other_programme_magic_count'] = len(other_programme_magics)
        detail['ambiguous_magic_count'] = len(ambiguous_magics)
        detail['unmapped_current_magic_count'] = len(unresolved_current_magics)
        print(f"  │ Group-mapped magic IDs: {len(authorized_tradeids)}")
        print(f"  │ Other programme IDs on same subaccount: {len(other_programme_magics)}")
        if ambiguous_magics:
            print(f"  │ ⚠️ Ambiguous magic IDs across programmes (not touched): {sorted(ambiguous_magics)[:12]}")
        if unresolved_current_magics:
            print(f"  │ ⚠️ Current-programme magic IDs without a chart-group match (not touched): {sorted(unresolved_current_magics)[:12]}")

        def _classify_subaccount_magic(magic):
            try:
                value = int(magic or 0)
            except Exception:
                value = 0
            if value <= 0:
                return 'unauthorized'
            if value in ambiguous_magics:
                return 'ambiguous'
            programme_ids = programmes_by_magic.get(value, set())
            if not programme_ids:
                return 'unauthorized'
            if programme_id in programme_ids:
                return 'authorized' if value in authorized_tradeids else 'unverified'
            return 'other_programme'

        anchor = investor_fetch_anchor(investor_id, sub_account_id)
        broker_balance = _coerce_float(_raw_first_key(anchor, 'broker_balance'), default=None)
        execution_start_date = _raw_first_key(anchor, 'execution_start_date')

        mt5_balance_now = None
        try:
            acc = mt5_account_info()
            if acc is not None:
                mt5_balance_now = float(acc.balance)
        except Exception:
            mt5_balance_now = None

        if broker_balance is None:
            broker_balance = mt5_balance_now
            print(f"  │ ⚠️ harvhub.broker_balance NULL — using MT5 balance "
                  f"as anchor (${(mt5_balance_now or 0):.2f})")

        pending_orders = mt5_pending_orders()
        open_positions = mt5_open_positions()

        unauth_orders = []
        cancelled_orders = 0
        for order in pending_orders:
            try:
                om = int(getattr(order, 'magic', 0) or 0)
            except Exception:
                om = 0
            magic_class = _classify_subaccount_magic(om)
            if magic_class != 'unauthorized':
                continue

            order_rec = {
                'ticket': getattr(order, 'ticket', None),
                'symbol': getattr(order, 'symbol', None),
                'volume': float(getattr(order, 'volume_current', 0) or 0),
                'price': float(getattr(order, 'price_open', 0) or 0),
                'magic': om,
                'action': 'pending',
            }
            print(f"  │ 🚫 UNAUTHORIZED PENDING #{order_rec['ticket']} "
                  f"{order_rec['symbol']} magic={om} — cancelling…")
            ok, msg = mt5_cancel_pending_order(order)
            if ok:
                order_rec['action'] = 'cancelled'
                cancelled_orders += 1
                print(f"  │    ✅ cancelled")
            else:
                order_rec['action'] = 'cancel_failed'
                print(f"  │    ✗ cancel failed: {msg}")
            unauth_orders.append(order_rec)

        unauth_positions = []
        closed_positions = 0
        for pos in open_positions:
            try:
                pm = int(getattr(pos, 'magic', 0) or 0)
            except Exception:
                pm = 0
            magic_class = _classify_subaccount_magic(pm)
            if magic_class != 'unauthorized':
                continue

            pos_rec = {
                'ticket': getattr(pos, 'ticket', None),
                'symbol': getattr(pos, 'symbol', None),
                'volume': float(getattr(pos, 'volume', 0) or 0),
                'price': float(getattr(pos, 'price_open', 0) or 0),
                'profit': float(getattr(pos, 'profit', 0) or 0),
                'magic': pm,
                'action': 'open',
            }
            print(f"  │ 🚫 UNAUTHORIZED POSITION #{pos_rec['ticket']} "
                  f"{pos_rec['symbol']} magic={pm} — closing…")
            ok, msg, close_price = mt5_close_position(pos)
            if ok:
                pos_rec['action'] = 'closed'
                pos_rec['close_price'] = close_price
                closed_positions += 1
                print(f"  │    ✅ closed @ {close_price}")
            else:
                pos_rec['action'] = 'close_failed'
                print(f"  │    ✗ close failed: {msg}")
            unauth_positions.append(pos_rec)

        start_dt = None
        if execution_start_date:
            for fmt in ("%Y-%m-%d", "%B %d, %Y", "%Y/%m/%d",
                        "%Y-%m-%d %H:%M:%S"):
                try:
                    start_dt = datetime.strptime(str(execution_start_date), fmt)
                    break
                except Exception:
                    continue
        if start_dt is None:
            start_dt = lagos_now() - timedelta(days=30)
        start_dt_naive = start_dt.replace(tzinfo=None)

        deals = mt5_history_deals_for_investor(start_dt_naive)

        authorized_deals = []
        unauthorized_deals = []
        total_auth_pnl = 0.0
        total_unauth_pnl = 0.0
        auth_count = 0
        unauth_count = 0

        per_day = {}

        for deal in deals:
            try:
                dm = int(getattr(deal, 'magic', 0) or 0)
            except Exception:
                dm = 0
            pnl = float(getattr(deal, 'profit', 0) or 0) \
                + float(getattr(deal, 'commission', 0) or 0) \
                + float(getattr(deal, 'swap', 0) or 0)
            deal_ts = getattr(deal, 'time', None) or getattr(deal, 'time_msc', None)
            deal_lagos = lagos_from_timestamp(deal_ts)
            day_key = lagos_date_str(deal_lagos)
            if day_key is None:
                continue

            rec = {
                'ticket': getattr(deal, 'ticket', None),
                'symbol': getattr(deal, 'symbol', None),
                'volume': float(getattr(deal, 'volume', 0) or 0),
                'pnl': pnl,
                'magic': dm,
                'time': deal_lagos.strftime('%Y-%m-%d %H:%M:%S') if deal_lagos else None,
            }

            bucket = per_day.setdefault(day_key, {
                'auth': 0.0, 'unauth': 0.0,
                'auth_n': 0, 'unauth_n': 0,
                'records': [],
            })
            bucket['records'].append(rec)

            magic_class = _classify_subaccount_magic(dm)
            if magic_class != 'unauthorized':
                # Keep the balance ledger complete for registered programme
                # magics, including another programme or unresolved legacy
                # group. Do not report an unresolvable mapping as a violation.
                authorized_deals.append(rec)
                total_auth_pnl += pnl
                auth_count += 1
                bucket['auth'] += pnl
                bucket['auth_n'] += 1
            else:
                unauthorized_deals.append(rec)
                total_unauth_pnl += pnl
                unauth_count += 1
                bucket['unauth'] += pnl
                bucket['unauth_n'] += 1

        expected_balance = None
        if broker_balance is not None:
            expected_balance = float(broker_balance) + total_auth_pnl + total_unauth_pnl

        unaccounted_deficit = 0.0
        if expected_balance is not None and mt5_balance_now is not None:
            if mt5_balance_now + 0.01 < expected_balance:
                unaccounted_deficit = round(expected_balance - mt5_balance_now, 2)

        has_trades = bool(unauthorized_deals)
        has_orders = bool(unauth_orders)
        has_positions = bool(unauth_positions)
        has_deficit = unaccounted_deficit > 0.01
        any_unauthorized = has_trades or has_orders or has_positions or has_deficit

        unauth_types = []
        if has_trades:
            unauth_types.append('unauthorized_trades')
        if has_orders:
            unauth_types.append('unauthorized_orders')
        if has_positions:
            unauth_types.append('unauthorized_positions')
        if has_deficit:
            unauth_types.append('unaccounted_balance_deficit')

        if any_unauthorized:
            application_status = 'approved_with_issues'
        else:
            application_status = 'approved'

        detail['unauthorized_orders'] = len(unauth_orders)
        detail['unauthorized_orders_cancelled'] = cancelled_orders
        detail['unauthorized_positions'] = len(unauth_positions)
        detail['unauthorized_positions_closed'] = closed_positions
        detail['unauthorized_trades'] = len(unauthorized_deals)
        detail['unauthorized_pnl_total'] = round(total_unauth_pnl, 2)
        detail['unaccounted_deficit'] = unaccounted_deficit
        detail['unauthorized_types'] = unauth_types
        detail['application_status'] = application_status

        print(f"  │ Authorized closed   : {auth_count} (P&L ${total_auth_pnl:+.2f})")
        print(f"  │ Unauthorized closed : {len(unauthorized_deals)} "
              f"(P&L ${total_unauth_pnl:+.2f})")
        print(f"  │ Unauthorized orders : {len(unauth_orders)} "
              f"(cancelled {cancelled_orders})")
        print(f"  │ Unauthorized pos.   : {len(unauth_positions)} "
              f"(closed {closed_positions})")
        if expected_balance is not None and mt5_balance_now is not None:
            print(f"  │ Expected balance    : ${expected_balance:.2f}")
            print(f"  │ MT5 balance         : ${mt5_balance_now:.2f}")
            if unaccounted_deficit > 0.01:
                print(f"  │ 🚫 Unaccounted deficit: ${unaccounted_deficit:.2f}")
        if any_unauthorized:
            print(f"  │ Status              : ⚠️ UNAUTHORIZED — {', '.join(unauth_types)}")
        else:
            print(f"  │ Status              : ✅ clean")

        # ==================================================================
        # SECTION 5b — PER-DAY balance_log WRITE (BATCHED — 2 queries total)
        # ==================================================================
        today = lagos_today_str()
        exec_start_str = None
        if execution_start_date:
            for fmt in ("%Y-%m-%d", "%B %d, %Y", "%Y/%m/%d",
                        "%Y-%m-%d %H:%M:%S"):
                try:
                    exec_start_str = datetime.strptime(
                        str(execution_start_date), fmt
                    ).strftime("%Y-%m-%d")
                    break
                except Exception:
                    continue

        if not exec_start_str:
            exec_start_str = today
        if exec_start_str > today:
            exec_start_str = today

        day_keys = list(lagos_daterange(exec_start_str, today))
        running_balance = float(broker_balance or 0.0)

        print(f"\n  📅 BALANCE_LOG HISTORY WRITE — {exec_start_str} → {today}")
        print(f"     ├─ Anchor (execution-start broker_balance): ${running_balance:.2f}")
        print(f"     ├─ Days with deals in window              : {len(per_day)}")
        print(f"     └─ Days to write                          : {len(day_keys)}")

        # PASS 1 — one SELECT for all days
        existing_map = {}
        if day_keys:
            ph = ','.join(['%s'] * len(day_keys))
            rows = db_fetch_all(
                f"SELECT id, date, day_unauthorized_withdrawals "
                f"FROM balance_log "
                f"WHERE userid = %s AND sub_account_id = %s "
                f"AND date IN ({ph})",
                [investor_id, sub_account_id] + day_keys,
                timeout=15.0, quiet=True,
            )
            for r in rows:
                d = str(_raw_first_key(r, 'date') or '')[:10]
                if d:
                    existing_map[d] = r

        # PASS 2 — build one big batch of UPDATE/INSERT statements
        statements = []
        day_actions = []
        for day_key in day_keys:
            bucket = per_day.get(day_key, {
                'auth': 0.0, 'unauth': 0.0,
                'auth_n': 0, 'unauth_n': 0,
                'records': [],
            })

            day_open = running_balance
            day_auth = float(bucket['auth'])
            day_unauth = float(bucket['unauth'])

            if day_key == today and mt5_balance_now is not None:
                day_close = float(mt5_balance_now)
            else:
                day_close = day_open + day_auth + day_unauth

            existing_row = existing_map.get(day_key, {})
            existing_withdrawals = _coerce_float(
                _raw_first_key(existing_row, 'day_unauthorized_withdrawals'),
                default=0.0,
            ) or 0.0

            day_unusual = (bucket['unauth_n'] > 0)

            if _raw_first_key(existing_row, 'id'):
                row_id = _coerce_int(_raw_first_key(existing_row, 'id'))
                statements.append(
                    "UPDATE balance_log SET "
                    f"day_starting_balance={round(day_open, 8)}, "
                    f"day_authorized_trades_pnl={round(day_auth, 8)}, "
                    f"day_unauthorized_trades_pnl={round(day_unauth, 8)}, "
                    f"day_unauthorized_withdrawals={round(existing_withdrawals, 8)}, "
                    f"day_closing_balance={round(day_close, 8)}, "
                    f"unusual_activity={1 if day_unusual else 0}, "
                    f"authorized_trades_count={int(bucket['auth_n'])}, "
                    f"unauthorized_trades_count={int(bucket['unauth_n'])} "
                    f"WHERE id={row_id}"
                )
                day_actions.append((day_key, 'updated', day_open,
                                    day_auth, day_unauth, day_close))
            else:
                statements.append(
                    "INSERT INTO balance_log "
                    "(userid, sub_account_id, date, "
                    "day_starting_balance, day_authorized_trades_pnl, "
                    "day_unauthorized_trades_pnl, day_unauthorized_withdrawals, "
                    "day_closing_balance, unusual_activity, "
                    "authorized_trades_count, unauthorized_trades_count) "
                    f"VALUES ({investor_id}, {sub_account_id}, '{day_key}', "
                    f"{round(day_open, 8)}, {round(day_auth, 8)}, "
                    f"{round(day_unauth, 8)}, {round(existing_withdrawals, 8)}, "
                    f"{round(day_close, 8)}, {1 if day_unusual else 0}, "
                    f"{int(bucket['auth_n'])}, {int(bucket['unauth_n'])})"
                )
                day_actions.append((day_key, 'inserted', day_open,
                                    day_auth, day_unauth, day_close))

            running_balance = day_close

        # ONE batched round-trip for every day
        if statements:
            db_query_many(statements, timeout=60.0, quiet=True)

        days_written = len(statements)
        last_action = day_actions[-1][1] if day_actions else None

        for (day_key, action, day_open, day_auth, day_unauth, day_close) in day_actions:
            bucket = per_day.get(day_key, {'auth_n': 0, 'unauth_n': 0})
            marker = ''
            if day_key == today:
                marker = '  ← today'
            elif bucket['auth_n'] + bucket['unauth_n'] > 0:
                marker = f"  ({bucket['auth_n']+bucket['unauth_n']} deal(s))"
            print(f"     │ {day_key}: open=${day_open:.2f} "
                  f"auth=${day_auth:+.2f} unauth=${day_unauth:+.2f} "
                  f"close=${day_close:.2f} [{action}]{marker}")

        detail['balance_log_action'] = last_action
        detail['balance_log_days_written'] = days_written
        print(f"     └─ {days_written} row(s) written to balance_log (batched)")

        if any_unauthorized:
            notif_count = 0
            notif_count += notify_investor_unauthorized(
                investor_id=investor_id,
                sub_account_id=sub_account_id,
                investor_email=investor_email,
                main_account_id=main_account_id,
            )
            _register_programme_violator(
                programme_id=programme_id,
                programme_name=programme_name,
                investor_id=investor_id,
                sub_account_id=sub_account_id,
                total_unauth_pnl=total_unauth_pnl,
                unauth_types=unauth_types,
            )
            detail['notifications_sent'] = notif_count

        return detail

    # ==================================================================
    # SECTION 6 — TOP-LEVEL ENUMERATION
    # ==================================================================
    if jobs is None:
        print("\n" + "─"*80)
        print(" 📁 LOADING INVESTOR JOBS FROM DB (VPS-scoped)")
        print("─"*80)

        local_ip = get_local_ip()
        if not local_ip:
            print(" ⚠️ Could not determine local IP")
            stats['errors'].append('local IP unknown')
            stats['processing_success'] = False
            unauthorized_alert['errors'] = list(stats['errors'])
            return stats
        print(f" │ Local IP: {local_ip}")

        all_jobs = fetch_vps_investor_jobs(local_ip)
        if not all_jobs:
            print(" │ ℹ️ No active investor jobs found for this VPS")
            stats['processing_success'] = True
            return stats

        if inv_db_id:
            jobs = [j for j in all_jobs if str(j.get('investor_id')) == str(inv_db_id)]
            if not jobs:
                print(f" ⚠️ Investor {inv_db_id} not found in any VPS-scoped job")
                stats['errors'].append(f"Investor {inv_db_id} not found")
                stats['processing_success'] = False
                unauthorized_alert['errors'] = list(stats['errors'])
                return stats
        else:
            jobs = all_jobs

        print(f" │ Loaded {len(jobs)} investor job(s) from DB")
        print("─"*80)

    for job in jobs:
        investor_id = job.get('investor_id')
        sub_account_id = job.get('sub_account_id')
        programme_id = job.get('programme_id')
        programme_name = job.get('programme_name') or f"programme_{programme_id}"

        if investor_id is None or sub_account_id is None or programme_id is None:
            continue

        try:
            detail = process_investor(investor_id, sub_account_id,
                                      programme_id, programme_name)
            stats['details'].append(detail)
            stats['investors_processed'] += 1
            stats['unauthorized_orders_found'] += detail.get('unauthorized_orders', 0)
            stats['unauthorized_orders_cancelled'] += detail.get('unauthorized_orders_cancelled', 0)
            stats['unauthorized_positions_found'] += detail.get('unauthorized_positions', 0)
            stats['unauthorized_positions_closed'] += detail.get('unauthorized_positions_closed', 0)
            stats['unauthorized_trades_found'] += detail.get('unauthorized_trades', 0)
            if detail.get('unaccounted_deficit', 0.0) > 0.01:
                stats['unaccounted_deficits_found'] += 1
            if detail.get('unauthorized_types'):
                stats['investors_with_unauthorized'] += 1
            stats['balance_log_rows_written'] += detail.get('balance_log_days_written', 0)
            stats['notifications_queued'] += detail.get('notifications_sent', 0)

            if not unauthorized_alert.get('programme_id'):
                unauthorized_alert['programme_id'] = programme_id
                unauthorized_alert['investor_id'] = investor_id
                unauthorized_alert['sub_account_id'] = sub_account_id
                unauthorized_alert['investor_authorized_tradeids'] = set()
                unauthorized_alert['application_status'] = detail.get('application_status', 'approved')
                if detail.get('unauthorized_types'):
                    unauthorized_alert['is_triggered'] = True
                    unauthorized_alert['unauthorized_types'] = detail['unauthorized_types']
                    unauthorized_alert['unauthorized_pnl_total'] = detail.get('unauthorized_pnl_total', 0.0)
                    unauthorized_alert['unaccounted_deficit'] = detail.get('unaccounted_deficit', 0.0)
        except Exception as e:
            print(f"\n[CRITICAL] unauthorized-audit investor={investor_id}: {e}")
            traceback.print_exc()
            stats['errors'].append(f"investor={investor_id}: {e}")

    # ==================================================================
    # SECTION 7 — AGGREGATED PROGRAMME-OWNER NOTIFICATIONS
    # ==================================================================
    if programme_violators:
        print(f"\n{'='*10} 📣 AGGREGATED PROGRAMME-OWNER NOTIFICATIONS {'='*10}")
        for pid, entry in programme_violators.items():
            programme_name = entry.get('programme_name') or f"programme {pid}"
            violators = entry.get('violators') or []
            if not violators:
                continue

            owner_email, resolved_name, owner_userid = programme_fetch_owner_identity(pid)
            if resolved_name and not entry.get('programme_name'):
                programme_name = resolved_name

            print(f"  │ Programme {pid} ('{programme_name}') — "
                  f"{len(violators)} violator(s) | owner={owner_email or '—'}")

            sent = notify_programme_owner_violators(
                programme_id=pid,
                programme_name=programme_name,
                owner_email=owner_email,
                violators=violators,
            )
            stats['notifications_queued'] += sent

    print(f"\n{'='*10} 🔍 AUTHORIZED-ACTIONS AUDIT SUMMARY (DB) {'='*10}")
    print(f"  Investors processed        : {stats['investors_processed']}")
    print(f"  Investors with unauthorized: {stats['investors_with_unauthorized']}")
    print(f"  Unauthorized orders        : {stats['unauthorized_orders_found']}")
    print(f"  Unauthorized orders cancelled: {stats['unauthorized_orders_cancelled']}")
    print(f"  Unauthorized positions     : {stats['unauthorized_positions_found']}")
    print(f"  Unauthorized positions closed: {stats['unauthorized_positions_closed']}")
    print(f"  Unauthorized trades        : {stats['unauthorized_trades_found']}")
    print(f"  Unaccounted deficits       : {stats['unaccounted_deficits_found']}")
    print(f"  balance_log rows written   : {stats['balance_log_rows_written']}")
    print(f"  Notifications queued       : {stats['notifications_queued']}")
    print(f"  Errors                     : {len(stats['errors'])}")
    for e in stats['errors']:
        print(f"     • {e}")
    print(f"{'='*10} 🏁 COMPLETE {'='*10}\n")

    stats['processing_success'] = True
    return stats

# ==============================================================================
#  RESTRICTED TIMERANGE — Restricted Dates FIRST, then Restricted Days
# ==============================================================================
def restricted_timerange_db(inv_db_id=None, jobs=None):
    print(f"\n{'='*10} ⏰ PROGRAMME RESTRICTED TIMERANGE {'='*10}")

    if jobs is None:
        local_ip = get_local_ip()
        if not local_ip:
            print("  ⚠️ Could not determine local IP")
            return {
                'investor_id': inv_db_id if inv_db_id else 'all',
                'processing_success': False,
                'errors': ['local IP unknown'],
                'restriction_map': {},
                'any_restricted': False,
            }

        print(f"  🌐 Local IP: {local_ip}")
        programmes = fetch_vps_programmes(local_ip)
    else:
        # Derive unique programmes from jobs (skip discovery entirely).
        seen = set()
        programmes = []
        for j in jobs:
            pid = j.get('programme_id')
            if pid and pid not in seen:
                seen.add(pid)
                programmes.append({'programme_id': pid, 'userid': None})

        # Pull owners in one shot since jobs didn't carry userid.
        pids = [p['programme_id'] for p in programmes]
        if pids:
            ph = ','.join(['%s'] * len(pids))
            rows = db_fetch_all(
                f"SELECT id, userid FROM programme WHERE id IN ({ph})",
                pids, timeout=15.0, quiet=True,
            )
            owner_by_programme = {}
            for row in rows:
                pid = _coerce_int(_pick_first_key(row, 'id'))
                uid = _coerce_int(_pick_first_key(row, 'userid'))
                if pid is not None:
                    owner_by_programme[pid] = uid
            for p in programmes:
                p['userid'] = owner_by_programme.get(p['programme_id'])

    if not programmes:
        print("  ℹ️ No programmes bound to this VPS")
        return {
            'investor_id': inv_db_id if inv_db_id else 'all',
            'processing_success': True,
            'programmes_checked': 0,
            'restriction_map': {},
            'any_restricted': False,
        }

    total_programmes = len(programmes)
    print(f"  📋 Found {total_programmes} programme(s) bound to this VPS")

    # ------------------------------------------------------------------
    # BATCH PREFETCH — metadata + restrictions + owner emails, in 4 queries.
    # ------------------------------------------------------------------
    programme_ids_all = [p['programme_id'] for p in programmes]
    meta_map  = fetch_programmes_metadata_batch(programme_ids_all)
    restr_map = fetch_programme_restrictions_batch(programme_ids_all)

    # BATCH owner emails for every programme on this VPS (1 query).
    owner_ids = list({p['userid'] for p in programmes if p.get('userid')})
    owner_email_map = {}
    if owner_ids:
        ph = ','.join(['%s'] * len(owner_ids))
        rows = db_fetch_all(
            f"SELECT id, email FROM harvhub "
            f"WHERE id IN ({ph}) AND email IS NOT NULL AND email <> ''",
            owner_ids, timeout=15.0, quiet=True,
        )
        for row in rows:
            oid = _coerce_int(_pick_first_key(row, 'id'))
            em  = _pick_first_key(row, 'email')
            if oid is not None and em:
                owner_email_map[oid] = str(em).strip().lower()

    restriction_map = {}

    def _notify_programme_restriction(programme_id, programme_name,
                                      userid, notif_kind,
                                      today_date_str, tz_str,
                                      extra=None):
        extra = extra or {}
        try:
            from notification_service import (
                record_programme_restriction_date_notification,
                record_programme_restriction_day_full_notification,
                record_programme_restriction_day_window_notification,
            )
        except Exception as e:
            print(f"      [PRG][NOTIFY] notification_service unavailable: {e}")
            return None

        owner_email = owner_email_map.get(userid, '')

        if not owner_email or '@' not in owner_email:
            print(f"      [PRG][NOTIFY] programme={programme_id} "
                  f"owner email unresolved (userid={userid}) — "
                  f"notification skipped")
            return None

        try:
            if notif_kind == 'restricted_date':
                nid = record_programme_restriction_date_notification(
                    _NOTIF_PDO,
                    programme_id=programme_id,
                    programme_name=programme_name,
                    owner_email=owner_email,
                    restricted_date=today_date_str,
                    timezone_str=tz_str,
                )
            elif notif_kind == 'restricted_day_full':
                nid = record_programme_restriction_day_full_notification(
                    _NOTIF_PDO,
                    programme_id=programme_id,
                    programme_name=programme_name,
                    owner_email=owner_email,
                    day_name=extra.get('day_name', ''),
                    restricted_date=today_date_str,
                    timezone_str=tz_str,
                )
            elif notif_kind == 'restricted_day_window':
                nid = record_programme_restriction_day_window_notification(
                    _NOTIF_PDO,
                    programme_id=programme_id,
                    programme_name=programme_name,
                    owner_email=owner_email,
                    day_name=extra.get('day_name', ''),
                    restricted_date=today_date_str,
                    from_label=extra.get('from_label', ''),
                    to_label=extra.get('to_label', ''),
                    timezone_str=tz_str,
                )
            else:
                return None

            if nid:
                print(f"      [PRG][NOTIFY] programme={programme_id} "
                      f"({notif_kind}) -> notification_id={nid}")
            else:
                print(f"      [PRG][NOTIFY] programme={programme_id} "
                      f"({notif_kind}) -> already sent or ineligible (dedup)")
            return nid
        except Exception as e:
            print(f"      [PRG][NOTIFY] {notif_kind} raised: {e}")
            traceback.print_exc()
            return None

    for idx, prog in enumerate(programmes, start=1):
        programme_id = prog['programme_id']
        developer_id = prog['userid']

        meta = meta_map.get(programme_id, {})
        programme_name = meta.get('programme_name') or ''
        tz_str = meta.get('timezone')

        if tz_str is None:
            print(f"\n  [{idx}/{total_programmes}] 📊 Programme "
                  f"'{programme_name}' (id={programme_id}) | "
                  f"⚠️ timezone NOT configured — skipping restriction check")
            restriction_map[programme_id] = {
                'is_restricted': False,
                'kind':          None,
                'day_name':      '',
                'window_desc':   'timezone not configured',
            }
            continue

        now_local, resolved = get_current_time_in_timezone(tz_str)
        if not resolved:
            print(f"\n  [{idx}/{total_programmes}] 📊 Programme "
                  f"'{programme_name}' (id={programme_id}) | "
                  f"⚠️ Could not resolve timezone '{tz_str}' "
                  f"— skipping restriction check")
            restriction_map[programme_id] = {
                'is_restricted': False,
                'kind':          None,
                'day_name':      '',
                'window_desc':   f"timezone '{tz_str}' unresolvable",
            }
            continue

        now_day_name = now_local.strftime("%A")
        today_date_str = now_local.strftime("%Y-%m-%d")

        print(f"\n  [{idx}/{total_programmes}] 📊 Programme '{programme_name}' "
              f"(id={programme_id}) | timezone='{tz_str}'")
        print(f"      🕐 Programme now: "
              f"{now_local.strftime('%Y-%m-%d %I:%M:%S %p')} | "
              f"day={now_day_name}")

        restr = restr_map.get(programme_id, {'dates': [], 'days': []})
        restricted_dates = restr['dates']

        today_in_dates = False
        if restricted_dates:
            for d in restricted_dates:
                if d == today_date_str:
                    today_in_dates = True
                    break

        print(f"      📋 Restricted dates for '{programme_name}':")
        if not restricted_dates:
            print(f"         (none configured)")
        else:
            for d in restricted_dates:
                marker = "  ⬅ TODAY" if d == today_date_str else ""
                print(f"         • {d}{marker}")

        if today_in_dates:
            print(f"      🚨 {today_date_str} IS a restricted date "
                  f"— FULL-DAY LOCKOUT (Stage 1)")
            day_state = "full_day_date"
            window_desc = "FULL DAY (restricted date)"

            restriction_map[programme_id] = {
                'is_restricted': True,
                'kind':          'restricted_date',
                'day_name':      now_day_name,
                'window_desc':   window_desc,
            }

            _notify_programme_restriction(
                programme_id=programme_id,
                programme_name=programme_name,
                userid=developer_id,
                notif_kind='restricted_date',
                today_date_str=today_date_str,
                tz_str=tz_str,
            )
        else:
            print(f"      ℹ️ {today_date_str} is not a restricted date "
                  f"— falling through to Restricted Days")

            restriction_rows = restr['days']

            today_row = None
            for r in restriction_rows:
                if r['day'].lower() == now_day_name.lower():
                    today_row = r
                    break

            if not restriction_rows:
                print("      ✅ No restricted days configured for this programme")
                day_state = "no_restriction"
                window_desc = "—"

                restriction_map[programme_id] = {
                    'is_restricted': False,
                    'kind':          None,
                    'day_name':      now_day_name,
                    'window_desc':   window_desc,
                }

            elif today_row is None:
                print(f"      ✅ {now_day_name} is NOT restricted for this programme")
                day_state = "not_today"
                window_desc = "—"

                restriction_map[programme_id] = {
                    'is_restricted': False,
                    'kind':          None,
                    'day_name':      now_day_name,
                    'window_desc':   window_desc,
                }

            else:
                from_h, from_m = parse_time_string(today_row['from_time'])
                to_h, to_m = parse_time_string(today_row['to_time'])

                if from_h is None or to_h is None:
                    print(f"      ⚠️ Invalid time format for today's row: "
                          f"{today_row['from_time']} -> {today_row['to_time']}")
                    day_state = "invalid"
                    window_desc = "invalid"

                    restriction_map[programme_id] = {
                        'is_restricted': False,
                        'kind':          None,
                        'day_name':      now_day_name,
                        'window_desc':   window_desc,
                    }

                elif is_full_day_window(from_h, from_m, to_h, to_m):
                    print(f"      🚨 {now_day_name} is FULLY RESTRICTED "
                          f"(no trading allowed all day)")
                    day_state = "full_day"
                    window_desc = "12:00 AM - 12:00 AM (FULL DAY)"

                    restriction_map[programme_id] = {
                        'is_restricted': True,
                        'kind':          'restricted_day_full',
                        'day_name':      now_day_name,
                        'window_desc':   window_desc,
                    }

                    _notify_programme_restriction(
                        programme_id=programme_id,
                        programme_name=programme_name,
                        userid=developer_id,
                        notif_kind='restricted_day_full',
                        today_date_str=today_date_str,
                        tz_str=tz_str,
                        extra={'day_name': now_day_name},
                    )
                else:
                    in_window = is_time_in_window(
                        now_local.hour, now_local.minute,
                        from_h, from_m, to_h, to_m,
                    )
                    window_desc = (f"{to_12hr(from_h, from_m)} - "
                                   f"{to_12hr(to_h, to_m)}")
                    day_state = "in_window" if in_window else "outside_window"
                    marker = ("🔴 WITHIN (restricted)" if in_window
                              else "🟢 OUTSIDE (free)")
                    print(f"      📅 {now_day_name} restricted window: "
                          f"{window_desc} | {marker}")

                    if in_window:
                        restriction_map[programme_id] = {
                            'is_restricted': True,
                            'kind':          'restricted_day_window',
                            'day_name':      now_day_name,
                            'window_desc':   window_desc,
                        }
                    else:
                        restriction_map[programme_id] = {
                            'is_restricted': False,
                            'kind':          None,
                            'day_name':      now_day_name,
                            'window_desc':   window_desc,
                        }

                    if in_window:
                        _notify_programme_restriction(
                            programme_id=programme_id,
                            programme_name=programme_name,
                            userid=developer_id,
                            notif_kind='restricted_day_window',
                            today_date_str=today_date_str,
                            tz_str=tz_str,
                            extra={
                                'day_name':   now_day_name,
                                'from_label': to_12hr(from_h, from_m),
                                'to_label':   to_12hr(to_h, to_m),
                            },
                        )

            print(f"      📋 Restricted days for '{programme_name}':")
            if not restriction_rows:
                print(f"         (none configured)")
            else:
                for r in restriction_rows:
                    fh, fm = parse_time_string(r['from_time'])
                    th, tm = parse_time_string(r['to_time'])
                    if fh is None or th is None:
                        label = f"{r['from_time'] or '—'} -> {r['to_time'] or '—'}"
                    elif is_full_day_window(fh, fm, th, tm):
                        label = "FULL DAY (12:00 AM -> 12:00 AM)"
                    else:
                        label = f"{to_12hr(fh, fm)} -> {to_12hr(th, tm)}"
                    print(f"         • {r['day']:<10} {label}")

        # =====================================================================
        # Per-investor printing (display only)
        # =====================================================================
        investors = fetch_active_investors_for_programme(programme_id)
        if not investors:
            print(f"      ℹ️ No ACTIVE investors associated with this programme")
            continue

        print(f"      👥 {len(investors)} active investor(s) associated:")

        # BATCH identity lookup for every investor on this programme (1 query).
        investor_ids = sorted({
            int(inv['investor_id']) for inv in investors
            if _coerce_int(inv.get('investor_id')) and int(inv['investor_id']) > 0
        })
        identity_map = {}
        if investor_ids:
            # The remote bridge accepts one bound placeholder per request;
            # these IDs are positive integers read directly from the database.
            investor_id_sql = ','.join(str(i) for i in investor_ids)
            rows = db_fetch_all(
                "SELECT id, fullname, username, email FROM harvhub "
                f"WHERE id IN ({investor_id_sql})",
                None, timeout=15.0, quiet=True,
            )
            for row in rows:
                iid = _coerce_int(_pick_first_key(row, 'id'))
                if iid is None:
                    continue
                identity_map[iid] = {
                    'id':       iid,
                    'fullname': (_pick_first_key(row, 'fullname') or '').strip(),
                    'username': (_pick_first_key(row, 'username') or '').strip(),
                    'email':    (_pick_first_key(row, 'email') or '').strip(),
                }

        for inv in investors:
            inv_db_id_val = inv['investor_id']
            sub_id_val = inv['sub_account_id']

            if inv_db_id is not None:
                try:
                    if str(inv_db_id_val) != str(inv_db_id):
                        continue
                except Exception:
                    pass

            identity = identity_map.get(inv_db_id_val, {})
            username = (identity.get('username')
                        or identity.get('fullname')
                        or f"user{inv_db_id_val}")

            print(
                f"\n         👤 Investor {username} "
                f"id{sub_id_val} is associated with this vps and invested "
                f"in Trader's {programme_name} id{programme_id}"
            )
            print(f"            so according to the trader programme:")

            if day_state == "full_day_date":
                print(f"            Restricted dates:")
                for d in restricted_dates:
                    marker = "  ⬅ TODAY" if d == today_date_str else ""
                    print(f"               • {d}{marker}")
                print(f"            Status right now: 🚨 FULL-DAY LOCKOUT "
                      f"(today is a configured restricted date) "
                      f"— no activities allowed")

            elif day_state == "no_restriction":
                print(f"            Restricted days:")
                print(f"               (none)")
                print(f"            Status right now: no restriction configured")

            elif day_state == "not_today":
                print(f"            Restricted days:")
                for r in restriction_rows:
                    fh, fm = parse_time_string(r['from_time'])
                    th, tm = parse_time_string(r['to_time'])
                    if fh is None or th is None:
                        label = f"{r['from_time'] or '—'} -> {r['to_time'] or '—'}"
                    elif is_full_day_window(fh, fm, th, tm):
                        label = "FULL DAY (12:00 AM -> 12:00 AM)"
                    else:
                        label = f"{to_12hr(fh, fm)} -> {to_12hr(th, tm)}"
                    print(f"               • {r['day']:<10} {label}")
                print(f"            Status right now: {now_day_name} is not restricted")

            elif day_state == "full_day":
                print(f"            Restricted days:")
                for r in restriction_rows:
                    fh, fm = parse_time_string(r['from_time'])
                    th, tm = parse_time_string(r['to_time'])
                    if fh is None or th is None:
                        label = f"{r['from_time'] or '—'} -> {r['to_time'] or '—'}"
                    elif is_full_day_window(fh, fm, th, tm):
                        label = "FULL DAY (12:00 AM -> 12:00 AM)"
                    else:
                        label = f"{to_12hr(fh, fm)} -> {to_12hr(th, tm)}"
                    print(f"               • {r['day']:<10} {label}")
                print(f"            Status right now: 🚨 FULL-DAY RESTRICTION ACTIVE "
                      f"— no activities allowed")

            elif day_state == "in_window":
                print(f"            Restricted days:")
                for r in restriction_rows:
                    fh, fm = parse_time_string(r['from_time'])
                    th, tm = parse_time_string(r['to_time'])
                    if fh is None or th is None:
                        label = f"{r['from_time'] or '—'} -> {r['to_time'] or '—'}"
                    elif is_full_day_window(fh, fm, th, tm):
                        label = "FULL DAY (12:00 AM -> 12:00 AM)"
                    else:
                        label = f"{to_12hr(fh, fm)} -> {to_12hr(th, tm)}"
                    print(f"               • {r['day']:<10} {label}")
                print(f"            Status right now: 🔴 WITHIN restricted window "
                      f"({window_desc}) — no activities allowed")

            elif day_state == "outside_window":
                print(f"            Restricted days:")
                for r in restriction_rows:
                    fh, fm = parse_time_string(r['from_time'])
                    th, tm = parse_time_string(r['to_time'])
                    if fh is None or th is None:
                        label = f"{r['from_time'] or '—'} -> {r['to_time'] or '—'}"
                    elif is_full_day_window(fh, fm, th, tm):
                        label = "FULL DAY (12:00 AM -> 12:00 AM)"
                    else:
                        label = f"{to_12hr(fh, fm)} -> {to_12hr(th, tm)}"
                    print(f"               • {r['day']:<10} {label}")
                print(f"            Status right now: 🟢 outside restricted window "
                      f"({window_desc}) — free to trade")

            else:
                print(f"            Status right now: ⚠️ restriction row is invalid")

    return {
        'investor_id':        inv_db_id if inv_db_id else 'all',
        'processing_success': True,
        'programmes_checked': total_programmes,
        'restriction_map':    restriction_map,
        'any_restricted':     any(
            v.get('is_restricted') for v in restriction_map.values()
        ),
    }


# ==============================================================================
#  RECENT HIGHEST BALANCE TARGET — DB-driven, programme-scoped
# ==============================================================================
def recent_highest_balance_revenue_target_db(inv_db_id=None, jobs=None):
    """
    DB-driven recent_highest_balance tracker + daily_target_revenue writer.

    READS:
      * programme_revenue_target          -> daily target config (per programme)
      * harvhub.recent_highest_balance    -> stored last-seen balance per sub-account
      * harvhub.recent_highest_balance_last_update -> last update date per sub-account
      * harvhub.execution_start_date / broker_balance -> contract anchors

    WRITES:
      * harvhub.recent_highest_balance (+ last_update)         per sub-account
      * daily_target_revenue              -> daily target met rows per sub-account
                                            INCLUDING:
                                              - daily_target_owed
                                              - daily_target_owed_day_balance
                                              - daily_target_owed_date

    No JSON. No files. No folders.
    """

    global recent_highest_alert

    from datetime import datetime, timedelta, timezone

    # ------------------------------------------------------------------
    # LAGOS TIMEZONE SETUP
    # ------------------------------------------------------------------
    try:
        from zoneinfo import ZoneInfo
        LAGOS_TZ = ZoneInfo("Africa/Lagos")
        _LAGOS_TZ_SOURCE = "zoneinfo"
    except Exception:
        LAGOS_TZ = timezone(timedelta(hours=1))
        _LAGOS_TZ_SOURCE = "fixed_utc+1"

    def lagos_now():
        return datetime.now(LAGOS_TZ)

    def lagos_today_str():
        return lagos_now().strftime("%Y-%m-%d")

    def lagos_yesterday_str():
        return (lagos_now() - timedelta(days=1)).strftime("%Y-%m-%d")

    def lagos_naive_now():
        return lagos_now().replace(tzinfo=None)

    def lagos_from_timestamp(ts):
        if not ts:
            return None
        try:
            ts_float = float(ts)
        except (TypeError, ValueError):
            return None
        if ts_float > 1e11:
            ts_float = ts_float / 1000.0
        try:
            return datetime.fromtimestamp(ts_float, tz=timezone.utc).astimezone(LAGOS_TZ)
        except Exception:
            return None

    def detect_mt5_server_offset_hours():
        try:
            if not mt5.terminal_info():
                return 0
            candidate_symbols = ["EURUSD", "GBPUSD", "USDJPY", "XAUUSD", "US30"]
            tick = None
            for sym in candidate_symbols:
                try:
                    tick = mt5.symbol_info_tick(sym)
                except Exception:
                    tick = None
                if tick is not None:
                    break
            if tick is None or not getattr(tick, 'time', None):
                return 0
            server_naive = datetime.fromtimestamp(tick.time)
            utc_naive = datetime.utcnow()
            diff_seconds = (server_naive - utc_naive).total_seconds()
            diff_seconds = round(diff_seconds / 900.0) * 900.0
            offset_hours = diff_seconds / 3600.0
            if offset_hours < -12 or offset_hours > 14:
                return 0
            return offset_hours
        except Exception:
            return 0

    def lagos_window_to_server_naive(lagos_start_aware, lagos_end_aware):
        server_offset = detect_mt5_server_offset_hours()
        start_utc_naive = lagos_start_aware.astimezone(timezone.utc).replace(tzinfo=None)
        end_utc_naive = lagos_end_aware.astimezone(timezone.utc).replace(tzinfo=None)
        start_server_naive = start_utc_naive + timedelta(hours=server_offset)
        end_server_naive = end_utc_naive + timedelta(hours=server_offset)
        return start_server_naive, end_server_naive, server_offset

    recent_highest_alert = {
        'is_triggered': False,
        'investor_id': inv_db_id if inv_db_id else "all",
        'old_balance': None,
        'new_balance': None,
        'difference': None,
        'timestamp': lagos_now().strftime('%I:%M:%S %p'),
        'action': None,
        'last_update_date': None,
        'today_profit': None,
        'daily_target_risk': None,
        'profit_met_risk_threshold': False,
        'alarm_blocked_reason': None
    }

    print(f"\n{'='*10} 📊 RECENT HIGHEST BALANCE TARGET (DB) {'='*10}")
    print(f" 🕒 Lagos time now: {lagos_now().strftime('%Y-%m-%d %H:%M:%S %Z')} (source: {_LAGOS_TZ_SOURCE})")
    if inv_db_id:
        print(f" 🎯 Target Investor: {inv_db_id}")

    stats = {
        "investor_id": inv_db_id if inv_db_id else "all",
        "investors_checked": 0,
        "investors_updated": 0,
        "investors_skipped": 0,
        "new_high_alerts": 0,
        "new_high_recorded_only": 0,
        "field_created": 0,
        "broker_balance_corrections": 0,
        "stale_record_resets": 0,
        "alarm_confirmations_blocked": 0,
        "activities_updated": 0,
        "daily_target_rows_written": 0,
        "errors": [],
        "current_time": lagos_now().strftime('%I:%M:%S %p'),
        "processing_success": False,
        "details": []
    }

    # ----------------------------------------------------------------
    # DB helpers specific to this stage
    # ----------------------------------------------------------------
    def db_get_harvhub_row(investor_id, sub_account_id):
        sql = """
            SELECT id, sub_account_id, main_account_id, fullname, username, email,
                   broker_balance, execution_start_date,
                   recent_highest_balance, recent_highest_balance_last_update
              FROM harvhub
             WHERE id = %s
               AND sub_account_id = %s
             LIMIT 1
        """
        return db_fetch_one(sql, [investor_id, sub_account_id], timeout=15.0, quiet=True)

    def db_write_recent_balance(investor_id, sub_account_id, balance, date_str):
        sql = """
            UPDATE harvhub
               SET recent_highest_balance = %s,
                   recent_highest_balance_last_update = %s
             WHERE id = %s
               AND sub_account_id = %s
        """
        ok, affected, msg = db_execute(
            sql, [balance, date_str, investor_id, sub_account_id],
            timeout=15.0, quiet=True,
        )
        return ok and affected >= 0

    def db_write_recent_balance_balance_only(investor_id, sub_account_id, balance):
        sql = """
            UPDATE harvhub
               SET recent_highest_balance = %s
             WHERE id = %s
               AND sub_account_id = %s
        """
        ok, affected, msg = db_execute(
            sql, [balance, investor_id, sub_account_id],
            timeout=15.0, quiet=True,
        )
        return ok

    def db_fetch_revenue_target_config_for_programme(programme_id):
        rows = db_fetch_all(
            """
            SELECT week_no, day_name, from_balance, to_balance,
                   target_amount, pre_pending_sum, include_missed_days,
                   target_daily_profit, include_current_day_as_owed,
                   loss_streak_chances, revenue_target_period
              FROM programme_revenue_target
             WHERE programme_id = %s
             ORDER BY week_no ASC, id ASC
            """,
            [programme_id], timeout=15.0, quiet=True,
        )
        if not rows:
            return None

        first = rows[0]
        period = str(_raw_first_key(first, 'revenue_target_period') or 'daily').strip().lower()
        if period not in ('daily', 'weekly', 'monthly', 'annually'):
            raise ValueError(f"Invalid revenue_target_period in programme {programme_id}: {period!r}")
        cfg = {
            'revenue_target_period': period,
            'include_current_day_as_owed': to_bool(_raw_first_key(first, 'include_current_day_as_owed')),
            'loss_streak_chances': _coerce_int(_raw_first_key(first, 'loss_streak_chances')) or 0,
        }

        for r in rows:
            wk = _coerce_int(_raw_first_key(r, 'week_no')) or 1
            day_name = (_raw_first_key(r, 'day_name') or '').strip()
            if not day_name:
                continue
            day_lower = day_name.lower()
            frm = _coerce_float(_raw_first_key(r, 'from_balance'), default=0.0)
            to_raw = _raw_first_key(r, 'to_balance')
            to_val = _coerce_float(to_raw, default=None)
            target = _coerce_float(_raw_first_key(r, 'target_amount'), default=0.0)
            pre_pending = to_bool(_raw_first_key(r, 'pre_pending_sum'))
            include_missed = to_bool(_raw_first_key(r, 'include_missed_days'))

            if to_val is None:
                range_key = f"{int(frm)}+_risk"
            else:
                range_key = f"{int(frm)}-{int(to_val)}_risk"

            week_key = f"week_{wk}"
            cfg.setdefault(week_key, {})
            cfg[week_key].setdefault('daily_target', {})
            cfg[week_key]['daily_target'].setdefault(day_lower, {})
            day_config = cfg[week_key]['daily_target'][day_lower]
            day_config[range_key] = float(target)
            day_config.setdefault('_range_options', {})[range_key] = {
                'pre-pending_sum': pre_pending,
                'include_missed_days': include_missed,
                'from_balance': float(frm),
                'to_balance': to_val,
            }

        return cfg

    def db_upsert_daily_revenue_target_row(userid, sub_account_id,
                                   week_label, day_label, date_str,
                                   daily_target, status,
                                   profit_allocated, remaining_needed,
                                   daily_target_owed=None,
                                   daily_target_owed_day_balance=None,
                                   daily_target_owed_date=None):
        existing = db_fetch_one(
            """
            SELECT id FROM daily_target_revenue
             WHERE userid = %s
               AND sub_account_id = %s
               AND week = %s
               AND day = %s
               AND date = %s
             LIMIT 1
            """,
            [userid, sub_account_id, week_label, day_label, date_str],
            timeout=10.0, quiet=True,
        )
        if existing and _raw_first_key(existing, 'id'):
            row_id = _coerce_int(_raw_first_key(existing, 'id'))
            ok, _, msg = db_execute(
                """
                UPDATE daily_target_revenue
                   SET daily_target = %s,
                       status = %s,
                       profit_allocated = %s,
                       remaining_needed = %s,
                       daily_target_owed = %s,
                       daily_target_owed_day_balance = %s,
                       daily_target_owed_date = %s
                 WHERE id = %s
                """,
                [daily_target, status, profit_allocated, remaining_needed,
                 daily_target_owed, daily_target_owed_day_balance,
                 daily_target_owed_date, row_id],
                timeout=15.0, quiet=False,
            )
            if not ok:
                print(f"  ❌ Revenue target daily-row UPDATE failed for {userid}/{sub_account_id}/{date_str}: {msg}")
            return ok
        else:
            ok, _, msg = db_execute(
                """
                INSERT INTO daily_target_revenue
                    (userid, sub_account_id, week, day, date,
                     daily_target, status, profit_allocated, remaining_needed,
                     daily_target_owed, daily_target_owed_day_balance,
                     daily_target_owed_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                [userid, sub_account_id, week_label, day_label, date_str,
                 daily_target, status, profit_allocated, remaining_needed,
                 daily_target_owed, daily_target_owed_day_balance,
                 daily_target_owed_date],
                timeout=15.0, quiet=False,
            )
            if not ok:
                print(f"  ❌ Revenue target daily-row INSERT failed for {userid}/{sub_account_id}/{date_str}: {msg}")
            return ok

    # ----------------------------------------------------------------
    # Existing logic helpers (no JSON, all DB-driven)
    # ----------------------------------------------------------------
    def get_mt5_balance():
        try:
            if not mt5.terminal_info():
                print(f"   MT5 not connected")
                return None
            account_info = mt5.account_info()
            if account_info:
                return account_info.balance
            return None
        except Exception as e:
            print(f"   Error getting MT5 balance: {e}")
            return None

    def get_mt5_today_profit_and_latest_trade_date():
        try:
            if not mt5.terminal_info():
                return 0.0, None, 0
            lagos_now_aware = lagos_now()
            lagos_start_aware = lagos_now_aware.replace(hour=0, minute=0, second=0, microsecond=0)
            start_srv, end_srv, srv_offset = lagos_window_to_server_naive(lagos_start_aware, lagos_now_aware)
            deals = mt5.history_deals_get(start_srv, end_srv)
            total = 0.0
            latest_time_lagos = None
            if deals:
                for deal in deals:
                    movement = 0.0
                    for field in ('profit', 'commission', 'swap', 'fee'):
                        try:
                            movement += float(getattr(deal, field, 0.0) or 0.0)
                        except Exception:
                            pass
                    total += movement
                    deal_ts = getattr(deal, 'time', None) or getattr(deal, 'time_msc', None)
                    deal_time_lagos = lagos_from_timestamp(deal_ts)
                    if deal_time_lagos is not None:
                        if latest_time_lagos is None or deal_time_lagos > latest_time_lagos:
                            latest_time_lagos = deal_time_lagos
            return total, latest_time_lagos, srv_offset
        except Exception as e:
            print(f"  ⚠️ Could not get today's profit / trade date: {e}")
            return 0.0, None, 0

    def get_week_number_from_date(start_date, target_date):
        if not start_date or not target_date:
            return 1
        start_day = start_date.date() if isinstance(start_date, datetime) else start_date
        target_day = target_date.date() if isinstance(target_date, datetime) else target_date
        diff_days = (target_day - start_day).days
        if diff_days < 0:
            return 1
        return (diff_days // 7) + 1

    def get_week_config(investor_data, week_number):
        daily_target_config = investor_data.get('daily_target_config', {})
        week_key = f"week_{week_number}"
        week_config = daily_target_config.get(week_key, {})
        if week_config:
            return week_config.get('daily_target', {})
        return None

    def _parse_range_key(range_str):
        if not range_str:
            return None
        try:
            if '+' in range_str:
                lo_str = range_str.split('+', 1)[0]
                return float(lo_str), float('inf')
            if '-' in range_str:
                lo_str, hi_str = range_str.split('-', 1)
                return float(lo_str), float(hi_str)
            return 0.0, float(range_str)
        except (TypeError, ValueError):
            return None

    def _resolve_target_entry_for_balance(day_config, balance):
        risk_pairs = []
        for key, value in day_config.items():
            if '_risk' not in key:
                continue
            clean = key.replace('_risk', '').strip()
            parsed = _parse_range_key(clean)
            if parsed is None:
                continue
            lo, hi = parsed
            try:
                target_val = float(value)
            except (TypeError, ValueError):
                continue
            risk_pairs.append((lo, hi, target_val, key))
        if not risk_pairs:
            return 0.0, None
        risk_pairs.sort(key=lambda item: item[0])
        if balance is None:
            chosen = risk_pairs[0]
        else:
            chosen = next((item for item in risk_pairs if item[0] <= balance <= item[1]), None)
            if chosen is None:
                below = [item for item in risk_pairs if item[0] <= balance]
                chosen = below[-1] if below else risk_pairs[0]
        return chosen[2], chosen[3]

    def _resolve_target_for_balance(day_config, balance):
        return _resolve_target_entry_for_balance(day_config, balance)[0]

    def get_weekly_targets_from_config(investor_data, week_number, balance=None):
        week_config = get_week_config(investor_data, week_number)
        if not week_config:
            return {}
        targets = {}
        for day_name_lower, day_config in week_config.items():
            if isinstance(day_config, dict):
                resolved = _resolve_target_for_balance(day_config, balance)
                targets[day_name_lower] = float(resolved) if resolved is not None else 0.0
            elif isinstance(day_config, (int, float)):
                targets[day_name_lower] = float(day_config)
            else:
                targets[day_name_lower] = 0.0
        return targets

    def _revenue_target_period_start_date(target_date, execution_start_date, period):
        """Period anniversaries are anchored to the exact contract start date."""
        from calendar import monthrange
        target_date = target_date.date() if isinstance(target_date, datetime) else target_date
        start_dt = parse_date_flexible(execution_start_date)
        if start_dt is None:
            return target_date
        start_date = start_dt.date()
        period = str(period or 'daily').lower()
        if target_date <= start_date or period == 'daily':
            return target_date if period == 'daily' else start_date
        if period == 'weekly':
            days = (target_date - start_date).days
            return start_date + timedelta(days=(days // 7) * 7)
        if period == 'monthly':
            months = (target_date.year - start_date.year) * 12 + target_date.month - start_date.month
            def month_anniversary(index):
                absolute = start_date.year * 12 + (start_date.month - 1) + index
                year, month0 = divmod(absolute, 12)
                month = month0 + 1
                day = min(start_date.day, monthrange(year, month)[1])
                return datetime(year, month, day).date()
            candidate = month_anniversary(months)
            if candidate > target_date:
                candidate = month_anniversary(max(0, months - 1))
            return candidate
        if period == 'annually':
            years = target_date.year - start_date.year
            def year_anniversary(index):
                year = start_date.year + index
                day = min(start_date.day, monthrange(year, start_date.month)[1])
                return datetime(year, start_date.month, day).date()
            candidate = year_anniversary(years)
            if candidate > target_date:
                candidate = year_anniversary(max(0, years - 1))
            return candidate
        raise ValueError(f"Unsupported revenue target period: {period}")

    _revenue_balance_history_cache = {}

    def _load_revenue_balance_history(execution_start_date):
        """Load one contract-to-now deal window and group full balance movements by Lagos date."""
        start_dt = parse_date_flexible(execution_start_date)
        if not start_dt:
            return None
        key = start_dt.strftime('%Y-%m-%d')
        if key in _revenue_balance_history_cache:
            return _revenue_balance_history_cache[key]
        try:
            if not mt5.terminal_info():
                return None
            lagos_start = datetime.combine(start_dt.date(), datetime.min.time()).replace(tzinfo=LAGOS_TZ)
            lagos_end = lagos_now()
            start_srv, end_srv, _ = lagos_window_to_server_naive(lagos_start, lagos_end)
            deals = mt5.history_deals_get(start_srv, end_srv)
            if deals is None:
                print(f"  ⚠️ MT5 history unavailable while reconstructing period opening balances: {mt5.last_error()}")
                return None
            grouped = {}
            for deal in deals:
                ts = getattr(deal, 'time', None) or getattr(deal, 'time_msc', None)
                deal_dt = lagos_from_timestamp(ts)
                if deal_dt is None:
                    continue
                movement = 0.0
                for field in ('profit', 'commission', 'swap', 'fee'):
                    try:
                        movement += float(getattr(deal, field, 0.0) or 0.0)
                    except Exception:
                        pass
                grouped.setdefault(deal_dt.date(), 0.0)
                grouped[deal_dt.date()] += movement
            _revenue_balance_history_cache[key] = grouped
            return grouped
        except Exception as exc:
            print(f"  ⚠️ Could not reconstruct period opening balances from MT5 deal history: {exc}")
            return None

    def _revenue_day_open_balance(day_date, execution_start_date,
                                  contract_start_balance, today_start_balance,
                                  current_mt5_balance):
        start_dt = parse_date_flexible(execution_start_date)
        if not start_dt:
            return None
        contract_date = start_dt.date()
        today_date = lagos_now().date()
        if day_date > today_date:
            return None
        if day_date == contract_date:
            return float(contract_start_balance) if contract_start_balance is not None else None
        if day_date == today_date and today_start_balance is not None:
            return float(today_start_balance)
        if current_mt5_balance is None:
            return None
        movements = _load_revenue_balance_history(execution_start_date)
        if movements is None:
            return None
        future_movement = sum(float(v) for d, v in movements.items() if d >= day_date)
        return float(current_mt5_balance) - future_movement

    def _revenue_target_for_date(investor_data, target_date, execution_start_date,
                                 daily_bracket_balance, contract_start_balance,
                                 today_start_balance, current_mt5_balance):
        """Resolve each daily target from its configured period-opening balance."""
        start_dt = parse_date_flexible(execution_start_date)
        target_date = target_date.date() if isinstance(target_date, datetime) else target_date
        week_number = get_week_number_from_date(start_dt, datetime.combine(target_date, datetime.min.time()))
        week_config = get_week_config(investor_data, week_number) or {}
        day_name = target_date.strftime('%A').lower()
        day_config = week_config.get(day_name, {}) if isinstance(week_config, dict) else {}
        period = str((investor_data.get('daily_target_config') or {}).get('revenue_target_period') or 'daily').lower()
        if period == 'daily':
            period_start = target_date
            bracket_balance = daily_bracket_balance
        else:
            period_start = _revenue_target_period_start_date(target_date, execution_start_date, period)
            bracket_balance = _revenue_day_open_balance(
                period_start, execution_start_date, contract_start_balance,
                today_start_balance, current_mt5_balance,
            )
        actual_day_open = _revenue_day_open_balance(
            target_date, execution_start_date, contract_start_balance,
            today_start_balance, current_mt5_balance,
        )
        if isinstance(day_config, dict):
            target, selected_range = _resolve_target_entry_for_balance(day_config, bracket_balance)
            range_options = (day_config.get('_range_options') or {}).get(selected_range, {})
        else:
            target, selected_range, range_options = 0.0, None, {}
        return {
            'target': float(target or 0.0),
            'day_start_balance': actual_day_open,
            'period_start_date': period_start.strftime('%Y-%m-%d'),
            'period_start_balance': bracket_balance,
            'revenue_target_period': period,
            'selected_range': selected_range,
            'pre_pending_sum': bool(range_options.get('pre-pending_sum', False)),
            'include_missed_days': bool(range_options.get('include_missed_days', False)),
        }

    def parse_date_flexible(date_str):
        if not date_str:
            return None
        if isinstance(date_str, str) and date_str.strip().upper() == "NULL":
            return None
        if isinstance(date_str, datetime):
            return date_str
        formats = [
            "%Y-%m-%d", "%B %d, %Y", "%B %d %Y", "%Y/%m/%d",
            "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%m-%d-%Y",
            "%Y-%m-%d %H:%M:%S", "%B %d, %Y %H:%M:%S"
        ]
        for fmt in formats:
            try:
                return datetime.strptime(str(date_str), fmt)
            except:
                continue
        return None

    def get_days_ordered_from_start(start_date, listed_days):
        if not start_date:
            return listed_days
        start_day_name = start_date.strftime("%A").lower()
        standard_days = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday']
        try:
            start_index = standard_days.index(start_day_name)
        except ValueError:
            start_index = 0
        ordered_days = standard_days[start_index:] + standard_days[:start_index]
        listed_days_lower = [day.lower() for day in listed_days]
        result = []
        for day in ordered_days:
            if day in listed_days_lower:
                result.append(day.capitalize())
        return result

    def _get_deals_for_lagos_day(day_date):
        try:
            if not mt5.terminal_info():
                return []
            lagos_start_aware = datetime.combine(
                day_date, datetime.min.time()
            ).replace(tzinfo=LAGOS_TZ)
            lagos_end_aware = lagos_start_aware + timedelta(days=1)
            start_srv, end_srv, _ = lagos_window_to_server_naive(
                lagos_start_aware, lagos_end_aware
            )
            deals = mt5.history_deals_get(start_srv, end_srv)
            if not deals:
                return []
            out = []
            for deal in deals:
                deal_ts = getattr(deal, 'time', None) or getattr(deal, 'time_msc', None)
                deal_time_lagos = lagos_from_timestamp(deal_ts)
                if deal_time_lagos is None:
                    continue
                movement = 0.0
                for field in ('profit', 'commission', 'swap', 'fee'):
                    try:
                        movement += float(getattr(deal, field, 0.0) or 0.0)
                    except Exception:
                        pass
                out.append((deal_time_lagos, movement))
            out.sort(key=lambda x: x[0])
            return out
        except Exception as e:
            print(f"  ⚠️ Could not fetch deals for {day_date}: {e}")
            return []

    def find_progress_halter_day(investor_data, execution_start_date, effective_today,
                                 contract_start_balance, srv_offset,
                                 current_mt5_balance=None, today_start_balance=None):
        result = {
            'anchor_found': False,
            'anchor_date': '',
            'anchor_day_balance': None,
            'anchor_daily_target': 0.0,
            'anchor_net_profit': 0.0,
            'anchor_week': 0,
            'anchor_day_name': '',
            'is_contract_start_day': False,
            'revenue_target_period': (investor_data.get('daily_target_config') or {}).get('revenue_target_period', 'daily'),
        }

        start_date = parse_date_flexible(execution_start_date)
        if not start_date:
            return result
        if effective_today is None:
            effective_today = lagos_naive_now()
        if effective_today.tzinfo is not None:
            effective_today = effective_today.astimezone(LAGOS_TZ).replace(tzinfo=None)

        start_date_only = start_date.date()
        scan_date = start_date_only
        end_date = effective_today.date()
        period = result['revenue_target_period']

        while scan_date <= end_date:
            scan_dt = datetime.combine(scan_date, datetime.min.time())
            wk = get_week_number_from_date(start_date, scan_dt)
            target_info = _revenue_target_for_date(
                investor_data, scan_date, execution_start_date,
                daily_bracket_balance=contract_start_balance,
                contract_start_balance=contract_start_balance,
                today_start_balance=today_start_balance,
                current_mt5_balance=current_mt5_balance,
            )
            day_target = float(target_info['target'])
            is_contract_start_day = (scan_date == start_date_only)

            # Only the active contract-start date may use live balance minus
            # contract anchor. Historical contract-start dates use that day's
            # realized MT5 deals, just like every other historical day.
            if is_contract_start_day and scan_date == end_date and current_mt5_balance is not None and contract_start_balance is not None:
                day_profit = float(current_mt5_balance) - float(contract_start_balance)
                print(f"  🧮 CONTRACT-START DAY NET P/L:")
                print(f"     Contract-aware start balance (C): ${float(contract_start_balance):.2f}")
                print(f"     Current live MT5 balance (M):    ${float(current_mt5_balance):.2f}")
                print(f"     Contract net P/L for day = M - C: ${day_profit:.2f}")
                print(f"     → {'LOSS' if day_profit < 0 else 'PROFIT / FLAT'}")
            else:
                day_deals = _get_deals_for_lagos_day(scan_date)
                day_profit = sum(p for _, p in day_deals)

            if day_profit < 0:
                halter_balance = target_info.get('day_start_balance')
                result.update({
                    'anchor_found': True,
                    'anchor_date': scan_date.strftime('%Y-%m-%d'),
                    'anchor_day_balance': float(halter_balance) if halter_balance is not None else None,
                    'anchor_daily_target': day_target,
                    'anchor_net_profit': float(day_profit),
                    'anchor_week': wk,
                    'anchor_day_name': scan_date.strftime('%A'),
                    'is_contract_start_day': is_contract_start_day,
                    'period_start_date': target_info.get('period_start_date'),
                    'period_start_balance': target_info.get('period_start_balance'),
                })
                print(f"  🎯 PROGRESS-HALTER DAY FOUND: {result['anchor_date']} ({result['anchor_day_name']}, week {wk})")
                if result['anchor_day_balance'] is None:
                    print("     Start-of-day balance: unavailable from MT5 deal history")
                else:
                    print(f"     Start-of-day balance: ${result['anchor_day_balance']:.2f}")
                print(f"     Revenue target period: {period} (period starts {result.get('period_start_date')})")
                print(f"     Daily target: ${result['anchor_daily_target']:.2f}")
                print(f"     Net P/L for day: ${result['anchor_net_profit']:.2f}")
                if is_contract_start_day:
                    print("     ⚠️ Contract-start day: the recorded loss IS what halts progress.")
                return result
            scan_date += timedelta(days=1)

        print(f"  ✅ No progress-halter (loss) day found from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        return result

    def compute_loss_streak_from_halter(anchor_info, srv_offset, loss_streak_chances,
                                        contract_start_balance=None,
                                        current_mt5_balance=None):
        out = {
            'streak_position': 0,
            'remaining_chances': int(loss_streak_chances) if loss_streak_chances else 0,
            'total_losses': 0,
            'total_wins': 0,
            'streak_started_at': None,
            'last_trade_time': None,
            'skipped_previous_contract_deals': 0,
            'reconciled': False,
            'boundary_index': None,
            'first_counted_deal_time': None,
            'streak_skipped_reason': None
        }

        if not anchor_info or not anchor_info.get('anchor_found'):
            return out

        is_contract_start_day = bool(anchor_info.get('is_contract_start_day', False))

        if is_contract_start_day:
            out['streak_skipped_reason'] = (
                "halter is the contract-start day — the recorded loss itself "
                "halts progress; no streak is counted"
            )
            print(f"  📉 Loss-streak: SKIPPED — {out['streak_skipped_reason']}")
            return out

        try:
            if not mt5.terminal_info():
                return out

            anchor_date_str = anchor_info['anchor_date']
            anchor_date = parse_date_flexible(anchor_date_str)
            if not anchor_date:
                return out

            lagos_start_aware = datetime.combine(
                anchor_date.date(), datetime.min.time()
            ).replace(tzinfo=LAGOS_TZ)
            lagos_now_aware = lagos_now()

            start_srv, end_srv, _ = lagos_window_to_server_naive(
                lagos_start_aware, lagos_now_aware
            )

            deals = mt5.history_deals_get(start_srv, end_srv)
            if not deals:
                return out

            trades = []
            for deal in deals:
                if deal.profit is None:
                    continue
                deal_ts = getattr(deal, 'time', None)
                if not deal_ts:
                    deal_ts = getattr(deal, 'time_msc', None)
                deal_time_lagos = lagos_from_timestamp(deal_ts)
                if deal_time_lagos is None:
                    continue
                trades.append((deal_time_lagos, float(deal.profit)))

            trades.sort(key=lambda x: x[0])
            if not trades:
                return out

            out['boundary_index'] = 0
            out['first_counted_deal_time'] = trades[0][0].strftime("%Y-%m-%d %H:%M:%S")

            cumulative = 0.0
            peak = 0.0
            streak = 0
            total_losses = 0
            total_wins = 0
            streak_started_at = None

            for t_time, t_profit in trades:
                cumulative += t_profit
                if t_profit < 0:
                    streak += 1
                    total_losses += 1
                    if streak == 1:
                        streak_started_at = t_time
                elif t_profit > 0:
                    total_wins += 1
                    if cumulative >= peak:
                        streak = 0
                        streak_started_at = None
                        peak = cumulative

            out['streak_position'] = streak
            out['total_losses'] = total_losses
            out['total_wins'] = total_wins
            if loss_streak_chances:
                out['remaining_chances'] = max(0, int(loss_streak_chances) - streak)
            else:
                out['remaining_chances'] = 0

            if streak_started_at:
                out['streak_started_at'] = streak_started_at.strftime("%Y-%m-%d %H:%M:%S")
            out['last_trade_time'] = trades[-1][0].strftime("%Y-%m-%d %H:%M:%S")

            print(f"  📉 Loss-streak from halter: position={streak}, "
                  f"remaining={out['remaining_chances']}, "
                  f"losses={total_losses}, wins={total_wins}")
            return out
        except Exception as e:
            print(f"  ⚠️ Could not compute loss streak from halter: {e}")
            return out

    def calculate_revenue_target_status(investor_data, profit, execution_start_date,
                                      inv_db_id=None, starting_balance_for_today=None,
                                      effective_today=None,
                                      anchor_info=None,
                                      halter_recovered=False,
                                      contract_start_balance=None,
                                      current_mt5_balance=None,
                                      today_start_balance=None):
        result = {
            'weekly_targets': {},
            'today_met': False,
            'alarm_trigger': False,
            'total_owed': 0.0,
            'total_owed_with_shortage': 0.0,
            'today_target_met': False,
            'pre_pending_total': 0.0,
            'pre_pending_enabled': False,
            'total_all_debt': 0.0,
            'daily_target_owed': 0.0,
            'balance_used_for_bracket': starting_balance_for_today,
            'revenue_target_period': 'daily',
            'include_current_day_as_owed': False,
            'effective_today': effective_today.strftime("%Y-%m-%d") if effective_today else None,
            'anchor_found': bool(anchor_info and anchor_info.get('anchor_found')),
            'anchor_date': anchor_info.get('anchor_date') if anchor_info else '',
            'anchor_day_balance': anchor_info.get('anchor_day_balance') if anchor_info else 0.0,
            'anchor_daily_target': anchor_info.get('anchor_daily_target') if anchor_info else 0.0,
            'anchor_net_profit': anchor_info.get('anchor_net_profit') if anchor_info else 0.0,
            'halter_recovered': bool(halter_recovered),
        }

        if not execution_start_date:
            print(f"  ⚠️ No execution start date, cannot calculate daily targets")
            return result

        start_date = parse_date_flexible(execution_start_date)
        if not start_date:
            print(f"  ⚠️ Could not parse execution start date: {execution_start_date}")
            return result

        if effective_today is None:
            effective_today = lagos_naive_now()
        else:
            if effective_today.tzinfo is not None:
                effective_today = effective_today.astimezone(LAGOS_TZ).replace(tzinfo=None)

        current_date = effective_today
        today_str = current_date.strftime("%Y-%m-%d")
        current_week = get_week_number_from_date(start_date, current_date)

        lagos_wall_now = lagos_naive_now()

        daily_target_config = investor_data.get('daily_target_config', {}) or {}
        revenue_target_period = str(daily_target_config.get('revenue_target_period') or 'daily').lower()
        include_current_day_as_owed = bool(daily_target_config.get('include_current_day_as_owed', False))
        result['revenue_target_period'] = revenue_target_period
        result['include_current_day_as_owed'] = include_current_day_as_owed

        print(f"\n  📊 CALCULATING DAILY TARGET STATUS (SECTION 2)")
        print(f"  ──────────────────────────────────────────")
        print(f"  📅 Execution Start: {start_date.strftime('%Y-%m-%d')} ({start_date.strftime('%A')})")
        print(f"  📅 Current Date (Lagos wall clock): {lagos_wall_now.strftime('%Y-%m-%d')} ({lagos_wall_now.strftime('%A')})")
        if effective_today.strftime("%Y-%m-%d") != lagos_wall_now.strftime("%Y-%m-%d"):
            print(f"  📅 EFFECTIVE TODAY (trade-date aligned, Lagos): {effective_today.strftime('%Y-%m-%d')} ({effective_today.strftime('%A')})")
            print(f"     ⚠️ Profit attributed to trade date, NOT calendar date")
        else:
            print(f"  📅 Effective Date (Lagos): {effective_today.strftime('%Y-%m-%d')} ({effective_today.strftime('%A')})")
        print(f"  📆 Current Week: {current_week}")
        print(f"  💰 Total Profit: ${profit:.2f}")
        print(f"  📌 Week starts on: {start_date.strftime('%A')} (execution start day)")
        print(f"  🗓️ Revenue Target Period: {revenue_target_period}")
        print(f"  🔁 Include Missed Days: configured independently for each day")
        print(f"  🔁 Include Current Day As Owed: {'YES ✅' if include_current_day_as_owed else 'NO ⏭️ (today is PENDING unless met)'}")
        if starting_balance_for_today is not None:
            print(f"  📍 Current-day opening balance: ${starting_balance_for_today:.2f}")
        else:
            print("  📍 Current-day opening balance: unavailable")
        if revenue_target_period == 'daily':
            print("  🎯 Daily period: existing daily bracket-balance behaviour is retained.")
        else:
            print("  🎯 Bracket balance: each day's configured period-opening balance (weekly/monthly/annual).")

        if anchor_info and anchor_info.get('anchor_found'):
            if halter_recovered:
                print(f"  ✅ PROGRESS-HALTER RECOVERED: {anchor_info['anchor_date']} ({anchor_info['anchor_day_name']})")
                print(f"     The loss has been bought back — normal revenue-target logic resumed.")
            else:
                print(f"  🛑 PROGRESS-HALTER ACTIVE: {anchor_info['anchor_date']} ({anchor_info['anchor_day_name']})")
                print(f"     Progress HALTED at this day until its loss is recovered.")
                print(f"     Halter net P/L: ${anchor_info['anchor_net_profit']:.2f}")
                print(f"     Halter daily target: ${anchor_info['anchor_daily_target']:.2f}")

        anchor_date_str = anchor_info.get('anchor_date') if anchor_info else None
        halt_active = bool(anchor_info and anchor_info.get('anchor_found')) and not halter_recovered

        for week_num in range(1, current_week + 1):
            week_key = f"week_{week_num}"
            week_targets = get_weekly_targets_from_config(
                investor_data, week_num, balance=starting_balance_for_today
            )
            if not week_targets:
                print(f"  ⚠️ No targets found for {week_key} - skipping")
                continue

            week_start_date = start_date + timedelta(days=(week_num - 1) * 7)
            week_end_date = week_start_date + timedelta(days=6)

            result['weekly_targets'][week_key] = {}

            print(f"\n  📆 {week_key.upper()}: {week_start_date.strftime('%Y-%m-%d')} ({week_start_date.strftime('%A')}) to {week_end_date.strftime('%Y-%m-%d')} ({week_end_date.strftime('%A')})")

            listed_days = list(week_targets.keys())
            ordered_days = get_days_ordered_from_start(week_start_date, listed_days)
            print(f"  📋 Ordered days (starting from {week_start_date.strftime('%A')}): {', '.join(ordered_days)}")

            remaining_profit = profit

            week_pre_pending_enabled = False
            week_pre_pending_total = 0.0

            for day_name_proper in ordered_days:
                day_name_lower = day_name_proper.lower()

                day_index = None
                for offset in range(7):
                    check_date = week_start_date + timedelta(days=offset)
                    if check_date.strftime("%A").lower() == day_name_lower:
                        day_index = offset
                        break

                if day_index is None:
                    continue

                check_date = week_start_date + timedelta(days=day_index)
                day_date_str = check_date.strftime("%Y-%m-%d")
                target_info = _revenue_target_for_date(
                    investor_data, check_date.date(), execution_start_date,
                    daily_bracket_balance=starting_balance_for_today,
                    contract_start_balance=contract_start_balance,
                    today_start_balance=today_start_balance,
                    current_mt5_balance=current_mt5_balance,
                )
                target = float(target_info.get('target', week_targets.get(day_name_lower, 0.0)) or 0.0)
                if day_date_str == today_str:
                    day_open = target_info.get('day_start_balance')
                    period_open = target_info.get('period_start_balance')
                    if revenue_target_period != 'daily':
                        day_label = f"${float(day_open):.2f}" if day_open is not None else 'N/A'
                        period_label = f"${float(period_open):.2f}" if period_open is not None else 'N/A'
                        print(f"  📍 TODAY BALANCE COMPARISON: actual {today_str} opening balance={day_label}; "
                              f"{revenue_target_period} opening balance ({target_info.get('period_start_date')})={period_label}.")
                if (revenue_target_period != 'daily'
                        and day_date_str == target_info.get('period_start_date')):
                    pbal = target_info.get('period_start_balance')
                    if pbal is None:
                        print(f"  ⚠️ {revenue_target_period.upper()} period starts {day_date_str}; opening balance is not available from MT5 history. Target bracket cannot be confirmed yet.")
                    else:
                        print(f"  🎯 {revenue_target_period.upper()} period starts {day_date_str}: opening balance ${float(pbal):.2f}; this balance controls the period's daily target brackets.")
                # Both switches belong to the exact configured day + balance
                # bracket selected for this date, not to the whole programme.
                pre_pending_flag = bool(target_info.get('pre_pending_sum', False))
                include_missed_day = bool(target_info.get('include_missed_days', False))

                is_today = (day_date_str == today_str)
                is_past_day = check_date.date() < current_date.date()
                is_future = check_date.date() > current_date.date()

                is_halter_day = bool(anchor_date_str) and (day_date_str == anchor_date_str)
                is_halted = bool(halt_active and anchor_date_str and (day_date_str >= anchor_date_str))

                if is_past_day:
                    if not include_missed_day:
                        status = 'skipped'
                        profit_allocated = 0.0
                    else:
                        if remaining_profit >= target:
                            if is_halted:
                                status = 'owed'
                                profit_allocated = remaining_profit
                                remaining_profit = 0
                            else:
                                status = 'met'
                                profit_allocated = target
                                remaining_profit -= target
                        else:
                            status = 'owed'
                            profit_allocated = remaining_profit
                            remaining_profit = 0
                elif is_today:
                    if remaining_profit >= target:
                        if is_halted:
                            if include_current_day_as_owed:
                                status = 'owed'
                                profit_allocated = remaining_profit
                                remaining_profit = 0
                            else:
                                status = 'pending'
                                profit_allocated = remaining_profit
                                remaining_profit = 0
                        else:
                            status = 'met'
                            profit_allocated = target
                            remaining_profit -= target
                    else:
                        if include_current_day_as_owed:
                            status = 'owed'
                            profit_allocated = remaining_profit
                            remaining_profit = 0
                        else:
                            status = 'pending'
                            profit_allocated = remaining_profit
                            remaining_profit = 0
                else:
                    status = 'pending'
                    profit_allocated = 0.0

                is_pre_pending_day = False
                if (week_num == current_week
                        and is_future
                        and pre_pending_flag):
                    is_pre_pending_day = True
                    week_pre_pending_enabled = True
                    week_pre_pending_total += target

                result['weekly_targets'][week_key][day_name_proper] = {
                    "date": day_date_str,
                    "daily_target": target,
                    "status": status,
                    "profit_allocated": profit_allocated,
                    "is_listed": True,
                    "remaining_needed": max(0, target - profit_allocated),
                    "day_start_balance": target_info.get('day_start_balance'),
                    "period_start_date": target_info.get('period_start_date'),
                    "period_start_balance": target_info.get('period_start_balance'),
                    "revenue_target_period": revenue_target_period,
                    "include_missed_days": include_missed_day,
                    "pre_pending_sum": pre_pending_flag,
                    "is_pre_pending_day": is_pre_pending_day,
                    "is_halter_day": is_halter_day,
                    "is_halted": is_halted,
                    "at_or_after_anchor": is_halted,
                    "halted": is_halted,
                }

                if is_today:
                    if status == 'met':
                        result['today_met'] = True
                        result['today_target_met'] = True
                        print(f"\n  ✅ TODAY ({day_name_proper}): Target MET! Profit allocation: ${profit_allocated:.2f} >= ${target:.2f}")
                    elif status == 'owed':
                        result['today_met'] = False
                        result['today_target_met'] = False
                        print(f"\n  ⚠️ TODAY ({day_name_proper}): Target OWED (include_current_day_as_owed=True). "
                              f"Profit allocation: ${profit_allocated:.2f} < ${target:.2f}")
                        print(f"     Remaining needed today (now added to owed debt): ${target - profit_allocated:.2f}")
                    else:
                        result['today_met'] = False
                        result['today_target_met'] = False
                        print(f"\n  ⏳ TODAY ({day_name_proper}): Target PENDING (in-progress). Profit allocation: ${profit_allocated:.2f} < ${target:.2f}")
                        print(f"     Remaining needed today: ${target - profit_allocated:.2f}")
                        print(f"     📌 Today is NEVER declared OWED — it is always PENDING until met.")
                        if is_halted and not is_halter_day:
                            print(f"     🛑 Progress still halted by the {anchor_date_str} loss.")

            week_met = sum(1 for d in result['weekly_targets'][week_key].values() if d['status'] == 'met')
            week_owed = sum(1 for d in result['weekly_targets'][week_key].values() if d['status'] == 'owed')
            week_pending = sum(1 for d in result['weekly_targets'][week_key].values() if d['status'] == 'pending')
            week_skipped = sum(1 for d in result['weekly_targets'][week_key].values() if d['status'] == 'skipped')
            week_profit_allocated = sum(d['profit_allocated'] for d in result['weekly_targets'][week_key].values())

            print(f"\n  💰 Week {week_num} allocated: ${week_profit_allocated:.2f}, Remaining profit: ${remaining_profit:.2f}")
            print(f"  📊 Week {week_num} summary: {week_met} MET, {week_owed} OWED, {week_pending} PENDING, {week_skipped} SKIPPED")
            if week_pre_pending_enabled:
                print(f"  🔮 Week {week_num} pre-pending total (forward, tomorrow->end of week): ${week_pre_pending_total:.2f}")

            if week_num == current_week:
                result['pre_pending_total'] = week_pre_pending_total
                result['pre_pending_enabled'] = week_pre_pending_enabled

            profit = remaining_profit

        total_owed = 0.0
        for week_key, week_data in result['weekly_targets'].items():
            for day, day_data in week_data.items():
                if day_data.get('is_listed', True) and day_data['status'] == 'owed':
                    total_owed += (day_data['daily_target'] - day_data['profit_allocated'])

        result['total_owed'] = total_owed
        result['total_owed_with_shortage'] = total_owed

        total_all_debt = total_owed + result['pre_pending_total']
        result['total_all_debt'] = total_all_debt
        result['daily_target_owed'] = total_all_debt

        if result['pre_pending_enabled']:
            print(f"\n  🔮 PRE-PENDING MODE ACTIVE (current week)")
            print(f"  ──────────────────────────────────────")
            print(f"  💰 Owed Debt (backward, PAST days only): ${total_owed:.2f}")
            print(f"  🔮 Pre-Pending Sum (forward, current week): ${result['pre_pending_total']:.2f}")
            print(f"  📊 Total All Debt (daily_target_owed): ${total_all_debt:.2f}")
            print("  📌 Missed-day inclusion was evaluated separately for each configured day.")

            if total_all_debt <= 0:
                result['alarm_trigger'] = True
                print(f"\n  🚨 ALARM TRIGGERED: Owed debt + pre-pending sum fully covered!")
                print(f"  📌 Today's date is IGNORED in pre-pending mode.")
                print(f"  📌 Alarm stays ON until a new week begins.")
            else:
                result['alarm_trigger'] = False
                print(f"\n  🔇 Alarm OFF: Total debt still outstanding (${total_all_debt:.2f} remaining needed)")
        else:
            if result['today_met'] and total_owed == 0:
                result['alarm_trigger'] = True
                print(f"\n  🚨 ALARM TRIGGERED: Today's target met and no owed debts!")
            else:
                result['alarm_trigger'] = False
                if not result['today_met']:
                    print(f"\n  🔇 Alarm OFF: Today's target not met (still PENDING)")
                if total_owed > 0:
                    print(f"\n  🔇 Alarm OFF: Owed debts exist (${total_owed:.2f} remaining needed)")

        print(f"\n  📊 FINAL STATUS:")
        print(f"  ──────────────────")
        print("  Include Missed Days: independently configured per day")
        print(f"  Revenue Target Period: {revenue_target_period}")
        print(f"  Include Current Day As Owed: {'YES ✅' if include_current_day_as_owed else 'NO ⏭️'}")
        print(f"  Today's Target Met: {'YES ✅' if result['today_met'] else 'NO ⏳ (PENDING)'}")
        print(f"  Total Owed (backward, PAST days only): ${total_owed:.2f}")
        print(f"  Pre-Pending Total (forward, current week): ${result['pre_pending_total']:.2f}")
        print(f"  Pre-Pending Enabled: {'YES 🔮' if result['pre_pending_enabled'] else 'NO'}")
        print(f"  Total All Debt: ${total_all_debt:.2f}")
        print(f"  Daily Target Owed (combined): ${result['daily_target_owed']:.2f}")
        print(f"  Alarm Trigger: {'YES 🚨' if result['alarm_trigger'] else 'NO 🔇'}")

        return result

    def confirm_alarm_with_mt5(alarm_result, mt5_balance, starting_balance,
                               today_profit, latest_trade_dt):
        try:
            if not alarm_result.get('alarm_trigger'):
                return False, "alarm_not_requested", 0.0
            if mt5_balance is None:
                return False, "mt5_balance_unavailable", 0.0
            starting_balance = float(starting_balance or 0.0)

            today_target = 0.0
            for week_data in alarm_result.get('weekly_targets', {}).values():
                for day_name, day_data in week_data.items():
                    if day_data.get('date') == alarm_result.get('effective_today'):
                        today_target = float(day_data.get('daily_target', 0.0) or 0.0)
                        break

            if alarm_result.get('pre_pending_enabled'):
                total_debt = float(alarm_result.get('total_all_debt', 0.0) or 0.0)
                required_minimum = starting_balance + total_debt
                mode_label = "PRE-PENDING"
            else:
                total_debt = 0.0
                required_minimum = starting_balance + today_target
                mode_label = "DAILY"

            print(f"\n  🔐 ALARM CONFIRMATION GUARD ({mode_label}):")
            print(f"     Stored recent_highest_balance math has triggered the alarm")
            print(f"     Now verifying against LIVE MT5 balance...")
            print(f"     ├─ Starting balance: ${starting_balance:.2f}")
            if alarm_result.get('pre_pending_enabled'):
                print(f"     ├─ Total debt (owed + pre-pending): ${total_debt:.2f}")
            else:
                print(f"     ├─ Today's target: ${today_target:.2f}")
            print(f"     ├─ Required minimum live MT5 balance: ${required_minimum:.2f}")
            print(f"     └─ Actual LIVE MT5 balance: ${mt5_balance:.2f}")

            if mt5_balance + 0.01 < required_minimum:
                shortfall = required_minimum - mt5_balance
                reason = (
                    f"live MT5 balance ${mt5_balance:.2f} is below required minimum "
                    f"${required_minimum:.2f} (shortfall ${shortfall:.2f})"
                )
                print(f"     🚫 ALARM BLOCKED — {reason}")
                return False, reason, required_minimum

            print(f"     ✅ ALARM CONFIRMED — live balance meets the required minimum")
            return True, "confirmed", required_minimum
        except Exception as e:
            return False, f"confirmation_error: {e}", 0.0

    # ================================================================
    # ENUMERATE INVESTORS (DB-driven)
    # ================================================================
    # ================================================================
    # ENUMERATE INVESTORS (DB-driven) — skipped when jobs= passed in
    # ================================================================
    if jobs is None:
        print("\n" + "─"*80)
        print(" 📁 LOADING INVESTOR JOBS FROM DB (VPS-scoped)")
        print("─"*80)

        local_ip = get_local_ip()
        if not local_ip:
            print(" ⚠️ Could not determine local IP — cannot enumerate investors")
            stats["errors"].append("local IP unknown")
            stats["processing_success"] = False
            return stats

        print(f" │ Local IP: {local_ip}")

        all_jobs = fetch_vps_investor_jobs(local_ip)
        if not all_jobs:
            print(" │ ℹ️ No active investor jobs found for this VPS")
            stats["processing_success"] = True
            return stats

        if inv_db_id:
            jobs = [j for j in all_jobs if str(j.get('investor_id')) == str(inv_db_id)]
            if not jobs:
                print(f" ⚠️ Investor {inv_db_id} not found in any VPS-scoped job")
                stats["errors"].append(f"Investor {inv_db_id} not found")
                stats["processing_success"] = False
                return stats
        else:
            jobs = all_jobs

        print(f" │ Loaded {len(jobs)} investor job(s) from DB")
        print("─"*80)

    investors_to_process = jobs

    print(f"\n 📋 Processing {len(investors_to_process)} investors...")

    has_updates = False

    for job in investors_to_process:
        inv_db_id_db = job.get('investor_id')
        sub_id_db = job.get('sub_account_id')
        programme_id = job.get('programme_id')
        programme_name = job.get('programme_name') or f"programme_{programme_id}"

        inv_db_id_str = str(inv_db_id_db)

        print(f"\n{'─'*80}")
        print(f" 📊 INVESTOR: {inv_db_id_db} (sub {sub_id_db}) | Programme: {programme_name} (id={programme_id})")
        print(f"{'─'*80}")

        identity = fetch_investor_identity(inv_db_id_db)
        username = (identity.get('username')
                    or identity.get('fullname')
                    or f"user{inv_db_id_db}")

        print(f"\n  📌 According to this programme '{programme_name}' (id={programme_id}):")
        print(f"     Investor {username} sub account {sub_id_db} "
              f"is bound to this programme's revenue target configuration.")

        # ============================================================
        # SECTION 1: MT5 BALANCE MANAGEMENT
        # ============================================================
        print(f"\n  🔵 SECTION 1: MT5 BALANCE MANAGEMENT")
        print(f"  ─────────────────────────────────────")

        mt5_balance = get_mt5_balance()
        if mt5_balance is None:
            print(f"  ⚠️ Could not get MT5 balance - Skipping investor {inv_db_id_db}")
            stats["errors"].append(f"Could not get MT5 balance for {inv_db_id_db}")
            stats["investors_skipped"] += 1
            continue

        print(f"  💰 Current MT5 Balance: ${mt5_balance:.2f}")

        hub_row = db_get_harvhub_row(inv_db_id_db, sub_id_db)
        if not hub_row:
            print(f"  ⚠️ No harvhub row found for id={inv_db_id_db} sub={sub_id_db} - skipping")
            stats["errors"].append(f"No harvhub row for {inv_db_id_db}/{sub_id_db}")
            stats["investors_skipped"] += 1
            continue

        broker_balance = _coerce_float(_raw_first_key(hub_row, 'broker_balance'), default=None)
        execution_start_date = _raw_first_key(hub_row, 'execution_start_date')

        existing_recent = _coerce_float(_raw_first_key(hub_row, 'recent_highest_balance'), default=None)
        existing_last_update = _raw_first_key(hub_row, 'recent_highest_balance_last_update')

        print(f"  📁 Recent highest balance last updated for {username} sub account {sub_id_db}: "
              f"{existing_last_update if existing_last_update else 'NEVER'}")
        if existing_recent is not None:
            print(f"  📁 Recent highest balance for {username} sub account {sub_id_db} is: "
                  f"${existing_recent:.2f}")

        today_profit, latest_trade_dt, srv_offset = get_mt5_today_profit_and_latest_trade_date()
        today_start_balance = mt5_balance - today_profit

        wall_clock_now = lagos_naive_now()
        effective_today = wall_clock_now

        if today_profit > 0 and latest_trade_dt is not None:
            latest_trade_lagos_naive = latest_trade_dt.replace(tzinfo=None)
            if latest_trade_lagos_naive.date() < wall_clock_now.date():
                effective_today = latest_trade_lagos_naive
                print(f"  📅 Today's realised profit: ${today_profit:.2f} (Lagos day)")
                print(f"  🎯 Today's START-OF-DAY balance: ${today_start_balance:.2f}")
                print(f"  🕒 Latest contributing trade (Lagos): {latest_trade_dt.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                print(f"  🌐 MT5 server offset detected: {srv_offset:+.2f}h from UTC")
                print(f"  ⚠️ TRADE-DATE MISMATCH (Lagos): Profit was made on "
                      f"{latest_trade_lagos_naive.strftime('%A %Y-%m-%d')}, "
                      f"but processing on {wall_clock_now.strftime('%A %Y-%m-%d')}")
                print(f"  🔁 SHIFTING target resolution to trade date: "
                      f"{effective_today.strftime('%Y-%m-%d')} ({effective_today.strftime('%A')})")
            else:
                print(f"  📅 Today's realised profit: ${today_profit:.2f} (Lagos day)")
                print(f"  🎯 Today's START-OF-DAY balance: ${today_start_balance:.2f}")
                print(f"  🕒 Latest contributing trade (Lagos): {latest_trade_dt.strftime('%Y-%m-%d %H:%M:%S %Z')} (same Lagos day ✅)")
                print(f"  🌐 MT5 server offset detected: {srv_offset:+.2f}h from UTC")
        else:
            print(f"  📅 Today's realised profit: ${today_profit:.2f} (Lagos day)")
            print(f"  🎯 Today's START-OF-DAY balance: ${today_start_balance:.2f}")
            print(f"  🌐 MT5 server offset detected: {srv_offset:+.2f}h from UTC")
            if today_profit > 0:
                print(f"  🕒 Latest contributing trade date: unavailable (using Lagos wall clock)")

        yesterday_str = lagos_yesterday_str()
        today_str = lagos_today_str()

        if execution_start_date:
            exec_start_parsed = parse_date_flexible(execution_start_date)
            if exec_start_parsed:
                exec_start_date_str = exec_start_parsed.strftime("%Y-%m-%d")
                if exec_start_date_str == today_str:
                    print(f"\n  🛡️ START-OF-DAY BALANCE GUARD:")
                    print(f"     Execution start date ({exec_start_date_str}) == today ({today_str})")
                    print(f"     → Falling back to broker_balance for bracket resolution")
                    print(f"     → Using contract-aware balance: ${broker_balance if broker_balance is not None else mt5_balance:.2f}")
                    today_start_balance = broker_balance if broker_balance is not None else mt5_balance
                else:
                    print(f"\n  🛡️ START-OF-DAY BALANCE GUARD:")
                    print(f"     Execution start date ({exec_start_date_str}) != today ({today_str})")
                    print(f"     → Using MT5 start-of-day balance for bracket resolution: ${today_start_balance:.2f}")
        else:
            print(f"\n  🛡️ START-OF-DAY BALANCE GUARD:")
            print(f"     No execution start date found — using MT5 start-of-day balance: ${today_start_balance:.2f}")

        stale_record_detected = False
        if existing_recent is not None and existing_last_update and execution_start_date:
            exec_start_parsed = parse_date_flexible(execution_start_date)
            stored_date_parsed = parse_date_flexible(existing_last_update)
            if exec_start_parsed and stored_date_parsed:
                if stored_date_parsed.date() < exec_start_parsed.date():
                    stale_record_detected = True

        if stale_record_detected:
            print(f"\n  🧹 STALE RECORD GUARD:")
            print(f"     Stored recent_highest_balance_last_update = {existing_last_update}")
            print(f"     Execution start date = {execution_start_date}")
            print(f"     → Stored record is from a PREVIOUS contract (date is behind execution start)")
            print(f"     → Resetting recent_highest_balance recording for the new contract")

            if broker_balance is not None:
                reset_balance = broker_balance
            else:
                reset_balance = mt5_balance

            if mt5_balance < reset_balance:
                print(f"     ⚠️ MT5 balance ${mt5_balance:.2f} < reset anchor ${reset_balance:.2f}")
                print(f"     → Using broker_balance as reset anchor: ${reset_balance:.2f}")
            else:
                reset_balance = mt5_balance
                print(f"     → Using MT5 balance as reset anchor: ${reset_balance:.2f}")

            print(f"     → New date: {yesterday_str} (yesterday, Lagos)")
            print(f"     → New balance: ${reset_balance:.2f}")

            db_write_recent_balance(inv_db_id_db, sub_id_db, round(reset_balance, 2), yesterday_str)

            existing_recent = reset_balance
            existing_last_update = yesterday_str
            stats["stale_record_resets"] += 1
            has_updates = True

            print(f"     ✅ Record reset complete")

        if existing_recent is None:
            print(f"\n  📝 RECENT HIGHEST BALANCE DOES NOT EXIST - INITIALIZING")
            print(f"  ─────────────────────────────────────────────────────")
            print(f"  💰 Current MT5 Balance: ${mt5_balance:.2f}")

            if broker_balance is not None:
                starting_balance = broker_balance
            else:
                starting_balance = mt5_balance

            print(f"  📊 Starting Balance (Broker): ${starting_balance:.2f}")

            if mt5_balance < starting_balance:
                init_balance = starting_balance
                print(f"\n  ⚠️ MT5 Balance (${mt5_balance:.2f}) is less than Starting Balance (${starting_balance:.2f})")
                print(f"  🔧 Using Starting Balance as initial Recent Highest Balance: ${init_balance:.2f}")
            else:
                init_balance = mt5_balance
                print(f"\n  ✅ Using MT5 Balance as initial Recent Highest Balance: ${init_balance:.2f}")

            total_profit = init_balance - starting_balance
            print(f"  📈 Total Profit: ${total_profit:.2f}")

            if not execution_start_date:
                execution_start_date = today_str

            print(f"\n  ✅ INITIALIZED: Recent Highest Balance = ${init_balance:.2f}")
            print(f"  📅 Date set to: {yesterday_str} (YESTERDAY, Lagos)")
            print(f"  🔇 Alarm OFF - Trading allowed")

            recent_highest_alert = {
                'is_triggered': False,
                'investor_id': inv_db_id_db,
                'old_balance': None,
                'new_balance': init_balance,
                'difference': None,
                'timestamp': lagos_now().strftime('%I:%M:%S %p'),
                'action': 'initialized',
                'last_update_date': yesterday_str,
                'today_profit': None,
                'daily_target_risk': None,
                'profit_met_risk_threshold': False,
                'alarm_blocked_reason': None,
                'init_source': 'mt5_balance' if mt5_balance >= starting_balance else 'starting_balance'
            }

            db_write_recent_balance(inv_db_id_db, sub_id_db, round(init_balance, 2), yesterday_str)
            print(f"  💾 Recent highest balance initialised for {username} sub account {sub_id_db}: "
                  f"${init_balance:.2f} on {yesterday_str}")

            stats["investors_updated"] += 1
            stats["investors_checked"] += 1
            stats["field_created"] += 1
            has_updates = True
            continue

        print(f"\n  📊 Checking for new high...")
        print(f"  📊 Stored Recent Highest: ${existing_recent:.2f}")
        print(f"  📊 Current MT5 Balance: ${mt5_balance:.2f}")

        if mt5_balance > existing_recent:
            print(f"\n  📈 NEW HIGHER BALANCE DETECTED!")
            print(f"  ──────────────────────────────")
            print(f"  🆕 New Balance: ${mt5_balance:.2f} (↑${mt5_balance - existing_recent:.2f})")
            print(f"  📅 Date NOT updated yet - waiting for daily target check")

            stats["investors_updated"] += 1
            stats["new_high_recorded_only"] += 1
            has_updates = True

            db_write_recent_balance_balance_only(inv_db_id_db, sub_id_db, round(mt5_balance, 2))
            print(f"  💾 Recent highest balance for {username} sub account {sub_id_db} updated to: "
                  f"${mt5_balance:.2f} (date preserved: {existing_last_update})")

            existing_recent = mt5_balance

            print(f"  ✅ Balance updated to: ${mt5_balance:.2f}")
            print(f"  📅 Date preserved: {existing_last_update}")

        # ============================================================
        # SECTION 2: DAILY TARGET CALCULATIONS
        # ============================================================
        print(f"\n  🟢 SECTION 2: DAILY TARGET CALCULATIONS")
        print(f"  ────────────────────────────────────────")
        print(f"  📊 Using recent_highest_balance (from harvhub): ${existing_recent:.2f}")

        daily_target_config = db_fetch_revenue_target_config_for_programme(programme_id)

        if not daily_target_config:
            print(f"  ⚠️ No programme revenue target config found for programme "
                  f"'{programme_name}' (id={programme_id}) — skipping revenue target calculations")
            stats["investors_skipped"] += 1
            stats["investors_checked"] += 1
            continue

        print(f"  ✅ Loaded programme revenue target config for '{programme_name}' (id={programme_id})")

        starting_balance = broker_balance
        if starting_balance is None:
            starting_balance = existing_recent
            print(f"  ⚠️ No broker_balance on harvhub — falling back to recent_highest_balance as anchor")

        total_profit = existing_recent - starting_balance
        print(f"  💰 Starting Balance: ${starting_balance:.2f}")
        print(f"  💰 Total Profit: ${total_profit:.2f}")

        if not execution_start_date:
            execution_start_date = today_str
            print(f"  ⚠️ No execution start date found - using today (Lagos): {execution_start_date}")

        contract_start_balance = broker_balance
        if contract_start_balance is None:
            contract_start_balance = starting_balance
        if contract_start_balance is None:
            contract_start_balance = today_start_balance

        print(f"  🎯 Contract-aware start balance (for halter + reconciliation): ${contract_start_balance:.2f}")

        print(f"\n  🔍 SCANNING FOR PROGRESS-HALTER DAY (contract start → today)...")

        loss_streak_chances = daily_target_config.get('loss_streak_chances', 0) or 0
        print(f"  📊 loss_streak_chances (from programme_revenue_target): {loss_streak_chances}")

        anchor_info = find_progress_halter_day(
            {'daily_target_config': daily_target_config},
            execution_start_date,
            effective_today,
            contract_start_balance,
            srv_offset,
            current_mt5_balance=mt5_balance,
            today_start_balance=today_start_balance
        )

        halter_recovered = False
        if anchor_info.get('anchor_found'):
            halter_net_profit = float(anchor_info.get('anchor_net_profit', 0.0) or 0.0)
            if contract_start_balance is not None:
                c = float(contract_start_balance)
                live = float(mt5_balance)
                if live + 1e-9 >= c:
                    halter_recovered = True
            if halter_net_profit >= 0:
                halter_recovered = True

            print(f"\n  🧮 HALTER RECOVERY CHECK:")
            print(f"     Contract-aware start balance (C): ${float(contract_start_balance):.2f}")
            print(f"     Live MT5 balance (M):            ${float(mt5_balance):.2f}")
            print(f"     Halter net P/L (loss):           ${halter_net_profit:.2f}")
            if halter_recovered:
                print(f"     → M >= C → loss bought back → RECOVERED")
            else:
                shortfall = float(contract_start_balance) - float(mt5_balance)
                print(f"     → M < C  → loss NOT bought back → STILL HALTED")
                print(f"       Shortfall to recover: ${shortfall:.2f}")

        if anchor_info.get('anchor_found'):
            print(f"\n  🛑 PROGRESS-HALTER DAY: {anchor_info['anchor_date']} "
                  f"({anchor_info['anchor_day_name']}, week {anchor_info['anchor_week']})")
            if anchor_info.get('anchor_day_balance') is None:
                print("     Balance at halter start: N/A (MT5 history unavailable)")
            else:
                print(f"     Balance at halter start: ${anchor_info['anchor_day_balance']:.2f}")
            print(f"     Daily target at halter: ${anchor_info['anchor_daily_target']:.2f}")
            print(f"     Net P/L that day: ${anchor_info['anchor_net_profit']:.2f}")
            print(f"     Contract-start day? {'YES' if anchor_info.get('is_contract_start_day') else 'NO'}")
            if halter_recovered:
                print(f"     ✅ RECOVERED — the loss has been bought back; normal logic resumes.")
            else:
                print(f"     🛑 ACTIVE — progress halted until the loss is recovered.")
        else:
            print(f"\n  ✅ NO PROGRESS-HALTER (loss) DAY FOUND — progress flows freely.")

        loss_streak_info = compute_loss_streak_from_halter(
            anchor_info, srv_offset, loss_streak_chances,
            contract_start_balance=contract_start_balance,
            current_mt5_balance=mt5_balance
        )

        result = calculate_revenue_target_status(
            {'daily_target_config': daily_target_config},
            total_profit,
            execution_start_date,
            inv_db_id_db,
            starting_balance_for_today=today_start_balance,
            effective_today=effective_today,
            anchor_info=anchor_info,
            halter_recovered=halter_recovered,
            contract_start_balance=contract_start_balance,
            current_mt5_balance=mt5_balance,
            today_start_balance=today_start_balance
        )

        weekly_targets = result['weekly_targets']
        today_met = result['today_met']
        alarm_trigger = result['alarm_trigger']
        total_owed = result['total_owed']
        total_owed_with_shortage = result['total_owed_with_shortage']
        today_target_met = result['today_target_met']
        pre_pending_total = result['pre_pending_total']
        pre_pending_enabled = result['pre_pending_enabled']
        total_all_debt = result['total_all_debt']
        daily_target_owed_combined = result['daily_target_owed']

        daily_target_owed_value = float(daily_target_owed_combined or 0.0)

        owed_day_balance_value = None
        owed_date_value = ''
        if anchor_info.get('anchor_found'):
            owed_date_value = anchor_info.get('anchor_date', '') or ''
            anchor_balance = anchor_info.get('anchor_day_balance')
            owed_day_balance_value = float(anchor_balance) if anchor_balance is not None else None
        elif daily_target_owed_value > 0:
            outstanding_days = []
            for week_data in result.get('weekly_targets', {}).values():
                for day_data in week_data.values():
                    if day_data.get('status') == 'owed' and float(day_data.get('remaining_needed', 0.0) or 0.0) > 0:
                        outstanding_days.append(day_data)
            outstanding_days.sort(key=lambda item: item.get('date') or '9999-12-31')
            if outstanding_days:
                first_owed = outstanding_days[0]
                owed_date_value = first_owed.get('date') or ''
                day_balance = first_owed.get('day_start_balance')
                owed_day_balance_value = float(day_balance) if day_balance is not None else None
            else:
                # A pre-pending-only debt has no past owed day; anchor it to the
                # first configured future day that contributes to that sum.
                pre_days = []
                for week_data in result.get('weekly_targets', {}).values():
                    for day_data in week_data.values():
                        if day_data.get('is_pre_pending_day'):
                            pre_days.append(day_data)
                pre_days.sort(key=lambda item: item.get('date') or '9999-12-31')
                if pre_days:
                    owed_date_value = pre_days[0].get('date') or ''
                    day_balance = pre_days[0].get('day_start_balance')
                    owed_day_balance_value = float(day_balance) if day_balance is not None else None
                else:
                    owed_date_value = effective_today.strftime('%Y-%m-%d')
                    owed_day_balance_value = float(today_start_balance) if today_start_balance is not None else None

        print(f"\n  📌 HANDOFF FIELDS (persisted to daily_target_revenue):")
        print(f"     daily_target_owed              : ${daily_target_owed_value:.2f}")
        print(f"     daily_target_owed_day_balance  : ${owed_day_balance_value:.2f}" if owed_day_balance_value is not None else "     daily_target_owed_day_balance  : N/A (history unavailable)")
        print(f"     daily_target_owed_date         : {owed_date_value or 'NULL'}")

        alarm_confirmed = False
        alarm_block_reason = None
        alarm_required_minimum = 0.0

        if alarm_trigger:
            print(f"\n  🔐 MATH SAYS ALARM SHOULD TRIGGER — requesting live confirmation...")
            alarm_confirmed, alarm_block_reason, alarm_required_minimum = confirm_alarm_with_mt5(
                result, mt5_balance, starting_balance, today_profit, latest_trade_dt
            )
            if not alarm_confirmed:
                alarm_trigger = False
                stats["alarm_confirmations_blocked"] += 1
                print(f"  🚫 ALARM DOWNGRADED to OFF due to failed confirmation")

        if weekly_targets:
            print(f"\n  📊 WEEKLY TARGET BREAKDOWN:")
            print(f"  ──────────────────────────")
            for week_key, week_data in weekly_targets.items():
                print(f"\n  📆 {week_key.upper()}:")
                for day, day_data in week_data.items():
                    status = day_data['status']
                    allocated = day_data['profit_allocated']
                    target = day_data['daily_target']
                    date_str = day_data['date']
                    is_listed = day_data.get('is_listed', True)
                    remaining_needed = day_data.get('remaining_needed', 0)
                    pre_pending_flag = day_data.get('pre_pending_sum', False)
                    is_pre_pending_day = day_data.get('is_pre_pending_day', False)

                    is_halter_day = day_data.get('is_halter_day', False)
                    is_halted = day_data.get('is_halted', False)

                    if not is_listed:
                        print(f"    ℹ️ {day} ({date_str}): NOT LISTED, no analysis")
                        continue

                    status_emoji = "✅" if status == 'met' else "⚠️" if status == 'owed' else "⏳"
                    status_text = "MET" if status == 'met' else "OWED" if status == 'owed' else "PENDING"
                    pre_pending_tag = " 🔮[PRE-PENDING]" if pre_pending_flag else ""
                    pre_pending_active = " ⬅️ ADDED TO PRE-PENDING" if is_pre_pending_day else ""
                    halter_tag = " 🛑[PROGRESS-HALTER]" if is_halter_day else ""
                    held_note = ""
                    if is_halted and not is_halter_day and not halter_recovered:
                        held_note = " (held by halter)"

                    print(f"    {status_emoji} {day} ({date_str}){pre_pending_tag}{pre_pending_active}{halter_tag}{held_note}:")
                    print(f"       Target: ${target:.2f}")
                    print(f"       Allocated: ${allocated:.2f}")
                    print(f"       Status: {status_text}")
                    if status == 'owed':
                        print(f"       Remaining Needed: ${remaining_needed:.2f}")
                    if is_pre_pending_day:
                        print(f"       ⬅️ Counted toward pre-pending sum: ${target:.2f}")

        rows_written = 0
        for week_key, week_data in weekly_targets.items():
            for day_name, day_data in week_data.items():
                status = day_data['status']
                target = day_data['daily_target']
                allocated = day_data['profit_allocated']
                remaining = day_data.get('remaining_needed', 0.0)
                date_str = day_data['date']

                ok = db_upsert_daily_revenue_target_row(
                    userid=inv_db_id_db,
                    sub_account_id=sub_id_db,
                    week_label=week_key,
                    day_label=day_name,
                    date_str=date_str,
                    daily_target=round(target, 8),
                    status=status,
                    profit_allocated=round(allocated, 8),
                    remaining_needed=round(remaining, 8),
                    daily_target_owed=round(daily_target_owed_value, 8),
                    daily_target_owed_day_balance=(round(owed_day_balance_value, 8) if owed_day_balance_value is not None else None),
                    daily_target_owed_date=owed_date_value or None,
                )
                if ok:
                    rows_written += 1

        stats["daily_target_rows_written"] += rows_written

        print(f"\n  💾 Daily target met updated to {username} sub account {sub_id_db}: "
              f"{rows_written} row(s) written to daily_target_revenue")
        print(f"     ├─ daily_target_owed             : ${daily_target_owed_value:.2f}")
        print(f"     ├─ daily_target_owed_day_balance : ${owed_day_balance_value:.2f}" if owed_day_balance_value is not None else "     ├─ daily_target_owed_day_balance : N/A (history unavailable)")
        print(f"     └─ daily_target_owed_date        : {owed_date_value or 'NULL'}")

        if alarm_trigger:
            print(f"\n  🚨 ALARM TRIGGERED (CONFIRMED)!")
            print(f"  ───────────────────")
            if pre_pending_enabled:
                print(f"  🔮 Pre-Pending Mode: All debt (owed + current-week pre-pending) covered")
                print(f"     Owed Debt (backward, PAST days only): ${total_owed:.2f}")
                print(f"     Pre-Pending Sum (forward): ${pre_pending_total:.2f}")
                print(f"     Daily Target Owed (combined): ${daily_target_owed_combined:.2f}")
                print(f"     📌 Today's date IGNORED - alarm stays ON until new week")
            else:
                print(f"  ✅ Today's target met: {today_met}")
                print(f"  ✅ No owed debts (PAST days only): {total_owed == 0}")
            print(f"  📅 Updating recent_highest_balance_last_update to TODAY (Lagos): {today_str}")
            print(f"  🚨 TRADING SUSPENDED!")

            recent_highest_alert = {
                'is_triggered': True,
                'investor_id': inv_db_id_db,
                'old_balance': existing_recent,
                'new_balance': existing_recent,
                'difference': 0,
                'timestamp': lagos_now().strftime('%I:%M:%S %p'),
                'action': 'alarm_triggered',
                'last_update_date': today_str,
                'today_profit': today_profit,
                'daily_target_risk': None,
                'profit_met_risk_threshold': True,
                'alarm_blocked_reason': None,
                'pre_pending_enabled': pre_pending_enabled,
                'pre_pending_total': pre_pending_total,
                'total_all_debt': total_all_debt,
                'daily_target_owed': daily_target_owed_combined,
                'effective_today': effective_today.strftime("%Y-%m-%d"),
                'trade_date_aligned': False,
                'timezone': "Africa/Lagos",
                'alarm_confirmed_against_live_mt5': True,
                'alarm_required_minimum': alarm_required_minimum
            }

            db_write_recent_balance(inv_db_id_db, sub_id_db,
                                    round(existing_recent, 2), today_str)

            print(f"  💾 Recent highest balance last updated for {username} sub account "
                  f"{sub_id_db} set to {today_str}")

            stats["new_high_alerts"] += 1
            stats["investors_updated"] += 1

        else:
            print(f"\n  🔇 ALARM OFF - Trading continues")
            if alarm_block_reason:
                print(f"  🚫 ALARM WAS BLOCKED BY CONFIRMATION GUARD:")
                print(f"     Reason: {alarm_block_reason}")
                if alarm_required_minimum > 0:
                    print(f"     Required live MT5 minimum: ${alarm_required_minimum:.2f}")
                    print(f"     Actual live MT5 balance:   ${mt5_balance:.2f}")
            if pre_pending_enabled:
                print(f"  🔮 Pre-Pending Mode Active - Total debt still outstanding")
                print(f"     Owed Debt (backward, PAST days only): ${total_owed:.2f}")
                print(f"     Pre-Pending Sum (forward): ${pre_pending_total:.2f}")
                print(f"     Daily Target Owed (combined): ${daily_target_owed_combined:.2f}")
            else:
                if not today_met:
                    print(f"   Today's target not met (still PENDING)")
                if total_owed > 0:
                    print(f"  ⚠️ Owed debts exist (PAST days only): ${total_owed:.2f} remaining needed")
            if anchor_info.get('anchor_found'):
                if halter_recovered:
                    print(f"  ✅ Progress-halter recovered: {anchor_info['anchor_date']}")
                else:
                    print(f"  🛑 Progress-halter active: {anchor_info['anchor_date']} — progress halted until recovered")
            print(f"  📅 Date NOT updated - kept as: {existing_last_update}")

            recent_highest_alert = {
                'is_triggered': False,
                'investor_id': inv_db_id_db,
                'old_balance': existing_recent,
                'new_balance': existing_recent,
                'difference': 0,
                'timestamp': lagos_now().strftime('%I:%M:%S %p'),
                'action': 'alarm_off',
                'last_update_date': existing_last_update,
                'today_profit': today_profit,
                'daily_target_risk': None,
                'profit_met_risk_threshold': today_met,
                'alarm_blocked_reason': (
                    alarm_block_reason
                    if alarm_block_reason
                    else (
                        f"Total debt outstanding: ${daily_target_owed_combined:.2f}" if pre_pending_enabled
                        else (f"Today's target still PENDING" if not today_met else f"Owed debts (PAST days): ${total_owed:.2f}")
                    )
                ),
                'pre_pending_enabled': pre_pending_enabled,
                'pre_pending_total': pre_pending_total,
                'total_all_debt': total_all_debt,
                'daily_target_owed': daily_target_owed_combined,
                'effective_today': effective_today.strftime("%Y-%m-%d"),
                'trade_date_aligned': False,
                'timezone': "Africa/Lagos"
            }

        stats["investors_checked"] += 1
        stats["investors_updated"] += 1
        has_updates = True

        stats["details"].append({
            "investor_id": inv_db_id_db,
            "sub_account_id": sub_id_db,
            "programme_id": programme_id,
            "action": "alarm_triggered" if alarm_trigger else "alarm_off",
            "recent_highest_balance": existing_recent,
            "starting_balance": starting_balance,
            "today_start_balance": today_start_balance,
            "contract_start_balance": contract_start_balance,
            "total_profit": total_profit,
            "today_target_met": today_met,
            "alarm_triggered": alarm_trigger,
            "alarm_confirmed_against_live_mt5": alarm_confirmed,
            "alarm_blocked_reason": alarm_block_reason,
            "alarm_required_minimum": alarm_required_minimum,
            "total_owed": total_owed,
            "total_owed_with_shortage": total_owed_with_shortage,
            "pre_pending_enabled": pre_pending_enabled,
            "pre_pending_total": pre_pending_total,
            "total_all_debt": total_all_debt,
            "daily_target_owed": daily_target_owed_combined,
            "daily_target_owed_day_balance": owed_day_balance_value,
            "daily_target_owed_date": owed_date_value,
            "include_current_day_as_owed": result.get('include_current_day_as_owed', False),
            "last_update_date": today_str if alarm_trigger else existing_last_update,
            "effective_today": effective_today.strftime("%Y-%m-%d"),
            "latest_trade_date": latest_trade_dt.strftime("%Y-%m-%d %H:%M:%S %Z") if latest_trade_dt else None,
            "trade_date_aligned": False,
            "timezone": "Africa/Lagos",
            "mt5_server_offset_hours": srv_offset,
            "recent_balance_source": "harvhub.recent_highest_balance",
            "daily_target_rows_written": rows_written,
            "anchor_found": bool(anchor_info.get('anchor_found')),
            "anchor_recovered": bool(halter_recovered),
            "anchor_date": anchor_info.get('anchor_date', ''),
            "anchor_day_balance": anchor_info.get('anchor_day_balance', 0.0),
            "anchor_daily_target": anchor_info.get('anchor_daily_target', 0.0),
            "anchor_net_profit": anchor_info.get('anchor_net_profit', 0.0),
            "anchor_is_contract_start_day": bool(anchor_info.get('is_contract_start_day', False)),
            "loss_streak_position": loss_streak_info.get('streak_position', 0),
            "loss_streak_remaining": loss_streak_info.get('remaining_chances', 0),
            "loss_streak_skipped_reason": loss_streak_info.get('skipped_reason'),
            "timestamp": lagos_now().strftime("%Y-%m-%d %H:%M:%S")
        })

    print(f"\n{'='*10} 📊 RECENT BROKER BALANCE SUMMARY (DB) {'='*10}")
    print(f"  Investors checked: {stats['investors_checked']}")
    print(f"  Field created: {stats['field_created']}")
    print(f"  Stale record resets: {stats['stale_record_resets']}")
    print(f"  Alarm confirmations blocked: {stats['alarm_confirmations_blocked']}")
    print(f"  New highs recorded (total): {stats['new_high_recorded_only']}")
    print(f"  New high alerts triggered: {stats['new_high_alerts']}")
    print(f"  Investors updated: {stats['investors_updated']}")
    print(f"  Investors skipped: {stats['investors_skipped']}")
    print(f"  daily_target_revenue rows written: {stats['daily_target_rows_written']}")

    if stats['new_high_alerts'] > 0:
        print(f"\n  🚨 ALARM ACTIVE: {stats['new_high_alerts']} investor(s)")
        for detail in stats['details']:
            if detail.get('alarm_triggered'):
                mode = "PRE-PENDING" if detail.get('pre_pending_enabled') else "DAILY"
                print(f"     • {detail['investor_id']}: Balance: ${detail['recent_highest_balance']:.2f} [{mode}] 🔴")

    if stats['alarm_confirmations_blocked'] > 0:
        print(f"\n  🚫 ALARM CONFIRMATIONS BLOCKED: {stats['alarm_confirmations_blocked']} investor(s)")
        for detail in stats['details']:
            if detail.get('alarm_blocked_reason'):
                print(f"     • {detail['investor_id']}: {detail['alarm_blocked_reason']}")

    if stats['stale_record_resets'] > 0:
        print(f"\n  🧹 STALE RECORD RESETS: {stats['stale_record_resets']} investor(s)")

    if stats['new_high_recorded_only'] > 0:
        print(f"\n  📈 NEW HIGHS RECORDED: {stats['new_high_recorded_only']} investor(s)")
        for detail in stats['details']:
            if detail.get('action') == 'alarm_off' and detail.get('total_owed', 0) == 0:
                print(f"     • {detail['investor_id']}: New high ${detail['recent_highest_balance']:.2f} recorded (alarm OFF) 📈")

    if recent_highest_alert and recent_highest_alert.get('is_triggered'):
        print(f"\n  🚨 ALARM ACTIVE: {recent_highest_alert['investor_id']} - Trading SUSPENDED")

    print(f"{'='*10} 🏁 COMPLETE {'='*10}\n")

    stats["processing_success"] = True
    return stats


# ==============================================================================
#  STAGE RUNNER — isolate any future stage failure
# ==============================================================================
def run_stage(name, fn, **kwargs):
    t0 = time.time()
    print(f"\n[STAGE ▶] {name}")
    try:
        result = fn(**kwargs)
        if not isinstance(result, dict):
            result = {'result': result}
        result.setdefault('processing_success', True)
        result['_stage_name'] = name
        result['_elapsed'] = round(time.time() - t0, 3)
        print(f"[STAGE ✓] {name} ({result['_elapsed']}s)")
        return result
    except Exception as e:
        print(f"[STAGE ✗] {name} raised: {e}")
        traceback.print_exc()
        return {
            '_stage_name': name,
            '_elapsed': round(time.time() - t0, 3),
            'processing_success': False,
            'error': str(e),
        }


def run_stages_sequential(stages, **common_kwargs):
    out = {}
    for name, fn in stages:
        out[name] = run_stage(name, fn, **common_kwargs)
    return out


# ==============================================================================
#  RESTRICTION GATE HELPERS
# ==============================================================================
def _programme_restriction_verdict(timerange_stats, programme_id):
    """
    Extract the restriction verdict for a specific programme from the
    result of restricted_timerange_db(). Returns {} if the programme was not
    classified (treated as "not restricted").
    """
    if not isinstance(timerange_stats, dict):
        return {}
    rmap = timerange_stats.get('restriction_map') or {}
    if not isinstance(rmap, dict):
        return {}
    verdict = rmap.get(programme_id)
    if not isinstance(verdict, dict):
        return {}
    return verdict


def _restriction_skip_banner(inv_db_id, sub_id, programme_id, programme_name,
                             verdict):
    kind = verdict.get('kind') or 'unknown'
    day_name = verdict.get('day_name') or ''
    window_desc = verdict.get('window_desc') or ''

    reason_map = {
        'restricted_date':      'today is a configured restricted date',
        'restricted_day_full':  'today is a fully paused trading day',
        'restricted_day_window':'the current time falls inside a paused window',
    }
    reason = reason_map.get(kind, 'a restriction is active')

    print(f"\n[STAGE ⏭] Investor {inv_db_id} (sub {sub_id}) | "
          f"programme '{programme_name}' (id={programme_id})")
    print(f"           🚫 RESTRICTED — {reason}")
    if day_name:
        print(f"           📅 Day: {day_name}")
    if window_desc:
        print(f"           🕐 Window: {window_desc}")
    print(f"           ⏭️  Skipping recent_highest_balance_revenue_target_db and every "
          f"future stage — no trade activity allowed right now.")


def _revenue_target_alarm_banner(inv_db_id, sub_id, programme_id, programme_name,
                               alert):
    """
    Uniform log banner for the revenue-target alarm short-circuit. Mirrors
    the local file's `recent_highest_hit` path.
    """
    mode = "PRE-PENDING" if alert.get('pre_pending_enabled') else "DAILY"
    owed = alert.get('daily_target_owed')
    pre_pending = alert.get('pre_pending_total')
    total_debt = alert.get('total_all_debt')
    effective_today = alert.get('effective_today')

    print(f"\n[STAGE ⏭] Investor {inv_db_id} (sub {sub_id}) | "
          f"programme '{programme_name}' (id={programme_id})")
    print(f"           🚨 REVENUE-TARGET ALARM ACTIVE [{mode}] — "
          f"trading suspended, all future stages skipped.")
    if effective_today:
        print(f"           📅 Effective trade date: {effective_today}")
    if owed is not None:
        print(f"           💰 Daily target owed (combined): ${float(owed):.2f}")
    if pre_pending is not None:
        print(f"           🔮 Pre-pending total: ${float(pre_pending):.2f}")
    if total_debt is not None:
        print(f"           📊 Total all debt: ${float(total_debt):.2f}")

# ==============================================================================
#  RECOVERY_GENERATOR — DB version (complete, non-grid)
#
#  Configuration ownership:
#    ┌─────────────────────────────────────────────────────────────────────┐
#    │ PROGRAMME CONFIGURATION  (fetched ONCE per programme, shared)        │
#    │   source: accountmanagement_trades_recovery_generator                        │
#    │           accountmanagement_balance_default_risk                     │
#    │           accountmanagement.fixed_risk_reward / minimum_risk_reward  │
#    │           programme_revenue_target.loss_streak_chances               │
#    │           signals_provider_interest.risk_reward  (profit_factor R:R) │
#    ├─────────────────────────────────────────────────────────────────────┤
#    │ INVESTOR STATE  (fetched PER INVESTOR, independent per investor)     │
#    │   source: harvhub (broker_balance = execution-start balance)         │
#    │           daily_target_revenue (owed, day_balance, owed_date)        │
#    │           MT5 (current balance, positions, pending orders)           │
#    │           programme_trades (investor-scoped pending rows)            │
#    └─────────────────────────────────────────────────────────────────────┘
#
#  profit_factor semantics:
#     The R:R is a multiplier that turns RISK into REWARD.
#     The target we must recover is the REWARD. So:
#
#         required_risk   = target_reward / rr
#         required_volume = required_risk / (SL_distance × contract)
#
#     If the SL is reached: volume × SL_distance × contract is lost.
#     If the TP is reached: that amount × rr is gained.
#
#     Rows are PENDING — no event has occurred. Log lines use "risk X
#     → reward Y" (or "risk X" for stoploss_factor). Never "hit".
#
#  stoploss_factor semantics: unchanged — risk = target.
# ==============================================================================
def recovery_generator_system_db(inv_db_id=None, jobs=None):
    global recovery_generator_alert
    from datetime import datetime, timedelta, timezone

    # ------------------------------------------------------------------
    # LAGOS TIMEZONE
    # ------------------------------------------------------------------
    try:
        from zoneinfo import ZoneInfo
        LAGOS_TZ = ZoneInfo("Africa/Lagos")
        _LAGOS_TZ_SOURCE = "zoneinfo"
    except Exception:
        LAGOS_TZ = timezone(timedelta(hours=1))
        _LAGOS_TZ_SOURCE = "fixed_utc+1"

    def lagos_now():
        return datetime.now(LAGOS_TZ)

    def lagos_now_str():
        return lagos_now().strftime("%Y-%m-%d %H:%M:%S")

    recovery_generator_alert = {
        'is_triggered': False,
        'investor_id': inv_db_id if inv_db_id else 'all',
        'programme_id': None,
        'stage': 0,
        'stage_drawdown': 0.0,
        'total_drawdown': 0.0,
        'sequence_next_risk': 0.0,
        'rows_updated': 0,
        'rows_restored': 0,
        'mt5_cancelled': 0,
        'mode': None,
        'factor': None,
        'programme_rr': None,
        'errors': [],
        'timestamp': lagos_now_str(),
    }

    print(f"\n{'='*10} 🎰 RECOVERY GENERATOR SYSTEM (DB) {'='*10}")
    print(f" 🕒 Lagos time now: {lagos_now().strftime('%Y-%m-%d %H:%M:%S %Z')} (source: {_LAGOS_TZ_SOURCE})")
    if inv_db_id:
        print(f" 🎯 Target Investor: {inv_db_id}")

    stats = {
        'investor_id': inv_db_id if inv_db_id else 'all',
        'investors_processed': 0,
        'investors_skipped': 0,
        'programmes_processed': 0,
        'rows_updated': 0,
        'rows_restored': 0,
        'mt5_cancelled': 0,
        'errors': [],
        'processing_success': False,
        'details': [],
    }

    # The stage is invoked immediately after Revenue Target. Respect its alarm
    # before touching trade rows, then preserve the old audit -> sizing -> recovery
    # dependency order inside this stage so no later sizing pass overwrites volumes.
    revenue_alert = recent_highest_alert or {}
    if revenue_alert.get('is_triggered'):
        stats['processing_success'] = True
        stats['skipped'] = True
        stats['skip_reason'] = 'revenue_target_alarm_active'
        print("  🔇 Recovery Generator skipped because Revenue Target alarm is active.")
        return stats

    try:
        stats['authorization_audit'] = check_and_record_unauthorized_actions_db(
            inv_db_id=inv_db_id, jobs=jobs,
        )
        stats['investor_order_sizing'] = calculate_investor_symbols_orders_db(
            inv_db_id=inv_db_id, jobs=jobs,
        )
    except Exception as prerequisite_error:
        stats['errors'].append(f"Recovery prerequisites failed: {prerequisite_error}")
        print(f"  ❌ Recovery Generator prerequisite failure: {prerequisite_error}")
        stats['processing_success'] = False
        return stats

    # ==================================================================
    # SECTION 1 — PROGRAMME CONFIGURATION READERS
    # ==================================================================
    def programme_fetch_recovery_generator_config(programme_id):
        return db_fetch_all(
            """
            SELECT id, developerid, programme_id,
                   from_balance, to_balance,
                   enable_recovery_generator, recovery_generator_type, recovery_generator_factor,
                   recovery_generator_for_position_order_scale,
                   recovery_generator_loss_recovery_adder_percentage,
                   pre_drawdown_assumption, recovery_generator_pre_scaling,
                   recovery_generator_linear_scaling,
                   enable_initial_risk_retention,
                   initial_risk_retention_percentage,
                   recovery_generator_per_stage_drawdown_amount
              FROM accountmanagement_trades_recovery_generator
             WHERE programme_id = %s
             ORDER BY from_balance ASC, id ASC
            """,
            [programme_id], timeout=15.0, quiet=True,
        )

    def programme_fetch_default_risk_rows(programme_id):
        return db_fetch_all(
            """
            SELECT from_balance_range, to_balance_range, risk_amount
              FROM accountmanagement_balance_default_risk
             WHERE programme_id = %s
            """,
            [programme_id], timeout=10.0, quiet=True,
        )

    def programme_fetch_rr_config(programme_id):
        return db_fetch_one(
            """
            SELECT fixed_risk_reward, minimum_risk_reward
              FROM accountmanagement
             WHERE developerid = 0 AND programme_id = %s AND investorid = 0
             LIMIT 1
            """,
            [programme_id], timeout=10.0, quiet=True,
        )

    def programme_fetch_loss_streak_chances(programme_id):
        row = db_fetch_one(
            """
            SELECT loss_streak_chances
              FROM programme_revenue_target
             WHERE programme_id = %s
             LIMIT 1
            """,
            [programme_id], timeout=10.0, quiet=True,
        )
        return _coerce_int(_raw_first_key(row or {}, 'loss_streak_chances')) or 0

    def programme_fetch_signal_provider_risk_reward(programme_id):
        row = db_fetch_one(
            """
            SELECT risk_reward
              FROM signals_provider_interest
             WHERE programme_id = %s
               AND risk_reward IS NOT NULL
               AND risk_reward > 0
             ORDER BY id DESC
             LIMIT 1
            """,
            [programme_id], timeout=10.0, quiet=True,
        )
        if not row:
            return None
        rr = _coerce_float(_raw_first_key(row, 'risk_reward'), default=None)
        if rr is None or rr <= 0:
            return None
        return float(rr)

    # ==================================================================
    # SECTION 2 — INVESTOR-SPECIFIC READERS
    # ==================================================================
    def investor_fetch_harvhub_anchor(investor_id, sub_account_id):
        return db_fetch_one(
            """
            SELECT id, sub_account_id, broker_balance, execution_start_date
              FROM harvhub
             WHERE id = %s AND sub_account_id = %s
             LIMIT 1
            """,
            [investor_id, sub_account_id], timeout=15.0, quiet=True,
        )

    def investor_fetch_daily_target(investor_id, sub_account_id):
        return db_fetch_one(
            """
            SELECT id, userid, sub_account_id, week, day, date,
                   daily_target, status, profit_allocated, remaining_needed,
                   daily_target_owed, daily_target_owed_day_balance,
                   daily_target_owed_date
              FROM daily_target_revenue
             WHERE userid = %s
               AND sub_account_id = %s
               AND daily_target_owed IS NOT NULL
             ORDER BY updated_at DESC, id DESC
             LIMIT 1
            """,
            [investor_id, sub_account_id], timeout=15.0, quiet=True,
        )

    def investor_fetch_pending_trades(investor_id, sub_account_id, programme_id):
        programme_userid = resolve_programme_userid(programme_id)
        if not programme_userid:
            print(f"      [Recovery Generator] missing programme owner userid for programme={programme_id}")
            return []
        hub = investor_fetch_harvhub_anchor(investor_id, sub_account_id) or {}
        execution_start_date = _raw_first_key(hub, 'execution_start_date')
        rows, eligibility = fetch_live_investor_programme_trades(
            programme_id, programme_userid, investor_id, sub_account_id,
            execution_start_date, pending_only=True,
        )
        if eligibility.get('reason'):
            print(f"      [Recovery Generator] live-trade gate closed: {eligibility['reason']}")
        if eligibility.get('skipped_old_contract') or eligibility.get('skipped_missing_trade_time') or eligibility.get('skipped_future_trade_time'):
            print(
                "      [Recovery Generator] excluded rows: "
                f"old_contract={eligibility.get('skipped_old_contract', 0)}, "
                f"missing_trade_time={eligibility.get('skipped_missing_trade_time', 0)}, "
                f"future_trade_time={eligibility.get('skipped_future_trade_time', 0)}"
            )
        return rows

    def update_trade_volume(trade_row_id, new_volume):
        ok, affected, msg = db_execute(
            "UPDATE programme_trades SET volume = %s WHERE id = %s",
            [round(float(new_volume), 8), trade_row_id],
            timeout=15.0, quiet=True,
        )
        return ok and affected >= 0

    def delete_trade_row(trade_row_id):
        ok, affected, msg = db_execute(
            "DELETE FROM programme_trades WHERE id = %s",
            [trade_row_id], timeout=15.0, quiet=True,
        )
        return ok

    def restore_trade_row(investor_id, sub_account_id, programme_id,
                          tradeid, symbol, ordertype, entry, stoploss,
                          takeprofit, volume, timeframe=None,
                          trade_risk_reward=None, trade_time=None,
                          setup_group_id=None):
        programme_userid = resolve_programme_userid(programme_id)
        group_id = _coerce_int(setup_group_id)
        trade_columns = _db_table_columns('programme_trades')
        if not programme_userid or group_id is None or group_id <= 0:
            print(f"      [restore] missing programme owner or canonical setup_group_id for tradeid={tradeid}")
            return False
        if 'setup_group_id' not in trade_columns:
            print("      [restore] programme_trades.setup_group_id is missing; refusing an incorrectly grouped copy")
            return False
        existing = db_fetch_one(
            """
            SELECT id FROM programme_trades
             WHERE userid = %s AND programmeid = %s
               AND investor_id = %s AND investor_sub_account_id = %s
               AND tradeid = %s AND setup_group_id = %s
             LIMIT 1
            """,
            [programme_userid, programme_id, investor_id, sub_account_id, tradeid, group_id],
            timeout=10.0, quiet=True,
        )
        if existing:
            return False
        tf = timeframe or 'M1'
        if 'trade_time' in trade_columns:
            insert_sql = """
                INSERT INTO programme_trades
                    (userid, programmeid, setup_group_id, investor_id, investor_sub_account_id,
                     tradeid, symbol, ordertype, orderstatus, timeframe, trade_time,
                     entry, stoploss, takeprofit, trade_risk_reward, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'pending', %s, %s,
                        %s, %s, %s, %s, %s)
            """
            insert_params = [programme_userid, programme_id, group_id, investor_id, sub_account_id,
                             tradeid, symbol, ordertype, tf, trade_time,
                             entry, stoploss, takeprofit, trade_risk_reward,
                             round(float(volume), 8)]
        else:
            insert_sql = """
                INSERT INTO programme_trades
                    (userid, programmeid, setup_group_id, investor_id, investor_sub_account_id,
                     tradeid, symbol, ordertype, orderstatus, timeframe,
                     entry, stoploss, takeprofit, trade_risk_reward, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'pending', %s,
                        %s, %s, %s, %s, %s)
            """
            insert_params = [programme_userid, programme_id, group_id, investor_id, sub_account_id,
                             tradeid, symbol, ordertype, tf,
                             entry, stoploss, takeprofit, trade_risk_reward,
                             round(float(volume), 8)]
        ok, _, _ = db_execute(insert_sql, insert_params, timeout=15.0, quiet=True)
        return ok

    # ==================================================================
    # SECTION 3 — MT5 READERS (investor-specific)
    # ==================================================================
    def get_effective_current_balance():
        acc = mt5_account_info()
        if acc is None:
            return None
        try:
            return float(acc.balance)
        except Exception:
            return None

    def get_open_positions_total_risk():
        """Return the open-position risk that must be added to pre-drawdown.

        A position is exempt ONLY when its stop loss guarantees at least
        0.01 of positive P/L in the MT5 account currency. Floating profit is
        deliberately not considered: an open position that is currently in
        profit but whose SL still allows a loss remains part of pre-drawdown.

        Positions with no SL (or with insufficient data to value their SL)
        are marked unbounded. The caller must fail closed instead of silently
        excluding them from the recovery calculation.
        """
        positions = mt5_positions_get()
        if not positions:
            return 0.0, []

        LOCKED_PROFIT_EXEMPTION_MIN = 0.01
        total_risk = 0.0
        details = []

        for pos in positions:
            sym = str(getattr(pos, 'symbol', '') or '').strip()
            ticket = _coerce_int(getattr(pos, 'ticket', None))
            volume = _coerce_float(getattr(pos, 'volume', None), default=None)
            entry = _coerce_float(getattr(pos, 'price_open', None), default=None)
            sl = _coerce_float(getattr(pos, 'sl', None), default=0.0) or 0.0
            position_type = _coerce_int(getattr(pos, 'type', None))

            is_buy = position_type == getattr(mt5, 'POSITION_TYPE_BUY', 0)
            is_sell = position_type == getattr(mt5, 'POSITION_TYPE_SELL', 1)
            direction = 'BUY' if is_buy else ('SELL' if is_sell else 'UNKNOWN')

            base_detail = {
                'ticket': ticket,
                'symbol': sym,
                'volume': volume,
                'entry': entry,
                'sl': sl,
                'type': direction,
                'floating_profit': _coerce_float(
                    getattr(pos, 'profit', None), default=0.0
                ) or 0.0,
            }

            # A missing SL does not make the position safe. Its downside is
            # not bounded, so recovery sizing must stop rather than ignore it.
            if sl <= 0:
                details.append({
                    **base_detail,
                    'status': 'unbounded',
                    'risk': None,
                    'reason': 'no stop loss; downside exposure cannot be bounded',
                })
                continue

            if (not sym or volume is None or volume <= 0 or entry is None
                    or entry <= 0 or not (is_buy or is_sell)):
                details.append({
                    **base_detail,
                    'status': 'unbounded',
                    'risk': None,
                    'reason': 'incomplete position data; stop-loss exposure cannot be valued',
                })
                continue

            calc_type = (
                getattr(mt5, 'ORDER_TYPE_BUY', 0)
                if is_buy else getattr(mt5, 'ORDER_TYPE_SELL', 1)
            )
            sl_outcome = mt5_order_calc_profit(
                calc_type, sym, float(volume), float(entry), float(sl)
            )
            calculation_method = 'mt5_order_calc_profit'

            # Prefer MT5's account-currency conversion. If it cannot value
            # the SL leg, use the same contract-size approximation previously
            # used by this helper; if even that is unavailable, fail closed.
            if sl_outcome is None:
                info = mt5_symbol_info(sym)
                contract_size = _coerce_float(
                    getattr(info, 'trade_contract_size', None), default=None
                ) if info is not None else None
                if contract_size is None or contract_size <= 0:
                    details.append({
                        **base_detail,
                        'status': 'unbounded',
                        'risk': None,
                        'reason': 'MT5 could not value SL and contract size is unavailable',
                    })
                    continue

                if is_buy:
                    sl_outcome = (float(sl) - float(entry)) * float(volume) * contract_size
                else:
                    sl_outcome = (float(entry) - float(sl)) * float(volume) * contract_size
                calculation_method = 'contract_size_fallback'

            try:
                sl_outcome = float(sl_outcome)
            except (TypeError, ValueError):
                details.append({
                    **base_detail,
                    'status': 'unbounded',
                    'risk': None,
                    'reason': 'SL outcome was not numeric',
                })
                continue

            # Only a guaranteed stop-loss outcome of +0.01 or more qualifies
            # for exemption. A floating profit alone never qualifies.
            if sl_outcome >= LOCKED_PROFIT_EXEMPTION_MIN - 1e-9:
                details.append({
                    **base_detail,
                    'status': 'locked_profit_exempt',
                    'risk': 0.0,
                    'locked_profit': sl_outcome,
                    'calculation_method': calculation_method,
                    'reason': 'SL guarantees at least +0.01 profit',
                })
                continue

            if sl_outcome < 0:
                # The SL can still close the position at a loss. Include the
                # full entry-to-SL loss even when the position is floating green.
                risk = abs(sl_outcome)
                reason = 'SL still permits a loss; included in pre-drawdown'
            else:
                # At breakeven or with a sub-cent locked gain, the strict
                # +0.01 exemption threshold has not been met. Keep the position
                # represented by a conservative 0.01 minimum exposure amount.
                risk = LOCKED_PROFIT_EXEMPTION_MIN
                reason = 'SL lock is below +0.01; conservative 0.01 floor included'

            total_risk += risk
            details.append({
                **base_detail,
                'status': 'included',
                'risk': risk,
                'sl_outcome': sl_outcome,
                'calculation_method': calculation_method,
                'reason': reason,
            })

        return total_risk, details

    def mt5_pending_orders_for_symbols(symbols):
        out = []
        for sym in symbols:
            orders = mt5_orders_get(symbol=sym) or []
            out.extend(orders)
        return out

    # ==================================================================
    # SECTION 4 — MARGIN / LEVERAGE
    # ==================================================================
    def _get_account_margin_state():
        acc = mt5_account_info()
        if acc is None:
            return None
        return {
            "balance": float(acc.balance),
            "equity": float(acc.equity),
            "margin": float(acc.margin),
            "free_margin": float(acc.margin_free),
            "margin_level": float(acc.margin_level) if acc.margin_level else None,
            "leverage": int(acc.leverage),
            "currency": acc.currency,
        }

    def _calc_order_margin(order_type_str, symbol, volume, price):
        try:
            ot = order_type_str.strip().lower()
            if ot.startswith("buy"):
                mt5_type = 0
            elif ot.startswith("sell"):
                mt5_type = 1
            else:
                mt5_type = 0 if 'buy' in ot else 1
            margin = mt5_order_calc_margin(mt5_type, symbol, float(volume), float(price))
            if margin is None or margin <= 0:
                return None
            return float(margin)
        except Exception:
            return None

    def _shrink_volume_to_fit_margin(order, side, symbol_info, desired_volume,
                                     max_margin_allowed, price):
        if desired_volume is None or desired_volume <= 0:
            return 0.0, 0.0, False
        min_vol = symbol_info.volume_min if symbol_info else 0.01
        step = symbol_info.volume_step if symbol_info and symbol_info.volume_step > 0 else 0.01
        margin = _calc_order_margin(side, order.get("symbol"), desired_volume, price)
        if margin is None:
            return desired_volume, 0.0, False
        if margin <= max_margin_allowed:
            return desired_volume, margin, False
        low = min_vol
        high = desired_volume
        best_vol = 0.0
        best_margin = 0.0
        low_margin = _calc_order_margin(side, order.get("symbol"), low, price)
        if low_margin is None or low_margin > max_margin_allowed:
            return 0.0, 0.0, True
        best_vol = low
        best_margin = low_margin
        for _ in range(40):
            mid = (low + high) / 2.0
            steps = round(mid / step) if step > 0 else 0
            mid = max(min_vol, round(steps * step, 2))
            if mid <= best_vol:
                break
            m = _calc_order_margin(side, order.get("symbol"), mid, price)
            if m is None:
                high = mid
                continue
            if m <= max_margin_allowed:
                best_vol = mid
                best_margin = m
                low = mid
            else:
                high = mid
        steps = round(best_vol / step) if step > 0 else 0
        best_vol = max(min_vol, round(steps * step, 2))
        best_margin = _calc_order_margin(side, order.get("symbol"), best_vol, price) or 0.0
        return best_vol, best_margin, True

    def _validate_single_order_margin(order, side, desired_volume, buffer_pct=10.0):
        acc_state = _get_account_margin_state()
        if acc_state is None:
            return desired_volume, 0.0, 0.0, True, False, "no_account_info"
        symbol = order.get("symbol")
        entry = order.get("entry")
        if not symbol or entry is None:
            return desired_volume, 0.0, 0.0, False, False, "missing_data"
        symbol_info = mt5_symbol_info(symbol)
        if not symbol_info:
            return desired_volume, 0.0, 0.0, False, False, "no_symbol_info"
        free_margin = acc_state["free_margin"]
        allowed_margin = free_margin * (1.0 - buffer_pct / 100.0)
        if allowed_margin <= 0:
            return 0.0, 0.0, free_margin, False, True, "no_free_margin"
        margin = _calc_order_margin(side, symbol, desired_volume, entry)
        if margin is None:
            return desired_volume, 0.0, free_margin, False, False, "margin_calc_failed"
        if margin <= allowed_margin:
            return desired_volume, margin, free_margin, True, False, "fits"
        final_vol, final_margin, shrunk = _shrink_volume_to_fit_margin(
            order, side, symbol_info, desired_volume, allowed_margin, entry
        )
        if final_vol <= 0:
            return 0.0, 0.0, free_margin, False, True, "cannot_fit_min_volume"
        return final_vol, final_margin, free_margin, True, True, "shrunk_to_fit"

    # ==================================================================
    # SECTION 5 — SEQUENCE / RISK MATHS
    # ==================================================================
    def parse_percent(value):
        if value is None:
            return 0.0
        s = str(value).strip().replace('%', '')
        try:
            return float(s)
        except (TypeError, ValueError):
            return 0.0

    def resolve_default_risk_from_rows(rows, balance, fallback):
        if not rows:
            return float(fallback), 'fallback_default_minimum_risk'
        best = None
        for r in rows:
            try:
                lo = float(_coerce_float(_raw_first_key(r, 'from_balance_range'), default=0.0) or 0.0)
                hi = float(_coerce_float(_raw_first_key(r, 'to_balance_range'), default=0.0) or 0.0)
                amt = float(_coerce_float(_raw_first_key(r, 'risk_amount'), default=0.0) or 0.0)
            except Exception:
                continue
            if lo <= balance <= hi:
                return amt, f"range:{lo}-{hi}"
            if best is None:
                best = (lo, hi, amt)
        if best:
            return best[2], 'fallback_nearest'
        return float(fallback), 'fallback_default_minimum_risk'

    def resolve_rr_from_cfg(rr_cfg):
        am = rr_cfg or {}
        fixed = _coerce_float(_raw_first_key(am, 'fixed_risk_reward'), default=None)
        minimum = _coerce_float(_raw_first_key(am, 'minimum_risk_reward'), default=None)
        if fixed is not None and fixed > 0:
            if minimum is None or fixed >= minimum:
                return 1.0, float(fixed), f"fixed_risk_reward = 1:{fixed}"
        if minimum is not None and minimum > 0:
            return 1.0, float(minimum), f"minimum_risk_reward = 1:{minimum}"
        return None, None, None

    def build_target_sum_sequence(target_balance, target_owed, reward_ratio,
                                  max_count_hint=None):
        if target_balance <= 0 or target_owed <= 0:
            return [], {'count': 0, 'within_chances': 0, 'excess_count': 0,
                        'excess_sequence': [], 'natural_sum': 0.0,
                        'capped': False, 'target_owed': target_owed,
                        'target_balance': target_balance}
        reward_ratio = max(1e-9, float(reward_ratio))
        sequence = []
        S = 0.0
        safety_limit = 10000
        while S < target_balance and len(sequence) < safety_limit:
            r = round((S + target_owed) / reward_ratio, 2)
            if r <= 0:
                r = 0.01
            if S + r >= target_balance:
                remainder = round(target_balance - S, 2)
                if remainder > 0:
                    sequence.append(remainder)
                    S += remainder
                break
            sequence.append(r)
            S += r
        sequence = [round(v, 2) for v in sequence]
        residual_cents = int(round((target_balance - sum(sequence)) * 100))
        if residual_cents != 0 and sequence:
            sequence[-1] = round(sequence[-1] + residual_cents * 0.01, 2)
            if sequence[-1] <= 0:
                sequence[-1] = 0.01
        natural_sum = round(sum(sequence), 2)
        max_count_hint = max_count_hint if max_count_hint and max_count_hint > 0 else len(sequence)
        within = min(len(sequence), max_count_hint)
        excess_sequence = sequence[max_count_hint:] if len(sequence) > max_count_hint else []
        return sequence, {
            'count': len(sequence),
            'within_chances': within,
            'excess_count': len(excess_sequence),
            'excess_sequence': excess_sequence,
            'natural_sum': natural_sum,
            'capped': natural_sum >= target_balance - 0.01,
            'target_owed': target_owed,
            'target_balance': target_balance,
        }

    def stage_max_risk_for_balance(config_rows, balance):
        if not config_rows:
            return 100.0
        for r in config_rows:
            lo = _coerce_float(_raw_first_key(r, 'from_balance'), default=None)
            hi = _coerce_float(_raw_first_key(r, 'to_balance'), default=None)
            amt = _coerce_float(_raw_first_key(r, 'recovery_generator_per_stage_drawdown_amount'),
                                default=None)
            if lo is None or amt is None:
                continue
            if hi is None:
                if balance >= lo:
                    return float(amt)
                continue
            if lo <= balance <= hi:
                return float(amt)
        last_amt = _coerce_float(
            _raw_first_key(config_rows[-1], 'recovery_generator_per_stage_drawdown_amount'),
            default=100.0,
        )
        return float(last_amt or 100.0)

    # ==================================================================
    # SECTION 6 — SIZE HELPERS
    # ==================================================================
    def _risk_per_lot(order, symbol_info):
        entry = _coerce_float(order.get('entry'), default=None)
        stop  = _coerce_float(order.get('stoploss'), default=None)
        if entry is None or stop is None:
            return 0.0
        sl_distance = abs(entry - stop)
        contract_size = float(getattr(symbol_info, 'trade_contract_size', 1.0))
        return sl_distance * contract_size

    def _size_volume_from_risk_target(order, symbol_info, target_risk_dollars):
        risk_per_lot = _risk_per_lot(order, symbol_info)
        if risk_per_lot <= 0:
            return 0.0, 0.0, 0.0
        vol = target_risk_dollars / risk_per_lot
        step = float(getattr(symbol_info, 'volume_step', 0.01)) or 0.01
        min_vol = float(getattr(symbol_info, 'volume_min', 0.01)) or 0.01
        steps = round(vol / step) if step > 0 else 0
        vol = max(min_vol, round(steps * step, 2))
        actual_risk = vol * risk_per_lot
        return vol, actual_risk, risk_per_lot

    def _size_volume_from_tp_target(order, symbol_info, target_reward_dollars, programme_rr):
        """
        profit_factor: size so that volume × SL_distance × contract × rr
        equals the target reward. Equivalent to:
            required_risk   = target_reward / rr
            required_volume = required_risk / risk_per_lot
        Returns (volume, reward_if_tp, risk_if_sl).
        """
        if programme_rr is None or programme_rr <= 0:
            return 0.0, 0.0, 0.0
        risk_per_lot = _risk_per_lot(order, symbol_info)
        if risk_per_lot <= 0:
            return 0.0, 0.0, 0.0

        required_risk = target_reward_dollars / float(programme_rr)
        vol = required_risk / risk_per_lot

        step = float(getattr(symbol_info, 'volume_step', 0.01)) or 0.01
        min_vol = float(getattr(symbol_info, 'volume_min', 0.01)) or 0.01
        steps = round(vol / step) if step > 0 else 0
        vol = max(min_vol, round(steps * step, 2))

        risk_if_sl = vol * risk_per_lot
        reward_if_tp = risk_if_sl * float(programme_rr)
        return vol, reward_if_tp, risk_if_sl

    def _reward_per_lot_from_programme_rr(order, symbol_info, programme_rr):
        if programme_rr is None or programme_rr <= 0:
            return 0.0
        risk_per_lot = _risk_per_lot(order, symbol_info)
        if risk_per_lot <= 0:
            return 0.0
        return risk_per_lot * float(programme_rr)

    def get_direction_from_order(order):
        ot = (order.get('ordertype') or '').lower()
        if 'sell-limit' in ot or 'buy-stop' in ot:
            return "STEPUP"
        if 'buy-limit' in ot or 'sell-stop' in ot:
            return "STEPDOWN"
        return "UNKNOWN"

    def group_rows_by_linear(rows):
        stepup_linear = []
        stepdown_linear = []
        for idx, order in enumerate(rows):
            ot = (order.get('ordertype') or '').lower()
            entry = order.get('entry')
            if entry is None:
                continue
            info = {'order': order, 'index': idx}
            if 'sell-limit' in ot or 'buy-stop' in ot:
                stepup_linear.append(info)
            elif 'buy-limit' in ot or 'sell-stop' in ot:
                stepdown_linear.append(info)
        stepup_linear.sort(key=lambda x: _coerce_float(x['order'].get('entry'), default=0) or 0)
        stepdown_linear.sort(key=lambda x: _coerce_float(x['order'].get('entry'), default=0) or 0, reverse=True)
        return stepup_linear, stepdown_linear

    # ==================================================================
    # SECTION 7 — PER-INVESTOR PIPELINE
    # ==================================================================
    def process_investor(investor_id, sub_account_id, programme_id,
                         programme_name, programme_cfg):
        detail = {
            'investor_id': investor_id,
            'sub_account_id': sub_account_id,
            'programme_id': programme_id,
            'programme_name': programme_name,
            'mt5_balance': None,
            'execution_start_balance': None,
            'daily_target_owed': 0.0,
            'daily_target_owed_day_balance': None,
            'daily_target_owed_date': None,
            'drawdown': 0.0,
            'stage': 0,
            'stage_drawdown': 0.0,
            'sequence_next_risk': 0.0,
            'final_target_risk': 0.0,
            'per_symbol_target': 0.0,
            'programme_rr': None,
            'rows_seen': 0,
            'rows_updated': 0,
            'rows_deleted': 0,
            'rows_restored': 0,
            'mt5_cancelled': 0,
            'mode': None,
            'factor': None,
            'error': None,
        }

        enable_recovery_generator = programme_cfg['enable_recovery_generator']
        recovery_generator_type = programme_cfg['recovery_generator_type']
        recovery_generator_factor = programme_cfg['recovery_generator_factor']
        recovery_adder_pct = programme_cfg['recovery_adder_pct']
        pre_drawdown_assumption = programme_cfg['pre_drawdown_assumption']
        recovery_generator_pre_scaling = programme_cfg['recovery_generator_pre_scaling']
        linear_scaling = programme_cfg['linear_scaling']
        enable_retention = programme_cfg['enable_retention']
        retention_pct = programme_cfg['retention_pct']
        config_rows = programme_cfg['config_rows']
        default_risk_rows = programme_cfg['default_risk_rows']
        rr_cfg = programme_cfg['rr_cfg']
        loss_streak_chances = programme_cfg['loss_streak_chances']
        programme_rr = programme_cfg.get('programme_rr')

        detail['mode'] = recovery_generator_type
        detail['factor'] = recovery_generator_factor
        detail['programme_rr'] = programme_rr

        if not enable_recovery_generator:
            print(f"\n  ⏭️ Investor {investor_id} | programme '{programme_name}' "
                  f"(id={programme_id}) — Recovery Generator DISABLED")
            detail['error'] = 'recovery_generator_disabled'
            return detail

        if recovery_generator_factor == 'profit_factor' and (programme_rr is None or programme_rr <= 0):
            print(f"\n  ⚠️ Investor {investor_id} | programme '{programme_name}' "
                  f"(id={programme_id}) — profit_factor set but no "
                  f"signals_provider_interest.risk_reward available; skipping")
            detail['error'] = 'programme_rr_missing'
            return detail

        print(f"\n{'─'*72}")
        print(f" 🎰 Investor {investor_id} (sub {sub_account_id}) | "
              f"programme '{programme_name}' (id={programme_id})")
        print(f"{'─'*72}")
        print(f"  PROGRAMME CONFIGURATION (id={programme_id}):")
        print(f"  │ Mode              : {recovery_generator_type}")
        print(f"  │ Factor            : {recovery_generator_factor}")
        if recovery_generator_factor == 'profit_factor':
            print(f"  │ Programme R:R     : 1:{programme_rr}  "
                  f"(risk → reward multiplier; the target is a reward)")
        print(f"  │ Pre-drawdown      : {'ON' if pre_drawdown_assumption else 'OFF'}")
        print(f"  │ Pre-scaling       : {'ON' if recovery_generator_pre_scaling else 'OFF'}")
        print(f"  │ Linear scaling    : {'ON' if linear_scaling else 'OFF'}")
        print(f"  │ Retention         : {'ON' if enable_retention else 'OFF'} ({retention_pct}%)")
        print(f"  │ Recovery adder    : {recovery_adder_pct}%")
        print(f"  │ Loss streak chance: {loss_streak_chances}")
        print(f"  INVESTOR STATE (sub {sub_account_id}):")

        effective_balance = get_effective_current_balance()
        if effective_balance is None:
            detail['error'] = 'mt5_balance_unavailable'
            print(f"  ✗ Failed to get MT5 balance")
            return detail
        current_balance = effective_balance
        detail['mt5_balance'] = current_balance
        print(f"  │ Current MT5 balance: ${current_balance:.2f}")

        hub = investor_fetch_harvhub_anchor(investor_id, sub_account_id) or {}
        broker_balance = _coerce_float(_raw_first_key(hub, 'broker_balance'), default=None)
        if broker_balance is None:
            detail['error'] = 'broker_balance_missing'
            print(f"  ⚠️ harvhub.broker_balance is NULL — skipping investor")
            return detail
        detail['execution_start_balance'] = broker_balance
        print(f"  │ Execution-start balance: ${broker_balance:.2f}")

        dtr = investor_fetch_daily_target(investor_id, sub_account_id) or {}
        daily_target_owed = _coerce_float(_raw_first_key(dtr, 'daily_target_owed'), default=0.0) or 0.0
        owed_day_balance = _coerce_float(
            _raw_first_key(dtr, 'daily_target_owed_day_balance'), default=None)
        owed_date = _raw_first_key(dtr, 'daily_target_owed_date')
        detail['daily_target_owed'] = daily_target_owed
        detail['daily_target_owed_day_balance'] = owed_day_balance
        detail['daily_target_owed_date'] = owed_date
        owed_balance_label = f"${owed_day_balance:.2f}" if owed_day_balance is not None else 'N/A'
        print(f"  │ Revenue target owed : ${daily_target_owed:.2f} "
              f"(owed-day opening balance {owed_balance_label}, date {owed_date or '—'})")

        default_minimum_risk = 2.0
        default_risk_value, default_risk_src = resolve_default_risk_from_rows(
            default_risk_rows, current_balance, default_minimum_risk)
        print(f"  │ Default risk floor: ${default_risk_value:.2f} ({default_risk_src})")

        base_drawdown = max(0.0, float(broker_balance) - float(current_balance))
        total_drawdown = base_drawdown + daily_target_owed
        if pre_drawdown_assumption:
            print(f"\n  🔄 PRE-DRAWDOWN ASSUMPTION ENABLED:")
            print(f"  │ Locked-profit exemption: SL must guarantee at least +$0.01")
            print(f"  │ Floating profit is NOT treated as locked profit")

            positions_risk, positions_details = get_open_positions_total_risk()
            locked_positions = [
                p for p in positions_details
                if p.get('status') == 'locked_profit_exempt'
            ]
            included_positions = [
                p for p in positions_details
                if p.get('status') == 'included'
            ]
            unbounded_positions = [
                p for p in positions_details
                if p.get('status') == 'unbounded'
            ]

            for pos in locked_positions:
                print(
                    f"  │ ⏭️ {pos['symbol']} {pos['type']} ticket #{pos['ticket']}: "
                    f"SL locks +${pos['locked_profit']:.2f} "
                    f"(>= $0.01) — excluded from pre-drawdown"
                )

            for pos in included_positions:
                print(
                    f"  │ ➕ {pos['symbol']} {pos['type']} ticket #{pos['ticket']}: "
                    f"${pos['risk']:.2f} risk included "
                    f"(floating P/L ${pos['floating_profit']:+.2f}; {pos['reason']})"
                )

            if unbounded_positions:
                print(f"  │ 🚫 Cannot safely calculate pre-drawdown: "
                      f"{len(unbounded_positions)} open position(s) have unbounded/unknown SL risk")
                for pos in unbounded_positions:
                    print(
                        f"  │   ├─ {pos.get('symbol') or 'Unknown symbol'} "
                        f"{pos.get('type', 'UNKNOWN')} ticket #{pos.get('ticket')}: "
                        f"{pos.get('reason')}"
                    )
                print(f"  │ Recovery Generator sizing is stopped for this investor; "
                      f"unprotected positions cannot be silently excluded.")
                detail['error'] = 'pre_drawdown_position_risk_unbounded'
                detail['pre_drawdown_unbounded_positions'] = len(unbounded_positions)
                return detail

            if included_positions:
                print(f"  │ Included positions: {len(included_positions)}")
            if locked_positions:
                print(f"  │ Excluded profit-locked positions: {len(locked_positions)}")

            if positions_risk > 0:
                print(f"  │ Total open positions risk added: ${positions_risk:.2f}")
                print(f"  │ ${total_drawdown:.2f} + ${positions_risk:.2f} = "
                      f"${total_drawdown + positions_risk:.2f}")
                total_drawdown += positions_risk
            elif not locked_positions:
                print(f"  │ No positive stop-loss exposure found")
            else:
                print(f"  │ All open positions qualify for the locked-profit exemption")
        else:
            print(f"  │ Pre-drawdown assumption DISABLED")
        detail['drawdown'] = total_drawdown
        print(f"  │ Base drawdown    : ${base_drawdown:.2f}")
        print(f"  │ Total drawdown   : ${total_drawdown:.2f}")

        stage_max_risk = stage_max_risk_for_balance(config_rows, current_balance)
        print(f"  │ Stage max risk (per round): ${stage_max_risk:.2f}")
        if total_drawdown > 0 and stage_max_risk > 0:
            current_stage = int(total_drawdown // stage_max_risk) + 1
            current_stage_drawdown = total_drawdown % stage_max_risk
            if current_stage_drawdown == 0 and current_stage > 1:
                current_stage_drawdown = default_risk_value
                print(f"  │ EXACT STAGE COMPLETION - using floor risk: ${default_risk_value:.2f}")
            print(f"  │ Current stage: {current_stage}")
            print(f"  │ Stage drawdown: ${current_stage_drawdown:.2f}")
            print(f"  │ Total drawdown across all stages: ${total_drawdown:.2f}")
        else:
            current_stage = 1
            current_stage_drawdown = 0.0
            print(f"  │ No drawdown to recover")
        detail['stage'] = current_stage
        detail['stage_drawdown'] = current_stage_drawdown

        sequence_risk = None

        if recovery_generator_type == 'loss_streak':
            print(f"\n  📊 LOSS_STREAK TARGET CALCULATION")
            if daily_target_owed > 0 and (owed_day_balance is None or not owed_date):
                detail['error'] = 'revenue_target_owed_anchor_missing'
                print("  │ ⚠️ Revenue Target has outstanding debt but its actual owed-day opening balance/date is unavailable.")
                print("  │ Recovery sizing is skipped rather than treating the owed dollar amount as an account balance.")
                return detail
            if daily_target_owed <= 0:
                print(f"  │ No daily_target_owed — falling back to default risk floor")
                sequence_risk = default_risk_value
            else:
                risk_ratio, reward_ratio, rr_src = resolve_rr_from_cfg(rr_cfg)
                if risk_ratio is None or reward_ratio is None:
                    print(f"  │ ⚠️ R:R unavailable — falling back to default risk")
                    sequence_risk = default_risk_value
                else:
                    target_balance = float(owed_day_balance)
                    target_owed = daily_target_owed

                    sequence, meta = build_target_sum_sequence(
                        target_balance=target_balance,
                        target_owed=target_owed,
                        reward_ratio=reward_ratio,
                        max_count_hint=loss_streak_chances or None,
                    )
                    if not sequence:
                        print(f"  │ ⚠️ Empty sequence — falling back to default risk")
                        sequence_risk = default_risk_value
                    else:
                        rr_display = reward_ratio / risk_ratio if risk_ratio > 0 else reward_ratio
                        print(f"\n{'='*60}")
                        print(f"  🎰 LOSS STREAK PURE SEQUENCE")
                        print(f"{'='*60}")
                        print(f"  │ Daily target owed   : ${target_owed:.2f}")
                        print(f"  │ Daily target balance: ${target_balance:.2f}")
                        print(f"  │ Loss streak chances : {loss_streak_chances}")
                        print(f"  │ R:R                 : 1:{rr_display:.0f} ({rr_src})")
                        print(f"  │ Total risks needed  : {meta['count']}")
                        print(f"  │ Total (sum)         : ${round(sum(sequence), 2):.2f}")
                        print(f"  {'─'*58}")
                        chunk_size = 5
                        print(f"  📋 Risks sequence:")
                        for i in range(0, len(sequence), chunk_size):
                            chunk = sequence[i:i + chunk_size]
                            print("     " + ", ".join(f"${v:.2f}" for v in chunk))
                        if meta["excess_count"] > 0:
                            print(f"  {'─'*58}")
                            print(f"  📈 EXCESS sequence ({meta['excess_count']} risk(s)):")
                            for i in range(0, len(meta["excess_sequence"]), chunk_size):
                                chunk = meta["excess_sequence"][i:i + chunk_size]
                                print("     " + ", ".join(f"${v:.2f}" for v in chunk))
                        print(f"  {'─'*58}")

                        base_drawdown_loss = max(0.0, round(total_drawdown - daily_target_owed, 2))
                        cumulative = 0.0
                        loss_prefix = []
                        approx_position = 0
                        for i, r in enumerate(sequence):
                            cumulative = round(cumulative + r, 2)
                            loss_prefix.append(r)
                            approx_position = i + 1
                            if cumulative + 1e-9 >= base_drawdown_loss:
                                break
                        if base_drawdown_loss <= 0:
                            loss_prefix = []
                            approx_position = 0
                            cumulative = 0.0

                        next_index = approx_position
                        if next_index >= len(sequence):
                            next_index = len(sequence) - 1

                        positions_exist = bool(mt5_positions_get())
                        advanced = False
                        if positions_exist and pre_drawdown_assumption:
                            next_index = min(next_index + 1, len(sequence) - 1)
                            advanced = True

                        sequence_risk = float(sequence[next_index])
                        remaining_chances = max(0, len(sequence) - (next_index + 1))

                        print(f"\n  🎯 DRAWDOWN / NEXT RISK ANALYSIS")
                        print(f"  │ Base loss to recover   : ${base_drawdown_loss:.2f}")
                        print(f"  │ Approx current drawdown: ${cumulative:.2f} "
                              f"(≈ position {approx_position}, ceil-fit)")
                        print(f"  │ Position exists        : {'YES' if positions_exist else 'NO'}")
                        print(f"  │ Pre-drawdown assumption: {'ON' if pre_drawdown_assumption else 'OFF'}")
                        if advanced:
                            print(f"  │ → advanced 1 step")
                        print(f"  │ NEXT RISK to use       : ${sequence_risk:.2f}")
                        print(f"  │   ├─ Position in sequence: #{next_index + 1} of {len(sequence)}")
                        print(f"  │   ├─ Recovers previous  : ${cumulative:.2f}")
                        print(f"  │   ├─ Plus daily target  : ${target_owed:.2f}")
                        print(f"  │   └─ Total win target   : ${round(cumulative + target_owed, 2):.2f}")
                        print(f"  │ Remaining Chances       : {remaining_chances}")
                        print(f"{'='*60}\n")

        else:  # balance_based
            print(f"\n  📊 BALANCE-BASED TARGET CALCULATION")
            base_target_risk = total_drawdown
            if base_target_risk == 0:
                print(f"  │ No drawdown — using default risk from config")
                base_target_risk = default_risk_value
            sequence_risk = base_target_risk
            print(f"  │ Base target risk: ${base_target_risk:.2f}")

        detail['sequence_next_risk'] = float(sequence_risk or 0.0)

        pending_rows = investor_fetch_pending_trades(
            investor_id, sub_account_id, programme_id)
        detail['rows_seen'] = len(pending_rows)
        print(f"\n  📋 Pending trades for this investor: {len(pending_rows)}")
        if not pending_rows:
            print(f"  │ ℹ️ No investor programme_trades copy rows found. Continuing with canonical MT5-order reconciliation so an eligible live order can be restored safely.")

        symbol_info_cache = {}
        def get_symbol_info(sym):
            si = symbol_info_cache.get(sym)
            if si is None:
                si = mt5_symbol_info(sym)
                if si is None:
                    mt5_symbol_select(sym, True)
                    si = mt5_symbol_info(sym)
                symbol_info_cache[sym] = si
            return si

        rows_updated_vol = 0
        rows_deleted = 0

        # ------------------------------------------------------------------
        # STEP 1: recovery / reset (sequence-driven)
        # ------------------------------------------------------------------
        if current_stage_drawdown > 0 or recovery_generator_type == 'loss_streak':
            print(f"\n  📝 STEP 1: Recovery Processing (SEQUENCE-DRIVEN)")
        else:
            print(f"\n  📝 STEP 1: Reset to SEQUENCE RISK")

        print(f"  {'─'*40}")
        print(f"  │ SEQUENCE NEXT RISK : ${sequence_risk:.2f}")

        # --- remove invalid price levels ---
        print(f"\n  🗑️ STEP: Removing Invalid Price Levels")
        print(f"  {'─'*60}")
        total_removed = 0
        invalid_mt5_removed = 0
        for row in pending_rows[:]:
            sym = (_raw_first_key(row, 'symbol') or '').strip()
            rid = _coerce_int(_raw_first_key(row, 'id'))
            tradeid = _coerce_int(_raw_first_key(row, 'tradeid'))
            if not sym:
                continue
            si = get_symbol_info(sym)
            if not si:
                continue
            entry = _coerce_float(_raw_first_key(row, 'entry'), default=None)
            ot = (_raw_first_key(row, 'ordertype') or '').lower()
            if entry is None:
                continue
            bid = getattr(si, 'bid', None)
            ask = getattr(si, 'ask', None)
            if bid is None or ask is None:
                continue
            is_valid = True
            if ot in ('buy-stop', 'sell-limit'):
                if entry <= ask:
                    is_valid = False
            elif ot in ('sell-stop', 'buy-limit'):
                if entry >= bid:
                    is_valid = False
            if not is_valid:
                mt5_removal_confirmed = True
                if tradeid is not None and tradeid > 0:
                    try:
                        matching_tickets = [
                            order for order in (mt5_pending_orders_for_symbols([sym]) or [])
                            if _coerce_int(getattr(order, 'magic', 0) or 0) == tradeid
                            and _identity_text(getattr(order, 'symbol', '')) == _identity_text(sym)
                        ]
                        for ticket_order in matching_tickets:
                            ticket = getattr(ticket_order, 'ticket', None)
                            if not ticket:
                                mt5_removal_confirmed = False
                                continue
                            remove_result = mt5_order_send({
                                'action': mt5.TRADE_ACTION_REMOVE,
                                'order': ticket,
                            })
                            if remove_result and remove_result.retcode == mt5.TRADE_RETCODE_DONE:
                                invalid_mt5_removed += 1
                                print(f"  │ 🗑️ Removed invalid MT5 pending ticket#{ticket} ({sym}, magic={tradeid})")
                            else:
                                mt5_removal_confirmed = False
                                retcode = getattr(remove_result, 'retcode', None) if remove_result else None
                                print(f"  │ ⚠️ Could not confirm removal of invalid MT5 ticket#{ticket} (retcode={retcode}); DB row retained")
                    except Exception as remove_error:
                        mt5_removal_confirmed = False
                        print(f"  │ ❌ Invalid MT5 order cancellation failed for row {rid}: {remove_error}; DB row retained")
                if mt5_removal_confirmed and rid is not None and delete_trade_row(rid):
                    total_removed += 1
                    rows_deleted += 1
                    pending_rows.remove(row)
                    print(f"  │ ✗ Removed invalid row {rid} ({sym} {ot} entry={entry}) after MT5 removal was confirmed")
        if total_removed:
            print(f"  │ Removed {total_removed} invalid row(s)")

        # --- apply sequence risk ---
        for row in pending_rows:
            rid = _coerce_int(_raw_first_key(row, 'id'))
            tradeid = _coerce_int(_raw_first_key(row, 'tradeid'))
            sym = (_raw_first_key(row, 'symbol') or '').strip()
            ot = (_raw_first_key(row, 'ordertype') or '').lower()
            entry = _coerce_float(_raw_first_key(row, 'entry'), default=None)
            stoploss = _coerce_float(_raw_first_key(row, 'stoploss'), default=None)

            if not sym or entry is None:
                print(f"  │ ⚠️ row {rid}: missing symbol/entry — skipped")
                continue
            si = get_symbol_info(sym)
            if si is None:
                print(f"  │ ⚠️ row {rid}: no symbol_info for {sym} — skipped")
                continue

            if recovery_generator_factor == 'profit_factor':
                new_vol, reward_if_tp, risk_if_sl = _size_volume_from_tp_target(
                    row, si, sequence_risk, programme_rr)
                if new_vol > 0:
                    print(f"  │ 🧮 row {rid}: recover ${sequence_risk:.2f} at 1:{programme_rr} "
                          f"→ {new_vol:.2f} lots | risk ${risk_if_sl:.2f} → "
                          f"reward ${reward_if_tp:.2f}")
            else:
                if stoploss is None:
                    print(f"  │ ⚠️ row {rid}: no stoploss — skipped")
                    continue
                new_vol, risk_if_sl, _per_lot = _size_volume_from_risk_target(
                    row, si, sequence_risk)
                reward_if_tp = 0.0

            if new_vol <= 0:
                print(f"  │ ⚠️ row {rid}: could not size volume — skipped")
                continue

            order_for_margin = {"symbol": sym, "entry": entry, "ordertype": ot}
            side = "buy" if 'buy' in ot else "sell"
            valid_vol, margin, free_margin, ok, shrunk, reason = \
                _validate_single_order_margin(order_for_margin, side, new_vol, buffer_pct=10.0)
            if not ok or valid_vol <= 0:
                print(f"  │ ⚠️ row {rid}: LEVERAGE CHECK failed "
                      f"(free ${free_margin:.2f}, reason: {reason})")
                continue
            if shrunk:
                print(f"  │ 🔻 row {rid}: LEVERAGE shrunk {new_vol:.2f} → {valid_vol:.2f}")
                new_vol = valid_vol
                risk_per_lot = _risk_per_lot(row, si)
                risk_if_sl = new_vol * risk_per_lot
                reward_if_tp = risk_if_sl * float(programme_rr) if recovery_generator_factor == 'profit_factor' else 0.0

            if update_trade_volume(rid, new_vol):
                rows_updated_vol += 1
                if recovery_generator_factor == 'profit_factor':
                    print(f"  │ ✓ row {rid} ({sym} {ot} tradeid={tradeid}) "
                          f"→ {new_vol:.2f} lots | risk ${risk_if_sl:.2f} → "
                          f"reward ${reward_if_tp:.2f}")
                else:
                    print(f"  │ ✓ row {rid} ({sym} {ot} tradeid={tradeid}) "
                          f"→ {new_vol:.2f} lots | risk ${risk_if_sl:.2f}")
            else:
                print(f"  │ ✗ row {rid}: volume update failed")

        # ------------------------------------------------------------------
        # STEP 2: pre-scaling
        # ------------------------------------------------------------------
        if recovery_generator_pre_scaling:
            print(f"\n{'='*60}")
            print(f"  🎯 PRE-SCALING ANALYSIS")
            print(f"{'='*60}")
            print(f"  │ Mode: {recovery_generator_type.upper()}")
            print(f"  │ Assumption: {recovery_generator_factor.upper()}")
            if recovery_generator_factor == 'profit_factor':
                print(f"  │ Programme R:R: 1:{programme_rr} "
                      f"(risk → reward multiplier)")
            print(f"  │ Linear scaling: {'✓ ENABLED' if linear_scaling else '✗ DISABLED'}")
            print(f"  │ Initial risk adder: {'✓ ENABLED' if enable_retention else '✗ DISABLED'}")
            print(f"  │   - retention: {retention_pct}%")
            print(f"  │ Pre-drawdown assumption: {'✓ ENABLED' if pre_drawdown_assumption else '✗ DISABLED'}")
            print(f"  │ Stage max risk: ${stage_max_risk:.2f}")
            print(f"  │ Recovery adder: {recovery_adder_pct}%")
            print(f"  │ Total drawdown: ${total_drawdown:.2f}")
            print(f"{'─'*60}")

            base_target_risk = total_drawdown if total_drawdown > 0 else default_risk_value

            print(f"\n{'='*60}")
            print(f"  📊 CALCULATING FINAL TARGET RISK")
            print(f"{'='*60}")
            print(f"  │ Base target risk: ${base_target_risk:.2f}")

            final_target_risk = base_target_risk

            if enable_retention:
                print(f"\n  │ Initial-risk retention ON — computing contribution")
                default_risk_val_for_bal, _ = resolve_default_risk_from_rows(
                    default_risk_rows, current_balance, default_minimum_risk)
                kept = default_risk_val_for_bal * (retention_pct / 100.0)
                print(f"  │   ├─ Default risk for balance: ${default_risk_val_for_bal:.2f}")
                print(f"  │   ├─ Keeping {retention_pct}%: ${kept:.2f}")
                print(f"  │   └─ Added to target: +${kept:.2f}")
                final_target_risk += kept

            if recovery_adder_pct > 0:
                applied = final_target_risk * (recovery_adder_pct / 100.0)
                print(f"\n  │ Recovery adder ({recovery_adder_pct}%): +${applied:.2f}")
                final_target_risk += applied

            detail['final_target_risk'] = final_target_risk
            print(f"\n  │ ✅ FINAL TARGET RISK: ${final_target_risk:.2f}")
            print(f"{'='*60}")

            symbols_in_rows = set()
            for row in pending_rows:
                sym = (_raw_first_key(row, 'symbol') or '').strip()
                if sym:
                    symbols_in_rows.add(sym)
            num_symbols = max(1, len(symbols_in_rows))
            per_symbol_target = final_target_risk / num_symbols
            detail['per_symbol_target'] = per_symbol_target
            print(f"\n  📊 SYMBOL-INDEPENDENT PRE-SCALING TARGETS:")
            print(f"  │ Total symbols: {num_symbols}")
            print(f"  │ Symbols: {', '.join(sorted(symbols_in_rows))}")
            print(f"  │ Target per symbol: ${per_symbol_target:.2f}")

            print(f"\n  📈 STEP: Scaling Limit Orders")
            print(f"  {'─'*60}")

            for sym in sorted(symbols_in_rows):
                si = get_symbol_info(sym)
                if not si:
                    continue
                symbol_rows = [r for r in pending_rows
                               if (_raw_first_key(r, 'symbol') or '').strip() == sym]
                if not symbol_rows:
                    continue

                print(f"\n  🔹 [NON-GRID] Scaling {sym} orders "
                      f"(independent target: ${per_symbol_target:.2f}):")

                if recovery_generator_factor == 'profit_factor':
                    print(f"     Target ${per_symbol_target:.2f} is the reward to earn at 1:{programme_rr}")
                    for row in symbol_rows:
                        rid = _coerce_int(_raw_first_key(row, 'id'))
                        entry = _coerce_float(_raw_first_key(row, 'entry'), default=None)
                        sl = _coerce_float(_raw_first_key(row, 'stoploss'), default=None)
                        if entry is None or sl is None:
                            continue
                        risk_per_lot = _risk_per_lot(row, si)
                        if risk_per_lot <= 0:
                            print(f"        ⚠️ row {rid}: invalid entry/SL — skipped")
                            continue
                        required_risk = per_symbol_target / float(programme_rr)
                        required_vol = required_risk / risk_per_lot
                        step = float(getattr(si, 'volume_step', 0.01)) or 0.01
                        min_vol = float(getattr(si, 'volume_min', 0.01)) or 0.01
                        steps = round(required_vol / step)
                        required_vol = max(min_vol, round(steps * step, 2))
                        risk_if_sl = required_vol * risk_per_lot
                        reward_if_tp = risk_if_sl * float(programme_rr)

                        order_for_margin = {
                            "symbol": sym, "entry": entry,
                            "ordertype": (_raw_first_key(row, 'ordertype') or '').lower(),
                        }
                        side = "buy" if 'buy' in order_for_margin["ordertype"] else "sell"
                        valid_vol, margin, free_margin, ok, shrunk, reason = \
                            _validate_single_order_margin(order_for_margin, side,
                                                          required_vol, buffer_pct=10.0)
                        if not ok or valid_vol <= 0:
                            print(f"        ⚠️ row {rid}: LEVERAGE failed "
                                  f"(free ${free_margin:.2f}, {reason})")
                            continue
                        if shrunk:
                            print(f"        🔻 row {rid}: LEVERAGE shrink "
                                  f"{required_vol:.2f} → {valid_vol:.2f}")
                            required_vol = valid_vol
                            risk_if_sl = required_vol * risk_per_lot
                            reward_if_tp = risk_if_sl * float(programme_rr)
                        if update_trade_volume(rid, required_vol):
                            rows_updated_vol += 1
                            print(f"        ✅ row {rid} [{sym}]: {required_vol:.2f} lots | "
                                  f"risk ${risk_if_sl:.2f} → reward ${reward_if_tp:.2f} "
                                  f"(target ${per_symbol_target:.2f})")
                else:
                    print(f"     Target ${per_symbol_target:.2f} is the risk")
                    for row in symbol_rows:
                        rid = _coerce_int(_raw_first_key(row, 'id'))
                        entry = _coerce_float(_raw_first_key(row, 'entry'), default=None)
                        sl = _coerce_float(_raw_first_key(row, 'stoploss'), default=None)
                        if entry is None or sl is None:
                            continue
                        risk_per_lot = _risk_per_lot(row, si)
                        if risk_per_lot <= 0:
                            continue
                        required_vol = per_symbol_target / risk_per_lot
                        step = float(getattr(si, 'volume_step', 0.01)) or 0.01
                        min_vol = float(getattr(si, 'volume_min', 0.01)) or 0.01
                        steps = round(required_vol / step)
                        required_vol = max(min_vol, round(steps * step, 2))
                        risk_if_sl = required_vol * risk_per_lot

                        order_for_margin = {
                            "symbol": sym, "entry": entry,
                            "ordertype": (_raw_first_key(row, 'ordertype') or '').lower(),
                        }
                        side = "buy" if 'buy' in order_for_margin["ordertype"] else "sell"
                        valid_vol, margin, free_margin, ok, shrunk, reason = \
                            _validate_single_order_margin(order_for_margin, side,
                                                          required_vol, buffer_pct=10.0)
                        if not ok or valid_vol <= 0:
                            print(f"        ⚠️ row {rid}: LEVERAGE failed "
                                  f"(free ${free_margin:.2f}, {reason})")
                            continue
                        if shrunk:
                            print(f"        🔻 row {rid}: LEVERAGE shrink "
                                  f"{required_vol:.2f} → {valid_vol:.2f}")
                            required_vol = valid_vol
                            risk_if_sl = required_vol * risk_per_lot
                        if update_trade_volume(rid, required_vol):
                            rows_updated_vol += 1
                            print(f"        ✅ row {rid} [{sym}]: {required_vol:.2f} lots | "
                                  f"risk ${risk_if_sl:.2f}")

            if linear_scaling:
                print(f"\n{'─'*60}")
                print(f"  📈 STEP: Linear Scaling (Progressive)")
                print(f"{'─'*60}")
                print(f"  IMPORTANT: NEVER scales DOWN, only UP or keep same")

                for sym in sorted(symbols_in_rows):
                    si = get_symbol_info(sym)
                    if not si:
                        continue
                    symbol_rows = [r for r in pending_rows
                                   if (_raw_first_key(r, 'symbol') or '').strip() == sym]
                    if not symbol_rows:
                        continue

                    print(f"\n  🔹 Linear Scaling {sym} "
                          f"(independent target: ${per_symbol_target:.2f}):")

                    stepup_linear, stepdown_linear = group_rows_by_linear(symbol_rows)

                    first_row = symbol_rows[0]
                    first_vol = _coerce_float(_raw_first_key(first_row, 'volume'), default=0.01) or 0.01
                    leader_amount = 0.0
                    if recovery_generator_factor == 'profit_factor':
                        per_lot = _reward_per_lot_from_programme_rr(first_row, si, programme_rr)
                        if per_lot > 0:
                            leader_amount = per_lot * first_vol - per_symbol_target
                            if leader_amount < 0:
                                leader_amount = per_symbol_target * 0.01
                            print(f"     LEADER reward after recovery: ${leader_amount:.2f}")
                    else:
                        per_lot = _risk_per_lot(first_row, si)
                        if per_lot > 0:
                            leader_amount = per_lot * first_vol
                            print(f"     LEADER risk: ${leader_amount:.2f}")

                    if leader_amount <= 0:
                        print(f"     No valid leader amount for {sym}")
                        continue

                    def apply_linear(linear_group, name):
                        if not linear_group:
                            return
                        print(f"\n     📊 {name.upper()} LINEAR SCALING ({len(linear_group)} orders):")
                        prev_amount = None
                        for item in linear_group:
                            row = item['order']
                            rid = _coerce_int(_raw_first_key(row, 'id'))
                            entry = _coerce_float(_raw_first_key(row, 'entry'), default=None)
                            vol = _coerce_float(_raw_first_key(row, 'volume'), default=0.01) or 0.01
                            ot = (_raw_first_key(row, 'ordertype') or '').lower()
                            if entry is None:
                                continue

                            if recovery_generator_factor == 'profit_factor':
                                per_lot = _reward_per_lot_from_programme_rr(row, si, programme_rr)
                            else:
                                per_lot = _risk_per_lot(row, si)
                            if per_lot <= 0:
                                continue
                            amount = per_lot * vol

                            if prev_amount is None:
                                print(f"        🔹 Order #{rid} (LEADER) amount=${amount:.2f} vol={vol:.2f}")
                                prev_amount = amount
                                continue

                            if amount >= prev_amount:
                                print(f"        #{rid}: already ≥ target (${amount:.2f} vs ${prev_amount:.2f}) — keep")
                                prev_amount = amount
                                continue

                            needed_amount = prev_amount
                            needed_vol = needed_amount / per_lot
                            step = float(getattr(si, 'volume_step', 0.01)) or 0.01
                            min_vol = float(getattr(si, 'volume_min', 0.01)) or 0.01
                            steps = round(needed_vol / step)
                            needed_vol = max(min_vol, round(steps * step, 2))

                            order_for_margin = {"symbol": sym, "entry": entry,
                                                "ordertype": ot}
                            side = "buy" if 'buy' in ot else "sell"
                            valid_vol, margin, free_margin, ok, shrunk, reason = \
                                _validate_single_order_margin(order_for_margin, side,
                                                              needed_vol, buffer_pct=10.0)
                            if not ok or valid_vol <= 0:
                                print(f"        ⚠️ #{rid}: LEVERAGE failed — keep current vol")
                                prev_amount = amount
                                continue
                            if shrunk:
                                print(f"        🔻 #{rid}: LEVERAGE shrink "
                                      f"{needed_vol:.2f} → {valid_vol:.2f}")
                                needed_vol = valid_vol
                            if update_trade_volume(rid, needed_vol):
                                nonlocal_updated[0] += 1
                                print(f"        ✅ #{rid}: {vol:.2f} → {needed_vol:.2f} lots "
                                      f"(cascade to ${needed_amount:.2f})")
                                prev_amount = needed_amount
                            else:
                                prev_amount = amount

                    nonlocal_updated = [0]
                    if stepup_linear:
                        apply_linear(stepup_linear, "STEPUP")
                    if stepdown_linear:
                        apply_linear(stepdown_linear, "STEPDOWN")
                    rows_updated_vol += nonlocal_updated[0]

        # ------------------------------------------------------------------
        # STEP 3: MT5 reconciliation (cancel mismatches)
        # ------------------------------------------------------------------
        print(f"\n{'='*60}")
        print(f"  🗑️ CHECKING MT5 PENDING ORDERS")
        print(f"{'='*60}")

        pending_rows = investor_fetch_pending_trades(
            investor_id, sub_account_id, programme_id)

        system_analysis_results = {}
        pending_row_by_magic = {}
        for row in pending_rows:
            tid = _coerce_int(_raw_first_key(row, 'tradeid'))
            sym = (_raw_first_key(row, 'symbol') or '').strip()
            if tid is None or tid <= 0 or not sym:
                continue
            si = get_symbol_info(sym)
            if not si:
                continue
            entry = _coerce_float(_raw_first_key(row, 'entry'), default=None)
            sl = _coerce_float(_raw_first_key(row, 'stoploss'), default=None)
            vol = _coerce_float(_raw_first_key(row, 'volume'), default=0.0) or 0.0
            if entry is None or sl is None or vol <= 0:
                continue
            per_lot_risk = abs(entry - sl) * float(getattr(si, 'trade_contract_size', 1.0))
            risk = per_lot_risk * vol
            if recovery_generator_factor == 'profit_factor':
                reward = risk * float(programme_rr)
                system_analysis_results[tid] = {
                    'symbol': sym, 'volume': vol, 'risk': risk, 'reward': reward,
                }
                pending_row_by_magic[tid] = row
            else:
                system_analysis_results[tid] = {
                    'symbol': sym, 'volume': vol, 'risk': risk, 'reward': 0.0,
                }
                pending_row_by_magic[tid] = row

        print(f"\n  📊 System Analysis Results:")
        for tid, res in system_analysis_results.items():
            sym = res['symbol']
            if recovery_generator_factor == 'profit_factor':
                print(f"     magic={tid} {sym}: {res['volume']:.2f} lots | "
                      f"risk ${res['risk']:.2f} → reward ${res['reward']:.2f}")
            else:
                print(f"     magic={tid} {sym}: {res['volume']:.2f} lots | "
                      f"risk ${res['risk']:.2f}")

        cancelled = invalid_mt5_removed
        symbols_in_rows = {v['symbol'] for v in system_analysis_results.values()}
        if symbols_in_rows:
            mt5_orders = mt5_pending_orders_for_symbols(list(symbols_in_rows))
            for order in mt5_orders:
                sym = order.symbol
                order_ticket = order.ticket
                volume = float(getattr(order, 'volume_current', None)
                               or getattr(order, 'volume_initial', 0) or 0)
                entry_o = float(order.price_open)
                sl_o = float(order.sl) if order.sl else None
                order_magic = _coerce_int(getattr(order, 'magic', 0) or 0)
                sys_res = system_analysis_results.get(order_magic)
                if sys_res is None or _identity_text(sym) != _identity_text(sys_res.get('symbol')):
                    # Another programme, manual order, unknown magic, or a
                    # symbol mismatch: never delete it from this programme job.
                    continue

                si = get_symbol_info(sym)
                contract_size = float(getattr(si, 'trade_contract_size', 1.0)) if si else 1.0

                if recovery_generator_factor == 'profit_factor':
                    if sl_o is None:
                        continue
                    live_risk = abs(entry_o - sl_o) * volume * contract_size
                    live_amount = live_risk * float(programme_rr)
                    sys_amount = sys_res['reward']
                else:
                    if sl_o is None:
                        continue
                    live_amount = abs(entry_o - sl_o) * volume * contract_size
                    sys_amount = sys_res['risk']

                sys_vol = sys_res['volume']
                tolerance = 0.10
                amount_match = (abs(live_amount - sys_amount) / sys_amount <= tolerance
                                if sys_amount > 0 else False)
                volume_match = abs(volume - sys_vol) <= 0.001

                if amount_match and volume_match:
                    print(f"  ✓ {sym} ticket#{order_ticket}: PRESERVED "
                          f"(amount ${live_amount:.2f} ≈ ${sys_amount:.2f}, vol {volume:.2f})")
                    continue

                print(f"  ❌ {sym} ticket#{order_ticket}: mismatch "
                      f"(amount ${live_amount:.2f} vs ${sys_amount:.2f}, "
                      f"vol {volume:.2f} vs {sys_vol:.2f}) — CANCEL MT5 TICKET; KEEP DB ROW FOR RE-PLACEMENT")
                try:
                    req = {"action": mt5.TRADE_ACTION_REMOVE, "order": order_ticket}
                    res = mt5_order_send(req)
                    if res and res.retcode == mt5.TRADE_RETCODE_DONE:
                        cancelled += 1
                        print(f"     ✅ MT5 ticket#{order_ticket} removed; programme_trades row retained with desired volume {sys_vol:.2f}. Place stage will recreate it.")
                    else:
                        retcode = getattr(res, 'retcode', None) if res else None
                        print(f"     ⚠️ MT5 removal not confirmed (retcode={retcode}); database row retained.")
                except Exception as e:
                    print(f"     ❌ MT5 cancellation error (DB row retained): {e}")

        # ------------------------------------------------------------------
        # STEP 4: restore MT5 orders not in DB (order.magic → tradeid)
        # ------------------------------------------------------------------
        symbols_in_rows = set()
        for row in pending_rows:
            sym = (_raw_first_key(row, 'symbol') or '').strip()
            if sym:
                symbols_in_rows.add(sym)

        programme_userid = resolve_programme_userid(programme_id)
        canonical_groups = fetch_programme_analytics_magic_groups(
            programme_id, programme_userid, require_live=True,
        )
        subaccount_catalog = fetch_subaccount_magic_catalog(investor_id, sub_account_id)
        restore_tradeids = set(canonical_groups.get('canonical_tradeids') or set())
        restore_ambiguous = set(canonical_groups.get('ambiguous') or set())
        if not subaccount_catalog.get('ok') or not canonical_groups.get('ok'):
            restore_tradeids = set()
        restore_ambiguous.update(subaccount_catalog.get('ambiguous') or set())
        restore_tradeids.difference_update(restore_ambiguous)

        # A DB copy may be missing while the matching canonical order is still
        # live in this verified investor MT5 account. Use only symbols from
        # unambiguous canonical analytics identities; never scan unrelated symbols.
        if not symbols_in_rows and restore_tradeids:
            for tid in restore_tradeids:
                group = (canonical_groups.get('by_magic') or {}).get(tid, {}).get('group', {})
                canonical_symbol = (_raw_first_key(group, 'symbol') or '').strip()
                if canonical_symbol:
                    symbols_in_rows.add(canonical_symbol)

        if symbols_in_rows and restore_tradeids:
            mt5_orders = mt5_pending_orders_for_symbols(list(symbols_in_rows))
            existing_tradeids = set()
            for row in pending_rows:
                tid = _coerce_int(_raw_first_key(row, 'tradeid'))
                if tid is not None:
                    existing_tradeids.add(tid)

            type_map = {0: 'buy', 1: 'sell',
                        2: 'buy-limit', 3: 'sell-limit',
                        4: 'buy-stop', 5: 'sell-stop'}

            for order in mt5_orders:
                order_magic = getattr(order, 'magic', None)
                if order_magic is None or int(order_magic) <= 0:
                    continue
                if int(order_magic) in existing_tradeids:
                    continue
                if int(order_magic) not in restore_tradeids:
                    continue

                sym = order.symbol
                canonical_group = (canonical_groups.get('by_magic') or {}).get(int(order_magic), {}).get('group', {})
                if _identity_text(sym) != _identity_text(_raw_first_key(canonical_group, 'symbol')):
                    continue
                typ = int(order.type)
                ordertype = type_map.get(typ, 'buy-limit')
                entry = float(order.price_open)
                sl = float(order.sl) if order.sl else None
                tp = float(order.tp) if order.tp else None
                vol = float(getattr(order, 'volume_current', None)
                            or getattr(order, 'volume_initial', 0) or 0)

                restored = restore_trade_row(
                    investor_id=investor_id,
                    sub_account_id=sub_account_id,
                    programme_id=programme_id,
                    tradeid=int(order_magic),
                    symbol=sym,
                    ordertype=ordertype,
                    entry=entry,
                    stoploss=sl,
                    takeprofit=tp,
                    volume=vol,
                    timeframe=(_raw_first_key(canonical_group, 'timeframe')
                               or (_raw_first_key(pending_rows[0], 'timeframe') if pending_rows else None)
                               or 'M1'),
                    trade_risk_reward=None,
                    setup_group_id=_raw_first_key(canonical_group, 'setup_group_id'),
                )
                if restored:
                    existing_tradeids.add(int(order_magic))
                    print(f"  ♻️ Restored MT5 order magic={order_magic} ({sym} {ordertype}) "
                          f"→ programme_trades as tradeid={order_magic}, vol={vol:.2f}")

        detail['rows_updated'] = rows_updated_vol
        detail['rows_deleted'] = rows_deleted
        detail['mt5_cancelled'] = cancelled

        print(f"\n  📊 Investor {investor_id} summary: "
              f"updated={rows_updated_vol} deleted={rows_deleted} cancelled={cancelled}")

        return detail

    # ==================================================================
    # SECTION 8 — TOP-LEVEL ENUMERATION (skipped when jobs= passed in)
    # ==================================================================
    if jobs is None:
        print("\n" + "─"*80)
        print(" 📁 LOADING INVESTOR JOBS FROM DB (VPS-scoped)")
        print("─"*80)

        local_ip = get_local_ip()
        if not local_ip:
            print(" ⚠️ Could not determine local IP")
            stats['errors'].append('local IP unknown')
            stats['processing_success'] = False
            recovery_generator_alert['errors'] = list(stats['errors'])
            return stats
        print(f" │ Local IP: {local_ip}")

        all_jobs = fetch_vps_investor_jobs(local_ip)
        if not all_jobs:
            print(" │ ℹ️ No active investor jobs found for this VPS")
            stats['processing_success'] = True
            recovery_generator_alert['errors'] = []
            return stats

        if inv_db_id:
            jobs = [j for j in all_jobs if str(j.get('investor_id')) == str(inv_db_id)]
            if not jobs:
                print(f" ⚠️ Investor {inv_db_id} not found in any VPS-scoped job")
                stats['errors'].append(f"Investor {inv_db_id} not found")
                stats['processing_success'] = False
                recovery_generator_alert['errors'] = list(stats['errors'])
                return stats
        else:
            jobs = all_jobs

        print(f" │ Loaded {len(jobs)} investor job(s) from DB")
        print("─"*80)

    programme_cfg_cache = {}

    def build_programme_cfg(programme_id, programme_name):
        print(f"\n{'='*60}")
        print(f"  📦 LOADING PROGRAMME CONFIGURATION (id={programme_id})")
        print(f"{'='*60}")

        config_rows = programme_fetch_recovery_generator_config(programme_id)
        if not config_rows:
            print(f"  │ ⚠️ No recovery_generator rows for programme {programme_id}")
            return None

        first = config_rows[0]
        programme_rr = programme_fetch_signal_provider_risk_reward(programme_id)

        cfg = {
            'config_rows': config_rows,
            'default_risk_rows': programme_fetch_default_risk_rows(programme_id),
            'rr_cfg': programme_fetch_rr_config(programme_id),
            'loss_streak_chances': programme_fetch_loss_streak_chances(programme_id),
            'programme_rr': programme_rr,
            'enable_recovery_generator': to_bool(_raw_first_key(first, 'enable_recovery_generator'), default=False),
            'recovery_generator_type': (_raw_first_key(first, 'recovery_generator_type') or 'balance_based').strip(),
            'recovery_generator_factor': (_raw_first_key(first, 'recovery_generator_factor') or 'stoploss_factor').strip(),
            'recovery_adder_pct': parse_percent(_raw_first_key(first, 'recovery_generator_loss_recovery_adder_percentage')),
            'pre_drawdown_assumption': to_bool(_raw_first_key(first, 'pre_drawdown_assumption'), default=False),
            'recovery_generator_pre_scaling': to_bool(_raw_first_key(first, 'recovery_generator_pre_scaling'), default=False),
            'linear_scaling': to_bool(_raw_first_key(first, 'recovery_generator_linear_scaling'), default=False),
            'enable_retention': to_bool(_raw_first_key(first, 'enable_initial_risk_retention'), default=False),
            'retention_pct': parse_percent(_raw_first_key(first, 'initial_risk_retention_percentage')),
        }

        print(f"  │ enable_recovery_generator        : {cfg['enable_recovery_generator']}")
        print(f"  │ recovery_generator_type          : {cfg['recovery_generator_type']}")
        print(f"  │ recovery_generator_factor        : {cfg['recovery_generator_factor']}")
        if cfg['recovery_generator_factor'] == 'profit_factor':
            print(f"  │ programme_rr             : {cfg['programme_rr']}  "
                  f"(risk → reward multiplier)")
        print(f"  │ recovery_adder_pct       : {cfg['recovery_adder_pct']}%")
        print(f"  │ pre_drawdown_assumption  : {cfg['pre_drawdown_assumption']}")
        print(f"  │ recovery_generator_pre_scaling   : {cfg['recovery_generator_pre_scaling']}")
        print(f"  │ linear_scaling           : {cfg['linear_scaling']}")
        print(f"  │ enable_retention         : {cfg['enable_retention']} ({cfg['retention_pct']}%)")
        print(f"  │ loss_streak_chances      : {cfg['loss_streak_chances']}")
        print(f"  │ default_risk_rows        : {len(cfg['default_risk_rows'])}")
        print(f"  │ rr_cfg                   : {cfg['rr_cfg']}")
        print(f"{'='*60}\n")
        return cfg

    for job in jobs:
        investor_id = job.get('investor_id')
        sub_account_id = job.get('sub_account_id')
        programme_id = job.get('programme_id')
        programme_name = job.get('programme_name') or f"programme_{programme_id}"

        if investor_id is None or sub_account_id is None or programme_id is None:
            stats['investors_skipped'] += 1
            continue

        try:
            if programme_id not in programme_cfg_cache:
                programme_cfg_cache[programme_id] = build_programme_cfg(
                    programme_id, programme_name)
                stats['programmes_processed'] += 1
            programme_cfg = programme_cfg_cache[programme_id]

            if not programme_cfg:
                print(f"\n ⏭️ Investor {investor_id}: no programme cfg — skipped")
                stats['investors_skipped'] += 1
                continue

            detail = process_investor(investor_id, sub_account_id, programme_id,
                                      programme_name, programme_cfg)
            stats['details'].append(detail)
            stats['investors_processed'] += 1

            if not recovery_generator_alert.get('programme_id'):
                recovery_generator_alert['programme_id'] = programme_id
                recovery_generator_alert['investor_id'] = investor_id
                recovery_generator_alert['stage'] = detail.get('stage', 0)
                recovery_generator_alert['stage_drawdown'] = detail.get('stage_drawdown', 0.0)
                recovery_generator_alert['total_drawdown'] = detail.get('drawdown', 0.0)
                recovery_generator_alert['sequence_next_risk'] = detail.get('sequence_next_risk', 0.0)
                recovery_generator_alert['mode'] = detail.get('mode')
                recovery_generator_alert['factor'] = detail.get('factor')
                recovery_generator_alert['programme_rr'] = detail.get('programme_rr')
        except Exception as e:
            print(f"\n[CRITICAL] Recovery Generator investor={investor_id}: {e}")
            traceback.print_exc()
            stats['errors'].append(f"investor={investor_id}: {e}")
            stats['investors_skipped'] += 1

    stats['rows_updated'] = sum(d.get('rows_updated', 0) for d in stats['details'])
    stats['rows_restored'] = sum(d.get('rows_restored', 0) for d in stats['details'])
    stats['mt5_cancelled'] = sum(d.get('mt5_cancelled', 0) for d in stats['details'])

    recovery_generator_alert['rows_updated'] = stats['rows_updated']
    recovery_generator_alert['rows_restored'] = stats['rows_restored']
    recovery_generator_alert['mt5_cancelled'] = stats['mt5_cancelled']
    recovery_generator_alert['errors'] = list(stats['errors'])

    print(f"\n{'='*10} 🎰 RECOVERY_GENERATOR SUMMARY (DB) {'='*10}")
    print(f"  Programmes processed: {stats['programmes_processed']}")
    print(f"  Investors processed : {stats['investors_processed']}")
    print(f"  Investors skipped   : {stats['investors_skipped']}")
    print(f"  Rows volume-updated : {stats['rows_updated']}")
    print(f"  Rows restored       : {stats['rows_restored']}")
    print(f"  MT5 cancelled       : {stats['mt5_cancelled']}")
    print(f"  Errors              : {len(stats['errors'])}")
    for e in stats['errors']:
        print(f"     • {e}")
    print(f"{'='*10} 🏁 COMPLETE {'='*10}\n")

    stats['processing_success'] = True
    return stats

# ==============================================================================
#  CALCULATE INVESTOR SYMBOLS ORDERS — DB version
#
#  Source of truth for R:R: signals_provider_interest (programme-scoped).
#  The three supported policies are:
#
#    fixed_risk_reward:
#      Every eligible trade is rewritten to exactly the configured R:R.
#    minimum_risk_reward:
#      Only trades below the configured R:R floor are raised to that floor.
#      Trades already at/above the floor are left unchanged.
#    custom_and_default_risk_reward:
#      There is no configured target. Keep trades at/above 1:1 and raise
#      trades below 1:1 to exactly 1:1.
#
#  R:R is a price-distance ratio. Corrections therefore update takeprofit;
#  volume is calculated separately for the financial risk policy.
#  Only mapped programme signals with programme_analytics.live_trades=1,
#  trade_configured=1, and trade_time within the investor subaccount's
#  harvhub.execution_start_date..now window are considered.
#  Writes are investor-row scoped. No MT5 orders are sent by this stage.
# ==============================================================================

def _validate_single_order_margin_for_calc(order, side, desired_volume, buffer_pct=10.0):
    """
    Module-level wrapper around MT5 margin validation for the calculate
    stage. Returns (final_volume, margin_used, free_margin, allowed,
    shrunk, reason). Falls back to a no-op "allowed" result when the
    account is unavailable, so the calculate stage never blocks on a
    missing MT5 connection.
    """
    acc = mt5_account_info()
    if acc is None:
        return desired_volume, 0.0, 0.0, True, False, "no_account_info"
    symbol = order.get("symbol")
    entry = order.get("entry")
    if not symbol or entry is None:
        return desired_volume, 0.0, 0.0, False, False, "missing_data"
    si = mt5_symbol_info(symbol)
    if not si:
        return desired_volume, 0.0, 0.0, False, False, "no_symbol_info"
    try:
        free_margin = float(acc.margin_free)
    except Exception:
        return desired_volume, 0.0, 0.0, True, False, "no_free_margin_field"
    allowed = free_margin * (1.0 - buffer_pct / 100.0)
    if allowed <= 0:
        return 0.0, 0.0, free_margin, False, True, "no_free_margin"
    mt5_type = 0 if side == "buy" else 1
    try:
        margin = mt5_order_calc_margin(mt5_type, symbol, float(desired_volume), float(entry))
    except Exception:
        margin = None
    if margin is None or margin <= 0:
        return desired_volume, 0.0, free_margin, True, False, "margin_calc_failed"
    if margin <= allowed:
        return desired_volume, float(margin), free_margin, True, False, "fits"
    # shrink linearly
    step = float(getattr(si, 'volume_step', 0.01) or 0.01)
    min_vol = float(getattr(si, 'volume_min', 0.01) or 0.01)
    ratio = allowed / margin
    raw = max(min_vol, desired_volume * ratio)
    steps = round(raw / step) if step > 0 else 0
    shrunk_vol = max(min_vol, round(steps * step, 2))
    try:
        shrunk_margin = mt5_order_calc_margin(mt5_type, symbol, shrunk_vol, float(entry))
    except Exception:
        shrunk_margin = None
    if shrunk_margin is None or shrunk_margin > allowed:
        return 0.0, 0.0, free_margin, False, True, "cannot_fit_min_volume"
    return shrunk_vol, float(shrunk_margin), free_margin, True, True, "shrunk_to_fit"

# ==============================================================================
#  CALCULATE INVESTOR SYMBOLS ORDERS — DB version
#
#  Source of truth for R:R:  signals_provider_interest (programme-scoped)
#      interest_risk_reward       -> numeric R:R ratio (e.g. 8 means 1:8)
#      interest_risk_reward_type  -> string classifier:
#
#          'minimum_risk_reward'   minimum floor
#              - compute current R:R from entry to takeprofit
#              - if current < value  -> recalculate volume so R:R == value
#              - if current >= value -> NO change
#
#          'fixed_risk_reward'            exact rewrite
#              - compute current R:R
#              - if current != value -> correct takeprofit so R:R == value
#
#          'custom_and_default_risk_reward'     do nothing
#              - no recalculation at all
#              - EXCEPT: if current R:R < 1.0, force recalc to R:R == 1.0
#
#  The function ALSO syncs the per-investor authorisation snapshot:
#  it copies interest_risk_reward + interest_risk_reward_type down to each
#  investor row (same values the signals dashboard uses) so downstream
#  consumers see a consistent view.
#
#  ACTIONABLE-ROW PREDICATE (must match every other stage):
#      orderstatus = 'pending'  OR  orderstatus IS NULL  OR  orderstatus = ''
#
#  NO files, NO JSON, NO local folders. Pure DB.
# ==============================================================================
def calculate_investor_symbols_orders_db(inv_db_id=None, jobs=None):
    print(f"\n{'='*10} 📐 CALCULATE INVESTOR SYMBOLS ORDERS (DB) {'='*10}")

    stats = {
        'investor_id': inv_db_id if inv_db_id else 'all',
        'programmes_processed': 0,
        'investors_processed': 0,
        'rows_seen': 0,
        'rows_calculated': 0,
        'rows_skipped': 0,
        'rows_synced_only': 0,
        'rows_volume_seeded': 0,
        'rows_risk_resized': 0,
        'rows_rr_resized': 0,
        'rows_tp_corrected': 0,
        'rows_tp_synthesized': 0,
        'errors': [],
        'details': [],
        'processing_success': False,
    }

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    def _norm_rr_type(raw):
        """Normalize current enum values and known legacy labels."""
        if raw is None:
            return None
        value = str(raw).strip().lower().replace('-', '_').replace(' ', '_')
        while '__' in value:
            value = value.replace('__', '_')
        aliases = {
            'fixed': 'fixed_risk_reward',
            'fixed_rr': 'fixed_risk_reward',
            'exact': 'fixed_risk_reward',
            'exact_risk_reward': 'fixed_risk_reward',
            'minimum': 'minimum_risk_reward',
            'min': 'minimum_risk_reward',
            'floor': 'minimum_risk_reward',
            'custom_minimum_risk_reward': 'minimum_risk_reward',
            'custom_minimum': 'minimum_risk_reward',
            'custom_fixed_risk_reward': 'custom_and_default_risk_reward',
            'custom_fixed': 'custom_and_default_risk_reward',
            'custom_and_default': 'custom_and_default_risk_reward',
            'custom_default_risk_reward': 'custom_and_default_risk_reward',
        }
        value = aliases.get(value, value)
        if value in ('fixed_risk_reward', 'minimum_risk_reward',
                     'custom_and_default_risk_reward'):
            return value
        return None

    def _fetch_programme_rr(programme_id):
        row = db_fetch_one(
            """
            SELECT id, risk_reward, risk_reward_type
              FROM signals_provider_interest
             WHERE programme_id = %s
             ORDER BY id DESC
             LIMIT 1
            """,
            [programme_id], timeout=10.0, quiet=True,
        )
        if not row:
            return None, None, None
        rr_val = _coerce_float(_raw_first_key(row, 'risk_reward'), default=None)
        rr_type = _raw_first_key(row, 'risk_reward_type')
        rr_norm = _norm_rr_type(rr_type)
        if rr_norm in ('fixed_risk_reward', 'minimum_risk_reward'):
            if rr_val is None or rr_val <= 0:
                return None, rr_type, row
            rr_val = float(rr_val)
        elif rr_norm == 'custom_and_default_risk_reward':
            # This policy intentionally has no configured target value.
            # The only enforced threshold is the policy's intrinsic 1:1 floor.
            rr_val = None
        else:
            return None, rr_type, row
        return rr_val, rr_type, row

    def _fetch_programme_default_risk_brackets(programme_id):
        rows = db_fetch_all(
            """
            SELECT from_balance_range, to_balance_range, risk_amount
              FROM accountmanagement_balance_default_risk
             WHERE programme_id = %s
             ORDER BY from_balance_range ASC, id ASC
            """,
            [programme_id], timeout=10.0, quiet=True,
        )
        out = []
        for r in rows:
            try:
                lo = _coerce_float(_raw_first_key(r, 'from_balance_range'), default=None)
                hi = _coerce_float(_raw_first_key(r, 'to_balance_range'),   default=None)
                rk = _coerce_float(_raw_first_key(r, 'risk_amount'),        default=None)
            except Exception:
                continue
            if lo is None or rk is None:
                continue
            if hi is None or hi <= 0:
                hi = float('inf')
            out.append({'from': float(lo), 'to': float(hi), 'risk': float(rk)})
        return out

    def _resolve_default_risk_for_balance(brackets, balance):
        if not brackets or balance is None:
            return None, None
        for b in brackets:
            if b['from'] <= balance <= b['to']:
                return b['risk'], b
        below = [b for b in brackets if b['from'] <= balance]
        if below:
            chosen = below[-1]
            return chosen['risk'], chosen
        return brackets[0]['risk'], brackets[0]

    def _compute_current_rr(entry, sl, tp):
        if entry is None or sl is None or tp is None:
            return None, None, None
        risk   = abs(float(entry) - float(sl))
        reward = abs(float(tp) - float(entry))
        if risk <= 0:
            return None, risk, reward
        return reward / risk, risk, reward

    def _round_volume_to_symbol(volume, symbol_info):
        if volume is None or volume <= 0:
            return 0.0
        step    = float(getattr(symbol_info, 'volume_step', 0.01) or 0.01)
        min_vol = float(getattr(symbol_info, 'volume_min', 0.01) or 0.01)
        max_vol = float(getattr(symbol_info, 'volume_max', 100.0) or 100.0)
        if step <= 0:
            step = 0.01
        steps = round(volume / step)
        return max(min_vol, min(max_vol, round(steps * step, 2)))

    def _min_volume_for_symbol(symbol_info):
        if symbol_info is None:
            return 0.01
        try:
            v = float(getattr(symbol_info, 'volume_min', 0.01) or 0.01)
            return v if v > 0 else 0.01
        except Exception:
            return 0.01

    def _price_digits(symbol_info, tick_size):
        try:
            if symbol_info is not None and hasattr(symbol_info, 'digits'):
                d = int(symbol_info.digits)
                if d >= 0:
                    return d
        except Exception:
            pass
        try:
            ts = float(tick_size) if tick_size else 0.00001
            if ts <= 0:
                ts = 0.00001
            if ts < 1:
                return len(f"{ts:.10f}".rstrip('0').split('.')[-1])
        except Exception:
            pass
        return 5

    def _rr_precision_tolerance(entry, sl, symbol_info):
        """Half-tick R:R tolerance so fixed targets are idempotent at broker precision."""
        try:
            risk_dist = abs(float(entry) - float(sl))
            tick_size = float(getattr(symbol_info, 'trade_tick_size', 0) or
                              getattr(symbol_info, 'point', 0) or 0)
            if risk_dist <= 0 or tick_size <= 0:
                return 1e-6
            return max(1e-6, min(0.02, tick_size / (2.0 * risk_dist) + 1e-9))
        except Exception:
            return 1e-6

    def _synthesize_tp(entry, sl, rr_value, is_buy, digits,
                       tick_size=None, round_up=False):
        if entry is None or sl is None or rr_value is None or rr_value <= 0:
            return None
        try:
            import math
            entry_f = float(entry)
            sl_f    = float(sl)
            risk_dist = abs(entry_f - sl_f)
            if risk_dist <= 0:
                return None
            reward_dist = risk_dist * float(rr_value)
            tp = (entry_f + reward_dist) if is_buy else (entry_f - reward_dist)
            tick = float(tick_size or 0)
            if tick > 0:
                units = tp / tick
                if round_up:
                    units = math.ceil(units - 1e-10) if is_buy else math.floor(units + 1e-10)
                else:
                    units = round(units)
                tp = units * tick
            return round(tp, digits)
        except Exception:
            return None

    def _risk_per_lot(entry, sl, symbol_info):
        if entry is None or sl is None or symbol_info is None:
            return 0.0
        try:
            price_dist = abs(float(entry) - float(sl))
            contract   = float(getattr(symbol_info, 'trade_contract_size', 1.0) or 1.0)
            return price_dist * contract
        except Exception:
            return 0.0

    def _risk_driven_volume(target_risk, risk_per_lot, symbol_info):
        if target_risk is None or target_risk <= 0:
            return None
        if risk_per_lot is None or risk_per_lot <= 0:
            return None
        try:
            raw = float(target_risk) / float(risk_per_lot)
        except Exception:
            return None
        return _round_volume_to_symbol(raw, symbol_info)

    # ==================================================================
    # TOP-LEVEL ENUMERATION (VPS-scoped) — skipped when jobs= passed in
    # ==================================================================
    if jobs is None:
        print("\n" + "─"*80)
        print(" 📁 LOADING INVESTOR JOBS FROM DB (VPS-scoped)")
        print("─"*80)

        local_ip = get_local_ip()
        if not local_ip:
            print(" ⚠️ Could not determine local IP")
            stats['errors'].append('local IP unknown')
            return stats
        print(f" │ Local IP: {local_ip}")

        all_jobs = fetch_vps_investor_jobs(local_ip)
        if not all_jobs:
            print(" │ ℹ️ No active investor jobs found for this VPS")
            stats['processing_success'] = True
            return stats

        if inv_db_id:
            jobs = [j for j in all_jobs if str(j.get('investor_id')) == str(inv_db_id)]
            if not jobs:
                print(f" ⚠️ Investor {inv_db_id} not found in any VPS-scoped job")
                stats['errors'].append(f"Investor {inv_db_id} not found")
                return stats
        else:
            jobs = all_jobs

        print(f" │ Loaded {len(jobs)} investor job(s) from DB")
        print("─"*80)

    programme_rr_cache      = {}
    programme_bracket_cache = {}
    symbol_info_cache       = {}

    def get_symbol_info(sym):
        si = symbol_info_cache.get(sym)
        if si is None:
            si = mt5_symbol_info(sym)
            if si is None:
                mt5_symbol_select(sym, True)
                si = mt5_symbol_info(sym)
            symbol_info_cache[sym] = si
        return si

    def _current_balance():
        acc = mt5_account_info()
        if acc is None:
            return None
        try:
            return float(acc.balance)
        except Exception:
            return None

    # ==================================================================
    # PER-INVESTOR PIPELINE
    # ==================================================================
    for job in jobs:
        investor_id    = job.get('investor_id')
        sub_account_id = job.get('sub_account_id')
        programme_id   = job.get('programme_id')
        programme_name = job.get('programme_name') or f"programme_{programme_id}"
        programme_userid = resolve_programme_userid(programme_id, job)

        if investor_id is None or sub_account_id is None or programme_id is None or not programme_userid:
            stats['rows_skipped'] += 1
            continue

        cfg = programme_rr_cache.get(programme_id)
        if cfg is None:
            rr_val, rr_type, raw_row = _fetch_programme_rr(programme_id)
            cfg = {
                'rr_value':     rr_val,
                'rr_type_raw':  rr_type,
                'rr_type_norm': _norm_rr_type(rr_type),
                'raw_row':      raw_row,
            }
            programme_rr_cache[programme_id] = cfg
            stats['programmes_processed'] += 1

        brackets = programme_bracket_cache.get(programme_id)
        if brackets is None:
            brackets = _fetch_programme_default_risk_brackets(programme_id)
            programme_bracket_cache[programme_id] = brackets

        print(f"\n{'─'*72}")
        print(f" 📐 Investor {investor_id} (sub {sub_account_id}) | "
              f"programme '{programme_name}' (id={programme_id})")
        print(f"{'─'*72}")
        effective_rr_display = (cfg['rr_value'] if cfg['rr_value'] is not None else
                                (1.0 if cfg['rr_type_norm'] == 'custom_and_default_risk_reward' else None))
        print(f"  │ Programme R:R value : "
              f"{'1:' + str(effective_rr_display) if effective_rr_display is not None else '—'}")
        print(f"  │ Programme R:R type  : {cfg['rr_type_raw'] or '—'}")
        print(f"  │ Normalised type     : '{cfg['rr_type_norm']}'")
        print(f"  │ Default risk brackets: {len(brackets)}")

        rr_type = cfg['rr_type_norm']
        if rr_type not in ('fixed_risk_reward', 'minimum_risk_reward', 'custom_and_default_risk_reward'):
            print(f"  │ ⚠️ Unsupported or missing risk_reward_type — skipping investor")
            stats['rows_skipped'] += 1
            continue
        if rr_type in ('fixed_risk_reward', 'minimum_risk_reward') and (cfg['rr_value'] is None or cfg['rr_value'] <= 0):
            print(f"  │ ⚠️ {rr_type} requires a positive risk_reward value — skipping investor")
            stats['rows_skipped'] += 1
            continue

        balance = _current_balance()
        target_risk     = None
        matched_bracket = None
        if balance is not None and brackets:
            target_risk, matched_bracket = _resolve_default_risk_for_balance(brackets, balance)
        print(f"  │ Live MT5 balance    : "
              f"{'$' + format(balance, '.2f') if balance is not None else '—'}")
        if matched_bracket is not None and target_risk is not None:
            print(f"  │ Default risk applied: ${target_risk:.2f} "
                  f"(bracket {matched_bracket['from']:.2f} - {matched_bracket['to']:.2f})")
        else:
            print(f"  │ Default risk applied: — (no bracket available)")

        # ------------------------------------------------------------------
        # ACTIONABLE ROWS
        # Same predicate as place_usd_orders_db, chunk, recovery_generator:
        #   orderstatus = 'pending'  OR  IS NULL  OR  = ''
        # ------------------------------------------------------------------
        start_row = db_fetch_one(
            "SELECT execution_start_date FROM harvhub WHERE id = %s AND sub_account_id = %s LIMIT 1",
            [investor_id, sub_account_id], timeout=10.0, quiet=True,
        ) or {}
        execution_start_date = _raw_first_key(start_row, 'execution_start_date')
        rows, eligibility = fetch_live_investor_programme_trades(
            programme_id, programme_userid, investor_id, sub_account_id,
            execution_start_date, pending_only=True,
        )
        if eligibility.get('reason'):
            print(f"  │ ⚠️ Live trade eligibility gate: {eligibility['reason']} — no rows processed")
            stats['errors'].append(f"programme {programme_id}, investor {investor_id}: {eligibility['reason']}")

        stats['rows_seen'] += len(rows)
        stats['investors_processed'] += 1

        if not rows:
            print(f"  │ ℹ️ No actionable rows for this investor")
            continue

        print(f"  │ Actionable rows     : {len(rows)}")

        for r in rows:
            row_id  = _coerce_int(_raw_first_key(r, 'id'))
            tradeid = _coerce_int(_raw_first_key(r, 'tradeid'))
            sym     = (_raw_first_key(r, 'symbol') or '').strip()
            ot      = (_raw_first_key(r, 'ordertype') or '').strip().lower()
            entry   = _coerce_float(_raw_first_key(r, 'entry'), default=None)
            sl      = _coerce_float(_raw_first_key(r, 'stoploss'), default=None)
            tp      = _coerce_float(_raw_first_key(r, 'takeprofit'), default=None)
            cur_vol = _coerce_float(_raw_first_key(r, 'volume'), default=None)

            if row_id is None:
                stats['rows_skipped'] += 1
                continue

            try:
                db_execute(
                    """
                    UPDATE programme_trades
                       SET interest_risk_reward      = %s,
                           interest_risk_reward_type = %s
                     WHERE id = %s
                    """,
                    [cfg['rr_value'], cfg['rr_type_raw'], row_id],
                    timeout=10.0, quiet=True,
                )
                stats['rows_synced_only'] += 1
            except Exception:
                pass

            if not sym or entry is None or sl is None:
                stats['rows_skipped'] += 1
                print(f"  │ ⚠️ row {row_id}: missing symbol/entry/sl — skipped")
                continue

            si = get_symbol_info(sym)
            if si is None:
                stats['rows_skipped'] += 1
                print(f"  │ ⚠️ row {row_id}: no symbol_info for {sym} — skipped")
                continue

            if not (ot.startswith('buy') or ot.startswith('sell')):
                stats['rows_skipped'] += 1
                print(f"  │ ⚠️ row {row_id}: unknown ordertype '{ot}' — cannot determine R:R direction")
                continue
            is_buy = ot.startswith('buy')
            if (is_buy and float(sl) >= float(entry)) or ((not is_buy) and float(sl) <= float(entry)):
                stats['rows_skipped'] += 1
                print(f"  │ ⚠️ row {row_id}: stoploss is on the wrong side of entry — R:R cannot be corrected safely")
                continue

            # ---------- 1. DETERMINE AND CORRECT THE TAKE-PROFIT PRICE ----------
            rr_type = cfg['rr_type_norm']
            target_rr = (float(cfg['rr_value'])
                         if rr_type in ('fixed_risk_reward', 'minimum_risk_reward')
                         else 1.0)
            digits = _price_digits(si, getattr(si, 'trade_tick_size', None))
            tp_synthesized = False
            tp_corrected = False

            current_rr, risk_dist, reward_dist = _compute_current_rr(entry, sl, tp)
            tp_on_correct_side = (
                tp is not None and
                ((is_buy and float(tp) > float(entry)) or
                 ((not is_buy) and float(tp) < float(entry)))
            )
            if rr_type == 'fixed_risk_reward':
                fixed_tolerance = _rr_precision_tolerance(entry, sl, si)
                needs_tp_correction = (
                    current_rr is None or not tp_on_correct_side or
                    abs(float(current_rr or 0.0) - target_rr) > fixed_tolerance
                )
                policy_reason = f"fixed R:R requires exactly 1:{target_rr}"
            elif rr_type == 'minimum_risk_reward':
                needs_tp_correction = (
                    current_rr is None or not tp_on_correct_side or
                    float(current_rr or 0.0) < target_rr - 1e-9
                )
                policy_reason = f"minimum R:R requires at least 1:{target_rr}"
            else:
                needs_tp_correction = (
                    current_rr is None or not tp_on_correct_side or
                    float(current_rr or 0.0) < 1.0 - 1e-9
                )
                policy_reason = 'custom/default R:R requires at least 1:1'

            if needs_tp_correction:
                tick_size = float(getattr(si, 'trade_tick_size', 0) or getattr(si, 'point', 0) or 0)
                new_tp = _synthesize_tp(
                    entry, sl, target_rr, is_buy, digits,
                    tick_size=tick_size,
                    round_up=(rr_type in ('minimum_risk_reward', 'custom_and_default_risk_reward')),
                )
                if new_tp is None:
                    stats['rows_skipped'] += 1
                    print(f"  │ ⚠️ row {row_id}: TP cannot be corrected because entry/SL is invalid (entry={entry}, sl={sl})")
                    continue
                tp_synthesized = tp is None or abs(float(tp or 0.0)) < 1e-12
                tp_corrected = True
                tp = new_tp
                stats['rows_tp_corrected'] += 1
                stats['rows_rr_resized'] += 1  # legacy counter name retained for compatibility
                if tp_synthesized:
                    stats['rows_tp_synthesized'] += 1
                print(f"  │ 🎯 row {row_id}: {policy_reason}; TP corrected to {tp} (R:R = 1:{target_rr})")

            current_rr, risk_dist, reward_dist = _compute_current_rr(entry, sl, tp)
            if current_rr is None:
                stats['rows_skipped'] += 1
                print(f"  │ ⚠️ row {row_id}: could not compute R:R after TP handling — skipped")
                continue

            # ---------- 2. SELF-HEALING BASELINE VOLUME ----------
            vol_seeded = False
            if cur_vol is None or cur_vol <= 0:
                cur_vol = _min_volume_for_symbol(si)
                vol_seeded = True
                stats['rows_volume_seeded'] += 1
                print(f"  │ 🩹 row {row_id}: volume was NULL/zero — seeding from {sym} volume_min = {cur_vol}")

            # ---------- 3. FINANCIAL RISK-DRIVEN VOLUME ----------
            risk_resized = False
            if target_risk is not None:
                rpl = _risk_per_lot(entry, sl, si)
                if rpl > 0:
                    risk_vol = _risk_driven_volume(target_risk, rpl, si)
                    if risk_vol is not None and risk_vol > 0:
                        if abs(risk_vol - cur_vol) > 1e-9:
                            risk_resized = True
                            stats['rows_risk_resized'] += 1
                        cur_vol = risk_vol

            print(f"\n  │ 📄 row {row_id} (tradeid={tradeid}, {sym} {ot.upper()})")
            print(f"  │    entry={entry}  sl={sl}  tp={tp}")
            print(f"  │    current R:R       = 1:{round(current_rr, 4)}")
            print(f"  │    policy            = {rr_type}")
            print(f"  │    required R:R      = 1:{target_rr}")
            print(f"  │    TP correction     = {'YES' if tp_corrected else 'NO'}")
            print(f"  │    volume            = {cur_vol} (risk-sized={risk_resized}, seeded={vol_seeded})")

            if (not risk_resized) and (not vol_seeded) and (not tp_corrected):
                stats['rows_skipped'] += 1
                print(f"  │    ℹ️ no change")
                continue

            # ---------- 4. MARGIN GUARD (financial volume only) ----------
            final_vol = cur_vol
            order_for_margin = {"symbol": sym, "entry": entry, "ordertype": ot}
            side = "buy" if is_buy else "sell"
            try:
                valid_vol, margin, free_margin, ok, shrunk, mreason = \
                    _validate_single_order_margin_for_calc(
                        order_for_margin, side, final_vol, buffer_pct=10.0
                    )
                if not ok or valid_vol <= 0:
                    stats['rows_skipped'] += 1
                    print(f"  │    ⚠️ margin check failed ({mreason}) — skipped")
                    continue
                if shrunk:
                    print(f"  │    🔻 margin shrink: {final_vol:.2f} -> {valid_vol:.2f}")
                    final_vol = valid_vol
            except NameError:
                pass

            # ---------- 5. WRITE VOLUME AND TP (when corrected) ----------
            if tp_corrected:
                ok_w, _, msg = db_execute(
                    "UPDATE programme_trades SET volume = %s, takeprofit = %s WHERE id = %s",
                    [round(float(final_vol), 8), float(tp), row_id],
                    timeout=15.0, quiet=True,
                )
            else:
                ok_w, _, msg = db_execute(
                    "UPDATE programme_trades SET volume = %s WHERE id = %s",
                    [round(float(final_vol), 8), row_id],
                    timeout=15.0, quiet=True,
                )
            if not ok_w:
                stats['rows_skipped'] += 1
                stats['errors'].append(f"row {row_id}: volume/TP write failed: {msg}")
                print(f"  │    ✗ volume/TP write failed for row {row_id}: {msg}")
                continue

            stats['rows_calculated'] += 1
            stats['details'].append({
                'row_id': row_id,
                'tradeid': tradeid,
                'symbol': sym,
                'ordertype': ot,
                'final_volume': float(final_vol),
                'takeprofit': float(tp) if tp is not None else None,
                'current_rr': round(current_rr, 6),
                'target_rr': float(target_rr),
                'rr_type': rr_type,
                'tp_corrected': tp_corrected,
                'tp_synthesized': tp_synthesized,
                'risk_resized': risk_resized,
                'volume_seeded': vol_seeded,
                'target_risk': float(target_risk) if target_risk is not None else None,
            })

    # ==================================================================
    # SUMMARY
    # ==================================================================
    print(f"\n{'='*10} 📐 CALCULATE SUMMARY (DB) {'='*10}")
    print(f"  Programmes processed : {stats['programmes_processed']}")
    print(f"  Investors processed  : {stats['investors_processed']}")
    print(f"  Rows seen            : {stats['rows_seen']}")
    print(f"  Rows calculated      : {stats['rows_calculated']}")
    print(f"  Rows synced only     : {stats['rows_synced_only']}")
    print(f"  Rows volume-seeded   : {stats['rows_volume_seeded']}")
    print(f"  Rows risk-resized    : {stats['rows_risk_resized']}")
    print(f"  Rows TP corrected    : {stats['rows_tp_corrected']}")
    print(f"  Rows TP synthesized  : {stats['rows_tp_synthesized']}")
    print(f"  Rows skipped         : {stats['rows_skipped']}")
    print(f"  Errors               : {len(stats['errors'])}")
    for e in stats['errors']:
        print(f"     • {e}")
    print(f"{'='*10} 🏁 COMPLETE {'='*10}\n")

    stats['processing_success'] = True
    return stats

# ==============================================================================
#  CHUNK ORDERS WITH VOLUME BEYOND MAX — DB version
#
#  SEMANTICS
#  ---------
#  Reads programme_trades rows where orderstatus = 'pending' for the investor
#  (VPS-scoped via fetch_vps_investor_jobs) and, for each row:
#
#    * Looks up the symbol's MT5 volume_max / volume_min / volume_step.
#    * IF volume <= volume_max:
#         -> chunked_pieces is forced to 0 (idempotent).
#         -> volume is NEVER touched.
#    * IF volume > volume_max:
#         -> chunked_pieces = ceil(volume / volume_max).
#         -> volume is NEVER touched.
#         -> No new rows are inserted; no rows are deleted.
#         -> The `volume` column keeps the full original volume. The chunk
#            count is the only thing this stage writes.
#
#  WHY CHUNKED_PIECES AND NOT CHILD ROWS?
#  --------------------------------------
#  The downstream placer (place_usd_orders) reads chunked_pieces and
#  iterates the MT5 submission in `chunked_pieces` pieces of ≤ volume_max
#  each. The DB row remains the single source of truth for the trade; the
#  piece count is metadata.
#
#  GUARANTEES
#  ----------
#  * volume is NEVER updated by this stage.
#  * chunked_pieces is the ONLY field written.
#  * Idempotent: safe to run every cycle.
#  * No JSON. No local folders. No MT5 order submission.
# ==============================================================================

def chunk_orders_with_volume_beyond_max_db(inv_db_id=None, jobs=None):
    """
    DB version of chunk_orders_with_volume_beyond_max_local_version.

    INVESTOR-SCOPED, NOT ID-SCOPED:
        Every pending programme_trades row belonging to the investor
        (matched by investor_id + investor_sub_account_id + programmeid,
        and further guarded by userid) is processed. Row `id` is only
        read so we can write back to the correct row. The `tradeid` is
        NOT used as a unique key — multiple rows may share the same
        tradeid (e.g. tree pieces), and ALL of them are chunked.

    For each pending row:
      * Look up the symbol's MT5 volume_max.
      * If volume > volume_max:
            chunked_pieces = ceil(volume / volume_max)
            volume is NEVER touched.
      * If volume <= volume_max:
            chunked_pieces = 0
            volume is NEVER touched.

    Returns a stats dict.
    """
    import math

    print(f"\n{'='*10} 📦 CHUNKING ORDERS WITH VOLUME BEYOND MAX (DB) {'='*10}")

    stats = {
        'investor_id':          inv_db_id if inv_db_id else 'all',
        'investors_processed':  0,
        'rows_seen':            0,
        'rows_chunked':         0,
        'rows_unchanged':       0,
        'rows_skipped':         0,
        'total_pieces_created': 0,
        'total_volume_chunked': 0.0,
        'errors':               [],
        'details':              [],
        'processing_success':   False,
    }

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    def _floor_to_step(value, step):
        if step <= 0:
            return value
        try:
            multiples = math.floor(round(value / step, 10))
            return round(multiples * step, 10)
        except Exception:
            return value

    def _ceil_to_step(value, step):
        if step <= 0:
            return value
        try:
            multiples = math.ceil(round(value / step, 10))
            return round(multiples * step, 10)
        except Exception:
            return value

    _symbol_info_cache = {}

    def _get_symbol_info(sym):
        si = _symbol_info_cache.get(sym)
        if si is None:
            si = mt5_symbol_info(sym)
            if si is None:
                mt5_symbol_select(sym, True)
                si = mt5_symbol_info(sym)
            _symbol_info_cache[sym] = si
        return si

    # ------------------------------------------------------------------
    # enumeration (VPS-scoped) — skipped when jobs= passed in
    # ------------------------------------------------------------------
    if jobs is None:
        print("\n" + "─" * 80)
        print(" 📁 LOADING INVESTOR JOBS FROM DB (VPS-scoped)")
        print("─" * 80)

        local_ip = get_local_ip()
        if not local_ip:
            print(" ⚠️ Could not determine local IP")
            stats['errors'].append('local IP unknown')
            return stats
        print(f" │ Local IP: {local_ip}")

        all_jobs = fetch_vps_investor_jobs(local_ip)
        if not all_jobs:
            print(" │ ℹ️ No active investor jobs found for this VPS")
            stats['processing_success'] = True
            return stats

        if inv_db_id:
            jobs = [j for j in all_jobs if str(j.get('investor_id')) == str(inv_db_id)]
            if not jobs:
                print(f" ⚠️ Investor {inv_db_id} not found in any VPS-scoped job")
                stats['errors'].append(f"Investor {inv_db_id} not found")
                return stats
        else:
            jobs = all_jobs

        print(f" │ Loaded {len(jobs)} investor job(s) from DB")
        print("─" * 80)

    # ------------------------------------------------------------------
    # per-investor processing
    # ------------------------------------------------------------------
    for job in jobs:
        investor_id    = job.get('investor_id')
        sub_account_id = job.get('sub_account_id')
        programme_id   = job.get('programme_id')
        programme_name = job.get('programme_name') or f"programme_{programme_id}"
        programme_userid = resolve_programme_userid(programme_id, job)

        if investor_id is None or sub_account_id is None or programme_id is None or not programme_userid:
            stats['rows_skipped'] += 1
            continue

        print(f"\n{'─'*72}")
        print(f" 📦 Investor {investor_id} (sub {sub_account_id}) | "
              f"programme '{programme_name}' (id={programme_id})")
        print(f"{'─'*72}")

        # Only configured live programme trades inherited by this exact investor
        # subaccount may be chunked. execution_start_date remains the time boundary.
        hub = db_fetch_one(
            f"SELECT execution_start_date FROM harvhub "
            f"WHERE id = {int(investor_id)} AND sub_account_id = {int(sub_account_id)} LIMIT 1",
            None, timeout=10.0, quiet=True,
        ) or {}
        rows, eligibility = fetch_live_investor_programme_trades(
            programme_id, programme_userid, investor_id, sub_account_id,
            _raw_first_key(hub, 'execution_start_date'), pending_only=True,
        )
        if eligibility.get('reason'):
            stats['errors'].append(
                f"investor={investor_id}, programme={programme_id}: "
                f"live-trade eligibility blocked: {eligibility['reason']}"
            )

        stats['rows_seen'] += len(rows)
        stats['investors_processed'] += 1

        if not rows:
            print(f"  │ ℹ️ No pending rows for this investor")
            continue

        print(f"  │ Pending rows: {len(rows)}")

        investor_chunked = 0
        investor_unchanged = 0
        investor_skipped = 0
        investor_pieces = 0
        investor_volume = 0.0

        for r in rows:
            row_id    = _coerce_int(_raw_first_key(r, 'id'))
            tradeid   = _coerce_int(_raw_first_key(r, 'tradeid'))
            sym       = (_raw_first_key(r, 'symbol') or '').strip()
            ot        = (_raw_first_key(r, 'ordertype') or '').strip().lower()
            volume    = _coerce_float(_raw_first_key(r, 'volume'), default=None)
            existing_pieces = _coerce_int(_raw_first_key(r, 'chunked_pieces')) or 0

            if row_id is None:
                stats['rows_skipped'] += 1
                investor_skipped += 1
                continue

            if not sym or volume is None or volume <= 0:
                # Nothing to chunk — mark chunked_pieces = 0
                if existing_pieces != 0:
                    db_execute(
                        "UPDATE programme_trades SET chunked_pieces = 0 WHERE id = %s",
                        [row_id], timeout=10.0, quiet=True,
                    )
                stats['rows_unchanged'] += 1
                investor_unchanged += 1
                continue

            si = _get_symbol_info(sym)
            if si is None:
                print(f"  │ ⚠️ row {row_id}: no symbol_info for {sym} — skipped")
                stats['rows_skipped'] += 1
                investor_skipped += 1
                continue

            try:
                max_volume = float(getattr(si, 'volume_max', 0) or 0)
                min_volume = float(getattr(si, 'volume_min', 0.01) or 0.01)
                volume_step = float(getattr(si, 'volume_step', 0.01) or 0.01)
            except Exception:
                print(f"  │ ⚠️ row {row_id}: cannot read volume limits for {sym} — skipped")
                stats['rows_skipped'] += 1
                investor_skipped += 1
                continue

            if max_volume <= 0:
                # No max limit → never chunk
                if existing_pieces != 0:
                    db_execute(
                        "UPDATE programme_trades SET chunked_pieces = 0 WHERE id = %s",
                        [row_id], timeout=10.0, quiet=True,
                    )
                stats['rows_unchanged'] += 1
                investor_unchanged += 1
                continue

            # ============================================================
            # CASE 1: volume within max → chunked_pieces = 0
            # ============================================================
            if volume <= max_volume + 1e-9:
                if existing_pieces != 0:
                    db_execute(
                        "UPDATE programme_trades SET chunked_pieces = 0 WHERE id = %s",
                        [row_id], timeout=10.0, quiet=True,
                    )
                    print(f"  │ ✓ row {row_id} ({sym}) tradeid={tradeid}: "
                          f"volume {volume:.2f} ≤ max {max_volume:.2f} "
                          f"→ chunked_pieces = 0 (reset from {existing_pieces})")
                else:
                    print(f"  │ − row {row_id} ({sym}) tradeid={tradeid}: "
                          f"volume {volume:.2f} ≤ max {max_volume:.2f} "
                          f"→ chunked_pieces = 0")
                stats['rows_unchanged'] += 1
                investor_unchanged += 1
                continue

            # ============================================================
            # CASE 2: volume exceeds max → chunked_pieces = ceil(volume / max)
            # ============================================================
            needed_pieces = int(math.ceil(round(volume / max_volume, 10)))
            if needed_pieces < 1:
                needed_pieces = 1

            print(f"\n  │ 📦 row {row_id} ({sym} {ot}) tradeid={tradeid}")
            print(f"  │    volume        : {volume:.8f}")
            print(f"  │    volume_max    : {max_volume:.2f}")
            print(f"  │    volume_step   : {volume_step}")
            print(f"  │    volume_min    : {min_volume}")
            print(f"  │    needed pieces : {needed_pieces}")
            print(f"  │    current pieces: {existing_pieces}")

            if needed_pieces == existing_pieces:
                print(f"  │    ℹ️ chunked_pieces already = {existing_pieces} — no change")
                stats['rows_unchanged'] += 1
                investor_unchanged += 1
                continue

            # ---- Only chunked_pieces is written. volume is NEVER touched. ----
            ok, _, msg = db_execute(
                "UPDATE programme_trades SET chunked_pieces = %s WHERE id = %s",
                [int(needed_pieces), row_id],
                timeout=15.0, quiet=True,
            )

            if not ok:
                print(f"  │    ✗ failed to write chunked_pieces: {msg}")
                stats['errors'].append(f"row {row_id}: write failed")
                stats['rows_skipped'] += 1
                investor_skipped += 1
                continue

            stats['rows_chunked'] += 1
            investor_chunked += 1
            stats['total_pieces_created'] += needed_pieces
            investor_pieces += needed_pieces
            stats['total_volume_chunked'] += volume
            investor_volume += volume

            print(f"  │    ✅ chunked_pieces = {needed_pieces} "
                  f"(volume left at {volume:.8f})")

            stats['details'].append({
                'row_id':          row_id,
                'tradeid':         tradeid,
                'symbol':          sym,
                'ordertype':       ot,
                'volume':          volume,
                'volume_max':      max_volume,
                'needed_pieces':   needed_pieces,
                'previous_pieces': existing_pieces,
            })

        print(f"\n  │ Investor summary:")
        print(f"  │   chunked   : {investor_chunked}")
        print(f"  │   unchanged : {investor_unchanged}")
        print(f"  │   skipped   : {investor_skipped}")
        print(f"  │   pieces    : {investor_pieces}")
        print(f"  │   volume    : {investor_volume:.8f}")

    # ------------------------------------------------------------------
    # summary
    # ------------------------------------------------------------------
    print(f"\n{'='*10} 📦 CHUNKING SUMMARY (DB) {'='*10}")
    print(f"  Investors processed  : {stats['investors_processed']}")
    print(f"  Rows seen            : {stats['rows_seen']}")
    print(f"  Rows chunked         : {stats['rows_chunked']}")
    print(f"  Rows unchanged       : {stats['rows_unchanged']}")
    print(f"  Rows skipped         : {stats['rows_skipped']}")
    print(f"  Total pieces created : {stats['total_pieces_created']}")
    print(f"  Total volume chunked : {stats['total_volume_chunked']:.8f}")
    print(f"  Errors               : {len(stats['errors'])}")
    for e in stats['errors']:
        print(f"     • {e}")
    print(f"{'='*10} 🏁 COMPLETE {'='*10}\n")

    stats['processing_success'] = True
    return stats

# ==============================================================================
#  PLACE USD ORDERS — DB version (reconciliation-aware, chunked-pieces, no grid)
#
#  READS:
#    * programme_trades     -> pending rows for this investor
#    * harvhub              -> execution_start_date (for MT5 history anchoring)
#    * accountmanagement    -> default / maximum risk brackets (per programme)
#    * MT5                  -> pending orders / open positions / history deals
#
#  WRITES:
#    * programme_trades     -> orderstatus reflects what was placed at MT5:
#                                 'pending'  → MT5 pending order (limit / stop)
#                                 'running'  → MT5 open position (instant buy/sell)
#                                 'closed'   → row is done (found in MT5 history)
#    * (No JSON. No local files. No grid.)
#
#  ORDERSTATUS vs MT5 RECONCILIATION (BEFORE ANY PLACEMENT)
#  ---------------------------------------------------------
#  For every DB row the placer examines (investor-scoped, ordered by id):
#
#    STEP 1 — DB orderstatus pre-filter
#      * 'closed' or 'running'  → SKIP ENTIRELY (no reconciliation, no placement).
#      * 'pending' or NULL/''   → proceed to MT5 reconciliation.
#
#    STEP 2 — MT5 history deals (magic == tradeid)  ← CHECKED FIRST!
#      * found in MT5 history   → ROW IS CLOSED.
#                                 - Delete any MT5 pending order with the
#                                   same magic (the "bypassed" order) via
#                                   TRADE_ACTION_REMOVE.
#                                 - Force DB orderstatus = 'closed'.
#                                 - SKIP placement.
#
#    STEP 3 — MT5 open positions (magic == tradeid)
#      * found in MT5 positions → ROW IS RUNNING.
#                                 Ensure DB orderstatus = 'running'. SKIP.
#
#    STEP 4 — MT5 pending orders (magic == tradeid)
#      * found in MT5 pending   → ROW IS A PENDING ORDER.
#                                 Ensure DB orderstatus = 'pending'. SKIP.
#
#    STEP 5 — Not found anywhere → PLACE
#      * If orderstatus = 'pending' → PLACE (subject to risk guard + chunks).
#      * If orderstatus = NULL/''   → PLACE (subject to risk guard + chunks).
#      * If orderstatus = 'closed' / 'running' (handled at step 1) → never reach here.
#
#  POST-PLACEMENT DB STATUS (STEP 5)
#  ---------------------------------
#    * If the MT5 type we sent is a PENDING order (buy-limit / sell-limit /
#      buy-stop / sell-stop) → DB orderstatus = 'pending'.
#    * If the MT5 type we sent is an INSTANT MARKET order (buy / sell)
#      → DB orderstatus = 'running'.
#
#  CHUNKING SEMANTICS:
#    * chunked_pieces <= 1 → place ONE order of `volume`.
#    * chunked_pieces  > 1 → place `chunked_pieces` pieces of volume / pieces,
#                            capped by volume_max, floored by volume_min, and
#                            every piece subject to the max-risk guard.
#
#  NO GRID LOGIC. NO JSON. NO LOCAL FOLDERS.
# ==============================================================================

def place_usd_orders_db(inv_db_id=None, jobs=None):
    """
    Reconciliation-aware DB placer.

    Key safety rule enforced here:
    * A tradeid/magic that already appears in MT5 HISTORY (closed) is
      considered DONE. Any live MT5 pending order carrying that same
      magic is a LEAK — it is CANCELLED in MT5 immediately, and the
      DB row is forced to orderstatus='closed'. The row is never
      re-placed.

    Reconciliation order (per row, when DB says pending/NULL):
        STEP 2 — MT5 history deals (magic == tradeid)  → cancel live pending, mark closed, skip.
        STEP 3 — MT5 open positions (magic == tradeid) → mark running, skip.
        STEP 4 — MT5 pending orders (magic == tradeid) → mark pending, skip.
        STEP 5 — Not found anywhere                    → PLACE.

    FAILURE RECORDING:
    * Every failure in STEP 5 (incomplete row, missing symbol_info,
      missing tick, unknown order type, missing entry price, risk-guard
      rejection, MT5 placement error, or partial chunked placement)
      writes:
            orderstatus   = 'failed'
            failure_reason = '<human-readable reason>'
      so the dashboard can categorize and explain the failure.
    * Every success writes:
            orderstatus   = 'pending' (pending order) or 'running' (market)
            failure_reason = NULL
      clearing any stale reason from a previous failed attempt.

    MODIFY-TRADE ENFORCEMENT (STEP 9, runs after STEP 5 for each investor):
    * Any programme_trades row whose modify_trade column equals
      'close-position' triggers a market close of the matching live MT5
      position (magic == tradeid). On success:
            orderstatus = 'closed', failure_reason = NULL, modify_trade = NULL
    * Any programme_trades row whose modify_trade column equals
      'delete-order' triggers a TRADE_ACTION_REMOVE of the matching live
      MT5 pending order (magic == tradeid). On success:
            orderstatus = 'closed', failure_reason = NULL, modify_trade = NULL
    * If NO matching MT5 entity exists, the row is still marked 'closed',
      failure_reason=NULL, and modify_trade is cleared — the request is
      considered satisfied.
    * If a matching MT5 entity WAS found but MT5 rejected the action,
      the row is marked:
            orderstatus = 'failed'
            failure_reason = 'modify_trade_failed: <detail>'
            modify_trade  = KEPT (so the next cycle retries)

    PROGRAMME-LEVEL MIRROR:
    * orderstatus/failure_reason belong to the investor/subaccount row and
      are NOT mirrored to the programme master. A single investor's success
      or failure must not change another investor's placement state.
      The PHP sync currently copies master orderstatus back to investor rows;
      that synchronization rule needs a later PHP-side adjustment.
    """
    from datetime import datetime, timedelta, timezone
    import math

    # ------------------------------------------------------------------
    # LAGOS TIMEZONE
    # ------------------------------------------------------------------
    try:
        from zoneinfo import ZoneInfo
        LAGOS_TZ = ZoneInfo("Africa/Lagos")
        _LAGOS_TZ_SOURCE = "zoneinfo"
    except Exception:
        LAGOS_TZ = timezone(timedelta(hours=1))
        _LAGOS_TZ_SOURCE = "fixed_utc+1"

    def lagos_now():
        return datetime.now(LAGOS_TZ)

    print(f"\n{'='*10} 🚀 PLACE USD ORDERS (DB) {'='*10}")
    print(f" 🕒 Lagos time now: {lagos_now().strftime('%Y-%m-%d %H:%M:%S %Z')} (source: {_LAGOS_TZ_SOURCE})")
    if inv_db_id:
        print(f" 🎯 Target Investor: {inv_db_id}")

    stats = {
        'investor_id':                       inv_db_id if inv_db_id else 'all',
        'investors_processed':               0,
        'investors_skipped':                 0,
        'programmes_processed':              0,
        'rows_seen':                         0,
        'rows_skipped_old_contract':         0,
        'rows_skipped_missing_trade_time':   0,
        'rows_skipped_future_trade_time':    0,
        'eligibility_gate_failures':         0,
        'rows_skipped_closed':               0,
        'rows_skipped_running':              0,
        'rows_marked_pending':               0,
        'rows_marked_running':               0,
        'rows_marked_closed':                0,
        'rows_marked_failed':                0,
        'rows_cleared_failure_reason':       0,
        'rows_skipped_found_pending':        0,
        'rows_closed_and_pending_purged':    0,
        'mt5_pending_purged_count':          0,
        'rows_placed':                       0,
        'rows_skipped':                      0,
        'pieces_placed':                     0,
        'pieces_failed':                     0,
        'orders_rejected_by_risk':           0,
        # ---- MODIFY-TRADE ENFORCEMENT COUNTERS ----
        'modify_close_positions_done':       0,
        'modify_delete_orders_done':         0,
        'modify_requests_failed':            0,
        'modify_requests_cleared_no_mt5_hit':0,
        # ---- LEGACY MIRROR COUNTER (lifecycle status is investor-scoped) ----
        'programme_level_mirrors':           0,
        'errors':                            [],
        'details':                           [],
        'processing_success':                False,
    }

    # ==================================================================
    # SECTION 1 — SYMBOL / MT5 HELPERS
    # ==================================================================
    _symbol_info_cache = {}

    def _get_symbol_info(sym):
        si = _symbol_info_cache.get(sym)
        if si is None:
            si = mt5_symbol_info(sym)
            if si is None:
                mt5_symbol_select(sym, True)
                si = mt5_symbol_info(sym)
            _symbol_info_cache[sym] = si
        return si

    def _get_tick(sym):
        return mt5_symbol_tick(sym)

    def _round_to_step(value, step):
        if step is None or step <= 0:
            return value
        try:
            multiples = round(round(value / step, 10))
            return round(multiples * step, 10)
        except Exception:
            return value

    def _price_digits(si):
        try:
            if si is not None and hasattr(si, 'digits'):
                d = int(si.digits)
                if d >= 0:
                    return d
        except Exception:
            pass
        return 5

    def _broker_min_distance(si):
        try:
            point = float(getattr(si, 'point', 0) or 0)
            stops = float(getattr(si, 'trade_stops_level', 0) or 0)
            return max(0.0, point * stops)
        except Exception:
            return 0.0

    def _prevalidate_order_prices(order_type, mt5_type, entry, sl, tp, tick, si, sym):
        """Return a detailed failure reason, or None when the price set is valid."""
        ot = (order_type or '').strip().lower().replace('_', '-').replace(' ', '-')
        ask = float(getattr(tick, 'ask', 0) or 0)
        bid = float(getattr(tick, 'bid', 0) or 0)
        minimum = _broker_min_distance(si)
        is_buy = mt5_type in (mt5.ORDER_TYPE_BUY, mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_BUY_STOP)
        is_pending = mt5_type in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT,
                                  mt5.ORDER_TYPE_BUY_STOP, mt5.ORDER_TYPE_SELL_STOP)

        if ask <= 0 or bid <= 0:
            return f"Invalid {ot.title()} Entry Price: MT5 returned invalid bid/ask for {sym} (bid={bid}, ask={ask}); trade was not placed."

        if entry is None or float(entry) <= 0:
            label = ot.title() if ot else 'Trade'
            return f"Invalid {label} Entry Price: trade requires a positive signal entry price; got {entry}."

        if is_pending:
            price = float(entry)
            if mt5_type == mt5.ORDER_TYPE_BUY_STOP:
                distance = price - ask
                if distance <= 0:
                    return (f"Invalid BuyStop Entry Price: trade wasn't placed because entry {price} is at/below "
                            f"current ask {ask}; a BuyStop must be above the market and price has moved against the entry.")
            elif mt5_type == mt5.ORDER_TYPE_SELL_STOP:
                distance = bid - price
                if distance <= 0:
                    return (f"Invalid SellStop Entry Price: trade wasn't placed because entry {price} is at/above "
                            f"current bid {bid}; a SellStop must be below the market and price has moved against the entry.")
            elif mt5_type == mt5.ORDER_TYPE_BUY_LIMIT:
                distance = ask - price
                if distance <= 0:
                    return (f"Invalid BuyLimit Entry Price: trade wasn't placed because entry {price} is at/above "
                            f"current ask {ask}; a BuyLimit must be below the market and price has moved against the entry.")
            else:  # SELL_LIMIT
                distance = price - bid
                if distance <= 0:
                    return (f"Invalid SellLimit Entry Price: trade wasn't placed because entry {price} is at/below "
                            f"current bid {bid}; a SellLimit must be above the market and price has moved against the entry.")
            if minimum > 0 and distance < minimum:
                return (f"Entry too close to current price: {ot.title()} entry {price} is only {distance:g} "
                        f"from market; broker minimum stop distance for {sym} is {minimum:g}.")
            reference = price
        else:
            reference = ask if is_buy else bid
            # Stops on a market BUY are checked against Bid; market SELL
            # stops are checked against Ask (MT5's close-side quote).
            protection_reference = bid if is_buy else ask
            # A market order is only valid while the signal's requested entry
            # has not been crossed adversely. More favourable price movement is allowed.
            if entry is not None and float(entry) > 0:
                signal_entry = float(entry)
                if is_buy and ask > signal_entry:
                    return (f"Invalid Buy Entry Price: trade wasn't placed because current ask {ask} is above "
                            f"signal entry {signal_entry}; price has moved against the buy entry.")
                if not is_buy and bid < signal_entry:
                    return (f"Invalid Sell Entry Price: trade wasn't placed because current bid {bid} is below "
                            f"signal entry {signal_entry}; price has moved against the sell entry.")

        # Stop-loss must be on the loss side of entry/market, and must respect
        # the broker's minimum stop distance when a positive value is supplied.
        if not is_pending:
            stop_reference = protection_reference
        else:
            stop_reference = reference
        if sl is not None and float(sl) > 0:
            sl_value = float(sl)
            sl_distance = (stop_reference - sl_value) if is_buy else (sl_value - stop_reference)
            if sl_distance <= 0:
                return (f"Invalid StopLoss Price: {ot.title()} stop-loss {sl_value} is on the wrong side of "
                        f"entry/reference {stop_reference}; {'BUY' if is_buy else 'SELL'} protection is invalid.")
            if minimum > 0 and sl_distance < minimum:
                return (f"Stoploss too close to entry: {ot.title()} stop-loss {sl_value} is {sl_distance:g} "
                        f"from reference {stop_reference}; broker minimum for {sym} is {minimum:g}.")

        if tp is not None and float(tp) > 0:
            tp_value = float(tp)
            tp_distance = (tp_value - stop_reference) if is_buy else (stop_reference - tp_value)
            if tp_distance <= 0:
                return (f"Invalid Take Profit Price: {ot.title()} take-profit {tp_value} is on the wrong side of "
                        f"entry/reference {stop_reference}; {'BUY' if is_buy else 'SELL'} target is invalid.")
            if minimum > 0 and tp_distance < minimum:
                return (f"Take profit too close to entry: {ot.title()} take-profit {tp_value} is {tp_distance:g} "
                        f"from reference {stop_reference}; broker minimum for {sym} is {minimum:g}.")
        return None

    def _describe_mt5_failure(result, order_type, sym, entry, sl, tp, piece_i, num_pieces):
        comment = (getattr(result, 'comment', '') or '').strip() if result is not None else ''
        retcode = getattr(result, 'retcode', None) if result is not None else None
        try:
            code = int(retcode)
        except Exception:
            code = None
        detail = f"MT5 retcode={retcode}, comment='{comment or 'no comment'}'"
        suffix = f" symbol={sym}, entry={entry}, SL={sl}, TP={tp}, piece={piece_i}/{num_pieces}"
        if code == 10018 or 'market closed' in comment.lower():
            return f"Market Closed: MT5 rejected the {order_type} order for {sym}; trading session is closed. {detail}{suffix}"
        if code == 10015 or 'invalid price' in comment.lower():
            label = (order_type or 'trade').replace('-', ' ').title()
            return (f"Invalid {label} Entry Price: MT5 rejected the entry because the market price changed or "
                    f"moved against the requested entry. {detail}{suffix}")
        if code == 10016 or 'invalid stops' in comment.lower():
            return (f"Invalid StopLoss/Take Profit Price: MT5 rejected SL/TP for {sym}; the stop/target may be "
                    f"on the wrong side of entry or too close to current price/entry. {detail}{suffix}")
        if code in (10004, 10020, 10021) or any(x in comment.lower() for x in ('requote', 'price changed', 'off quotes')):
            label = (order_type or 'trade').replace('-', ' ').title()
            return (f"Invalid {label} Entry Price: price changed, requote occurred, or quotes became unavailable; "
                    f"the price may have moved against the entry. {detail}{suffix}")
        if code == 10017 or 'trade disabled' in comment.lower():
            return f"Trading Disabled: MT5/broker disabled trading for {sym}. {detail}{suffix}"
        if result is None:
            return f"MT5 order placement failed: no response. last_error={mt5_last_error()}{suffix}"
        return f"MT5 order placement failed: {detail}{suffix}"

    def _order_type_to_mt5(order_type_str, entry=None, ask=None, bid=None):
        ot = (order_type_str or '').strip().lower().replace('_', '-').replace(' ', '-')
        mapping = {
            'buy':        mt5.ORDER_TYPE_BUY,
            'sell':       mt5.ORDER_TYPE_SELL,
            'buy-limit':  mt5.ORDER_TYPE_BUY_LIMIT,
            'sell-limit': mt5.ORDER_TYPE_SELL_LIMIT,
            'buy-stop':   mt5.ORDER_TYPE_BUY_STOP,
            'sell-stop':  mt5.ORDER_TYPE_SELL_STOP,
        }
        mt5_type = mapping.get(ot)
        if mt5_type is None:
            return None, False, f"Unsupported MT5 order type '{order_type_str}'"
        return mt5_type, False, 'order type mapped without market conversion'

    def _get_supported_filling_modes(si, is_pending_order=False):
        if si is None:
            return [mt5.ORDER_FILLING_IOC, mt5.ORDER_FILLING_FOK, mt5.ORDER_FILLING_RETURN]
        try:
            execution_mode = si.trade_exemode
            filling_mode_mask = si.filling_mode
        except Exception:
            return [mt5.ORDER_FILLING_IOC, mt5.ORDER_FILLING_FOK, mt5.ORDER_FILLING_RETURN]

        SYMBOL_FILLING_FOK = 0x01
        SYMBOL_FILLING_IOC = 0x02
        SYMBOL_FILLING_BOC = 0x04

        if is_pending_order:
            return [mt5.ORDER_FILLING_RETURN]

        modes = []
        try:
            if execution_mode == mt5.SYMBOL_TRADE_EXECUTION_EXCHANGE:
                if filling_mode_mask & SYMBOL_FILLING_BOC:
                    modes.append(mt5.ORDER_FILLING_BOC)
                if filling_mode_mask & SYMBOL_FILLING_IOC:
                    modes.append(mt5.ORDER_FILLING_IOC)
                if filling_mode_mask & SYMBOL_FILLING_FOK:
                    modes.append(mt5.ORDER_FILLING_FOK)
                modes.append(mt5.ORDER_FILLING_RETURN)
            elif execution_mode == mt5.SYMBOL_TRADE_EXECUTION_MARKET:
                if filling_mode_mask & SYMBOL_FILLING_IOC:
                    modes.append(mt5.ORDER_FILLING_IOC)
                modes.append(mt5.ORDER_FILLING_RETURN)
                if filling_mode_mask & SYMBOL_FILLING_FOK:
                    modes.append(mt5.ORDER_FILLING_FOK)
            elif execution_mode in (mt5.SYMBOL_TRADE_EXECUTION_INSTANT,
                                    mt5.SYMBOL_TRADE_EXECUTION_REQUEST):
                if filling_mode_mask & SYMBOL_FILLING_FOK:
                    modes.append(mt5.ORDER_FILLING_FOK)
                modes.append(mt5.ORDER_FILLING_RETURN)
            else:
                modes = [mt5.ORDER_FILLING_IOC, mt5.ORDER_FILLING_FOK, mt5.ORDER_FILLING_RETURN]
        except Exception:
            modes = [mt5.ORDER_FILLING_IOC, mt5.ORDER_FILLING_FOK, mt5.ORDER_FILLING_RETURN]

        uniq = []
        for m in modes:
            if m not in uniq:
                uniq.append(m)
        return uniq

    def _send_order_with_auto_filling(request_template, filling_modes, is_pending_order):
        last_err = None
        last_result = None
        last_mode = None
        invalid_fill = getattr(mt5, 'TRADE_RETCODE_INVALID_FILL', 10030)
        for filling_mode in filling_modes:
            req = dict(request_template)
            req["type_filling"] = filling_mode
            req["action"] = (mt5.TRADE_ACTION_PENDING if is_pending_order
                             else mt5.TRADE_ACTION_DEAL)
            res = mt5_order_send(req)
            last_result, last_mode = res, filling_mode
            if res is not None and res.retcode == mt5.TRADE_RETCODE_DONE:
                return res, filling_mode, None
            if res is not None and getattr(res, 'retcode', None) == invalid_fill:
                last_err = f"unsupported filling mode {filling_mode}: {getattr(res, 'comment', '')}"
                continue
            if res is not None:
                return res, filling_mode, f"retcode={res.retcode} comment={getattr(res, 'comment', '')}"
            last_err = f"no_response last_error={mt5_last_error()}"
        if last_result is not None:
            return last_result, last_mode, last_err or (
                f"retcode={getattr(last_result, 'retcode', None)} "
                f"comment={getattr(last_result, 'comment', '')}"
            )
        return None, None, (last_err or "all filling modes failed")

    # ==================================================================
    # SECTION 2 — RECONCILIATION HELPERS (magic == tradeid)
    # ==================================================================
    def _row_is_in_mt5_history(tradeid, execution_start_date):
        if not tradeid:
            return False
        start_dt = _parse_lagos_datetime(execution_start_date)
        if start_dt is None:
            return False
        start_naive = start_dt
        end_naive = lagos_now().replace(tzinfo=None)
        try:
            deals = mt5_history_deals(start_naive, end_naive) or []
        except Exception:
            return False
        for deal in deals:
            try:
                if int(getattr(deal, 'magic', 0) or 0) == int(tradeid):
                    return True
            except Exception:
                continue
        return False

    def _cancel_mt5_pending_by_magic(tradeid):
        cancelled_tickets = []
        if not tradeid:
            return cancelled_tickets
        try:
            live_pending = mt5_orders_get() or []
        except Exception:
            return cancelled_tickets

        for o in live_pending:
            try:
                if int(getattr(o, 'magic', 0) or 0) != int(tradeid):
                    continue
            except Exception:
                continue

            ticket = getattr(o, 'ticket', None)
            if ticket is None:
                continue

            print(f"  │    🗑️ Cancelling leaked MT5 pending #{ticket} "
                  f"(magic={tradeid}, symbol={getattr(o, 'symbol', '?')})")
            try:
                req = {"action": mt5.TRADE_ACTION_REMOVE, "order": int(ticket)}
                res = mt5_order_send(req)
                if res is not None and res.retcode == mt5.TRADE_RETCODE_DONE:
                    cancelled_tickets.append(int(ticket))
                    print(f"  │       ✅ MT5 pending #{ticket} cancelled")
                else:
                    err = (getattr(res, 'comment', '') if res is not None
                           else f"no_response last_error={mt5_last_error()}")
                    print(f"  │       ✗ cancel #{ticket} failed — {err}")
            except Exception as e:
                print(f"  │       ✗ cancel #{ticket} raised: {e}")

        return cancelled_tickets

    def _set_orderstatus(row_id, new_status):
        ok, _, msg = db_execute(
            "UPDATE programme_trades SET orderstatus = %s WHERE id = %s",
            [new_status, row_id], timeout=10.0, quiet=True,
        )
        return ok, msg

    # ------------------------------------------------------------------
    # PROGRAMME-LEVEL MIRROR HELPER
    # ------------------------------------------------------------------
    def _mirror_to_programme_level(tradeid, programme_id, userid,
                                   new_status=None, clear_modify=False,
                                   failure_reason=None):
        """
        Mirror a status change from an INVESTOR row up to the matching
        PROGRAMME-LEVEL row (investor_id IS NULL, same programmeid +
        tradeid + userid).

        This is what stops syncProgrammeTradesToInvestors() in the PHP
        from reverting the investor row on the next live poll.

        Rules:
          * new_status is None  → don't touch orderstatus
          * new_status is set   → set orderstatus = new_status
                                  and failure_reason = failure_reason or NULL
          * clear_modify        → also SET modify_trade = NULL

        Returns (ok, affected_rows, msg).
        """
        if tradeid is None or programme_id is None or userid is None:
            return False, 0, "missing_ctx"

        sets = []
        params = []

        if new_status is not None:
            sets.append("orderstatus = %s")
            params.append(new_status)
            sets.append("failure_reason = %s")
            params.append(failure_reason)  # None clears it

        if clear_modify:
            sets.append("modify_trade = NULL")

        if not sets:
            return False, 0, "nothing_to_do"

        params.extend([programme_id, int(tradeid), userid])

        sql = (
            "UPDATE programme_trades "
            f"   SET {', '.join(sets)} "
            " WHERE programmeid = %s "
            "   AND tradeid = %s "
            "   AND userid = %s "
            "   AND investor_id IS NULL"
        )
        ok, affected, msg = db_execute(
            sql, params, timeout=10.0, quiet=True,
        )
        if ok and affected > 0:
            stats['programme_level_mirrors'] += affected
        return ok, affected, msg

    def _set_orderstatus_and_reason(row_id, new_status, reason=None,
                                    mirror_ctx=None):
        """Update the investor/subaccount row only.

        Trade placement and its failures are investor-specific. Writing the
        status to the programme master would cause PHP synchronization to
        copy one investor's running/failed state to every other investor.
        The optional mirror_ctx parameter is retained for call compatibility;
        it is deliberately not used for order lifecycle statuses.
        """
        if str(new_status).strip().lower() == 'failed':
            reason_text = str(reason or 'unknown_failure').strip()[:250] or 'unknown_failure'
            ok, _, msg = db_execute(
                "UPDATE programme_trades "
                "   SET orderstatus = %s, failure_reason = %s "
                " WHERE id = %s",
                ['failed', reason_text, row_id], timeout=10.0, quiet=True,
            )
            return ok, msg

        ok, _, msg = db_execute(
            "UPDATE programme_trades "
            "   SET orderstatus = %s, failure_reason = NULL "
            " WHERE id = %s",
            [new_status, row_id], timeout=10.0, quiet=True,
        )
        return ok, msg

    def _mark_row_failed(row_id, reason, tradeid=None, sym=None, ot=None,
                         mirror_ctx=None):
        """Convenience wrapper for the failure path."""
        ok, msg = _set_orderstatus_and_reason(
            row_id, 'failed', reason, mirror_ctx=mirror_ctx
        )
        if ok:
            stats['rows_marked_failed'] += 1
            print(f"  │    ⚠️ row {row_id}: orderstatus='failed'  reason='{reason}'")
        else:
            print(f"  │    ✗ row {row_id}: failed to write failure status — {msg}")
            stats['errors'].append(f"row {row_id}: mark failed failed — {msg}")
        stats['details'].append({
            'row_id': row_id,
            'tradeid': tradeid,
            'symbol': sym,
            'ordertype': ot,
            'action': 'marked_failed',
            'failure_reason': reason,
        })
        return ok

    # ==================================================================
    # SECTION 3 — RISK GUARD
    # ==================================================================
    def _order_risk_in_dollars(entry, sl, symbol, volume, is_buy):
        if entry is None or sl is None or sl == 0:
            return 0.0
        try:
            calc_type = mt5.ORDER_TYPE_BUY if is_buy else mt5.ORDER_TYPE_SELL
            profit = mt5_order_calc_profit(calc_type, symbol, float(volume),
                                           float(entry), float(sl))
            return abs(float(profit)) if profit else 0.0
        except Exception:
            return 0.0

    def _normalize_mt5_direction(order_or_position):
        t = getattr(order_or_position, 'type', None)
        if t in (mt5.ORDER_TYPE_BUY, mt5.ORDER_TYPE_BUY_LIMIT,
                 mt5.ORDER_TYPE_BUY_STOP, mt5.ORDER_TYPE_BUY_STOP_LIMIT):
            return 'buy'
        if t in (mt5.ORDER_TYPE_SELL, mt5.ORDER_TYPE_SELL_LIMIT,
                 mt5.ORDER_TYPE_SELL_STOP, mt5.ORDER_TYPE_SELL_STOP_LIMIT):
            return 'sell'
        return None

    def _collect_existing_risk_for_entry(symbol, direction, entry):
        total = 0.0
        try:
            pending = mt5_orders_get(symbol=symbol) or []
        except Exception:
            pending = []
        try:
            positions = mt5_positions_get(symbol=symbol) or []
        except Exception:
            positions = []

        for o in pending:
            if _normalize_mt5_direction(o) != direction:
                continue
            try:
                if abs(float(o.price_open) - float(entry)) > 1e-6:
                    continue
            except Exception:
                continue
            vol = float(getattr(o, 'volume_initial', 0) or getattr(o, 'volume_current', 0) or 0)
            sl = getattr(o, 'sl', None)
            total += _order_risk_in_dollars(o.price_open, sl, symbol, vol, direction == 'buy')

        for p in positions:
            if _normalize_mt5_direction(p) != direction:
                continue
            try:
                if abs(float(p.price_open) - float(entry)) > 1e-6:
                    continue
            except Exception:
                continue
            vol = float(getattr(p, 'volume', 0) or 0)
            sl = getattr(p, 'sl', None)
            total += _order_risk_in_dollars(p.price_open, sl, symbol, vol, direction == 'buy')

        return total

    # ==================================================================
    # SECTION 4 — PROGRAMME RISK CONFIG
    # ==================================================================
    _risk_cfg_cache = {}

    def _load_programme_risk_cfg(programme_id):
        cfg = _risk_cfg_cache.get(programme_id)
        if cfg is not None:
            return cfg

        rows = db_fetch_all(
            """
            SELECT from_balance_range, to_balance_range, risk_amount
              FROM accountmanagement_balance_default_risk
             WHERE programme_id = %s
             ORDER BY from_balance_range ASC, id ASC
            """,
            [programme_id], timeout=10.0, quiet=True,
        )
        brackets = []
        for r in rows:
            try:
                lo = _coerce_float(_raw_first_key(r, 'from_balance_range'), default=None)
                hi = _coerce_float(_raw_first_key(r, 'to_balance_range'),   default=None)
                rk = _coerce_float(_raw_first_key(r, 'risk_amount'),        default=None)
            except Exception:
                continue
            if lo is None or rk is None:
                continue
            if hi is None or hi <= 0:
                hi = float('inf')
            brackets.append({'from': float(lo), 'to': float(hi), 'risk': float(rk)})

        max_rows = db_fetch_all(
            """
            SELECT from_balance_range, to_balance_range, risk_amount
              FROM accountmanagement_balance_maximum_risk
             WHERE programme_id = %s
             ORDER BY from_balance_range ASC, id ASC
            """,
            [programme_id], timeout=10.0, quiet=True,
        )
        max_brackets = []
        for r in max_rows:
            try:
                lo = _coerce_float(_raw_first_key(r, 'from_balance_range'), default=None)
                hi = _coerce_float(_raw_first_key(r, 'to_balance_range'),   default=None)
                rk = _coerce_float(_raw_first_key(r, 'risk_amount'),        default=None)
            except Exception:
                continue
            if lo is None or rk is None:
                continue
            if hi is None or hi <= 0:
                hi = float('inf')
            max_brackets.append({'from': float(lo), 'to': float(hi), 'risk': float(rk)})

        def _pick(brackets, balance, fallback=None):
            if not brackets or balance is None:
                return fallback
            for b in brackets:
                if b['from'] <= balance <= b['to']:
                    return b['risk']
            below = [b for b in brackets if b['from'] <= balance]
            if below:
                return below[-1]['risk']
            return brackets[0]['risk']

        cfg = {
            'default_brackets': brackets,
            'max_brackets':     max_brackets,
            'pick_default':     lambda bal: _pick(brackets, bal),
            'pick_max':         lambda bal: _pick(max_brackets, bal) or _pick(brackets, bal),
        }
        _risk_cfg_cache[programme_id] = cfg
        return cfg

    def _resolve_risk_for_balance(programme_id, balance):
        cfg = _load_programme_risk_cfg(programme_id)
        default_r = cfg['pick_default'](balance)
        maximum_r = cfg['pick_max'](balance)
        if default_r is None and maximum_r is None:
            return None, None
        if default_r is None:
            default_r = maximum_r
        if maximum_r is None:
            maximum_r = default_r
        if maximum_r < default_r:
            maximum_r = default_r
        return float(default_r), float(maximum_r)

    # ==================================================================
    # SECTION 4b — MODIFY-TRADE ENFORCEMENT HELPERS
    # ==================================================================
    def _fetch_modify_trade_requests(investor_id, sub_account_id, programme_id, programme_userid):
        if not programme_userid:
            return []
        return db_fetch_all(
            """
            SELECT id, tradeid, symbol, ordertype, orderstatus,
                   entry, stoploss, takeprofit, volume, modify_trade
              FROM programme_trades
             WHERE investor_id = %s
               AND investor_sub_account_id = %s
               AND programmeid = %s
               AND userid = %s
               AND modify_trade IN ('close-position', 'delete-order')
             ORDER BY id ASC
            """,
            [investor_id, sub_account_id, programme_id, programme_userid],
            timeout=15.0, quiet=True,
        )

    def _close_live_position_by_magic(tradeid):
        """
        Returns (ok_any, had_matching_entity, tickets_closed, last_error).
        """
        closed_tickets = []
        last_error = None
        if not tradeid:
            return False, False, closed_tickets, "no_tradeid"

        try:
            positions = mt5_positions_get() or []
        except Exception as e:
            return False, False, closed_tickets, f"positions_get failed: {e}"

        matching = []
        for p in positions:
            try:
                if int(getattr(p, 'magic', 0) or 0) == int(tradeid):
                    matching.append(p)
            except Exception:
                continue

        if not matching:
            return False, False, closed_tickets, None

        for pos in matching:
            ticket = getattr(pos, 'ticket', None)
            sym    = getattr(pos, 'symbol', None)
            vol    = float(getattr(pos, 'volume', 0) or 0)
            ptype  = int(getattr(pos, 'type', -1))

            if ticket is None or not sym or vol <= 0:
                last_error = f"missing_fields on ticket={ticket}"
                continue

            tick = _get_tick(sym)
            if tick is None:
                last_error = f"no_tick:{sym}"
                continue

            if ptype == 0:
                close_type = mt5.ORDER_TYPE_SELL
                close_price = tick.bid
            else:
                close_type = mt5.ORDER_TYPE_BUY
                close_price = tick.ask

            si = _get_symbol_info(sym)
            digits = _price_digits(si)

            attempts = [
                {
                    "action":     mt5.TRADE_ACTION_DEAL,
                    "symbol":     sym,
                    "volume":     float(vol),
                    "type":       close_type,
                    "position":   int(ticket),
                    "price":      round(float(close_price), digits),
                    "deviation":  20,
                    "magic":      int(tradeid),
                    "comment":    "MODIFY-CLOSE",
                    "type_time":  mt5.ORDER_TIME_GTC,
                    "type_filling": mt5.ORDER_FILLING_IOC,
                },
                {
                    "action":     mt5.TRADE_ACTION_DEAL,
                    "symbol":     sym,
                    "volume":     float(vol),
                    "type":       close_type,
                    "position":   int(ticket),
                    "price":      round(float(close_price), digits),
                    "deviation":  50,
                    "magic":      int(tradeid),
                    "comment":    "MODIFY-CLOSE-RETRY",
                    "type_filling": mt5.ORDER_FILLING_RETURN,
                },
            ]

            done = False
            for req in attempts:
                try:
                    res = mt5_order_send(req)
                    if res is not None and res.retcode == mt5.TRADE_RETCODE_DONE:
                        closed_tickets.append(int(ticket))
                        print(f"  │       ✅ Closed MT5 position #{ticket} "
                              f"({sym} {vol} lots) for magic={tradeid}")
                        done = True
                        break
                    if res is not None:
                        last_error = f"retcode={res.retcode} comment={getattr(res,'comment','')}"
                    else:
                        last_error = f"no_response last_error={mt5_last_error()}"
                except Exception as e:
                    last_error = f"exception: {e}"
            if not done:
                print(f"  │       ✗ close #{ticket} failed — {last_error}")

        return bool(closed_tickets), True, closed_tickets, last_error

    def _delete_live_pending_by_magic(tradeid):
        """
        Returns (ok_any, had_matching_entity, tickets_removed, last_error).
        """
        removed_tickets = []
        last_error = None
        if not tradeid:
            return False, False, removed_tickets, "no_tradeid"

        try:
            pending = mt5_orders_get() or []
        except Exception as e:
            return False, False, removed_tickets, f"orders_get failed: {e}"

        matching = []
        for o in pending:
            try:
                if int(getattr(o, 'magic', 0) or 0) == int(tradeid):
                    matching.append(o)
            except Exception:
                continue

        if not matching:
            return False, False, removed_tickets, None

        for order in matching:
            ticket = getattr(order, 'ticket', None)
            if ticket is None:
                last_error = "no_ticket"
                continue
            try:
                req = {"action": mt5.TRADE_ACTION_REMOVE, "order": int(ticket)}
                res = mt5_order_send(req)
                if res is not None and res.retcode == mt5.TRADE_RETCODE_DONE:
                    removed_tickets.append(int(ticket))
                    print(f"  │       ✅ Deleted MT5 pending #{ticket} "
                          f"({getattr(order,'symbol','?')}) for magic={tradeid}")
                else:
                    last_error = (getattr(res, 'comment', '') if res is not None
                                  else f"no_response last_error={mt5_last_error()}")
                    print(f"  │       ✗ delete #{ticket} failed — {last_error}")
            except Exception as e:
                last_error = f"exception: {e}"
                print(f"  │       ✗ delete #{ticket} raised: {e}")

        return bool(removed_tickets), True, removed_tickets, last_error

    def _clear_modify_trade(row_id):
        ok, _, msg = db_execute(
            "UPDATE programme_trades SET modify_trade = NULL WHERE id = %s",
            [row_id], timeout=10.0, quiet=True,
        )
        return ok, msg

    def _enforce_modify_trade_requests(investor_id, sub_account_id,
                                       programme_id, programme_name, programme_userid):
        """
        STEP 9 — runs AFTER STEP 5 for each investor.

        Investor row status changes are kept subaccount-specific. The
        explicit programme-level call below only clears the shared
        modify_trade command after it has been consumed.
        """
        requests = _fetch_modify_trade_requests(
            investor_id, sub_account_id, programme_id, programme_userid)

        if not requests:
            return

        print(f"\n  🛠️ STEP 9 — MODIFY-TRADE ENFORCEMENT: "
              f"{len(requests)} request(s) for investor {investor_id}")

        for req in requests:
            row_id   = _coerce_int(_raw_first_key(req, 'id'))
            tradeid  = _coerce_int(_raw_first_key(req, 'tradeid'))
            sym      = (_raw_first_key(req, 'symbol') or '').strip()
            mode     = (_raw_first_key(req, 'modify_trade') or '').strip().lower()

            if row_id is None or tradeid is None:
                continue

            mirror_ctx = {
                'programme_id': programme_id,
                'userid':       programme_userid,
                'tradeid':      tradeid,
            }

            print(f"\n  │ 🛠️ row {row_id} (tradeid={tradeid}, {sym}) — "
                  f"modify_trade='{mode}'")

            if mode == 'close-position':
                ok_any, had_entity, tickets, err = _close_live_position_by_magic(tradeid)

                if ok_any:
                    ok1, msg1 = _set_orderstatus_and_reason(
                        row_id, 'closed', mirror_ctx=mirror_ctx)
                    ok2, msg2 = _clear_modify_trade(row_id)
                    _mirror_to_programme_level(
                        tradeid=tradeid, programme_id=programme_id,
                        userid=programme_userid, clear_modify=True)
                    if ok1 and ok2:
                        stats['modify_close_positions_done'] += 1
                        stats['rows_marked_closed'] += 1
                        stats['rows_cleared_failure_reason'] += 1
                        print(f"  │    🏁 row {row_id}: MT5 position(s) closed "
                              f"({tickets}) → orderstatus='closed', modify_trade cleared "
                              f"(mirrored to programme level)")
                    else:
                        stats['errors'].append(
                            f"row {row_id}: close-position DB write failed "
                            f"({msg1} / {msg2})")
                        print(f"  │    ⚠️ row {row_id}: MT5 closed but DB update "
                              f"failed — {msg1} / {msg2}")
                elif had_entity:
                    # MT5 entity existed but MT5 rejected the action.
                    reason = f'modify_trade_failed: close-position — {err}'
                    _set_orderstatus_and_reason(
                        row_id, 'failed', reason=reason, mirror_ctx=mirror_ctx)
                    stats['modify_requests_failed'] += 1
                    print(f"  │    ⚠️ row {row_id}: MT5 refused the close — "
                          f"marked 'failed', modify_trade KEPT for retry")
                else:
                    # Nothing at broker → satisfy the request
                    ok1, msg1 = _set_orderstatus_and_reason(
                        row_id, 'closed', mirror_ctx=mirror_ctx)
                    ok2, msg2 = _clear_modify_trade(row_id)
                    _mirror_to_programme_level(
                        tradeid=tradeid, programme_id=programme_id,
                        userid=programme_userid, clear_modify=True)
                    if ok1 and ok2:
                        stats['modify_requests_cleared_no_mt5_hit'] += 1
                        stats['rows_marked_closed'] += 1
                        stats['rows_cleared_failure_reason'] += 1
                        print(f"  │    ℹ️ row {row_id}: no live MT5 position for "
                              f"magic={tradeid} → marked 'closed' anyway, "
                              f"modify_trade cleared (mirrored)")
                    else:
                        stats['errors'].append(
                            f"row {row_id}: close-position no-hit DB write failed "
                            f"({msg1} / {msg2})")

            elif mode == 'delete-order':
                ok_any, had_entity, tickets, err = _delete_live_pending_by_magic(tradeid)

                if ok_any:
                    ok1, msg1 = _set_orderstatus_and_reason(
                        row_id, 'closed', mirror_ctx=mirror_ctx)
                    ok2, msg2 = _clear_modify_trade(row_id)
                    _mirror_to_programme_level(
                        tradeid=tradeid, programme_id=programme_id,
                        userid=programme_userid, clear_modify=True)
                    if ok1 and ok2:
                        stats['modify_delete_orders_done'] += 1
                        stats['rows_marked_closed'] += 1
                        stats['rows_cleared_failure_reason'] += 1
                        print(f"  │    🏁 row {row_id}: MT5 pending removed "
                              f"({tickets}) → orderstatus='closed', modify_trade cleared "
                              f"(mirrored to programme level)")
                    else:
                        stats['errors'].append(
                            f"row {row_id}: delete-order DB write failed "
                            f"({msg1} / {msg2})")
                        print(f"  │    ⚠️ row {row_id}: MT5 removed but DB update "
                              f"failed — {msg1} / {msg2}")
                elif had_entity:
                    reason = f'modify_trade_failed: delete-order — {err}'
                    _set_orderstatus_and_reason(
                        row_id, 'failed', reason=reason, mirror_ctx=mirror_ctx)
                    stats['modify_requests_failed'] += 1
                    print(f"  │    ⚠️ row {row_id}: MT5 refused the delete — "
                          f"marked 'failed', modify_trade KEPT for retry")
                else:
                    ok1, msg1 = _set_orderstatus_and_reason(
                        row_id, 'closed', mirror_ctx=mirror_ctx)
                    ok2, msg2 = _clear_modify_trade(row_id)
                    _mirror_to_programme_level(
                        tradeid=tradeid, programme_id=programme_id,
                        userid=programme_userid, clear_modify=True)
                    if ok1 and ok2:
                        stats['modify_requests_cleared_no_mt5_hit'] += 1
                        stats['rows_marked_closed'] += 1
                        stats['rows_cleared_failure_reason'] += 1
                        print(f"  │    ℹ️ row {row_id}: no live MT5 pending for "
                              f"magic={tradeid} → marked 'closed' anyway, "
                              f"modify_trade cleared (mirrored)")
                    else:
                        stats['errors'].append(
                            f"row {row_id}: delete-order no-hit DB write failed "
                            f"({msg1} / {msg2})")

            else:
                continue

            stats['details'].append({
                'row_id': row_id,
                'tradeid': tradeid,
                'symbol': sym,
                'modify_trade': mode,
                'action': f'modify_{mode}_enforced',
            })

    # ==================================================================
    # SECTION 5 — MT5 CONNECTION CHECK
    # ==================================================================
    acc = mt5_account_info()
    if acc is None:
        print(f"  ⚠️ MT5 not connected — cannot place orders")
        stats['errors'].append('MT5 not connected')
        return stats

    try:
        current_balance = float(acc.balance)
    except Exception:
        current_balance = 0.0
    print(f"  💰 Current MT5 balance: ${current_balance:.2f}")

    # ==================================================================
    # SECTION 6 — ENUMERATION (VPS-scoped) — skipped when jobs= passed in
    # ==================================================================
    if jobs is None:
        print("\n" + "─"*80)
        print(" 📁 LOADING INVESTOR JOBS FROM DB (VPS-scoped)")
        print("─"*80)

        local_ip = get_local_ip()
        if not local_ip:
            print(" ⚠️ Could not determine local IP")
            stats['errors'].append('local IP unknown')
            return stats
        print(f" │ Local IP: {local_ip}")

        all_jobs = fetch_vps_investor_jobs(local_ip)
        if not all_jobs:
            print(" │ ℹ️ No active investor jobs found for this VPS")
            stats['processing_success'] = True
            return stats

        if inv_db_id:
            jobs = [j for j in all_jobs if str(j.get('investor_id')) == str(inv_db_id)]
            if not jobs:
                print(f" ⚠️ Investor {inv_db_id} not found in any VPS-scoped job")
                stats['errors'].append(f"Investor {inv_db_id} not found")
                return stats
        else:
            jobs = all_jobs

        print(f" │ Loaded {len(jobs)} investor job(s) from DB")
        print("─"*80)

    # ==================================================================
    # SECTION 7 — PER-INVESTOR PIPELINE
    # ==================================================================
    for job in jobs:
        investor_id    = job.get('investor_id')
        sub_account_id = job.get('sub_account_id')
        programme_id   = job.get('programme_id')
        programme_name = job.get('programme_name') or f"programme_{programme_id}"
        programme_userid = resolve_programme_userid(programme_id, job)

        if investor_id is None or sub_account_id is None or programme_id is None or not programme_userid:
            stats['investors_skipped'] += 1
            stats['errors'].append(
                f"investor={investor_id}: missing investor/subaccount/programme owner identity"
            )
            continue

        print(f"\n{'─'*72}")
        print(f" 🚀 Investor {investor_id} (sub {sub_account_id}) | "
              f"programme '{programme_name}' (id={programme_id})")
        print(f"{'─'*72}")

        stats['investors_processed'] += 1
        stats['programmes_processed'] += 1

        # --- resolve risk brackets for this programme / balance ---
        default_risk, maximum_risk = _resolve_risk_for_balance(programme_id, current_balance)
        if default_risk is None or maximum_risk is None:
            print(f"  ⚠️ No risk brackets for programme {programme_id} — skipping")
            stats['errors'].append(f"programme {programme_id}: no risk brackets")
            stats['investors_skipped'] += 1
            continue
        print(f"  🎯 Risk guard: default=${default_risk:.2f}  maximum=${maximum_risk:.2f}")

        # --- fetch execution_start_date once from the exact investor subaccount row ---
        exec_row = db_fetch_one(
            f"SELECT execution_start_date FROM harvhub "
            f"WHERE id = {int(investor_id)} AND sub_account_id = {int(sub_account_id)} LIMIT 1",
            None, timeout=10.0, quiet=True,
        ) or {}
        execution_start_date = _raw_first_key(exec_row, 'execution_start_date')

        # --- ONLY the active programme's live analytics trades from the
        # investor's execution start date through now are eligible.
        rows, eligibility = fetch_live_investor_programme_trades(
            programme_id, programme_userid, investor_id, sub_account_id,
            execution_start_date, pending_only=True,
        )
        stats['rows_skipped_old_contract'] += int(eligibility.get('skipped_old_contract', 0))
        stats['rows_skipped_missing_trade_time'] += int(eligibility.get('skipped_missing_trade_time', 0))
        stats['rows_skipped_future_trade_time'] += int(eligibility.get('skipped_future_trade_time', 0))
        if eligibility.get('reason'):
            stats['eligibility_gate_failures'] += 1
            stats['errors'].append(
                f"programme={programme_id}, investor={investor_id}, sub={sub_account_id}: "
                f"live-trade eligibility blocked: {eligibility['reason']}"
            )
            print(f"  ⚠️ Live-trade eligibility blocked: {eligibility['reason']}")

        stats['rows_seen'] += len(rows)
        if not rows:
            print(f"  │ ℹ️ No pending/NULL rows for this investor")
        else:
            print(f"  │ Candidate rows: {len(rows)}")

        # ---------- Pre-fetch MT5 snapshots once per investor ----------
        try:
            pending_snapshot = mt5_orders_get() or []
        except Exception:
            pending_snapshot = []
        try:
            positions_snapshot = mt5_positions_get() or []
        except Exception:
            positions_snapshot = []

        pending_magics = set()
        for o in pending_snapshot:
            try:
                m = int(getattr(o, 'magic', 0) or 0)
                if m > 0:
                    pending_magics.add(m)
            except Exception:
                continue

        position_magics = set()
        for p in positions_snapshot:
            try:
                m = int(getattr(p, 'magic', 0) or 0)
                if m > 0:
                    position_magics.add(m)
            except Exception:
                continue

        _history_cache = {}

        def _history_has_magic(tid):
            if not tid:
                return False
            cached = _history_cache.get(tid)
            if cached is not None:
                return cached
            found = _row_is_in_mt5_history(tid, execution_start_date)
            _history_cache[tid] = found
            return found

        for r in rows:
            row_id    = _coerce_int(_raw_first_key(r, 'id'))
            tradeid   = _coerce_int(_raw_first_key(r, 'tradeid'))
            db_status = (_raw_first_key(r, 'orderstatus') or '')
            if isinstance(db_status, str):
                db_status = db_status.strip().lower()
            else:
                db_status = ''

            sym       = (_raw_first_key(r, 'symbol') or '').strip()
            ot        = (_raw_first_key(r, 'ordertype') or '').strip().lower()
            entry     = _coerce_float(_raw_first_key(r, 'entry'), default=None)
            sl        = _coerce_float(_raw_first_key(r, 'stoploss'), default=None)
            tp        = _coerce_float(_raw_first_key(r, 'takeprofit'), default=None)
            volume    = _coerce_float(_raw_first_key(r, 'volume'), default=None)
            pieces    = _coerce_int(_raw_first_key(r, 'chunked_pieces')) or 0

            print(f"\n  │ 📄 row {row_id} (tradeid={tradeid}, {sym} {ot}, db_status='{db_status or 'NULL'}')")
            print(f"  │    entry={entry}  sl={sl}  tp={tp}")
            print(f"  │    volume={volume}  chunked_pieces={pieces}")

            if row_id is None:
                stats['rows_skipped'] += 1
                continue

            # Build mirror context for this row
            mirror_ctx = {
                'programme_id': programme_id,
                'userid':       programme_userid,
                'tradeid':      tradeid,
            }

            # ==============================================================
            # STEP 1 — DB orderstatus pre-filter
            # ==============================================================
            if db_status == 'closed':
                print(f"  │    ⏭️ DB says 'closed' — SKIPPING without reconciliation")
                stats['rows_skipped_closed'] += 1
                stats['details'].append({
                    'row_id': row_id, 'tradeid': tradeid, 'symbol': sym,
                    'ordertype': ot, 'action': 'skipped_db_closed',
                })
                continue

            if db_status == 'running':
                print(f"  │    ⏭️ DB says 'running' — SKIPPING without reconciliation")
                stats['rows_skipped_running'] += 1
                stats['details'].append({
                    'row_id': row_id, 'tradeid': tradeid, 'symbol': sym,
                    'ordertype': ot, 'action': 'skipped_db_running',
                })
                continue

            # ==============================================================
            # STEP 2 — MT5 HISTORY (magic == tradeid)
            # ==============================================================
            if tradeid and _history_has_magic(tradeid):
                print(f"  │    🕓 Found in MT5 history (magic={tradeid}) — "
                      f"trade is CLOSED. Reconciling.")

                leaked_before = int(tradeid) in pending_magics
                if leaked_before:
                    print(f"  │       ⚠️ Leaked MT5 pending order(s) detected for "
                          f"magic={tradeid} — purging…")
                cancelled_tickets = _cancel_mt5_pending_by_magic(tradeid)
                if cancelled_tickets:
                    stats['mt5_pending_purged_count'] += len(cancelled_tickets)
                    stats['rows_closed_and_pending_purged'] += 1
                    for tkt in cancelled_tickets:
                        pending_magics.discard(int(tradeid))
                    try:
                        pending_snapshot[:] = [
                            o for o in pending_snapshot
                            if int(getattr(o, 'magic', 0) or 0) != int(tradeid)
                        ]
                    except Exception:
                        pass

                if db_status != 'closed':
                    ok, msg = _set_orderstatus_and_reason(
                        row_id, 'closed', mirror_ctx=mirror_ctx)
                    if ok:
                        stats['rows_marked_closed'] += 1
                        stats['rows_cleared_failure_reason'] += 1
                        print(f"  │       DB updated: orderstatus='closed' (success, investor row)")
                    else:
                        print(f"  │       ⚠️ DB update failed: {msg}")
                        stats['errors'].append(f"row {row_id}: mark closed failed — {msg}")
                else:
                    print(f"  │       DB already 'closed'")

                print(f"  │    ⏭️ Row is CLOSED — not placing")
                stats['details'].append({
                    'row_id': row_id, 'tradeid': tradeid, 'symbol': sym,
                    'ordertype': ot,
                    'action': 'closed_history_purged_pending',
                    'purged_tickets': cancelled_tickets,
                })
                continue

            # ==============================================================
            # STEP 3 — MT5 open positions
            # ==============================================================
            if tradeid and int(tradeid) in position_magics:
                if db_status != 'running':
                    print(f"  │    ✅ Found in MT5 open positions (magic={tradeid}) — "
                          f"marking DB as 'running'")
                    ok, msg = _set_orderstatus_and_reason(
                        row_id, 'running', mirror_ctx=mirror_ctx)
                    if ok:
                        stats['rows_marked_running'] += 1
                        stats['rows_cleared_failure_reason'] += 1
                        print(f"  │       DB updated: orderstatus='running' (success, investor row)")
                    else:
                        print(f"  │       ⚠️ DB update failed: {msg}")
                        stats['errors'].append(f"row {row_id}: mark running failed — {msg}")
                else:
                    print(f"  │    ✅ Found in MT5 open positions (magic={tradeid}) — "
                          f"DB already 'running', SKIPPING placement")
                stats['details'].append({
                    'row_id': row_id, 'tradeid': tradeid, 'symbol': sym,
                    'ordertype': ot, 'action': 'marked_running_from_mt5_position',
                })
                continue

            # ==============================================================
            # STEP 4 — MT5 pending orders
            # ==============================================================
            if tradeid and int(tradeid) in pending_magics:
                if db_status != 'pending':
                    print(f"  │    ✅ Found in MT5 pending (magic={tradeid}) — "
                          f"DB was '{db_status or 'NULL'}' → marking 'pending'")
                    ok, msg = _set_orderstatus_and_reason(
                        row_id, 'pending', mirror_ctx=mirror_ctx)
                    if ok:
                        stats['rows_marked_pending'] += 1
                        stats['rows_cleared_failure_reason'] += 1
                        print(f"  │       DB updated: orderstatus='pending' (success, investor row)")
                    else:
                        print(f"  │       ⚠️ DB update failed: {msg}")
                        stats['errors'].append(f"row {row_id}: mark pending failed — {msg}")
                else:
                    print(f"  │    ✅ Found in MT5 pending (magic={tradeid}) — "
                          f"DB already 'pending', SKIPPING placement")
                stats['rows_skipped_found_pending'] += 1
                stats['details'].append({
                    'row_id': row_id, 'tradeid': tradeid, 'symbol': sym,
                    'ordertype': ot, 'action': 'skipped_found_in_mt5_pending',
                })
                continue

            # ==============================================================
            # STEP 5 — Not found anywhere → PLACE
            # ==============================================================
            print(f"  │    🎯 Not found in MT5 pending/positions/history — PLACING")

            if not sym or volume is None or volume <= 0:
                reason = ('missing_symbol' if not sym
                          else 'missing_or_zero_volume')
                print(f"  │    ⚠️ incomplete row ({reason}) — marking FAILED")
                _mark_row_failed(row_id, reason, tradeid=tradeid, sym=sym, ot=ot,
                                 mirror_ctx=mirror_ctx)
                stats['rows_skipped'] += 1
                continue

            si = _get_symbol_info(sym)
            if si is None:
                reason = f'symbol_info_unavailable:{sym}'
                print(f"  │    ⚠️ no symbol_info for {sym} — marking FAILED")
                _mark_row_failed(row_id, reason, tradeid=tradeid, sym=sym, ot=ot,
                                 mirror_ctx=mirror_ctx)
                stats['rows_skipped'] += 1
                continue

            tick = _get_tick(sym)
            if tick is None:
                reason = f'no_tick_data:{sym}'
                print(f"  │    ⚠️ no tick for {sym} — marking FAILED")
                _mark_row_failed(row_id, reason, tradeid=tradeid, sym=sym, ot=ot,
                                 mirror_ctx=mirror_ctx)
                stats['rows_skipped'] += 1
                continue

            digits   = _price_digits(si)
            vol_min  = float(getattr(si, 'volume_min', 0.01) or 0.01)
            vol_max  = float(getattr(si, 'volume_max', 100.0) or 100.0)
            vol_step = float(getattr(si, 'volume_step', 0.01) or 0.01)

            num_pieces = 1
            if pieces and pieces > 1:
                num_pieces = int(pieces)

            raw_piece_vol = float(volume) / float(num_pieces)
            if raw_piece_vol > vol_max:
                num_pieces = int(math.ceil(volume / vol_max))
                raw_piece_vol = float(volume) / float(num_pieces)

            piece_vol = _round_to_step(raw_piece_vol, vol_step)
            if piece_vol < vol_min:
                piece_vol = vol_min
            if piece_vol > vol_max:
                piece_vol = vol_max

            print(f"  │    pieces={num_pieces}  piece_vol≈{piece_vol}  "
                  f"(vol_min={vol_min}, vol_max={vol_max}, step={vol_step})")

            if piece_vol <= 0 or piece_vol < vol_min:
                reason = (f'invalid_lotsize: '
                          f'piece_vol={piece_vol} vol_min={vol_min} vol_max={vol_max}')
                print(f"  │    ⚠️ invalid lotsize — marking FAILED")
                _mark_row_failed(row_id, reason, tradeid=tradeid, sym=sym, ot=ot,
                                 mirror_ctx=mirror_ctx)
                stats['rows_skipped'] += 1
                continue

            mt5_type, converted, conv_reason = _order_type_to_mt5(
                ot, entry=entry, ask=tick.ask, bid=tick.bid
            )
            if mt5_type is None:
                reason = f'invalid_order_type:{ot or "empty"}'
                print(f"  │    ⚠️ {conv_reason} — marking FAILED")
                _mark_row_failed(row_id, reason, tradeid=tradeid, sym=sym, ot=ot,
                                 mirror_ctx=mirror_ctx)
                stats['rows_skipped'] += 1
                continue
            if converted:
                print(f"  │    🔄 {conv_reason}")

            is_pending = mt5_type in (mt5.ORDER_TYPE_BUY_LIMIT,
                                      mt5.ORDER_TYPE_SELL_LIMIT,
                                      mt5.ORDER_TYPE_BUY_STOP,
                                      mt5.ORDER_TYPE_SELL_STOP)
            direction = 'buy' if mt5_type in (mt5.ORDER_TYPE_BUY,
                                              mt5.ORDER_TYPE_BUY_LIMIT,
                                              mt5.ORDER_TYPE_BUY_STOP) else 'sell'

            target_status = 'pending' if is_pending else 'running'

            bad_price_reason = _prevalidate_order_prices(
                ot, mt5_type, entry, sl, tp, tick, si, sym
            )
            if bad_price_reason:
                print(f"  │    ⚠️ {bad_price_reason} — marking FAILED")
                _mark_row_failed(row_id, bad_price_reason,
                                 tradeid=tradeid, sym=sym, ot=ot,
                                 mirror_ctx=mirror_ctx)
                stats['rows_skipped'] += 1
                continue

            pieces_placed_this_row  = 0
            pieces_failed_this_row  = 0
            pieces_skipped_this_row = 0
            last_failure_reason     = None

            for piece_i in range(1, num_pieces + 1):
                entry_for_risk = entry if is_pending else (
                    tick.ask if direction == 'buy' else tick.bid
                )
                piece_risk = _order_risk_in_dollars(
                    entry_for_risk, sl, sym, piece_vol, direction == 'buy'
                )
                existing_risk = _collect_existing_risk_for_entry(
                    sym, direction, entry_for_risk
                )
                total_risk = existing_risk + piece_risk

                if piece_risk > 0 and total_risk > maximum_risk:
                    last_failure_reason = (
                        f'risk_guard_rejected: total=${total_risk:.2f} '
                        f'> max=${maximum_risk:.2f} '
                        f'(existing=${existing_risk:.2f} + piece=${piece_risk:.2f})'
                    )
                    print(f"  │    🚫 piece {piece_i}/{num_pieces}: "
                          f"total risk ${total_risk:.2f} > max ${maximum_risk:.2f} — STOP")
                    stats['orders_rejected_by_risk'] += 1
                    pieces_skipped_this_row += 1
                    break

                req_price = entry_for_risk
                if not is_pending:
                    req_price = tick.ask if direction == 'buy' else tick.bid

                request = {
                    "symbol": sym,
                    "volume": float(piece_vol),
                    "type": mt5_type,
                    "price": round(float(req_price), digits),
                    "deviation": 20,
                    "magic": int(tradeid or 0),
                    "comment": f"DBPIECE{piece_i}OF{num_pieces}"[:31],
                    "type_time": mt5.ORDER_TIME_GTC,
                }
                if sl is not None and sl > 0:
                    request["sl"] = round(float(sl), digits)
                if tp is not None and tp > 0:
                    request["tp"] = round(float(tp), digits)

                filling_modes = _get_supported_filling_modes(si, is_pending_order=is_pending)

                res, used_mode, err = _send_order_with_auto_filling(
                    request, filling_modes, is_pending_order=is_pending
                )

                if res is not None and res.retcode == mt5.TRADE_RETCODE_DONE:
                    pieces_placed_this_row += 1
                    stats['pieces_placed'] += 1
                    print(f"  │    ✅ piece {piece_i}/{num_pieces}: "
                          f"{sym} {direction.upper()} {piece_vol} lots "
                          f"@ {request['price']} ticket={res.order} "
                          f"[filling={used_mode}]")
                else:
                    pieces_failed_this_row += 1
                    stats['pieces_failed'] += 1
                    last_failure_reason = _describe_mt5_failure(
                        res, ot, sym, req_price, sl, tp, piece_i, num_pieces
                    )
                    print(f"  │    ✗ piece {piece_i}/{num_pieces}: "
                          f"placement failed — {err}")

            if num_pieces == 1:
                if pieces_placed_this_row == 1:
                    ok, msg = _set_orderstatus_and_reason(
                        row_id, target_status, mirror_ctx=mirror_ctx)
                    if ok:
                        stats['rows_placed'] += 1
                        stats['rows_cleared_failure_reason'] += 1
                        print(f"  │    🏁 row {row_id} → orderstatus='{target_status}' "
                              f"(single piece, success, investor row)")
                    else:
                        print(f"  │    ⚠️ row {row_id}: status update failed — {msg}")
                        stats['errors'].append(f"row {row_id}: status update failed — {msg}")
                else:
                    reason = last_failure_reason or (
                        f'piece_placement_failed: 0/{num_pieces} placed'
                    )
                    _mark_row_failed(row_id, reason, tradeid=tradeid,
                                     sym=sym, ot=ot, mirror_ctx=mirror_ctx)
                    stats['rows_skipped'] += 1
                    print(f"  │    ✗ row {row_id}: single piece failed → marked FAILED")
            else:
                if pieces_placed_this_row == num_pieces:
                    ok, msg = _set_orderstatus_and_reason(
                        row_id, target_status, mirror_ctx=mirror_ctx)
                    if ok:
                        stats['rows_placed'] += 1
                        stats['rows_cleared_failure_reason'] += 1
                        print(f"  │    🏁 row {row_id} → orderstatus='{target_status}' "
                              f"(all {num_pieces} pieces placed, success, investor row)")
                    else:
                        print(f"  │    ⚠️ row {row_id}: status update failed — {msg}")
                        stats['errors'].append(f"row {row_id}: status update failed — {msg}")
                else:
                    reason = last_failure_reason or (
                        f'partial_placement: {pieces_placed_this_row}/{num_pieces} '
                        f'pieces placed, {pieces_failed_this_row} failed, '
                        f'{pieces_skipped_this_row} skipped'
                    )
                    _mark_row_failed(row_id, reason, tradeid=tradeid,
                                     sym=sym, ot=ot, mirror_ctx=mirror_ctx)
                    stats['rows_skipped'] += 1
                    print(f"  │    ⏸ row {row_id}: only {pieces_placed_this_row}/"
                          f"{num_pieces} pieces placed → marked FAILED")

            stats['details'].append({
                'row_id':            row_id,
                'tradeid':           tradeid,
                'symbol':            sym,
                'ordertype':         ot,
                'chunked_pieces':    num_pieces,
                'piece_volume':      piece_vol,
                'pieces_placed':     pieces_placed_this_row,
                'pieces_failed':     pieces_failed_this_row,
                'pieces_skipped':    pieces_skipped_this_row,
                'converted':         converted,
                'target_status':     target_status,
                'action':            'placed',
            })

        # ==================================================================
        # STEP 9 — LEGACY PROGRAMME MODIFY-TRADE DISABLED
        # Explicit programme authority is handled only by the priority
        # process_programme_order_requests_db() stage and the PHP
        # order_request_status / order_request_filled columns. The legacy
        # modify_trade field is intentionally not executed here.
        # ==================================================================

    # ==================================================================
    # SECTION 8 — SUMMARY
    # ==================================================================
    print(f"\n{'='*10} 🚀 PLACE USD ORDERS SUMMARY (DB) {'='*10}")
    print(f"  Investors processed                : {stats['investors_processed']}")
    print(f"  Investors skipped                  : {stats['investors_skipped']}")
    print(f"  Rows seen (live + execution-date eligible): {stats['rows_seen']}")
    print(f"  Skipped (before execution start)   : {stats['rows_skipped_old_contract']}")
    print(f"  Skipped (missing trade_time)       : {stats['rows_skipped_missing_trade_time']}")
    print(f"  Skipped (future trade_time)        : {stats['rows_skipped_future_trade_time']}")
    print(f"  Eligibility gate failures          : {stats['eligibility_gate_failures']}")
    print(f"  -- DB-side skips --")
    print(f"  Skipped (DB 'closed')              : {stats['rows_skipped_closed']}")
    print(f"  Skipped (DB 'running')             : {stats['rows_skipped_running']}")
    print(f"  -- MT5 reconciliation --")
    print(f"  Closed via MT5 history + purged    : {stats['rows_closed_and_pending_purged']}")
    print(f"  MT5 pending orders purged          : {stats['mt5_pending_purged_count']}")
    print(f"  Skipped (found in MT5 pending)     : {stats['rows_skipped_found_pending']}")
    print(f"  Marked 'pending' (MT5 pending)     : {stats['rows_marked_pending']}")
    print(f"  Marked 'running' (MT5 position)    : {stats['rows_marked_running']}")
    print(f"  Marked 'closed'  (MT5 history)     : {stats['rows_marked_closed']}")
    print(f"  -- placement --")
    print(f"  Rows placed                        : {stats['rows_placed']}")
    print(f"  Rows marked FAILED                 : {stats['rows_marked_failed']}")
    print(f"  Rows with success                  : {stats['rows_cleared_failure_reason']}")
    print(f"  Rows skipped                       : {stats['rows_skipped']}")
    print(f"  Pieces placed                      : {stats['pieces_placed']}")
    print(f"  Pieces failed                      : {stats['pieces_failed']}")
    print(f"  Rejected by risk guard             : {stats['orders_rejected_by_risk']}")
    print(f"  -- modify-trade enforcement --")
    print(f"  Positions closed (close-position)  : {stats['modify_close_positions_done']}")
    print(f"  Orders deleted   (delete-order)    : {stats['modify_delete_orders_done']}")
    print(f"  No MT5 hit (marked closed anyway)  : {stats['modify_requests_cleared_no_mt5_hit']}")
    print(f"  Modify requests failed             : {stats['modify_requests_failed']}")
    print(f"  -- programme-level mirrors --")
    print(f"  Programme rows updated via mirror  : {stats['programme_level_mirrors']}")
    print(f"  Errors                             : {len(stats['errors'])}")
    for e in stats['errors']:
        print(f"     • {e}")
    print(f"{'='*10} 🏁 COMPLETE {'='*10}\n")

    stats['processing_success'] = True
    return stats
   
# ==============================================================================
#  CHECK PENDING ORDERS RISK — DB version
#
#  Enforces the programme's risk ceiling (accountmanagement_balance_maximum_risk)
#  against every live MT5 pending order for the investor, and purges any order
#  whose REAL risk (entry -> SL, in account currency) exceeds the hard cap.
#
#  SOURCES
#  -------
#  READS:
#    * accountmanagement_balance_default_risk  -> per-programme target risk
#                                                 (fallback when maximum is missing)
#    * accountmanagement_balance_maximum_risk  -> per-programme hard cap
#    * programme_trades                        -> row identity (tradeid <-> magic)
#    * MT5                                     -> live pending orders + SL
#  WRITES:
#    * MT5  -> TRADE_ACTION_REMOVE for offending tickets
#    * programme_trades -> orderstatus = 'closed' for the matching tradeid
#                          (the pending order is gone; nothing left to place)
#
#  RULES (mirrors check_pending_orders_risk_local)
#  ----------------------------------------------
#    0. Order has no SL                        -> REMOVE
#    1. risk <= default                        -> KEEP
#    2. default < risk <= maximum              -> KEEP
#    3. risk > maximum                         -> REMOVE
#    4. default missing                        -> maximum used as both
#    5. maximum missing                        -> default used as both
#    6. both missing                           -> skip investor
#
#  Magic semantics: order.magic == programme_trades.tradeid (same as the
#  rest of signals_trading.py).
# ==============================================================================
def check_pending_orders_risk_db(inv_db_id=None, jobs=None):
    print(f"\n{'='*10} 🛡️ PENDING ORDERS RISK AUDIT (DB) {'='*10}")

    stats = {
        'investor_id':                       inv_db_id if inv_db_id else 'all',
        'investors_processed':               0,
        'investors_skipped':                 0,
        'programmes_processed':              0,
        'orders_checked':                    0,
        'orders_kept':                       0,
        'orders_removed':                    0,
        'orders_removed_no_sl':              0,
        'orders_removed_exceeded_max':       0,
        'cancel_failures':                   0,
        'rows_marked_closed':                0,
        'errors':                            [],
        'details':                           [],
        'processing_success':                False,
    }

    # ------------------------------------------------------------------
    # MT5 helpers
    # ------------------------------------------------------------------
    def _digits(si):
        try:
            if si is not None and hasattr(si, 'digits'):
                d = int(si.digits)
                if d >= 0:
                    return d
        except Exception:
            pass
        return 5

    def _order_risk_usd(order, si):
        """
        Risk (account currency) from order.price_open -> order.sl.
        Returns (risk_abs, sl_distance, contract_size, direction) or
        (None, ...) when it cannot be computed.
        """
        try:
            entry = float(order.price_open)
            sl = float(order.sl) if order.sl else 0.0
        except Exception:
            return None, 0.0, 1.0, None

        if sl <= 0:
            return None, 0.0, 1.0, None

        t = int(getattr(order, 'type', -1))
        is_buy = t in (mt5.ORDER_TYPE_BUY_LIMIT,
                       mt5.ORDER_TYPE_BUY_STOP,
                       mt5.ORDER_TYPE_BUY_STOP_LIMIT)
        direction = 'buy' if is_buy else 'sell'

        sl_distance = abs(entry - sl)
        if sl_distance <= 0:
            return 0.0, 0.0, 1.0, direction

        contract = 1.0
        try:
            if si is not None:
                contract = float(getattr(si, 'trade_contract_size', 1.0) or 1.0)
        except Exception:
            contract = 1.0

        vol = float(getattr(order, 'volume_current', 0)
                    or getattr(order, 'volume_initial', 0) or 0)
        if vol <= 0:
            return 0.0, sl_distance, contract, direction

        try:
            calc_type = mt5.ORDER_TYPE_BUY if is_buy else mt5.ORDER_TYPE_SELL
            profit = mt5_order_calc_profit(
                calc_type, order.symbol, vol, entry, sl
            )
            if profit is None:
                return sl_distance * vol * contract, sl_distance, contract, direction
            return abs(float(profit)), sl_distance, contract, direction
        except Exception:
            return sl_distance * vol * contract, sl_distance, contract, direction

    def _cancel_pending(ticket):
        try:
            req = {"action": mt5.TRADE_ACTION_REMOVE, "order": int(ticket)}
            res = mt5_order_send(req)
            if res is not None and res.retcode == mt5.TRADE_RETCODE_DONE:
                return True, 'cancelled'
            if res is not None:
                return False, f"retcode={res.retcode} comment={getattr(res, 'comment', '')}"
            return False, f"no_response last_error={mt5_last_error()}"
        except Exception as e:
            return False, f"exception: {e}"

    def _mark_row_closed_by_tradeid(tradeid, investor_id, sub_account_id, programme_id, programme_userid):
        if tradeid is None or not programme_userid:
            return False
        ok, affected, msg = db_execute(
            """
            UPDATE programme_trades
               SET orderstatus = 'closed'
             WHERE userid = %s
               AND programmeid = %s
               AND investor_id = %s
               AND investor_sub_account_id = %s
               AND tradeid = %s
               AND orderstatus = 'pending'
            """,
            [programme_userid, programme_id, investor_id, sub_account_id, int(tradeid)],
            timeout=15.0, quiet=True,
        )
        return bool(ok) and (affected or 0) >= 0

    # ------------------------------------------------------------------
    # Programme risk-bracket readers (cached)
    # ------------------------------------------------------------------
    _risk_cfg_cache = {}

    def _load_programme_risk_cfg(programme_id):
        cfg = _risk_cfg_cache.get(programme_id)
        if cfg is not None:
            return cfg

        def _read(table):
            rows = db_fetch_all(
                f"""
                SELECT from_balance_range, to_balance_range, risk_amount
                  FROM {table}
                 WHERE programme_id = %s
                 ORDER BY from_balance_range ASC, id ASC
                """,
                [programme_id], timeout=10.0, quiet=True,
            )
            out = []
            for r in rows:
                try:
                    lo = _coerce_float(_raw_first_key(r, 'from_balance_range'), default=None)
                    hi = _coerce_float(_raw_first_key(r, 'to_balance_range'),   default=None)
                    rk = _coerce_float(_raw_first_key(r, 'risk_amount'),        default=None)
                except Exception:
                    continue
                if lo is None or rk is None:
                    continue
                if hi is None or hi <= 0:
                    hi = float('inf')
                out.append({'from': float(lo), 'to': float(hi), 'risk': float(rk)})
            return out

        def _pick(brackets, balance):
            if not brackets or balance is None:
                return None
            for b in brackets:
                if b['from'] <= balance <= b['to']:
                    return b['risk']
            below = [b for b in brackets if b['from'] <= balance]
            if below:
                return below[-1]['risk']
            return brackets[0]['risk']

        cfg = {
            'default_brackets': _read('accountmanagement_balance_default_risk'),
            'max_brackets':     _read('accountmanagement_balance_maximum_risk'),
            'pick_default':     lambda bal: _pick(
                _read('accountmanagement_balance_default_risk'), bal
            ),
            'pick_max':         lambda bal: _pick(
                _read('accountmanagement_balance_maximum_risk'), bal
            ),
        }
        _risk_cfg_cache[programme_id] = cfg
        return cfg

    def _resolve_risk_pair(programme_id, balance):
        cfg = _load_programme_risk_cfg(programme_id)
        default_r = cfg['pick_default'](balance)
        maximum_r = cfg['pick_max'](balance)

        if default_r is None and maximum_r is None:
            return None, None, 'no_config'
        if default_r is None:
            return float(maximum_r), float(maximum_r), 'max_only'
        if maximum_r is None:
            return float(default_r), float(default_r), 'default_only'
        if maximum_r < default_r:
            maximum_r = default_r
        return float(default_r), float(maximum_r), 'both'

    # ------------------------------------------------------------------
    # MT5 connection check
    # ------------------------------------------------------------------
    acc = mt5_account_info()
    if acc is None:
        print(f"  ⚠️ MT5 not connected — cannot audit pending risk")
        stats['errors'].append('MT5 not connected')
        return stats

    try:
        current_balance = float(acc.balance)
    except Exception:
        current_balance = 0.0
    print(f"  💰 Current MT5 balance: ${current_balance:.2f}")

    # ------------------------------------------------------------------
    # Enumeration — skipped when jobs= passed in
    # ------------------------------------------------------------------
    if jobs is None:
        print("\n" + "─" * 80)
        print(" 📁 LOADING INVESTOR JOBS FROM DB (VPS-scoped)")
        print("─" * 80)

        local_ip = get_local_ip()
        if not local_ip:
            print(" ⚠️ Could not determine local IP")
            stats['errors'].append('local IP unknown')
            return stats
        print(f" │ Local IP: {local_ip}")

        all_jobs = fetch_vps_investor_jobs(local_ip)
        if not all_jobs:
            print(" │ ℹ️ No active investor jobs found for this VPS")
            stats['processing_success'] = True
            return stats

        if inv_db_id:
            jobs = [j for j in all_jobs if str(j.get('investor_id')) == str(inv_db_id)]
            if not jobs:
                print(f" ⚠️ Investor {inv_db_id} not found in any VPS-scoped job")
                stats['errors'].append(f"Investor {inv_db_id} not found")
                return stats
        else:
            jobs = all_jobs

        print(f" │ Loaded {len(jobs)} investor job(s) from DB")
        print("─" * 80)

    # ------------------------------------------------------------------
    # Per-investor loop
    # ------------------------------------------------------------------
    for job in jobs:
        investor_id    = job.get('investor_id')
        sub_account_id = job.get('sub_account_id')
        programme_id   = job.get('programme_id')
        programme_name = job.get('programme_name') or f"programme_{programme_id}"
        programme_userid = resolve_programme_userid(programme_id, job)

        if investor_id is None or sub_account_id is None or programme_id is None or not programme_userid:
            stats['investors_skipped'] += 1
            continue

        print(f"\n{'─'*72}")
        print(f" 🛡️ Investor {investor_id} (sub {sub_account_id}) | "
              f"programme '{programme_name}' (id={programme_id})")
        print(f"{'─'*72}")

        stats['investors_processed'] += 1
        stats['programmes_processed']  += 1

        default_risk, maximum_risk, cfg_src = _resolve_risk_pair(
            programme_id, current_balance
        )
        if default_risk is None or maximum_risk is None:
            print(f"  │ ⚠️ No risk configuration — skipping investor")
            stats['investors_skipped'] += 1
            continue
        if cfg_src == 'max_only':
            print(f"  │ ℹ️ default missing → using maximum for both")
        elif cfg_src == 'default_only':
            print(f"  │ ℹ️ maximum missing → using default for both")
        print(f"  │ Target risk (default) : ${default_risk:.2f}")
        print(f"  │ Maximum allowed (cap) : ${maximum_risk:.2f}")

        # Build a lookup from current live trades only; unrelated or stale
        # MT5 orders must never be modified by this programme's risk audit.
        hub = db_fetch_one(
            f"SELECT execution_start_date FROM harvhub "
            f"WHERE id = {int(investor_id)} AND sub_account_id = {int(sub_account_id)} LIMIT 1",
            None, timeout=10.0, quiet=True,
        ) or {}
        live_rows, eligibility = fetch_live_investor_programme_trades(
            programme_id, programme_userid, investor_id, sub_account_id,
            _raw_first_key(hub, 'execution_start_date'), pending_only=True,
        )
        row_lookup = {}
        for r in live_rows:
            tid = _coerce_int(_raw_first_key(r, 'tradeid'))
            rid = _coerce_int(_raw_first_key(r, 'id'))
            if tid is not None and rid is not None:
                row_lookup[tid] = rid
        if eligibility.get('reason'):
            print(f"  │ ⚠️ Live-trade risk audit gate closed: {eligibility['reason']}")

        # Snapshot live pending orders
        try:
            live_pending = mt5_orders_get() or []
        except Exception:
            live_pending = []

        if not live_pending:
            print(f"  │ 🔘 No live pending orders")
            continue

        print(f"  │ 🔍 Scanning {len(live_pending)} pending order(s)…")

        symbol_info_cache = {}

        def _si(sym):
            si = symbol_info_cache.get(sym)
            if si is None:
                si = mt5_symbol_info(sym)
                if si is None:
                    mt5_symbol_select(sym, True)
                    si = mt5_symbol_info(sym)
                symbol_info_cache[sym] = si
            return si

        for order in live_pending:
            ticket = getattr(order, 'ticket', None)
            sym = getattr(order, 'symbol', '?')
            tradeid = None
            try:
                tradeid = int(getattr(order, 'magic', 0) or 0)
            except Exception:
                tradeid = 0

            if not tradeid or tradeid not in row_lookup:
                # Not a live current-contract programme trade for this investor.
                continue

            si = _si(sym)
            digits = _digits(si)

            print(f"\n  │ 📋 ticket #{ticket} | {sym} | magic={tradeid} "
                  f"| vol={float(getattr(order, 'volume_current', 0) or 0):.2f}")

            # Rule 0 — no SL
            sl_raw = getattr(order, 'sl', None)
            try:
                sl_val = float(sl_raw) if sl_raw else 0.0
            except Exception:
                sl_val = 0.0

            if sl_val <= 0:
                print(f"  │    🗑️ PURGING: no stoploss protection detected")
                ok, msg = _cancel_pending(ticket)
                stats['orders_checked'] += 1
                if ok:
                    stats['orders_removed'] += 1
                    stats['orders_removed_no_sl'] += 1
                    print(f"  │    ✅ Order removed")
                    if tradeid and tradeid in row_lookup:
                        if _mark_row_closed_by_tradeid(
                            tradeid, investor_id, sub_account_id, programme_id, programme_userid
                        ):
                            stats['rows_marked_closed'] += 1
                else:
                    stats['cancel_failures'] += 1
                    print(f"  │    ✗ Cancel failed: {msg}")
                continue

            # Compute real risk
            risk_usd, sl_dist, contract, direction = _order_risk_usd(order, si)
            stats['orders_checked'] += 1
            if risk_usd is None:
                print(f"  │    ⚠️ Could not compute risk — leaving order alone")
                stats['orders_kept'] += 1
                continue

            print(f"  │    order risk : ${risk_usd:.2f}  "
                  f"(SL dist={sl_dist}, contract={contract}, {direction})")
            print(f"  │    target     : ${default_risk:.2f} | "
                  f"maximum: ${maximum_risk:.2f}")

            if risk_usd > maximum_risk + 1e-9:
                print(f"  │    🗑️ PURGING: risk exceeds maximum threshold")
                print(f"  │       ${risk_usd:.2f} > ${maximum_risk:.2f} "
                      f"(excess ${risk_usd - maximum_risk:.2f})")
                ok, msg = _cancel_pending(ticket)
                if ok:
                    stats['orders_removed'] += 1
                    stats['orders_removed_exceeded_max'] += 1
                    print(f"  │    ✅ Order removed")
                    if tradeid and tradeid in row_lookup:
                        if _mark_row_closed_by_tradeid(
                            tradeid, investor_id, sub_account_id, programme_id, programme_userid
                        ):
                            stats['rows_marked_closed'] += 1
                else:
                    stats['cancel_failures'] += 1
                    print(f"  │    ✗ Cancel failed: {msg}")
            elif risk_usd <= default_risk + 1e-9:
                stats['orders_kept'] += 1
                print(f"  │    ✅ KEEPING: within target range")
            else:
                stats['orders_kept'] += 1
                print(f"  │    ✅ KEEPING: between target and maximum")

            stats['details'].append({
                'investor_id':     investor_id,
                'sub_account_id':  sub_account_id,
                'programme_id':    programme_id,
                'ticket':          ticket,
                'symbol':          sym,
                'magic_tradeid':   tradeid,
                'risk_usd':        float(risk_usd),
                'default_risk':    float(default_risk),
                'maximum_risk':    float(maximum_risk),
                'action':          'removed_exceeded_max' if risk_usd > maximum_risk + 1e-9
                                   else 'kept',
            })

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print(f"\n{'='*10} 🛡️ PENDING ORDERS RISK SUMMARY (DB) {'='*10}")
    print(f"  Investors processed            : {stats['investors_processed']}")
    print(f"  Investors skipped              : {stats['investors_skipped']}")
    print(f"  Orders checked                 : {stats['orders_checked']}")
    print(f"  Orders kept                    : {stats['orders_kept']}")
    print(f"  Orders removed (total)         : {stats['orders_removed']}")
    print(f"    - No stoploss                : {stats['orders_removed_no_sl']}")
    print(f"    - Exceeded maximum risk      : {stats['orders_removed_exceeded_max']}")
    print(f"  Cancel failures                : {stats['cancel_failures']}")
    print(f"  programme_trades marked closed : {stats['rows_marked_closed']}")
    print(f"  Errors                         : {len(stats['errors'])}")
    for e in stats['errors']:
        print(f"     • {e}")
    print(f"{'='*10} 🏁 COMPLETE {'='*10}\n")

    stats['processing_success'] = True
    return stats

# ==============================================================================
#  ORDERS REWARD CORRECTION — DB version
#
#  Aligns live MT5 pending orders AND open positions' TPs to the programme's
#  R:R configuration, using the SAME classifier semantics as
#  calculate_investor_symbols_orders_db.
#
#  SOURCES
#  -------
#  READS:
#    * signals_provider_interest.risk_reward
#      signals_provider_interest.risk_reward_type   (programme-scoped)
#    * accountmanagement.fixed_risk_reward
#      accountmanagement.minimum_risk_reward        (fallback when
#                                                    signals_provider_interest
#                                                    has no usable row)
#    * accountmanagement_balance_default_risk       (only used for logging the
#                                                    base risk alongside)
#    * programme_trades                             (for the magic <-> tradeid
#                                                    link, so we can look up
#                                                    ordertype/comment when
#                                                    MT5 doesn't carry it)
#    * MT5                                          (live pending + positions)
#
#  WRITES:
#    * MT5 -> TRADE_ACTION_MODIFY on pending orders (tp only)
#    * MT5 -> TRADE_ACTION_SLTP  on open positions  (tp only)
#    * programme_trades -> takeprofit is NOT touched. The DB is the source of
#      the ORIGINAL signal; the R:R applied at MT5 is a live correction.
#
#  R:R CLASSIFIER (identical to calculate_investor_symbols_orders_db)
#  ---------------------------------------------------------------
#    'minimum_risk_reward'   minimum floor
#        current_rr <  value   -> raise to value (new TP)
#        current_rr >= value   -> NO change
#
#    'fixed_risk_reward'            exact rewrite
#        current_rr != value   -> rewrite to value
#        current_rr == value   -> NO change
#
#    'custom_and_default_risk_reward'     no configured target
#        current_rr >= 1.0     -> NO change
#        current_rr <  1.0     -> force to 1.0
#
#  TP SYNTHESIS
#  ------------
#  If takeprofit is missing (tp == 0), the correct TP is synthesized from
#  entry + SL and the target R:R, exactly as the DB calculate stage does.
#  Rounded to symbol digits.
#
#  ORDER TYPE
#  ----------
#  Applies to BOTH pending orders (TRADE_ACTION_MODIFY, tp only) and open
#  positions (TRADE_ACTION_SLTP, sl preserved + tp updated). Market orders
#  and pending orders are treated identically — the target R:R is the same.
#
#  No JSON. No local folders. No grid. Magic == programme_trades.tradeid.
# ==============================================================================
def orders_reward_correction_db(inv_db_id=None, jobs=None):
    print(f"\n{'='*10} 📐 ORDERS REWARD CORRECTION (DB) {'='*10}")

    stats = {
        'investor_id':                  inv_db_id if inv_db_id else 'all',
        'investors_processed':          0,
        'investors_skipped':            0,
        'programmes_processed':         0,
        'orders_checked':               0,
        'orders_adjusted':              0,
        'orders_skipped':               0,
        'orders_error':                 0,
        'positions_checked':            0,
        'positions_adjusted':           0,
        'positions_skipped':            0,
        'positions_error':              0,
        'rows_tp_synthesized':          0,
        'rr_matches':                   {},
        'rr_mismatches':                0,
        'mode':                         None,
        'programme_rr':                 None,
        'programme_rr_type_raw':        None,
        'programme_rr_type_norm':       None,
        'errors':                       [],
        'details':                      [],
        'processing_success':           False,
    }

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    def _norm_rr_type(raw):
        """Return one of the current signals_provider_interest enum values."""
        if raw is None:
            return None
        value = str(raw).strip().lower().replace('-', '_').replace(' ', '_')
        while '__' in value:
            value = value.replace('__', '_')
        aliases = {
            'fixed': 'fixed_risk_reward',
            'fixed_rr': 'fixed_risk_reward',
            'exact': 'fixed_risk_reward',
            'exact_risk_reward': 'fixed_risk_reward',
            'custom_minimum': 'minimum_risk_reward',
            'custom_minimum_risk_reward': 'minimum_risk_reward',
            'minimum': 'minimum_risk_reward',
            'min': 'minimum_risk_reward',
            'floor': 'minimum_risk_reward',
            'custom_fixed': 'custom_and_default_risk_reward',
            'custom_fixed_risk_reward': 'custom_and_default_risk_reward',
            'custom_and_default': 'custom_and_default_risk_reward',
            'custom_default_risk_reward': 'custom_and_default_risk_reward',
        }
        value = aliases.get(value, value)
        if value in ('fixed_risk_reward', 'minimum_risk_reward',
                     'custom_and_default_risk_reward'):
            return value
        return None

    def _digits(si):
        try:
            if si is not None and hasattr(si, 'digits'):
                d = int(si.digits)
                if d >= 0:
                    return d
        except Exception:
            pass
        return 5

    def _min_volume_for_symbol(si):
        if si is None:
            return 0.01
        try:
            v = float(getattr(si, 'volume_min', 0.01) or 0.01)
            return v if v > 0 else 0.01
        except Exception:
            return 0.01

    def _compute_current_rr(entry, sl, tp):
        if entry is None or sl is None or tp is None:
            return None, None, None
        risk   = abs(float(entry) - float(sl))
        reward = abs(float(tp) - float(entry))
        if risk <= 0:
            return None, risk, reward
        return reward / risk, risk, reward

    def _rr_precision_tolerance(entry, sl, symbol_info):
        """Half-tick R:R tolerance for idempotent exact-target correction."""
        try:
            risk_dist = abs(float(entry) - float(sl))
            tick_size = float(getattr(symbol_info, 'trade_tick_size', 0) or
                              getattr(symbol_info, 'point', 0) or 0)
            if risk_dist <= 0 or tick_size <= 0:
                return 1e-6
            return max(1e-6, min(0.02, tick_size / (2.0 * risk_dist) + 1e-9))
        except Exception:
            return 1e-6

    def _synthesize_tp(entry, sl, rr_value, is_buy, digits,
                       tick_size=None, round_up=False):
        if entry is None or sl is None or rr_value is None or rr_value <= 0:
            return None
        try:
            import math
            entry_f = float(entry)
            sl_f    = float(sl)
            risk_dist = abs(entry_f - sl_f)
            if risk_dist <= 0:
                return None
            reward_dist = risk_dist * float(rr_value)
            tp = (entry_f + reward_dist) if is_buy else (entry_f - reward_dist)
            tick = float(tick_size or 0)
            if tick > 0:
                units = tp / tick
                if round_up:
                    units = math.ceil(units - 1e-10) if is_buy else math.floor(units + 1e-10)
                else:
                    units = round(units)
                tp = units * tick
            return round(tp, digits)
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Programme-scoped readers (cached per run)
    # ------------------------------------------------------------------
    _prog_cfg_cache = {}
    _symbol_info_cache = {}

    def _get_symbol_info(sym):
        si = _symbol_info_cache.get(sym)
        if si is None:
            si = mt5_symbol_info(sym)
            if si is None:
                mt5_symbol_select(sym, True)
                si = mt5_symbol_info(sym)
            _symbol_info_cache[sym] = si
        return si

    def _fetch_programme_rr(programme_id):
        """Return (configured_value, raw_type, canonical_type, source).

        custom_and_default_risk_reward intentionally has no configured target;
        its effective 1:1 floor is applied by the classifier below.
        """
        row = db_fetch_one(
            """
            SELECT id, risk_reward, risk_reward_type
              FROM signals_provider_interest
             WHERE programme_id = %s
             ORDER BY id DESC
             LIMIT 1
            """,
            [programme_id], timeout=10.0, quiet=True,
        )
        if row:
            rr_val = _coerce_float(_raw_first_key(row, 'risk_reward'), default=None)
            rr_type = _raw_first_key(row, 'risk_reward_type')
            rr_norm = _norm_rr_type(rr_type)
            if rr_norm == 'custom_and_default_risk_reward':
                # Ignore stale numeric values: custom/default has no target.
                return None, rr_type, rr_norm, 'signals_provider_interest'
            if rr_norm in ('fixed_risk_reward', 'minimum_risk_reward') and rr_val is not None and rr_val > 0:
                return float(rr_val), rr_type, rr_norm, 'signals_provider_interest'
            if rr_norm is not None:
                return None, rr_type, rr_norm, 'signals_provider_interest'

        # Legacy numeric fallback remains for programmes without a usable
        # signals_provider_interest classifier row.
        am = db_fetch_one(
            """
            SELECT fixed_risk_reward, minimum_risk_reward
              FROM accountmanagement
             WHERE developerid = 0 AND programme_id = %s AND investorid = 0
             LIMIT 1
            """,
            [programme_id], timeout=10.0, quiet=True,
        )
        if not am:
            return None, None, None, None

        fixed = _coerce_float(_raw_first_key(am, 'fixed_risk_reward'), default=None)
        minimum = _coerce_float(_raw_first_key(am, 'minimum_risk_reward'), default=None)
        if fixed is not None and fixed > 0 and (minimum is None or fixed >= minimum):
            return float(fixed), 'fixed_risk_reward', 'fixed_risk_reward', 'accountmanagement.fixed_risk_reward'
        if minimum is not None and minimum > 0:
            return float(minimum), 'minimum_risk_reward', 'minimum_risk_reward', 'accountmanagement.minimum_risk_reward'
        return None, None, None, None

    def _programme_cfg(programme_id):
        cfg = _prog_cfg_cache.get(programme_id)
        if cfg is not None:
            return cfg
        rr_val, rr_type_raw, rr_type_norm, src = _fetch_programme_rr(programme_id)
        cfg = {
            'rr_value':       rr_val,
            'rr_type_raw':    rr_type_raw,
            'rr_type_norm':   rr_type_norm,
            'source':         src,
        }
        _prog_cfg_cache[programme_id] = cfg
        return cfg

    # ------------------------------------------------------------------
    # MT5 modify helpers
    # ------------------------------------------------------------------
    def _modify_pending_tp(ticket, entry, sl, new_tp, digits):
        req = {
            "action": mt5.TRADE_ACTION_MODIFY,
            "order":  int(ticket),
            "price":  round(float(entry), digits),
            "sl":     round(float(sl), digits) if sl else 0.0,
            "tp":     round(float(new_tp), digits),
        }
        try:
            res = mt5_order_send(req)
            if res is not None and res.retcode == mt5.TRADE_RETCODE_DONE:
                return True, 'ok'
            if res is not None:
                return False, f"retcode={res.retcode} comment={getattr(res, 'comment', '')}"
            return False, f"no_response last_error={mt5_last_error()}"
        except Exception as e:
            return False, f"exception: {e}"

    def _modify_position_tp(ticket, sl, new_tp, digits):
        req = {
            "action":   mt5.TRADE_ACTION_SLTP,
            "position": int(ticket),
            "sl":       round(float(sl), digits) if sl else 0.0,
            "tp":       round(float(new_tp), digits),
        }
        try:
            res = mt5_order_send(req)
            if res is not None and res.retcode == mt5.TRADE_RETCODE_DONE:
                return True, 'ok'
            if res is not None:
                return False, f"retcode={res.retcode} comment={getattr(res, 'comment', '')}"
            return False, f"no_response last_error={mt5_last_error()}"
        except Exception as e:
            return False, f"exception: {e}"

    # ------------------------------------------------------------------
    # R:R classifier (identical semantics to calculate_investor_symbols_orders_db)
    # ------------------------------------------------------------------
    def _decide_action(current_rr, target_rr, rr_type, direction_valid=True,
                       rr_tolerance=1e-6):
        """Apply the three current programme R:R policies to an MT5 trade."""
        if rr_type == 'minimum_risk_reward':
            if current_rr is None or not direction_valid:
                return 'rewrite', target_rr, f"minimum R:R: missing/invalid TP; enforce at least 1:{target_rr}"
            if current_rr < target_rr - 1e-9:
                return 'rewrite', target_rr, f"minimum R:R: current 1:{round(current_rr, 4)} < 1:{target_rr}; raise TP"
            return 'noop', None, f"minimum R:R: current 1:{round(current_rr, 4)} >= 1:{target_rr}"

        if rr_type == 'fixed_risk_reward':
            if current_rr is None or not direction_valid:
                return 'rewrite', target_rr, f"fixed R:R: missing/invalid TP; enforce exactly 1:{target_rr}"
            if abs(current_rr - target_rr) > rr_tolerance:
                return 'rewrite', target_rr, f"fixed R:R: current 1:{round(current_rr, 4)} != 1:{target_rr}; rewrite TP"
            return 'noop', None, f"fixed R:R: current already equals 1:{target_rr}"

        if rr_type == 'custom_and_default_risk_reward':
            floor = 1.0
            if current_rr is None or not direction_valid:
                return 'rewrite', floor, "custom/default R:R: missing/invalid TP; enforce at least 1:1"
            if current_rr < floor - 1e-9:
                return 'rewrite', floor, f"custom/default R:R: current 1:{round(current_rr, 4)} < 1:1; raise TP to 1:1"
            return 'noop', None, f"custom/default R:R: current 1:{round(current_rr, 4)} >= 1:1"

        return 'unsupported', None, f"unsupported risk_reward_type '{rr_type}'"

    # ------------------------------------------------------------------
    # MT5 connection check
    # ------------------------------------------------------------------
    acc = mt5_account_info()
    if acc is None:
        print(f"  ⚠️ MT5 not connected — cannot correct reward")
        stats['errors'].append('MT5 not connected')
        return stats

    try:
        current_balance = float(acc.balance)
    except Exception:
        current_balance = 0.0
    print(f"  💰 Current MT5 balance: ${current_balance:.2f}")

    # ------------------------------------------------------------------
    # Enumeration — skipped when jobs= passed in
    # ------------------------------------------------------------------
    if jobs is None:
        print("\n" + "─" * 80)
        print(" 📁 LOADING INVESTOR JOBS FROM DB (VPS-scoped)")
        print("─" * 80)

        local_ip = get_local_ip()
        if not local_ip:
            print(" ⚠️ Could not determine local IP")
            stats['errors'].append('local IP unknown')
            return stats
        print(f" │ Local IP: {local_ip}")

        all_jobs = fetch_vps_investor_jobs(local_ip)
        if not all_jobs:
            print(" │ ℹ️ No active investor jobs found for this VPS")
            stats['processing_success'] = True
            return stats

        if inv_db_id:
            jobs = [j for j in all_jobs if str(j.get('investor_id')) == str(inv_db_id)]
            if not jobs:
                print(f" ⚠️ Investor {inv_db_id} not found in any VPS-scoped job")
                stats['errors'].append(f"Investor {inv_db_id} not found")
                return stats
        else:
            jobs = all_jobs

        print(f" │ Loaded {len(jobs)} investor job(s) from DB")
        print("─" * 80)

    # ------------------------------------------------------------------
    # Per-investor loop
    # ------------------------------------------------------------------
    for job in jobs:
        investor_id    = job.get('investor_id')
        sub_account_id = job.get('sub_account_id')
        programme_id   = job.get('programme_id')
        programme_name = job.get('programme_name') or f"programme_{programme_id}"

        if investor_id is None or sub_account_id is None or programme_id is None:
            stats['investors_skipped'] += 1
            continue

        print(f"\n{'─'*72}")
        print(f" 📐 Investor {investor_id} (sub {sub_account_id}) | "
              f"programme '{programme_name}' (id={programme_id})")
        print(f"{'─'*72}")

        stats['investors_processed'] += 1

        programme_userid = resolve_programme_userid(programme_id, job)
        if not programme_userid:
            print(f"  │ ⚠️ Programme owner userid unavailable for programme {programme_id} — skipping investor")
            stats['investors_skipped'] += 1
            continue

        cfg = _programme_cfg(programme_id)
        rr_type = cfg.get('rr_type_norm') if cfg else None
        if rr_type not in ('fixed_risk_reward', 'minimum_risk_reward', 'custom_and_default_risk_reward'):
            print(f"  │ ⚠️ Unsupported/missing risk_reward_type for programme {programme_id} — skipping investor")
            stats['investors_skipped'] += 1
            continue
        if rr_type in ('fixed_risk_reward', 'minimum_risk_reward') and (
            cfg.get('rr_value') is None or float(cfg['rr_value']) <= 0
        ):
            print(f"  │ ⚠️ {rr_type} requires a positive risk_reward value — skipping investor")
            stats['investors_skipped'] += 1
            continue

        rr_value = (float(cfg['rr_value'])
                    if rr_type in ('fixed_risk_reward', 'minimum_risk_reward')
                    else 1.0)
        rr_type_raw = cfg['rr_type_raw']
        rr_source   = cfg['source']

        stats['programme_rr'] = rr_value
        stats['programme_rr_type_raw'] = rr_type_raw
        stats['programme_rr_type_norm'] = rr_type
        stats['mode'] = rr_type

        print(f"  │ Programme R:R value : {'no configured target (1:1 floor)' if rr_type == 'custom_and_default_risk_reward' else '1:' + str(rr_value)}")
        print(f"  │ Programme R:R type  : {rr_type_raw or '—'}")
        print(f"  │ Normalised type     : '{rr_type}'")
        print(f"  │ Source              : {rr_source}")

        # Only adjust MT5 entities whose magic belongs to a LIVE inherited programme
        # signal created on/after this investor subaccount's execution start.
        hub = db_fetch_one(
            f"SELECT execution_start_date FROM harvhub "
            f"WHERE id = {int(investor_id)} AND sub_account_id = {int(sub_account_id)} LIMIT 1",
            None, timeout=10.0, quiet=True,
        ) or {}
        live_rows, eligibility = fetch_live_investor_programme_trades(
            programme_id, programme_userid, investor_id, sub_account_id,
            _raw_first_key(hub, 'execution_start_date'), pending_only=False,
        )
        known_tradeids = {
            tid for tid in (_coerce_int(_raw_first_key(r, 'tradeid')) for r in live_rows)
            if tid is not None and tid > 0
        }
        if eligibility.get('reason'):
            print(f"  │ ⚠️ Live-trade R:R correction gate closed: {eligibility['reason']}")
        if not known_tradeids:
            print(f"  │ ℹ️ No live inherited programme trade IDs — no MT5 positions/orders will be changed")
            stats['investors_skipped'] += 1
            continue

        # Snapshot MT5 once per investor
        try:
            live_pending = mt5_orders_get() or []
        except Exception:
            live_pending = []
        try:
            live_positions = mt5_positions_get() or []
        except Exception:
            live_positions = []

        # ==============================================================
        # SECTION A — OPEN POSITIONS (MARKET)
        # ==============================================================
        if live_positions:
            print(f"\n  └─ 🔍 Scanning {len(live_positions)} open positions (MARKET)…")

        for pos in live_positions:
            try:
                position_magic = int(getattr(pos, 'magic', 0) or 0)
            except Exception:
                position_magic = 0
            if position_magic not in known_tradeids:
                continue
            stats['positions_checked'] += 1

            sym  = getattr(pos, 'symbol', None)
            tkt  = getattr(pos, 'ticket', None)
            mt5_type_pos = int(getattr(pos, 'type', -1))
            is_buy = (mt5_type_pos == 0)

            if not sym:
                stats['positions_skipped'] += 1
                continue

            si = _get_symbol_info(sym)
            if si is None:
                stats['positions_skipped'] += 1
                continue

            digits = _digits(si)

            print(f"\n    └─ 📋 Position #{tkt} | "
                  f"{'BUY (MARKET)' if is_buy else 'SELL (MARKET)'} | {sym}")

            entry = float(getattr(pos, 'price_open', 0) or 0)
            sl    = float(getattr(pos, 'sl', 0) or 0)
            tp    = float(getattr(pos, 'tp', 0) or 0)

            if sl <= 0:
                print(f"       ⏭️ No SL — cannot compute R:R. Skipping.")
                stats['positions_skipped'] += 1
                continue

            current_rr, risk_dist, reward_dist = _compute_current_rr(entry, sl, tp)
            sl_on_correct_side = (sl < entry) if is_buy else (sl > entry)
            tp_on_correct_side = (tp > entry) if is_buy else (tp < entry)
            direction_valid = bool(sl_on_correct_side and tp_on_correct_side)
            if not sl_on_correct_side:
                print(f"       ⚠️ Stoploss is on the invalid side of entry; cannot correct R:R safely")
                stats['positions_skipped'] += 1
                continue
            if not tp_on_correct_side:
                current_rr = None

            decision, target_for_calc, reason = _decide_action(
                current_rr, rr_value, rr_type, direction_valid=direction_valid,
                rr_tolerance=_rr_precision_tolerance(entry, sl, si),
            )
            print(f"       → {reason}")

            if decision in ('noop', 'unsupported'):
                stats['positions_skipped'] += 1
                stats['rr_matches'][f"noop_{rr_type}"] = (
                    stats['rr_matches'].get(f"noop_{rr_type}", 0) + 1
                )
                continue

            # Effective target R:R for this action
            effective_rr = float(target_for_calc)

            # Synthesize TP if needed (tp == 0)
            tp_synthesized = False
            if tp <= 0:
                new_tp = _synthesize_tp(
                    entry, sl, effective_rr, is_buy, digits,
                    tick_size=float(getattr(si, 'trade_tick_size', 0) or getattr(si, 'point', 0) or 0),
                    round_up=(rr_type in ('minimum_risk_reward', 'custom_and_default_risk_reward')),
                )
                if new_tp is None:
                    print(f"       ⚠️ Could not synthesize TP (entry={entry}, sl={sl}, "
                          f"rr=1:{effective_rr})")
                    stats['positions_error'] += 1
                    stats['orders_error'] += 1
                    continue
                tp_synthesized = True
                stats['rows_tp_synthesized'] += 1
                print(f"       📝 No TP set — synthesizing to {new_tp} "
                      f"(target R:R = 1:{effective_rr})")
                new_tp_final = new_tp
            else:
                current_rr_v, _, _ = _compute_current_rr(entry, sl, tp)
                if current_rr_v is None:
                    print(f"       ⚠️ Cannot compute current R:R. Skipping.")
                    stats['positions_error'] += 1
                    stats['orders_error'] += 1
                    continue
                # Build the new TP that yields effective_rr
                synthesized = _synthesize_tp(
                    entry, sl, effective_rr, is_buy, digits,
                    tick_size=float(getattr(si, 'trade_tick_size', 0) or getattr(si, 'point', 0) or 0),
                    round_up=(rr_type in ('minimum_risk_reward', 'custom_and_default_risk_reward')),
                )
                if synthesized is None:
                    print(f"       ⚠️ Could not synthesize TP. Skipping.")
                    stats['positions_error'] += 1
                    stats['orders_error'] += 1
                    continue
                new_tp_final = synthesized

            # Direction sanity
            if is_buy and new_tp_final <= entry:
                print(f"       ⚠️ Synthesized TP ({new_tp_final}) not above entry — skipping")
                stats['positions_skipped'] += 1
                continue
            if (not is_buy) and new_tp_final >= entry:
                print(f"       ⚠️ Synthesized TP ({new_tp_final}) not below entry — skipping")
                stats['positions_skipped'] += 1
                continue

            old_rr_disp = f"{round(current_rr, 4)}" if current_rr is not None else "—"
            print(f"       entry={entry}  sl={sl}  tp={tp if tp > 0 else '—'}")
            print(f"       current R:R = 1:{old_rr_disp}  →  target R:R = 1:{effective_rr}")
            print(f"       new TP = {new_tp_final}")

            ok, msg = _modify_position_tp(tkt, sl, new_tp_final, digits)
            if ok:
                stats['positions_adjusted'] += 1
                stats['orders_adjusted'] += 1
                stats['rr_matches'][f"{rr_type}_{effective_rr}"] = (
                    stats['rr_matches'].get(f"{rr_type}_{effective_rr}", 0) + 1
                )
                print(f"       ✅ TP adjusted successfully to {new_tp_final}")
            else:
                stats['positions_error'] += 1
                stats['orders_error'] += 1
                print(f"       ✗ Modify failed: {msg}")

        # ==============================================================
        # SECTION B — PENDING ORDERS (LIMIT & STOP)
        # ==============================================================
        if live_pending:
            print(f"\n  └─ 🔍 Scanning {len(live_pending)} pending orders (LIMIT & STOP)…")

        for order in live_pending:
            try:
                order_magic = int(getattr(order, 'magic', 0) or 0)
            except Exception:
                order_magic = 0
            if order_magic not in known_tradeids:
                continue
            stats['orders_checked'] += 1

            sym = getattr(order, 'symbol', None)
            tkt = getattr(order, 'ticket', None)
            mt5_type_ord = int(getattr(order, 'type', -1))
            is_buy = mt5_type_ord in (
                mt5.ORDER_TYPE_BUY_LIMIT,
                mt5.ORDER_TYPE_BUY_STOP,
                mt5.ORDER_TYPE_BUY_STOP_LIMIT,
            )

            if not sym:
                stats['orders_skipped'] += 1
                continue

            si = _get_symbol_info(sym)
            if si is None:
                stats['orders_skipped'] += 1
                continue

            digits = _digits(si)

            # Magic == tradeid lookup for logging
            try:
                tradeid = int(getattr(order, 'magic', 0) or 0)
            except Exception:
                tradeid = 0
            in_db = tradeid in known_tradeids if tradeid else False

            print(f"\n    └─ 📋 Order #{tkt} | "
                  f"{'BUY' if is_buy else 'SELL'} | {sym} | magic={tradeid}"
                  f"{'  (known)' if in_db else ''}")

            entry = float(getattr(order, 'price_open', 0) or 0)
            sl    = float(getattr(order, 'sl', 0) or 0)
            tp    = float(getattr(order, 'tp', 0) or 0)

            if sl <= 0:
                print(f"       ⏭️ No SL — cannot compute R:R. Skipping.")
                stats['orders_skipped'] += 1
                continue

            current_rr, risk_dist, reward_dist = _compute_current_rr(entry, sl, tp)
            sl_on_correct_side = (sl < entry) if is_buy else (sl > entry)
            tp_on_correct_side = (tp > entry) if is_buy else (tp < entry)
            direction_valid = bool(sl_on_correct_side and tp_on_correct_side)
            if not sl_on_correct_side:
                print(f"       ⚠️ Stoploss is on the invalid side of entry; cannot correct R:R safely")
                stats['orders_skipped'] += 1
                continue
            if not tp_on_correct_side:
                current_rr = None

            decision, target_for_calc, reason = _decide_action(
                current_rr, rr_value, rr_type, direction_valid=direction_valid,
                rr_tolerance=_rr_precision_tolerance(entry, sl, si),
            )
            print(f"       → {reason}")

            if decision in ('noop', 'unsupported'):
                stats['orders_skipped'] += 1
                stats['rr_matches'][f"noop_{rr_type}"] = (
                    stats['rr_matches'].get(f"noop_{rr_type}", 0) + 1
                )
                continue

            effective_rr = float(target_for_calc)

            tp_synthesized = False
            if tp <= 0:
                new_tp = _synthesize_tp(
                    entry, sl, effective_rr, is_buy, digits,
                    tick_size=float(getattr(si, 'trade_tick_size', 0) or getattr(si, 'point', 0) or 0),
                    round_up=(rr_type in ('minimum_risk_reward', 'custom_and_default_risk_reward')),
                )
                if new_tp is None:
                    print(f"       ⚠️ Could not synthesize TP (entry={entry}, sl={sl}, "
                          f"rr=1:{effective_rr})")
                    stats['orders_error'] += 1
                    continue
                tp_synthesized = True
                stats['rows_tp_synthesized'] += 1
                print(f"       📝 No TP set — synthesizing to {new_tp} "
                      f"(target R:R = 1:{effective_rr})")
                new_tp_final = new_tp
            else:
                synthesized = _synthesize_tp(
                    entry, sl, effective_rr, is_buy, digits,
                    tick_size=float(getattr(si, 'trade_tick_size', 0) or getattr(si, 'point', 0) or 0),
                    round_up=(rr_type in ('minimum_risk_reward', 'custom_and_default_risk_reward')),
                )
                if synthesized is None:
                    print(f"       ⚠️ Could not synthesize TP. Skipping.")
                    stats['orders_error'] += 1
                    continue
                new_tp_final = synthesized

            if is_buy and new_tp_final <= entry:
                print(f"       ⚠️ Synthesized TP ({new_tp_final}) not above entry — skipping")
                stats['orders_skipped'] += 1
                continue
            if (not is_buy) and new_tp_final >= entry:
                print(f"       ⚠️ Synthesized TP ({new_tp_final}) not below entry — skipping")
                stats['orders_skipped'] += 1
                continue

            old_rr_disp = f"{round(current_rr, 4)}" if current_rr is not None else "—"
            print(f"       entry={entry}  sl={sl}  tp={tp if tp > 0 else '—'}")
            print(f"       current R:R = 1:{old_rr_disp}  →  target R:R = 1:{effective_rr}")
            print(f"       new TP = {new_tp_final}")

            ok, msg = _modify_pending_tp(tkt, entry, sl, new_tp_final, digits)
            if ok:
                stats['orders_adjusted'] += 1
                stats['rr_matches'][f"{rr_type}_{effective_rr}"] = (
                    stats['rr_matches'].get(f"{rr_type}_{effective_rr}", 0) + 1
                )
                print(f"       ✅ TP adjusted successfully to {new_tp_final}")
            else:
                stats['orders_error'] += 1
                print(f"       ✗ Modify failed: {msg}")

        stats['programmes_processed'] += 1

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print(f"\n{'='*10} 📐 ORDERS REWARD CORRECTION SUMMARY (DB) {'='*10}")
    print(f"  Investor ID                     : {stats['investor_id']}")
    print(f"  Investors processed             : {stats['investors_processed']}")
    print(f"  Investors skipped               : {stats['investors_skipped']}")
    print(f"  Mode (R:R type)                 : {stats['mode'] or '—'}")
    if stats['programme_rr'] is not None:
        print(f"  Programme R:R value             : 1:{stats['programme_rr']}")
    print(f"  Positions checked               : {stats['positions_checked']}")
    print(f"  Positions adjusted              : {stats['positions_adjusted']}")
    print(f"  Positions skipped               : {stats['positions_skipped']}")
    print(f"  Positions errors                : {stats['positions_error']}")
    print(f"  Pending orders checked          : {stats['orders_checked']}")
    print(f"  Pending orders adjusted         : {stats['orders_adjusted']}")
    print(f"  Pending orders skipped          : {stats['orders_skipped']}")
    print(f"  Pending orders errors           : {stats['orders_error']}")
    print(f"  Rows TP synthesized             : {stats['rows_tp_synthesized']}")
    print(f"  Total checked                   : {stats['positions_checked'] + stats['orders_checked']}")
    print(f"  Total adjusted                  : {stats['positions_adjusted'] + stats['orders_adjusted']}")
    if stats['rr_matches']:
        print(f"\n  📊 R:R Usage Breakdown:")
        for k, v in sorted(stats['rr_matches'].items()):
            print(f"     • [{k}]: {v}")
    print(f"  Errors                          : {len(stats['errors'])}")
    for e in stats['errors']:
        print(f"     • {e}")
    print(f"{'='*10} 🏁 COMPLETE {'='*10}\n")

    stats['processing_success'] = True
    return stats

# ==============================================================================
#  APPLY DYNAMIC BREAKEVEN — DB version
#
#  Investor-scoped. For every OPEN MT5 position belonging to the investor
#  (magic == programme_trades.tradeid), compute the running reward multiple
#  (current floating profit / risk at SL) and — if it meets a threshold from
#  accountmanagement_trades_breakeven — push the stop loss forward.
#
#  READS:
#    * accountmanagement_trades_breakeven   -> programme-scoped rules:
#                                                floating_profit_at_risk_reward
#                                                breakeven_at_risk_reward
#    * harvhub                              -> investor identity (for logs)
#    * MT5                                  -> open positions + tick data
#
#  WRITES:
#    * MT5 -> TRADE_ACTION_SLTP on open positions (sl only; tp preserved)
#
#  RULES (mirrors apply_dynamic_breakeven_local)
#  ---------------------------------------------
#    * Only PROFITABLE positions are considered (position.profit > 0).
#    * Risk is computed as |entry - SL| × volume × contract_size
#      (falls back to mt5.order_calc_profit on the SL leg, then to
#       0.5 × profit as a last resort — same as the local version).
#    * current_r_multiple = position.profit / risk_usd
#    * Match the HIGHEST rule whose if_floating_profit_at_risk_reward is
#      satisfied by current_r_multiple.
#    * Target SL = entry ± (risk_distance × breakeven_at_risk_reward)
#      (plus for BUY, minus for SELL).
#    * Fallback ladder: if the target SL violates stops_level or the
#      direction check, drop to the next-lower rule and retry.
#
#  NO DEDUP:
#    Every row in accountmanagement_trades_breakeven is respected exactly
#    as it exists in the DB. Rows that share a breakeven_at_risk_reward
#    (but differ in trigger) are ALL loaded. The fallback ladder resolves
#    any practical overlap at runtime.
#
#  Note: MT5 preserves our tp as-is; we only push the SL.
# ==============================================================================
def apply_dynamic_breakeven_db(inv_db_id=None, jobs=None):
    print(f"\n{'='*10} 🎯 DYNAMIC BREAKEVEN (DB) {'='*10}")

    stats = {
        'investor_id':                inv_db_id if inv_db_id else 'all',
        'investors_processed':        0,
        'investors_skipped':          0,
        'programmes_processed':       0,
        'positions_checked':          0,
        'positions_profitable':       0,
        'positions_adjusted':         0,
        'positions_skipped':          0,
        'positions_error':            0,
        'breakeven_events':           0,
        'rules_loaded':               0,
        'errors':                     [],
        'details':                    [],
        'processing_success':         False,
    }

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    _symbol_info_cache = {}
    _tick_cache = {}

    def _get_symbol_info(sym):
        si = _symbol_info_cache.get(sym)
        if si is None:
            si = mt5_symbol_info(sym)
            if si is None:
                mt5_symbol_select(sym, True)
                si = mt5_symbol_info(sym)
            _symbol_info_cache[sym] = si
        return si

    def _get_tick(sym):
        t = _tick_cache.get(sym)
        if t is None:
            t = mt5_symbol_tick(sym)
            _tick_cache[sym] = t
        return t

    def _digits(si):
        try:
            if si is not None and hasattr(si, 'digits'):
                d = int(si.digits)
                if d >= 0:
                    return d
        except Exception:
            pass
        return 5

    # ------------------------------------------------------------------
    # Programme-scoped reader (cached per run)
    #
    # NO DEDUP: every row returned by the DB is preserved. Rows are
    # ordered ascending by trigger so the per-position code can pick
    # `applicable_rules[-1]` (the highest-trigger match) and then walk
    # downward through every lower-index rule as a fallback.
    # ------------------------------------------------------------------
    _breakeven_cache = {}

    def _load_breakeven_rules(programme_id):
        cached = _breakeven_cache.get(programme_id)
        if cached is not None:
            return cached

        rows = db_fetch_all(
            """
            SELECT id,
                   floating_profit_at_risk_reward,
                   breakeven_at_risk_reward
              FROM accountmanagement_trades_breakeven
             WHERE programme_id = %s
             ORDER BY floating_profit_at_risk_reward ASC, id ASC
            """,
            [programme_id], timeout=10.0, quiet=True,
        )

        cleaned = []
        for r in rows:
            trig = _coerce_float(
                _raw_first_key(r, 'floating_profit_at_risk_reward'),
                default=None,
            )
            tgt = _coerce_float(
                _raw_first_key(r, 'breakeven_at_risk_reward'),
                default=None,
            )
            if trig is None or tgt is None:
                continue
            row_id = _coerce_int(_raw_first_key(r, 'id'))
            cleaned.append({
                'id':                                 row_id,
                'if_floating_profit_at_risk_reward':  float(trig),
                'breakeven_at_risk_reward':           float(tgt),
            })

        # Stable ascending sort by trigger, then by id — no dedup.
        cleaned.sort(
            key=lambda x: (
                x['if_floating_profit_at_risk_reward'],
                x['id'] if x['id'] is not None else 0,
            )
        )

        print(f"  │ [BE] loaded {len(cleaned)} rule(s) "
              f"from DB (rows fetched = {len(rows)})")

        _breakeven_cache[programme_id] = cleaned
        return cleaned

    # ------------------------------------------------------------------
    # MT5 connection check
    # ------------------------------------------------------------------
    acc = mt5_account_info()
    if acc is None:
        print(f"  ⚠️ MT5 not connected — cannot apply dynamic breakeven")
        stats['errors'].append('MT5 not connected')
        return stats

    # ------------------------------------------------------------------
    # Enumeration — skipped when jobs= passed in
    # ------------------------------------------------------------------
    if jobs is None:
        print("\n" + "─" * 80)
        print(" 📁 LOADING INVESTOR JOBS FROM DB (VPS-scoped)")
        print("─" * 80)

        local_ip = get_local_ip()
        if not local_ip:
            print(" ⚠️ Could not determine local IP")
            stats['errors'].append('local IP unknown')
            return stats
        print(f" │ Local IP: {local_ip}")

        all_jobs = fetch_vps_investor_jobs(local_ip)
        if not all_jobs:
            print(" │ ℹ️ No active investor jobs found for this VPS")
            stats['processing_success'] = True
            return stats

        if inv_db_id:
            jobs = [j for j in all_jobs if str(j.get('investor_id')) == str(inv_db_id)]
            if not jobs:
                print(f" ⚠️ Investor {inv_db_id} not found in any VPS-scoped job")
                stats['errors'].append(f"Investor {inv_db_id} not found")
                return stats
        else:
            jobs = all_jobs

        print(f" │ Loaded {len(jobs)} investor job(s) from DB")
        print("─" * 80)

    # ------------------------------------------------------------------
    # Per-investor loop
    # ------------------------------------------------------------------
    for job in jobs:
        investor_id    = job.get('investor_id')
        sub_account_id = job.get('sub_account_id')
        programme_id   = job.get('programme_id')
        programme_name = job.get('programme_name') or f"programme_{programme_id}"
        programme_userid = resolve_programme_userid(programme_id, job)

        if investor_id is None or sub_account_id is None or programme_id is None or not programme_userid:
            stats['investors_skipped'] += 1
            continue

        print(f"\n{'─'*72}")
        print(f" 🎯 Investor {investor_id} (sub {sub_account_id}) | "
              f"programme '{programme_name}' (id={programme_id})")
        print(f"{'─'*72}")

        stats['investors_processed'] += 1
        stats['programmes_processed'] += 1

        # -------- load programme breakeven rules --------
        breakeven_config = _load_breakeven_rules(programme_id)
        if not breakeven_config:
            print(f"  │ ⏭️ No breakeven rules configured for programme {programme_id}")
            stats['investors_skipped'] += 1
            continue

        stats['rules_loaded'] += len(breakeven_config)
        print(f"  │ ✅ Loaded {len(breakeven_config)} breakeven level(s):")
        for rule in breakeven_config:
            rid_display = f"id={rule['id']}" if rule.get('id') is not None else "id=—"
            print(f"  │    • [{rid_display}] Floating profit at >= "
                  f"{rule['if_floating_profit_at_risk_reward']}R → "
                  f"SL at {rule['breakeven_at_risk_reward']}R")

        # -------- load current LIVE tradeids from this subaccount's inherited programme-trades copies --------
        hub = db_fetch_one(
            f"SELECT execution_start_date FROM harvhub "
            f"WHERE id = {int(investor_id)} AND sub_account_id = {int(sub_account_id)} LIMIT 1",
            None, timeout=10.0, quiet=True,
        ) or {}
        live_rows, eligibility = fetch_live_investor_programme_trades(
            programme_id, programme_userid, investor_id, sub_account_id,
            _raw_first_key(hub, 'execution_start_date'), pending_only=False,
        )
        known_tradeids = {
            tid for tid in (_coerce_int(_raw_first_key(r, 'tradeid')) for r in live_rows)
            if tid is not None and tid > 0
        }
        if eligibility.get('reason'):
            print(f"  │ ⚠️ Live-trade breakeven gate closed: {eligibility['reason']}")

        # -------- snapshot live positions --------
        try:
            positions = mt5_positions_get() or []
        except Exception:
            positions = []

        if not positions:
            print(f"  │ 📭 No open positions")
            continue

        # Filter by known tradeids (magic == tradeid)
        investor_positions = []
        for pos in positions:
            try:
                m = int(getattr(pos, 'magic', 0) or 0)
            except Exception:
                m = 0
            if m in known_tradeids:
                investor_positions.append(pos)

        if not investor_positions:
            print(f"  │ 📭 No positions belonging to this investor "
                  f"(by magic/tradeid match)")
            continue

        # -------- keep only profitable ones --------
        profitable_positions = [p for p in investor_positions
                                if float(getattr(p, 'profit', 0) or 0) > 0]

        stats['positions_checked'] += len(investor_positions)
        stats['positions_profitable'] += len(profitable_positions)

        if not profitable_positions:
            print(f"  │ 📭 {len(investor_positions)} position(s) checked, "
                  f"none profitable — nothing to do")
            continue

        print(f"  │ 🔍 {len(profitable_positions)} profitable position(s) to audit")

        for idx, position in enumerate(profitable_positions, 1):
            print(f"\n  📊 POSITION #{idx}/{len(profitable_positions)}")
            print(f"  ────────────────────────────────────────")
            print(f"    Ticket:      #{position.ticket}")
            print(f"    Symbol:      {position.symbol}")
            print(f"    Type:        {'BUY' if position.type == 0 else 'SELL'}")
            print(f"    Volume:      {position.volume}")
            print(f"    Open Price:  {position.price_open:.5f}")
            print(f"    Current SL:  {position.sl:.5f}")
            print(f"    Current TP:  {position.tp:.5f}")
            print(f"    Profit/Loss: ${position.profit:.2f}")

            si = _get_symbol_info(position.symbol)
            if si is None:
                print(f"    ⚠️ Symbol info not found — skipping")
                stats['positions_skipped'] += 1
                continue

            tick = _get_tick(position.symbol)
            if tick is None:
                print(f"    ⚠️ Tick info not found — skipping")
                stats['positions_skipped'] += 1
                continue

            is_buy = (position.type == 0)
            current_price = tick.ask if is_buy else tick.bid
            digits = _digits(si)

            print(f"    Current Price: {current_price:.5f}")

            # -------- risk distance --------
            if position.sl == 0:
                print(f"    ⚠️ No stop loss set — skipping")
                stats['positions_skipped'] += 1
                continue

            if is_buy:
                risk_distance = position.price_open - position.sl
            else:
                risk_distance = position.sl - position.price_open

            if risk_distance <= 0:
                print(f"    ⚠️ Invalid risk distance: {risk_distance:.5f} — skipping")
                stats['positions_error'] += 1
                continue

            print(f"    Risk Distance: {risk_distance:.5f} points")

            # -------- risk USD (with local's fallback ladder) --------
            calc_type = mt5.ORDER_TYPE_BUY if is_buy else mt5.ORDER_TYPE_SELL
            sl_profit = mt5_order_calc_profit(
                calc_type, position.symbol, position.volume,
                position.price_open, position.sl,
            )

            if sl_profit is None or sl_profit == 0:
                print(f"    ⚠️ order_calc_profit returned {sl_profit} — using fallback")
                try:
                    contract_size = float(getattr(si, 'trade_contract_size', 0) or 0)
                    if contract_size > 0:
                        risk_usd = abs(risk_distance * position.volume * contract_size)
                    else:
                        risk_usd = abs(float(position.profit)) * 0.5
                except Exception:
                    risk_usd = abs(float(position.profit)) * 0.5
            else:
                risk_usd = round(abs(float(sl_profit)), 2)

            if risk_usd <= 0:
                print(f"    ⚠️ Risk USD is zero or negative ({risk_usd:.2f}) — skipping")
                stats['positions_skipped'] += 1
                continue

            current_r_multiple = float(position.profit) / risk_usd if risk_usd > 0 else 0.0

            print(f"    Risk Amount:  ${risk_usd:.2f}")
            print(f"    Current R/R:  {current_r_multiple:.2f}R")

            # -------- find applicable rules --------
            applicable_rules = [
                rule for rule in breakeven_config
                if current_r_multiple >= rule['if_floating_profit_at_risk_reward']
            ]

            if not applicable_rules:
                min_thresh = breakeven_config[0]['if_floating_profit_at_risk_reward']
                print(f"    ⏭️ Current R/R ({current_r_multiple:.2f}R) below minimum "
                      f"threshold ({min_thresh}R) — skipping")
                stats['positions_skipped'] += 1
                continue

            highest_rule = applicable_rules[-1]
            target_reward = highest_rule['breakeven_at_risk_reward']

            print(f"    ✅ Matched: floating >= "
                  f"{highest_rule['if_floating_profit_at_risk_reward']}R "
                  f"→ SL at {target_reward}R "
                  f"(rule id={highest_rule.get('id')})")

            # -------- stops level --------
            try:
                stops_level_points = max(int(getattr(si, 'trade_stops_level', 0) or 0), 10)
            except Exception:
                stops_level_points = 10
            try:
                point = float(getattr(si, 'point', 0.0) or 0.0)
                if point <= 0:
                    point = 0.00001
            except Exception:
                point = 0.00001
            stoplevel = stops_level_points * point
            print(f"    Stop Level:   {stoplevel:.5f}")

            # -------- fallback ladder --------
            # Walk from the highest-trigger match downward through every
            # lower-index rule. Rows sharing a breakeven level are still
            # tried independently because nothing is deduped.
            current_index = breakeven_config.index(highest_rule)
            sl_set = False

            for i in range(current_index, -1, -1):
                test_reward = breakeven_config[i]['breakeven_at_risk_reward']
                test_rule_id = breakeven_config[i].get('id')

                if is_buy:
                    target_sl = position.price_open + (risk_distance * test_reward)
                    is_valid_direction = (
                        target_sl < current_price - stoplevel and
                        target_sl >= position.sl - (point * 5)
                    )
                    print(f"    └─ BUY attempt: target SL={target_sl:.5f} "
                          f"({test_reward}R, id={test_rule_id}) "
                          f"valid={'YES' if is_valid_direction else 'NO'}")
                else:
                    target_sl = position.price_open - (risk_distance * test_reward)
                    is_valid_direction = (
                        target_sl > current_price + stoplevel and
                        target_sl <= position.sl + (point * 5)
                    )
                    print(f"    └─ SELL attempt: target SL={target_sl:.5f} "
                          f"({test_reward}R, id={test_rule_id}) "
                          f"valid={'YES' if is_valid_direction else 'NO'}")

                if not is_valid_direction:
                    continue

                target_sl = round(target_sl, digits)
                if target_sl <= 0:
                    print(f"       ⚠️ Invalid target SL value {target_sl} — skipping")
                    continue

                # ---- send modify ----
                req = {
                    "action": mt5.TRADE_ACTION_SLTP,
                    "position": int(position.ticket),
                    "sl": float(target_sl),
                    "tp": float(position.tp),
                }
                try:
                    res = mt5_order_send(req)
                except Exception as e:
                    print(f"       ✗ Modify raised: {e}")
                    res = None

                if res is not None and res.retcode == mt5.TRADE_RETCODE_DONE:
                    stats['positions_adjusted'] += 1
                    stats['breakeven_events'] += 1
                    sl_set = True
                    print(f"       ✅ SUCCESS — SL moved to {target_sl:.5f} "
                          f"({test_reward}R)")
                    stats['details'].append({
                        'investor_id':    investor_id,
                        'sub_account_id': sub_account_id,
                        'programme_id':   programme_id,
                        'ticket':         position.ticket,
                        'symbol':         position.symbol,
                        'direction':      'buy' if is_buy else 'sell',
                        'r_multiple':     round(current_r_multiple, 4),
                        'target_r':       float(test_reward),
                        'rule_id':        test_rule_id,
                        'new_sl':         float(target_sl),
                        'old_sl':         float(position.sl),
                        'action':         'sl_moved',
                    })
                    break
                else:
                    err = (getattr(res, 'comment', '') if res is not None
                           else f"no_response last_error={mt5_last_error()}")
                    retcode = getattr(res, 'retcode', 'None') if res is not None else 'None'
                    print(f"       ✗ Modify failed: {err} (retcode={retcode})")

            if not sl_set:
                stats['positions_error'] += 1
                print(f"    ✗ All fallback attempts failed for #{position.ticket}")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print(f"\n{'='*10} 🎯 DYNAMIC BREAKEVEN SUMMARY (DB) {'='*10}")
    print(f"  Investors processed       : {stats['investors_processed']}")
    print(f"  Investors skipped         : {stats['investors_skipped']}")
    print(f"  Programmes processed      : {stats['programmes_processed']}")
    print(f"  Rules loaded (total)      : {stats['rules_loaded']}")
    print(f"  Positions checked         : {stats['positions_checked']}")
    print(f"  Positions profitable      : {stats['positions_profitable']}")
    print(f"  Positions adjusted        : {stats['positions_adjusted']}")
    print(f"  Positions skipped         : {stats['positions_skipped']}")
    print(f"  Positions errors          : {stats['positions_error']}")
    print(f"  Breakeven events          : {stats['breakeven_events']}")
    print(f"  Errors                    : {len(stats['errors'])}")
    for e in stats['errors']:
        print(f"     • {e}")
    print(f"{'='*10} 🏁 COMPLETE {'='*10}\n")

    stats['processing_success'] = True
    return stats

# ==============================================================================
#  TRADES ANALYTICS — DB version
#
#  Investor-scoped. Walks MT5 history from execution_start_date → contract
#  expiry (or now), groups deals by position_id, and writes:
#
#    * investors_analytics   — ONE row per (userid, sub_account_id) with all the
#                              flat scalars your useranalytics.php consumes.
#    * authorized_trades     — one row per authorized closed trade.
#    * unauthorized_trades   — one row per unauthorized closed trade.
#
#  AUTHORIZATION
#  -------------
#  A trade is AUTHORIZED iff its entry_deal.magic resolves through the exact
#  investor-subaccount copy, canonical programme_trades row, chart identity,
#  and matching programme_analytics group. Magic values shared by programmes
#  or unresolved groups are kept out of this programme's authorized/unauthorized
#  detail tables rather than being guessed.
#
#  WRITES (all idempotent — upsert where a natural key exists):
#    investors_analytics:
#      total_trades, total_pnl, profit_trades, loss_trades,
#      profit_amount, loss_amount,
#      lowest_trades_per_day, highest_trades_per_day, average_trades_per_day,
#      lowest_trades_per_week, highest_trades_per_week, average_trades_per_week,
#      highest_loss_per_trade, highest_drawdown,
#      symbols_traded, closed_deals_with_sl_tp, closed_deals_without_sl_tp,
#      consecutive_losses_count, total_loss_pnl,
#      consecutive_days_in_loss_count,
#      consecutive_days_in_loss_count_total_loss_pnl,
#      revenue_percentage, revenue_profit_percentage, revenue_loss_percentage,
#      recent_risk_reward
#    authorized_trades / unauthorized_trades:
#      one row per trade (dedup by ticket).
#
#  No JSON. No local folders. Magic == programme_trades.tradeid.
# ==============================================================================
def trades_analytics_db(inv_db_id=None, jobs=None):
    print(f"\n{'='*10} 📊 TRADES ANALYTICS (DB) {'='*10}")

    from datetime import datetime, timedelta, timezone
    import re as _re

    # ------------------------------------------------------------------
    # LAGOS TIMEZONE
    # ------------------------------------------------------------------
    try:
        from zoneinfo import ZoneInfo
        LAGOS_TZ = ZoneInfo("Africa/Lagos")
        _LAGOS_TZ_SOURCE = "zoneinfo"
    except Exception:
        LAGOS_TZ = timezone(timedelta(hours=1))
        _LAGOS_TZ_SOURCE = "fixed_utc+1"

    def lagos_now():
        return datetime.now(LAGOS_TZ)

    stats = {
        'investor_id':             inv_db_id if inv_db_id else 'all',
        'investors_processed':     0,
        'investors_skipped':       0,
        'programmes_processed':    0,
        'total_deals_seen':        0,
        'total_trades_grouped':    0,
        'authorized_trades':       0,
        'unauthorized_trades':     0,
        'analytics_rows_upserted': 0,
        'authorized_rows_written': 0,
        'unauthorized_rows_written': 0,
        'other_programme_trades':  0,
        'unverified_trades':       0,
        'ambiguous_trades':        0,
        'errors':                  [],
        'details':                 [],
        'processing_success':      False,
    }

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    def _extract_rr_from_comment(comment):
        if not comment or not isinstance(comment, str):
            return 0.0
        m = _re.search(r'R([0-9]+(?:\.[0-9]+)?)', comment)
        if m:
            try:
                return float(m.group(1))
            except (ValueError, TypeError):
                return 0.0
        return 0.0

    def _day_key(ts):
        try:
            return datetime.fromtimestamp(int(ts)).strftime('%Y-%m-%d')
        except Exception:
            return None

    def _week_key(ts):
        try:
            d = datetime.fromtimestamp(int(ts)).isocalendar()
            return f"{d[0]}-W{d[1]:02d}"
        except Exception:
            return None

    def _iso(ts):
        try:
            return datetime.fromtimestamp(int(ts)).strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            return None

    def _parse_date_flexible(date_str):
        if not date_str:
            return None
        if isinstance(date_str, datetime):
            return date_str
        s = str(date_str).strip()
        if s.upper() == "NULL" or s == "":
            return None
        for fmt in ("%Y-%m-%d", "%B %d, %Y", "%B %d %Y", "%Y/%m/%d",
                    "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%m-%d-%Y",
                    "%Y-%m-%d %H:%M:%S", "%B %d, %Y %H:%M:%S"):
            try:
                return datetime.strptime(s, fmt)
            except Exception:
                continue
        return None

    def _ensure_analytics_row(investor_id, sub_account_id):
        """Guarantee a row exists so the UPDATE below has somewhere to land."""
        row = db_fetch_one(
            """
            SELECT id FROM investors_analytics
             WHERE userid = %s AND sub_account_id = %s
             LIMIT 1
            """,
            [investor_id, sub_account_id], timeout=10.0, quiet=True,
        )
        if row and _raw_first_key(row, 'id'):
            return _coerce_int(_raw_first_key(row, 'id'))
        ok, new_id, _ = db_execute(
            """
            INSERT INTO investors_analytics (userid, sub_account_id)
            VALUES (%s, %s)
            """,
            [investor_id, sub_account_id], timeout=10.0, quiet=True,
        )
        if not ok:
            return None
        row = db_fetch_one(
            """
            SELECT id FROM investors_analytics
             WHERE userid = %s AND sub_account_id = %s
             ORDER BY id DESC
             LIMIT 1
            """,
            [investor_id, sub_account_id], timeout=10.0, quiet=True,
        )
        return _coerce_int(_raw_first_key(row or {}, 'id'))

    # ------------------------------------------------------------------
    # MT5 connection check
    # ------------------------------------------------------------------
    acc = mt5_account_info()
    if acc is None:
        print(f"  ⚠️ MT5 not connected — cannot read trade history")
        stats['errors'].append('MT5 not connected')
        return stats

    # ------------------------------------------------------------------
    # Enumeration — skipped when jobs= passed in
    # ------------------------------------------------------------------
    if jobs is None:
        print("\n" + "─" * 80)
        print(" 📁 LOADING INVESTOR JOBS FROM DB (VPS-scoped)")
        print("─" * 80)

        local_ip = get_local_ip()
        if not local_ip:
            print(" ⚠️ Could not determine local IP")
            stats['errors'].append('local IP unknown')
            return stats
        print(f" │ Local IP: {local_ip}")

        all_jobs = fetch_vps_investor_jobs(local_ip)
        if not all_jobs:
            print(" │ ℹ️ No active investor jobs found for this VPS")
            stats['processing_success'] = True
            return stats

        if inv_db_id:
            jobs = [j for j in all_jobs if str(j.get('investor_id')) == str(inv_db_id)]
            if not jobs:
                print(f" ⚠️ Investor {inv_db_id} not found in any VPS-scoped job")
                stats['errors'].append(f"Investor {inv_db_id} not found")
                return stats
        else:
            jobs = all_jobs

        print(f" │ Loaded {len(jobs)} investor job(s) from DB")
        print("─" * 80)

    # ------------------------------------------------------------------
    # Per-investor pipeline
    # ------------------------------------------------------------------
    for job in jobs:
        investor_id    = job.get('investor_id')
        sub_account_id = job.get('sub_account_id')
        programme_id   = job.get('programme_id')
        programme_name = job.get('programme_name') or f"programme_{programme_id}"
        programme_userid = resolve_programme_userid(programme_id, job)

        if investor_id is None or sub_account_id is None or programme_id is None or not programme_userid:
            stats['investors_skipped'] += 1
            continue

        print(f"\n{'─'*72}")
        print(f" 📊 Investor {investor_id} (sub {sub_account_id}) | "
              f"programme '{programme_name}' (id={programme_id})")
        print(f"{'─'*72}")

        stats['investors_processed'] += 1
        stats['programmes_processed'] += 1

        # -------- anchor + window --------
        anchor = db_fetch_one(
            """
            SELECT broker_balance, execution_start_date
              FROM harvhub
             WHERE id = %s AND sub_account_id = %s
             LIMIT 1
            """,
            [investor_id, sub_account_id], timeout=10.0, quiet=True,
        ) or {}
        execution_start_date = _raw_first_key(anchor, 'execution_start_date')
        start_dt = _parse_date_flexible(execution_start_date)
        if start_dt is None:
            print(f"  │ ⚠️ No usable execution_start_date — skipping investor")
            stats['investors_skipped'] += 1
            continue

        # Contract end = start + duration from accountmanagement if available
        contract_days = None
        am = db_fetch_one(
            """
            SELECT contract_duration
              FROM accountmanagement
             WHERE developerid = 0 AND programme_id = %s AND investorid = 0
             LIMIT 1
            """,
            [programme_id], timeout=10.0, quiet=True,
        )
        if am:
            try:
                contract_days = int(_coerce_int(_raw_first_key(am, 'contract_duration')) or 0)
            except Exception:
                contract_days = None

        end_dt = lagos_now().replace(tzinfo=None)
        if contract_days and contract_days > 0:
            candidate = start_dt + timedelta(days=contract_days)
            if candidate < end_dt:
                end_dt = candidate
        if end_dt < start_dt:
            end_dt = lagos_now().replace(tzinfo=None)

        print(f"  │ Window : {start_dt.strftime('%Y-%m-%d')} → {end_dt.strftime('%Y-%m-%d')}")

        # -------- group-aligned MT5 magic catalogue --------
        group_resolution = fetch_investor_programme_analytics_magic_groups(
            programme_id, programme_userid, investor_id, sub_account_id,
        )
        magic_catalog = fetch_subaccount_magic_catalog(investor_id, sub_account_id)
        if not group_resolution.get('ok'):
            stats['errors'].append(
                f"investor {investor_id}/sub {sub_account_id}: analytics magic mapping: "
                f"{group_resolution.get('reason') or 'unavailable'}"
            )
        if not magic_catalog.get('ok'):
            stats['errors'].append(
                f"investor {investor_id}/sub {sub_account_id}: subaccount magic catalogue: "
                f"{magic_catalog.get('reason') or 'unavailable'}"
            )
        catalog_by_magic = magic_catalog.get('by_magic') or {}
        ambiguous_magics = set(magic_catalog.get('ambiguous') or set())
        ambiguous_magics.update(group_resolution.get('ambiguous') or set())
        authorized_tradeids = (
            set(group_resolution.get('by_magic') or {}) - ambiguous_magics
            if group_resolution.get('ok') and magic_catalog.get('ok') else set()
        )
        print(f"  │ Programme-analytics group-mapped magic IDs: {len(authorized_tradeids)}")
        if ambiguous_magics:
            print(f"  │ ⚠️ Ambiguous magic IDs excluded: {len(ambiguous_magics)}")

        # -------- pull MT5 history --------
        try:
            deals = mt5_history_deals(start_dt, end_dt) or []
        except Exception as e:
            print(f"  │ ✗ Could not read MT5 history: {e}")
            stats['errors'].append(f"investor {investor_id}: mt5 history error: {e}")
            stats['investors_skipped'] += 1
            continue

        stats['total_deals_seen'] += len(deals)
        if not deals:
            print(f"  │ 📭 No closed deals in window")
            continue

        try:
            hist_orders = mt5_history_orders(start_dt, end_dt) or []
        except Exception:
            hist_orders = []

        # Collect order comments + SL/TP per position_id
        order_comments = {}
        order_sl_tp = {}
        for o in hist_orders:
            pid = getattr(o, 'position_id', None)
            if pid is None:
                continue
            if pid not in order_comments:
                order_comments[pid] = getattr(o, 'comment', '') or ''
            sl = float(getattr(o, 'sl', 0) or 0)
            tp = float(getattr(o, 'tp', 0) or 0)
            cur = order_sl_tp.setdefault(pid, {'sl': 0.0, 'tp': 0.0})
            if sl > 0 and cur['sl'] == 0.0:
                cur['sl'] = sl
            if tp > 0 and cur['tp'] == 0.0:
                cur['tp'] = tp

        # -------- group deals by position_id --------
        deals_by_position = {}
        for d in deals:
            # Only entry/exit deal types (0=BUY, 1=SELL). Ignore balance ops.
            if getattr(d, 'type', None) in (0, 1):
                deals_by_position.setdefault(d.position_id, []).append(d)

        master_trades = []
        for position_id, dlist in deals_by_position.items():
            dlist.sort(key=lambda x: x.time)
            if len(dlist) < 2:
                continue

            total_profit     = sum(float(getattr(d, 'profit', 0) or 0) for d in dlist)
            total_commission = sum(float(getattr(d, 'commission', 0) or 0) for d in dlist)
            total_swap       = sum(float(getattr(d, 'swap', 0) or 0) for d in dlist)
            total_pnl        = total_profit + total_commission + total_swap

            entry_deal = dlist[0]
            exit_deal  = dlist[-1]

            try:
                entry_magic = int(getattr(entry_deal, 'magic', 0) or 0)
            except Exception:
                entry_magic = 0
            programme_ids_for_magic = catalog_by_magic.get(entry_magic, set())
            if entry_magic in ambiguous_magics:
                auth_group = 'ambiguous'
            elif programme_id in programme_ids_for_magic:
                auth_group = 'authorized' if entry_magic in authorized_tradeids else 'unverified'
            elif programme_ids_for_magic:
                auth_group = 'other_programme'
            elif not group_resolution.get('ok') or not magic_catalog.get('ok'):
                auth_group = 'unverified'
            else:
                auth_group = 'unauthorized'

            trade_type = 'BUY' if entry_deal.type == 0 else 'SELL'
            mt5_action = mt5.ORDER_TYPE_BUY if entry_deal.type == 0 else mt5.ORDER_TYPE_SELL

            total_volume = float(getattr(entry_deal, 'volume', 0) or 0)
            entry_price  = float(getattr(entry_deal, 'price', 0) or 0)
            exit_price   = float(getattr(exit_deal,  'price', 0) or 0)

            sl_tp_rec = order_sl_tp.get(position_id, {})
            stoploss   = float(getattr(entry_deal, 'sl', 0) or 0) or sl_tp_rec.get('sl', 0.0)
            takeprofit = float(getattr(entry_deal, 'tp', 0) or 0) or sl_tp_rec.get('tp', 0.0)

            trade_risk = 0.0
            if stoploss > 0:
                calc = mt5_order_calc_profit(
                    mt5_action, entry_deal.symbol, total_volume,
                    entry_price, stoploss,
                )
                if calc is not None:
                    trade_risk = abs(float(calc))

            risk_reward = 0.0
            if stoploss > 0 and stoploss != entry_price and takeprofit > 0:
                risk_dist = abs(entry_price - stoploss)
                if risk_dist > 0:
                    risk_reward = round(abs(takeprofit - entry_price) / risk_dist, 2)

            comment = getattr(entry_deal, 'comment', '') or order_comments.get(position_id, '')
            if risk_reward == 0 and comment:
                rr = _extract_rr_from_comment(comment)
                if rr > 0:
                    risk_reward = rr

            close_ts = int(getattr(exit_deal, 'time', 0) or 0)

            master_trades.append({
                'ticket':         position_id,
                'symbol':         getattr(entry_deal, 'symbol', '') or '',
                'type':           trade_type,
                'entry_price':    round(entry_price, 5),
                'exit_price':     round(exit_price, 5),
                'stoploss':       round(stoploss, 5),
                'take_profit':    round(takeprofit, 5),
                'trade_risk':     round(trade_risk, 2),
                'risk_reward':    float(risk_reward),
                'volume':         round(total_volume, 2),
                'time_open':      _iso(getattr(entry_deal, 'time', 0)),
                'time_close':     _iso(close_ts),
                'closed_time':    _iso(close_ts),
                'magic':          entry_magic,
                'total_pnl':      round(total_pnl, 2),
                'state':          'CLOSED',
                'raw_close_time': close_ts,
                'auth_group':     auth_group,
                'comment':        comment,
            })

        stats['total_trades_grouped'] += len(master_trades)

        if not master_trades:
            print(f"  │ 📭 No grouped trades after filtering")
            continue

        # -------- build scalar analytics --------
        ordered = sorted(master_trades, key=lambda x: x['raw_close_time'])
        p_list = [t for t in ordered if t['total_pnl'] > 0]
        l_list = [t for t in ordered if t['total_pnl'] < 0]

        total_pnl_all = sum(t['total_pnl'] for t in ordered)
        p_rev = sum(t['total_pnl'] for t in p_list)
        l_rev = sum(t['total_pnl'] for t in l_list)

        highest_loss_per_trade = 0.0
        for t in ordered:
            if t['total_pnl'] < 0:
                v = abs(t['total_pnl'])
                if v > highest_loss_per_trade:
                    highest_loss_per_trade = v

        sl_tp   = sum(1 for t in ordered if t['stoploss'] > 0 and t['take_profit'] > 0)
        no_sl_tp = sum(1 for t in ordered if not (t['stoploss'] > 0 and t['take_profit'] > 0))

        # per-day counts
        per_day = {}
        for t in ordered:
            d = _day_key(t['raw_close_time'])
            if not d:
                continue
            per_day[d] = per_day.get(d, 0) + 1

        if per_day:
            values = list(per_day.values())
            lowest  = min(values)
            highest = max(values)
            average = round(sum(values) / len(values))
        else:
            lowest = highest = average = 0

        # per-week counts
        per_week = {}
        for t in ordered:
            w = _week_key(t['raw_close_time'])
            if not w:
                continue
            per_week[w] = per_week.get(w, 0) + 1

        if per_week:
            wvalues = list(per_week.values())
            wlowest  = min(wvalues)
            whighest = max(wvalues)
            waverage = round(sum(wvalues) / len(wvalues))
        else:
            wlowest = whighest = waverage = 0

        # drawdown
        equity = 0.0
        peak = 0.0
        max_dd = 0.0
        for t in ordered:
            equity += t['total_pnl']
            if equity > peak:
                peak = equity
            dd = peak - equity
            if dd > max_dd:
                max_dd = dd

        # consecutive losing trades
        max_loss_count = 0
        max_loss_amount = 0.0
        cur_count = 0
        cur_amount = 0.0
        for t in ordered:
            if t['total_pnl'] < 0:
                cur_count += 1
                cur_amount += abs(t['total_pnl'])
                if cur_count > max_loss_count:
                    max_loss_count = cur_count
                    max_loss_amount = cur_amount
                elif cur_count == max_loss_count and cur_amount > max_loss_amount:
                    max_loss_amount = cur_amount
            else:
                cur_count = 0
                cur_amount = 0.0

        # consecutive losing days
        day_pnl = {}
        for t in ordered:
            d = _day_key(t['raw_close_time'])
            if not d:
                continue
            day_pnl[d] = day_pnl.get(d, 0.0) + t['total_pnl']

        max_days = 0
        max_days_loss = 0.0
        cur_days = 0
        cur_days_loss = 0.0
        for d in sorted(day_pnl.keys()):
            if day_pnl[d] < 0:
                cur_days += 1
                cur_days_loss += abs(day_pnl[d])
                if cur_days > max_days:
                    max_days = cur_days
                    max_days_loss = cur_days_loss
                elif cur_days == max_days and cur_days_loss > max_days_loss:
                    max_days_loss = cur_days_loss
            else:
                cur_days = 0
                cur_days_loss = 0.0

        # symbols count + highest rr
        sym_set = set()
        highest_rr = 0.0
        for t in ordered:
            sym_set.add(t['symbol'])
            rr = t.get('risk_reward', 0) or 0
            if rr > highest_rr:
                highest_rr = rr

        # revenue split
        total_rev = abs(p_rev) + abs(l_rev)
        if total_rev > 0:
            p_pct = round((abs(p_rev) / total_rev) * 100.0, 2)
            l_pct = round((abs(l_rev) / total_rev) * 100.0, 2)
            rev_pct = round(p_pct - l_pct, 2)
            if rev_pct < 0:
                rev_pct = 0.0
        else:
            p_pct = l_pct = rev_pct = 0.0

        # -------- upsert investors_analytics --------
        _ensure_analytics_row(investor_id, sub_account_id)

        upd_sql = """
            UPDATE investors_analytics
               SET total_trades                               = %s,
                   total_pnl                                  = %s,
                   profit_trades                              = %s,
                   loss_trades                                = %s,
                   profit_amount                              = %s,
                   loss_amount                                = %s,
                   lowest_trades_per_day                      = %s,
                   highest_trades_per_day                     = %s,
                   average_trades_per_day                     = %s,
                   lowest_trades_per_week                     = %s,
                   highest_trades_per_week                    = %s,
                   average_trades_per_week                    = %s,
                   highest_loss_per_trade                     = %s,
                   highest_drawdown                           = %s,
                   symbols_traded                             = %s,
                   closed_deals_with_sl_tp                    = %s,
                   closed_deals_without_sl_tp                 = %s,
                   consecutive_losses_count                   = %s,
                   total_loss_pnl                             = %s,
                   consecutive_days_in_loss_count             = %s,
                   consecutive_days_in_loss_count_total_loss_pnl = %s,
                   revenue_percentage                         = %s,
                   revenue_profit_percentage                  = %s,
                   revenue_loss_percentage                    = %s,
                   recent_risk_reward                         = %s
             WHERE userid = %s
               AND sub_account_id = %s
        """
        params = [
            int(len(ordered)),
            round(total_pnl_all, 2),
            int(len(p_list)),
            int(len(l_list)),
            round(p_rev, 2),
            round(abs(l_rev), 2),
            int(lowest),
            int(highest),
            int(average),
            int(wlowest),
            int(whighest),
            int(waverage),
            round(highest_loss_per_trade, 2),
            round(max_dd, 2),
            int(len(sym_set)),
            int(sl_tp),
            int(no_sl_tp),
            int(max_loss_count),
            round(-max_loss_amount, 2),
            int(max_days),
            round(-max_days_loss, 2),
            round(rev_pct, 2),
            round(p_pct, 2),
            round(l_pct, 2),
            round(highest_rr, 4),
            investor_id,
            sub_account_id,
        ]
        ok, affected, msg = db_execute(upd_sql, params, timeout=20.0, quiet=True)
        if ok:
            stats['analytics_rows_upserted'] += 1
            print(f"  │ ✅ investors_analytics upserted "
                  f"(trades={len(ordered)}, pnl=${total_pnl_all:+.2f}, "
                  f"profit={len(p_list)}, loss={len(l_list)}, "
                  f"dd=${max_dd:.2f}, symbols={len(sym_set)})")
        else:
            print(f"  │ ✗ investors_analytics upsert failed: {msg}")
            stats['errors'].append(f"investor {investor_id}: analytics upsert: {msg}")

        # -------- write authorized/unauthorized trade rows --------
        def _write_trade_rows(table, trades):
            written = 0
            for t in trades:
                exists = db_fetch_one(
                    f"""
                    SELECT id FROM {table}
                     WHERE userid = %s
                       AND sub_account_id = %s
                       AND ticket = %s
                     LIMIT 1
                    """,
                    [investor_id, sub_account_id, int(t['ticket'])],
                    timeout=10.0, quiet=True,
                )
                if exists and _raw_first_key(exists, 'id'):
                    ok_up, _, _ = db_execute(
                        f"""
                        UPDATE {table}
                           SET symbol        = %s,
                               type          = %s,
                               volume        = %s,
                               entry         = %s,
                               exit_price    = %s,
                               stoploss      = %s,
                               target        = %s,
                               pnl           = %s,
                               risk_reward   = %s,
                               closed_time   = %s,
                               comment       = %s,
                               magic         = %s
                         WHERE id = %s
                        """,
                        [
                            t['symbol'], t['type'], t['volume'],
                            t['entry_price'], t['exit_price'],
                            t['stoploss'], t['take_profit'],
                            t['total_pnl'], t['risk_reward'],
                            t['closed_time'], t['comment'], t['magic'],
                            _coerce_int(_raw_first_key(exists, 'id')),
                        ],
                        timeout=15.0, quiet=True,
                    )
                    if ok_up:
                        written += 1
                else:
                    ok_ins, _, _ = db_execute(
                        f"""
                        INSERT INTO {table}
                            (userid, sub_account_id, ticket, symbol, type,
                             volume, entry, exit_price, stoploss, target,
                             pnl, risk_reward, closed_time, comment, magic)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s)
                        """,
                        [
                            investor_id, sub_account_id, int(t['ticket']),
                            t['symbol'], t['type'], t['volume'],
                            t['entry_price'], t['exit_price'],
                            t['stoploss'], t['take_profit'],
                            t['total_pnl'], t['risk_reward'],
                            t['closed_time'], t['comment'], t['magic'],
                        ],
                        timeout=15.0, quiet=True,
                    )
                    if ok_ins:
                        written += 1
            return written

        auth_trades   = [t for t in ordered if t['auth_group'] == 'authorized']
        unauth_trades = [t for t in ordered if t['auth_group'] == 'unauthorized']
        other_programme_trades = [t for t in ordered if t['auth_group'] == 'other_programme']
        unverified_trades = [t for t in ordered if t['auth_group'] == 'unverified']
        ambiguous_trades = [t for t in ordered if t['auth_group'] == 'ambiguous']
        stats['other_programme_trades'] += len(other_programme_trades)
        stats['unverified_trades'] += len(unverified_trades)
        stats['ambiguous_trades'] += len(ambiguous_trades)

        auth_written = _write_trade_rows('authorized_trades', auth_trades)
        stats['authorized_trades'] += len(auth_trades)
        stats['authorized_rows_written'] += auth_written
        print(f"  │ ✅ authorized_trades : {auth_written} row(s) written "
              f"({len(auth_trades)} trade(s))")

        if unauth_trades:
            unauth_written = _write_trade_rows('unauthorized_trades', unauth_trades)
            stats['unauthorized_trades'] += len(unauth_trades)
            stats['unauthorized_rows_written'] += unauth_written
            print(f"  │ ⚠️ unauthorized_trades: {unauth_written} row(s) written "
                  f"({len(unauth_trades)} trade(s))")
        else:
            print(f"  │ ✅ unauthorized_trades: none")
        if other_programme_trades or unverified_trades or ambiguous_trades:
            print(
                f"  │ ℹ️ Not attributed to this programme: other_programme={len(other_programme_trades)}, "
                f"unverified={len(unverified_trades)}, ambiguous={len(ambiguous_trades)}"
            )

        stats['details'].append({
            'investor_id':          investor_id,
            'sub_account_id':       sub_account_id,
            'programme_id':         programme_id,
            'total_trades':         len(ordered),
            'authorized_trades':    len(auth_trades),
            'unauthorized_trades':  len(unauth_trades),
            'other_programme_trades': len(other_programme_trades),
            'unverified_trades':    len(unverified_trades),
            'ambiguous_trades':     len(ambiguous_trades),
            'total_pnl':            round(total_pnl_all, 2),
            'highest_drawdown':     round(max_dd, 2),
            'symbols_traded':       len(sym_set),
        })

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print(f"\n{'='*10} 📊 TRADES ANALYTICS SUMMARY (DB) {'='*10}")
    print(f"  Investors processed        : {stats['investors_processed']}")
    print(f"  Investors skipped          : {stats['investors_skipped']}")
    print(f"  Programmes processed       : {stats['programmes_processed']}")
    print(f"  MT5 deals seen             : {stats['total_deals_seen']}")
    print(f"  Trades grouped             : {stats['total_trades_grouped']}")
    print(f"  Authorized trades          : {stats['authorized_trades']}  "
          f"(rows written {stats['authorized_rows_written']})")
    print(f"  Unauthorized trades        : {stats['unauthorized_trades']}  "
          f"(rows written {stats['unauthorized_rows_written']})")
    print(f"  Other-programme trades     : {stats['other_programme_trades']}")
    print(f"  Unverified group trades    : {stats['unverified_trades']}")
    print(f"  Ambiguous magic trades     : {stats['ambiguous_trades']}")
    print(f"  investors_analytics upserts: {stats['analytics_rows_upserted']}")
    print(f"  Errors                     : {len(stats['errors'])}")
    for e in stats['errors']:
        print(f"     • {e}")
    print(f"{'='*10} 🏁 COMPLETE {'='*10}\n")

    stats['processing_success'] = True
    return stats

# ==============================================================================
#  WORKER: one process per investor job (DB-driven)
#
#  MIRRORS process_single_investor_from_db_local ROUTING, BUT DB-ONLY.
#
#  Gate order (matches the local file's conditional routing):
#    1. restricted_timerange_db  -> if restricted, skip everything else.
#    2. recent_highest_balance_revenue_target_db -> populates recent_highest_alert.
#    3. If recent_highest_alert['is_triggered'] -> skip every future
#       trading stage (local file deletes open orders/positions at this
#       point; the DB orchestrator will do the same once that stage is
#       ported, via a DB-driven delete function).
# ==============================================================================
def process_single_investor_from_db_(job):
    """
    SOLO TEST PATH — runs:
        1. place_usd_orders_db(inv_db_id=..., jobs=...)

    No restriction gate, no alarm gate, no cleanup, no grid.
    The parent's job is reused directly — no re-discovery.

    Skipped entirely when no MT5 terminal path can be resolved.
    """

    global unauthorized_alert

    inv_db_id = job.get('investor_id')
    sub_id = job.get('sub_account_id')
    programme_id = job.get('programme_id')
    programme_name = job.get('programme_name') or ''

    print(f"\n[START][TEST] ⚙️ Full solo pipeline for "
          f"Investor {inv_db_id} (sub {sub_id}) | "
          f"programme '{programme_name}' (id={programme_id})")

    account_stats = {
        'investor_id':    inv_db_id,
        'sub_account_id': sub_id,
        'programme_id':   programme_id,
        'success':        False,
        'stages':         {},
    }

    _worker_init()

    # ------------------------------------------------------------------
    # MT5 PATH RESOLUTION — must succeed, else skip investor entirely
    # ------------------------------------------------------------------
    terminal_path = resolve_investor_terminal_path(inv_db_id, sub_id)

    if terminal_path is None:
        print(f"[MT5] ⏭️ No terminal path for investor {inv_db_id} "
              f"(sub {sub_id}) — SKIPPING investor entirely")
        account_stats['success'] = False
        account_stats['error'] = 'no_terminal_path'
        try:
            if _db is not None:
                _db.release_channel()
        except Exception:
            pass
        return account_stats

    try:
        print(f"[MT5-PATH] final resolved terminal = {terminal_path}")

        mt5_ok = mt5_connect(terminal_path=terminal_path, timeout_ms=15000)
        if mt5_ok:
            if not verify_mt5_subaccount_identity(inv_db_id, sub_id):
                mt5_disconnect()
                account_stats['success'] = False
                account_stats['error'] = 'mt5_account_identity_mismatch'
                try:
                    if _db is not None:
                        _db.release_channel()
                except Exception:
                    pass
                return account_stats
            print(f"[MT5] ✅ Connected (custom path) "
                  f"for investor {inv_db_id} (sub {sub_id})")
        else:
            print(f"[MT5] ⚠️ mt5_connect returned False "
                  f"(last_error={mt5_last_error()}) — SKIPPING investor")
            account_stats['success'] = False
            account_stats['error'] = 'mt5_connect_failed'
            try:
                if _db is not None:
                    _db.release_channel()
            except Exception:
                pass
            return account_stats
    except Exception as e:
        print(f"[MT5] ⚠️ mt5_connect raised: {e} — SKIPPING investor")
        account_stats['success'] = False
        account_stats['error'] = f'mt5_connect_exception: {e}'
        try:
            if _db is not None:
                _db.release_channel()
        except Exception:
            pass
        return account_stats

    try:
        # ==================================================================
        # JOB LIST — reuse the parent's job verbatim. No re-discovery.
        # ==================================================================
        _cached_jobs = [job]

        place_stats = run_stage(
            'place_usd_orders_db',
            place_usd_orders_db,
            inv_db_id=inv_db_id,
            jobs=_cached_jobs,
        )
        account_stats['stages']['place_usd_orders_db'] = place_stats
        account_stats['place_stats'] = place_stats

        account_stats['success'] = True

    except Exception as e:
        print(f"[CRITICAL ERROR][TEST] investor={inv_db_id}: {e}")
        traceback.print_exc()
        account_stats['success'] = False
        account_stats['error'] = str(e)

    finally:
        try:
            mt5_disconnect()
        except Exception:
            pass
        try:
            if _db is not None:
                _db.release_channel()
        except Exception:
            pass

    return account_stats

# ==============================================================================
#  PROGRAMME-REQUESTED MT5 EXECUTION
#
#  This stage is intentionally independent of programme_training_configuration.
#  The canonical programme_trades request is the command source; every investor
#  subaccount executes it against its own MT5 terminal and its own trade copy.
#  Failed requests remain unfilled, retain a visible failure reason, and are
#  retried on the next programme-trades loop.
# ==============================================================================

def _pt_request_normalize(value):
    value = str(value or '').strip().lower()
    value = ' '.join(value.replace('_', ' ').replace('-', ' ').split())
    aliases = {
        'close position requested': 'position closing requested',
        'position close requested': 'position closing requested',
        'close order requested': 'order closing requested',
        'order close requested': 'order closing requested',
        'breakeven request': 'breakeven requested',
        'break even requested': 'breakeven requested',
        'break even request': 'breakeven requested',
    }
    return aliases.get(value, value)


def _pt_sql_literal(value):
    """Safely inline a single SQL literal for the one-placeholder DB bridge."""
    if value is None:
        return 'NULL'
    if isinstance(value, bool):
        return '1' if value else '0'
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if value != value or value in (float('inf'), float('-inf')):
            return 'NULL'
        return repr(value)
    text = str(value).replace('\x00', '')
    text = text.replace('\\', '\\\\').replace("'", "''")
    return "'" + text + "'"


def _pt_request_retry_reason(message):
    message = ' '.join(str(message or '').split()).strip()
    if not message:
        message = 'MT5 has not confirmed the requested action.'
    if 'will keep trying' not in message.lower():
        message = message.rstrip('. ') + '; will keep trying on the next programme-trades cycle.'
    return message[:490]


def _pt_request_mt5_snapshot():
    """Return (ok, orders, positions, error); None from MT5 means query failure."""
    if not mt5_available():
        return False, [], [], 'MetaTrader5 module is unavailable.'
    with _MT5_IPC_LOCK:
        try:
            orders = mt5.orders_get()
            if orders is None:
                try:
                    err = mt5.last_error()
                except Exception:
                    err = None
                return False, [], [], f'MT5 orders_get failed: {err}'
            positions = mt5.positions_get()
            if positions is None:
                try:
                    err = mt5.last_error()
                except Exception:
                    err = None
                return False, [], [], f'MT5 positions_get failed: {err}'
            return True, list(orders or []), list(positions or []), None
        except Exception as exc:
            return False, [], [], f'MT5 trade snapshot failed: {exc}'


def _pt_request_symbol_key(value):
    return str(value or '').strip().upper()


def _pt_request_signal_side(value):
    """Normalize programme/investor order type to BUY, SELL, or None."""
    raw = str(value or '').strip().lower()
    if not raw:
        return None
    compact = raw.replace('_', ' ').replace('-', ' ')
    if 'buy' in compact or compact in {'long', '0', '2', '4', '6'}:
        return 'buy'
    if 'sell' in compact or compact in {'short', '1', '3', '5', '7'}:
        return 'sell'
    try:
        numeric = int(float(compact))
        if numeric in (0, 2, 4, 6):
            return 'buy'
        if numeric in (1, 3, 5, 7):
            return 'sell'
    except Exception:
        pass
    return None


def _pt_request_item_side(item, is_position=False):
    raw_type = _coerce_int(getattr(item, 'type', None))
    if raw_type is None:
        return None
    if is_position:
        if raw_type == getattr(mt5, 'POSITION_TYPE_BUY', 0):
            return 'buy'
        if raw_type == getattr(mt5, 'POSITION_TYPE_SELL', 1):
            return 'sell'
        return None
    buy_types = {
        getattr(mt5, 'ORDER_TYPE_BUY', 0),
        getattr(mt5, 'ORDER_TYPE_BUY_LIMIT', 2),
        getattr(mt5, 'ORDER_TYPE_BUY_STOP', 4),
        getattr(mt5, 'ORDER_TYPE_BUY_STOP_LIMIT', 6),
        0, 2, 4, 6,
    }
    sell_types = {
        getattr(mt5, 'ORDER_TYPE_SELL', 1),
        getattr(mt5, 'ORDER_TYPE_SELL_LIMIT', 3),
        getattr(mt5, 'ORDER_TYPE_SELL_STOP', 5),
        getattr(mt5, 'ORDER_TYPE_SELL_STOP_LIMIT', 7),
        1, 3, 5, 7,
    }
    if raw_type in buy_types:
        return 'buy'
    if raw_type in sell_types:
        return 'sell'
    return None


def _pt_request_signal_has_price_levels(signal):
    """Compatibility helper: validate the magic's symbol/direction metadata.

    The name is retained to avoid breaking any external helper callers. Price
    levels are intentionally not required because the canonical MT5 magic is
    the stronger identity and account-management can adjust those levels.
    """
    signal = signal or {}
    symbol = _pt_request_symbol_key(signal.get('symbol'))
    side = _pt_request_signal_side(signal.get('ordertype'))
    return bool(symbol and side in ('buy', 'sell'))

def _pt_request_matches_signal(item, signal, tradeid, is_position=False):
    """Match an MT5 child order/position by its canonical programme trade magic.

    Primary identity: item.magic == programme_trades.tradeid.
    Safety checks: exact symbol and compatible BUY/SELL direction.

    A Recovery Generator/chunking stage may produce multiple broker tickets from one
    programme signal. Those child tickets retain the same magic and are all
    included, regardless of their individual volume or execution prices.
    Prices are deliberately not used as identifiers: unrelated trades can
    share identical price levels, while account-management may legitimately
    adjust entry/SL/TP after signal inheritance.
    """
    expected_magic = _coerce_int(tradeid)
    actual_magic = _coerce_int(getattr(item, 'magic', None))
    if expected_magic is None or expected_magic <= 0 or actual_magic != expected_magic:
        return False

    signal = signal or {}
    expected_symbol = _pt_request_symbol_key(signal.get('symbol'))
    actual_symbol = _pt_request_symbol_key(getattr(item, 'symbol', ''))
    if not expected_symbol or actual_symbol != expected_symbol:
        return False

    expected_side = _pt_request_signal_side(signal.get('ordertype'))
    actual_side = _pt_request_item_side(item, is_position=is_position)
    if expected_side not in ('buy', 'sell') or actual_side not in ('buy', 'sell'):
        return False
    return expected_side == actual_side

def _pt_request_remove_order(order):
    ticket = _coerce_int(getattr(order, 'ticket', None))
    if not ticket:
        return False, 'MT5 pending order has no valid ticket.'
    request = {
        'action': getattr(mt5, 'TRADE_ACTION_REMOVE', 8),
        'order': int(ticket),
    }
    try:
        result = mt5_order_send(request)
    except Exception as exc:
        return False, f'Could not send pending-order removal for ticket {ticket}: {exc}'
    if result is None:
        return False, f'Pending-order removal for ticket {ticket} returned no MT5 response: {mt5_last_error()}'
    retcode = getattr(result, 'retcode', None)
    accepted = {
        getattr(mt5, 'TRADE_RETCODE_DONE', 10009),
        getattr(mt5, 'TRADE_RETCODE_PLACED', 10008),
    }
    if retcode in accepted:
        return True, None
    comment = str(getattr(result, 'comment', '') or '').strip()
    if retcode == getattr(mt5, 'TRADE_RETCODE_ORDER_CHANGED', -99999):
        return True, None
    return False, f'Could not remove pending order #{ticket}: MT5 retcode {retcode} {comment}'.strip()


def _pt_request_close_position(position, tradeid):
    ticket = _coerce_int(getattr(position, 'ticket', None))
    symbol = str(getattr(position, 'symbol', '') or '').strip()
    # Volume is read only after the position has already matched the canonical
    # tradeid/magic plus symbol/direction. MT5 requires the ticket volume to
    # send its close request; it is never used to identify/group duplicates.
    volume = _coerce_float(getattr(position, 'volume', None), None)
    position_type = _coerce_int(getattr(position, 'type', None))
    if not ticket or not symbol or volume is None or volume <= 0 or position_type not in (0, 1):
        return False, f'Open MT5 position for magic {tradeid} has incomplete ticket/symbol/volume/type data.'

    if not mt5_symbol_select(symbol, True):
        # Some terminals already have the symbol selected; still check for a live tick.
        if mt5_symbol_info(symbol) is None:
            return False, f'MT5 symbol {symbol} is unavailable; cannot close the position.'
    tick = mt5_symbol_tick(symbol)
    if tick is None:
        return False, f'Market is closed or no live quote is available for {symbol}; position close will keep retrying.'

    if position_type == getattr(mt5, 'POSITION_TYPE_BUY', 0):
        close_type = getattr(mt5, 'ORDER_TYPE_SELL', 1)
        close_price = _coerce_float(getattr(tick, 'bid', None), None)
    else:
        close_type = getattr(mt5, 'ORDER_TYPE_BUY', 0)
        close_price = _coerce_float(getattr(tick, 'ask', None), None)
    if close_price is None or close_price <= 0:
        return False, f'Market is closed or {symbol} has no executable bid/ask; position close will keep retrying.'

    symbol_info = mt5_symbol_info(symbol)
    filling_candidates = []
    for attr in ('ORDER_FILLING_IOC', 'ORDER_FILLING_RETURN', 'ORDER_FILLING_FOK'):
        value = getattr(mt5, attr, None)
        if value is not None and value not in filling_candidates:
            filling_candidates.append(value)
    if not filling_candidates:
        filling_candidates = [1, 2, 0]

    last_error = None
    for fill_mode in filling_candidates:
        request = {
            'action': getattr(mt5, 'TRADE_ACTION_DEAL', 1),
            'symbol': symbol,
            'volume': float(volume),
            'type': int(close_type),
            'position': int(ticket),
            'price': float(close_price),
            'deviation': 50,
            'magic': int(tradeid),
            'comment': 'PROGRAMME-REQUEST-CLOSE',
            'type_filling': int(fill_mode),
        }
        try:
            result = mt5_order_send(request)
        except Exception as exc:
            last_error = f'close request exception: {exc}'
            continue
        if result is None:
            last_error = f'no MT5 response: {mt5_last_error()}'
            continue
        retcode = getattr(result, 'retcode', None)
        accepted = {
            getattr(mt5, 'TRADE_RETCODE_DONE', 10009),
            getattr(mt5, 'TRADE_RETCODE_DONE_PARTIAL', 10010),
            getattr(mt5, 'TRADE_RETCODE_PLACED', 10008),
            getattr(mt5, 'TRADE_RETCODE_POSITION_CLOSED', -99998),
        }
        if retcode in accepted:
            return True, None
        comment = str(getattr(result, 'comment', '') or '').strip()
        last_error = f'MT5 retcode {retcode} {comment}'.strip()

    if last_error and '10018' in last_error:
        last_error = f'Market is closed for {symbol}; MT5 could not close position #{ticket}. {last_error}'
    return False, f'Could not close MT5 position #{ticket} ({symbol}): {last_error or "no accepted filling mode"}'



def _pt_request_close_order_on_mt5(tradeid, signal):
    """Remove every pending order matching this canonical trade magic."""
    ok, orders, positions, snapshot_error = _pt_request_mt5_snapshot()
    if not ok:
        return False, _pt_request_retry_reason(snapshot_error)

    matches = [
        order for order in orders
        if _pt_request_matches_signal(order, signal, tradeid, is_position=False)
    ]
    if not matches:
        # If the matching signal has already become a position, the selected
        # UI action is no longer valid; do not silently close the position.
        running_matches = [
            position for position in positions
            if _pt_request_matches_signal(position, signal, tradeid, is_position=True)
        ]
        if running_matches:
            return False, _pt_request_retry_reason(
                "The inherited signal is now an open position, not a pending order; "
                "submit a Position Closing request for this trade."
            )
        return True, None

    failures = []
    for order in matches:
        removed, error = _pt_request_remove_order(order)
        if not removed and error:
            failures.append(error)

    verify_ok, remaining_orders, _, verify_error = _pt_request_mt5_snapshot()
    if not verify_ok:
        return False, _pt_request_retry_reason(verify_error)

    remaining = [
        order for order in remaining_orders
        if _pt_request_matches_signal(order, signal, tradeid, is_position=False)
    ]
    if not remaining:
        return True, None

    detail = failures[-1] if failures else "MT5 still reports matching pending orders."
    return False, _pt_request_retry_reason(
        f"{detail} Matching pending orders remaining: {len(remaining)}."
    )


def _pt_request_close_positions_on_mt5(tradeid, signal):
    """Close every open position matching this canonical trade magic."""
    ok, orders, positions, snapshot_error = _pt_request_mt5_snapshot()
    if not ok:
        return False, _pt_request_retry_reason(snapshot_error)

    matches = [
        position for position in positions
        if _pt_request_matches_signal(position, signal, tradeid, is_position=True)
    ]
    if not matches:
        pending_matches = [
            order for order in orders
            if _pt_request_matches_signal(order, signal, tradeid, is_position=False)
        ]
        if pending_matches:
            return False, _pt_request_retry_reason(
                "The inherited signal is still represented by pending order(s), "
                "not open positions; submit an Order Closing request."
            )
        # A close-position request is already achieved when the matching signal
        # has no live positions left on this account.
        return True, None

    failures = []
    for position in matches:
        closed, error = _pt_request_close_position(position, tradeid)
        if not closed and error:
            failures.append(error)

    verify_ok, _, remaining_positions, verify_error = _pt_request_mt5_snapshot()
    if not verify_ok:
        return False, _pt_request_retry_reason(verify_error)

    remaining = [
        position for position in remaining_positions
        if _pt_request_matches_signal(position, signal, tradeid, is_position=True)
    ]
    if not remaining:
        return True, None

    detail = failures[-1] if failures else "MT5 still reports matching open positions."
    return False, _pt_request_retry_reason(
        f"{detail} Matching open positions remaining: {len(remaining)}."
    )

def _pt_request_set_position_breakeven(position, tradeid):
    """Move one open position's SL to its entry, preserving its TP."""
    ticket = _coerce_int(getattr(position, 'ticket', None))
    symbol = str(getattr(position, 'symbol', '') or '').strip()
    position_type = _coerce_int(getattr(position, 'type', None))
    entry = _coerce_float(getattr(position, 'price_open', None), None)
    current_sl = _coerce_float(getattr(position, 'sl', 0.0), 0.0)
    current_tp = _coerce_float(getattr(position, 'tp', 0.0), 0.0)
    if not ticket or not symbol or entry is None or entry <= 0 or position_type not in (0, 1):
        return False, 'MT5 position is missing a valid ticket, symbol, entry price, or direction.'

    if not mt5_symbol_select(symbol, True) and mt5_symbol_info(symbol) is None:
        return False, f'MT5 symbol {symbol} is unavailable; breakeven will keep trying.'
    info = mt5_symbol_info(symbol)
    tick = mt5_symbol_tick(symbol)
    if info is None or tick is None:
        return False, f'Market is closed or no live quote is available for {symbol}; breakeven will keep trying.'

    point = _coerce_float(getattr(info, 'point', None), 0.0) or 0.0
    digits = _coerce_int(getattr(info, 'digits', None))
    tick_size = _coerce_float(getattr(info, 'trade_tick_size', None), None)
    if tick_size is None or tick_size <= 0:
        tick_size = point if point > 0 else 0.00000001
    if digits is None:
        digits = 5

    # Respect the instrument's tradable tick grid.
    be_price = round(round(float(entry) / tick_size) * tick_size, digits)
    tolerance = max(point * 0.51, tick_size * 0.51, 1e-9)

    # An SL already at breakeven or further into profit is accepted. Never
    # weaken an already more-protective stop by moving it backwards to entry.
    if position_type == getattr(mt5, 'POSITION_TYPE_BUY', 0):
        if current_sl > 0 and current_sl >= be_price - tolerance:
            return True, None
        live_price = _coerce_float(getattr(tick, 'bid', None), None)
        direction_label = 'buy'
    else:
        if current_sl > 0 and current_sl <= be_price + tolerance:
            return True, None
        live_price = _coerce_float(getattr(tick, 'ask', None), None)
        direction_label = 'sell'

    if live_price is None or live_price <= 0:
        return False, f'Market is closed or {symbol} has no executable quote; breakeven will keep trying.'

    stop_level = max(0, _coerce_int(getattr(info, 'trade_stops_level', 0)) or 0)
    freeze_level = max(0, _coerce_int(getattr(info, 'trade_freeze_level', 0)) or 0)
    minimum_gap = max(stop_level, freeze_level) * max(point, 0.0)
    if position_type == getattr(mt5, 'POSITION_TYPE_BUY', 0):
        distance = float(live_price) - be_price
    else:
        distance = be_price - float(live_price)

    # If market price is at/through entry, or inside the broker's stop/freeze
    # band, the broker cannot accept SL=entry yet. Persist a useful reason and
    # retry on the next loop instead of marking the request complete.
    if distance <= 0 or distance + tolerance < minimum_gap:
        return False, (
            f'Cannot breakeven {symbol}: current price is too close to entry '
            f'for the broker stop/freeze distance (distance={distance:.{digits}f}, '
            f'minimum={minimum_gap:.{digits}f}); will keep trying.'
        )

    request = {
        'action': getattr(mt5, 'TRADE_ACTION_SLTP', 6),
        'symbol': symbol,
        'position': int(ticket),
        'sl': float(be_price),
        'tp': float(current_tp or 0.0),
        'magic': int(tradeid),
        'comment': 'PROGRAMME-REQUEST-BREAKEVEN',
    }
    try:
        result = mt5_order_send(request)
    except Exception as exc:
        return False, f'Breakeven SL update raised an exception for {symbol} position #{ticket}: {exc}; will keep trying.'
    if result is None:
        return False, f'Breakeven SL update returned no MT5 response for {symbol} position #{ticket}: {mt5_last_error()}; will keep trying.'

    retcode = getattr(result, 'retcode', None)
    accepted = {
        getattr(mt5, 'TRADE_RETCODE_DONE', 10009),
        getattr(mt5, 'TRADE_RETCODE_NO_CHANGES', 10025),
    }
    if retcode in accepted:
        return True, None

    comment = str(getattr(result, 'comment', '') or '').strip()
    invalid_stops = retcode in {
        getattr(mt5, 'TRADE_RETCODE_INVALID_STOPS', 10016),
        getattr(mt5, 'TRADE_RETCODE_FROZEN', 10029),
    }
    market_closed = retcode == getattr(mt5, 'TRADE_RETCODE_MARKET_CLOSED', 10018)
    if invalid_stops:
        return False, (
            f'Cannot breakeven {symbol}: MT5 rejected SL at entry because the '
            f'current price may be too close to entry or within the stop/freeze level '
            f'({comment or retcode}); will keep trying.'
        )
    if market_closed:
        return False, f'Market is closed for {symbol}; breakeven SL cannot be modified now; will keep trying.'
    return False, f'MT5 rejected breakeven for {symbol} position #{ticket}: retcode {retcode} {comment}; will keep trying.'



def _pt_request_breakeven_trade_on_mt5(tradeid, signal):
    ok, orders, positions, snapshot_error = _pt_request_mt5_snapshot()
    if not ok:
        return False, _pt_request_retry_reason(snapshot_error)

    matching_positions = [
        position for position in positions
        if _pt_request_matches_signal(position, signal, tradeid, is_position=True)
    ]
    matching_orders = [
        order for order in orders
        if _pt_request_matches_signal(order, signal, tradeid, is_position=False)
    ]

    if not matching_positions:
        if matching_orders:
            return False, _pt_request_retry_reason(
                "Breakeven is waiting for the inherited pending order to become "
                "an open position; no matching position exists yet."
            )
        return False, _pt_request_retry_reason(
            "No open position matching the inherited programme trade magic, "
            "symbol and direction was found; waiting for the investor position."
        )

    failures = []
    for position in matching_positions:
        updated, error = _pt_request_set_position_breakeven(position, tradeid)
        if not updated and error:
            failures.append(error)

    verify_ok, _, verify_positions, verify_error = _pt_request_mt5_snapshot()
    if not verify_ok:
        return False, _pt_request_retry_reason(verify_error)

    remaining = [
        position for position in verify_positions
        if _pt_request_matches_signal(position, signal, tradeid, is_position=True)
    ]
    if not remaining:
        return False, _pt_request_retry_reason(
            "Matching positions disappeared before their breakeven stop could be verified."
        )

    not_at_breakeven = []
    for position in remaining:
        entry = _coerce_float(getattr(position, 'price_open', None), None)
        sl = _coerce_float(getattr(position, 'sl', 0.0), 0.0) or 0.0
        position_type = _coerce_int(getattr(position, 'type', None))
        info = mt5_symbol_info(str(getattr(position, 'symbol', '') or ''))
        point = _coerce_float(getattr(info, 'point', None), 0.0) if info is not None else 0.0
        tolerance = max((point or 0.0) * 1.51, 1e-8)
        if entry is None or position_type not in (0, 1):
            not_at_breakeven.append(
                f"position #{getattr(position, 'ticket', '?')} entry/direction could not be verified"
            )
        elif position_type == getattr(mt5, 'POSITION_TYPE_BUY', 0):
            if sl <= 0 or sl < entry - tolerance:
                not_at_breakeven.append(
                    f"position #{getattr(position, 'ticket', '?')} SL is not at entry"
                )
        else:
            if sl <= 0 or sl > entry + tolerance:
                not_at_breakeven.append(
                    f"position #{getattr(position, 'ticket', '?')} SL is not at entry"
                )

    if not_at_breakeven:
        detail = failures[-1] if failures else "One or more matching positions have not reached breakeven."
        detail += " " + "; ".join(not_at_breakeven[:3])
        if len(not_at_breakeven) > 3:
            detail += f" (+{len(not_at_breakeven) - 3} more position(s))"
        return False, _pt_request_retry_reason(detail)

    return True, None

def _pt_request_update_analytics(master_id, programme_userid, programme_id,
                                 request_status, filled, failure_reason):
    """Mirror manual-request state to matching Analytics rows when identity exists."""
    try:
        ci_cols = _db_table_columns('programme_trade_chart_identity')
        pa_cols = _db_table_columns('programme_analytics')
        required_ci = {
            'programme_trade_id', 'userid', 'programmeid', 'root_id',
            'confirmation_root_id', 'family_number', 'setup_group_id',
            'symbol', 'timeframe', 'source_timeframe'
        }
        required_pa = {
            'userid', 'programme_id', 'root_id', 'confirmation_root_id',
            'family_number', 'setup_group_id', 'symbol', 'timeframe',
            'order_request_status', 'order_request_filled',
            'order_request_filled_failure_reason'
        }
        if not required_ci.issubset(ci_cols) or not required_pa.issubset(pa_cols):
            return

        mid = _coerce_int(master_id)
        uid = _coerce_int(programme_userid)
        pid = _coerce_int(programme_id)
        if not all((mid and mid > 0, uid and uid > 0, pid and pid > 0)):
            return
        status_sql = _pt_sql_literal(request_status)
        filled_sql = '1' if int(bool(filled)) else '0'
        failure_sql = _pt_sql_literal(failure_reason)

        sql = f"""
            UPDATE programme_analytics pa
            INNER JOIN programme_trade_chart_identity ci
                    ON ci.userid = pa.userid
                   AND ci.programmeid = pa.programme_id
                   AND ci.root_id = pa.root_id
                   AND ci.confirmation_root_id <=> pa.confirmation_root_id
                   AND ci.family_number = pa.family_number
                   AND ci.setup_group_id = pa.setup_group_id
                   AND UPPER(ci.symbol) = UPPER(pa.symbol)
                   AND (
                        LOWER(ci.timeframe) = LOWER(pa.timeframe)
                        OR LOWER(ci.source_timeframe) = LOWER(pa.timeframe)
                   )
               SET pa.order_request_status = {status_sql},
                   pa.order_request_filled = {filled_sql},
                   pa.order_request_filled_failure_reason = {failure_sql}
             WHERE ci.programme_trade_id = {mid}
               AND ci.userid = {uid}
               AND ci.programmeid = {pid}
               AND (
                    TRIM(COALESCE(pa.order_request_status, '')) = ''
                    OR LOWER(REPLACE(REPLACE(TRIM(COALESCE(pa.order_request_status, '')), '_', ' '), '-', ' ')) = {status_sql}
               )
               AND EXISTS (
                    SELECT 1 FROM programme_trades p
                     WHERE p.id = ci.programme_trade_id
                       AND p.userid = {uid}
                       AND p.programmeid = {pid}
                       AND p.investor_id IS NULL
                       AND LOWER(REPLACE(REPLACE(TRIM(COALESCE(p.order_request_status, '')), '_', ' '), '-', ' ')) = {status_sql}
               )
        """
        ok, _, error = db_execute(sql, None, timeout=20.0, quiet=True)
        if not ok:
            print(f"  [REQUEST] Analytics request-state mirror failed: {error}")
    except Exception as exc:
        # Analytics mirroring is supplementary; canonical programme_trades is
        # the manual-request source of truth.
        print(f"  [REQUEST] Analytics request-state mirror skipped: {exc}")

def _pt_request_aggregate_programme_fill(master_id, tradeid, programme_userid,
                                         programme_id, request_status,
                                         forced_failure=None):
    """Fill parent only after every currently contract_active deal pair is confirmed."""
    mid = _coerce_int(master_id)
    tid = _coerce_int(tradeid)
    uid = _coerce_int(programme_userid)
    pid = _coerce_int(programme_id)
    if not all((mid and mid > 0, tid and tid > 0, uid and uid > 0, pid and pid > 0)):
        return False, 'Manual request aggregation is missing a valid parent/trade/programme identity.'

    status_sql = _pt_sql_literal(request_status)
    parent_guard = (
        f"id = {mid} AND userid = {uid} AND programmeid = {pid} "
        "AND investor_id IS NULL "
        f"AND LOWER(REPLACE(REPLACE(TRIM(COALESCE(order_request_status, '')), '_', ' '), '-', ' ')) = {status_sql}"
    )

    if forced_failure:
        reason = _pt_request_retry_reason(forced_failure)
        db_execute(
            f"UPDATE programme_trades SET order_request_filled = 0, "
            f"order_request_filled_failure_reason = {_pt_sql_literal(reason)} "
            f"WHERE {parent_guard}",
            None, timeout=15.0, quiet=True,
        )
        _pt_request_update_analytics(mid, uid, pid, request_status, 0, reason)
        return False, reason

    # Start with PHP-eligible revenue-deal pairs, then LEFT JOIN this signal's
    # investor copy. That way an active investor whose copy has not synchronized
    # yet prevents the programme master from being marked filled prematurely.
    sql = f"""
        SELECT DISTINCT
               d.investor_id,
               d.investor_sub_account_id,
               cp.id AS investor_trade_id,
               cp.order_request_status,
               cp.order_request_filled,
               cp.order_request_filled_failure_reason
          FROM programme_revenue_deals d
          INNER JOIN harvhub h
                  ON h.id = d.investor_id
                 AND h.sub_account_id = d.investor_sub_account_id
                 AND h.account_contract_status = 'contract_active'
          LEFT JOIN programme_trades cp
                 ON cp.userid = {uid}
                AND cp.programmeid = {pid}
                AND cp.tradeid = {tid}
                AND cp.investor_id = d.investor_id
                AND cp.investor_sub_account_id = d.investor_sub_account_id
                AND cp.live = 1
         WHERE d.programme_id = {pid}
           AND d.investor_id > 0
           AND d.investor_sub_account_id > 0
         ORDER BY d.investor_id ASC, d.investor_sub_account_id ASC
    """
    rows, query_error = _strict_db_rows(sql, None, timeout=20.0)
    if rows is None:
        reason = _pt_request_retry_reason(
            'Could not read active investor deal/copy rows for request aggregation: '
            + str(query_error)
        )
        db_execute(
            f"UPDATE programme_trades SET order_request_filled = 0, "
            f"order_request_filled_failure_reason = {_pt_sql_literal(reason)} "
            f"WHERE {parent_guard}",
            None, timeout=15.0, quiet=True,
        )
        _pt_request_update_analytics(mid, uid, pid, request_status, 0, reason)
        return False, reason

    if not rows:
        reason = _pt_request_retry_reason(
            'No investor/subaccount currently qualifies under the PHP contract_active rule for this programme.'
        )
        db_execute(
            f"UPDATE programme_trades SET order_request_filled = 0, "
            f"order_request_filled_failure_reason = {_pt_sql_literal(reason)} "
            f"WHERE {parent_guard}",
            None, timeout=15.0, quiet=True,
        )
        _pt_request_update_analytics(mid, uid, pid, request_status, 0, reason)
        return False, reason

    incomplete = []
    seen_pairs = set()
    for row in rows:
        inv_id = _coerce_int(_raw_first_key(row, 'investor_id')) or 0
        sub_id = _coerce_int(_raw_first_key(row, 'investor_sub_account_id')) or 0
        if inv_id <= 0 or sub_id <= 0:
            continue
        pair = (inv_id, sub_id)
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)

        label = f'investor {inv_id}/subaccount {sub_id}'
        copy_id = _coerce_int(_raw_first_key(row, 'investor_trade_id'))
        copy_status = _pt_request_normalize(_raw_first_key(row, 'order_request_status'))
        copy_filled = int(_coerce_int(_raw_first_key(row, 'order_request_filled')) or 0)
        copy_reason = str(_raw_first_key(row, 'order_request_filled_failure_reason') or '').strip()
        if not copy_id:
            incomplete.append(f'{label}: LIVE inherited programme_trades copy is missing')
        elif copy_status == request_status and copy_filled == 1 and not copy_reason:
            continue
        else:
            incomplete.append(f'{label}: {copy_reason or "awaiting MT5 confirmation"}')

    if not incomplete:
        filled = 1
        reason = None
    else:
        filled = 0
        details = ' | '.join(incomplete[:3])
        if len(incomplete) > 3:
            details += f' (+{len(incomplete) - 3} more account(s))'
        reason = _pt_request_retry_reason(details)

    update_sql = f"""
        UPDATE programme_trades
           SET order_request_filled = {1 if filled else 0},
               order_request_filled_failure_reason = {_pt_sql_literal(reason)}
         WHERE {parent_guard}
    """
    update_ok, _, update_error = db_execute(update_sql, None, timeout=15.0, quiet=True)
    if not update_ok:
        reason = _pt_request_retry_reason(
            'Investor MT5 actions completed, but canonical programme_trades fill update failed: '
            + str(update_error)
        )
        db_execute(
            f"UPDATE programme_trades SET order_request_filled = 0, "
            f"order_request_filled_failure_reason = {_pt_sql_literal(reason)} "
            f"WHERE {parent_guard}",
            None, timeout=15.0, quiet=True,
        )
        filled = 0

    _pt_request_update_analytics(mid, uid, pid, request_status, filled, reason)
    return bool(filled), reason

def _pt_request_record_preconnect_failure(job, reason):
    """Persist a retry reason on this investor copy if MT5 cannot be reached."""
    try:
        investor_id = _coerce_int((job or {}).get('investor_id'))
        sub_account_id = _coerce_int((job or {}).get('sub_account_id'))
        programme_id = _coerce_int((job or {}).get('programme_id'))
        programme_userid = resolve_programme_userid(programme_id, job)
        if not all((investor_id and investor_id > 0, sub_account_id and sub_account_id > 0,
                    programme_id and programme_id > 0, programme_userid and programme_userid > 0)):
            return 0

        columns = _db_table_columns('programme_trades')
        required = {
            'id', 'userid', 'programmeid', 'investor_id',
            'investor_sub_account_id', 'tradeid', 'live', 'orderstatus',
            'order_request_status', 'order_request_filled',
            'order_request_filled_failure_reason'
        }
        if not required.issubset(columns):
            return 0

        uid = int(programme_userid)
        pid = int(programme_id)
        inv = int(investor_id)
        sub = int(sub_account_id)
        rows, query_error = _strict_db_rows(
            f"""
            SELECT p.id AS programme_trade_id, p.tradeid, p.order_request_status
              FROM programme_trades p
             WHERE p.userid = {uid}
               AND p.programmeid = {pid}
               AND p.investor_id IS NULL
               AND p.live = 1
               AND p.tradeid IS NOT NULL
               AND TRIM(COALESCE(p.order_request_status, '')) <> ''
            """,
            None, timeout=15.0,
        )
        if rows is None:
            print(f"  [REQUEST] Could not load pre-connect requests: {query_error}")
            return 0

        saved = 0
        retry_reason = _pt_request_retry_reason(reason)
        for row in rows:
            request_status = _pt_request_normalize(
                _raw_first_key(row, 'order_request_status')
            )
            if request_status not in (
                'position closing requested',
                'order closing requested',
                'breakeven requested',
            ):
                continue
            master_id = _coerce_int(_raw_first_key(row, 'programme_trade_id'))
            tradeid = _coerce_int(_raw_first_key(row, 'tradeid'))
            if not master_id or not tradeid:
                continue

            status_sql = _pt_sql_literal(request_status)
            update_sql = f"""
                UPDATE programme_trades cp
                INNER JOIN programme_trades p
                        ON p.id = {int(master_id)}
                       AND p.userid = {uid}
                       AND p.programmeid = {pid}
                       AND p.investor_id IS NULL
                       AND LOWER(REPLACE(REPLACE(TRIM(COALESCE(p.order_request_status, '')), '_', ' '), '-', ' ')) = {status_sql}
                   SET cp.order_request_status = {status_sql},
                       cp.order_request_filled = 0,
                       cp.order_request_filled_failure_reason = {_pt_sql_literal(retry_reason)}
                 WHERE cp.userid = {uid}
                   AND cp.programmeid = {pid}
                   AND cp.tradeid = {int(tradeid)}
                   AND cp.investor_id = {inv}
                   AND cp.investor_sub_account_id = {sub}
                   AND cp.live = 1
                   AND LOWER(TRIM(COALESCE(cp.orderstatus, ''))) IN ('pending', 'running')
            """
            copy_ok, _, copy_error = db_execute(
                update_sql, None, timeout=15.0, quiet=True
            )
            if copy_ok:
                saved += 1
            else:
                print(
                    f"  [REQUEST] Could not persist pre-connect failure for "
                    f"investor={inv} sub={sub} tradeid={tradeid}: {copy_error}"
                )

            _pt_request_aggregate_programme_fill(
                int(master_id), int(tradeid), uid, pid,
                request_status, forced_failure=retry_reason,
            )
        return saved
    except Exception as exc:
        print(f"  [REQUEST] Could not persist pre-connect retry reason: {exc}")
        return 0

def process_programme_order_requests_db(inv_db_id=None, jobs=None):
    """Execute only the explicit programme manual-request fields on this investor's MT5 account.

    Parent programme_trades rows are the request source; the investor-specific
    copy supplies the per-subaccount symbol/direction, and tradeid is matched
    against MT5 magic. All same-magic child tickets are acted on regardless
    of their individual volume or execution prices.
    """
    stats = {
        'processing_success': True,
        'requests_seen': 0,
        'requests_succeeded_for_account': 0,
        'requests_retrying': 0,
        'requests_skipped': 0,
        'matched_mt5_objects': 0,
        'errors': [],
    }
    if not mt5_available():
        stats['processing_success'] = False
        stats['errors'].append('MetaTrader5 module is unavailable.')
        return stats

    if isinstance(jobs, dict):
        jobs = [jobs]
    jobs = jobs if isinstance(jobs, (list, tuple)) else []
    if not jobs:
        return stats

    trade_columns = _db_table_columns('programme_trades')
    required_columns = {
        'id', 'userid', 'programmeid', 'investor_id', 'investor_sub_account_id',
        'tradeid', 'symbol', 'ordertype', 'entry', 'stoploss', 'takeprofit',
        'live', 'orderstatus', 'order_request_status', 'order_request_filled',
        'order_request_filled_failure_reason',
    }
    if not required_columns.issubset(trade_columns):
        missing = sorted(required_columns - trade_columns)
        stats['processing_success'] = False
        stats['errors'].append(
            'programme_trades is missing manual-request/signal columns: '
            + ', '.join(missing)
        )
        print(
            '  [REQUEST] Cannot execute manual requests; missing columns: '
            + ', '.join(missing)
        )
        return stats

    for job in jobs:
        investor_id = _coerce_int(job.get('investor_id'))
        sub_account_id = _coerce_int(job.get('sub_account_id'))
        programme_id = _coerce_int(job.get('programme_id'))
        if inv_db_id is not None and investor_id != _coerce_int(inv_db_id):
            continue

        programme_userid = resolve_programme_userid(programme_id, job)
        if not all((investor_id and investor_id > 0, sub_account_id and sub_account_id > 0,
                    programme_id and programme_id > 0, programme_userid and programme_userid > 0)):
            stats['requests_skipped'] += 1
            continue

        inv = int(investor_id)
        sub = int(sub_account_id)
        pid = int(programme_id)
        uid = int(programme_userid)

        active_now, active_reason = is_active_investor_for_programme(inv, sub, pid)
        if not active_now:
            stats['requests_skipped'] += 1
            stats['errors'].append(
                f"Investor {inv}/subaccount {sub} is not currently eligible for programme {pid}: {active_reason}"
            )
            print(
                f"  [REQUEST] ⏭️ Skipping inactive/unverified investor={inv} sub={sub} "
                f"programme={pid}: {active_reason}"
            )
            continue

        # tradeid links the programme-owned master row and is also the MT5 magic
        # identity used to find every child order/position. SQL literals are inlined
        # because mysqlremote substitutes one bound placeholder per request.
        request_sql = f"""
            SELECT p.id AS programme_trade_id,
                   p.userid AS programme_userid,
                   p.programmeid AS programme_id,
                   p.tradeid,
                   p.symbol AS programme_symbol,
                   p.ordertype AS programme_ordertype,
                   p.entry AS programme_entry,
                   p.stoploss AS programme_stoploss,
                   p.takeprofit AS programme_takeprofit,
                   p.orderstatus AS programme_orderstatus,
                   p.live AS programme_live,
                   p.order_request_status,
                   p.order_request_filled AS programme_request_filled,
                   p.order_request_filled_failure_reason AS programme_failure_reason,
                   t.id AS investor_trade_id,
                   t.symbol AS investor_symbol,
                   t.ordertype AS investor_ordertype,
                   t.entry AS investor_entry,
                   t.stoploss AS investor_stoploss,
                   t.takeprofit AS investor_takeprofit,
                   t.orderstatus AS investor_orderstatus,
                   t.live AS investor_live,
                   t.order_request_status AS investor_request_status,
                   t.order_request_filled AS investor_request_filled,
                   t.order_request_filled_failure_reason AS investor_failure_reason
              FROM programme_trades p
              LEFT JOIN programme_trades t
                     ON t.userid = p.userid
                    AND t.programmeid = p.programmeid
                    AND t.tradeid = p.tradeid
                    AND t.investor_id = {inv}
                    AND t.investor_sub_account_id = {sub}
                    AND t.live = 1
                    AND LOWER(TRIM(COALESCE(t.orderstatus, ''))) IN ('pending', 'running')
             WHERE p.userid = {uid}
               AND p.programmeid = {pid}
               AND p.investor_id IS NULL
               AND p.live = 1
               AND p.tradeid IS NOT NULL
               AND TRIM(COALESCE(p.order_request_status, '')) <> ''
               AND EXISTS (
                    SELECT 1
                      FROM programme_revenue_deals d
                      INNER JOIN harvhub h
                              ON h.id = d.investor_id
                             AND h.sub_account_id = d.investor_sub_account_id
                     WHERE d.programme_id = p.programmeid
                       AND d.investor_id = {inv}
                       AND d.investor_sub_account_id = {sub}
                       AND h.account_contract_status = 'contract_active'
               )
             ORDER BY p.id ASC, t.id ASC
        """
        request_rows, request_error = _strict_db_rows(
            request_sql, None, timeout=20.0
        )
        if request_rows is None:
            stats['processing_success'] = False
            stats['errors'].append(
                f"Could not read manual requests for investor={inv}/sub={sub}, "
                f"programme={pid}: {request_error}"
            )
            continue
        if not request_rows:
            continue

        counts_sql = f"""
            SELECT tradeid, COUNT(*) AS row_count
              FROM programme_trades
             WHERE userid = {uid}
               AND programmeid = {pid}
               AND investor_id IS NULL
               AND tradeid IS NOT NULL
             GROUP BY tradeid
        """
        canonical_count_rows, counts_error = _strict_db_rows(
            counts_sql, None, timeout=15.0
        )
        if canonical_count_rows is None:
            stats['processing_success'] = False
            stats['errors'].append(
                f"Could not verify programme trade identities for programme={pid}: {counts_error}"
            )
            continue
        master_tradeid_counts = {}
        for item in canonical_count_rows:
            tid = _coerce_int(_raw_first_key(item, 'tradeid'))
            count = _coerce_int(_raw_first_key(item, 'row_count')) or 0
            if tid:
                master_tradeid_counts[tid] = count

        # The inherited copy establishes the programme/subaccount relationship.
        # The magic catalogue proves that this MT5 magic is not shared with a
        # different programme on this exact investor terminal.
        magic_catalogue = fetch_subaccount_magic_catalog(inv, sub)

        processed_master_ids = set()
        for row in request_rows:
            master_id = _coerce_int(_raw_first_key(row, 'programme_trade_id'))
            if not master_id or master_id in processed_master_ids:
                continue
            processed_master_ids.add(master_id)

            request_status = _pt_request_normalize(
                _raw_first_key(row, 'order_request_status')
            )
            if request_status not in (
                'position closing requested',
                'order closing requested',
                'breakeven requested',
            ):
                continue

            stats['requests_seen'] += 1
            tradeid = _coerce_int(_raw_first_key(row, 'tradeid'))
            investor_row_id = _coerce_int(_raw_first_key(row, 'investor_trade_id'))
            status_sql = _pt_sql_literal(request_status)

            latest_master_sql = f"""
                SELECT order_request_status, orderstatus, live
                  FROM programme_trades
                 WHERE id = {int(master_id)}
                   AND userid = {uid}
                   AND programmeid = {pid}
                   AND investor_id IS NULL
                 LIMIT 1
            """
            latest_master = db_fetch_one(
                latest_master_sql, None, timeout=10.0, quiet=True
            ) or {}
            if _pt_request_normalize(
                _raw_first_key(latest_master, 'order_request_status')
            ) != request_status:
                stats['requests_skipped'] += 1
                continue

            if not tradeid:
                reason = _pt_request_retry_reason(
                    'The canonical programme trade has no tradeid; its investor copy cannot be resolved.'
                )
                db_execute(
                    f"UPDATE programme_trades SET order_request_filled = 0, "
                    f"order_request_filled_failure_reason = {_pt_sql_literal(reason)} "
                    f"WHERE id = {int(master_id)} AND userid = {uid} "
                    f"AND programmeid = {pid} AND investor_id IS NULL "
                    f"AND LOWER(REPLACE(REPLACE(TRIM(COALESCE(order_request_status, '')), '_', ' '), '-', ' ')) = {status_sql}",
                    None, timeout=15.0, quiet=True,
                )
                _pt_request_update_analytics(
                    master_id, uid, pid, request_status, 0, reason
                )
                stats['requests_retrying'] += 1
                stats['errors'].append(reason)
                continue

            required_parent_status = (
                'pending' if request_status == 'order closing requested' else 'running'
            )
            latest_status = str(
                _raw_first_key(latest_master, 'orderstatus') or ''
            ).strip().lower()
            if int(_coerce_int(_raw_first_key(latest_master, 'live')) or 0) != 1:
                reason = _pt_request_retry_reason(
                    'The canonical programme signal is no longer marked LIVE; manual execution is blocked.'
                )
                _pt_request_aggregate_programme_fill(
                    master_id, tradeid, uid, pid, request_status,
                    forced_failure=reason,
                )
                stats['requests_retrying'] += 1
                stats['errors'].append(reason)
                continue
            if latest_status != required_parent_status:
                reason = _pt_request_retry_reason(
                    f"Request '{request_status}' requires programme orderstatus "
                    f"'{required_parent_status}', but the master row is '{latest_status or 'empty'}'."
                )
                _pt_request_aggregate_programme_fill(
                    master_id, tradeid, uid, pid, request_status,
                    forced_failure=reason,
                )
                stats['requests_retrying'] += 1
                stats['errors'].append(reason)
                continue

            if not investor_row_id:
                reason = _pt_request_retry_reason(
                    f'No LIVE investor programme_trades copy exists for tradeid {tradeid} '
                    f'on investor {inv}/subaccount {sub}; waiting for sync.'
                )
                db_execute(
                    f"UPDATE programme_trades SET order_request_filled = 0, "
                    f"order_request_filled_failure_reason = {_pt_sql_literal(reason)} "
                    f"WHERE id = {int(master_id)} AND userid = {uid} "
                    f"AND programmeid = {pid} AND investor_id IS NULL "
                    f"AND LOWER(REPLACE(REPLACE(TRIM(COALESCE(order_request_status, '')), '_', ' '), '-', ' ')) = {status_sql}",
                    None, timeout=15.0, quiet=True,
                )
                _pt_request_update_analytics(
                    master_id, uid, pid, request_status, 0, reason
                )
                stats['requests_retrying'] += 1
                stats['errors'].append(reason)
                continue

            investor_orderstatus = str(
                _raw_first_key(row, 'investor_orderstatus') or ''
            ).strip().lower()
            investor_live = int(
                _coerce_int(_raw_first_key(row, 'investor_live')) or 0
            )
            if investor_live != 1 or investor_orderstatus not in ('pending', 'running'):
                reason = _pt_request_retry_reason(
                    'The investor signal copy is not LIVE and pending/running; refusing an unrelated or historical trade row.'
                )
                update_copy_failure_sql = f"""
                    UPDATE programme_trades
                       SET order_request_filled = 0,
                           order_request_filled_failure_reason = {_pt_sql_literal(reason)}
                     WHERE userid = {uid} AND programmeid = {pid}
                       AND tradeid = {int(tradeid)} AND investor_id = {inv}
                       AND investor_sub_account_id = {sub} AND live = 1
                """
                db_execute(update_copy_failure_sql, None, timeout=15.0, quiet=True)
                _pt_request_aggregate_programme_fill(
                    master_id, tradeid, uid, pid,
                    request_status, forced_failure=reason,
                )
                stats['requests_retrying'] += 1
                stats['errors'].append(reason)
                continue

            reason = None
            matched_this_request = 0
            if master_tradeid_counts.get(tradeid, 0) != 1:
                action_ok = False
                reason = _pt_request_retry_reason(
                    f'Canonical programme tradeid {tradeid} is duplicated inside programme {pid}; refusing an ambiguous source row.'
                )
            elif not magic_catalogue.get('ok'):
                action_ok = False
                reason = _pt_request_retry_reason(
                    'Could not verify this subaccount magic catalogue: '
                    + str(magic_catalogue.get('reason') or 'catalogue unavailable')
                )
            elif tradeid in (magic_catalogue.get('ambiguous') or set()):
                action_ok = False
                reason = _pt_request_retry_reason(
                    f'MT5 magic {tradeid} is associated with more than one programme on this investor subaccount; refusing cross-programme action.'
                )
            elif pid not in set(magic_catalogue.get('by_magic', {}).get(tradeid, set())):
                action_ok = False
                reason = _pt_request_retry_reason(
                    f'MT5 magic {tradeid} is not mapped to programme {pid} on this investor subaccount.'
                )
            else:
                # The inherited copy supplies the instrument and direction.
                # tradeid/magic is the unique primary identity; price levels
                # are not used to select broker tickets.
                def row_value(copy_key, parent_key):
                    value = _raw_first_key(row, copy_key)
                    return value if value not in (None, '', 'NULL') else _raw_first_key(row, parent_key)

                signal = {
                    'symbol': str(
                        row_value('investor_symbol', 'programme_symbol') or ''
                    ).strip(),
                    'ordertype': row_value('investor_ordertype', 'programme_ordertype'),
                    'entry': row_value('investor_entry', 'programme_entry'),
                    'stoploss': row_value('investor_stoploss', 'programme_stoploss'),
                    'takeprofit': row_value('investor_takeprofit', 'programme_takeprofit'),
                }
                programme_symbol = str(
                    _raw_first_key(row, 'programme_symbol') or ''
                ).strip()
                if (
                    not signal['symbol'] or not programme_symbol
                    or _pt_request_symbol_key(signal['symbol']) != _pt_request_symbol_key(programme_symbol)
                ):
                    action_ok = False
                    reason = _pt_request_retry_reason(
                        'The investor programme-trade copy symbol does not match the canonical programme signal.'
                    )
                elif not _pt_request_signal_has_price_levels(signal):
                    action_ok = False
                    reason = _pt_request_retry_reason(
                        'The inherited programme-trade copy has no valid symbol/direction metadata for its canonical magic; refusing to guess.'
                    )
                else:
                    snap_ok, snap_orders, snap_positions, snap_error = _pt_request_mt5_snapshot()
                    if not snap_ok:
                        action_ok = False
                        reason = _pt_request_retry_reason(snap_error)
                    else:
                        if request_status == 'order closing requested':
                            matches = [
                                obj for obj in snap_orders
                                if _pt_request_matches_signal(obj, signal, tradeid, is_position=False)
                            ]
                        else:
                            matches = [
                                obj for obj in snap_positions
                                if _pt_request_matches_signal(obj, signal, tradeid, is_position=True)
                            ]
                        matched_this_request = len(matches)
                        stats['matched_mt5_objects'] += matched_this_request
                        if request_status == 'order closing requested':
                            action_ok, reason = _pt_request_close_order_on_mt5(tradeid, signal)
                        elif request_status == 'position closing requested':
                            action_ok, reason = _pt_request_close_positions_on_mt5(tradeid, signal)
                        else:
                            action_ok, reason = _pt_request_breakeven_trade_on_mt5(tradeid, signal)

            if action_ok:
                reason = None
                print(
                    f"  [REQUEST] ✅ {request_status} magic-identity match completed for "
                    f"investor={inv} sub={sub}, programme={pid}, tradeid={tradeid}; "
                    f"matched MT5 object(s)={matched_this_request}"
                )
            else:
                reason = _pt_request_retry_reason(reason)
                print(
                    f"  [REQUEST] ⏳ {request_status} not yet completed for "
                    f"investor={inv} sub={sub}, programme={pid}, tradeid={tradeid}: {reason}"
                )

            # Guard against a request being cancelled/replaced while MT5 acts.
            latest_after_action_sql = f"""
                SELECT order_request_status
                  FROM programme_trades
                 WHERE id = {int(master_id)}
                   AND userid = {uid}
                   AND programmeid = {pid}
                   AND investor_id IS NULL
                 LIMIT 1
            """
            latest_after_action = db_fetch_one(
                latest_after_action_sql, None, timeout=10.0, quiet=True
            ) or {}
            if _pt_request_normalize(
                _raw_first_key(latest_after_action, 'order_request_status')
            ) != request_status:
                stats['requests_skipped'] += 1
                print(
                    f"  [REQUEST] Parent request changed during MT5 handling for "
                    f"tradeid={tradeid}; not updating stale request flags."
                )
                continue

            update_copy_sql = f"""
                UPDATE programme_trades cp
                INNER JOIN programme_trades p
                        ON p.id = {int(master_id)}
                       AND p.userid = {uid}
                       AND p.programmeid = {pid}
                       AND p.investor_id IS NULL
                       AND LOWER(REPLACE(REPLACE(TRIM(COALESCE(p.order_request_status, '')), '_', ' '), '-', ' ')) = {status_sql}
                   SET cp.order_request_status = {status_sql},
                       cp.order_request_filled = {1 if action_ok else 0},
                       cp.order_request_filled_failure_reason = {_pt_sql_literal(reason)}
                 WHERE cp.userid = {uid}
                   AND cp.programmeid = {pid}
                   AND cp.tradeid = {int(tradeid)}
                   AND cp.investor_id = {inv}
                   AND cp.investor_sub_account_id = {sub}
                   AND cp.live = 1
                   AND LOWER(TRIM(COALESCE(cp.orderstatus, ''))) IN ('pending', 'running')
            """
            copy_ok, _, copy_error = db_execute(
                update_copy_sql, None, timeout=15.0, quiet=True
            )
            forced_failure = None
            if not copy_ok:
                forced_failure = _pt_request_retry_reason(
                    'MT5 action status could not be saved to this investor trade copy: '
                    + str(copy_error)
                )
                action_ok = False
                reason = forced_failure

            all_investors_filled, aggregate_reason = _pt_request_aggregate_programme_fill(
                master_id, tradeid, uid, pid,
                request_status, forced_failure=forced_failure,
            )
            if action_ok:
                stats['requests_succeeded_for_account'] += 1
            else:
                stats['requests_retrying'] += 1
                stats['errors'].append(
                    reason or aggregate_reason or 'Manual request is awaiting MT5 confirmation.'
                )
            if all_investors_filled:
                print(
                    f"  [REQUEST] ✅ Programme request for master tradeid={tradeid} "
                    'is filled across all active inherited investor copies.'
                )

    return stats

def process_single_investor_from_db(job):
    """
    DB version of process_single_investor_local().

    Stage order:
        programme-requested MT5 Breakeven / Close commands (priority stage)
        gate 1 (restriction)
        Revenue Target calculation
        Recovery Generator (immediately after Revenue Target; internally runs
        the authorization audit and investor risk-sizing prerequisites)
        Revenue Target alarm gate
        if not alarmed:
            chunk_orders_with_volume_beyond_max_db
            place_usd_orders_db
            check_pending_orders_risk_db
            orders_reward_correction_db
            apply_dynamic_breakeven_db
            trades_analytics_db

    The parent already did discovery — we reuse its job list verbatim
    instead of re-running fetch_vps_investor_jobs() (which has been
    observed to return 0 in worker processes for reasons that are not
    fully understood, possibly a bridge/IP edge case).

    Skipped entirely when no MT5 terminal path can be resolved.
    """

    global recovery_generator_alert
    global unauthorized_alert

    inv_db_id = job.get('investor_id')
    sub_id = job.get('sub_account_id')
    programme_id = job.get('programme_id')
    programme_name = job.get('programme_name') or ''

    eligible, eligibility_reason = is_active_investor_for_programme(
        job.get('investor_id'), job.get('sub_account_id'), job.get('programme_id')
    )
    if not eligible:
        print(
            f"[ELIGIBILITY] SKIPPED investor={job.get('investor_id')} "
            f"sub={job.get('sub_account_id')} programme={job.get('programme_id')}: "
            f"{eligibility_reason}"
        )
        return {
            'investor_id': job.get('investor_id'),
            'sub_account_id': job.get('sub_account_id'),
            'programme_id': job.get('programme_id'),
            'success': False,
            'error': 'inactive_or_unverified_programme_contract',
        }

    print(f"\n[START] ⚙️ Investor {inv_db_id} (sub {sub_id}) | "
          f"programme '{programme_name}' (id={programme_id})")

    account_stats = {
        'investor_id':    inv_db_id,
        'sub_account_id': sub_id,
        'programme_id':   programme_id,
        'success':        False,
        'restricted':     False,
        'restricted_kind': None,
        'restricted_window': None,
        'restricted_day': None,
        'alarm_hit':      False,
        'alarm_mode':     None,
        'alarm_details':  None,
        'unauthorized_hit': False,
        'unauthorized_types': [],
        'account_mode':   'UNKNOWN',
        'account_type':   'UNKNOWN',
        'is_real_account': False,
        'stages': {},
    }

    _worker_init()

    # ------------------------------------------------------------------
    # MT5 PATH RESOLUTION — must succeed, else skip investor entirely
    # ------------------------------------------------------------------
    terminal_path = resolve_investor_terminal_path(inv_db_id, sub_id)

    if terminal_path is None:
        print(f"[MT5] ⏭️ No terminal path for investor {inv_db_id} "
              f"(sub {sub_id}) — SKIPPING investor entirely")
        _pt_request_record_preconnect_failure(
            job, 'Investor MT5 terminal path is unavailable; programme request was not executed.'
        )
        account_stats['success'] = False
        account_stats['error'] = 'no_terminal_path'
        try:
            if _db is not None:
                _db.release_channel()
        except Exception:
            pass
        return account_stats

    # ------------------------------------------------------------------
    # MT5 CONNECT
    # ------------------------------------------------------------------
    try:
        print(f"[MT5-PATH] final resolved terminal = {terminal_path}")

        mt5_ok = mt5_connect(terminal_path=terminal_path, timeout_ms=15000)
        if mt5_ok:
            if not verify_mt5_subaccount_identity(inv_db_id, sub_id):
                _pt_request_record_preconnect_failure(
                    job, 'MT5 login/server identity verification failed; refusing to execute this programme request on an unverified account.'
                )
                mt5_disconnect()
                account_stats['success'] = False
                account_stats['error'] = 'mt5_account_identity_mismatch'
                try:
                    if _db is not None:
                        _db.release_channel()
                except Exception:
                    pass
                return account_stats
            print(f"[MT5] ✅ Connected (custom path) "
                  f"for investor {inv_db_id} (sub {sub_id})")
        else:
            print(f"[MT5] ⚠️ mt5_connect returned False "
                  f"(last_error={mt5_last_error()}) — SKIPPING investor")
            _pt_request_record_preconnect_failure(
                job, f'MT5 connection failed ({mt5_last_error()}); programme request will retry when the terminal reconnects.'
            )
            account_stats['success'] = False
            account_stats['error'] = 'mt5_connect_failed'
            try:
                if _db is not None:
                    _db.release_channel()
            except Exception:
                pass
            return account_stats
    except Exception as e:
        print(f"[MT5] ⚠️ mt5_connect raised: {e} — SKIPPING investor")
        _pt_request_record_preconnect_failure(
            job, f'MT5 connection raised an exception ({e}); programme request will retry when the terminal reconnects.'
        )
        account_stats['success'] = False
        account_stats['error'] = f'mt5_connect_exception: {e}'
        try:
            if _db is not None:
                _db.release_channel()
        except Exception:
            pass
        return account_stats

    try:
        # ==================================================================
        # JOB LIST — use the parent's programme-VPS-scoped job unchanged.
        # The parent discovers programme owner/follower bindings from
        # programme_vps + active programme_vps_hosts_followers rows, then
        # discovers actual investor jobs from LIVE pending/running investor
        # copies in programme_trades. The worker must not broaden that scope
        # with account-VPS rows or revenue-deal contract-status checks.
        # ==================================================================
        _cached_jobs = [job]

        # ==================================================================
        # ACCOUNT INFO / TYPE
        # ==================================================================
        try:
            acc = mt5_account_info()
            if acc is not None:
                account_stats['account_type'] = 'REAL' if getattr(acc, 'trade_mode', 0) == 0 else 'DEMO'
                account_stats['account_mode'] = 'real' if getattr(acc, 'trade_mode', 0) == 0 else 'demo'
                account_stats['is_real_account'] = bool(getattr(acc, 'trade_mode', 0) == 0)
                print(f"[{inv_db_id}] Account mode: {account_stats['account_mode'].upper()} "
                      f"({account_stats['account_type']})")
        except Exception as acc_err:
            print(f"[{inv_db_id}] ⚠️ account_info failed: {acc_err}")

        # ==================================================================
        # MANUAL PROGRAMME REQUESTS — PRIORITY EXECUTION STAGE
        # This deliberately runs BEFORE restricted-timerange and revenue-target
        # gates. Programme close/breakeven commands must reach each connected
        # investor subaccount even when normal new-trade activity is restricted.
        # It does not read programme_training_configuration. Unfilled failures
        # remain queued with a reason and are retried on the next loop.
        # ==================================================================
        manual_request_stats = run_stage(
            'process_programme_order_requests_db',
            process_programme_order_requests_db,
            inv_db_id=inv_db_id,
            jobs=_cached_jobs,
        )
        account_stats['stages']['process_programme_order_requests_db'] = manual_request_stats
        account_stats['programme_order_request_stats'] = manual_request_stats

        # ==================================================================
        # GATE 1 — RESTRICTED TIMERANGE
        # ==================================================================
        timerange_stats = run_stage(
            'restricted_timerange_db',
            restricted_timerange_db,
            inv_db_id=inv_db_id,
            jobs=_cached_jobs,
        )
        account_stats['stages']['restricted_timerange_db'] = timerange_stats
        account_stats['restricted_timerange_db_stats'] = timerange_stats

        verdict = _programme_restriction_verdict(timerange_stats, programme_id)
        is_restricted_now = bool(verdict.get('is_restricted'))

        if is_restricted_now:
            _restriction_skip_banner(
                inv_db_id=inv_db_id,
                sub_id=sub_id,
                programme_id=programme_id,
                programme_name=programme_name,
                verdict=verdict,
            )

            account_stats['restricted'] = True
            account_stats['restricted_kind'] = verdict.get('kind')
            account_stats['restricted_window'] = verdict.get('window_desc')
            account_stats['restricted_day'] = verdict.get('day_name')
            account_stats['success'] = True
            return account_stats

        account_stats['restricted'] = False

        # ==================================================================
        # GATE 2 — RECENT HIGHEST BALANCE TARGET + DAILY-TARGET ALARM
        # ==================================================================
        recent_balance_stats = run_stage(
            'recent_highest_balance_revenue_target_db',
            recent_highest_balance_revenue_target_db,
            inv_db_id=inv_db_id,
            jobs=_cached_jobs,
        )
        account_stats['stages']['recent_highest_balance_revenue_target_db'] = recent_balance_stats
        account_stats['recent_highest_balance_stats'] = recent_balance_stats

        recovery_stats = run_stage(
            'recovery_generator_system_db',
            recovery_generator_system_db,
            inv_db_id=inv_db_id,
            jobs=_cached_jobs,
        )
        account_stats['stages']['recovery_generator_system_db'] = recovery_stats
        account_stats['recovery_generator_stats'] = recovery_stats

        alert = recent_highest_alert or {}
        alarm_hit = bool(alert.get('is_triggered'))

        if alarm_hit:
            _revenue_target_alarm_banner(
                inv_db_id=inv_db_id,
                sub_id=sub_id,
                programme_id=programme_id,
                programme_name=programme_name,
                alert=alert,
            )

            account_stats['alarm_hit'] = True
            account_stats['alarm_mode'] = (
                "PRE-PENDING" if alert.get('pre_pending_enabled') else "DAILY"
            )
            account_stats['alarm_details'] = {
                'daily_target_owed': alert.get('daily_target_owed'),
                'pre_pending_total': alert.get('pre_pending_total'),
                'total_all_debt':    alert.get('total_all_debt'),
                'effective_today':   alert.get('effective_today'),
                'action':            alert.get('action'),
            }
            account_stats['success'] = True
            return account_stats

        account_stats['alarm_hit'] = False

        # ==================================================================
        # CONDITION A — NO RESTRICTIONS: RUN THE FULL TRADING PIPELINE
        # ==================================================================
        print(f"  ✅ Trading allowed for {inv_db_id}")

        # ------------------------------------------------------------------
        # A3. CHUNK ORDERS WITH VOLUME BEYOND MAX
        # ------------------------------------------------------------------
        chunk_stats = run_stage(
            'chunk_orders_with_volume_beyond_max_db',
            chunk_orders_with_volume_beyond_max_db,
            inv_db_id=inv_db_id,
            jobs=_cached_jobs,
        )
        account_stats['stages']['chunk_orders_with_volume_beyond_max_db'] = chunk_stats
        account_stats['chunk_stats'] = chunk_stats

        # ------------------------------------------------------------------
        # A5. PLACE USD ORDERS
        # ------------------------------------------------------------------
        place_stats = run_stage(
            'place_usd_orders_db',
            place_usd_orders_db,
            inv_db_id=inv_db_id,
            jobs=_cached_jobs,
        )
        account_stats['stages']['place_usd_orders_db'] = place_stats
        account_stats['place_stats'] = place_stats

        # ------------------------------------------------------------------
        # A6. CHECK PENDING ORDERS RISK
        # ------------------------------------------------------------------
        risk_stats = run_stage(
            'check_pending_orders_risk_db',
            check_pending_orders_risk_db,
            inv_db_id=inv_db_id,
            jobs=_cached_jobs,
        )
        account_stats['stages']['check_pending_orders_risk_db'] = risk_stats
        account_stats['pending_risk_stats'] = risk_stats

        # ------------------------------------------------------------------
        # A7. ORDERS REWARD CORRECTION
        # ------------------------------------------------------------------
        reward_stats = run_stage(
            'orders_reward_correction_db',
            orders_reward_correction_db,
            inv_db_id=inv_db_id,
            jobs=_cached_jobs,
        )
        account_stats['stages']['orders_reward_correction_db'] = reward_stats
        account_stats['reward_correction_stats'] = reward_stats

        # ------------------------------------------------------------------
        # A8. APPLY DYNAMIC BREAKEVEN
        # ------------------------------------------------------------------
        breakeven_stats = run_stage(
            'apply_dynamic_breakeven_db',
            apply_dynamic_breakeven_db,
            inv_db_id=inv_db_id,
            jobs=_cached_jobs,
        )
        account_stats['stages']['apply_dynamic_breakeven_db'] = breakeven_stats
        account_stats['breakeven_stats'] = breakeven_stats

        # ------------------------------------------------------------------
        # A9. TRADES ANALYTICS DB
        # ------------------------------------------------------------------
        analytics_stats = run_stage(
            'trades_analytics_db',
            trades_analytics_db,
            inv_db_id=inv_db_id,
            jobs=_cached_jobs,
        )
        account_stats['stages']['trades_analytics_db'] = analytics_stats
        account_stats['analytics_stats'] = analytics_stats

        ua = unauthorized_alert or {}
        account_stats['unauthorized_hit'] = bool(ua.get('is_triggered'))
        account_stats['unauthorized_types'] = list(ua.get('unauthorized_types') or [])

        account_stats['success'] = True
        print(f"[SUCCESS] Finished full pipeline execution context for "
              f"Investor {inv_db_id} ({account_stats['account_mode']})")

    except Exception as e:
        print(f"[CRITICAL ERROR] investor={inv_db_id}: {e}")
        traceback.print_exc()
        account_stats['success'] = False
        account_stats['error'] = str(e)

    finally:
        try:
            mt5_disconnect()
        except Exception:
            pass
        try:
            if _db is not None:
                _db.release_channel()
        except Exception:
            pass

    return account_stats

# ==============================================================================
#  ORCHESTRATOR — DB-driven, no local folders
# ==============================================================================
def discover_investor_jobs():
    local_ip = get_local_ip()
    if not local_ip:
        print("⚠️ Could not determine local IP — no jobs will be created")
        return []
    return fetch_vps_investor_jobs(local_ip)


def main_loop():
    # The manager supplies a standard execution-type/interval pair to every
    # service. Preserve the legacy --loop=... switches for direct callers.
    run_as_loop = os.environ.get('HARVHUB_EXECUTION_TYPE', 'once').strip().lower() == 'loop'
    loop_interval = min(10, max(5, int(os.environ.get('HARVHUB_LOOP_INTERVAL', '5') or 5)))
    max_loops = None
    args = list(sys.argv[1:])

    for index, arg in enumerate(args):
        lower = arg.lower()
        if lower.startswith('--execution-type='):
            run_as_loop = lower.split('=', 1)[1].strip() == 'loop'
        elif lower == '--execution-type' and index + 1 < len(args):
            run_as_loop = args[index + 1].strip().lower() == 'loop'
        elif lower.startswith('--loop='):
            run_as_loop = arg.split('=')[1].lower() in ('true', 'yes', '1', 'on')
        elif lower.startswith('--interval='):
            try:
                loop_interval = min(10, max(5, int(arg.split('=', 1)[1])))
            except ValueError:
                pass
        elif lower == '--interval' and index + 1 < len(args):
            try:
                loop_interval = min(10, max(5, int(args[index + 1])))
            except ValueError:
                pass
        elif lower.startswith('--max-loops='):
            try:
                max_loops = int(arg.split('=')[1])
            except ValueError:
                pass

    loop_interval = min(10, max(5, int(loop_interval)))
    print("\n" + "=" * 60)
    print("         SIGNALS TRADING — PROGRAMME REQUEST + TRADE ORCHESTRATOR", flush=True)
    print("=" * 60)
    print("  Investor jobs: contract_active programme revenue-deal pairs on programmes hosted by this VPS")
    print("  Programme authority: explicit manual requests matched by programme tradeid/MT5 magic only")
    print(f"  Loop Mode: {'ENABLED' if run_as_loop else 'DISABLED'}")
    if run_as_loop:
        print(f"  Interval: {loop_interval}s")
        print(f"  Max Loops: {max_loops if max_loops else 'Infinite'}")
    print("=" * 60 + "\n")

    _worker_init()

    cpu_cores = os.cpu_count() or 1
    pool_size = max(1, min(cpu_cores, 4))
    print(f"🖥️  Cores detected: {cpu_cores} | Persistent worker count: {pool_size}")

    pool = mp.Pool(processes=pool_size)
    print(f"✅ Persistent pool ready ({pool_size} workers).\n")

    loop_count = 0
    try:
        while True:
            loop_count += 1
            if run_as_loop:
                print("\n" + "=" * 60)
                print(f"LOOP #{loop_count} STARTED", flush=True)
                print("=" * 60)

            try:
                print("[ORCHESTRATOR] Checking the live MySQL Remote query path...", flush=True)
                health = db_query('SELECT 1 AS bridge_health', timeout=15.0, quiet=True)
                if not _is_ok(health):
                    raise _BridgeError(f"MySQL Remote query health check failed: {health.get('message', 'unknown error')}")
                print("[ORCHESTRATOR] Database query path is healthy. Discovering active investor jobs...", flush=True)
                jobs = discover_investor_jobs()
                print(f"[ORCHESTRATOR] Job discovery complete: {len(jobs)} active investor job(s).", flush=True)

                if not jobs:
                    print(f" └─ 🔘 No active investor jobs returned by DB for "
                          f"this VPS. Sleeping {loop_interval}s.", flush=True)
                    time.sleep(loop_interval)
                    continue

                print(f"📊 Submitting {len(jobs)} investor job(s) "
                      f"via persistent pool")

                t0 = time.time()
                async_jobs = [
                    pool.apply_async(process_single_investor_from_db, args=(job,))
                    for job in jobs
                ]

                results = []
                for aj in async_jobs:
                    try:
                        results.append(aj.get(timeout=600))
                    except mp.TimeoutError:
                        print("⚠️ A worker timed out; moving on.")
                        results.append(None)
                    except Exception as job_err:
                        print(f"⚠️ Worker raised: {job_err}")
                        results.append(None)

                elapsed = time.time() - t0
                ok = sum(1 for r in results if r and r.get('success'))
                print(f"--- Cycle Complete in {elapsed:.2f}s | "
                      f"ok={ok}/{len(jobs)} ---")

            except Exception as e:
                print(f"Critical Error in Orchestrator Loop: {e}")
                traceback.print_exc()

            if not run_as_loop:
                print("Single execution completed. Exiting...")
                break

            if max_loops and loop_count >= max_loops and not run_as_loop:
                print(f"Maximum loops ({max_loops}) reached. Exiting...")
                break

            if run_as_loop and loop_interval > 0:
                for _ in range(loop_interval):
                    time.sleep(1)

    except KeyboardInterrupt:
        print("\n[main] interrupted — shutting down")

    finally:
        print("Shutting down persistent pool...")
        try:
            pool.close()
            pool.join()
        except Exception as shutdown_err:
            print(f"[WARN] Pool shutdown issue: {shutdown_err}")

        try:
            if _db is not None:
                _db.release_channel()
        except Exception:
            pass

        print("Pool shut down cleanly.")


if __name__ == "__main__":
    main_loop()
