# brokercharts.py
"""
Programme-scoped OHLC collector — DB-only, no JSON, no local files.
The server_account.chart_total_candles setting is the historical backfill and
repair target for each programme, symbol, and timeframe. Existing rows are
never purged to enforce that target; live candles may grow stored history.
REPLACES update_developers() + import os.py (fetch_charts_all_brokers).
ARCHITECTURE
------------
* Same bridge pattern as brokerdetails.py / programmetrades.py: this file
  is a TCP CLIENT of the long-running mysqlremote bridge. It does not
  launch Chrome and does not import mysqlremote.
* One channel per worker process. Batching is used aggressively so the
  bridge is hit as few times as possible.
SCOPE
-----
* Reads ONLY programme accounts (never sub-accounts / investors).
* Sources of truth:
      programme.broker / server / login / broker_password
      programme.broker_login_status  -> 'valid_credentials' only
      programme.selected_symbols     -> CSV
      programme.selected_timeframes  -> JSON array (or CSV)
      programme.timezone             -> IANA name
      programme.broker_symbols       -> CSV   (validation set)
      programme.broker_timeframes    -> JSON  (validation set)
      programme.userid               -> owner id (for email lookup)
      harvhub.email                  -> owner email
      candle_records                 -> table of stored candles
        (programmeid, userid, symbol, timeframe, candle_time, open_time,
         close_time, timezone, last_recorded_candle_time, open, high,
         low, close, candle_center, body_center, high_wick_center,
         low_wick_center, candle_width_center, volume)
CONFIGURATION GATES (in strict order)
-------------------------------------
Before touching MT5 at all:
    1. selected_symbols must be non-empty
    2. selected_timeframes must be non-empty
    3. timezone must be non-empty and resolvable
Each missing gate fires a one-time programme notification to the owner
and the programme is skipped for this cycle.
MT5 BEHAVIOUR
-------------
* Initializes the terminal with the SAME path-resolution rules as
  brokerdetails.py (rebuilt folder from server+login, no DB
  Terminal_path trust).
* Uses mt5.initialize(path=..., portable=True) WITHOUT attempting any
  login. If the terminal is not already logged in, we skip.
* Short timeout (10s) so unconfigured programmes never block the loop.
CANDLE SYNCHRONIZATION — PAGINATED ROTATIONS
-------------------------------------------
For every selected (symbol, timeframe) pair:
  1. Fetch the newest 1,500 MT5 bars from position 0, including the currently
     forming candle so its OHLC snapshot can be checked on every cycle.
  2. Compare every valid MT5 OHLCV snapshot with the row already stored at the
     same candle_time. If values match, leave that database record untouched.
  3. If a stored OHLCV snapshot is inaccurate, delete only that incorrect row
     and insert the complete corrected MT5 candle. Insert genuinely new
     timestamps normally. A latest-candle refresh is never a routine delete.
  4. Re-fetch the newest stored timestamp directly from MT5 where possible so
     the current forming candle is compared against the freshest source values.
  5. Preserve valid source candles even when their open is outside the previous
     candle range; such a move may be a genuine market/broker gap, not bad OHLC.
  6. Fetch the next older page using copy_rates_from_pos(..., start_pos, count).
     Advance start_pos by actual new source bars, not by corrected row inserts.
  7. Stop when repeated empty responses confirm no more history is available
     or the pair reaches the configured stored-candle target. Failed API requests retry at the
     same position with smaller requests; a persistent one-bar error never
     advances the cursor or silently skips the candle.
Rows are scoped by programme, symbol, and timeframe. The configured row count is
a historical backfill target, not a deletion limit. Existing valid rows are
never purged to enforce it; newly arriving live candles may grow the table
beyond that target.
BATCHING
--------
candle_records is populated via multi-row INSERT statements. Batch
size is 200 rows / statement (matches the old update_developers
pattern and keeps the PHP response frame well under the bridge cap).
NOTIFICATIONS
-------------
Priority order (the first failing gate wins):
    selected_symbols   -> record_programme_no_selected_symbols_notification
    selected_timeframes-> record_programme_no_selected_timeframes_notification
    timezone           -> record_programme_no_timezone_notification
"""
import os
import re
import sys
import time
import json
import math
import socket
import struct
import threading
import traceback
import multiprocessing as mp
import urllib.request
from datetime import datetime, timezone as dt_timezone
try:
    import pytz
    from zoneinfo import ZoneInfo
except Exception:
    pytz = None
    ZoneInfo = None
try:
    import tzlocal as _tzlocal
except Exception:
    _tzlocal = None
try:
    import MetaTrader5 as mt5
except Exception:
    mt5 = None
# ==============================================================================
#  PATHS — must match brokerdetails.py exactly
# ==============================================================================
DEFAULT_MT5_PATH     = r"C:\xampp\htdocs\harvhubAI\mt5\MetaTrader 5"
MT5_DESTINATION_PATH = r"C:\xampp\htdocs\harvhubAI\mt5"
# ==============================================================================
#  BRIDGE CLIENT — same protocol as brokerdetails.py / programmetrades.py
# ==============================================================================
BRIDGE_HOST = "127.0.0.1"
BRIDGE_PORT = 5599
BRIDGE_OWNER = "brokercharts"
_HEADER = struct.Struct(">I")
MAX_FRAME = 64 * 1024 * 1024
_MT5_IPC_LOCK = threading.RLock()
class _BridgeError(Exception):
    pass
class _BridgeClient:
    """
    Persistent-socket bridge client. One TCP connection per worker.
    Reconnects once on transport failure.
    """
    def __init__(self, host=BRIDGE_HOST, port=BRIDGE_PORT, owner=BRIDGE_OWNER):
        self.host = host
        self.port = port
        # mysqlremote reuses an idle channel when the owner string matches.
        # The collector's parent process and pool workers must never share one
        # owner/channel: a worker releases its channel when its job finishes,
        # which otherwise invalidates the parent's next candidate query.
        self.owner = f"{owner}:{os.getpid()}"
        self.channel_id = None
        self._channel_lock = threading.Lock()
        self._sock = None
        self._sock_lock = threading.Lock()
    @staticmethod
    def _recv_exact(sock, n):
        buf = bytearray()
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:
                return None
            buf.extend(chunk)
        return bytes(buf)
    def _connect(self):
        return socket.create_connection((self.host, self.port), timeout=15)
    def _close_sock_locked(self):
        if self._sock is not None:
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None
    def _send_request(self, req):
        with self._sock_lock:
            payload = json.dumps(req, ensure_ascii=False).encode('utf-8')
            frame = _HEADER.pack(len(payload)) + payload
            for attempt in (1, 2):
                try:
                    if self._sock is None:
                        self._sock = self._connect()
                    self._sock.sendall(frame)
                    header = self._recv_exact(self._sock, _HEADER.size)
                    if header is None:
                        raise _BridgeError("bridge closed connection (no header)")
                    (length,) = _HEADER.unpack(header)
                    if length == 0 or length > MAX_FRAME:
                        raise _BridgeError(f"bad frame length: {length}")
                    body = self._recv_exact(self._sock, length)
                    if body is None:
                        raise _BridgeError("bridge closed connection (no body)")
                    return json.loads(body.decode('utf-8'))
                except _BridgeError:
                    self._close_sock_locked()
                    raise
                except Exception as e:
                    self._close_sock_locked()
                    if attempt == 2:
                        raise _BridgeError(
                            f"cannot talk to bridge {self.host}:{self.port}: {e}"
                        )
    def acquire_channel(self):
        with self._channel_lock:
            if self.channel_id is not None:
                return self.channel_id
            resp = self._send_request({
                'op': 'acquire_channel',
                'owner': self.owner,
            })
            if resp.get('status') != 'success':
                raise _BridgeError(
                    f"acquire_channel failed: {resp.get('message')}"
                )
            self.channel_id = int(resp['channel_id'])
            print(f"[bridge] {self.owner}: acquired channel "
                  f"queryrequest_{self.channel_id} / "
                  f"queryresult_{self.channel_id} "
                  f"(window #{resp.get('window_id')})")
            return self.channel_id
    def release_channel(self):
        with self._channel_lock:
            if self.channel_id is None:
                return
            cid = self.channel_id
            self.channel_id = None
            try:
                self._send_request({'op': 'release_channel',
                                    'channel_id': cid})
                print(f"[bridge] {self.owner}: released channel {cid}")
            except Exception as e:
                print(f"[bridge] release_channel({cid}) failed: {e}")
            finally:
                with self._sock_lock:
                    self._close_sock_locked()
    def put(self, request, timeout=None):
        req = dict(request)
        if 'sql' not in req or 'channel_id' in req:
            return self._send_request(req)

        # A stale server-side channel must not turn a database failure into an
        # apparent empty programme list. Reacquire once when mysqlremote says
        # the channel was released, then repeat the same SQL request.
        for attempt in range(2):
            channel_id = self.acquire_channel()
            req['channel_id'] = channel_id
            response = self._send_request(req)
            message = str((response or {}).get('message') or '').lower()
            stale_channel = (
                (response or {}).get('status') == 'error'
                and 'unknown channel_id' in message
            )
            if not stale_channel:
                return response

            print(
                f"[bridge] {self.owner}: server no longer has channel "
                f"{channel_id}; reacquiring and retrying SQL"
            )
            with self._channel_lock:
                if self.channel_id == channel_id:
                    self.channel_id = None
            req.pop('channel_id', None)

            if attempt == 1:
                return response

        return response
# ==============================================================================
#  MODULE-SCOPE HELPERS
# ==============================================================================
_db = None
def _worker_init():
    global _db
    _db = _BridgeClient()
def db_query(sql, params=None, timeout=20.0, quiet=False):
    global _db
    if _db is None:
        _db = _BridgeClient()
    start = time.perf_counter()
    try:
        result = _db.put({'sql': sql, 'params': params, 'timeout': timeout})
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
def _is_ok(res):
    return bool(res) and res.get('status') == 'success'
def db_fetch_all(sql, params=None, timeout=20.0, quiet=True):
    res = db_query(sql, params, timeout=timeout, quiet=quiet)
    if not _is_ok(res):
        return []
    return [_normalize_row(r) for r in (res.get('results') or [])]
def db_fetch_one(sql, params=None, timeout=20.0, quiet=True):
    rows = db_fetch_all(sql, params, timeout=timeout, quiet=quiet)
    return rows[0] if rows else None
def db_execute(sql, params=None, timeout=20.0, quiet=True):
    res = db_query(sql, params, timeout=timeout, quiet=quiet)
    if not _is_ok(res):
        return False, 0, res.get('message', 'error')
    return True, int(res.get('affected_rows') or 0), res.get('message', '')
# ---------------------------------------------------------------------------
# Notification PDO adapter (single instance per worker)
# ---------------------------------------------------------------------------
try:
    import notification_service as _notif_service
    _NOTIF_PDO = _notif_service.make_pdo(db_query)
except Exception:
    _notif_service = None
    _NOTIF_PDO = None
# ---------------------------------------------------------------------------
# Row normalisation + primitive coercion
# ---------------------------------------------------------------------------
def _normalize_row(row):
    if row is None:
        return None
    if isinstance(row, dict):
        return {
            (k.strip().lower() if isinstance(k, str) else k): v
            for k, v in row.items()
        }
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
def _is_nullish(value):
    if value is None:
        return True
    s = str(value).strip().lower()
    return s in ('', 'null', 'none', 'nan')
# ---------------------------------------------------------------------------
# SQL literal helpers
# ---------------------------------------------------------------------------
def _sql_quote(value):
    if value is None:
        return 'NULL'
    if isinstance(value, bool):
        return '1' if value else '0'
    if isinstance(value, (int, float)):
        return repr(value)
    s = str(value)
    s = s.replace('\\\\\\\\', '\\\\\\\\\\\\\\\\').replace("'", "''")
    return f"'{s}'"
def _sql_int(value):
    try:
        if value is None:
            return 'NULL'
        return str(int(value))
    except Exception:
        return 'NULL'
def _sql_dt(value):
    if value is None:
        return 'NULL'
    if isinstance(value, datetime):
        return "'" + value.strftime('%Y-%m-%d %H:%M:%S') + "'"
    s = str(value).strip()
    if not s or s.lower() in ('null', 'none', 'nan'):
        return 'NULL'
    return "'" + s.replace("'", "''") + "'"
# ==============================================================================
#  TIMEFRAMES
# ==============================================================================
def _build_timeframe_map():
    if mt5 is None:
        return {}
    return {
        "1m":  mt5.TIMEFRAME_M1,  "2m":  mt5.TIMEFRAME_M2,  "3m":  mt5.TIMEFRAME_M3,
        "4m":  mt5.TIMEFRAME_M4,  "5m":  mt5.TIMEFRAME_M5,  "6m":  mt5.TIMEFRAME_M6,
        "10m": mt5.TIMEFRAME_M10, "12m": mt5.TIMEFRAME_M12, "15m": mt5.TIMEFRAME_M15,
        "20m": mt5.TIMEFRAME_M20, "30m": mt5.TIMEFRAME_M30,
        "1h":  mt5.TIMEFRAME_H1,  "2h":  mt5.TIMEFRAME_H2,  "3h":  mt5.TIMEFRAME_H3,
        "4h":  mt5.TIMEFRAME_H4,  "6h":  mt5.TIMEFRAME_H6,  "8h":  mt5.TIMEFRAME_H8,
        "12h": mt5.TIMEFRAME_H12, "1d":  mt5.TIMEFRAME_D1,  "1w":  mt5.TIMEFRAME_W1,
        "1mo": mt5.TIMEFRAME_MN1,
    }
TIMEFRAME_SECONDS = {
    "1m": 60, "2m": 120, "3m": 180, "4m": 240, "5m": 300, "6m": 360,
    "10m": 600, "12m": 720, "15m": 900, "20m": 1200, "30m": 1800,
    "1h": 3600, "2h": 7200, "3h": 10800, "4h": 14400, "6h": 21600,
    "8h": 28800, "12h": 43200, "1d": 86400, "1w": 604800, "1mo": 2592000,
}
# ---- Rotation milestones -----------------------------------------------------
# Progress milestones for a single programme/symbol/timeframe pair.
ROUND_TARGETS = ()
# Set from server_account.chart_total_candles separately for each programme job.
# Zero means unconfigured and prevents candle collection.
MAX_BARS_PER_TF = 0
# A worker reconciles its existing stored history against MT5 once per pair
# after startup, repairing valid-looking partial OHLC snapshots left by older
# collector versions without repeatedly scanning the full configured target every cycle.
_REPAIRED_PAIRS_THIS_PROCESS = set()
# Last MT5 source candle time seen by _latest_pass for each pair. Used only
# to advance historical pagination when genuinely newer MT5 bars appear;
# correcting an existing row must not advance the MT5 cursor.
_LATEST_SOURCE_TIMES_THIS_PROCESS = {}
# Per-job page sizes are assigned from the configured target before MT5 work.
LATEST_PROBE_BARS = 0
BACKFILL_BATCH_BARS = 0
# Rows per batched INSERT statement.
INSERT_BATCH_SIZE = 200
# ==============================================================================
#  TIMEZONE RESOLUTION
# ==============================================================================
def _resolve_timezone(tz_name):
    """
    Return a tzinfo object for the given IANA name, or None.
    Never falls back to Africa/Lagos — the caller handles the None case
    by notifying the owner.
    """
    if _is_nullish(tz_name):
        return None
    s = str(tz_name).strip()
    if ZoneInfo is not None:
        try:
            return ZoneInfo(s)
        except Exception:
            pass
    if pytz is not None:
        try:
            return pytz.timezone(s)
        except Exception:
            pass
    return None


_COMPUTER_TIMEZONE_CACHE = None
_COMPUTER_TIMEZONE_CACHE_AT = 0.0
_COMPUTER_TIMEZONE_CACHE_SECONDS = 300

def _detect_computer_timezone():
    """
    Detect the computer's current network/proxy timezone, then fall back to
    the computer's configured OS timezone.

    The IP timezone request uses urllib's normal proxy discovery, including
    active Python/environment or supported system proxy settings. ipapi returns
    an IANA timezone. If network geolocation is unavailable, tzlocal and the
    standard-library OS timezone sources are used instead.

    Returns (display_name, tzinfo). The result is cached briefly so a programme
    loop does not repeatedly call the geolocation service.
    """
    global _COMPUTER_TIMEZONE_CACHE, _COMPUTER_TIMEZONE_CACHE_AT

    now_monotonic = time.monotonic()
    if (
        _COMPUTER_TIMEZONE_CACHE is not None
        and now_monotonic - _COMPUTER_TIMEZONE_CACHE_AT < _COMPUTER_TIMEZONE_CACHE_SECONDS
    ):
        return _COMPUTER_TIMEZONE_CACHE

    # Resolve the outbound IP first. urllib automatically uses active proxy
    # settings, so this detects the proxy's egress region when one is configured.
    try:
        request = urllib.request.Request(
            'https://ipapi.co/timezone/',
            headers={'User-Agent': 'brokercharts/1.0'},
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            network_timezone = response.read(256).decode('utf-8', errors='replace').strip()
        network_zone = _resolve_timezone(network_timezone)
        if network_zone is not None:
            _COMPUTER_TIMEZONE_CACHE = (
                f'{network_timezone} (proxy/public IP)', network_zone
            )
            _COMPUTER_TIMEZONE_CACHE_AT = now_monotonic
            return _COMPUTER_TIMEZONE_CACHE
    except Exception as exc:
        print(f"[CHARTS][TIMEZONE] network/proxy timezone lookup unavailable: {exc}")

    candidates = []
    native_timezone_name = None

    if _tzlocal is not None:
        try:
            getter = getattr(_tzlocal, 'get_localzone_name', None)
            if callable(getter):
                name = getter()
                if name:
                    candidates.append(str(name).strip())
        except Exception:
            pass
        try:
            local_zone = _tzlocal.get_localzone()
            zone_key = getattr(local_zone, 'key', None) or getattr(local_zone, 'zone', None)
            if zone_key:
                candidates.append(str(zone_key).strip())
        except Exception:
            pass

    env_timezone = os.environ.get('TZ')
    if env_timezone:
        candidates.append(env_timezone.strip().lstrip(':'))

    if os.name == 'nt':
        try:
            import winreg
            with winreg.OpenKey(
                winreg.HKEY_LOCAL_MACHINE,
                r'SYSTEM\CurrentControlSet\Control\TimeZoneInformation'
            ) as key:
                windows_name = winreg.QueryValueEx(key, 'TimeZoneKeyName')[0]
            if windows_name:
                native_timezone_name = str(windows_name).strip()
                candidates.append(native_timezone_name)
        except Exception:
            pass
    else:
        try:
            with open('/etc/timezone', 'r', encoding='utf-8') as handle:
                file_timezone = handle.read().strip()
            if file_timezone:
                candidates.append(file_timezone)
        except Exception:
            pass
        try:
            localtime_path = os.path.realpath('/etc/localtime')
            marker = os.sep + 'zoneinfo' + os.sep
            if marker in localtime_path:
                candidates.append(localtime_path.split(marker, 1)[1])
        except Exception:
            pass

    seen = set()
    for candidate in candidates:
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        zone = _resolve_timezone(candidate)
        if zone is not None:
            _COMPUTER_TIMEZONE_CACHE = (candidate, zone)
            _COMPUTER_TIMEZONE_CACHE_AT = now_monotonic
            return _COMPUTER_TIMEZONE_CACHE

    # A Windows native timezone name may not be an IANA identifier. The
    # conversion then uses the operating system's local timezone rules.
    if native_timezone_name:
        _COMPUTER_TIMEZONE_CACHE = (native_timezone_name, None)
    else:
        try:
            local_now = datetime.now().astimezone()
            display_name = local_now.tzname() or str(local_now.tzinfo) or 'system-local'
        except Exception:
            display_name = 'system-local'
        _COMPUTER_TIMEZONE_CACHE = (display_name, None)

    _COMPUTER_TIMEZONE_CACHE_AT = now_monotonic
    return _COMPUTER_TIMEZONE_CACHE

# ==============================================================================
#  TERMINAL PATH RESOLUTION (mirrors brokerdetails.py)
# ==============================================================================
def _sanitize_folder_component(value):
    if value is None:
        return ''
    s = str(value).strip()
    s = re.sub(r'[<>:"/\\|?*]', '_', s)
    s = s.replace(' ', '_')
    return s
def build_mt5_terminal_path_prog(server, login, user_id, programme_id):
    s   = _sanitize_folder_component(server)
    l   = _sanitize_folder_component(login)
    u   = _sanitize_folder_component(user_id)
    aid = _sanitize_folder_component(programme_id)
    folder_name  = f"{s}_{l}_U{u}_P{aid}"
    folder_abs   = os.path.join(MT5_DESTINATION_PATH, folder_name)
    terminal_exe = os.path.join(folder_abs, "terminal64.exe")
    return folder_name, folder_abs, terminal_exe
def resolve_programme_terminal_path(server, login, userid, programme_id):
    if not server or not login or not userid or not programme_id:
        return None
    _, _, terminal_exe = build_mt5_terminal_path_prog(
        server, login, userid, programme_id
    )
    if os.path.isfile(terminal_exe):
        return terminal_exe
    return None
# ==============================================================================
#  MT5 INIT / SHUTDOWN (no login attempt, short timeout)
# ==============================================================================
def mt5_available():
    return mt5 is not None
def mt5_connect_quick(terminal_path, timeout_ms=10000):
    """
    Initialize MT5 against an already-running, already-logged-in terminal.
    Never calls mt5.login(). If the terminal is not logged in, initialization
    may still succeed (MT5 returns True) but account_info() will be None —
    the caller must check that and skip.
    """
    with _MT5_IPC_LOCK:
        if not mt5_available():
            return False
        if not terminal_path:
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
def mt5_account_logged_in():
    with _MT5_IPC_LOCK:
        if not mt5_available():
            return False, None
        try:
            acc = mt5.account_info()
        except Exception:
            return False, None
        if acc is None:
            return False, None
        return True, acc
def mt5_terminal_info():
    """Return terminal settings, including the configured maxbars value."""
    with _MT5_IPC_LOCK:
        if not mt5_available():
            return None
        try:
            return mt5.terminal_info()
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
def mt5_copy_rates_from_pos(symbol, timeframe_const, start_pos, count):
    with _MT5_IPC_LOCK:
        if not mt5_available():
            return None
        try:
            return mt5.copy_rates_from_pos(symbol, timeframe_const, start_pos, count)
        except Exception:
            return None


def mt5_copy_rate_at_epoch(symbol, timeframe_const, timestamp_seconds):
    """Fetch exactly the MT5 candle whose open time is this Unix timestamp."""
    with _MT5_IPC_LOCK:
        if not mt5_available():
            return None
        try:
            date_from = datetime.fromtimestamp(
                int(timestamp_seconds), tz=dt_timezone.utc
            )
            return mt5.copy_rates_from(symbol, timeframe_const, date_from, 1)
        except Exception as exc:
            print(
                f"      [CHARTS][LATEST CHECK] exact-time MT5 fetch failed "
                f"for {symbol} at {timestamp_seconds}: {exc}"
            )
            return None

# A request is retried at the same MT5 position before its size is reduced.
# A persistent API error at a one-candle request is retried without advancing
# the position, so a failed page cannot be mistaken for completed history.
MT5_FETCH_RETRIES_PER_SIZE = 3
MT5_FETCH_RETRY_DELAY = 0.75
MT5_FETCH_ERROR_RETRY_DELAY = 5.0

def _mt5_fetch_complete_page(symbol, timeframe_const, start_pos, requested, label):
    """
    Fetch a contiguous MT5 history window without silently skipping positions.

    Failed requests are retried at the same position and then retried with a
    smaller count. Short non-empty responses continue from the exact next MT5
    position until the requested window is full or MT5 repeatedly confirms
    that no more bars are available. Persistent API errors at a one-bar request
    keep retrying the same position; the cursor never moves on an API failure.

    Returns (rates_as_list, request_failed, history_exhausted).
    """
    cursor = max(0, int(start_pos))
    remaining = max(0, int(requested))
    collected = []
    if remaining <= 0:
        return collected, False, False

    while remaining > 0:
        chunk_size = remaining
        while True:
            nonempty_rates = None
            empty_responses = 0

            for attempt in range(1, MT5_FETCH_RETRIES_PER_SIZE + 1):
                rates = mt5_copy_rates_from_pos(
                    symbol, timeframe_const, cursor, chunk_size
                )
                if rates is None:
                    try:
                        mt5_error = mt5.last_error() if mt5 is not None else 'MT5 unavailable'
                    except Exception:
                        mt5_error = 'unknown MT5 error'
                    print(
                        f"      [CHARTS][FETCH RETRY] {label}: request at "
                        f"start_pos={cursor}, count={chunk_size} failed "
                        f"({attempt}/{MT5_FETCH_RETRIES_PER_SIZE}); {mt5_error}"
                    )
                else:
                    try:
                        rate_count = len(rates)
                    except Exception:
                        rate_count = -1
                    if rate_count > 0:
                        nonempty_rates = rates
                        break
                    if rate_count == 0:
                        empty_responses += 1

                if attempt < MT5_FETCH_RETRIES_PER_SIZE:
                    time.sleep(MT5_FETCH_RETRY_DELAY)

            if nonempty_rates is not None:
                take = min(len(nonempty_rates), remaining, chunk_size)
                collected.extend(list(nonempty_rates[:take]))
                cursor += take
                remaining -= take
                break

            if empty_responses == MT5_FETCH_RETRIES_PER_SIZE:
                # Try smaller page sizes before accepting that the source has
                # no bar at this position.
                if chunk_size > 1:
                    smaller = max(1, chunk_size // 2)
                    print(
                        f"      [CHARTS][FETCH REDUCE] {label}: no bars returned "
                        f"at start_pos={cursor}; reducing request {chunk_size} "
                        f"-> {smaller} and retrying"
                    )
                    chunk_size = smaller
                    continue
                print(
                    f"      [CHARTS][FETCH END] {label}: MT5 confirmed no "
                    f"bar at start_pos={cursor} after retries"
                )
                return collected, False, True

            # An API error (None/invalid response) does not move the cursor.
            # Retry with smaller pages; at one bar, continue retrying that same
            # position instead of skipping the candle or the timeframe.
            if chunk_size > 1:
                smaller = max(1, chunk_size // 2)
                print(
                    f"      [CHARTS][FETCH REDUCE] {label}: MT5 request failed "
                    f"at start_pos={cursor}; reducing request {chunk_size} "
                    f"-> {smaller} and retrying"
                )
                chunk_size = smaller
                continue

            print(
                f"      [CHARTS][FETCH WAIT] {label}: one-candle request at "
                f"start_pos={cursor} is still failing; retrying same position"
            )
            time.sleep(MT5_FETCH_ERROR_RETRY_DELAY)

    return collected, False, False

# ==============================================================================
#  PROGRAMME FETCH
# ==============================================================================
def _fetch_candidate_programmes():
    """
    Programme accounts only. We only ever touch programme rows whose broker
    login is 'valid_credentials' and that have a broker/server/login set.
    Status 'valid_credentials' is the gate that tells us the terminal for
    this programme is expected to already be running and logged in.
    """
    sql = """
        SELECT id, userid, program_name,
               broker, server, login, broker_password,
               broker_login_status, demo_account,
               broker_symbols, broker_timeframes,
               selected_symbols, selected_timeframes,
               timezone
          FROM programme
         WHERE broker_login_status = 'valid_credentials'
           AND broker IS NOT NULL
           AND server IS NOT NULL AND server <> ''
           AND login  IS NOT NULL AND login  <> ''
           AND broker_password IS NOT NULL AND broker_password <> ''
         ORDER BY id ASC
    """
    response = db_query(sql, None, timeout=20.0, quiet=True)
    if not _is_ok(response):
        print(
            "[CHARTS][CANDIDATE QUERY ERROR] Could not read programme "
            f"candidates: {(response or {}).get('message', 'unknown bridge/database error')}"
        , flush=True)
        return None
    return [
        _normalize_row(row)
        for row in (response.get('results') or [])
        if _normalize_row(row) is not None
    ]
def _resolve_owner_email(userid):
    uid = _coerce_int(userid)
    if uid is None or uid <= 0:
        return ''
    row = db_fetch_one(
        "SELECT email FROM harvhub WHERE id = %s LIMIT 1",
        [uid], timeout=10.0, quiet=True,
    )
    if not row:
        return ''
    val = _raw_first_key(row, 'email')
    if _is_nullish(val):
        return ''
    return str(val).strip().lower()
# ==============================================================================
#  CONFIGURATION PARSING
# ==============================================================================
def _parse_csv_list(raw):
    if raw is None:
        return []
    s = str(raw).strip()
    if not s or s.lower() in ('null', 'none'):
        return []
    if s.startswith('[') and s.endswith(']'):
        try:
            arr = json.loads(s)
            if isinstance(arr, list):
                return [str(x).strip() for x in arr if str(x).strip()]
        except Exception:
            pass
    parts = [p.strip().strip('"').strip("'") for p in s.split(',')]
    return [p for p in parts if p]
def _parse_json_list(raw):
    if raw is None:
        return []
    s = str(raw).strip()
    if not s or s.lower() in ('null', 'none'):
        return []
    if s.startswith('['):
        try:
            arr = json.loads(s)
            if isinstance(arr, list):
                return [str(x).strip() for x in arr if str(x).strip()]
        except Exception:
            pass
    return _parse_csv_list(s)
# ==============================================================================
#  CANDLE STORAGE HELPERS
# ==============================================================================
def _parse_db_datetime(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    s = str(value).strip()
    if not s or s.lower() in ('null', 'none'):
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None
def _count_stored_rows(programme_id, userid, symbol, timeframe):
    """
    How many candle rows are already stored for this pair.
    Used to work out which rotation round we're in.
    """
    row = db_fetch_one(
        """
        SELECT COUNT(*) AS c
          FROM candle_records
         WHERE programmeid = %s
           AND userid = %s
           AND symbol = %s
           AND timeframe = %s
        """,
        [programme_id, userid, symbol, timeframe],
        timeout=15.0, quiet=True,
    )
    if not row:
        return 0
    v = _raw_first_key(row, 'c')
    return _coerce_int(v) or 0
def _trim_oldest_rows(programme_id, userid, symbol, timeframe,
                      keep_limit=None):
    """
    Non-destructive retention check using the current server-configured target.
    Existing candle rows are never purged to enforce the target. The stored
    history may grow as new live candles arrive. Returns zero because no rows
    are deleted here.
    """
    if keep_limit is None:
        keep_limit = MAX_BARS_PER_TF
    if keep_limit is None or int(keep_limit) < 1:
        return None
    current_count = _count_stored_rows(programme_id, userid, symbol, timeframe)
    if current_count > int(keep_limit):
        print(
            f"      [CHARTS][RETENTION] {symbol} {timeframe}: "
            f"{current_count} rows stored (backfill target {int(keep_limit)}); "
            "all existing rows preserved"
        )
    return 0

def _existing_times_in_window(programme_id, userid, symbol, timeframe,
                              start_dt, end_dt):
    """
    Return a set of candle_time datetimes already stored in the window
    [start_dt, end_dt]. Used to avoid inserting duplicates.
    """
    rows = db_fetch_all(
        """
        SELECT candle_time
          FROM candle_records
         WHERE programmeid = %s
           AND userid = %s
           AND symbol = %s
           AND timeframe = %s
           AND candle_time BETWEEN %s AND %s
        """,
        [programme_id, userid, symbol, timeframe,
         start_dt.strftime('%Y-%m-%d %H:%M:%S'),
         end_dt.strftime('%Y-%m-%d %H:%M:%S')],
        timeout=15.0, quiet=True,
    )
    out = set()
    for r in rows:
        v = _raw_first_key(r, 'candle_time')
        dt = _parse_db_datetime(v)
        if dt is not None:
            out.add(dt)
    return out
def _insert_candle_rows_batched(rows):
    """
    rows: list of dicts with the candle_records columns we write.
    Returns (number of rows actually inserted, whether any batch failed).
    """
    if not rows:
        return 0, False
    cols = [
        'programmeid', 'userid', 'symbol', 'timeframe',
        'candle_time', 'open_time', 'close_time', 'timezone',
        'last_recorded_candle_time',
        'open', 'high', 'low', 'close',
        'candle_center', 'body_center',
        'high_wick_center', 'low_wick_center',
        'candle_width_center', 'volume',
    ]
    cols_sql = ', '.join(f"`{c}`" for c in cols)
    inserted_total = 0
    failed = False
    for i in range(0, len(rows), INSERT_BATCH_SIZE):
        chunk = rows[i:i + INSERT_BATCH_SIZE]
        value_groups = []
        for r in chunk:
            vals = [
                _sql_int(r.get('programmeid')),
                _sql_int(r.get('userid')),
                _sql_quote(r.get('symbol')),
                _sql_quote(r.get('timeframe')),
                _sql_dt(r.get('candle_time')),
                _sql_dt(r.get('open_time')),
                _sql_dt(r.get('close_time')),
                _sql_quote(r.get('timezone')),
                _sql_dt(r.get('last_recorded_candle_time')),
                _sql_quote(r.get('open')),
                _sql_quote(r.get('high')),
                _sql_quote(r.get('low')),
                _sql_quote(r.get('close')),
                _sql_quote(r.get('candle_center')),
                _sql_quote(r.get('body_center')),
                _sql_quote(r.get('high_wick_center')),
                _sql_quote(r.get('low_wick_center')),
                _sql_quote(r.get('candle_width_center')),
                _sql_quote(r.get('volume')),
            ]
            value_groups.append("(" + ", ".join(vals) + ")")
        sql = (f"INSERT INTO candle_records ({cols_sql}) "
               f"VALUES {', '.join(value_groups)}")
        ok, affected, msg = db_execute(sql, None, timeout=90.0, quiet=True)
        if ok:
            inserted_total += len(chunk)
        else:
            failed = True
            print(f"      [CHARTS][INSERT FAIL] {msg}")
    return inserted_total, failed


def _fetch_existing_candle_rows_in_window(programme_id, userid, symbol,
                                         timeframe, start_dt, end_dt):
    """Fetch stored OHLCV values for exact source-vs-database comparison."""
    return db_fetch_all(
        """SELECT id, candle_time, open, high, low, close, volume
             FROM candle_records
            WHERE programmeid = %s AND userid = %s
              AND symbol = %s AND timeframe = %s
              AND candle_time BETWEEN %s AND %s
            ORDER BY candle_time ASC, id ASC""",
        [programme_id, userid, symbol, timeframe,
         start_dt.strftime('%Y-%m-%d %H:%M:%S'),
         end_dt.strftime('%Y-%m-%d %H:%M:%S')],
        timeout=90.0, quiet=True,
    )


def _stored_ohlcv_matches_source(stored, source_row):
    """Compare stored OHLCV with the rounded values supplied by MT5."""
    for field in ('open', 'high', 'low', 'close', 'volume'):
        try:
            actual = float(_raw_first_key(stored, field))
            expected = float(source_row[field])
        except (TypeError, ValueError, OverflowError):
            return False
        if not math.isfinite(actual) or not math.isfinite(expected):
            return False
        # candle_records is written at eight decimal places. Compare at that
        # same precision to avoid needless replacements caused by DB formatting.
        if round(actual, 8) != round(expected, 8):
            return False
    return True


def _reconcile_candle_rows_batched(programme_id, userid, symbol, timeframe,
                                   rows, insert_missing=True):
    """
    Preserve accurate candle records and replace only incorrect snapshots.

    For every valid MT5 source row:
      - if a stored OHLCV row matches, leave it untouched;
      - if stored rows exist but their OHLCV differs, delete only those
        incorrect row IDs and insert the complete MT5 replacement;
      - if the timestamp is not stored, insert it when insert_missing=True.

    A correct duplicate at a timestamp is preserved. Incorrect duplicate rows
    at that same timestamp may be removed because a correct source-matching row
    already exists. Returns (rows_inserted, failed, timestamps_replaced,
    timestamps_already_correct, timestamps_new).
    """
    if not rows:
        return 0, False, 0, 0, 0

    unique_rows = {}
    for row in rows:
        candle_time = _parse_db_datetime(row.get('candle_time'))
        if candle_time is not None:
            unique_rows[candle_time] = row
    if not unique_rows:
        return 0, False, 0, 0, 0

    ordered_rows = [unique_rows[key] for key in sorted(unique_rows)]
    window_start = ordered_rows[0]['candle_time']
    window_end = ordered_rows[-1]['candle_time']
    stored_rows = _fetch_existing_candle_rows_in_window(
        programme_id, userid, symbol, timeframe, window_start, window_end,
    )

    stored_by_time = {}
    for stored in stored_rows:
        candle_time = _parse_db_datetime(_raw_first_key(stored, 'candle_time'))
        if candle_time is not None:
            stored_by_time.setdefault(candle_time, []).append(stored)

    delete_ids = []
    delete_id_to_time = {}
    rows_to_insert = {}
    replaced_count = 0
    correct_count = 0
    new_count = 0

    for source_row in ordered_rows:
        candle_time = source_row['candle_time']
        existing_at_time = stored_by_time.get(candle_time, [])
        if not existing_at_time:
            if insert_missing:
                rows_to_insert[candle_time] = source_row
                new_count += 1
            continue

        matching_rows = [
            stored for stored in existing_at_time
            if _stored_ohlcv_matches_source(stored, source_row)
        ]
        mismatching_rows = [
            stored for stored in existing_at_time
            if not _stored_ohlcv_matches_source(stored, source_row)
        ]

        if matching_rows:
            # Preserve the good record. If bad duplicates exist at the same
            # timestamp, remove only those inaccurate rows; no replacement is
            # needed because an accurate copy already remains stored.
            correct_count += 1
            for stored in mismatching_rows:
                row_id = _coerce_int(_raw_first_key(stored, 'id'))
                if row_id is not None and row_id > 0:
                    delete_ids.append(row_id)
                    delete_id_to_time[row_id] = candle_time
            continue

        # No stored version matches MT5: replace all records for this timestamp.
        timestamp_ids = []
        for stored in existing_at_time:
            row_id = _coerce_int(_raw_first_key(stored, 'id'))
            if row_id is not None and row_id > 0:
                timestamp_ids.append(row_id)
                delete_ids.append(row_id)
                delete_id_to_time[row_id] = candle_time
        if timestamp_ids:
            rows_to_insert[candle_time] = source_row
            replaced_count += 1
        else:
            # Without a database row ID we cannot safely delete the stale row.
            print(
                f"      [CHARTS][REPAIR FAIL] {symbol} {timeframe}: "
                f"stored candle {candle_time} differs from MT5 but has no usable id"
            )

    failed = False
    failed_delete_times = set()
    if delete_ids:
        for offset in range(0, len(delete_ids), 400):
            batch_ids = delete_ids[offset:offset + 400]
            placeholders = ', '.join(['%s'] * len(batch_ids))
            sql = (
                "DELETE FROM candle_records WHERE programmeid = %s AND userid = %s "
                "AND symbol = %s AND timeframe = %s AND id IN (" + placeholders + ")"
            )
            ok, affected, message = db_execute(
                sql, [programme_id, userid, symbol, timeframe] + batch_ids,
                timeout=90.0, quiet=True,
            )
            if not ok:
                failed = True
                failed_delete_times.update(
                    delete_id_to_time[row_id]
                    for row_id in batch_ids
                    if row_id in delete_id_to_time
                )
                print(
                    f"      [CHARTS][REPAIR DELETE FAIL] {symbol} {timeframe}: "
                    f"could not remove incorrect OHLC row(s): {message}"
                )

    # Never insert a replacement if its old inaccurate row could not be deleted.
    for candle_time in failed_delete_times:
        rows_to_insert.pop(candle_time, None)

    # Confirm replacement timestamps are actually absent before inserting.
    replacement_times = [
        candle_time for candle_time in rows_to_insert
        if candle_time in stored_by_time
    ]
    if replacement_times:
        check_rows = db_fetch_all(
            """SELECT candle_time FROM candle_records
                WHERE programmeid = %s AND userid = %s
                  AND symbol = %s AND timeframe = %s
                  AND candle_time BETWEEN %s AND %s""",
            [programme_id, userid, symbol, timeframe,
             min(replacement_times).strftime('%Y-%m-%d %H:%M:%S'),
             max(replacement_times).strftime('%Y-%m-%d %H:%M:%S')],
            timeout=90.0, quiet=True,
        )
        still_present = {
            _parse_db_datetime(_raw_first_key(item, 'candle_time'))
            for item in check_rows
        }
        for candle_time in replacement_times:
            if candle_time in still_present:
                rows_to_insert.pop(candle_time, None)
                failed = True
                print(
                    f"      [CHARTS][REPAIR FAIL] {symbol} {timeframe}: "
                    f"incorrect stored candle {candle_time} remains; "
                    "replacement was not inserted to avoid a duplicate"
                )

    insert_rows = [rows_to_insert[key] for key in sorted(rows_to_insert)]
    inserted = 0
    if insert_rows:
        inserted, insert_failed = _insert_candle_rows_batched(insert_rows)
        failed = failed or insert_failed

        # Verify inserts/replacements and retry only timestamps still absent.
        verify_times = [row['candle_time'] for row in insert_rows]
        verify_existing = _existing_times_in_window(
            programme_id, userid, symbol, timeframe,
            min(verify_times), max(verify_times),
        )
        missing = [row for row in insert_rows
                   if row['candle_time'] not in verify_existing]
        if missing:
            retry_inserted, retry_failed = _insert_candle_rows_batched(missing)
            inserted += retry_inserted
            failed = failed or retry_failed
            final_existing = _existing_times_in_window(
                programme_id, userid, symbol, timeframe,
                min(row['candle_time'] for row in missing),
                max(row['candle_time'] for row in missing),
            )
            still_missing = [row for row in missing
                             if row['candle_time'] not in final_existing]
            if still_missing:
                failed = True
                print(
                    f"      [CHARTS][REPAIR FAIL] {symbol} {timeframe}: "
                    f"{len(still_missing)} corrected/new candle row(s) remain absent"
                )

    return inserted, failed, replaced_count, correct_count, new_count


def _cleanup_broken_candle_rows(programme_id, userid, symbol, timeframe, tz_name=None):
    """
    Non-destructively audit stored candle rows.

    Older versions deleted malformed rows, duplicates, or candles whose open
    was outside the previous candle range. This routine no longer deletes
    stored candles. The MT5 reconciliation pass below replaces an existing row
    only after it can compare that timestamp with a valid authoritative source
    OHLCV record. Genuine market gaps are preserved.

    Returns zero because this audit never removes candle records.
    """
    rows = db_fetch_all(
        """SELECT id, candle_time, open_time, close_time,
                  open, high, low, close, volume
             FROM candle_records
            WHERE programmeid = %s AND userid = %s
              AND symbol = %s AND timeframe = %s
            ORDER BY candle_time ASC, id ASC""",
        [programme_id, userid, symbol, timeframe],
        timeout=90.0, quiet=True,
    )
    invalid_count = 0
    for stored in rows:
        try:
            candle_time = _parse_db_datetime(_raw_first_key(stored, 'candle_time'))
            open_time = _parse_db_datetime(_raw_first_key(stored, 'open_time'))
            close_time = _parse_db_datetime(_raw_first_key(stored, 'close_time'))
            values = [
                float(_raw_first_key(stored, field))
                for field in ('open', 'high', 'low', 'close', 'volume')
            ]
            o, h, l, c, volume = values
            valid = (
                candle_time is not None and open_time is not None and
                close_time is not None and
                abs((open_time - candle_time).total_seconds()) <= 1 and
                close_time > open_time and
                all(math.isfinite(value) for value in values) and
                h >= max(o, c) and l <= min(o, c) and h >= l and volume >= 0
            )
        except (TypeError, ValueError, OverflowError):
            valid = False
        if not valid:
            invalid_count += 1

    if invalid_count:
        print(
            f"      [CHARTS][AUDIT] {symbol} {timeframe}: found "
            f"{invalid_count} malformed stored candle row(s); preserving them "
            "until a valid MT5 source row is available for correction"
        )
    else:
        print(
            f"      [CHARTS][AUDIT] {symbol} {timeframe}: "
            f"checked {len(rows)} stored row(s); no malformed OHLC rows found"
        )
    return 0

# ==============================================================================
#  CANDLE BUILD (MT5 rates -> candle_records rows)
# ==============================================================================
def _mt5_epoch_to_programme_time(timestamp_seconds, programme_tz,
                                  computer_tz=None):
    """
    Convert an MT5 Unix timestamp using the computer's configured timezone
    first, then the programme's selected timezone. The timestamp is an absolute
    Unix instant; fromtimestamp applies the computer's timezone and its historic
    daylight-saving rules before astimezone converts that instant to programme_tz.

    If no IANA computer timezone can be identified (for example, a Windows
    timezone registry name without tzlocal installed), the OS local timezone
    engine is used directly. Returns an aware datetime in programme_tz.
    """
    if programme_tz is None:
        raise ValueError("A valid programme timezone is required")
    timestamp_seconds = int(timestamp_seconds)
    if computer_tz is not None:
        computer_local_dt = datetime.fromtimestamp(
            timestamp_seconds, tz=computer_tz
        )
    else:
        computer_local_dt = datetime.fromtimestamp(
            timestamp_seconds
        ).astimezone()
    return computer_local_dt.astimezone(programme_tz)

def _rates_to_rows(programme_id, userid, symbol, timeframe_key,
                   tz_obj, tz_name, rates, computer_tz=None):
    """
    Convert MT5 rates into candle_records rows. Each Unix timestamp is first
    localized to the computer's configured timezone and then converted to the
    programme's selected IANA timezone. Candle open and close instants are
    converted independently for correct daylight-saving handling. MySQL
    DATETIME values remain naive programme-local values, with the target IANA
    timezone stored in the existing timezone column.
    """
    if rates is None or len(rates) == 0:
        return []
    if tz_obj is None:
        raise ValueError(
            f"Programme timezone {tz_name!r} was not resolved; refusing to "
            "store candle timestamps in a different timezone."
        )
    tf_sec = TIMEFRAME_SECONDS.get(timeframe_key, 60)
    rows = []
    for r in rates:
        try:
            ts_unix = int(r['time'])
        except Exception as exc:
            print(
                f"      [CHARTS][TIMEZONE] {symbol} {timeframe_key}: "
                f"invalid MT5 candle timestamp {r!r}: {exc}"
            )
            continue
        try:
            # Convert both Unix instants independently through computer local
            # time into programme local time, preserving DST transitions.
            open_local_aware = _mt5_epoch_to_programme_time(
                ts_unix, tz_obj, computer_tz
            )
            close_local_aware = _mt5_epoch_to_programme_time(
                ts_unix + tf_sec, tz_obj, computer_tz
            )
            # MySQL DATETIME has no timezone metadata. Keep the converted
            # programme wall-clock values and retain the IANA name separately.
            open_dt = open_local_aware.replace(tzinfo=None)
            close_dt = close_local_aware.replace(tzinfo=None)

            # Keep the currently forming candle in the source rows. _latest_pass
            # re-fetches the newest stored timestamp and replaces it only when
            # its stored OHLCV does not match the latest MT5 snapshot.
        except Exception as exc:
            # Never silently fall back to UTC and label it as another zone.
            print(
                f"      [CHARTS][TIMEZONE] {symbol} {timeframe_key}: "
                f"could not convert timestamp {ts_unix} to {tz_name!r}: {exc}; "
                "candle skipped."
            )
            continue
        try:
            o = float(r['open'])
            h = float(r['high'])
            l = float(r['low'])
            c = float(r['close'])
            v = float(r['tick_volume'])
        except Exception as exc:
            print(
                f"      [CHARTS] {symbol} {timeframe_key}: invalid OHLCV row "
                f"at timestamp {ts_unix}: {exc}"
            )
            continue

        # Never write malformed source candles. A valid OHLC bar must contain
        # both body prices inside its high/low range, and all values must be
        # finite. This prevents corrupt MT5/API values from reaching the chart.
        if not all(math.isfinite(value) for value in (o, h, l, c, v)):
            print(
                f"      [CHARTS][INVALID OHLC] {symbol} {timeframe_key}: "
                f"non-finite value at timestamp {ts_unix}; candle rejected"
            )
            continue
        if h < max(o, c) or l > min(o, c) or h < l or v < 0:
            print(
                f"      [CHARTS][INVALID OHLC] {symbol} {timeframe_key}: "
                f"O={o} H={h} L={l} C={c} V={v} at timestamp {ts_unix}; "
                "candle rejected"
            )
            continue

        # Retain the existing database precision, then validate again because
        # rounding must never invert the candle's high/low relationship.
        o, h, l, c, v = (round(value, 8) for value in (o, h, l, c, v))
        if h < max(o, c) or l > min(o, c) or h < l:
            print(
                f"      [CHARTS][INVALID OHLC] {symbol} {timeframe_key}: "
                f"precision-normalized candle is inconsistent at {ts_unix}; rejected"
            )
            continue

        upper_body = max(o, c)
        lower_body = min(o, c)
        rows.append({
            'programmeid':               programme_id,
            'userid':                    userid,
            'symbol':                    symbol,
            'timeframe':                 timeframe_key,
            'candle_time':               open_dt,
            'open_time':                 open_dt,
            'close_time':                close_dt,
            'timezone':                  tz_name,
            'last_recorded_candle_time': open_dt,
            'open':                      o,
            'high':                      h,
            'low':                       l,
            'close':                     c,
            'candle_center':             round((h + l) / 2.0, 8),
            'body_center':               round((o + c) / 2.0, 8),
            'high_wick_center':          round((h + upper_body) / 2.0, 8),
            'low_wick_center':           round((l + lower_body) / 2.0, 8),
            'candle_width_center':       round((h - l) / 2.0, 8),
            'volume':                    v,
        })
    return rows

def _reject_interbar_gap_rows(rows, timeframe_key, label):
    """
    Preserve authoritative MT5 candles even when a candle opens outside the
    preceding candle's range. Such a move may be a genuine broker/market gap,
    not corrupt OHLC. Incorrect stored values are repaired by comparing the
    full OHLCV tuple with MT5 in _reconcile_candle_rows_batched().

    The function name remains for compatibility with existing call sites.
    """
    return sorted(rows or [], key=lambda item: item['candle_time'])

# ==============================================================================
#  ROTATION-ROUND HELPERS
# ==============================================================================
def _round_target_from_filled(filled):
    """
    Return the target bar count for the current round, based on how many
    rows are already stored. Self-healing across process restarts.
    """
    if MAX_BARS_PER_TF < 1:
        return 0
    for target in ROUND_TARGETS:
        if filled < target:
            return target
    return MAX_BARS_PER_TF


def _repair_existing_history_once(programme_id, userid, symbol, timeframe_key,
                                  tf_const, tz_obj, tz_name, computer_tz=None):
    """
    Reconcile existing database rows against the newest configured MT5 bars.

    Older collector versions permanently kept the first snapshot of a candle
    because an existing timestamp was treated as a completed row. A single
    startup reconciliation corrects those stale-but-plausible OHLC values
    across the whole retained window. It does not add missing history here;
    normal latest/backfill rotation still owns inserts and the storage cap.
    """
    pair_key = (
        int(programme_id), int(userid), str(symbol).upper(),
        str(timeframe_key).lower(), int(MAX_BARS_PER_TF),
    )
    if pair_key in _REPAIRED_PAIRS_THIS_PROCESS:
        return True

    rates, fetch_failed, history_exhausted = _mt5_fetch_complete_page(
        symbol, tf_const, 0, MAX_BARS_PER_TF,
        f"{symbol} {timeframe_key} existing-history repair",
    )
    if fetch_failed:
        print(
            f"      [CHARTS][REPAIR] {symbol} {timeframe_key}: MT5 history "
            "fetch failed; repair will retry on a later cycle"
        )
        return False
    if not rates:
        print(
            f"      [CHARTS][REPAIR] {symbol} {timeframe_key}: no MT5 history "
            "available; repair not marked complete"
        )
        return False

    rows = _rates_to_rows(
        programme_id, userid, symbol, timeframe_key, tz_obj, tz_name, rates,
        computer_tz=computer_tz,
    )
    rows = _reject_interbar_gap_rows(
        rows, timeframe_key, f"{symbol} {timeframe_key} existing-history repair"
    )
    rows.sort(key=lambda row: row['candle_time'])
    if not rows:
        print(
            f"      [CHARTS][REPAIR] {symbol} {timeframe_key}: source history "
            "contained no valid OHLC rows; repair not marked complete"
        )
        return False

    inserted, repair_failed, replaced, correct, _new = _reconcile_candle_rows_batched(
        programme_id, userid, symbol, timeframe_key, rows,
        insert_missing=False,
    )
    if repair_failed:
        print(
            f"      [CHARTS][REPAIR] {symbol} {timeframe_key}: database "
            "reconciliation failed; repair will retry on a later cycle"
        )
        return False

    _REPAIRED_PAIRS_THIS_PROCESS.add(pair_key)
    print(
        f"      [CHARTS][REPAIR] {symbol} {timeframe_key}: checked "
        f"{len(rows)} MT5 source bars; corrected={replaced}, "
        f"already_correct={correct}, replacement_rows_inserted={inserted}; "
        f"history_exhausted={history_exhausted}"
    )
    return True


def _latest_pass(programme_id, userid, symbol, timeframe_key,
                 tf_const, tz_obj, tz_name, computer_tz=None):
    """
    Synchronize the newest MT5 page without routinely deleting stored candles.

    Existing OHLCV is compared with MT5 at the same candle_time. Matching rows
    are left untouched. Only a mismatching stored row is deleted and replaced
    with the complete valid MT5 source row. Missing timestamps are inserted.

    Returns (source_bars_fetched, rows_inserted_or_replaced, failed,
             newly_appeared_source_bars).
    """
    rates, fetch_failed, history_exhausted = _mt5_fetch_complete_page(
        symbol, tf_const, 0, LATEST_PROBE_BARS,
        f"{symbol} {timeframe_key} latest",
    )
    if fetch_failed:
        return 0, 0, True, 0
    if not rates:
        print(f"      [CHARTS][LATEST] {symbol} {timeframe_key}: "
              "MT5 returned no latest bars after retries")
        return 0, 0, False, 0

    fetched = len(rates)
    pair_key = (
        int(programme_id), int(userid), str(symbol).upper(),
        str(timeframe_key).lower(),
    )

    # Record real source-position movement independently of database changes.
    source_epoch_by_time = {}
    for rate in rates:
        try:
            epoch = int(rate['time'])
            source_time = _mt5_epoch_to_programme_time(
                epoch, tz_obj, computer_tz
            ).replace(tzinfo=None)
            source_epoch_by_time[source_time] = epoch
        except Exception:
            continue

    source_times = sorted(source_epoch_by_time)
    previous_latest_source_time = _LATEST_SOURCE_TIMES_THIS_PROCESS.get(pair_key)
    newly_appeared_source_bars = 0
    if previous_latest_source_time is not None:
        newly_appeared_source_bars = sum(
            1 for source_time in source_times
            if source_time > previous_latest_source_time
        )
    if source_times:
        newest_source_time = source_times[-1]
        if (previous_latest_source_time is None or
                newest_source_time > previous_latest_source_time):
            _LATEST_SOURCE_TIMES_THIS_PROCESS[pair_key] = newest_source_time

    latest_stored_row = db_fetch_one(
        """SELECT id, candle_time
             FROM candle_records
            WHERE programmeid = %s AND userid = %s
              AND symbol = %s AND timeframe = %s
            ORDER BY candle_time DESC, id DESC
            LIMIT 1""",
        [programme_id, userid, symbol, timeframe_key],
        timeout=20.0, quiet=True,
    )
    latest_stored_time = _parse_db_datetime(
        _raw_first_key(latest_stored_row, 'candle_time')
    ) if latest_stored_row else None

    rows = _rates_to_rows(
        programme_id, userid, symbol, timeframe_key, tz_obj, tz_name, rates,
        computer_tz=computer_tz,
    )

    # Re-fetch the exact newest stored timestamp to get the freshest forming-
    # candle snapshot. This does not delete anything; reconciliation below
    # decides whether the stored OHLCV actually differs from this source row.
    if latest_stored_time is not None and latest_stored_time in source_epoch_by_time:
        exact_epoch = source_epoch_by_time[latest_stored_time]
        exact_rates = None
        for attempt in range(1, MT5_FETCH_RETRIES_PER_SIZE + 1):
            candidate_rates = mt5_copy_rate_at_epoch(
                symbol, tf_const, exact_epoch
            )
            if candidate_rates is not None:
                try:
                    if (len(candidate_rates) == 1 and
                            int(candidate_rates[0]['time']) == exact_epoch):
                        exact_rates = candidate_rates
                        break
                except Exception:
                    pass
            if attempt < MT5_FETCH_RETRIES_PER_SIZE:
                time.sleep(MT5_FETCH_RETRY_DELAY)

        if exact_rates is not None:
            exact_rows = _rates_to_rows(
                programme_id, userid, symbol, timeframe_key, tz_obj, tz_name,
                exact_rates, computer_tz=computer_tz,
            )
            if exact_rows and exact_rows[0]['candle_time'] == latest_stored_time:
                rows = [row for row in rows
                        if row['candle_time'] != latest_stored_time]
                rows.append(exact_rows[0])
                print(
                    f"      [CHARTS][LATEST CHECK] {symbol} {timeframe_key}: "
                    f"fetched exact stored candle {latest_stored_time}; "
                    "stored row will be replaced only if OHLCV differs"
                )
            else:
                print(
                    f"      [CHARTS][LATEST CHECK] {symbol} {timeframe_key}: "
                    f"exact MT5 candle {latest_stored_time} was malformed; "
                    "existing database row will be preserved"
                )
        else:
            print(
                f"      [CHARTS][LATEST CHECK] {symbol} {timeframe_key}: "
                f"exact MT5 re-fetch for {latest_stored_time} failed; "
                "using the latest page snapshot without deleting stored data"
            )
    elif latest_stored_time is not None:
        print(
            f"      [CHARTS][LATEST CHECK] {symbol} {timeframe_key}: stored "
            f"latest candle {latest_stored_time} is outside this MT5 probe; "
            "it will be left untouched"
        )

    rows.sort(key=lambda item: item['candle_time'])
    if not rows:
        return fetched, 0, False, newly_appeared_source_bars

    inserted, failed, replaced, correct, new_count = _reconcile_candle_rows_batched(
        programme_id, userid, symbol, timeframe_key, rows,
        insert_missing=True,
    )

    trimmed = _trim_oldest_rows(programme_id, userid, symbol, timeframe_key)
    if trimmed is None:
        failed = True

    print(
        f"      [CHARTS] {symbol} {timeframe_key}: latest page fetched={fetched} "
        f"checked={len(rows)} already_correct={correct} corrected={replaced} "
        f"new_timestamps={new_count} inserted_or_replaced={inserted}; "
        f"new_MT5_positions={newly_appeared_source_bars}"
    )
    return fetched, inserted, failed, newly_appeared_source_bars

def _backfill_pass(programme_id, userid, symbol, timeframe_key,
                   tf_const, tz_obj, tz_name, start_pos, requested,
                   computer_tz=None):
    """
    Fetch exactly the next older MT5 page and insert every candle in it.
    MT5 bar positions count backwards from the present (position zero).
    Therefore, after the latest page has been stored, historical pages must
    use an increasing start_pos. Resetting start_pos to zero would fetch the
    same newest bars again and create the overlap that previously reduced
    the inserted count.
    Returns (source_bars_fetched, rows_inserted, insert_failed).
    """
    start_pos = max(0, int(start_pos))
    requested = max(0, int(requested))
    if requested <= 0:
        return 0, 0, False
    rates, fetch_failed, history_exhausted = _mt5_fetch_complete_page(
        symbol, tf_const, start_pos, requested,
        f"{symbol} {timeframe_key} backfill",
    )
    if fetch_failed:
        return 0, 0, True
    fetched = len(rates)
    if fetched == 0:
        return 0, 0, False
    rows = _rates_to_rows(
        programme_id, userid, symbol, timeframe_key, tz_obj, tz_name, rates,
        computer_tz=computer_tz,
    )
    rows = _reject_interbar_gap_rows(
        rows, timeframe_key, f"{symbol} {timeframe_key} backfill"
    )
    if not rows:
        print(f"      [CHARTS][BACKFILL] {symbol} {timeframe_key}: "
              f"fetched={fetched} at start_pos={start_pos}, but no valid rows")
        return fetched, 0, False

    rows.sort(key=lambda x: x['candle_time'])
    inserted, failed, replaced, correct, new_count = _reconcile_candle_rows_batched(
        programme_id, userid, symbol, timeframe_key, rows,
        insert_missing=True,
    )
    trimmed = _trim_oldest_rows(programme_id, userid, symbol, timeframe_key)
    if trimmed is None:
        failed = True
    print(f"      [CHARTS][BACKFILL] {symbol} {timeframe_key}: "
          f"rotation at MT5 positions {start_pos}-"
          f"{start_pos + fetched - 1}: fetched={fetched}, "
          f"already_correct={correct}, corrected={replaced}, "
          f"new={new_count}, inserted_or_replaced={inserted}")
    if failed:
        print(f"      [CHARTS][BACKFILL] {symbol} {timeframe_key}: "
              "database reconciliation was incomplete; stopping this pair "
              "so the failed page is not silently skipped")
        return fetched, inserted, True
    return fetched, inserted, False
#  SYNC ONE (programme, symbol, timeframe) — ONE ROUND
# ==============================================================================
def _sync_one_symbol_timeframe(programme_id, userid, symbol, timeframe_key,
                               tf_const, tz_obj, tz_name, computer_tz=None):
    """
    Synchronize one programme/symbol/timeframe pair. MAX_BARS_PER_TF is the
    historical backfill target, not a reason to delete existing candles.
    Existing valid OHLC rows are preserved, inaccurate rows are replaced from
    MT5, and newly arriving candles are appended without purging older rows.
    Historical backfill stops as soon as the pair reaches the configured row target.
    """
    if not mt5_symbol_select(symbol, True):
        print(f"      [CHARTS] {symbol} {timeframe_key}: symbol_select failed")
        return {'inserted': 0, 'skipped': True}
    # Audit stored data without deleting it. MT5 comparison below is the only
    # authority allowed to replace an existing candle's OHLCV values.
    initial_cleanup = _cleanup_broken_candle_rows(
        programme_id, userid, symbol, timeframe_key, tz_name=tz_name,
    )
    if initial_cleanup is None:
        return {'inserted': 0, 'skipped': True, 'reason': 'candle_cleanup_failed'}
    # Compare retained history against broker OHLCV. Only incorrect records
    # are replaced; correct candles and legitimate opening gaps are preserved.
    _repair_existing_history_once(
        programme_id, userid, symbol, timeframe_key, tf_const,
        tz_obj, tz_name, computer_tz=computer_tz,
    )
    repair_cleanup = _cleanup_broken_candle_rows(
        programme_id, userid, symbol, timeframe_key, tz_name=tz_name,
    )
    if repair_cleanup is None:
        return {'inserted': 0, 'skipped': True, 'reason': 'post_repair_cleanup_failed'}
    initial_trim = _trim_oldest_rows(programme_id, userid, symbol, timeframe_key)
    if initial_trim is None:
        return {'inserted': 0, 'skipped': True, 'reason': 'retention_cleanup_failed'}
    latest_inserted = 0
    backfill_inserted = 0
    rotations = 0
    history_exhausted = False
    insert_failed = False
    latest_page_fetched = 0
    # ---- Rotation 1: newest bars ---------------------------------------
    latest_page_fetched, n_latest, latest_failed, _latest_source_advance = _latest_pass(
        programme_id, userid, symbol, timeframe_key, tf_const, tz_obj, tz_name,
        computer_tz=computer_tz,
    )
    latest_inserted += n_latest
    latest_cleanup = _cleanup_broken_candle_rows(
        programme_id, userid, symbol, timeframe_key, tz_name=tz_name,
    )
    if latest_cleanup is None:
        latest_failed = True
    if latest_failed:
        insert_failed = True
        print(f"      [CHARTS] {symbol} {timeframe_key}: latest insert failed; "
              "historical pagination stopped")
    filled_now = _count_stored_rows(
        programme_id, userid, symbol, timeframe_key
    )
    # The position index represents bars fetched from the present, including
    # bars already present in the DB. Use the larger value to avoid repeating
    # the latest probe when the database already contains older history.
    next_start_pos = max(int(latest_page_fetched or 0), int(filled_now or 0))
    target = _round_target_from_filled(filled_now)
    if not insert_failed and filled_now < MAX_BARS_PER_TF:
        while next_start_pos < MAX_BARS_PER_TF and filled_now < MAX_BARS_PER_TF:
            filled_now = _count_stored_rows(
                programme_id, userid, symbol, timeframe_key
            )
            if filled_now >= MAX_BARS_PER_TF:
                break
            target = _round_target_from_filled(filled_now)
            remaining_storage = MAX_BARS_PER_TF - filled_now
            remaining_positions = MAX_BARS_PER_TF - next_start_pos
            request_count = min(
                BACKFILL_BATCH_BARS,
                remaining_storage,
                remaining_positions,
            )
            if request_count <= 0:
                break
            fetched, inserted, page_failed = _backfill_pass(
                programme_id, userid, symbol, timeframe_key, tf_const,
                tz_obj, tz_name, next_start_pos, request_count,
                computer_tz=computer_tz,
            )
            if fetched <= 0:
                history_exhausted = not page_failed
                if history_exhausted:
                    print(f"      [CHARTS] {symbol} {timeframe_key}: "
                          f"MT5 has no more bars at start_pos={next_start_pos}; "
                          "all currently offered history has been processed")
                else:
                    print(f"      [CHARTS] {symbol} {timeframe_key}: "
                          "MT5 backfill request failed; stopping this pair")
                insert_failed = page_failed
                break
            rotations += 1
            backfill_inserted += inserted
            next_start_pos += fetched
            page_cleanup = _cleanup_broken_candle_rows(
                programme_id, userid, symbol, timeframe_key, tz_name=tz_name,
            )
            if page_cleanup is None:
                page_failed = True
            if page_failed:
                insert_failed = True
                break
            # Revisit the newest page after each older page. If a new candle
            # closed while backfill was running, it is inserted now and shifts
            # all older MT5 position indexes by the number of new bars.
            latest_fetched_again, latest_new, latest_failed_again, latest_source_advance = _latest_pass(
                programme_id, userid, symbol, timeframe_key,
                tf_const, tz_obj, tz_name, computer_tz=computer_tz,
            )
            latest_inserted += latest_new
            # Advance only when MT5 has genuinely exposed newer source bars.
            # OHLCV replacements do not count as new MT5 positions and must
            # never skip historical bars.
            next_start_pos += latest_source_advance
            if latest_failed_again:
                insert_failed = True
                print(f"      [CHARTS] {symbol} {timeframe_key}: latest refresh "
                      "failed during pagination; stopping this pair")
                break
            filled_now = _count_stored_rows(
                programme_id, userid, symbol, timeframe_key
            )
            if filled_now >= target:
                next_target = _round_target_from_filled(filled_now)
                if next_target > target:
                    print(f"      [CHARTS] {symbol} {timeframe_key}: "
                          f"reached rotation milestone {target}; "
                          f"continuing toward {next_target}")
            # A short MT5 page is not assumed to be the end. The next request
            # probes the next position. Only an empty response ends history.
    final_cleanup = _cleanup_broken_candle_rows(
        programme_id, userid, symbol, timeframe_key, tz_name=tz_name,
    )
    if final_cleanup is None:
        insert_failed = True
    final_trim = _trim_oldest_rows(programme_id, userid, symbol, timeframe_key)
    if final_trim is None:
        insert_failed = True
    filled_now = _count_stored_rows(
        programme_id, userid, symbol, timeframe_key
    )
    target = _round_target_from_filled(filled_now)
    saturated = filled_now >= MAX_BARS_PER_TF or next_start_pos >= MAX_BARS_PER_TF
    return {
        'inserted': latest_inserted + backfill_inserted,
        'latest': latest_inserted,
        'backfill': backfill_inserted,
        'skipped': False,
        'filled': filled_now,
        'target': target,
        'rotations': rotations,
        'history_exhausted': history_exhausted,
        'saturated': saturated,
        'insert_failed': insert_failed,
    }
#  PROGRAMME PROCESSING
# ==============================================================================
def _process_programme(programme, tf_map):
    programme_id = _coerce_int(_raw_first_key(programme, 'id'))
    userid       = _coerce_int(_raw_first_key(programme, 'userid'))
    prog_name    = (_raw_first_key(programme, 'program_name') or '').strip()
    server       = (_raw_first_key(programme, 'server') or '').strip()
    login        = _raw_first_key(programme, 'login')
    tz_name_raw  = _raw_first_key(programme, 'timezone')
    print(f"\n{'='*70}")
    print(f"[CHARTS] Programme {programme_id} '{prog_name}' | user={userid}")
    print(f"{'='*70}")
    if programme_id is None or userid is None or programme_id <= 0:
        print("[CHARTS] missing id/userid — skip")
        return {'inserted': 0, 'skipped': True}

    global MAX_BARS_PER_TF, ROUND_TARGETS, LATEST_PROBE_BARS, BACKFILL_BATCH_BARS
    try:
        chart_setting = db_fetch_one(
            "SELECT `chart_total_candles` FROM `server_account` WHERE `id` = 1 LIMIT 1",
            [], timeout=15.0, quiet=False,
        )
    except Exception as exc:
        print(f"[CHARTS] could not read server_account.chart_total_candles — skip programme {programme_id}: {exc}")
        return {'inserted': 0, 'skipped': True, 'reason': 'chart_total_candles_unavailable'}
    chart_total_candles = _coerce_int(_raw_first_key(chart_setting, 'chart_total_candles'))
    if chart_total_candles is None or chart_total_candles < 1:
        print("[CHARTS] server_account.chart_total_candles is empty or invalid — skipping before MT5; no fallback is applied")
        return {'inserted': 0, 'skipped': True, 'reason': 'chart_total_candles_unconfigured'}

    MAX_BARS_PER_TF = chart_total_candles
    ROUND_TARGETS = tuple(sorted({max(1, chart_total_candles // 2), chart_total_candles}))
    LATEST_PROBE_BARS = min(1500, chart_total_candles)
    BACKFILL_BATCH_BARS = min(1500, chart_total_candles)
    print(f"[CHARTS] Chart Total Candles = {MAX_BARS_PER_TF:,} (server_account.chart_total_candles)")
    owner_email = _resolve_owner_email(userid)
    # ---- Gate 1: selected_symbols ----
    selected_symbols = _parse_csv_list(_raw_first_key(programme, 'selected_symbols'))
    if not selected_symbols:
        print("[CHARTS] no selected_symbols — notifying owner, skipping")
        if _NOTIF_PDO is not None and owner_email:
            try:
                _notif_service.record_programme_no_selected_symbols_notification(
                    _NOTIF_PDO, programme_id, prog_name, owner_email,
                )
            except Exception as e:
                print(f"[CHARTS] notification raised: {e}")
        return {'inserted': 0, 'skipped': True, 'reason': 'no_symbols'}
    # ---- Gate 2: selected_timeframes ----
    selected_timeframes = _parse_json_list(_raw_first_key(programme, 'selected_timeframes'))
    selected_timeframes = [tf for tf in selected_timeframes if tf in tf_map]
    if not selected_timeframes:
        print("[CHARTS] no selected_timeframes — notifying owner, skipping")
        if _NOTIF_PDO is not None and owner_email:
            try:
                _notif_service.record_programme_no_selected_timeframes_notification(
                    _NOTIF_PDO, programme_id, prog_name, owner_email,
                )
            except Exception as e:
                print(f"[CHARTS] notification raised: {e}")
        return {'inserted': 0, 'skipped': True, 'reason': 'no_timeframes'}
    # ---- Gate 3: timezone ----
    tz_obj = _resolve_timezone(tz_name_raw)
    if tz_obj is None:
        print(f"[CHARTS] no resolvable timezone (raw={tz_name_raw!r}) — "
              f"notifying owner, skipping")
        if _NOTIF_PDO is not None and owner_email:
            try:
                _notif_service.record_programme_no_timezone_notification(
                    _NOTIF_PDO, programme_id, prog_name, owner_email,
                )
            except Exception as e:
                print(f"[CHARTS] notification raised: {e}")
        return {'inserted': 0, 'skipped': True, 'reason': 'no_timezone'}
    tz_name = str(tz_name_raw).strip()
    # ---- Optional: filter selected_symbols against broker_symbols ----
    broker_symbols_set = set(_parse_csv_list(_raw_first_key(programme, 'broker_symbols')))
    if broker_symbols_set:
        filtered = [s for s in selected_symbols if s in broker_symbols_set]
        if filtered:
            selected_symbols = filtered
    # ---- Terminal path ----
    terminal_path = resolve_programme_terminal_path(
        server=server, login=login, userid=userid, programme_id=programme_id,
    )
    if not terminal_path:
        print(f"[CHARTS] terminal path not found for programme "
              f"{programme_id} — skip")
        return {'inserted': 0, 'skipped': True, 'reason': 'no_terminal_path'}
    print(f"[CHARTS] terminal = {terminal_path}")
    print(f"[CHARTS] symbols  = {selected_symbols}")
    print(f"[CHARTS] timeframes = {selected_timeframes}")
    computer_timezone, computer_tz_obj = _detect_computer_timezone()
    print(f"[CHARTS] detected computer timezone = {computer_timezone}")
    print(f"[CHARTS] programme timezone = {tz_name}")
    print(f"[CHARTS] candle conversion = {computer_timezone} -> {tz_name}")
    # ---- MT5 init, no login attempt ----
    ok = mt5_connect_quick(terminal_path, timeout_ms=10000)
    if not ok:
        print(f"[CHARTS] mt5.initialize failed for programme {programme_id} "
              f"— skip")
        return {'inserted': 0, 'skipped': True, 'reason': 'mt5_init_failed'}
    logged_in, acc = mt5_account_logged_in()
    if not logged_in:
        print(f"[CHARTS] terminal not logged in for programme {programme_id} "
              f"— skip (login is handled elsewhere)")
        mt5_disconnect()
        return {'inserted': 0, 'skipped': True, 'reason': 'not_logged_in'}
    try:
        if (
                getattr(acc, 'server', None) and server and
                str(acc.server).strip().lower() != server.strip().lower()
        ):
            print(f"[CHARTS] terminal server {acc.server!r} != expected "
                  f"{server!r} — skip")
            mt5_disconnect()
            return {'inserted': 0, 'skipped': True, 'reason': 'server_mismatch'}
    except Exception:
        pass
    # MT5 exposes history only within the terminal's configured Max bars in chart.
    # Report this setting so a terminal-side history cap is immediately visible.
    terminal_info = mt5_terminal_info()
    terminal_maxbars = _coerce_int(getattr(terminal_info, 'maxbars', None)) if terminal_info else None
    if terminal_maxbars is not None:
        print(f"[CHARTS] MT5 Max bars in chart = {terminal_maxbars}")
        if terminal_maxbars < MAX_BARS_PER_TF:
            print("[CHARTS][NOTE] MT5 terminal Max bars in chart is below the "
                  f"backfill target ({MAX_BARS_PER_TF}). The collector can only "
                  "download bars that the terminal makes available. Increase "
                  "Tools > Options > Charts > Max bars in chart and restart MT5 "
                  "to expose a deeper history.")
    total_inserted = 0
    per_pair_summary = []
    try:
        # Complete every selected timeframe for the current symbol before
        # moving to the next symbol. Each pair's stored row count determines
        # how much history remains to fetch.
        for symbol in selected_symbols:
            for tf_key in selected_timeframes:
                tf_const = tf_map.get(tf_key)
                if tf_const is None:
                    continue
                try:
                    res = _sync_one_symbol_timeframe(
                        programme_id=programme_id,
                        userid=userid,
                        symbol=symbol,
                        timeframe_key=tf_key,
                        tf_const=tf_const,
                        tz_obj=tz_obj,
                        tz_name=tz_name,
                        computer_tz=computer_tz_obj,
                    )
                    n = int(res.get('inserted', 0) or 0)
                    total_inserted += n
                    per_pair_summary.append({
                        'symbol': symbol,
                        'timeframe': tf_key,
                        'inserted': n,
                        'latest': int(res.get('latest', 0) or 0),
                        'backfill': int(res.get('backfill', 0) or 0),
                        'filled': int(res.get('filled', 0) or 0),
                        'target': int(res.get('target', 0) or 0),
                        'rotations': int(res.get('rotations', 0) or 0),
                        'history_exhausted': bool(res.get('history_exhausted', False)),
                        'insert_failed': bool(res.get('insert_failed', False)),
                        'saturated': bool(res.get('saturated', False)),
                    })
                except Exception as e:
                    print(f"[CHARTS] {symbol} {tf_key} raised: {e}")
                    traceback.print_exc()
    finally:
        try:
            mt5_disconnect()
        except Exception:
            pass
    # ---- Per-pair summary ------------------------------------------------
    print(f"\n[CHARTS] programme {programme_id} pair summary:")
    for s in per_pair_summary:
        state = (
            'MAX CAP REACHED' if s['saturated'] else
            'MT5 HISTORY EXHAUSTED' if s['history_exhausted'] else
            'INSERT ERROR' if s['insert_failed'] else
            'ROTATION COMPLETE'
        )
        print(f"   {s['symbol']:<10} {s['timeframe']:<4} "
              f"filled={s['filled']}/{s['target']} "
              f"(latest +{s['latest']}, backfill +{s['backfill']}, "
              f"rotations={s['rotations']}) [{state}]")
    # Optional: fire the "chart ready" notification the first time a
    # programme is fully configured. Dedups on the DB side.
    try:
        if _NOTIF_PDO is not None and owner_email:
            _notif_service.record_programme_chart_ready_notification(
                _NOTIF_PDO, programme_id, prog_name, owner_email,
            )
    except Exception:
        pass
    print(f"[CHARTS] programme {programme_id} done | "
          f"inserted={total_inserted}")
    return {
        'inserted': total_inserted,
        'skipped': False,
        'pairs': per_pair_summary,
    }
# ==============================================================================
#  WORKER
# ==============================================================================
def process_programme_job(programme_job):
    """
    One programme per worker. Called via a process pool.
    """
    _worker_init()
    try:
        tf_map = _build_timeframe_map()
        if not tf_map:
            return {'success': False, 'error': 'mt5_timeframes_unavailable'}
        res = _process_programme(programme_job, tf_map)
        res['success'] = True
        return res
    except Exception as e:
        print(f"[CHARTS][CRITICAL] programme "
              f"{_raw_first_key(programme_job, 'id')}: {e}")
        traceback.print_exc()
        return {'success': False, 'error': str(e)}
    finally:
        try:
            if _db is not None:
                _db.release_channel()
        except Exception:
            pass
# ==============================================================================
#  ORCHESTRATOR
# ==============================================================================
def main_loop():
    run_as_loop = False
    loop_interval = 10
    max_loops = None
    args = list(sys.argv[1:])
    for index, arg in enumerate(args):
        lower = arg.lower()
        if lower.startswith('--execution-type='):
            run_as_loop = lower.split('=', 1)[1].strip() == 'loop'
        elif lower in ('--loop', '--loop=true', '--loop=yes', '--loop=1'):
            run_as_loop = True
        elif lower in ('--once', '--loop=false', '--loop=no', '--loop=0'):
            run_as_loop = False
        elif lower.startswith('--interval='):
            try:
                loop_interval = min(10, max(5, int(lower.split('=', 1)[1])))
            except ValueError:
                pass
        elif lower == '--interval' and index + 1 < len(args):
            try:
                loop_interval = min(10, max(5, int(args[index + 1])))
            except ValueError:
                pass
        elif lower.startswith('--max-loops='):
            try:
                max_loops = int(lower.split('=', 1)[1])
            except ValueError:
                pass
    loop_interval = min(10, max(5, int(loop_interval)))
    print("\n" + "=" * 60)
    print("           BROKERCHARTS — PROGRAMME OHLC COLLECTOR", flush=True)
    print("=" * 60)
    print(f"  Loop Mode: {'ENABLED' if run_as_loop else 'DISABLED'}")
    if run_as_loop:
        print(f"  Inter-loop delay: {loop_interval}s (clamped to 5–10s)")
        print(f"  Max Loops: {max_loops if max_loops else 'Infinite'}")
    print("  Candle target source: server_account.chart_total_candles (empty/invalid values skip programme jobs)")
    print("=" * 60 + "\n")
    _worker_init()
    cpu_cores = os.cpu_count() or 1
    pool_size = max(1, min(cpu_cores, 4))
    print(f"🖥️  Cores detected: {cpu_cores} | Worker count: {pool_size}")
    pool = mp.Pool(processes=pool_size)
    print(f"✅ Persistent pool ready ({pool_size} workers).\n")
    loop_count = 0
    # Retain every programme discovered as eligible during this service run.
    # A transient bridge/channel failure must never make a previously found
    # programme disappear from later collection cycles.
    known_programmes = {}
    try:
        while True:
            loop_count += 1
            if run_as_loop:
                print("\n" + "=" * 60)
                print(f"LOOP #{loop_count} STARTED", flush=True)
                print("=" * 60)
            try:
                print(f"[CHARTS] Loop #{loop_count}: querying programme candidates...", flush=True)
                fetched_programmes = _fetch_candidate_programmes()
                if fetched_programmes is None:
                    print(
                        f"[CHARTS] Loop #{loop_count}: candidate query failed; "
                        "retaining the previously discovered programme set.",
                        flush=True,
                    )
                else:
                    print(
                        f"[CHARTS] Loop #{loop_count}: database returned "
                        f"{len(fetched_programmes)} candidate programme(s).",
                        flush=True,
                    )
                    for candidate in fetched_programmes:
                        candidate_id = _coerce_int(_raw_first_key(candidate, 'id'))
                        if candidate_id is not None and candidate_id > 0:
                            known_programmes[candidate_id] = candidate

                programmes = list(known_programmes.values())
                print(
                    f"[CHARTS] Loop #{loop_count}: processing "
                    f"{len(programmes)} known programme(s) "
                    "(including programmes discovered on earlier loops).",
                    flush=True,
                )
                if not programmes:
                    print(" └─ 🔘 No candidate programmes discovered yet.", flush=True)
                    if not run_as_loop:
                        print("Single execution completed. Exiting...")
                        break
                    if max_loops and loop_count >= max_loops:
                        print(f"Maximum loops ({max_loops}) reached. Exiting...")
                        break
                    print(f"Next Broker Charts scan in {loop_interval}s.", flush=True)
                    time.sleep(loop_interval)
                    continue
                print(f"📊 Submitting {len(programmes)} programme job(s)", flush=True)
                t0 = time.time()
                async_jobs = [
                    pool.apply_async(process_programme_job, args=(p,))
                    for p in programmes
                ]
                results = []
                for aj in async_jobs:
                    try:
                        results.append(aj.get(timeout=1800))
                    except mp.TimeoutError:
                        print("⚠️ A worker timed out.")
                        results.append(None)
                    except Exception as job_err:
                        print(f"⚠️ Worker raised: {job_err}")
                        results.append(None)
                elapsed = time.time() - t0
                ok = sum(1 for r in results if r and r.get('success'))
                total = sum(int((r or {}).get('inserted', 0) or 0)
                            for r in results if r)
                print(f"--- Cycle complete in {elapsed:.2f}s | "
                      f"ok={ok}/{len(programmes)} | inserted={total} ---", flush=True)
            except Exception as e:
                print(f"Critical error in loop: {e}")
                traceback.print_exc()
            if not run_as_loop:
                print("Single execution completed. Exiting...")
                break
            if max_loops and loop_count >= max_loops:
                print(f"Maximum loops ({max_loops}) reached. Exiting...")
                break
            print(f"Next Broker Charts scan in {loop_interval}s.", flush=True)
            time.sleep(loop_interval)
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
