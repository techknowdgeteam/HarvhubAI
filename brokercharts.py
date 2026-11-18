# brokercharts.py
"""
Programme-scoped OHLC collector — DB-only, no JSON, no local files.
This version keeps a strict maximum of 5,000 candles for each programme,
symbol, and timeframe. It stores the newest candles first, backfills older
candles only until the 5,000-row limit is reached, and removes the oldest
rows whenever new candles would exceed that limit.
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
  1. Fetch the newest 1,500 MT5 bars from position 0 and insert missing rows.
  2. Fetch the next older page using copy_rates_from_pos(..., start_pos, count).
  3. Advance start_pos by the exact number of source bars fetched. Never reset
     historical backfill to position 0, because that would re-fetch the newest
     bars and create overlap.
  4. Insert every new candle in the page, refresh the latest page, and repeat.
  5. Stop when repeated empty responses confirm no more history is available
     or the pair reaches 5,000 stored candles. Failed API requests retry at the
     same position with smaller requests; a persistent one-bar error never
     advances the cursor or silently skips the candle.
Rows are scoped by programme, symbol, and timeframe. If the pair already
contains more than 5,000 rows, the oldest rows are deleted first.
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
import socket
import struct
import threading
import traceback
import multiprocessing as mp
import urllib.request
from datetime import datetime
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
        self.owner = owner
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
        if 'sql' in req and 'channel_id' not in req:
            req['channel_id'] = self.acquire_channel()
        return self._send_request(req)
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
ROUND_TARGETS = (2000, 5000)
# Strict retention ceiling for each programme/symbol/timeframe pair.
MAX_BARS_PER_TF = 5000
# First rotation: capture the newest 1,500 candles in one pass.
# Historical rotations then request the next older page using an increasing
# MT5 start_pos, so the newest page is never fetched again as backfill.
LATEST_PROBE_BARS = 1500
BACKFILL_BATCH_BARS = 1500
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
    return db_fetch_all(sql, None, timeout=20.0, quiet=True)
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
                      keep_limit=MAX_BARS_PER_TF):
    """Keep only the newest keep_limit rows for this exact programme pair.
    Returns the number of deleted rows, or None if cleanup failed.
    """
    keep_limit = max(0, int(keep_limit))
    current_count = _count_stored_rows(programme_id, userid, symbol, timeframe)
    if current_count <= keep_limit:
        return 0
    scope_params = [programme_id, userid, symbol, timeframe]
    if keep_limit == 0:
        ok, affected, message = db_execute(
            """DELETE FROM candle_records
               WHERE programmeid = %s AND userid = %s
                 AND symbol = %s AND timeframe = %s""",
            scope_params, timeout=90.0, quiet=True,
        )
        if not ok:
            print(f"      [CHARTS][RETENTION FAIL] {symbol} {timeframe}: {message}")
            return None
        return _count_stored_rows(programme_id, userid, symbol, timeframe)
    boundary_row = db_fetch_one(
        f"""SELECT candle_time
              FROM candle_records
             WHERE programmeid = %s AND userid = %s
               AND symbol = %s AND timeframe = %s
             ORDER BY candle_time DESC
             LIMIT 1 OFFSET {keep_limit - 1}""",
        scope_params, timeout=30.0, quiet=True,
    )
    boundary = _raw_first_key(boundary_row, 'candle_time') if boundary_row else None
    if _is_nullish(boundary):
        print(f"      [CHARTS][RETENTION FAIL] {symbol} {timeframe}: "
              "could not determine the retention boundary")
        return None
    delete_sql = """DELETE FROM candle_records
                     WHERE programmeid = %s AND userid = %s
                       AND symbol = %s AND timeframe = %s
                       AND candle_time < %s"""
    ok, affected, message = db_execute(
        delete_sql, scope_params + [boundary], timeout=90.0, quiet=True,
    )
    if not ok:
        print(f"      [CHARTS][RETENTION FAIL] {symbol} {timeframe}: {message}")
        return None
    remaining = _count_stored_rows(programme_id, userid, symbol, timeframe)
    if remaining > keep_limit:
        # Several rows can share the boundary timestamp. Delete that timestamp
        # too rather than allowing duplicate-time rows to break the hard cap.
        delete_sql = """DELETE FROM candle_records
                         WHERE programmeid = %s AND userid = %s
                           AND symbol = %s AND timeframe = %s
                           AND candle_time <= %s"""
        ok, affected, message = db_execute(
            delete_sql, scope_params + [boundary], timeout=90.0, quiet=True,
        )
        if not ok:
            print(f"      [CHARTS][RETENTION FAIL] {symbol} {timeframe}: {message}")
            return None
        remaining = _count_stored_rows(programme_id, userid, symbol, timeframe)
        if remaining > keep_limit:
            print(f"      [CHARTS][RETENTION FAIL] {symbol} {timeframe}: "
                  f"rows remain above cap ({remaining}/{keep_limit})")
            return None
    deleted = max(0, current_count - remaining)
    print(f"      [CHARTS][RETENTION] {symbol} {timeframe}: "
          f"deleted {deleted} oldest row(s); retained {remaining}/{keep_limit}")
    return deleted
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
            'open':                      round(o, 8),
            'high':                      round(h, 8),
            'low':                       round(l, 8),
            'close':                     round(c, 8),
            'candle_center':             round((h + l) / 2.0, 8),
            'body_center':               round((o + c) / 2.0, 8),
            'high_wick_center':          round((h + c) / 2.0, 8),
            'low_wick_center':           round((l + o) / 2.0 if c >= o else (c + l) / 2.0, 8),
            'candle_width_center':       round((h - l) / 2.0, 8),
            'volume':                    round(v, 8),
        })
    return rows
# ==============================================================================
#  ROTATION-ROUND HELPERS
# ==============================================================================
def _round_target_from_filled(filled):
    """
    Return the target bar count for the current round, based on how many
    rows are already stored. Self-healing across process restarts.
    """
    for target in ROUND_TARGETS:
        if filled < target:
            return target
    return MAX_BARS_PER_TF
def _latest_pass(programme_id, userid, symbol, timeframe_key,
                 tf_const, tz_obj, tz_name, computer_tz=None):
    """
    Capture the newest MT5 page and insert any bars not already stored.
    Returns (source_bars_fetched, rows_inserted, insert_failed).
    source_bars_fetched is important: the next historical start_pos is a
    position in the MT5 series, not the number of rows newly inserted.
    """
    rates, fetch_failed, history_exhausted = _mt5_fetch_complete_page(
        symbol, tf_const, 0, LATEST_PROBE_BARS,
        f"{symbol} {timeframe_key} latest",
    )
    if fetch_failed:
        return 0, 0, True
    if not rates:
        print(f"      [CHARTS][LATEST] {symbol} {timeframe_key}: "
              "MT5 returned no latest bars after retries")
        return 0, 0, False
    fetched = len(rates)
    rows = _rates_to_rows(
        programme_id, userid, symbol, timeframe_key, tz_obj, tz_name, rates,
        computer_tz=computer_tz,
    )
    if not rows:
        return fetched, 0, False
    rows.sort(key=lambda x: x['candle_time'])
    window_start = rows[0]['candle_time']
    window_end = rows[-1]['candle_time']
    existing = _existing_times_in_window(
        programme_id, userid, symbol, timeframe_key,
        window_start, window_end,
    )
    fresh = [r for r in rows if r['candle_time'] not in existing]
    if not fresh:
        return fetched, 0, False
    current_count = _count_stored_rows(programme_id, userid, symbol, timeframe_key)
    projected_count = current_count + len(fresh)
    if projected_count > MAX_BARS_PER_TF:
        # Make room before writing, so the table does not temporarily grow
        # beyond the retention ceiling while the newest page is inserted.
        keep_before_insert = max(0, MAX_BARS_PER_TF - len(fresh))
        trimmed = _trim_oldest_rows(
            programme_id, userid, symbol, timeframe_key,
            keep_limit=keep_before_insert,
        )
        if trimmed is None:
            return fetched, 0, True
    inserted, failed = _insert_candle_rows_batched(fresh)
    trimmed = _trim_oldest_rows(programme_id, userid, symbol, timeframe_key)
    if trimmed is None:
        failed = True
    print(f"      [CHARTS] {symbol} {timeframe_key}: latest page "
          f"fetched={fetched} new={len(fresh)} inserted={inserted}")
    return fetched, inserted, failed
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
    if not rows:
        print(f"      [CHARTS][BACKFILL] {symbol} {timeframe_key}: "
              f"fetched={fetched} at start_pos={start_pos}, but no valid rows")
        return fetched, 0, False
    # MT5 data is normally chronological, but sort explicitly before using
    # the datetime window for duplicate detection and SQL insertion.
    rows.sort(key=lambda x: x['candle_time'])
    window_start = rows[0]['candle_time']
    window_end = rows[-1]['candle_time']
    existing = _existing_times_in_window(
        programme_id, userid, symbol, timeframe_key,
        window_start, window_end,
    )
    fresh = [r for r in rows if r['candle_time'] not in existing]
    if not fresh:
        print(f"      [CHARTS][BACKFILL] {symbol} {timeframe_key}: "
              f"rotation at MT5 positions {start_pos}-"
              f"{start_pos + fetched - 1}: fetched={fetched}, all already stored")
        return fetched, 0, False
    inserted, failed = _insert_candle_rows_batched(fresh)
    trimmed = _trim_oldest_rows(programme_id, userid, symbol, timeframe_key)
    if trimmed is None:
        failed = True
    print(f"      [CHARTS][BACKFILL] {symbol} {timeframe_key}: "
          f"rotation at MT5 positions {start_pos}-"
          f"{start_pos + fetched - 1}: fetched={fetched}, "
          f"new={len(fresh)}, inserted={inserted}")
    if failed or inserted < len(fresh):
        print(f"      [CHARTS][BACKFILL] {symbol} {timeframe_key}: "
              "database insert was incomplete; stopping this pair so the "
              "failed page is not silently skipped")
        return fetched, inserted, True
    return fetched, inserted, False
#  SYNC ONE (programme, symbol, timeframe) — ONE ROUND
# ==============================================================================
def _sync_one_symbol_timeframe(programme_id, userid, symbol, timeframe_key,
                               tf_const, tz_obj, tz_name, computer_tz=None):
    """
    Synchronize one programme/symbol/timeframe pair while retaining no more
    than MAX_BARS_PER_TF candles. Existing excess rows are pruned first;
    new latest candles replace the oldest rows when the pair is full.
    Historical backfill stops as soon as the pair reaches the 5,000-row cap.
    """
    if not mt5_symbol_select(symbol, True):
        print(f"      [CHARTS] {symbol} {timeframe_key}: symbol_select failed")
        return {'inserted': 0, 'skipped': True}
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
    latest_page_fetched, n_latest, latest_failed = _latest_pass(
        programme_id, userid, symbol, timeframe_key, tf_const, tz_obj, tz_name,
        computer_tz=computer_tz,
    )
    latest_inserted += n_latest
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
            if page_failed:
                insert_failed = True
                break
            # Revisit the newest page after each older page. If a new candle
            # closed while backfill was running, it is inserted now and shifts
            # all older MT5 position indexes by the number of new bars.
            latest_fetched_again, latest_new, latest_failed_again = _latest_pass(
                programme_id, userid, symbol, timeframe_key,
                tf_const, tz_obj, tz_name, computer_tz=computer_tz,
            )
            latest_inserted += latest_new
            next_start_pos += latest_new
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
                  f"collector cap ({MAX_BARS_PER_TF}). The collector can only "
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
    loop_interval = 5
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
                loop_interval = max(1, int(lower.split('=', 1)[1]))
            except ValueError:
                pass
        elif lower == '--interval' and index + 1 < len(args):
            try:
                loop_interval = max(1, int(args[index + 1]))
            except ValueError:
                pass
        elif lower.startswith('--max-loops='):
            try:
                max_loops = int(lower.split('=', 1)[1])
            except ValueError:
                pass
    print("\n" + "=" * 60)
    print("           BROKERCHARTS — PROGRAMME OHLC COLLECTOR", flush=True)
    print("=" * 60)
    print(f"  Loop Mode: {'ENABLED' if run_as_loop else 'DISABLED'}")
    if run_as_loop:
        print(f"  Interval: {loop_interval}s")
        print(f"  Max Loops: {max_loops if max_loops else 'Infinite'}")
    print(f"  Rotation targets: {ROUND_TARGETS}  cap={MAX_BARS_PER_TF}")
    print("=" * 60 + "\n")
    _worker_init()
    cpu_cores = os.cpu_count() or 1
    pool_size = max(1, min(cpu_cores, 4))
    print(f"🖥️  Cores detected: {cpu_cores} | Worker count: {pool_size}")
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
                print(f"[CHARTS] Loop #{loop_count}: querying programme candidates...", flush=True)
                programmes = _fetch_candidate_programmes()
                print(f"[CHARTS] Loop #{loop_count}: found {len(programmes)} candidate programme(s).", flush=True)
                if not programmes:
                    print(" └─ 🔘 No candidate programmes found.")
                    if not run_as_loop:
                        print("Single execution completed. Exiting...")
                        break
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
                time.sleep(5)
            if not run_as_loop:
                print("Single execution completed. Exiting...")
                break
            if max_loops and loop_count >= max_loops:
                print(f"Maximum loops ({max_loops}) reached. Exiting...")
                break
            if run_as_loop and loop_interval > 0:
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
