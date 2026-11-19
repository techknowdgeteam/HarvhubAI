# brokerdetails.py
"""
Handles MT5 broker detection for two FULLY INDEPENDENT account families:

    [SA]   sub-accounts / investors        (harvhub + vps tables)
    [PRG]  programme / trader accounts     (programme + programme_vps tables)

DB ACCESS MODEL
---------------
This script is a CLIENT of the long-running mysqlremote bridge process.
It does NOT launch Chrome itself and does NOT import mysqlremote.
It opens a TCP connection to 127.0.0.1:5599 and exchanges framed JSON.

MULTIPLEXED CHANNELS
--------------------
On first DB call this client ACQUIRES a dedicated channel from the bridge
(`queryrequest_{id}` / `queryresult_{id}`). The bridge maps that channel to
exactly ONE DOM window inside phpmyadmintemplate.php. Every subsequent
query from this process is sent on the same channel, so no other script
(e.g. script_b.py) can overwrite our textarea / results pane, and
vice versa. The channel is released at interpreter shutdown via atexit.

BATCH QUERIES
-------------
The bridge substitutes only ONE `%s` per request (see its
_substitute_params). Because of that, any place that needs N
statements with N different parameter values sends them as a single
`db_query_many()` call, with all values inlined. The bridge splits
the `;`-separated SQL, runs each statement, and returns a
`statement_results` list on the response. Every helper in this file
that used to issue N sequential round-trips now issues ONE batched
round-trip instead. There is still exactly one channel per
brokerdetails process, so all statements land in the same DOM
window.

BALANCE VERIFICATION (SA only)
------------------------------
After a live, matching probe for an SA sub-account whose DB
`broker_login_status` is `valid_credentials`, and whose
`balance_verification` column is `applied-for-verification`, this script
reads `mt5.account_info()` under the same IPC lock and writes
`broker_balance`, `balance_verification='verified'`, and (when the mode
is known and differs) `account_mode` back to `harvhub` — batched with
the notification insert. No JSON files, no temp files. Programme
accounts are never balance-verified here.

PROGRAMME BROKER SYMBOLS / TIMEFRAMES (PRG only)
------------------------------------------------
Whenever a programme account is confirmed LIVE with matching login + name
(newly_valid / reconnected / silent keep), this script opens the same
terminal under the MT5 IPC lock, enumerates every symbol via
`mt5.symbols_get()`, and probes the supported timeframes using the exact
same logic as the OHLC collector. The resulting lists are then written
back to `programme.broker_symbols` and `programme.broker_timeframes` in
a single batched CLEAR + WRITE. No notification is fired for this — it
is a silent background refresh.

On a successful SA balance verification, a one-time contract notification
is fired via notification_service.record_balance_verification_notification():
"Your MT5 balance verification has been updated. Go to your dashboard
for more details." Deduplicated per sub-account.
"""

import atexit
import ctypes
import os
import re
import sys
import time
import json
import socket
import struct
import shutil
import threading
import subprocess
import traceback
from datetime import datetime


import notification_service as notifier

try:
    from harvhub_ipc import manager_request
except Exception:
    manager_request = None

try:
    import MetaTrader5 as mt5
except Exception:
    mt5 = None


# ==============================================================================
#  BRIDGE CLIENT — channel-aware, talks to mysqlremote over localhost TCP
# ==============================================================================
BRIDGE_HOST = "127.0.0.1"
BRIDGE_PORT = 5599

BRIDGE_OWNER = "brokerdetails"

_HEADER = struct.Struct(">I")
MAX_FRAME = 64 * 1024 * 1024


class _BridgeError(Exception):
    pass


class _BridgeClient:
    def __init__(self, host=BRIDGE_HOST, port=BRIDGE_PORT, owner=BRIDGE_OWNER):
        self.host = host
        self.port = port
        self.owner = owner
        self.channel_id = None
        self._channel_lock = threading.Lock()

        self.queryrequest = self
        self.queryresult  = self

    def _send_request(self, req):
        try:
            sock = socket.create_connection((self.host, self.port), timeout=15)
        except Exception as e:
            raise _BridgeError(
                f"cannot connect to bridge {self.host}:{self.port}: {e}")

        try:
            payload = json.dumps(req, ensure_ascii=False).encode('utf-8')
            sock.sendall(_HEADER.pack(len(payload)) + payload)

            header = self._recv_exact(sock, _HEADER.size)
            if header is None:
                raise _BridgeError("bridge closed the connection (no header)")
            (length,) = _HEADER.unpack(header)
            if length == 0 or length > MAX_FRAME:
                raise _BridgeError(f"bad frame length from bridge: {length}")

            body = self._recv_exact(sock, length)
            if body is None:
                raise _BridgeError("bridge closed the connection (no body)")

            return json.loads(body.decode('utf-8'))
        finally:
            try:
                sock.close()
            except Exception:
                pass

    @staticmethod
    def _recv_exact(sock, n):
        buf = bytearray()
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:
                return None
            buf.extend(chunk)
        return bytes(buf)

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
                    f"acquire_channel failed: {resp.get('message')}")
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

    def channel_status(self):
        if self.channel_id is None:
            return None
        return self._send_request({
            'op': 'channel_status',
            'channel_id': self.channel_id,
        })

    def list_channels(self):
        return self._send_request({'op': 'list_channels'})

    def put(self, request, timeout=None):
        req = dict(request)
        if 'sql' in req and 'channel_id' not in req:
            cid = self.acquire_channel()
            req['channel_id'] = cid
        return self._send_request(req)

    def get(self, timeout=None, block=True):
        raise _BridgeError(
            "brokerdetails now uses request/response in one call; "
            "queryresult.get() should not be used directly."
        )


db = _BridgeClient()
atexit.register(db.release_channel)


# ==============================================================================
#  CONFIG
# ==============================================================================
DEFAULT_MT5_PATH     = r"C:\xampp\htdocs\harvhubAI\mt5\MetaTrader 5"
MT5_DESTINATION_PATH = r"C:\xampp\htdocs\harvhubAI\mt5"

MANUAL_POLL_INTERVAL = 1.5
COPY_FEEDBACK_MS     = 900

TERMINAL_BOOT_WAIT   = 9.0

MT5_PROCESS_NAMES = (
    'terminal64.exe',
    'metatester64.exe',
    'mt5liveupdate.exe',
    'metaeditor64.exe',
)

TERMINAL_KILL_WAIT_TOTAL = 15.0
TERMINAL_KILL_POLL       = 0.5

DELETE_RETRY_COUNT = 10
DELETE_RETRY_WAIT  = 3.0

TERMINAL_NEGATIVE_STATUSES = (
    'invalid_credentials',
    'fullname_mismatch',
    'invalid_account',
)

PROGRAMME_NEGATIVE_STATUSES = (
    'invalid_credentials',
    'fullname_mismatch',
)

VALID_STATUS = 'valid_credentials'

BALANCE_VERIFICATION_TRIGGERS = (
    'applied-for-verification',
    'applied_for_verification',
    'applied',
)

CREATE_NO_WINDOW = 0x08000000

_MT5_IPC_LOCK = threading.RLock()


# ==============================================================================
#  TIMEFRAME MAP  (mirrors ohlc collector exactly)
# ==============================================================================
TIMEFRAME_MAP = None  # populated lazily below once mt5 is available


def _build_timeframe_map():
    global TIMEFRAME_MAP
    if mt5 is None:
        TIMEFRAME_MAP = {}
        return
    TIMEFRAME_MAP = {
        "1m":  mt5.TIMEFRAME_M1,  "2m":  mt5.TIMEFRAME_M2,  "3m":  mt5.TIMEFRAME_M3,
        "4m":  mt5.TIMEFRAME_M4,  "5m":  mt5.TIMEFRAME_M5,  "6m":  mt5.TIMEFRAME_M6,
        "10m": mt5.TIMEFRAME_M10, "12m": mt5.TIMEFRAME_M12, "15m": mt5.TIMEFRAME_M15,
        "20m": mt5.TIMEFRAME_M20, "30m": mt5.TIMEFRAME_M30,
        "1h":  mt5.TIMEFRAME_H1,  "2h":  mt5.TIMEFRAME_H2,  "3h":  mt5.TIMEFRAME_H3,
        "4h":  mt5.TIMEFRAME_H4,  "6h":  mt5.TIMEFRAME_H6,  "8h":  mt5.TIMEFRAME_H8,
        "12h": mt5.TIMEFRAME_H12, "1d":  mt5.TIMEFRAME_D1,  "1w":  mt5.TIMEFRAME_W1,
        "1mo": mt5.TIMEFRAME_MN1,
    }


# ==============================================================================
#  Local IP
# ==============================================================================
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
#  DB wrappers  (single-statement + batched)
# ==============================================================================
def db_query(sql, params=None, timeout=20.0, quiet=False):
    start = time.perf_counter()
    try:
        result = db.put(
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


def _is_ok(res):
    return bool(res) and res.get('status') == 'success'


def db_query_many(statements, timeout=60.0, quiet=False):
    """
    Send multiple SQL statements to the bridge in ONE request.

    The bridge only substitutes one `%s` per request, so batched SQL
    must inline every value. Callers that need parameter binding
    should call db_query() per statement instead.

    Returns a normalised dict:
        {
          'status':  'success' | 'partial' | 'error',
          'message': ...,
          'statements': [
              {'status': 'success'|'empty'|'error'|'skipped',
               'rows': [...],
               'affected_rows': int,
               'message': str},
              ...
          ],
          'raw_response': <full bridge response>,
          'elapsed': float,
        }

    On any transport failure, returns the same shape with
    status='error' and statements=[] so callers can treat it uniformly.
    """
    if not statements:
        return {
            'status': 'success', 'message': 'no statements',
            'statements': [], 'raw_response': None, 'elapsed': 0.0,
        }

    joined = ";\n".join(
        s.strip().rstrip(';') for s in statements if s and s.strip()
    )
    if not joined:
        return {
            'status': 'success', 'message': 'no executable statements',
            'statements': [], 'raw_response': None, 'elapsed': 0.0,
        }

    start = time.perf_counter()
    try:
        result = db.put({'sql': joined, 'timeout': timeout})
    except _BridgeError as e:
        return {
            'status': 'error',
            'message': f'bridge error: {e}',
            'statements': [], 'raw_response': None,
            'elapsed': time.perf_counter() - start,
        }

    elapsed = time.perf_counter() - start

    if not quiet and result.get('status') != 'success':
        print(f"      [SQL-MANY FAIL] {result.get('message')}")
        print(f"      [SQL-MANY WAS ] {joined[:300]}")

    stmts = result.get('statement_results')
    if stmts is None:
        stmts = [{
            'status': result.get('status', 'error'),
            'rows': result.get('results') or [],
            'affected_rows': result.get('affected_rows') or 0,
            'message': result.get('message', ''),
        }]

    normalised = []
    for s in stmts:
        if isinstance(s, dict):
            normalised.append({
                'status':        s.get('status', 'success'),
                'rows':          s.get('rows') or [],
                'affected_rows': s.get('affected_rows') or 0,
                'message':       s.get('message') or '',
            })
        else:
            normalised.append({
                'status': 'error', 'rows': [], 'affected_rows': 0,
                'message': f'unexpected statement_result shape: {type(s).__name__}',
            })

    return {
        'status':       result.get('status', 'error'),
        'message':      result.get('message', ''),
        'statements':   normalised,
        'raw_response': result,
        'elapsed':      elapsed,
    }


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
#  SQL-safe literal helpers  (used when inlining into batched SQL)
# ---------------------------------------------------------------------------
def _sql_quote(value):
    """Return a SQL string literal for a Python value — or NULL/0/1."""
    if value is None:
        return 'NULL'
    if isinstance(value, bool):
        return '1' if value else '0'
    if isinstance(value, (int, float)):
        return str(value)
    s = str(value)
    s = s.replace('\\', '\\\\').replace("'", "''")
    return f"'{s}'"


def _sql_int(value):
    try:
        if value is None:
            return 'NULL'
        return str(int(value))
    except Exception:
        return 'NULL'


# ---------------------------------------------------------------------------
#  Programme-scoped notification PDO adapter (single instance)
# ---------------------------------------------------------------------------
_PDO = notifier.make_pdo(db_query)


# ==============================================================================
#  IN-MEMORY TERMINAL STATE
# ==============================================================================
_ACCOUNT_STATE = {}
_ACCOUNT_STATE_LOCK = threading.Lock()


def _mark_terminal_live(kind, owner_id, account_id):
    with _ACCOUNT_STATE_LOCK:
        _ACCOUNT_STATE[(kind, owner_id, account_id)] = 'live'


def _mark_terminal_dark(kind, owner_id, account_id):
    with _ACCOUNT_STATE_LOCK:
        _ACCOUNT_STATE[(kind, owner_id, account_id)] = 'dark'


def _was_terminal_dark(kind, owner_id, account_id):
    with _ACCOUNT_STATE_LOCK:
        return _ACCOUNT_STATE.get((kind, owner_id, account_id)) == 'dark'


def _was_terminal_live(kind, owner_id, account_id):
    with _ACCOUNT_STATE_LOCK:
        return _ACCOUNT_STATE.get((kind, owner_id, account_id)) == 'live'


# ==============================================================================
#  Helpers
# ==============================================================================
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


def _is_empty(value) -> bool:
    if value is None:
        return True
    return str(value).strip().lower() in ('', 'null', 'none', 'nan')


def _sanitize_folder_component(value):
    if value is None:
        return ''
    s = str(value).strip()
    s = re.sub(r'[<>:"/\\|?*]', '_', s)
    s = s.replace(' ', '_')
    return s


def _expected_fullname_from_row(row):
    first = (_pick_first_key(row, 'first_name') or '').strip()
    last  = (_pick_first_key(row, 'last_name')  or '').strip()
    if first or last:
        return (first + ' ' + last).strip()
    full = (_pick_first_key(row, 'fullname') or '').strip()
    return full


def _name_tokens(value):
    return set(re.findall(r'[a-z0-9]+', (value or '').lower()))


def _name_flat(value):
    return re.sub(r'[^a-z0-9]+', '', (value or '').lower())


def _names_match(expected, actual):
    e = (expected or '').strip().lower()
    a = (actual or '').strip().lower()
    if not e or not a:
        return True
    if _name_tokens(e) & _name_tokens(a):
        return True
    e_flat = _name_flat(e)
    a_flat = _name_flat(a)
    if e_flat and a_flat and (e_flat in a_flat or a_flat in e_flat):
        return True
    return False


def _normalize_row(row):
    if row is None:
        return None
    if isinstance(row, dict):
        return {
            (k.strip().lower() if isinstance(k, str) else k): v
            for k, v in row.items()
        }
    if isinstance(row, (list, tuple)):
        return {
            'id':                         row[0]  if len(row) > 0  else None,
            'sub_account_id':             row[1]  if len(row) > 1  else None,
            'main_account_id':            row[2]  if len(row) > 2  else None,
            'email':                      row[3]  if len(row) > 3  else None,
            'broker':                     row[4]  if len(row) > 4  else None,
            'server':                     row[5]  if len(row) > 5  else None,
            'login':                      row[6]  if len(row) > 6  else None,
            'broker_password':            row[7]  if len(row) > 7  else None,
            'broker_login_status':        row[8]  if len(row) > 8  else None,
            'broker_login_attempt_used':  row[9]  if len(row) > 9  else None,
            'broker_total_login_attempt': row[10] if len(row) > 10 else None,
            'demo_account':               row[11] if len(row) > 11 else None,
        }
    return None


# ==============================================================================
#  harvhub / programme column whitelists
# ==============================================================================
HARVHUB_SELECT_COLUMNS = [
    'id', 'main_account_id', 'sub_account_id', 'sub_account_name',
    'is_main_account', 'broker', 'email', 'fullname', 'first_name',
    'last_name', 'username', 'server', 'login', 'broker_password',
    'application_status', 'broker_balance', 'recent_highest_balance',
    'balance_verification', 'profitandloss', 'balance_display',
    'execution_start_date', 'loyalties', 'reset_contract', 'contract_id',
    'account_mode', 'demo_account', 'Terminal_path', 'broker_login_status',
    'broker_login_attempt_used', 'broker_total_login_attempt',
    'disconnect_broker_request',
]


def _broker_select_sql():
    return ', '.join(HARVHUB_SELECT_COLUMNS)


PROGRAMME_SELECT_COLUMNS = [
    'id', 'userid', 'program_name', 'broker', 'server', 'login',
    'broker_password', 'broker_login_status', 'broker_login_attempt_used',
    'broker_total_login_attempt', 'disconnect_broker_request', 'demo_account',
    'broker_symbols', 'broker_timeframes',
]


def _programme_select_sql():
    return ', '.join(PROGRAMME_SELECT_COLUMNS)


# ==============================================================================
#  Folder / path
# ==============================================================================
def build_mt5_folder_name(server, login, user_id, kind, account_id):
    s   = _sanitize_folder_component(server)
    l   = _sanitize_folder_component(login)
    u   = _sanitize_folder_component(user_id)
    aid = _sanitize_folder_component(account_id)
    return f"{s}_{l}_U{u}_{kind}{aid}"


def build_mt5_terminal_path_sa(server, login, user_id, sub_account_id):
    folder_name = build_mt5_folder_name(server, login, user_id, 'SA', sub_account_id)
    folder_abs   = os.path.join(MT5_DESTINATION_PATH, folder_name)
    terminal_exe = os.path.join(folder_abs, "terminal64.exe")
    return folder_name, folder_abs, terminal_exe


def build_mt5_terminal_path_prog(server, login, user_id, programme_id):
    folder_name = build_mt5_folder_name(server, login, user_id, 'P', programme_id)
    folder_abs   = os.path.join(MT5_DESTINATION_PATH, folder_name)
    terminal_exe = os.path.join(folder_abs, "terminal64.exe")
    return folder_name, folder_abs, terminal_exe


def find_all_folders_for_sub_account(user_id, sub_account_id):
    matches = []
    if not os.path.isdir(MT5_DESTINATION_PATH):
        return matches
    suffix = f"_U{user_id}_SA{sub_account_id}"
    try:
        for name in os.listdir(MT5_DESTINATION_PATH):
            full = os.path.join(MT5_DESTINATION_PATH, name)
            if not os.path.isdir(full):
                continue
            if name.endswith(suffix):
                matches.append(full)
    except Exception as e:
        print(f"   [SA][GLOB] listdir failed: {e}")
    return matches


def find_all_folders_for_programme(user_id, programme_id):
    matches = []
    if not os.path.isdir(MT5_DESTINATION_PATH):
        return matches
    suffix = f"_U{user_id}_P{programme_id}"
    try:
        for name in os.listdir(MT5_DESTINATION_PATH):
            full = os.path.join(MT5_DESTINATION_PATH, name)
            if not os.path.isdir(full):
                continue
            if name.endswith(suffix):
                matches.append(full)
    except Exception as e:
        print(f"   [PRG][GLOB] listdir failed: {e}")
    return matches


def ensure_mt5_folder(server, login, user_id, kind, account_id):
    if kind == 'SA':
        folder_name, folder_abs, terminal_exe = build_mt5_terminal_path_sa(
            server, login, user_id, account_id
        )
    else:
        folder_name, folder_abs, terminal_exe = build_mt5_terminal_path_prog(
            server, login, user_id, account_id
        )

    if not os.path.isdir(DEFAULT_MT5_PATH):
        return False, folder_abs, terminal_exe, (
            f"Source MT5 template not found: {DEFAULT_MT5_PATH}"
        )

    if os.path.isdir(folder_abs):
        if os.path.exists(terminal_exe):
            return True, folder_abs, terminal_exe, "Folder already exists"
        try:
            shutil.rmtree(folder_abs, ignore_errors=True)
        except Exception:
            pass

    os.makedirs(MT5_DESTINATION_PATH, exist_ok=True)

    try:
        print(f"   [{kind}][COPY-FOLDER] MT5 template -> {folder_name}")
        shutil.copytree(
            DEFAULT_MT5_PATH,
            folder_abs,
            ignore_dangling_symlinks=True,
            ignore=shutil.ignore_patterns('*.lock', '*.log'),
        )
    except Exception as e:
        if os.path.isdir(folder_abs):
            shutil.rmtree(folder_abs, ignore_errors=True)
        return False, folder_abs, terminal_exe, f"Copy failed: {e}"

    if not os.path.exists(terminal_exe):
        return False, folder_abs, terminal_exe, (
            f"terminal64.exe missing after copy: {terminal_exe}"
        )

    return True, folder_abs, terminal_exe, "Folder created"


def delete_mt5_folder(folder_abs, tag='SA'):
    if not folder_abs or not os.path.isdir(folder_abs):
        return False
    try:
        shutil.rmtree(folder_abs, ignore_errors=True)
        print(f"   [{tag}][DELETE-FOLDER] {folder_abs}")
        return True
    except Exception as e:
        print(f"   [{tag}][DELETE-FOLDER] failed: {e}")
        return False


# ==============================================================================
#  Terminal launcher
# ==============================================================================
# Keep a short startup reservation rather than a permanent "launched" flag.
# This prevents concurrent scans from opening the same terminal while Windows
# is still creating its process, but still permits a manually closed terminal
# to be reopened on a later scan.
_LAUNCHED_FOLDERS = {}
_LAUNCH_LOCK = threading.Lock()
_TERMINAL_STARTUP_GRACE = 15.0


def _normalise_windows_path(value):
    value = os.path.normcase(os.path.normpath(os.path.abspath(str(value or ''))))
    if value.startswith('\\\\?\\'):
        value = value[4:]
    return value


def _terminal_running_for_folder(folder_abs):
    """Check the actual process image path, not whether the EXE can be opened.

    Opening terminal64.exe in append mode is not a process check: Windows often
    permits opening the executable while it is running. Enumerate Windows
    processes and compare QueryFullProcessImageNameW with this portable folder.
    """
    target_exe = os.path.join(folder_abs, "terminal64.exe")
    if not os.path.isfile(target_exe):
        return False

    if os.name != 'nt':
        # The production collector runs on Windows. Avoid treating an unrelated
        # terminal64.exe elsewhere on the system as this portable terminal.
        return False

    try:
        from ctypes import wintypes

        class PROCESSENTRY32W(ctypes.Structure):
            _fields_ = [
                ("dwSize", wintypes.DWORD),
                ("cntUsage", wintypes.DWORD),
                ("th32ProcessID", wintypes.DWORD),
                ("th32DefaultHeapID", ctypes.c_size_t),
                ("th32ModuleID", wintypes.DWORD),
                ("cntThreads", wintypes.DWORD),
                ("th32ParentProcessID", wintypes.DWORD),
                ("pcPriClassBase", ctypes.c_long),
                ("dwFlags", wintypes.DWORD),
                ("szExeFile", wintypes.WCHAR * 260),
            ]

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateToolhelp32Snapshot.argtypes = [wintypes.DWORD, wintypes.DWORD]
        kernel32.CreateToolhelp32Snapshot.restype = wintypes.HANDLE
        kernel32.Process32FirstW.argtypes = [wintypes.HANDLE, ctypes.POINTER(PROCESSENTRY32W)]
        kernel32.Process32FirstW.restype = wintypes.BOOL
        kernel32.Process32NextW.argtypes = [wintypes.HANDLE, ctypes.POINTER(PROCESSENTRY32W)]
        kernel32.Process32NextW.restype = wintypes.BOOL
        kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        kernel32.OpenProcess.restype = wintypes.HANDLE
        kernel32.QueryFullProcessImageNameW.argtypes = [
            wintypes.HANDLE, wintypes.DWORD, wintypes.LPWSTR,
            ctypes.POINTER(wintypes.DWORD),
        ]
        kernel32.QueryFullProcessImageNameW.restype = wintypes.BOOL
        kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel32.CloseHandle.restype = wintypes.BOOL

        snapshot = kernel32.CreateToolhelp32Snapshot(0x00000002, 0)
        invalid_handle = ctypes.c_void_p(-1).value
        # A HANDLE restype is converted to a Python int by ctypes on Windows;
        # do not cast that integer as though it were already a pointer object.
        snapshot_value = snapshot.value if isinstance(snapshot, ctypes.c_void_p) else snapshot
        if snapshot_value in (None, 0, invalid_handle):
            raise OSError(ctypes.get_last_error(), "CreateToolhelp32Snapshot failed")

        target = _normalise_windows_path(target_exe)
        entry = PROCESSENTRY32W()
        entry.dwSize = ctypes.sizeof(PROCESSENTRY32W)
        found = False
        try:
            more = kernel32.Process32FirstW(snapshot, ctypes.byref(entry))
            while more:
                if entry.szExeFile.lower() == "terminal64.exe":
                    process_handle = kernel32.OpenProcess(
                        0x1000, False, int(entry.th32ProcessID)
                    )
                    if process_handle:
                        try:
                            buffer = ctypes.create_unicode_buffer(32768)
                            size = wintypes.DWORD(len(buffer))
                            if kernel32.QueryFullProcessImageNameW(
                                process_handle, 0, buffer, ctypes.byref(size)
                            ):
                                if _normalise_windows_path(buffer.value) == target:
                                    found = True
                                    break
                        finally:
                            kernel32.CloseHandle(process_handle)
                more = kernel32.Process32NextW(snapshot, ctypes.byref(entry))
        finally:
            kernel32.CloseHandle(snapshot)
        return found
    except Exception as e:
        print(f"      [LAUNCH][PROCESS-CHECK] Could not inspect Windows process paths: {e}")
        return False


def launch_terminal(terminal_exe, folder_abs):
    folder_key = _normalise_windows_path(folder_abs)

    # One lock covers process detection and Popen. Retain the Popen handle so
    # later scans can tell whether the exact process started by this service is
    # still alive, even if a Windows process-path query is temporarily denied.
    with _LAUNCH_LOCK:
        now = time.monotonic()
        reservation = _LAUNCHED_FOLDERS.get(folder_key)
        launched_at = None
        if isinstance(reservation, dict):
            launched_at = reservation.get('started_at')
            process = reservation.get('process')
            if process is not None:
                try:
                    if process.poll() is None:
                        print(f"      [LAUNCH] this service already owns the running terminal: {terminal_exe}")
                        return False, True
                except Exception:
                    pass
        elif reservation is not None:
            # Compatibility with an in-memory reservation created by an older
            # version during a hot reload.
            launched_at = reservation

        if _terminal_running_for_folder(folder_abs):
            _LAUNCHED_FOLDERS[folder_key] = {'started_at': now, 'process': None}
            print(f"      [LAUNCH] exact portable terminal already running: {terminal_exe}")
            return False, True

        if launched_at is not None and (now - float(launched_at)) < _TERMINAL_STARTUP_GRACE:
            print(f"      [LAUNCH] startup already requested; suppressing duplicate: {terminal_exe}")
            return False, True

        # Stale reservations are discarded so a terminal closed by its user
        # can be started again on a later scan.
        _LAUNCHED_FOLDERS.pop(folder_key, None)

        try:
            DETACHED_PROCESS = 0x00000008
            CREATE_NEW_PROCESS_GROUP = 0x00000200
            process = subprocess.Popen(
                [terminal_exe, "/portable"],
                cwd=folder_abs,
                creationflags=DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP,
                close_fds=True,
            )
        except Exception as e:
            print(f"      [LAUNCH] failed: {e}")
            return False, False

        _LAUNCHED_FOLDERS[folder_key] = {'started_at': now, 'process': process}
        print(f"      [LAUNCH] started once: {terminal_exe} (PID={getattr(process, 'pid', 'unknown')})")
        return True, True


def bring_terminal_to_front(terminal_exe):
    try:
        os.startfile(terminal_exe)
        print(f"      [FOCUS] opened: {terminal_exe}")
        return True
    except Exception as e:
        print(f"      [FOCUS] os.startfile failed: {e}")
        try:
            subprocess.Popen([terminal_exe], close_fds=True)
            print(f"      [FOCUS] Popen opened: {terminal_exe}")
            return True
        except Exception as e2:
            print(f"      [FOCUS] Popen failed: {e2}")
            return False


# ==============================================================================
#  Terminal discovery + kill helpers
# ==============================================================================
def _run_shell(cmd, timeout=15):
    try:
        return subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            creationflags=CREATE_NO_WINDOW,
            timeout=timeout,
        ).stdout.decode(errors='ignore')
    except Exception as e:
        print(f"      [CMD] failed: {cmd!r} -> {e}")
        return ''


def _list_pids_for_image(image_name):
    out = _run_shell(
        f'tasklist /FI "IMAGENAME eq {image_name}" /FO CSV /NH',
        timeout=10,
    )
    pids = []
    for line in out.splitlines():
        parts = [p.strip('"') for p in line.split('","')]
        if len(parts) >= 2 and parts[0].lower() == image_name.lower():
            try:
                pids.append(int(parts[1]))
            except Exception:
                pass
    return pids


def list_running_mt5_processes():
    found = []
    for image in MT5_PROCESS_NAMES:
        for pid in _list_pids_for_image(image):
            found.append((image, pid))
    return found


def _is_pid_alive(pid):
    out = _run_shell(f'tasklist /FI "PID eq {pid}" /FO CSV /NH', timeout=5)
    for line in out.splitlines():
        parts = [p.strip('"') for p in line.split('","')]
        if len(parts) >= 2:
            try:
                if int(parts[1]) == pid:
                    return True
            except Exception:
                pass
    return False


def kill_pids(pids):
    killed = []
    for pid in pids:
        if not _is_pid_alive(pid):
            killed.append(pid)
            continue
        _run_shell(f'taskkill /F /PID {pid}', timeout=10)
        killed.append(pid)
        print(f"      [KILL-TERMINAL] taskkill /F /PID {pid}")

    deadline = time.time() + TERMINAL_KILL_WAIT_TOTAL
    pending = list(killed)

    while pending and time.time() < deadline:
        still_alive = [pid for pid in pending if _is_pid_alive(pid)]
        if not still_alive:
            break
        time.sleep(TERMINAL_KILL_POLL)
        pending = still_alive

    if pending:
        print(f"      [KILL-TERMINAL] still alive after wait: {pending}")
    else:
        print(f"      [KILL-TERMINAL] all confirmed dead: {killed}")

    return killed


def kill_terminal_for_folder(folder_abs):
    procs = list_running_mt5_processes()

    if not procs:
        print(f"      [KILL-TERMINAL] no MT5 process running")
        with _LAUNCH_LOCK:
            _LAUNCHED_FOLDERS.pop(_normalise_windows_path(folder_abs), None)
        return 0

    print(f"      [KILL-TERMINAL] found running MT5 processes: {procs}")
    pids = [pid for _, pid in procs]
    kill_pids(pids)

    with _LAUNCH_LOCK:
        _LAUNCHED_FOLDERS.pop(_normalise_windows_path(folder_abs), None)

    return len(pids)


def _rmtree_via_cmd(folder_abs):
    try:
        result = subprocess.run(
            f'cmd /c rmdir /s /q "{folder_abs}"',
            shell=True,
            capture_output=True,
            creationflags=CREATE_NO_WINDOW,
            timeout=60,
        )
        if not os.path.isdir(folder_abs):
            return True
        print(
            f"   [FORCE-DELETE] cmd rmdir exit={result.returncode} "
            f"stderr={result.stderr.decode(errors='ignore').strip()!r}"
        )
    except Exception as e:
        print(f"   [FORCE-DELETE] cmd rmdir failed: {e}")
    return False


def force_delete_folder(folder_abs, terminal_exe=None):
    if not folder_abs or not os.path.isdir(folder_abs):
        return True

    if terminal_exe is None:
        terminal_exe = os.path.join(folder_abs, "terminal64.exe")

    print(f"   [FORCE-DELETE] step 1: kill any MT5 process")
    kill_terminal_for_folder(folder_abs)

    last_err = None
    for attempt in range(1, DELETE_RETRY_COUNT + 1):
        try:
            shutil.rmtree(folder_abs, ignore_errors=False)
            print(f"   [FORCE-DELETE] removed on attempt {attempt}: {folder_abs}")
            return True
        except Exception as e:
            last_err = e
            print(f"   [FORCE-DELETE] attempt {attempt} failed: {e}")
            if not os.path.isdir(folder_abs):
                print(f"   [FORCE-DELETE] folder already gone")
                return True
            time.sleep(DELETE_RETRY_WAIT)

    print(f"   [FORCE-DELETE] step 3: cmd /c rmdir /s /q")
    if _rmtree_via_cmd(folder_abs):
        print(f"   [FORCE-DELETE] removed via cmd rmdir: {folder_abs}")
        return True

    try:
        trash = folder_abs + f"_trash_{int(time.time())}"
        os.rename(folder_abs, trash)
        time.sleep(0.5)
        _rmtree_via_cmd(trash)
        if not os.path.isdir(trash):
            print(f"   [FORCE-DELETE] renamed+removed: {trash}")
            return True
        else:
            print(f"   [FORCE-DELETE] renamed but still present: {trash}")
    except Exception as e:
        print(f"   [FORCE-DELETE] rename attempt failed: {e}")

    print(f"   [FORCE-DELETE] giving up, last error: {last_err}")
    return not os.path.isdir(folder_abs)


# ==============================================================================
#  Read-only IPC probe
# ==============================================================================
def probe_terminal_account(terminal_exe, folder_abs):
    with _MT5_IPC_LOCK:
        if mt5 is None:
            return 'unreachable', {'error': 'MetaTrader5 module not available'}

        if not os.path.exists(terminal_exe):
            return 'unreachable', {'error': f'Terminal not found: {terminal_exe}'}

        try:
            mt5.shutdown()
        except Exception:
            pass

        try:
            ok = mt5.initialize(
                path=terminal_exe,
                timeout=10000,
                portable=True,
            )
        except Exception as e:
            return 'unreachable', {'error': f'initialize() raised: {e}'}

        if not ok:
            return 'unreachable', {'error': f'initialize() failed: {mt5.last_error()}'}

        info = None
        try:
            acc = mt5.account_info()
            if acc is not None and getattr(acc, 'login', None):
                info = {
                    'login':    int(acc.login),
                    'server':   getattr(acc, 'server', None),
                    'name':     getattr(acc, 'name', None),
                    'balance':  getattr(acc, 'balance', None),
                    'currency': getattr(acc, 'currency', None),
                    'leverage': getattr(acc, 'leverage', None),
                }
        except Exception:
            info = None

        try:
            mt5.shutdown()
        except Exception:
            pass

        if info is None:
            return 'not_logged_in', None
        return 'logged_in', info


# ==============================================================================
#  BALANCE VERIFICATION (SA only)
# ==============================================================================
def _read_live_balance(terminal_exe):
    with _MT5_IPC_LOCK:
        if mt5 is None:
            return False, {'error': 'MetaTrader5 module not available'}

        if not os.path.exists(terminal_exe):
            return False, {'error': f'Terminal not found: {terminal_exe}'}

        try:
            mt5.shutdown()
        except Exception:
            pass

        try:
            ok = mt5.initialize(
                path=terminal_exe,
                timeout=10000,
                portable=True,
            )
        except Exception as e:
            return False, {'error': f'initialize() raised: {e}'}

        if not ok:
            return False, {'error': f'initialize() failed: {mt5.last_error()}'}

        payload = None
        try:
            acc = mt5.account_info()
            if acc is not None:
                trade_mode = getattr(acc, 'trade_mode', None)
                payload = {
                    'login':      int(acc.login) if getattr(acc, 'login', None) else None,
                    'server':     getattr(acc, 'server', None),
                    'name':       getattr(acc, 'name', None),
                    'balance':    float(getattr(acc, 'balance', 0) or 0),
                    'currency':   getattr(acc, 'currency', None) or '',
                    'leverage':   getattr(acc, 'leverage', None),
                    'trade_mode': trade_mode,
                }
        except Exception as e:
            payload = {'error': f'account_info() raised: {e}'}
        finally:
            try:
                mt5.shutdown()
            except Exception:
                pass

        if payload is None:
            return False, {'error': 'account_info() returned None'}
        if 'error' in payload:
            return False, payload
        return True, payload


def _mt5_trade_mode_to_str(trade_mode):
    if mt5 is None or trade_mode is None:
        return 'unknown'
    try:
        if trade_mode == mt5.ACCOUNT_TRADE_MODE_REAL:
            return 'real'
        if trade_mode in (mt5.ACCOUNT_TRADE_MODE_DEMO,
                          mt5.ACCOUNT_TRADE_MODE_CONTEST):
            return 'demo'
    except Exception:
        pass
    return 'unknown'


def _verify_and_persist_balance(user_id, sub_account_id, terminal_exe, row):
    if mt5 is None:
        return {'ok': False, 'reason': 'mt5_unavailable'}

    pk_id = _coerce_int(_pick_first_key(row, 'id'))
    if pk_id is None:
        pk_id = _coerce_int(user_id)
    if pk_id is None:
        return {'ok': False, 'reason': 'no_pk_id'}

    raw_bv = (_pick_first_key(row, 'balance_verification') or '').strip().lower()
    if raw_bv not in BALANCE_VERIFICATION_TRIGGERS:
        return {'ok': False, 'reason': f'not_applied:{raw_bv}'}

    demo_account_raw = (_pick_first_key(row, 'demo_account') or '')
    demo_account_str = str(demo_account_raw).strip()
    account_mode_raw = (_pick_first_key(row, 'account_mode') or '')
    account_mode_str = str(account_mode_raw).strip().lower()

    ok, payload = _read_live_balance(terminal_exe)
    if not ok:
        print(f"      [SA][VERIFY] sub={sub_account_id} balance read failed: "
              f"{payload.get('error')}")
        return {'ok': False, 'reason': 'read_failed', 'error': payload.get('error')}

    balance      = float(payload.get('balance') or 0)
    currency     = payload.get('currency') or ''
    term_login   = payload.get('login')
    term_server  = payload.get('server')
    actual_mode  = _mt5_trade_mode_to_str(payload.get('trade_mode'))

    print(f"      [SA][VERIFY] sub={sub_account_id} acct={term_login} "
          f"mode={actual_mode} balance={currency} {balance:,.2f}")

    if actual_mode == 'demo':
        if demo_account_str == '0':
            msg = 'DEMO account disabled (demo_account=0)'
            print(f"      [SA][VERIFY] sub={sub_account_id} skip: {msg}")
            return {'ok': False, 'reason': 'demo_disabled'}
        if demo_account_str != '1':
            msg = 'DEMO account not enabled'
            print(f"      [SA][VERIFY] sub={sub_account_id} skip: {msg}")
            return {'ok': False, 'reason': 'demo_not_enabled'}

    # Build ONE batched statement list: the balance update + any
    # column write + the notification insert are all grouped so the
    # bridge handles them in a single round-trip. The notification
    # insert is added by the notifier (not here); we still do the
    # UPDATE as one statement, but route it through db_query_many so
    # the path is uniform with the rest of the file.
    set_pairs = [
        ("broker_balance",       f"{balance:.2f}"),
        ("balance_verification", "verified"),
    ]
    if account_mode_str != actual_mode and actual_mode != 'unknown':
        set_pairs.append(("account_mode", actual_mode))

    set_sql = ', '.join(f"`{k}` = {_sql_quote(v)}" for k, v in set_pairs)
    upd_stmt = (
        f"UPDATE harvhub SET {set_sql} "
        f"WHERE id = {_sql_int(pk_id)}"
    )

    batch = db_query_many([upd_stmt], timeout=15.0, quiet=True)
    if batch.get('status') not in ('success', 'partial'):
        print(f"      [SA][VERIFY] sub={sub_account_id} DB write failed: "
              f"{batch.get('message')}")
        return {'ok': False, 'reason': 'db_write_failed',
                'error': batch.get('message')}

    print(f"      [SA][VERIFY] sub={sub_account_id} -> VERIFIED "
          f"({currency} {balance:,.2f}) written to harvhub.id={pk_id}")

    try:
        notif_id = notifier.record_balance_verification_notification(_PDO, row)
        if notif_id:
            print(f"      [SA][VERIFY] sub={sub_account_id} "
                  f"-> balance-verification notification_id={notif_id}")
        else:
            print(f"      [SA][VERIFY] sub={sub_account_id} "
                  f"-> balance-verification notification already sent "
                  f"(dedup) or ineligible")
    except Exception as e:
        print(f"      [SA][VERIFY] balance-verification notification raised: {e}")
        traceback.print_exc()

    return {
        'ok':       True,
        'status':   'verified',
        'balance':  balance,
        'currency': currency,
        'login':    term_login,
        'server':   term_server,
        'mode':     actual_mode,
    }


# ==============================================================================
#  PROGRAMME BROKER SYMBOLS / TIMEFRAMES COLLECTION
# ==============================================================================
def _probe_timeframes_for_broker(available_symbols_set, sample_size=5):
    if not available_symbols_set:
        return []
    preferred = ["EURUSD", "GBPUSD", "USDJPY", "XAUUSD", "BTCUSD", "ETHUSD",
                 "US30", "NAS100", "SPX500", "AUDUSD", "USDCAD", "USDCHF"]
    probes = [s for s in preferred if s in available_symbols_set]
    for s in available_symbols_set:
        if s not in probes:
            probes.append(s)
        if len(probes) >= sample_size:
            break
    probes = probes[:sample_size]

    supported = []
    for tf_key, tf_const in TIMEFRAME_MAP.items():
        for sym in probes:
            try:
                if not mt5.symbol_select(sym, True):
                    continue
                rates = mt5.copy_rates_from_pos(sym, tf_const, 0, 5)
                if rates is not None and len(rates) > 0:
                    supported.append(tf_key)
                    break
            except Exception:
                continue
    return [tf for tf in TIMEFRAME_MAP.keys() if tf in supported]


def _collect_broker_symbols_and_timeframes(terminal_exe):
    if mt5 is None:
        return False, [], [], 'MetaTrader5 module not available'

    if not os.path.exists(terminal_exe):
        return False, [], [], f'Terminal not found: {terminal_exe}'

    _build_timeframe_map()
    if not TIMEFRAME_MAP:
        return False, [], [], 'TIMEFRAME_MAP is empty'

    with _MT5_IPC_LOCK:
        try:
            mt5.shutdown()
        except Exception:
            pass

        try:
            ok = mt5.initialize(
                path=terminal_exe,
                timeout=10000,
                portable=True,
            )
        except Exception as e:
            return False, [], [], f'initialize() raised: {e}'

        if not ok:
            return False, [], [], f'initialize() failed: {mt5.last_error()}'

        symbols_out = []
        timeframes_out = []
        err = None
        try:
            all_symbols = mt5.symbols_get()
            if not all_symbols:
                err = f'symbols_get() returned empty: {mt5.last_error()}'
            else:
                broker_symbols = [s.name for s in all_symbols]
                available_symbols_set = set(broker_symbols)
                symbols_out = sorted(broker_symbols)
                print(f"      [PRG][META] symbols_get() -> "
                      f"{len(broker_symbols)} symbol(s)")

                try:
                    supported_tfs = _probe_timeframes_for_broker(
                        available_symbols_set, sample_size=5
                    )
                    timeframes_out = supported_tfs
                    print(f"      [PRG][META] timeframe probe -> "
                          f"{len(supported_tfs)} timeframe(s): {supported_tfs}")
                except Exception as tf_err:
                    err = f'timeframe probe failed: {tf_err}'
                    print(f"      [PRG][META] {err}")
        except Exception as e:
            err = f'enumeration raised: {e}'
        finally:
            try:
                mt5.shutdown()
            except Exception:
                pass

        if err is not None and not symbols_out:
            return False, [], [], err
        return True, symbols_out, timeframes_out, err


def _persist_programme_broker_metadata(programme_id,
                                       symbols,
                                       timeframes):
    """
    CLEAR then write `broker_symbols` and `broker_timeframes` for the
    given programme row — as ONE batched bridge request.

    ALWAYS clears the columns first so every refresh is a full snapshot
    of what the broker currently offers.
    """
    if programme_id is None:
        return False

    pid = _coerce_int(programme_id)
    if pid is None:
        return False

    symbols_csv = ','.join(symbols or [])
    timeframes_json = json.dumps(timeframes or [], ensure_ascii=False)

    clear_stmt = (
        "UPDATE programme "
        "SET broker_symbols = NULL, broker_timeframes = NULL "
        f"WHERE id = {_sql_int(pid)}"
    )
    write_stmt = (
        "UPDATE programme "
        f"SET broker_symbols = {_sql_quote(symbols_csv)}, "
        f"    broker_timeframes = {_sql_quote(timeframes_json)} "
        f"WHERE id = {_sql_int(pid)}"
    )

    batch = db_query_many([clear_stmt, write_stmt], timeout=30.0, quiet=True)
    if batch.get('status') not in ('success', 'partial'):
        print(f"      [PRG][META] batched write failed for programme={pid}: "
              f"{batch.get('message')}")
        return False

    # Sanity check the two statement results
    stmts = batch.get('statements') or []
    if len(stmts) >= 1 and stmts[0].get('status') == 'error':
        print(f"      [PRG][META] clear step errored for programme={pid}: "
              f"{stmts[0].get('message')}")
        return False
    if len(stmts) >= 2 and stmts[1].get('status') == 'error':
        print(f"      [PRG][META] write step errored for programme={pid}: "
              f"{stmts[1].get('message')}")
        return False

    print(f"      [PRG][META] programme={pid} updated: "
          f"symbols={len(symbols or [])} timeframes={len(timeframes or [])}")
    return True


def _refresh_programme_broker_metadata(user_id, programme_id, terminal_exe):
    try:
        ok, symbols, timeframes, err = _collect_broker_symbols_and_timeframes(
            terminal_exe
        )
        if not ok:
            print(f"      [PRG][META] collection failed for programme="
                  f"{programme_id}: {err}")
            return False

        return _persist_programme_broker_metadata(
            programme_id=programme_id,
            symbols=symbols,
            timeframes=timeframes,
        )
    except Exception as e:
        print(f"      [PRG][META] refresh raised for programme={programme_id}: {e}")
        traceback.print_exc()
        return False


# ==============================================================================
#  [SA] harvhub mutation helpers
# ==============================================================================
def _get_broker_login_status(user_id):
    sql = "SELECT broker_login_status FROM harvhub WHERE id = %s LIMIT 1"
    res = db_query(sql, [user_id], timeout=10.0, quiet=True)
    if res.get('status') == 'success' and res.get('results'):
        row = res['results'][0]
        for v in row.values():
            if v is None:
                return None
            return str(v).strip()
    return None


def _fetch_row_for_notify(row_id, sub_account_id):
    columns = (
        "id, sub_account_id, main_account_id, email, broker, server, login, "
        "broker_password, broker_login_status, broker_login_attempt_used, "
        "broker_total_login_attempt, demo_account, disconnect_broker_request"
    )
    if row_id is not None:
        res = db_query(
            f"SELECT {columns} FROM harvhub WHERE id = %s LIMIT 1",
            [row_id], timeout=15.0, quiet=True,
        )
        if res.get('status') == 'success' and res.get('results'):
            return _normalize_row(res['results'][0])

    if sub_account_id is not None:
        res = db_query(
            f"SELECT {columns} FROM harvhub WHERE sub_account_id = %s ORDER BY id DESC LIMIT 1",
            [sub_account_id], timeout=15.0, quiet=True,
        )
        if res.get('status') == 'success' and res.get('results'):
            return _normalize_row(res['results'][0])
    return None


def _set_broker_login_status(row_id, status):
    sql = "UPDATE harvhub SET broker_login_status = %s WHERE id = %s"
    res = db_query(sql, [status, row_id], timeout=10.0, quiet=True)
    return res.get('status') == 'success'


def _increment_broker_login_attempt(user_id):
    sql = (
        "UPDATE harvhub "
        "SET broker_login_attempt_used = COALESCE(broker_login_attempt_used, 0) + 1 "
        "WHERE id = %s"
    )
    res = db_query(sql, [user_id], timeout=10.0, quiet=True)
    if res.get('status') != 'success':
        return None

    q = "SELECT broker_login_attempt_used FROM harvhub WHERE id = %s LIMIT 1"
    res2 = db_query(q, [user_id], timeout=10.0, quiet=True)
    if res2.get('status') == 'success' and res2.get('results'):
        row = res2['results'][0]
        for v in row.values():
            return _coerce_int(v)
    return None


def _reset_broker_login_attempt(row_id):
    sql = "UPDATE harvhub SET broker_login_attempt_used = NULL WHERE id = %s"
    res = db_query(sql, [row_id], timeout=10.0, quiet=True)
    return res.get('status') == 'success'


def _notify_broker_failure(row_id, sub_account_id, new_status):
    if new_status not in TERMINAL_NEGATIVE_STATUSES:
        return None

    # Batched: mutate only the selected sub-account row, not its owner row.
    uid = _sql_int(row_id)
    st  = _sql_quote(new_status)

    clear_stmt = (
        "UPDATE harvhub "
        "SET broker = NULL, server = NULL, login = NULL, broker_password = NULL "
        f"WHERE id = {uid}"
    )
    inc_stmt = (
        "UPDATE harvhub "
        "SET broker_login_attempt_used = COALESCE(broker_login_attempt_used, 0) + 1 "
        f"WHERE id = {uid}"
    )
    stat_stmt = (
        f"UPDATE harvhub SET broker_login_status = {st} WHERE id = {uid}"
    )
    read_stmt = (
        "SELECT broker_login_attempt_used FROM harvhub "
        f"WHERE id = {uid} LIMIT 1"
    )

    batch = db_query_many(
        [clear_stmt, inc_stmt, stat_stmt, read_stmt],
        timeout=20.0, quiet=True,
    )

    new_used = None
    stmts = batch.get('statements') or []
    if len(stmts) >= 4 and stmts[3].get('rows'):
        first = _normalize_row(stmts[3]['rows'][0]) or {}
        for v in first.values():
            new_used = _coerce_int(v)
            break

    row = _fetch_row_for_notify(row_id, sub_account_id)
    if row is None:
        print(f"      [SA][NOTIFY] could not re-fetch target HarvHub row id={row_id}")
        return new_used

    try:
        notif_id = notifier.record_broker_login_status_notification(_PDO, row)
        if notif_id:
            print(
                f"      [SA][NOTIFY] row={row_id} status={new_status} "
                f"-> notification_id={notif_id} "
                f"(attempts used = {new_used})"
            )
        else:
            print(
                f"      [SA][NOTIFY] row={row_id} status={new_status} "
                f"-> no notification created (dedup or ineligible state)"
            )
    except Exception as e:
        print(f"      [SA][NOTIFY] raised: {e}")
        traceback.print_exc()

    return new_used


def _notify_broker_success(row_id, sub_account_id):
    row = _fetch_row_for_notify(row_id, sub_account_id)
    if row is None:
        print(f"      [SA][NOTIFY-SUCCESS] could not re-fetch target row id={row_id}")
        return None

    try:
        notif_id = notifier.record_broker_login_success_notification(_PDO, row)
        if notif_id:
            print(
                f"      [SA][NOTIFY-SUCCESS] row={row_id} "
                f"-> notification_id={notif_id}"
            )
        else:
            print(
                f"      [SA][NOTIFY-SUCCESS] row={row_id} "
                f"-> no notification created"
            )
        return notif_id
    except Exception as e:
        print(f"      [SA][NOTIFY-SUCCESS] raised: {e}")
        traceback.print_exc()
        return None


def _notify_broker_reconnected(row_id, sub_account_id):
    row = _fetch_row_for_notify(row_id, sub_account_id)
    if row is None:
        print(f"      [SA][NOTIFY-RECONNECTED] could not re-fetch target row id={row_id}")
        return None

    try:
        notif_id = notifier.record_broker_login_reconnected_notification(_PDO, row)
        if notif_id:
            print(
                f"      [SA][NOTIFY-RECONNECTED] row={row_id} "
                f"-> notification_id={notif_id}"
            )
        else:
            print(
                f"      [SA][NOTIFY-RECONNECTED] row={row_id} "
                f"-> no notification created"
            )
        return notif_id
    except Exception as e:
        print(f"      [SA][NOTIFY-RECONNECTED] raised: {e}")
        traceback.print_exc()
        return None


def _notify_disconnect_completed(user_id, sub_account_id):
    row = _fetch_row_for_notify(user_id, sub_account_id)
    if row is None:
        print(f"      [SA][NOTIFY-DISCONNECT] could not re-fetch row for user={user_id}")
        return None

    try:
        notif_id = notifier.record_disconnect_broker_completed_notification(_PDO, row)
        if notif_id:
            print(
                f"      [SA][NOTIFY-DISCONNECT] user={user_id} "
                f"-> notification_id={notif_id}"
            )
        else:
            print(
                f"      [SA][NOTIFY-DISCONNECT] user={user_id} "
                f"-> no notification created"
            )
        return notif_id
    except Exception as e:
        print(f"      [SA][NOTIFY-DISCONNECT] raised: {e}")
        traceback.print_exc()
        return None


def _clear_disconnect_request(user_id):
    sql = "UPDATE harvhub SET disconnect_broker_request = 0 WHERE id = %s"
    res = db_query(sql, [user_id], timeout=10.0, quiet=True)
    return res.get('status') == 'success'


# ==============================================================================
#  [PRG] programme mutation helpers
# ==============================================================================
def _get_programme_broker_login_status(programme_id):
    sql = "SELECT broker_login_status FROM programme WHERE id = %s LIMIT 1"
    res = db_query(sql, [programme_id], timeout=10.0, quiet=True)
    if res.get('status') == 'success' and res.get('results'):
        row = res['results'][0]
        for v in row.values():
            if v is None:
                return None
            return str(v).strip()
    return None


def _resolve_programme_owner_email(user_id):
    if user_id is None:
        return ''
    uid = _coerce_int(user_id)
    if uid is None or uid <= 0:
        return ''

    q = db_query(
        "SELECT email FROM harvhub WHERE id = %s LIMIT 1",
        [uid], timeout=10.0, quiet=True,
    )
    if q.get('status') == 'success' and q.get('results'):
        row = q['results'][0]
        for v in row.values():
            if v is None:
                return ''
            return str(v).strip().lower()
    return ''


def _fetch_programme_row_for_notify(programme_id, user_id):
    select_cols = _programme_select_sql()

    row = None

    if programme_id is not None:
        sql = f"SELECT {select_cols} FROM programme WHERE id = %s LIMIT 1"
        res = db_query(sql, [programme_id], timeout=15.0, quiet=True)
        if res.get('status') == 'success' and res.get('results'):
            row = _normalize_row(res['results'][0])

    if row is None and user_id is not None:
        sql2 = (
            f"SELECT {select_cols} FROM programme "
            f"WHERE userid = %s ORDER BY id ASC LIMIT 1"
        )
        res2 = db_query(sql2, [user_id], timeout=15.0, quiet=True)
        if res2.get('status') == 'success' and res2.get('results'):
            row = _normalize_row(res2['results'][0])

    if row is not None:
        owner_uid = _coerce_int(row.get('userid')) or user_id
        row['email'] = _resolve_programme_owner_email(owner_uid)

    return row


def _set_programme_broker_login_status(programme_id, status):
    sql = "UPDATE programme SET broker_login_status = %s WHERE id = %s"
    res = db_query(sql, [status, programme_id], timeout=10.0, quiet=True)
    return res.get('status') == 'success'


def _increment_programme_broker_login_attempt(programme_id):
    sql = (
        "UPDATE programme "
        "SET broker_login_attempt_used = COALESCE(broker_login_attempt_used, 0) + 1 "
        "WHERE id = %s"
    )
    res = db_query(sql, [programme_id], timeout=10.0, quiet=True)
    if res.get('status') != 'success':
        return None

    q = "SELECT broker_login_attempt_used FROM programme WHERE id = %s LIMIT 1"
    res2 = db_query(q, [programme_id], timeout=10.0, quiet=True)
    if res2.get('status') == 'success' and res2.get('results'):
        row = res2['results'][0]
        for v in row.values():
            return _coerce_int(v)
    return None


def _reset_programme_broker_login_attempt(programme_id):
    sql = "UPDATE programme SET broker_login_attempt_used = NULL WHERE id = %s"
    res = db_query(sql, [programme_id], timeout=10.0, quiet=True)
    return res.get('status') == 'success'


def _notify_programme_broker_failure(programme_id, user_id, new_status):
    if new_status not in PROGRAMME_NEGATIVE_STATUSES:
        return None

    pid = _sql_int(programme_id)
    st  = _sql_quote(new_status)

    clear_stmt = (
        "UPDATE programme "
        "SET broker = NULL, server = NULL, login = NULL, broker_password = NULL "
        f"WHERE id = {pid}"
    )
    inc_stmt = (
        "UPDATE programme "
        "SET broker_login_attempt_used = COALESCE(broker_login_attempt_used, 0) + 1 "
        f"WHERE id = {pid}"
    )
    stat_stmt = (
        f"UPDATE programme SET broker_login_status = {st} WHERE id = {pid}"
    )
    read_stmt = (
        "SELECT broker_login_attempt_used FROM programme "
        f"WHERE id = {pid} LIMIT 1"
    )

    batch = db_query_many(
        [clear_stmt, inc_stmt, stat_stmt, read_stmt],
        timeout=20.0, quiet=True,
    )

    new_used = None
    stmts = batch.get('statements') or []
    if len(stmts) >= 4 and stmts[3].get('rows'):
        first = _normalize_row(stmts[3]['rows'][0]) or {}
        for v in first.values():
            new_used = _coerce_int(v)
            break

    row = _fetch_programme_row_for_notify(programme_id, user_id)
    print(f"      [PRG][NOTIFY-DEBUG] fetched row before dispatch = {row!r}")
    if row is None:
        print(f"      [PRG][NOTIFY] could not re-fetch row for programme={programme_id}")
        return new_used

    try:
        notif_id = notifier.record_programme_broker_login_status_notification(_PDO, row)
        if notif_id:
            print(
                f"      [PRG][NOTIFY] programme={programme_id} status={new_status} "
                f"-> notification_id={notif_id} "
                f"(attempts used = {new_used})"
            )
        else:
            print(
                f"      [PRG][NOTIFY] programme={programme_id} status={new_status} "
                f"-> no notification created (dedup or ineligible state)"
            )
    except Exception as e:
        print(f"      [PRG][NOTIFY] raised: {e}")
        traceback.print_exc()

    return new_used


def _notify_programme_broker_success(programme_id, user_id):
    row = _fetch_programme_row_for_notify(programme_id, user_id)
    if row is None:
        print(f"      [PRG][NOTIFY-SUCCESS] could not re-fetch row for programme={programme_id}")
        return None

    try:
        notif_id = notifier.record_programme_broker_login_success_notification(_PDO, row)
        if notif_id:
            print(
                f"      [PRG][NOTIFY-SUCCESS] programme={programme_id} "
                f"-> notification_id={notif_id}"
            )
        else:
            print(
                f"      [PRG][NOTIFY-SUCCESS] programme={programme_id} "
                f"-> no notification created"
            )
        return notif_id
    except Exception as e:
        print(f"      [PRG][NOTIFY-SUCCESS] raised: {e}")
        traceback.print_exc()
        return None


def _notify_programme_broker_reconnected(programme_id, user_id):
    row = _fetch_programme_row_for_notify(programme_id, user_id)
    if row is None:
        print(f"      [PRG][NOTIFY-RECONNECTED] could not re-fetch row for programme={programme_id}")
        return None

    try:
        notif_id = notifier.record_programme_broker_login_reconnected_notification(_PDO, row)
        if notif_id:
            print(
                f"      [PRG][NOTIFY-RECONNECTED] programme={programme_id} "
                f"-> notification_id={notif_id}"
            )
        else:
            print(
                f"      [PRG][NOTIFY-RECONNECTED] programme={programme_id} "
                f"-> no notification created"
            )
        return notif_id
    except Exception as e:
        print(f"      [PRG][NOTIFY-RECONNECTED] raised: {e}")
        traceback.print_exc()
        return None


def _notify_programme_disconnect_completed(programme_id, user_id):
    row = _fetch_programme_row_for_notify(programme_id, user_id)
    if row is None:
        print(f"      [PRG][NOTIFY-DISCONNECT] could not re-fetch row for programme={programme_id}")
        return None

    try:
        notif_id = notifier.record_programme_disconnect_broker_completed_notification(_PDO, row)
        if notif_id:
            print(
                f"      [PRG][NOTIFY-DISCONNECT] programme={programme_id} "
                f"-> notification_id={notif_id}"
            )
        else:
            print(
                f"      [PRG][NOTIFY-DISCONNECT] programme={programme_id} "
                f"-> no notification created"
            )
        return notif_id
    except Exception as e:
        print(f"      [PRG][NOTIFY-DISCONNECT] raised: {e}")
        traceback.print_exc()
        return None


def _clear_programme_disconnect_request(programme_id):
    sql = "UPDATE programme SET disconnect_broker_request = 0 WHERE id = %s"
    res = db_query(sql, [programme_id], timeout=10.0, quiet=True)
    return res.get('status') == 'success'


# ==============================================================================
#  [SA] DISCONNECT-REQUEST PASS
# ==============================================================================
def _process_disconnect_requests():
    completed = 0

    sql = (
        "SELECT id, sub_account_id, main_account_id, email, "
        "server, login, disconnect_broker_request "
        "FROM harvhub "
        "WHERE disconnect_broker_request = 1"
    )
    res = db_query(sql, None, timeout=15.0, quiet=True)
    if not _is_ok(res):
        return 0

    rows = res.get('results') or []
    if not rows:
        return 0

    print(f"   [SA][DISCONNECT] pending rows: {len(rows)}")

    for raw in rows:
        row = _normalize_row(raw)

        user_id        = _coerce_int(row.get('id'))
        sub_account_id = _coerce_int(row.get('sub_account_id')) or user_id

        if not user_id or not sub_account_id:
            continue

        print(f"\n   [SA][DISCONNECT] user={user_id} sub={sub_account_id}")

        folders = find_all_folders_for_sub_account(user_id, sub_account_id)

        if not folders:
            print(
                f"   [SA][DISCONNECT] no folders matching "
                f"'*_U{user_id}_SA{sub_account_id}' found; nothing to delete"
            )
        else:
            print(
                f"   [SA][DISCONNECT] matched {len(folders)} folder(s) "
                f"for user={user_id} sub={sub_account_id}"
            )

            all_ok = True
            for folder_abs in folders:
                print(f"   [SA][DISCONNECT] processing: {folder_abs}")
                ok = force_delete_folder(folder_abs)
                if not ok:
                    all_ok = False
                    print(f"   [SA][DISCONNECT] failed to delete: {folder_abs}")

            if not all_ok:
                print(
                    "   [SA][DISCONNECT] one or more folders could not be "
                    "deleted; flag left as-is for next pass"
                )
                continue

        _clear_disconnect_request(user_id)
        _notify_disconnect_completed(user_id, sub_account_id)
        completed += 1

    return completed


# ==============================================================================
#  [PRG] DISCONNECT-REQUEST PASS
# ==============================================================================
def _process_programme_disconnect_requests():
    completed = 0

    sql = (
        "SELECT id, userid, program_name, "
        "server, login, disconnect_broker_request "
        "FROM programme "
        "WHERE disconnect_broker_request = 1"
    )
    res = db_query(sql, None, timeout=15.0, quiet=True)
    if not _is_ok(res):
        return 0

    rows = res.get('results') or []
    if not rows:
        return 0

    print(f"   [PRG][DISCONNECT] pending rows: {len(rows)}")

    for raw in rows:
        row = _normalize_row(raw)

        programme_id = _coerce_int(row.get('id'))
        user_id      = _coerce_int(row.get('userid'))

        if not programme_id or not user_id:
            continue

        print(f"\n   [PRG][DISCONNECT] programme={programme_id} user={user_id}")

        folders = find_all_folders_for_programme(user_id, programme_id)

        if not folders:
            print(
                f"   [PRG][DISCONNECT] no folders matching "
                f"'*_U{user_id}_P{programme_id}' found; nothing to delete"
            )
        else:
            print(
                f"   [PRG][DISCONNECT] matched {len(folders)} folder(s) "
                f"for programme={programme_id} user={user_id}"
            )

            all_ok = True
            for folder_abs in folders:
                print(f"   [PRG][DISCONNECT] processing: {folder_abs}")
                ok = force_delete_folder(folder_abs)
                if not ok:
                    all_ok = False
                    print(f"   [PRG][DISCONNECT] failed to delete: {folder_abs}")

            if not all_ok:
                print(
                    "   [PRG][DISCONNECT] one or more folders could not be "
                    "deleted; flag left as-is for next pass"
                )
                continue

        _clear_programme_disconnect_request(programme_id)
        _notify_programme_disconnect_completed(programme_id, user_id)
        completed += 1

    return completed


# ==============================================================================
#  [SHARED UI] Manual login requests are rendered inside HarvHub AI.
# ==============================================================================
class _ManualLoginWindow:
    """IPC-backed queue; no separate Tk window is created by this service."""

    def __init__(self):
        self._lock = threading.RLock()
        self._sa_items = {}
        self._prg_items = {}

    @staticmethod
    def _key(kind, owner_id, account_id):
        return (str(kind).upper(), str(owner_id), str(account_id))

    def _send(self, payload):
        if manager_request is None:
            print("      [MANUAL][UI ERROR] HarvHub manager IPC helper is unavailable; manual-login request cannot be shown in the manager.", flush=True)
            return None

        operation = str(payload.get("op") or "").strip().lower()
        # Upsert/remove are idempotent, so retry briefly if the manager's local
        # IPC listener is still binding or momentarily busy. A decision read is
        # deliberately not retried because it consumes the queued decision.
        attempts = 3 if operation in {"manual_login_upsert", "manual_login_remove"} else 1
        response = None
        for attempt in range(attempts):
            response = manager_request(payload, timeout=2.0)
            if isinstance(response, dict) and response.get("ok") is True:
                return response
            if attempt + 1 < attempts:
                time.sleep(0.25 * (attempt + 1))

        item = payload.get("item") if isinstance(payload.get("item"), dict) else {}
        kind = str(item.get("kind", payload.get("kind", "SA"))).upper()
        owner = str(item.get("owner_id", payload.get("owner_id", "")))
        account = str(item.get("account_id", payload.get("account_id", "")))
        print(
            f"      [MANUAL][UI ERROR] Manager did not acknowledge {operation or 'manual-login'} "
            f"for {kind} {account} (owner {owner}); check that HarvHub AI is open and its local control listener is available.",
            flush=True,
        )
        return response

    def add_item(self, kind, owner_id, account_id, broker, server,
                 login, password, terminal_exe):
        kind = str(kind).upper()
        item = {
            "kind": kind,
            "owner_id": str(owner_id),
            "account_id": str(account_id),
            "broker": broker or "N/A",
            "server": server or "N/A",
            "login": str(login or ""),
            "password": str(password or ""),
            "terminal_exe": str(terminal_exe or ""),
        }
        key = self._key(kind, owner_id, account_id)
        store = self._sa_items if kind == "SA" else self._prg_items
        with self._lock:
            store[key] = item
        response = self._send({"op": "manual_login_upsert", "item": item})
        if isinstance(response, dict) and response.get("ok") is True:
            print(
                f"      [MANUAL][UI] {kind} {account_id} owner={owner_id} added to HarvHub's manual-login attention list.",
                flush=True,
            )

    def remove_item(self, kind, owner_id, account_id):
        kind = str(kind).upper()
        key = self._key(kind, owner_id, account_id)
        store = self._sa_items if kind == "SA" else self._prg_items
        with self._lock:
            existed = store.pop(key, None) is not None
        self._send({
            "op": "manual_login_remove",
            "kind": kind, "owner_id": str(owner_id), "account_id": str(account_id),
        })
        return existed

    def take_decision(self, kind, owner_id, account_id):
        kind = str(kind).upper()
        key = self._key(kind, owner_id, account_id)
        response = self._send({
            "op": "manual_login_take_decision",
            "kind": kind, "owner_id": str(owner_id), "account_id": str(account_id),
        }) or {}
        decision = response.get("decision")
        if decision in ("valid", "invalid"):
            return decision
        return None

    def has_pending_items(self):
        with self._lock:
            return bool(self._sa_items or self._prg_items)

    def stop(self):
        # No independent UI/event loop is owned by this service any longer.
        return None


_MANUAL_WINDOW = _ManualLoginWindow()


# ==============================================================================
#  [SA] / [PRG] row fetchers
# ==============================================================================
def _fetch_broker_row(user_id, sub_account_id):
    """Fetch the exact HarvHub row for this SA target.

    VPS bindings carry both the owner/user id and the selected sub-account id.
    The selected sub-account must be resolved first; querying id=user_id first
    silently returned the owner's credentials for every child sub-account.
    """
    select_cols = _broker_select_sql()
    uid = _coerce_int(user_id)
    sid = _coerce_int(sub_account_id)

    if sid is not None and uid is not None and sid != uid:
        # Preferred relationship: the child's sub_account_id plus its owner.
        sql = (
            f"SELECT {select_cols} FROM harvhub "
            "WHERE sub_account_id = %s AND main_account_id = %s "
            "ORDER BY id DESC LIMIT 1"
        )
        res = db_query(sql, [sid, uid], timeout=15.0, quiet=True)
        if res.get('status') == 'success' and res.get('results'):
            row = _normalize_row(res['results'][0])
            print(f"      [SA][ROW] resolved exact child by sub_account_id={sid}, owner={uid}, row_id={row.get('id')}")
            return row

        # Some installations do not populate main_account_id consistently.
        sql = f"SELECT {select_cols} FROM harvhub WHERE sub_account_id = %s ORDER BY id DESC LIMIT 1"
        res = db_query(sql, [sid], timeout=15.0, quiet=True)
        if res.get('status') == 'success' and res.get('results'):
            row = _normalize_row(res['results'][0])
            row_owner = _coerce_int(_pick_first_key(row, 'main_account_id'))
            if row_owner in (None, uid):
                print(f"      [SA][ROW] resolved child by sub_account_id={sid}, row_id={row.get('id')}")
                return row
            print(f"      [SA][ROW] rejected sub_account_id={sid} because row owner={row_owner}, expected owner={uid}")

        # Compatibility with VPS rows which store the HarvHub child primary key.
        sql = f"SELECT {select_cols} FROM harvhub WHERE id = %s LIMIT 1"
        res = db_query(sql, [sid], timeout=15.0, quiet=True)
        if res.get('status') == 'success' and res.get('results'):
            row = _normalize_row(res['results'][0])
            row_owner = _coerce_int(_pick_first_key(row, 'main_account_id'))
            is_main = _coerce_int(_pick_first_key(row, 'is_main_account'))
            if row_owner in (None, uid) and is_main != 1:
                print(f"      [SA][ROW] resolved child by primary key={sid}, owner={uid}")
                return row

        print(f"      [SA][ROW ERROR] no exact HarvHub sub-account row found for owner={uid}, sub={sid}; refusing to use the owner's row")
        return None

    if uid is not None:
        sql = f"SELECT {select_cols} FROM harvhub WHERE id = %s LIMIT 1"
        res = db_query(sql, [uid], timeout=15.0, quiet=True)
        if res.get('status') == 'success' and res.get('results'):
            return _normalize_row(res['results'][0])

    return None


def _fetch_programme_row(programme_id, user_id):
    select_cols = _programme_select_sql()

    row = None

    if programme_id is not None:
        sql = f"SELECT {select_cols} FROM programme WHERE id = %s LIMIT 1"
        res = db_query(sql, [programme_id], timeout=15.0, quiet=True)
        if res.get('status') == 'success' and res.get('results'):
            row = _normalize_row(res['results'][0])

    if row is None and user_id is not None:
        sql = (f"SELECT {select_cols} FROM programme "
               f"WHERE userid = %s ORDER BY id ASC LIMIT 1")
        res = db_query(sql, [user_id], timeout=15.0, quiet=True)
        if res.get('status') == 'success' and res.get('results'):
            row = _normalize_row(res['results'][0])

    if row is not None:
        owner_uid = _coerce_int(row.get('userid')) or user_id
        row['email'] = _resolve_programme_owner_email(owner_uid)

    return row


# ==============================================================================
#  [SA] SUB-ACCOUNT TARGET PROCESSING
# ==============================================================================
def _process_target(row, role, user_id, sub_account_id):
    result = {
        'source':         'SA',
        'role':           role,
        'user_id':        user_id,
        'sub_account_id': sub_account_id,
        'ok':             False,
    }

    account_row_id = _coerce_int(_pick_first_key(row, 'id')) or user_id
    login         = _pick_first_key(row, 'login')
    server        = _pick_first_key(row, 'server') or ''
    broker        = _pick_first_key(row, 'broker') or ''
    password      = _pick_first_key(row, 'broker_password') or ''
    login_int     = _coerce_int(login)
    manual_status = (_pick_first_key(row, 'broker_login_status') or '').strip().lower()
    expected_name = _expected_fullname_from_row(row)
    demo_int      = _coerce_int(_pick_first_key(row, 'demo_account')) or 0

    already_notified = (manual_status == VALID_STATUS)

    if manual_status in TERMINAL_NEGATIVE_STATUSES:
        print(f"      [SA][SKIP] sub={sub_account_id} — {manual_status}")
        result['reason'] = manual_status
        return result

    if login_int is None or not server:
        print(f"      [SA][SKIP] sub={sub_account_id} — missing login/server")
        result['reason'] = 'missing login or server'
        return result

    ok_folder, folder_abs, terminal_exe, msg_folder = ensure_mt5_folder(
        server=server, login=login_int,
        user_id=user_id, kind='SA', account_id=sub_account_id,
    )
    print(f"      [SA][FOLDER] {msg_folder}")

    if not ok_folder:
        result['reason'] = f'folder: {msg_folder}'
        return result

    # Consume the user's decision BEFORE any launch attempt. The terminal is
    # already open for manual login; launching first on the decision cycle can
    # create the very duplicate window the Valid button is meant to finish.
    queued = _MANUAL_WINDOW.take_decision('SA', user_id, sub_account_id)
    if queued == 'valid':
        print(f"      [SA][UI-DECISION] sub={sub_account_id} -> valid (queued); no terminal launch will be attempted")
        reset_ok = _reset_broker_login_attempt(account_row_id)
        status_ok = _set_broker_login_status(account_row_id, VALID_STATUS)
        if not status_ok:
            print(f"      [SA][UI-DECISION][DB ERROR] could not persist valid_credentials for HarvHub row id={account_row_id}; keeping the manual-login item available for retry")
            _MANUAL_WINDOW.add_item(
                kind='SA', owner_id=user_id, account_id=sub_account_id,
                broker=broker, server=server, login=login_int,
                password=password, terminal_exe=terminal_exe,
            )
            result['reason'] = 'valid_status_write_failed'
            return result
        if not reset_ok:
            print(f"      [SA][UI-DECISION][WARN] could not reset login-attempt counter for row id={account_row_id}")
        _notify_broker_success(account_row_id, sub_account_id)
        _mark_terminal_live('SA', user_id, sub_account_id)
        _MANUAL_WINDOW.remove_item('SA', user_id, sub_account_id)
        result['ok'] = True
        result['reason'] = 'ui_valid'
        result['login'] = login_int
        result['server'] = server
        result['broker'] = broker
        result['folder'] = folder_abs
        result['terminal_exe'] = terminal_exe
        return result
    elif queued == 'invalid':
        print(f"      [SA][UI-DECISION] sub={sub_account_id} -> invalid (queued)")
        _notify_broker_failure(
            row_id=account_row_id,
            sub_account_id=sub_account_id,
            new_status='invalid_credentials',
        )
        _MANUAL_WINDOW.remove_item('SA', user_id, sub_account_id)
        result['reason'] = 'invalid_credentials'
        result['login'] = login_int
        result['server'] = server
        result['broker'] = broker
        result['folder'] = folder_abs
        result['terminal_exe'] = terminal_exe
        return result

    was_running_before = _terminal_running_for_folder(folder_abs)
    launched_now, ok_launch = launch_terminal(terminal_exe, folder_abs)
    if not ok_launch:
        result['reason'] = 'launch failed'
        return result

    time.sleep(TERMINAL_BOOT_WAIT)
    probe_state, probe_info = probe_terminal_account(terminal_exe, folder_abs)

    if probe_state == 'logged_in':
        term_login  = probe_info.get('login')
        term_server = probe_info.get('server')
        term_name   = probe_info.get('name') or ''

        if str(term_login) == str(login_int):
            if _names_match(expected_name, term_name):
                if demo_int == 0:
                    print(f"      [SA][INVALID-DEMO] sub={sub_account_id} | "
                          f"demo_account=0 but terminal is a demo "
                          f"({term_name}) — treating as invalid_account")

                    new_used = _notify_broker_failure(
                        row_id=account_row_id,
                        sub_account_id=sub_account_id,
                        new_status='invalid_account',
                    )
                    print(f"      [SA][ATTEMPT] broker_login_attempt_used -> "
                          f"{new_used if new_used is not None else '?'}")

                    delete_mt5_folder(folder_abs, tag='SA')
                    _MANUAL_WINDOW.remove_item('SA', user_id, sub_account_id)

                    result['reason'] = 'invalid_account'
                    result['login'] = term_login
                    result['server'] = term_server or server
                    result['broker'] = broker
                    result['folder'] = folder_abs
                    result['terminal_exe'] = terminal_exe
                    return result

                if already_notified:
                    was_dark = _was_terminal_dark('SA', user_id, sub_account_id)

                    if was_dark or launched_now or not was_running_before:
                        print(f"      [SA][RECONNECTED] sub={sub_account_id} | "
                              f"DB valid + terminal went dark and is now "
                              f"live (was_dark={was_dark}, "
                              f"launched_now={launched_now}) — "
                              f"firing reconnect email")

                        _notify_broker_reconnected(account_row_id, sub_account_id)

                        _mark_terminal_live('SA', user_id, sub_account_id)
                        _MANUAL_WINDOW.remove_item('SA', user_id, sub_account_id)

                        try:
                            verify_res = _verify_and_persist_balance(
                                user_id=user_id,
                                sub_account_id=sub_account_id,
                                terminal_exe=terminal_exe,
                                row=row,
                            )
                            result['balance_verification'] = verify_res
                        except Exception as ve:
                            print(f"      [SA][VERIFY] raised: {ve}")
                            traceback.print_exc()

                        result['ok'] = True
                        result['reason'] = 'reconnected'
                        result['login'] = term_login
                        result['server'] = term_server or server
                        result['broker'] = broker
                        result['folder'] = folder_abs
                        result['terminal_exe'] = terminal_exe
                        result.update(probe_info or {})
                        return result

                    print(f"      [SA][OK-NO-EMAIL] sub={sub_account_id} | "
                          f"still logged in as {term_login} @ {term_server} "
                          f"| name='{term_name}' matches "
                          f"'{expected_name}'; already live, silent keep")

                    _mark_terminal_live('SA', user_id, sub_account_id)
                    _MANUAL_WINDOW.remove_item('SA', user_id, sub_account_id)

                    try:
                        verify_res = _verify_and_persist_balance(
                            user_id=user_id,
                            sub_account_id=sub_account_id,
                            terminal_exe=terminal_exe,
                            row=row,
                        )
                        result['balance_verification'] = verify_res
                    except Exception as ve:
                        print(f"      [SA][VERIFY] raised: {ve}")
                        traceback.print_exc()

                    result['ok'] = True
                    result['reason'] = 'already_notified'
                    result['login'] = term_login
                    result['server'] = term_server or server
                    result['broker'] = broker
                    result['folder'] = folder_abs
                    result['terminal_exe'] = terminal_exe
                    result.update(probe_info or {})
                    return result

                print(f"      [SA][OK-NEW] sub={sub_account_id} | logged in as "
                      f"{term_login} @ {term_server} | name='{term_name}' "
                      f"matches '{expected_name}' | demo_account={demo_int} "
                      f"— firing success email")

                _reset_broker_login_attempt(account_row_id)
                _set_broker_login_status(account_row_id, VALID_STATUS)
                _notify_broker_success(account_row_id, sub_account_id)
                _mark_terminal_live('SA', user_id, sub_account_id)

                result['ok'] = True
                result['reason'] = 'newly_valid'
                result['login'] = term_login
                result['server'] = term_server or server
                result['broker'] = broker
                result['folder'] = folder_abs
                result['terminal_exe'] = terminal_exe
                result.update(probe_info or {})
                _MANUAL_WINDOW.remove_item('SA', user_id, sub_account_id)
                return result

            print(f"      [SA][INVALID-FULLNAME] sub={sub_account_id} | terminal "
                  f"name='{term_name}' != expected '{expected_name}'")

            new_used = _notify_broker_failure(
                row_id=account_row_id,
                sub_account_id=sub_account_id,
                new_status='fullname_mismatch',
            )
            print(f"      [SA][ATTEMPT] broker_login_attempt_used -> "
                  f"{new_used if new_used is not None else '?'}")

            delete_mt5_folder(folder_abs, tag='SA')
            _MANUAL_WINDOW.remove_item('SA', user_id, sub_account_id)

            result['reason'] = 'fullname_mismatch'
            result['login'] = term_login
            result['server'] = term_server or server
            result['broker'] = broker
            result['folder'] = folder_abs
            result['terminal_exe'] = terminal_exe
            result['fullname_mismatch'] = {
                'expected': expected_name,
                'actual':   term_name,
            }
            return result

        print(f"      [SA][MANUAL] sub={sub_account_id} | terminal logged in as "
              f"{term_login} @ {term_server}, expected {login_int} @ {server} "
              f"— needs manual fix")

    elif probe_state == 'not_logged_in':
        print(f"      [SA][MANUAL] sub={sub_account_id} | terminal has no session "
              f"— needs manual login")

    else:
        print(f"      [SA][MANUAL] sub={sub_account_id} | probe unavailable "
              f"({probe_info.get('error')}) — needs manual login")

    if already_notified:
        _mark_terminal_dark('SA', user_id, sub_account_id)
        print(f"      [SA][DARK] sub={sub_account_id} | DB valid but terminal "
              f"unreachable — marked DARK, will notify on next successful probe")

    _MANUAL_WINDOW.add_item(
        kind='SA',
        owner_id=user_id,
        account_id=sub_account_id,
        broker=broker,
        server=server,
        login=login_int,
        password=password,
        terminal_exe=terminal_exe,
    )
    result['reason'] = 'awaiting_manual_login'
    return result


# ==============================================================================
#  [PRG] PROGRAMME TARGET PROCESSING
# ==============================================================================
def _process_programme_target(row, user_id, programme_id):
    result = {
        'source':       'PRG',
        'role':         'programme',
        'user_id':      user_id,
        'programme_id': programme_id,
        'ok':           False,
    }

    login         = _pick_first_key(row, 'login')
    server        = _pick_first_key(row, 'server') or ''
    broker        = _pick_first_key(row, 'broker') or ''
    password      = _pick_first_key(row, 'broker_password') or ''
    login_int     = _coerce_int(login)
    manual_status = (_pick_first_key(row, 'broker_login_status') or '').strip().lower()
    expected_name = _expected_fullname_from_row(row)
    demo_int      = _coerce_int(_pick_first_key(row, 'demo_account')) or 0

    already_notified = (manual_status == VALID_STATUS)

    if manual_status in PROGRAMME_NEGATIVE_STATUSES:
        print(f"      [PRG][SKIP] programme={programme_id} — {manual_status}")
        result['reason'] = manual_status
        return result

    if login_int is None or not server:
        print(f"      [PRG][SKIP] programme={programme_id} — missing login/server")
        result['reason'] = 'missing login or server'
        return result

    ok_folder, folder_abs, terminal_exe, msg_folder = ensure_mt5_folder(
        server=server, login=login_int,
        user_id=user_id, kind='P', account_id=programme_id,
    )
    print(f"      [PRG][FOLDER] {msg_folder}")

    if not ok_folder:
        result['reason'] = f'folder: {msg_folder}'
        return result

    was_running_before = _terminal_running_for_folder(folder_abs)
    launched_now, ok_launch = launch_terminal(terminal_exe, folder_abs)
    if not ok_launch:
        result['reason'] = 'launch failed'
        return result

    queued = _MANUAL_WINDOW.take_decision('PRG', user_id, programme_id)
    if queued == 'valid':
        print(f"      [PRG][UI-DECISION] programme={programme_id} -> valid (queued)")
        _reset_programme_broker_login_attempt(programme_id)
        _set_programme_broker_login_status(programme_id, VALID_STATUS)
        _notify_programme_broker_success(programme_id, user_id)
        _mark_terminal_live('PRG', user_id, programme_id)
        _MANUAL_WINDOW.remove_item('PRG', user_id, programme_id)
        result['ok'] = True
        result['reason'] = 'ui_valid'
        result['login'] = login_int
        result['server'] = server
        result['broker'] = broker
        result['folder'] = folder_abs
        result['terminal_exe'] = terminal_exe

        try:
            _refresh_programme_broker_metadata(
                user_id=user_id,
                programme_id=programme_id,
                terminal_exe=terminal_exe,
            )
        except Exception as me:
            print(f"      [PRG][META] ui_valid refresh raised: {me}")
            traceback.print_exc()

        return result
    elif queued == 'invalid':
        print(f"      [PRG][UI-DECISION] programme={programme_id} -> invalid (queued)")
        _notify_programme_broker_failure(
            programme_id=programme_id,
            user_id=user_id,
            new_status='invalid_credentials',
        )
        _MANUAL_WINDOW.remove_item('PRG', user_id, programme_id)
        result['reason'] = 'invalid_credentials'
        result['login'] = login_int
        result['server'] = server
        result['broker'] = broker
        result['folder'] = folder_abs
        result['terminal_exe'] = terminal_exe
        return result

    time.sleep(TERMINAL_BOOT_WAIT)
    probe_state, probe_info = probe_terminal_account(terminal_exe, folder_abs)

    if probe_state == 'logged_in':
        term_login  = probe_info.get('login')
        term_server = probe_info.get('server')
        term_name   = probe_info.get('name') or ''

        if str(term_login) == str(login_int):
            if _names_match(expected_name, term_name):
                if already_notified:
                    was_dark = _was_terminal_dark('PRG', user_id, programme_id)

                    if was_dark or launched_now or not was_running_before:
                        print(f"      [PRG][RECONNECTED] programme={programme_id} | "
                              f"DB valid + terminal went dark and is now "
                              f"live (was_dark={was_dark}, "
                              f"launched_now={launched_now}) — "
                              f"firing reconnect email")

                        _notify_programme_broker_reconnected(programme_id, user_id)

                        _mark_terminal_live('PRG', user_id, programme_id)
                        _MANUAL_WINDOW.remove_item('PRG', user_id, programme_id)

                        result['ok'] = True
                        result['reason'] = 'reconnected'
                        result['login'] = term_login
                        result['server'] = term_server or server
                        result['broker'] = broker
                        result['folder'] = folder_abs
                        result['terminal_exe'] = terminal_exe
                        result['demo_account'] = demo_int
                        result.update(probe_info or {})

                        try:
                            _refresh_programme_broker_metadata(
                                user_id=user_id,
                                programme_id=programme_id,
                                terminal_exe=terminal_exe,
                            )
                        except Exception as me:
                            print(f"      [PRG][META] reconnected refresh raised: {me}")
                            traceback.print_exc()

                        return result

                    print(f"      [PRG][OK-NO-EMAIL] programme={programme_id} | "
                          f"still logged in as {term_login} @ {term_server} "
                          f"| name='{term_name}' matches "
                          f"'{expected_name}' | demo_account={demo_int} "
                          f"(allowed); already live, silent keep")

                    _mark_terminal_live('PRG', user_id, programme_id)
                    _MANUAL_WINDOW.remove_item('PRG', user_id, programme_id)
                    result['ok'] = True
                    result['reason'] = 'already_notified'
                    result['login'] = term_login
                    result['server'] = term_server or server
                    result['broker'] = broker
                    result['folder'] = folder_abs
                    result['terminal_exe'] = terminal_exe
                    result['demo_account'] = demo_int
                    result.update(probe_info or {})

                    try:
                        _refresh_programme_broker_metadata(
                            user_id=user_id,
                            programme_id=programme_id,
                            terminal_exe=terminal_exe,
                        )
                    except Exception as me:
                        print(f"      [PRG][META] silent-keep refresh raised: {me}")
                        traceback.print_exc()

                    return result

                print(f"      [PRG][OK-NEW] programme={programme_id} | logged in as "
                      f"{term_login} @ {term_server} | name='{term_name}' "
                      f"matches '{expected_name}' | demo_account={demo_int} "
                      f"(allowed) — firing success email")

                _reset_programme_broker_login_attempt(programme_id)
                _set_programme_broker_login_status(programme_id, VALID_STATUS)
                _notify_programme_broker_success(programme_id, user_id)
                _mark_terminal_live('PRG', user_id, programme_id)

                result['ok'] = True
                result['reason'] = 'newly_valid'
                result['login'] = term_login
                result['server'] = term_server or server
                result['broker'] = broker
                result['folder'] = folder_abs
                result['terminal_exe'] = terminal_exe
                result['demo_account'] = demo_int
                result.update(probe_info or {})
                _MANUAL_WINDOW.remove_item('PRG', user_id, programme_id)

                try:
                    _refresh_programme_broker_metadata(
                        user_id=user_id,
                        programme_id=programme_id,
                        terminal_exe=terminal_exe,
                    )
                except Exception as me:
                    print(f"      [PRG][META] newly-valid refresh raised: {me}")
                    traceback.print_exc()

                return result

            print(f"      [PRG][INVALID-FULLNAME] programme={programme_id} | "
                  f"terminal name='{term_name}' != expected '{expected_name}'")

            new_used = _notify_programme_broker_failure(
                programme_id=programme_id,
                user_id=user_id,
                new_status='fullname_mismatch',
            )
            print(f"      [PRG][ATTEMPT] broker_login_attempt_used -> "
                  f"{new_used if new_used is not None else '?'}")

            delete_mt5_folder(folder_abs, tag='PRG')
            _MANUAL_WINDOW.remove_item('PRG', user_id, programme_id)

            result['reason'] = 'fullname_mismatch'
            result['login'] = term_login
            result['server'] = term_server or server
            result['broker'] = broker
            result['folder'] = folder_abs
            result['terminal_exe'] = terminal_exe
            result['fullname_mismatch'] = {
                'expected': expected_name,
                'actual':   term_name,
            }
            return result

        print(f"      [PRG][MANUAL] programme={programme_id} | terminal logged in as "
              f"{term_login} @ {term_server}, expected {login_int} @ {server} "
              f"— needs manual fix")

    elif probe_state == 'not_logged_in':
        print(f"      [PRG][MANUAL] programme={programme_id} | terminal has no session "
              f"— needs manual login")

    else:
        print(f"      [PRG][MANUAL] programme={programme_id} | probe unavailable "
              f"({probe_info.get('error')}) — needs manual login")

    if already_notified:
        _mark_terminal_dark('PRG', user_id, programme_id)
        print(f"      [PRG][DARK] programme={programme_id} | DB valid but "
              f"terminal unreachable — marked DARK, will notify on next "
              f"successful probe")

    _MANUAL_WINDOW.add_item(
        kind='PRG',
        owner_id=user_id,
        account_id=programme_id,
        broker=broker,
        server=server,
        login=login_int,
        password=password,
        terminal_exe=terminal_exe,
    )
    result['reason'] = 'awaiting_manual_login'
    return result


# ==============================================================================
#  [PRG] programme discovery
# ==============================================================================
def _fetch_programmes_for_owner(user_id):
    select_cols = _programme_select_sql()
    sql = f"SELECT {select_cols} FROM programme WHERE userid = %s"
    res = db_query(sql, [user_id], timeout=15.0, quiet=True)
    if res.get('status') != 'success':
        return []
    out = []
    for raw in (res.get('results') or []):
        row = _normalize_row(raw)
        if row:
            out.append(row)
    return out


# ==============================================================================
#  [SA] PASS — vps table  (BATCHED follower lookup)
# ==============================================================================
def _collect_sa_candidates_for_ip(local_ip):
    owners    = []
    followers = []
    seen_owner_targets = set()
    seen_follower_targets = set()

    vps_sql = (
        "SELECT id, user_id, sub_account_id, programme_id, vps_ip_address "
        "FROM vps WHERE vps_ip_address = %s"
    )
    vps_res = db_query(vps_sql, [local_ip], timeout=15.0)
    if vps_res.get('status') != 'success':
        print(f"   [SA][ERROR] vps lookup failed: {vps_res.get('message')}")
        return owners, followers

    vps_rows = vps_res.get('results', []) or []
    print(f"   [SA][VPS] {len(vps_rows)} vps row(s) for IP {local_ip}")

    for row in vps_rows:
        uid   = _coerce_int(_pick_first_key(row, 'user_id', 'userid', 'uid'))
        sub   = _coerce_int(_pick_first_key(row, 'sub_account_id', 'subaccountid'))
        vpsid = _coerce_int(_pick_first_key(row, 'id', 'vps_id'))
        prog  = _coerce_int(_pick_first_key(row, 'programme_id', 'programmeid'))

        if uid is None:
            continue

        if prog is not None and prog > 0:
            continue

        sub_norm = sub if (sub is not None and sub > 0) else uid
        identity = (uid, sub_norm)
        if identity in seen_owner_targets:
            print(f"   [SA][OWNER][DEDUP] duplicate VPS binding ignored: user={uid} sub={sub_norm}")
            continue
        seen_owner_targets.add(identity)
        owners.append({
            'vps_row_id':     vpsid,
            'user_id':        uid,
            'sub_account_id': sub_norm,
        })

    print(f"   [SA][OWNER] Detected: {len(owners)}")

    # ---- Batch follower lookup: ONE round-trip for ALL owners ----
    if owners:
        owner_pairs = [(o['user_id'], o['sub_account_id']) for o in owners]
        pair_tuples = ', '.join(
            f"({_sql_int(u)}, {_sql_int(s)})" for (u, s) in owner_pairs
        )
        fol_sql = (
            "SELECT id, owner_id, sub_account_id, "
            "       follower_id, follower_sub_account_id, host_status "
            "  FROM vps_hosts_followers "
            f" WHERE (owner_id, sub_account_id) IN ({pair_tuples}) "
            "    AND host_status = 'active'"
        )
        fol_res = db_query(fol_sql, None, timeout=20.0, quiet=True)
        if fol_res.get('status') != 'success':
            print(f"   [SA][WARN] batched followers lookup failed: "
                  f"{fol_res.get('message')}")
        else:
            for row in fol_res.get('results', []) or []:
                fid  = _coerce_int(_pick_first_key(
                    row, 'follower_id', 'followerid'))
                fsub = _coerce_int(_pick_first_key(
                    row, 'follower_sub_account_id', 'followersubaccountid'))
                if fid is None:
                    continue
                fsub_norm = fsub if fsub is not None else fid
                identity = (fid, fsub_norm)
                if identity in seen_follower_targets:
                    print(f"   [SA][FOLLOWER][DEDUP] duplicate follower binding ignored: user={fid} sub={fsub_norm}")
                    continue
                seen_follower_targets.add(identity)
                followers.append({
                    'owner_id':                _coerce_int(_pick_first_key(row, 'owner_id')),
                    'owner_sub_account_id':    _coerce_int(_pick_first_key(row, 'sub_account_id')),
                    'follower_id':             fid,
                    'follower_sub_account_id': fsub_norm,
                })

    print(f"   [SA][FOLLOWER] Detected: {len(followers)}")
    return owners, followers


# ==============================================================================
#  [PRG] PASS — programme_vps table  (BATCHED follower-userid lookup)
# ==============================================================================
def _collect_prg_candidates_for_ip(local_ip):
    bindings           = []
    candidate_user_ids = set()
    follower_progs     = []

    pv_sql = (
        "SELECT id, programme_id, user_id, sub_account_id, "
        "programme_vps_ip_address "
        "FROM programme_vps "
        "WHERE programme_vps_ip_address = %s"
    )
    pv_res = db_query(pv_sql, [local_ip], timeout=15.0)
    if pv_res.get('status') != 'success':
        print(f"   [PRG][ERROR] programme_vps lookup failed: {pv_res.get('message')}")
        return bindings, follower_progs, candidate_user_ids

    pv_rows = pv_res.get('results', []) or []
    print(f"   [PRG][PROGRAMME_VPS] {len(pv_rows)} row(s) for IP {local_ip}")

    for row in pv_rows:
        uid   = _coerce_int(_pick_first_key(row, 'user_id', 'userid', 'uid'))
        pid   = _coerce_int(_pick_first_key(row, 'programme_id', 'programmeid'))
        pvid  = _coerce_int(_pick_first_key(row, 'id', 'pv_id'))

        if uid is None or pid is None or pid <= 0:
            continue

        print(
            f"   [PRG][PROGRAMME_VPS] row id={pvid} user_id={uid} "
            f"programme_id={pid} — owner binding"
        )
        bindings.append({
            'vps_row_id':   pvid,
            'user_id':      uid,
            'programme_id': pid,
        })
        candidate_user_ids.add(uid)

    print(f"   [PRG][BINDING] Owner bindings: {len(bindings)}")

    # ---- Batch follower lookup + batch follower-userid lookup ----
    owner_prog_ids = [b['programme_id'] for b in bindings]
    if owner_prog_ids:
        id_list = ', '.join(_sql_int(p) for p in owner_prog_ids)
        fol_sql = (
            "SELECT id, owner_programme_id, follower_programme_id, host_status "
            "  FROM programme_vps_hosts_followers "
            f" WHERE owner_programme_id IN ({id_list}) "
            "    AND host_status = 'active'"
        )
        fol_res = db_query(fol_sql, None, timeout=15.0, quiet=True)
        if fol_res.get('status') != 'success':
            print(f"   [PRG][WARN] follower lookup failed: {fol_res.get('message')}")
        else:
            for row in fol_res.get('results', []) or []:
                fpid = _coerce_int(_pick_first_key(
                    row, 'follower_programme_id', 'followerprogrammeid'))
                opid = _coerce_int(_pick_first_key(
                    row, 'owner_programme_id', 'ownerprogrammeid'))
                if fpid is None or fpid <= 0:
                    continue
                follower_progs.append({
                    'owner_programme_id':    opid,
                    'follower_programme_id': fpid,
                })

        # ONE batched userid lookup for every follower programme id.
        if follower_progs:
            fpid_list = ', '.join(
                _sql_int(f['follower_programme_id']) for f in follower_progs
            )
            uid_sql = (
                f"SELECT id, userid FROM programme WHERE id IN ({fpid_list})"
            )
            uid_res = db_query(uid_sql, None, timeout=15.0, quiet=True)
            if uid_res.get('status') != 'success':
                print(f"   [PRG][WARN] batched follower-userid lookup "
                      f"failed: {uid_res.get('message')}")
            else:
                for row in uid_res.get('results', []) or []:
                    fuid = _coerce_int(_pick_first_key(row, 'userid'))
                    if fuid is not None and fuid > 0:
                        candidate_user_ids.add(fuid)

    print(f"   [PRG][FOLLOWER] Follower programmes: {len(follower_progs)}")

    return bindings, follower_progs, candidate_user_ids


# ==============================================================================
#  MAIN ENTRY
# ==============================================================================
def check_if_broker_is_logged_in(inv_id=None):
    report = {
        'investor_id':       str(inv_id) if inv_id is not None else None,
        'local_ip':          None,
        'owners':            [],
        'followers':         [],
        'owner_results':     [],
        'follower_results':  [],
        'programme_bindings':  [],
        'programme_followers': [],
        'programme_results':   [],
        'success':           False,
        'message':           '',
    }

    print(f"\n{'='*70}")
    print(f"[CHECK] check_if_broker_is_logged_in(inv_id={inv_id})")
    print(f"{'='*70}")

    if not os.path.isdir(DEFAULT_MT5_PATH):
        report['message'] = f"DEFAULT_MT5_PATH missing: {DEFAULT_MT5_PATH}"
        print(f"   [ERROR] {report['message']}")
        return report

    if not os.path.isdir(MT5_DESTINATION_PATH):
        try:
            os.makedirs(MT5_DESTINATION_PATH, exist_ok=True)
        except Exception as e:
            report['message'] = f"Cannot create MT5_DESTINATION_PATH: {e}"
            print(f"   [ERROR] {report['message']}")
            return report

    print(f"   [PATH] DEFAULT : {DEFAULT_MT5_PATH}")
    print(f"   [PATH] DEST    : {MT5_DESTINATION_PATH}")

    local_ip = get_local_ip()
    report['local_ip'] = local_ip
    if not local_ip:
        report['message'] = 'Could not determine local IP'
        print(f"   [ERROR] {report['message']}")
        return report
    print(f"   [IP] {local_ip}")

    # ==================================================================
    #  [SA] PIPELINE
    # ==================================================================
    print(f"\n{'#'*70}")
    print(f"#  [SA] INVESTOR / SUB-ACCOUNT PIPELINE")
    print(f"#  Tables: vps, vps_hosts_followers, vps_hosts_requestors, harvhub")
    print(f"{'#'*70}")
    try:
        try:
            completed = _process_disconnect_requests()
            if completed:
                print(f"   [SA][DISCONNECT] completed: {completed}")
        except Exception as e:
            print(f"   [SA][DISCONNECT] pass raised: {e}")
            traceback.print_exc()

        sa_owners, sa_followers = _collect_sa_candidates_for_ip(local_ip)
        report['owners']    = sa_owners
        report['followers'] = sa_followers

        print(f"\n   --- [SA] OWNERS ---")
        processed_sa_candidates = set()
        processed_sa_row_ids = set()
        for owner in report['owners']:
            uid = owner['user_id']
            sub = owner['sub_account_id']
            candidate_key = (uid, sub)
            if candidate_key in processed_sa_candidates:
                print(f"      [SA][DEDUP] already handled user={uid} sub={sub}; skipping duplicate owner binding")
                continue
            processed_sa_candidates.add(candidate_key)
            print(f"\n   [SA][OWNER] user={uid} sub={sub}")
            row = _fetch_broker_row(user_id=uid, sub_account_id=sub)
            if not row:
                print(f"      [SA][WARN] No harvhub row")
                report['owner_results'].append({
                    'source': 'SA',
                    'user_id': uid, 'sub_account_id': sub,
                    'ok': False, 'reason': 'no harvhub row',
                })
                continue
            canonical_row_id = _coerce_int(_pick_first_key(row, 'id'))
            if canonical_row_id is not None and canonical_row_id in processed_sa_row_ids:
                print(f"      [SA][DEDUP] HarvHub row id={canonical_row_id} already processed; skipping duplicate owner target")
                continue
            if canonical_row_id is not None:
                processed_sa_row_ids.add(canonical_row_id)
            res = _process_target(row, role='owner', user_id=uid, sub_account_id=sub)
            report['owner_results'].append(res)

        if report['followers']:
            print(f"\n   --- [SA] FOLLOWERS ---")

        for fol in report['followers']:
            uid = fol['follower_id']
            sub = fol['follower_sub_account_id']
            candidate_key = (uid, sub)
            if candidate_key in processed_sa_candidates:
                print(f"      [SA][DEDUP] already handled user={uid} sub={sub}; skipping duplicate follower binding")
                continue
            processed_sa_candidates.add(candidate_key)
            print(f"\n   [SA][FOLLOWER] user={uid} sub={sub}")
            row = _fetch_broker_row(user_id=uid, sub_account_id=sub)
            if not row:
                print(f"      [SA][WARN] No harvhub row")
                report['follower_results'].append({
                    'source': 'SA',
                    'user_id': uid, 'sub_account_id': sub,
                    'ok': False, 'reason': 'no harvhub row',
                })
                continue
            canonical_row_id = _coerce_int(_pick_first_key(row, 'id'))
            if canonical_row_id is not None and canonical_row_id in processed_sa_row_ids:
                print(f"      [SA][DEDUP] HarvHub row id={canonical_row_id} already processed; skipping duplicate follower target")
                continue
            if canonical_row_id is not None:
                processed_sa_row_ids.add(canonical_row_id)
            res = _process_target(row, role='follower', user_id=uid, sub_account_id=sub)
            report['follower_results'].append(res)

    except Exception as e:
        print(f"   [SA] PIPELINE raised: {e}")
        traceback.print_exc()

    # ==================================================================
    #  [PRG] PIPELINE
    # ==================================================================
    print(f"\n{'#'*70}")
    print(f"#  [PRG] PROGRAMME / TRADER PIPELINE")
    print(f"#  Tables: programme_vps, programme_vps_hosts_followers,")
    print(f"#          programme_vps_hosts_requestors, programme")
    print(f"{'#'*70}")
    try:
        try:
            completed = _process_programme_disconnect_requests()
            if completed:
                print(f"   [PRG][DISCONNECT] completed: {completed}")
        except Exception as e:
            print(f"   [PRG][DISCONNECT] pass raised: {e}")
            traceback.print_exc()

        prg_bindings, prg_followers, prg_user_ids = (
            _collect_prg_candidates_for_ip(local_ip)
        )
        report['programme_bindings']  = prg_bindings
        report['programme_followers'] = prg_followers

        processed_programme_ids = set()

        def _process_one_programme(uid, programme_id, origin):
            if programme_id in processed_programme_ids:
                return
            processed_programme_ids.add(programme_id)

            prog_row = _fetch_programme_row(programme_id=programme_id, user_id=uid)
            if not prog_row:
                print(f"      [PRG][WARN] No programme row for id={programme_id}")
                report['programme_results'].append({
                    'source': 'PRG',
                    'user_id': uid,
                    'programme_id': programme_id,
                    'ok': False,
                    'reason': 'no programme row',
                })
                return

            manual_status = (
                _pick_first_key(prog_row, 'broker_login_status') or ''
            ).strip().lower()

            if manual_status in PROGRAMME_NEGATIVE_STATUSES:
                print(f"      [PRG][SKIP] programme={programme_id} — {manual_status}")
                report['programme_results'].append({
                    'source': 'PRG',
                    'user_id': uid,
                    'programme_id': programme_id,
                    'ok': False,
                    'reason': manual_status,
                })
                return

            login_v  = _pick_first_key(prog_row, 'login')
            server_v = _pick_first_key(prog_row, 'server')
            if not _coerce_int(login_v) or not (server_v or '').strip():
                print(f"      [PRG][SKIP] programme={programme_id} — no broker details")
                report['programme_results'].append({
                    'source': 'PRG',
                    'user_id': uid,
                    'programme_id': programme_id,
                    'ok': False,
                    'reason': 'no broker details',
                })
                return

            res = _process_programme_target(
                prog_row, user_id=uid, programme_id=programme_id
            )
            report['programme_results'].append(res)

        print(f"\n   --- [PRG] OWNER PROGRAMMES (programme_vps) ---")
        for binding in report['programme_bindings']:
            uid = binding['user_id']
            pid = binding['programme_id']
            print(f"\n   [PRG] user={uid} programme={pid} (owner binding)")
            _process_one_programme(uid, pid, origin='programme_vps')

        print(f"\n   --- [PRG] FOLLOWER PROGRAMMES ---")

        # ---- BATCH userid lookup for every follower programme ----
        follower_prog_ids = [
            f['follower_programme_id'] for f in report['programme_followers']
            if f.get('follower_programme_id') is not None
        ]
        follower_userid_map = {}
        if follower_prog_ids:
            id_list = ', '.join(_sql_int(p) for p in follower_prog_ids)
            uid_sql = (
                f"SELECT id, userid FROM programme WHERE id IN ({id_list})"
            )
            uid_res = db_query(uid_sql, None, timeout=15.0, quiet=True)
            if uid_res.get('status') == 'success':
                for row in uid_res.get('results', []) or []:
                    pid = _coerce_int(_pick_first_key(row, 'id'))
                    uid = _coerce_int(_pick_first_key(row, 'userid'))
                    if pid is not None:
                        follower_userid_map[pid] = uid

        for fol in report['programme_followers']:
            fpid = fol['follower_programme_id']
            opid = fol['owner_programme_id']
            fuid = follower_userid_map.get(fpid)
            if fuid is None:
                print(f"      [PRG][WARN] follower programme={fpid} has no userid")
                continue
            print(f"\n   [PRG] follower user={fuid} programme={fpid} "
                  f"(under owner programme={opid})")
            _process_one_programme(fuid, fpid, origin='follower')

    except Exception as e:
        print(f"   [PRG] PIPELINE raised: {e}")
        traceback.print_exc()

    # ==================================================================
    #  SUMMARY
    # ==================================================================
    ok_owners    = sum(1 for r in report['owner_results']    if r.get('ok'))
    ok_followers = sum(1 for r in report['follower_results'] if r.get('ok'))
    ok_progs     = sum(1 for r in report['programme_results'] if r.get('ok'))

    sa_total = len(report['owners']) + len(report['followers'])
    sa_ok    = ok_owners + ok_followers

    prg_total = len(report['programme_results'])
    prg_ok    = ok_progs

    report['success'] = (sa_ok + prg_ok) > 0
    report['message'] = (
        f"[SA] accounts={sa_total}({sa_ok} ok) | "
        f"[PRG] programmes={prg_total}({prg_ok} ok)"
    )
    print(f"\n   [RESULT] {report['message']}")
    return report


# ==============================================================================
#  MAIN LOOP
# ==============================================================================
def main_loop():
    run_as_loop = False
    loop_interval = 5
    for arg in sys.argv[1:]:
        lower = arg.lower()
        if lower.startswith("--execution-type="):
            run_as_loop = lower.split("=", 1)[1].strip() == "loop"
        elif lower in ("--loop", "--loop=true", "--loop=1", "--loop=yes"):
            run_as_loop = True
        elif lower in ("--once", "--loop=false", "--loop=0", "--loop=no"):
            run_as_loop = False
        elif lower.startswith("--interval="):
            try:
                loop_interval = max(1, int(lower.split("=", 1)[1]))
            except ValueError:
                pass
        elif lower == "--interval":
            # Supports the argparse-style two-token form.
            continue

    args = list(sys.argv[1:])
    for index, arg in enumerate(args[:-1]):
        if arg.lower() == "--interval":
            try:
                loop_interval = max(1, int(args[index + 1]))
            except ValueError:
                pass

    _build_timeframe_map()
    print(f"\n{'#'*70}")
    print("#  brokerdetails — broker login monitoring")
    print(f"#  started at {datetime.now().isoformat(timespec='seconds')}", flush=True)
    print(f"#  execution type: {'loop' if run_as_loop else 'once'} | interval: {loop_interval}s", flush=True)
    print(f"{'#'*70}")

    try:
        cycle_number = 0
        while True:
            cycle_number += 1
            print(f"\n[main] broker login scan #{cycle_number} starting...", flush=True)
            try:
                result = check_if_broker_is_logged_in()
                print(f"[main] scan #{cycle_number} result: {result.get('message')}", flush=True)
            except Exception as e:
                print(f"[main] check_if_broker_is_logged_in raised: {e}")
                traceback.print_exc()

            if not run_as_loop:
                # Preserve the manual-login workflow: in Once mode the scan is
                # complete, but keep this process alive while the manager has
                # unresolved rows so Valid/Invalid clicks can be applied safely.
                if not _MANUAL_WINDOW.has_pending_items():
                    break
                print("[main] Waiting for manual MT5 login decisions in the HarvHub window.", flush=True)
                while _MANUAL_WINDOW.has_pending_items():
                    time.sleep(float(loop_interval))
                    try:
                        result = check_if_broker_is_logged_in()
                        print(f"[main] manual-login follow-up: {result.get('message')}")
                    except Exception as e:
                        print(f"[main] manual-login follow-up raised: {e}")
                        traceback.print_exc()
                break

            time.sleep(loop_interval)
    except KeyboardInterrupt:
        print("[main] interrupted — shutting down")
    finally:
        _MANUAL_WINDOW.stop()


def main():
    return main_loop()

if __name__ == "__main__":
    main_loop()