from __future__ import annotations

import base64
import ctypes
from ctypes import wintypes
import hashlib
import json
import os
import queue
import shutil
import socket
import ssl
import struct
import subprocess
import sys
import threading
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from tkinter import messagebox, ttk
import tkinter as tk
from tkinter.scrolledtext import ScrolledText
from typing import Callable, Optional


APP_TITLE = "HarvHub AI"
APP_EXE_NAME = "HarvHubAI.exe"
PYTHON_VERSION = "3.13.17"
PYTHON_MINOR = (3, 13)
PYTHON_INSTALLER_URL = (
    "https://www.python.org/ftp/python/3.13.17/python-3.13.17-amd64.exe"
)
PYTHON_INSTALLER_SHA256 = (
    "5da012101ba2e17113a9a1cd901e9ce1"
    "441086f2b7ecb5de06ec94163899a190"
)
CHROME_MSI_URL = (
    "https://dl.google.com/dl/chrome/install/"
    "googlechromestandaloneenterprise64.msi"
)
MT5_SETUP_URL = (
    "https://download.mql5.com/cdn/web/metaquotes.software.corp/mt5/mt5setup.exe"
)
BRIDGE_HOST = "127.0.0.1"
BRIDGE_PORT = 5599
CONTROL_HOST = "127.0.0.1"
CONTROL_PORT = 5601
DEFAULT_LOOP_INTERVAL = 5
MAX_BRIDGE_CHANNELS = 32

# The deployed HarvHub AI root is also the permanent desktop-app installation
# directory. The VBS launcher installs/updates HarvHubAI.exe here.
HARVHUBAI_ROOT = Path(os.environ.get(
    "HARVHUBAI_ROOT", r"C:\xampp\htdocs\harvhubAI"
))
PYTHON_DIR = HARVHUBAI_ROOT / "python"
MT5_ROOT = HARVHUBAI_ROOT / "mt5"
MT5_TEMPLATE = MT5_ROOT / "MetaTrader 5"
LOG_DIR = HARVHUBAI_ROOT / "logs"
MANAGER_DIR = HARVHUBAI_ROOT / ".manager"
DOWNLOAD_DIR = MANAGER_DIR / "downloads"
STATE_FILE = MANAGER_DIR / "installation.json"
EXECUTION_CONFIG_FILE = MANAGER_DIR / "execution_config.json"

# All supplied service and support modules are deployed. Only the six genuine
# runnable services appear in the navigation; support modules remain
# importable dependencies in the Python folder.
BUNDLED_PYTHON_FILES = [
    "mysqlremote.py",
    "brokerdetails.py",
    "brokercharts.py",
    "programme_training.py",
    "programmesignals_trading.py",
    "signals_provision_challenge.py",
    "ip.py",
    "notification_email.py",
    "notification_service.py",
    "harvhub_ipc.py",
]

PROGRAMS = [
    {
        "key": "mysqlremote",
        "label": "MySQL Remote",
        "filename": "mysqlremote.py",
        "description": "Database bridge used by the HarvHub AI background programs.",
        "icon": "⌘",
        "args": [],
        "role": "service",
        "runnable": True,
        "interval": 5,
        "bridge": True,
    },
    {
        "key": "brokerdetails",
        "label": "Broker Details",
        "filename": "brokerdetails.py",
        "description": "Broker login monitoring and account-specific MT5 terminal provisioning.",
        "icon": "▣",
        "args": [],
        "role": "service",
        "runnable": True,
        "interval": 10,
        "bridge": False,
        "note": "Manual MT5 login requests are shown directly in this HarvHub window.",
    },
    {
        "key": "brokercharts",
        "label": "Broker Charts",
        "filename": "brokercharts.py",
        "description": "Collects broker chart data and programme OHLC candles.",
        "icon": "◈",
        "args": [],
        "role": "service",
        "runnable": True,
        "interval": 60,
        "bridge": False,
    },
    {
        "key": "programme_training",
        "label": "Programme Training",
        "filename": "programme_training.py",
        "description": "Matches programme configurations and writes programme analytics.",
        "icon": "▤",
        "args": [],
        "role": "service",
        "runnable": True,
        "interval": 60,
        "bridge": False,
    },
    {
        "key": "programmesignals_trading",
        "label": "Programme Signals Trading",
        "filename": "programmesignals_trading.py",
        "description": "Processes programme trade requests and the investor trading pipeline.",
        "icon": "↗",
        "args": [],
        "role": "service",
        "runnable": True,
        "interval": 5,
        "bridge": False,
        "warning": "This program can manage live MT5 positions and execute configured close/breakeven actions.",
    },
    {
        "key": "signals_provision_challenge",
        "label": "Signals Provision Challenge",
        "filename": "signals_provision_challenge.py",
        "description": "Monitors VPS-scoped challenges and programme publication/approval status.",
        "icon": "✓",
        "args": [],
        "role": "service",
        "runnable": True,
        "interval": 30,
        "bridge": False,
        "warning": "This program can change programme challenge status, advertisement and server approval fields.",
    },
]

COMPUTER_IP_PAGE = {
    "key": "computer_ip",
    "label": "Computer IP",
    "description": "View the Windows computer's local network address.",
    "icon": "⌁",
}
NAV_ITEMS = PROGRAMS + [COMPUTER_IP_PAGE]
RUN_ALL_ORDER = [
    "mysqlremote",
    "brokerdetails",
    "brokercharts",
    "programme_training",
    "programmesignals_trading",
    "signals_provision_challenge",
]

# Fixed HarvHub Server white/gold palette. Theme switching is intentionally
# removed: the desktop UI always follows the supplied server's light styling.
LIGHT = {
    "bg": "#faf7ef",
    "surface": "#fffdf8",
    "surface2": "#f7f0df",
    "surface3": "#eee4ce",
    "text": "#302718",
    "muted": "#82745d",
    "border": "#e8dec8",
    "gold": "#b88720",
    "gold_hover": "#946b16",
    "green": "#247a4b",
    "red": "#b94335",
    "console_bg": "#171915",
    "console_fg": "#e6eadf",
    "console_dim": "#a9b19f",
}


class _DATA_BLOB(ctypes.Structure):
    _fields_ = [
        ("cbData", wintypes.DWORD),
        ("pbData", ctypes.POINTER(ctypes.c_byte)),
    ]


def protect_current_user(data: bytes) -> bytes:
    """Encrypt bytes with Windows DPAPI for the current Windows user."""
    if not is_windows():
        raise OSError("Windows DPAPI is only available on Windows.")
    crypt32 = ctypes.WinDLL("crypt32", use_last_error=True)
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    crypt32.CryptProtectData.argtypes = [
        ctypes.POINTER(_DATA_BLOB), wintypes.LPCWSTR,
        ctypes.POINTER(_DATA_BLOB), ctypes.c_void_p, ctypes.c_void_p,
        wintypes.DWORD, ctypes.POINTER(_DATA_BLOB),
    ]
    crypt32.CryptProtectData.restype = wintypes.BOOL
    kernel32.LocalFree.argtypes = [ctypes.c_void_p]
    kernel32.LocalFree.restype = ctypes.c_void_p

    source_buffer = ctypes.create_string_buffer(data)
    source_blob = _DATA_BLOB(
        len(data), ctypes.cast(source_buffer, ctypes.POINTER(ctypes.c_byte))
    )
    encrypted_blob = _DATA_BLOB()
    ok = crypt32.CryptProtectData(
        ctypes.byref(source_blob), "HarvHub AI protected diagnostics",
        None, None, None, 0x1, ctypes.byref(encrypted_blob),
    )
    if not ok:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        return ctypes.string_at(encrypted_blob.pbData, encrypted_blob.cbData)
    finally:
        if encrypted_blob.pbData:
            kernel32.LocalFree(ctypes.cast(encrypted_blob.pbData, ctypes.c_void_p))


def append_protected_log(path: Path, line: str, max_bytes: int = 12 * 1024 * 1024) -> None:
    """Append one DPAPI-encrypted, base64-wrapped line; never write plaintext logs."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.stat().st_size >= max_bytes:
        previous = path.with_suffix(path.suffix + ".previous")
        try:
            previous.unlink(missing_ok=True)
            path.replace(previous)
        except OSError:
            path.unlink(missing_ok=True)
    raw = (line.rstrip("\r\n") + "\n").encode("utf-8", errors="replace")
    encrypted = protect_current_user(raw)
    encoded = base64.b64encode(encrypted)
    with path.open("ab") as stream:
        stream.write(encoded + b"\n")


def unprotect_current_user(data: bytes) -> bytes:
    """Decrypt a DPAPI-protected diagnostic blob for the current Windows user."""
    if not is_windows():
        raise OSError("Windows DPAPI is only available on Windows.")
    crypt32 = ctypes.WinDLL("crypt32", use_last_error=True)
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    crypt32.CryptUnprotectData.argtypes = [
        ctypes.POINTER(_DATA_BLOB), ctypes.c_void_p,
        ctypes.POINTER(_DATA_BLOB), ctypes.c_void_p, ctypes.c_void_p,
        wintypes.DWORD, ctypes.POINTER(_DATA_BLOB),
    ]
    crypt32.CryptUnprotectData.restype = wintypes.BOOL
    kernel32.LocalFree.argtypes = [ctypes.c_void_p]
    kernel32.LocalFree.restype = ctypes.c_void_p
    source_buffer = ctypes.create_string_buffer(data)
    source_blob = _DATA_BLOB(
        len(data), ctypes.cast(source_buffer, ctypes.POINTER(ctypes.c_byte))
    )
    plain_blob = _DATA_BLOB()
    ok = crypt32.CryptUnprotectData(
        ctypes.byref(source_blob), None, None, None, None, 0x1,
        ctypes.byref(plain_blob),
    )
    if not ok:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        return ctypes.string_at(plain_blob.pbData, plain_blob.cbData)
    finally:
        if plain_blob.pbData:
            kernel32.LocalFree(ctypes.cast(plain_blob.pbData, ctypes.c_void_p))


def read_protected_log(path: Path) -> list[str]:
    """Read a DPAPI/base64 line log; unreadable blobs are skipped, never exposed."""
    lines: list[str] = []
    if not path.is_file() or not is_windows():
        return lines
    try:
        with path.open("rb") as stream:
            for encoded in stream:
                encoded = encoded.strip()
                if not encoded:
                    continue
                try:
                    decoded = unprotect_current_user(base64.b64decode(encoded, validate=True))
                    lines.append(decoded.decode("utf-8", errors="replace").rstrip("\r\n"))
                except Exception:
                    continue
    except OSError:
        return lines
    return lines


class SetupCancelled(Exception):
    """Raised when the user leaves Setup / Repair before it finishes."""


@dataclass
class ProgramProcess:
    process: subprocess.Popen
    started_at: float


def is_windows() -> bool:
    return os.name == "nt"


def is_admin() -> bool:
    if not is_windows():
        return False
    try:
        return bool(ctypes.windll.shell32.IsUserAnAdmin())
    except Exception:
        return False


def relaunch_as_admin() -> bool:
    """Request UAC elevation when running the source build (the EXE has a manifest)."""
    if not is_windows() or is_admin():
        return True
    try:
        params = subprocess.list2cmdline(sys.argv)
        result = ctypes.windll.shell32.ShellExecuteW(
            None, "runas", sys.executable, params, None, 1
        )
        return int(result) > 32
    except Exception:
        return False


def resource_directory() -> Path:
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return Path(sys._MEIPASS) / "payload"
    return Path(__file__).resolve().parent / "payload"


def powershell_signature(path: Path) -> tuple[bool, str]:
    if not is_windows():
        return False, "Authenticode verification is available only on Windows."
    quoted = str(path).replace("'", "''")
    command = (
        f"$s=Get-AuthenticodeSignature -LiteralPath '{quoted}'; "
        "if ($s.Status -ne 'Valid') { Write-Output ('INVALID:' + $s.Status); exit 2 }; "
        "Write-Output $s.SignerCertificate.Subject"
    )
    try:
        result = subprocess.run(
            ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", command],
            capture_output=True,
            text=True,
            timeout=20,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        subject = (result.stdout or "").strip()
        return result.returncode == 0, subject or (result.stderr or "No signer information returned.").strip()
    except Exception as exc:
        return False, str(exc)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest().lower()


def candidate_python_is_compatible(executable: Path) -> bool:
    if not executable.is_file():
        return False
    code = (
        "import sys,struct; "
        "print(f'{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}'); "
        "print(struct.calcsize('P')*8)"
    )
    try:
        result = subprocess.run(
            [str(executable), "-c", code],
            capture_output=True,
            text=True,
            timeout=12,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        if result.returncode != 0:
            return False
        lines = (result.stdout or "").strip().splitlines()
        if len(lines) < 2:
            return False
        major_minor = tuple(int(part) for part in lines[0].split(".")[:2])
        return major_minor == PYTHON_MINOR and lines[1].strip() == "64"
    except Exception:
        return False


def locate_python() -> Optional[Path]:
    """Find the exact supported 64-bit Python runtime in machine or user scope.

    The single-file bootstrap installs Python per-user on a fresh VPS, while
    older HarvHub installations may have installed it for all users. Check both
    locations explicitly before relying on PATH or the Python launcher.
    """
    candidates: list[Path] = []
    program_files = Path(os.environ.get("ProgramFiles", r"C:\Program Files"))
    local_app_data = Path(os.environ.get("LOCALAPPDATA", str(Path.home() / "AppData" / "Local")))
    candidates.extend([
        program_files / "Python313" / "python.exe",
        local_app_data / "Programs" / "Python" / "Python313" / "python.exe",
    ])

    # Include Python313 installations discoverable beside the selected
    # pythonw.exe or from the per-user Python installation root.
    user_python_root = local_app_data / "Programs" / "Python"
    if user_python_root.is_dir():
        try:
            candidates.extend(
                folder / "python.exe"
                for folder in sorted(user_python_root.glob("Python*"), reverse=True)
            )
        except OSError:
            pass

    launcher = shutil.which("py.exe") or shutil.which("py")
    if launcher:
        try:
            result = subprocess.run(
                [launcher, "-3.13", "-c", "import sys; print(sys.executable)"],
                capture_output=True,
                text=True,
                timeout=15,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            found = (result.stdout or "").strip().splitlines()
            if result.returncode == 0 and found:
                candidates.append(Path(found[-1].strip()))
        except Exception:
            pass

    path_python = shutil.which("python.exe") or shutil.which("python")
    if path_python:
        candidates.append(Path(path_python))

    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate).lower()
        if key in seen:
            continue
        seen.add(key)
        if candidate_python_is_compatible(candidate):
            return candidate.resolve()
    return None


def build_service_command(python_exe, script, key, execution_type, interval, extra_args=()):
    """Build the one CLI contract accepted by every HarvHub runnable service."""
    mode = "loop" if str(execution_type).lower() == "loop" else "once"
    command = [str(python_exe), "-u", str(script), f"--execution-type={mode}", "--interval", str(max(1, int(interval)))]
    command.extend(str(arg) for arg in (extra_args or ()))
    return command


class HarvCoreManager:
    def __init__(self, root: tk.Tk, force_setup: bool = False):
        self.root = root
        self.events: queue.Queue = queue.Queue()
        self.processes: dict[str, ProgramProcess] = {}
        self.process_lock = threading.RLock()
        self.execution_config = self._load_execution_config()
        self.global_execution_type = self.execution_config.get("default", "loop")
        if self.global_execution_type not in ("loop", "once"):
            self.global_execution_type = "loop"
        self.program_execution_types = {
            item["key"]: self.execution_config.get("programs", {}).get(
                item["key"], self.global_execution_type
            ) for item in PROGRAMS
        }
        # MySQL Remote is a shared TCP server, not a one-shot worker.
        self.program_execution_types["mysqlremote"] = "loop"
        self.global_execution_type = "loop" if self.global_execution_type not in ("loop", "once") else self.global_execution_type
        self.global_execution_var = tk.StringVar(value=self.global_execution_type.title())
        self.program_execution_vars: dict[str, tk.StringVar] = {}
        self.manual_login_items: dict[tuple[str, str, str], dict] = {}
        self.manual_login_decisions: dict[tuple[str, str, str], str] = {}
        self.manual_login_lock = threading.RLock()
        self.manual_login_tab = "SA"
        self.mysql_metrics = {}
        self.mysql_metric_labels: dict[str, tk.Label] = {}
        self.mysql_channel_tree = None
        self.mysql_poll_started = False
        self.mysql_poll_stop = threading.Event()
        self.control_server_stop = threading.Event()
        self.control_server_socket = None
        self.console_widgets: dict[str, ScrolledText] = {}
        self.console_cards: dict[str, tk.Frame] = {}
        self.console_visible: dict[str, bool] = {}
        self.program_page_canvases: dict[str, tk.Canvas] = {}
        self.console_toggle_buttons: dict[str, tk.Button] = {}
        self.console_copy_buttons: dict[str, tk.Button] = {}
        self.program_stop_buttons: dict[str, tk.Button] = {}
        self.output_threads: dict[str, threading.Thread] = {}
        self.output_watchdog_stops: dict[str, threading.Event] = {}
        self.service_output_at: dict[str, float] = {}
        self.raw_logs: dict[str, list[str]] = {item["key"]: [] for item in PROGRAMS}
        self.setup_raw_lines: list[str] = []
        self.setup_step_states: dict[str, str] = {}
        self.active_setup_stage: Optional[str] = None
        self.log_lock = threading.RLock()
        if int(self.execution_config.get("schema_version", 1) or 1) < 2:
            # Save the one-time migration so a later explicit Once selection
            # is respected instead of being reset on every launch.
            self._persist_execution_config()
        self.status_widgets: dict[str, tk.Label] = {}
        self.setup_rows: dict[str, tuple[tk.Label, tk.Label]] = {}
        self.colors = LIGHT.copy()
        self.python_exe: Optional[Path] = None
        self.workspace_built = False
        self.setup_started = False
        self.setup_view_active = False
        self.setup_cancel_event = threading.Event()
        self.auto_setup_after_id = None
        self.global_run_all_button: Optional[tk.Button] = None
        self.global_stop_all_button: Optional[tk.Button] = None
        self.global_reset_all_button: Optional[tk.Button] = None
        self.global_update_button: Optional[tk.Button] = None
        self.global_header: Optional[tk.Frame] = None
        self.global_header_left: Optional[tk.Frame] = None
        self.global_header_right: Optional[tk.Frame] = None
        self.run_all_lock = threading.Lock()
        self.run_all_cancel = threading.Event()
        self.reset_lock = threading.Lock()
        self.stop_all_in_progress = False
        self.closing = False
        self.active_page_key = "home"
        self.pages: dict[str, tk.Frame] = {}
        self.nav_buttons: dict[str, tk.Button] = {}
        self._layout_after_id = None
        self._workspace_landscape = None
        self.ip_value_label = None
        self.computer_name_label = None

        self.root.title(APP_TITLE)
        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()
        window_width = min(1320, max(360, screen_width - 80))
        window_height = min(840, max(520, screen_height - 100))
        self.root.geometry(f"{window_width}x{window_height}")
        self.root.minsize(360, 520)
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        self.root.configure(bg=self.colors["bg"])
        self._configure_styles()
        self._build_shell()
        self._load_protected_logs()
        threading.Thread(target=self._control_server_loop, name="harvhub-manager-ipc", daemon=True).start()
        self.root.bind("<Configure>", self._on_root_configure, add="+")
        self.root.after(120, self._poll_events)

        if not is_windows():
            self._show_platform_error()
            return

        if force_setup:
            self.python_exe = locate_python()
            self._show_setup()
            self.auto_setup_after_id = self.root.after(350, self._auto_start_setup)
        elif self._installation_ready():
            self.python_exe = locate_python()
            self._show_workspace()
        else:
            self._show_setup()
            self.auto_setup_after_id = self.root.after(700, self._auto_start_setup)

    # ---------------------------- State and paths ----------------------------
    def _load_execution_config(self) -> dict:
        config = {"schema_version": 3, "default": "loop", "interval": DEFAULT_LOOP_INTERVAL, "programs": {}}
        try:
            loaded = json.loads(EXECUTION_CONFIG_FILE.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                # Version 1 configurations defaulted to Once, which caused
                # monitoring services to perform one pass and then exit.
                # Migrate those old settings to Loop exactly once. Version 2
                # preserves deliberate user selections going forward.
                try:
                    config_version = int(loaded.get("schema_version", 1) or 1)
                except (TypeError, ValueError):
                    config_version = 1
                config["schema_version"] = 3
                if config_version >= 3:
                    if loaded.get("default") in ("loop", "once"):
                        config["default"] = loaded["default"]
                    try:
                        config["interval"] = max(1, int(loaded.get("interval", DEFAULT_LOOP_INTERVAL)))
                    except (TypeError, ValueError):
                        pass
                    programs = loaded.get("programs")
                    if isinstance(programs, dict):
                        config["programs"] = {
                            str(key): value for key, value in programs.items()
                            if value in ("loop", "once")
                        }
                else:
                    # Previous manager releases persisted Once as the default,
                    # which made monitoring programs finish immediately. The
                    # first v3 launch migrates them to continuous monitoring.
                    config["default"] = "loop"
                    config["programs"] = {}
        except (OSError, ValueError, TypeError):
            pass
        return config

    def _persist_execution_config(self) -> None:
        config = {
            "schema_version": 3,
            "default": self.global_execution_type,
            "interval": DEFAULT_LOOP_INTERVAL,
            "programs": dict(self.program_execution_types),
            "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        try:
            MANAGER_DIR.mkdir(parents=True, exist_ok=True)
            temporary = EXECUTION_CONFIG_FILE.with_suffix(".json.new")
            temporary.write_text(json.dumps(config, indent=2), encoding="utf-8")
            temporary.replace(EXECUTION_CONFIG_FILE)
            self.execution_config = config
        except OSError as exc:
            self._write_manager_log(f"Could not save execution settings: {exc}")

    def _on_global_execution_changed(self, _event=None) -> None:
        selected = (self.global_execution_var.get() or "Loop").strip().lower()
        self.global_execution_type = "loop" if selected == "loop" else "once"
        for key in self.program_execution_types:
            self.program_execution_types[key] = "loop" if key == "mysqlremote" else self.global_execution_type
        for key, variable in self.program_execution_vars.items():
            variable.set("Loop" if key == "mysqlremote" else self.global_execution_type.title())
        self._persist_execution_config()

    def _on_program_execution_changed(self, key: str, variable: tk.StringVar) -> None:
        selected = (variable.get() or "Loop").strip().lower()
        self.program_execution_types[key] = "loop" if key == "mysqlremote" or selected == "loop" else "once"
        if key == "mysqlremote":
            variable.set("Loop")
        self._persist_execution_config()

    def _load_protected_logs(self) -> None:
        if not is_windows():
            return
        # Remove the plaintext log produced by earlier manager builds.
        try:
            (LOG_DIR / "manager.log").unlink(missing_ok=True)
        except OSError:
            pass
        for item in PROGRAMS:
            self.raw_logs[item["key"]] = read_protected_log(LOG_DIR / f"{item['key']}.hclog")[-30000:]
        self.setup_raw_lines = read_protected_log(LOG_DIR / "setup.hclog")[-50000:]

    def _save_state(self, **updates) -> None:
        MANAGER_DIR.mkdir(parents=True, exist_ok=True)
        state = {}
        try:
            state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
        state.update(updates)
        state["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")

    def _installation_ready(self) -> bool:
        python_exe = locate_python()
        required_files = [PYTHON_DIR / name for name in BUNDLED_PYTHON_FILES]
        if python_exe is None:
            return False
        if not (MT5_TEMPLATE / "terminal64.exe").is_file():
            return False
        if not (
            Path(r"C:\Program Files\Google\Chrome\Application\chrome.exe").is_file()
            or Path(r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe").is_file()
        ):
            return False
        if not all(path.is_file() for path in required_files):
            return False
        import_check = (
            "import MetaTrader5, pytz, tzdata, requests, urllib3, selenium, bs4, webdriver_manager; "
            "print('IMPORTS_OK')"
        )
        try:
            result = subprocess.run(
                [str(python_exe), "-c", import_check],
                capture_output=True, text=True, timeout=45,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            return result.returncode == 0 and "IMPORTS_OK" in (result.stdout or "")
        except Exception:
            return False

    def _write_manager_log(self, line: str) -> None:
        # Full diagnostic detail is stored encrypted with Windows DPAPI.
        # No plaintext manager.log is created.
        try:
            stamp = time.strftime("%Y-%m-%d %H:%M:%S")
            with self.log_lock:
                append_protected_log(LOG_DIR / "manager.hclog", f"[{stamp}] {line}")
        except Exception:
            # Diagnostics remain available in memory for the Copy Report action.
            pass

    # ------------------------------ UI shell --------------------------------
    def _build_shell(self) -> None:
        self.outer = tk.Frame(self.root, bg=self.colors["bg"])
        self.outer.pack(fill="both", expand=True)
        self._build_header(self.outer)
        self.content = tk.Frame(self.outer, bg=self.colors["bg"])
        self.content.pack(fill="both", expand=True, padx=18, pady=(8, 16))

    def _build_header(self, parent: tk.Widget) -> None:
        header = tk.Frame(
            parent, bg=self.colors["surface"],
            highlightbackground=self.colors["border"], highlightthickness=1,
        )
        header.pack(fill="x", padx=18, pady=(16, 8))
        left = tk.Frame(header, bg=self.colors["surface"])
        left.pack(side="left", fill="y", padx=18, pady=11)
        logo = tk.Label(
            left, text="H", bg=self.colors["gold"], fg="#ffffff",
            font=("Segoe UI", 15, "bold"), width=3, height=1, pady=5,
        )
        logo.pack(side="left", padx=(0, 11))
        title_area = tk.Frame(left, bg=self.colors["surface"])
        title_area.pack(side="left")
        tk.Label(
            title_area, text=APP_TITLE, bg=self.colors["surface"], fg=self.colors["text"],
            font=("Segoe UI", 15, "bold"),
        ).pack(anchor="w")
        right = tk.Frame(header, bg=self.colors["surface"])
        right.pack(side="right", padx=16, pady=12)
        button_group = tk.Frame(right, bg=self.colors["surface"])
        button_group.pack(side="right")

        # Workspace-wide execution type controls the Run All action. Individual
        # program pages can override this default before their own Play action.
        tk.Label(
            button_group, text="EXECUTION TYPE", bg=self.colors["surface"],
            fg=self.colors["muted"], font=("Segoe UI", 8, "bold"),
        ).pack(side="left", padx=(0, 6))
        self.global_execution_combo = ttk.Combobox(
            button_group, textvariable=self.global_execution_var,
            values=("Loop", "Once"), state="readonly", width=7,
            style="HarvHub.TCombobox",
        )
        self.global_execution_combo.pack(side="left", padx=(0, 9))
        self.global_execution_combo.bind("<<ComboboxSelected>>", self._on_global_execution_changed)

        # Their order is Execution Type, Run All, conditional Stop All, Update.
        self.global_run_all_button = self._button(
            button_group, "▶  RUN ALL", self.run_all, kind="success"
        )
        self.global_run_all_button.pack(side="left", padx=(0, 7))
        self.global_stop_all_button = self._button(
            button_group, "■  STOP ALL", self.stop_all, kind="danger"
        )
        self.global_stop_all_button.pack_forget()
        self.global_reset_all_button = self._button(
            button_group, "↺  RESET ALL", self.reset_all, kind="quiet"
        )
        self.global_reset_all_button.pack(side="left", padx=(0, 7))
        self.global_update_button = self._button(
            button_group, "↻  UPDATE", self.update_application, kind="quiet"
        )
        self.global_update_button.pack(side="left")

        self.global_header = header
        self.global_header_left = left
        self.global_header_right = right
        header.bind(
            "<Configure>",
            lambda event, h=header, l=left, r=right:
                self._configure_global_header(h, l, r),
        )
        self.root.after_idle(
            lambda h=header, l=left, r=right:
                self._configure_global_header(h, l, r)
        )
        self._update_global_actions()

    def _configure_styles(self) -> None:
        self.style = ttk.Style(self.root)
        try:
            self.style.theme_use("clam")
        except Exception:
            pass
        self._apply_styles()

    def _apply_styles(self) -> None:
        c = self.colors
        self.root.configure(bg=c["bg"])
        self.style.configure("TFrame", background=c["bg"])
        self.style.configure("Surface.TFrame", background=c["surface"])
        self.style.configure("TLabel", background=c["bg"], foreground=c["text"], font=("Segoe UI", 9))
        self.style.configure("Muted.TLabel", background=c["bg"], foreground=c["muted"], font=("Segoe UI", 9))
        self.style.configure("Title.TLabel", background=c["bg"], foreground=c["text"], font=("Segoe UI", 20, "bold"))
        self.style.configure("Section.TLabel", background=c["surface"], foreground=c["text"], font=("Segoe UI", 11, "bold"))
        self.style.configure("Card.TFrame", background=c["surface"], relief="flat")
        self.style.configure("TProgressbar", troughcolor=c["surface3"], background=c["gold"], bordercolor=c["border"], lightcolor=c["gold"], darkcolor=c["gold"])
        self.style.configure("Treeview", background=c["surface"], fieldbackground=c["surface"], foreground=c["text"], rowheight=30, bordercolor=c["border"], font=("Segoe UI", 9))
        self.style.configure("Treeview.Heading", background=c["surface2"], foreground=c["muted"], font=("Segoe UI", 9, "bold"), relief="flat")
        self.style.map("Treeview", background=[("selected", c["surface3"])], foreground=[("selected", c["text"])])

        # The application uses the same colored, borderless selector in the
        # header and on each program page (rather than the platform default).
        self.style.configure(
            "HarvHub.TCombobox",
            fieldbackground=c["surface2"],
            background=c["gold"],
            foreground=c["text"],
            arrowcolor="#ffffff",
            bordercolor=c["surface2"],
            lightcolor=c["surface2"],
            darkcolor=c["surface2"],
            relief="flat",
            borderwidth=0,
            padding=(9, 5),
            arrowsize=14,
            font=("Segoe UI", 9, "bold"),
        )
        self.style.map(
            "HarvHub.TCombobox",
            fieldbackground=[("readonly", c["surface2"]), ("focus", c["surface2"])],
            foreground=[("readonly", c["text"]), ("disabled", c["muted"])],
            background=[("active", c["gold_hover"]), ("readonly", c["gold"])],
            arrowcolor=[("active", "#ffffff"), ("!disabled", "#ffffff")],
            bordercolor=[("focus", c["surface2"]), ("!focus", c["surface2"])],
            lightcolor=[("focus", c["surface2"]), ("!focus", c["surface2"])],
            darkcolor=[("focus", c["surface2"]), ("!focus", c["surface2"])],
        )

    def _button(self, parent: tk.Widget, text: str, command: Callable, kind: str = "primary", width: Optional[int] = None) -> tk.Button:
        c = self.colors
        if kind == "primary":
            bg, fg, active = c["gold"], "#ffffff", c["gold_hover"]
        elif kind == "danger":
            bg, fg, active = c["red"], "#ffffff", c["red"]
        elif kind == "success":
            bg, fg, active = c["green"], "#ffffff", c["green"]
        elif kind == "quiet":
            bg, fg, active = c["surface2"], c["text"], c["surface3"]
        else:
            bg, fg, active = c["surface3"], c["text"], c["surface3"]
        options = {
            "text": text, "command": command, "bg": bg, "fg": fg,
            "activebackground": active, "activeforeground": fg,
            "relief": "flat", "bd": 0, "padx": 15, "pady": 9,
            "font": ("Segoe UI", 9, "bold"), "cursor": "hand2",
            "highlightthickness": 0,
        }
        if width:
            options["width"] = width
        return tk.Button(parent, **options)

    def _card(self, parent: tk.Widget, padding: int = 16) -> tk.Frame:
        return tk.Frame(parent, bg=self.colors["surface"], padx=padding, pady=padding,
                        highlightbackground=self.colors["border"], highlightthickness=1)

    def _clear_content(self) -> None:
        for child in self.content.winfo_children():
            child.destroy()
        self.console_widgets.clear()
        self.console_cards.clear()
        self.console_toggle_buttons.clear()
        self.console_copy_buttons.clear()
        self.program_stop_buttons.clear()
        self.console_visible.clear()
        self.status_widgets.clear()
        self.setup_rows.clear()
        self.console_placeholders = {}
        self.pages = {}
        self.ip_value_label = None
        self.computer_name_label = None
        self.setup_back_button = None
        self.nav_buttons = {}
        self.nav_panel = None
        self.page_panel = None

    def _show_platform_error(self) -> None:
        self._clear_content()
        card = self._card(self.content, 28)
        card.pack(fill="both", expand=True)
        tk.Label(card, text="Windows is required", bg=self.colors["surface"], fg=self.colors["text"], font=("Segoe UI", 22, "bold")).pack(anchor="w")
        tk.Label(card, text="HarvHub AI prepares the HarvHub AI environment and controls its Windows programs. Run this application on the buyer's Windows VPS.", bg=self.colors["surface"], fg=self.colors["muted"], wraplength=850, justify="left", font=("Segoe UI", 10)).pack(anchor="w", pady=(10, 0))

    # ----------------------------- Setup screen ------------------------------
    def _show_setup(self) -> None:
        self.workspace_built = False
        if self.global_run_all_button is not None:
            self.global_run_all_button.configure(state="disabled", text="▶  RUN ALL")
        if self.global_reset_all_button is not None:
            self.global_reset_all_button.configure(state="disabled", text="↺  RESET ALL")
        if self.global_update_button is not None:
            self.global_update_button.configure(state="disabled")
        self._clear_content()
        self.setup_view_active = True
        heading = tk.Frame(self.content, bg=self.colors["bg"])
        heading.pack(fill="x", pady=(4, 12))
        heading_top = tk.Frame(heading, bg=self.colors["bg"])
        heading_top.pack(fill="x")
        tk.Label(heading_top, text="Preparing your environment", bg=self.colors["bg"], fg=self.colors["text"], font=("Segoe UI", 21, "bold")).pack(side="left", anchor="w")
        self.setup_back_button = self._button(
            heading_top, "←  BACK TO HOME", self.back_to_home, kind="quiet"
        )
        self.setup_back_button.pack(side="right", padx=(8, 0))
        tk.Label(heading, text="Setup runs quietly in this window. Detailed diagnostics remain hidden unless copied.", bg=self.colors["bg"], fg=self.colors["muted"], font=("Segoe UI", 10)).pack(anchor="w", pady=(5, 0))

        body = tk.Frame(self.content, bg=self.colors["bg"])
        body.pack(fill="both", expand=True)
        left = self._card(body, 18)
        available_width = self.root.winfo_width()
        available_height = self.root.winfo_height()
        if available_width <= 1:
            available_width = min(1320, max(360, self.root.winfo_screenwidth() - 80))
        if available_height <= 1:
            available_height = min(840, max(520, self.root.winfo_screenheight() - 100))
        setup_landscape = available_width >= 850 and available_width >= available_height
        if setup_landscape:
            left.pack(side="left", fill="y", padx=(0, 12))
            left.configure(width=320)
            left.pack_propagate(False)
        else:
            left.pack(fill="x", pady=(0, 10))
            left.pack_propagate(True)
        tk.Label(left, text="SETUP PROGRESS", bg=self.colors["surface"], fg=self.colors["muted"], font=("Segoe UI", 8, "bold")).pack(anchor="w", pady=(0, 12))
        stages = [
            ("system", "Prepare Windows folders"),
            ("python", "Install Python runtime"),
            ("packages", "Install Python modules"),
            ("chrome", "Prepare browser service"),
            ("mt5", "Install MetaTrader 5"),
            ("sources", "Deploy current HarvHub AI files"),
            ("verify", "Verify installation"),
        ]
        for key, label in stages:
            row = tk.Frame(left, bg=self.colors["surface"])
            row.pack(fill="x", pady=7)
            icon = tk.Label(row, text="○", bg=self.colors["surface"], fg=self.colors["muted"], font=("Segoe UI", 12, "bold"), width=3)
            icon.pack(side="left")
            text = tk.Label(row, text=label, bg=self.colors["surface"], fg=self.colors["text"], font=("Segoe UI", 9), wraplength=300, justify="left")
            text.pack(side="left", fill="x", expand=True)
            self.setup_rows[key] = (icon, text)
            self.setup_step_states[key] = "pending"
        tk.Label(left, text="Installation target: HarvHub AI workspace", bg=self.colors["surface"], fg=self.colors["muted"], font=("Segoe UI", 8)).pack(anchor="w", side="bottom", pady=(22, 0))

        right = self._card(body, 22)
        if setup_landscape:
            right.pack(side="left", fill="both", expand=True)
        else:
            right.pack(fill="both", expand=True)
        self.setup_title = tk.Label(right, text="Setting up your VPS", bg=self.colors["surface"], fg=self.colors["text"], font=("Segoe UI", 15, "bold"))
        self.setup_title.pack(anchor="w")
        self.setup_subtitle = tk.Label(right, text="Checking installed components...", bg=self.colors["surface"], fg=self.colors["muted"], font=("Segoe UI", 9))
        self.setup_subtitle.pack(anchor="w", pady=(5, 18))
        self.setup_progress = ttk.Progressbar(right, mode="determinate", maximum=100)
        self.setup_progress.pack(fill="x", pady=(0, 18))

        report_panel = tk.Frame(right, bg=self.colors["surface2"], padx=18, pady=18)
        report_panel.pack(fill="both", expand=True)
        tk.Label(report_panel, text="INSTALLATION REPORT", bg=self.colors["surface2"], fg=self.colors["muted"], font=("Segoe UI", 8, "bold")).pack(anchor="w")
        self.setup_status_label = tk.Label(
            report_panel,
            text="Detailed output is hidden by default.\nUse Copy Setup Report to copy the original diagnostic details when support is needed.",
            bg=self.colors["surface2"], fg=self.colors["text"], font=("Segoe UI", 11),
            justify="left", wraplength=650,
        )
        self.setup_status_label.pack(anchor="w", pady=(12, 0))
        report_controls = tk.Frame(report_panel, bg=self.colors["surface2"])
        report_controls.pack(anchor="w", pady=(18, 0))
        self.setup_copy_button = self._button(report_controls, "COPY SETUP REPORT", self.copy_setup_report, kind="quiet")
        self.setup_copy_button.pack(side="left")

        footer = tk.Frame(self.content, bg=self.colors["bg"])
        footer.pack(fill="x", pady=(12, 0))
        self.setup_action = self._button(footer, "Start setup", self.start_setup, kind="primary")
        self.setup_action.pack(side="right")
        self.setup_action.configure(state="disabled" if self.setup_started else "normal")

    def _auto_start_setup(self) -> None:
        self.auto_setup_after_id = None
        if self.setup_view_active and not self.setup_cancel_event.is_set():
            self.start_setup()

    def back_to_home(self) -> None:
        """Stop the current setup safely, then return to the workspace home page."""
        if self.auto_setup_after_id is not None:
            try:
                self.root.after_cancel(self.auto_setup_after_id)
            except tk.TclError:
                pass
            self.auto_setup_after_id = None

        if self.setup_started:
            self.setup_cancel_event.set()
            if self.setup_back_button and self.setup_back_button.winfo_exists():
                self.setup_back_button.configure(state="disabled", text="STOPPING SETUP...")
            if hasattr(self, "setup_subtitle") and self.setup_subtitle.winfo_exists():
                self.setup_subtitle.configure(text="Stopping the current operation safely before returning home...")
            if hasattr(self, "setup_action") and self.setup_action.winfo_exists():
                self.setup_action.configure(state="disabled", text="Stopping...")
            return

        self.setup_view_active = False
        self._show_workspace()

    def _check_setup_cancelled(self) -> None:
        if self.setup_cancel_event.is_set():
            raise SetupCancelled("Setup was cancelled by the user.")

    def start_setup(self) -> None:
        if self.setup_started or not self.setup_view_active:
            return
        self.setup_cancel_event.clear()
        self.setup_started = True
        self.setup_raw_lines = []
        try:
            (LOG_DIR / "setup.hclog").unlink(missing_ok=True)
        except OSError:
            pass
        if hasattr(self, "setup_action"):
            self.setup_action.configure(state="disabled", text="Installing...")
        thread = threading.Thread(target=self._setup_worker, name="harvcore-setup", daemon=True)
        thread.start()

    def _setup_emit(self, event: str, *data) -> None:
        self.events.put((event, data))

    def _setup_worker(self) -> None:
        try:
            self._check_setup_cancelled()
            if not is_windows():
                raise RuntimeError("This setup program must run on Windows.")
            if not is_admin():
                raise RuntimeError("Administrator privileges are required. Close this window and run HarvHubAI.exe as Administrator.")
            if sys.maxsize <= 2**32:
                raise RuntimeError("The manager was started with a 32-bit runtime. Build and run the Windows x64 executable.")

            for folder in (HARVHUBAI_ROOT, PYTHON_DIR, MT5_ROOT, LOG_DIR, MANAGER_DIR, DOWNLOAD_DIR):
                folder.mkdir(parents=True, exist_ok=True)
            self._setup_step("system", "done", "Windows x64 and HarvHub AI folders are ready.")
            self._check_setup_cancelled()

            python_exe = locate_python()
            if python_exe is None:
                self._setup_step("python", "running", "Downloading Python 3.13.17 x64 from python.org.")
                installer = DOWNLOAD_DIR / f"python-{PYTHON_VERSION}-amd64.exe"
                self._download(PYTHON_INSTALLER_URL, installer, "Python installer", expected_sha256=PYTHON_INSTALLER_SHA256)
                self._check_setup_cancelled()
                target = Path(os.environ.get("ProgramFiles", r"C:\Program Files")) / "Python313"
                command = [
                    str(installer), "/quiet", "InstallAllUsers=1", "PrependPath=1",
                    "Include_pip=1", "Include_test=0", "Include_launcher=1",
                    "Shortcuts=0", f"TargetDir={target}",
                ]
                self._run_command(command, "Python installer", timeout=900)
                python_exe = target / "python.exe"
                if not candidate_python_is_compatible(python_exe):
                    raise RuntimeError("Python installer finished, but a 64-bit Python 3.13 interpreter could not be verified.")
            self.python_exe = python_exe.resolve()
            self._setup_step("python", "done", f"Python runtime verified: {self.python_exe}")
            self._check_setup_cancelled()

            self._setup_step("packages", "running", "Installing the modules imported by the supplied HarvHub AI programs.")
            requirements = resource_directory() / "requirements-runtime.txt"
            if not requirements.is_file():
                raise RuntimeError(f"Runtime requirements file is missing: {requirements}")
            self._run_command(
                [str(self.python_exe), "-m", "pip", "install", "--disable-pip-version-check", "--upgrade", "pip"],
                "Upgrade pip", timeout=900,
            )
            self._run_command(
                [str(self.python_exe), "-m", "pip", "install", "--disable-pip-version-check", "--upgrade", "--no-input", "-r", str(requirements)],
                "Install HarvHub AI Python modules", timeout=1800,
            )
            self._setup_step("packages", "done", "Python dependencies installed.")
            self._check_setup_cancelled()

            chrome_exe = Path(r"C:\Program Files\Google\Chrome\Application\chrome.exe")
            if not chrome_exe.is_file():
                self._setup_step("chrome", "running", "Downloading the official Google Chrome Enterprise x64 installer.")
                chrome_msi = DOWNLOAD_DIR / "googlechromestandaloneenterprise64.msi"
                self._download(CHROME_MSI_URL, chrome_msi, "Google Chrome MSI")
                self._check_setup_cancelled()
                self._require_signer(chrome_msi, ("google llc", "google inc"))
                self._run_command(
                    ["msiexec.exe", "/i", str(chrome_msi), "/qn", "/norestart"],
                    "Install Google Chrome", timeout=1200,
                )
                if not chrome_exe.is_file():
                    # Some Enterprise MSI variants use Program Files (x86). The
                    # supplied mysqlremote.py expects the 64-bit Program Files path.
                    raise RuntimeError("Chrome installation completed but the expected 64-bit chrome.exe was not found at C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe.")
            self._setup_step("chrome", "done", f"Chrome verified at {chrome_exe}")
            self._check_setup_cancelled()

            mt5_exe = MT5_TEMPLATE / "terminal64.exe"
            if not self._mt5_terminal_ready(mt5_exe):
                self._setup_step("mt5", "running", "Downloading and installing the official MetaTrader 5 terminal.")
                mt5_setup = DOWNLOAD_DIR / "mt5setup.exe"
                self._download(MT5_SETUP_URL, mt5_setup, "MetaTrader 5 installer")
                self._check_setup_cancelled()
                self._require_signer(mt5_setup, ("metaquotes",))
                self._install_mt5_terminal(mt5_setup, mt5_exe)
            self._setup_step("mt5", "done", f"MT5 template verified at {mt5_exe}")
            self._check_setup_cancelled()

            self._setup_step("sources", "running", "Updating the HarvHub AI program files and desktop app.")
            self._deploy_sources()
            self._check_setup_cancelled()
            self._deploy_manager_executable()
            self._setup_step("sources", "done", f"Current HarvHub AI files deployed under {HARVHUBAI_ROOT}")

            self._setup_step("verify", "running", "Checking interpreter, package imports, Chrome and MT5 terminal.")
            self._check_setup_cancelled()
            self._verify_runtime()
            self._setup_step("verify", "done", "All required components passed verification.")
            self._save_state(
                setup_complete=True,
                python_executable=str(self.python_exe),
                python_version=PYTHON_VERSION,
                mt5_template=str(MT5_TEMPLATE),
                python_directory=str(PYTHON_DIR),
            )
            self._setup_emit("setup_done")
        except SetupCancelled:
            self._write_manager_log("SETUP CANCELLED by user. Already completed components remain installed and can be repaired later.")
            self._setup_emit("setup_cancelled")
        except Exception as exc:
            self._write_manager_log(f"SETUP ERROR: {exc}")
            self._setup_emit("setup_error", str(exc))

    def _setup_step(self, key: str, status: str, detail: str = "") -> None:
        self._setup_emit("setup_step", key, status, detail)
        if detail:
            self._write_manager_log(f"{key}: {detail}")

    def _download_ssl_context(self) -> ssl.SSLContext:
        """Build a verified TLS context using certifi and Windows trusted roots.

        Python's urllib does not automatically use requests' certifi bundle.
        Some Windows Python installations therefore fail HTTPS downloads even
        though the runtime dependencies installed certifi successfully. Keep
        hostname and certificate verification enabled while combining the
        public Mozilla CA bundle with certificates trusted by Windows.
        """
        context = ssl.create_default_context()

        try:
            import certifi
            context.load_verify_locations(cafile=certifi.where())
            self._setup_emit("setup_log", "HTTPS certificate bundle: certifi plus Windows/system trust.")
        except (ImportError, OSError, ssl.SSLError) as exc:
            self._setup_emit(
                "setup_log",
                f"certifi CA bundle unavailable; using the operating-system trust store ({exc}).",
            )

        # On Windows, also include certificates explicitly trusted in the
        # machine ROOT store. This supports VPS environments using a legitimate
        # managed TLS inspection/proxy root without turning verification off.
        if os.name == "nt" and hasattr(ssl, "enum_certificates"):
            try:
                pem_roots = []
                for certificate, encoding, _trust in ssl.enum_certificates("ROOT"):
                    if encoding == "x509_asn":
                        pem_roots.append(ssl.DER_cert_to_PEM_cert(certificate))
                if pem_roots:
                    context.load_verify_locations(cadata="\n".join(pem_roots))
            except (OSError, ValueError, ssl.SSLError) as exc:
                self._setup_emit("setup_log", f"Additional Windows ROOT certificates could not be loaded: {exc}")

        return context

    def _download(self, url: str, destination: Path, label: str, expected_sha256: Optional[str] = None) -> None:
        self._check_setup_cancelled()
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.is_file() and destination.stat().st_size > 1024:
            self._setup_emit("setup_log", f"{label} already downloaded; reusing cached file.")
        else:
            request = urllib.request.Request(
                url,
                headers={"User-Agent": "HarvHubAI/1.0", "Accept": "*/*"},
            )
            temp_path = destination.with_suffix(destination.suffix + ".part")
            try:
                with urllib.request.urlopen(request, timeout=45, context=self._download_ssl_context()) as response, temp_path.open("wb") as output:
                    total = int(response.headers.get("Content-Length", "0") or "0")
                    downloaded = 0
                    while True:
                        self._check_setup_cancelled()
                        chunk = response.read(1024 * 1024)
                        if not chunk:
                            break
                        output.write(chunk)
                        downloaded += len(chunk)
                        if total:
                            percent = min(100, int(downloaded * 100 / total))
                            self._setup_emit("setup_download_progress", label, percent, downloaded, total)
                temp_path.replace(destination)
            except Exception:
                try:
                    temp_path.unlink(missing_ok=True)
                except Exception:
                    pass
                raise
        self._check_setup_cancelled()
        if expected_sha256:
            actual = sha256_file(destination)
            if actual.lower() != expected_sha256.lower():
                destination.unlink(missing_ok=True)
                raise RuntimeError(f"{label} SHA-256 verification failed. Expected {expected_sha256}, received {actual}.")
            self._setup_emit("setup_log", f"{label} SHA-256 verified.")
        self._setup_emit("setup_log", f"Downloaded {label}: {destination}")

    def _require_signer(self, path: Path, expected_fragments: tuple[str, ...]) -> None:
        valid, subject = powershell_signature(path)
        if not valid:
            raise RuntimeError(f"Digital signature check failed for {path.name}: {subject}")
        lowered = subject.lower()
        if not any(fragment in lowered for fragment in expected_fragments):
            raise RuntimeError(f"Unexpected publisher for {path.name}: {subject}")
        self._setup_emit("setup_log", f"Publisher verified: {subject}")

    def _run_command(self, command: list[str], label: str, timeout: int = 900) -> None:
        self._check_setup_cancelled()
        self._setup_emit("setup_log", f"Running: {label}")
        flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            creationflags=flags,
        )
        output_queue: queue.Queue[str] = queue.Queue()

        def collect_output() -> None:
            try:
                if process.stdout:
                    for output_line in process.stdout:
                        output_queue.put(output_line.rstrip("\r\n"))
            finally:
                output_queue.put(None)

        reader = threading.Thread(target=collect_output, name=f"{label}-output", daemon=True)
        reader.start()
        started = time.monotonic()
        reader_finished = False
        while process.poll() is None:
            if self.setup_cancel_event.is_set():
                if is_windows():
                    try:
                        subprocess.run(
                            ["taskkill.exe", "/PID", str(process.pid), "/T", "/F"],
                            capture_output=True, text=True, timeout=15,
                            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                        )
                    except Exception:
                        pass
                try:
                    process.kill()
                    process.wait(timeout=10)
                except Exception:
                    pass
                raise SetupCancelled("Setup was cancelled while an installer command was running.")
            if time.monotonic() - started > timeout:
                if is_windows():
                    try:
                        subprocess.run(
                            ["taskkill.exe", "/PID", str(process.pid), "/T", "/F"],
                            capture_output=True, text=True, timeout=15,
                            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                        )
                    except Exception:
                        pass
                try:
                    process.kill()
                except Exception:
                    pass
                raise TimeoutError(f"Timed out while running: {label}")
            try:
                line = output_queue.get(timeout=0.15)
                if line is None:
                    reader_finished = True
                else:
                    self._setup_emit("setup_log", line)
            except queue.Empty:
                pass
        # Drain anything the reader collected as the process exited.
        while not reader_finished:
            try:
                line = output_queue.get(timeout=0.2)
                if line is None:
                    reader_finished = True
                else:
                    self._setup_emit("setup_log", line)
            except queue.Empty:
                if not reader.is_alive():
                    break
        if process.returncode not in (0, 3010):
            raise RuntimeError(f"{label} failed with exit code {process.returncode}.")
        self._setup_emit("setup_log", f"{label} completed (exit code {process.returncode}).")

    def _mt5_terminal_ready(self, terminal_exe: Path) -> bool:
        try:
            return terminal_exe.is_file() and terminal_exe.stat().st_size >= 1024 * 1024
        except OSError:
            return False

    def _wait_for_mt5_terminal(self, terminal_exe: Path, timeout: int) -> bool:
        started = time.monotonic()
        while time.monotonic() - started < timeout:
            self._check_setup_cancelled()
            if self._mt5_terminal_ready(terminal_exe):
                return True
            time.sleep(1)
        return self._mt5_terminal_ready(terminal_exe)

    def _show_mt5_manual_fallback_prompt(self) -> None:
        """Show the manual-installer instructions on Tk's owning UI thread."""
        prompt_done = threading.Event()

        def show_prompt() -> None:
            try:
                messagebox.showinfo(
                    "HarvHub AI - MetaTrader 5",
                    "Automatic MetaTrader 5 installation did not produce the expected terminal.\n\n"
                    "The official MetaTrader 5 setup wizard will now open.\n"
                    "In the wizard, open Settings and set the installation folder to:\n\n"
                    f"{MT5_TEMPLATE}\n\n"
                    "Complete the installation. HarvHub AI will continue automatically "
                    "when terminal64.exe is detected in that folder.",
                    parent=self.root,
                )
            finally:
                prompt_done.set()

        self.root.after(0, show_prompt)
        while not prompt_done.wait(0.1):
            self._check_setup_cancelled()

    def _install_mt5_terminal(self, installer: Path, terminal_exe: Path) -> None:
        """Install MT5, verify its actual output, and fall back to its GUI wizard.

        MetaQuotes documents /auto and /path for unattended installation. The
        web installer can nevertheless return a non-zero process status on some
        VPS images. A return code is not treated as proof of failure when the
        requested terminal executable was actually created and is usable.
        """
        MT5_TEMPLATE.mkdir(parents=True, exist_ok=True)
        command_line = f'"{installer}" /auto /path:"{MT5_TEMPLATE}"'
        self._setup_emit(
            "setup_log",
            "Running MetaTrader 5 automatic installation with the documented /auto and /path options.",
        )
        self._setup_emit("setup_log", f"MT5 destination: {MT5_TEMPLATE}")
        process_exit: Optional[int] = None
        launch_error: Optional[str] = None

        try:
            # Pass a raw Windows command line so /path:"..." reaches the
            # MetaQuotes installer exactly as documented, including its quotes.
            process = subprocess.Popen(
                command_line,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            started = time.monotonic()
            terminal_seen_since: Optional[float] = None
            while process.poll() is None:
                self._check_setup_cancelled()
                now = time.monotonic()
                if self._mt5_terminal_ready(terminal_exe):
                    if terminal_seen_since is None:
                        terminal_seen_since = now
                    elif now - terminal_seen_since >= 8:
                        # Some web-installer builds launch the terminal as a
                        # child and do not promptly exit. Once the terminal file
                        # is present and stable, do not wait on that child forever.
                        break
                else:
                    terminal_seen_since = None
                if now - started > 1200:
                    try:
                        process.kill()
                    except Exception:
                        pass
                    raise TimeoutError("MetaTrader 5 automatic installer exceeded the 20-minute limit.")
                time.sleep(1)
            process_exit = process.poll()
            if process_exit is None:
                self._setup_emit(
                    "setup_log",
                    "MetaTrader 5 terminal64.exe appeared and remained stable; continuing without waiting for the installer child to close.",
                )
            else:
                self._setup_emit("setup_log", f"MetaTrader 5 automatic installer process exited with code {process_exit}.")
        except SetupCancelled:
            raise
        except Exception as exc:
            launch_error = str(exc)
            self._setup_emit("setup_log", f"Automatic MT5 launch warning: {launch_error}")

        # Accept verified output immediately, even if the small bootstrap
        # installer returned a non-zero code or left a child process running.
        if self._mt5_terminal_ready(terminal_exe) or self._wait_for_mt5_terminal(terminal_exe, timeout=60):
            if process_exit not in (None, 0, 3010):
                self._setup_emit(
                    "setup_log",
                    f"Installer returned exit code {process_exit}, but terminal64.exe was created and passed file verification. Accepting the verified installation.",
                )
            else:
                self._setup_emit("setup_log", "MetaTrader 5 terminal64.exe was created and verified.")
            return

        self._setup_emit(
            "setup_log",
            "Automatic MT5 installation did not create the expected terminal. Starting the official interactive installer as a fallback.",
        )
        self._show_mt5_manual_fallback_prompt()
        self._check_setup_cancelled()
        try:
            os.startfile(str(installer))  # type: ignore[attr-defined]
        except Exception as exc:
            raise RuntimeError(
                "MetaTrader 5 automatic installation failed"
                + (f" (exit code {process_exit})" if process_exit is not None else "")
                + f" and the interactive installer could not be opened: {exc}"
            ) from exc

        if not self._wait_for_mt5_terminal(terminal_exe, timeout=1200):
            reason = f"automatic installer exit code {process_exit}" if process_exit is not None else f"automatic installer launch error: {launch_error}"
            raise RuntimeError(
                "MetaTrader 5 was not installed into the required HarvHub AI template folder. "
                f"Last result: {reason}. Expected file: {terminal_exe}. "
                "The interactive wizard was opened; select the exact destination shown in the setup instructions and complete it."
            )
        self._setup_emit("setup_log", "MetaTrader 5 interactive installation completed; terminal64.exe was verified.")

    def _deploy_sources(self) -> None:
        """Deploy every bundled HarvHub AI file, updating older copies safely."""
        source_root = resource_directory()
        if not source_root.is_dir():
            raise RuntimeError(f"Bundled HarvHub AI Python payload folder is missing: {source_root}")
        PYTHON_DIR.mkdir(parents=True, exist_ok=True)
        backup_root = MANAGER_DIR / "backups" / time.strftime("%Y%m%d-%H%M%S")
        copied = 0
        updated = 0
        for name in BUNDLED_PYTHON_FILES:
            self._check_setup_cancelled()
            source = source_root / name
            if not source.is_file():
                raise RuntimeError(f"Bundled Python file is missing: {name}")
            destination = PYTHON_DIR / name
            if destination.is_file() and sha256_file(destination) == sha256_file(source):
                self._setup_emit("setup_log", f"{name}: already current.")
                continue
            if destination.is_file():
                backup_root.mkdir(parents=True, exist_ok=True)
                shutil.copy2(destination, backup_root / name)
                updated += 1
            else:
                copied += 1
            temporary = destination.with_suffix(destination.suffix + ".new")
            shutil.copy2(source, temporary)
            temporary.replace(destination)
            self._setup_emit("setup_log", f"Deployed latest bundled file: {name}")

        requirements_src = source_root / "requirements-runtime.txt"
        if requirements_src.is_file():
            target = PYTHON_DIR / "requirements-runtime.txt"
            if not target.is_file() or sha256_file(target) != sha256_file(requirements_src):
                shutil.copy2(requirements_src, target)
        self._setup_emit("setup_log", f"Deployment finished. New files: {copied}; updated files: {updated}.")
        if backup_root.exists():
            self._setup_emit("setup_log", f"Previous files were backed up under {backup_root}.")

    def _deploy_manager_executable(self) -> None:
        """Install/update HarvHub AI under the HarvHub AI root and create shortcuts."""
        if getattr(sys, "frozen", False):
            running_exe = Path(sys.executable).resolve()
        else:
            source_release_exe = Path(__file__).resolve().parent / "release" / APP_EXE_NAME
            if not source_release_exe.is_file():
                self._setup_emit("setup_log", f"Source-mode run detected; {APP_EXE_NAME} is not in the release folder yet.")
                return
            running_exe = source_release_exe.resolve()

        destination = HARVHUBAI_ROOT / APP_EXE_NAME
        try:
            already_running_from_target = destination.is_file() and running_exe == destination.resolve()
            same_installed_binary = False
            if destination.is_file() and running_exe.is_file():
                try:
                    same_installed_binary = sha256_file(destination) == sha256_file(running_exe)
                except OSError:
                    same_installed_binary = False
            if not already_running_from_target and not same_installed_binary:
                if destination.is_file():
                    backup_root = MANAGER_DIR / "backups" / time.strftime("%Y%m%d-%H%M%S")
                    backup_root.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(destination, backup_root / destination.name)
                temporary = destination.with_suffix(".exe.new")
                shutil.copy2(running_exe, temporary)
                temporary.replace(destination)
                self._setup_emit("setup_log", f"HarvHub AI installed: {destination}")
            else:
                self._setup_emit("setup_log", "HarvHub AI is already installed at the current version.")

            desktop = Path(os.environ.get("USERPROFILE", str(Path.home()))) / "Desktop"
            start_menu = Path(os.environ.get("APPDATA", str(Path.home()))) / "Microsoft" / "Windows" / "Start Menu" / "Programs"
            desktop.mkdir(parents=True, exist_ok=True)
            start_menu.mkdir(parents=True, exist_ok=True)
            shortcuts = [desktop / "HarvHub AI.lnk", start_menu / "HarvHub AI.lnk"]
            for shortcut in shortcuts:
                ps_path = str(shortcut).replace("'", "''")
                ps_target = str(destination).replace("'", "''")
                ps_work = str(HARVHUBAI_ROOT).replace("'", "''")
                powershell = (
                    "$w=New-Object -ComObject WScript.Shell; "
                    f"$s=$w.CreateShortcut('{ps_path}'); "
                    f"$s.TargetPath='{ps_target}'; "
                    f"$s.WorkingDirectory='{ps_work}'; "
                    "$s.Description='HarvHub AI'; $s.Save()"
                )
                result = subprocess.run(
                    ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", powershell],
                    capture_output=True, text=True, timeout=30,
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                )
                if result.returncode != 0:
                    self._setup_emit("setup_log", f"Shortcut could not be created: {shortcut}")
                else:
                    self._setup_emit("setup_log", f"Shortcut created: {shortcut}")
        except Exception as exc:
            raise RuntimeError(f"Could not install HarvHub AI: {exc}") from exc

    def _verify_runtime(self) -> None:
        if not self.python_exe or not candidate_python_is_compatible(self.python_exe):
            raise RuntimeError("The selected Python interpreter failed the Python 3.13 x64 check.")
        import_check = (
            "import MetaTrader5, pytz, tzdata, requests, urllib3, selenium, bs4, webdriver_manager; "
            "print('IMPORTS_OK')"
        )
        result = subprocess.run(
            [str(self.python_exe), "-c", import_check],
            capture_output=True,
            text=True,
            timeout=90,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        if result.returncode != 0 or "IMPORTS_OK" not in (result.stdout or ""):
            raise RuntimeError("Python package import verification failed: " + (result.stderr or result.stdout or "unknown import error")[-1200:])
        required = [PYTHON_DIR / name for name in BUNDLED_PYTHON_FILES]
        missing = [str(path) for path in required if not path.is_file()]
        if missing:
            raise RuntimeError("The following Python source files are missing: " + ", ".join(missing))
        if not (MT5_TEMPLATE / "terminal64.exe").is_file():
            raise RuntimeError("MT5 terminal64.exe was not found in the HarvHub AI template folder.")
        chrome = Path(r"C:\Program Files\Google\Chrome\Application\chrome.exe")
        if not chrome.is_file():
            raise RuntimeError(f"Google Chrome was not found at the path required by mysqlremote.py: {chrome}")
        self._setup_emit("setup_log", f"Python verified: {self.python_exe}")
        self._setup_emit("setup_log", f"MT5 verified: {MT5_TEMPLATE / 'terminal64.exe'}")
        self._setup_emit("setup_log", f"Chrome verified: {chrome}")

    def _wait_for_file(self, path: Path, timeout: int) -> bool:
        started = time.monotonic()
        while time.monotonic() - started < timeout:
            self._check_setup_cancelled()
            if path.is_file() and path.stat().st_size > 0:
                return True
            time.sleep(2)
        return False

    # ------------------------------ Workspace --------------------------------
    def _show_workspace(self) -> None:
        if self.workspace_built:
            self._show_page("home")
            return
        self.setup_view_active = False
        self.workspace_built = True
        if self.global_run_all_button is not None:
            self.global_run_all_button.configure(state="normal", text="▶  RUN ALL")
        if self.global_reset_all_button is not None:
            self.global_reset_all_button.configure(state="normal", text="↺  RESET ALL")
        if self.global_update_button is not None:
            self.global_update_button.configure(state="normal")
        self._clear_content()

        self.workspace_frame = tk.Frame(self.content, bg=self.colors["bg"])
        self.workspace_frame.pack(fill="both", expand=True)
        self.nav_panel = tk.Frame(self.workspace_frame, bg=self.colors["surface"],
                                  highlightbackground=self.colors["border"], highlightthickness=1)
        self.page_panel = tk.Frame(self.workspace_frame, bg=self.colors["bg"])
        self._build_navigation()
        self._build_pages()
        self._configure_workspace_layout(force=True)
        if not self.mysql_poll_started:
            self.mysql_poll_started = True
            threading.Thread(target=self._mysql_status_poll_loop, name="harvhub-mysql-status", daemon=True).start()
        self._show_page("home")
        self._update_summary()
        self.root.after(350, self._refresh_all_statuses)

    def _build_navigation(self) -> None:
        nav = self.nav_panel
        # The H brand mark is reserved for the main window header only.
        separator = tk.Frame(nav, bg=self.colors["border"], height=1)
        separator.pack(fill="x", padx=12, pady=(14, 10))
        self.nav_list = tk.Frame(nav, bg=self.colors["surface"])
        self.nav_list.pack(fill="both", expand=True, padx=9, pady=(0, 12))
        self.nav_buttons = {}
        for item in NAV_ITEMS:
            btn = tk.Button(
                self.nav_list,
                text=f"{item['icon']}   {item['label']}",
                command=lambda key=item["key"]: self._show_page(key),
                bg=self.colors["surface"], fg=self.colors["text"],
                activebackground=self.colors["surface2"], activeforeground=self.colors["text"],
                relief="flat", bd=0, anchor="w", padx=12, pady=11,
                font=("Segoe UI", 9, "bold"), cursor="hand2", highlightthickness=0,
            )
            btn.bind("<Enter>", lambda event, key=item["key"]: self._on_nav_hover(key, True))
            btn.bind("<Leave>", lambda event, key=item["key"]: self._on_nav_hover(key, False))
            btn.pack(fill="x", pady=2)
            self.nav_buttons[item["key"]] = btn

    def _on_nav_hover(self, key: str, hovered: bool) -> None:
        button = self.nav_buttons.get(key)
        if not button:
            return
        if key == self.active_page_key:
            button.configure(bg=self.colors["gold_hover"] if hovered else self.colors["gold"])
        else:
            button.configure(bg=self.colors["surface2"] if hovered else self.colors["surface"])

    def _build_pages(self) -> None:
        self.pages = {}
        home = tk.Frame(self.page_panel, bg=self.colors["bg"])
        self.pages["home"] = home
        home_card = self._card(home, 28)
        home_card.pack(fill="both", expand=True)
        center = tk.Frame(home_card, bg=self.colors["surface"])
        center.place(relx=0.5, rely=0.47, anchor="center")
        tk.Label(center, text="Choose a program from the navigation to view its controls.",
                 bg=self.colors["surface"], fg=self.colors["muted"], font=("Segoe UI", 9),
                 wraplength=480, justify="center").pack()

        for item in PROGRAMS:
            page = tk.Frame(self.page_panel, bg=self.colors["bg"])
            self.pages[item["key"]] = page
            self._build_program_page(page, item)

        ip_page = tk.Frame(self.page_panel, bg=self.colors["bg"])
        self.pages["computer_ip"] = ip_page
        self._build_computer_ip_page(ip_page)

    # ----------------------- Embedded service dashboards ---------------------
    def _build_mysql_dashboard(self, parent: tk.Frame) -> None:
        panel = tk.Frame(parent, bg=self.colors["surface"])
        panel.pack(fill="both", expand=True)
        title_row = tk.Frame(panel, bg=self.colors["surface"])
        title_row.pack(fill="x", pady=(0, 4))
        tk.Label(title_row, text="MYSQL REMOTE CONNECTION MONITOR", bg=self.colors["surface"],
                 fg=self.colors["text"], font=("Segoe UI", 11, "bold")).pack(side="left")
        self.mysql_listener_badge = tk.Label(
            title_row, text="● CHECKING", bg=self.colors["surface2"],
            fg=self.colors["muted"], font=("Segoe UI", 8, "bold"), padx=10, pady=5,
        )
        self.mysql_listener_badge.pack(side="right")
        tk.Label(
            panel,
            text="Live local listener and the remote database-browser session. Channel counts are read from the bridge registry.",
            bg=self.colors["surface"], fg=self.colors["muted"],
            font=("Segoe UI", 8), wraplength=900, justify="left",
        ).pack(anchor="w", pady=(0, 14))

        cards = tk.Frame(panel, bg=self.colors["surface"])
        cards.pack(fill="x")
        metrics = [
            ("listener", "CONNECTION LISTENER", "OFFLINE"),
            ("portal", "REMOTE PORTAL SESSION", "WAITING"),
            ("allocated", "ALLOCATED CHANNELS", "0"),
            ("in_use", "CHANNELS IN USE", "0"),
            ("idle", "IDLE / UNUSED", "0"),
            ("free", "FREE CHANNEL SLOTS", str(MAX_BRIDGE_CHANNELS)),
        ]
        for index, (key, label, value) in enumerate(metrics):
            row, col = divmod(index, 3)
            card = tk.Frame(
                cards, bg=self.colors["surface2"], padx=13, pady=11,
                highlightbackground=self.colors["border"], highlightthickness=1,
            )
            card.grid(row=row, column=col, sticky="nsew", padx=4, pady=4)
            cards.grid_columnconfigure(col, weight=1)
            tk.Label(card, text=label, bg=self.colors["surface2"], fg=self.colors["muted"],
                     font=("Segoe UI", 7, "bold"), anchor="w").pack(anchor="w")
            value_label = tk.Label(card, text=value, bg=self.colors["surface2"], fg=self.colors["text"],
                                   font=("Segoe UI", 15, "bold"), anchor="w")
            value_label.pack(anchor="w", pady=(5, 0))
            self.mysql_metric_labels[key] = value_label

        details = tk.Frame(panel, bg=self.colors["surface"])
        details.pack(fill="x", pady=(11, 7))
        self.mysql_host_label = tk.Label(
            details, text=f"Listener: {BRIDGE_HOST}:{BRIDGE_PORT}", bg=self.colors["surface"],
            fg=self.colors["muted"], font=("Segoe UI", 8), anchor="w",
        )
        self.mysql_host_label.pack(anchor="w")
        self.mysql_server_label = tk.Label(
            details, text="Remote server: waiting for bridge status…", bg=self.colors["surface"],
            fg=self.colors["muted"], font=("Segoe UI", 8), anchor="w",
        )
        self.mysql_server_label.pack(anchor="w", pady=(3, 0))

        tk.Label(panel, text="CONNECTED CHANNELS", bg=self.colors["surface"],
                 fg=self.colors["text"], font=("Segoe UI", 9, "bold")).pack(anchor="w", pady=(10, 6))
        table_frame = tk.Frame(panel, bg=self.colors["surface"])
        table_frame.pack(fill="both", expand=True)
        columns = ("channel", "owner", "state", "last_used")
        tree = ttk.Treeview(table_frame, columns=columns, show="headings", height=7)
        for column, heading, width in (
            ("channel", "CHANNEL", 90), ("owner", "PROGRAM / OWNER", 250),
            ("state", "STATE", 120), ("last_used", "LAST ACTIVITY", 180),
        ):
            tree.heading(column, text=heading)
            tree.column(column, width=width, anchor="w", stretch=True)
        scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=scrollbar.set)
        tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        self.mysql_channel_tree = tree
        self.mysql_metric_labels.setdefault("listener", self.mysql_metric_labels.get("listener"))

    def _build_manual_login_dashboard(self, parent: tk.Frame) -> None:
        panel = tk.Frame(parent, bg=self.colors["surface"])
        panel.pack(fill="both", expand=True)
        title_row = tk.Frame(panel, bg=self.colors["surface"])
        title_row.pack(fill="x")
        tk.Label(title_row, text="MANUAL BROKER LOGIN", bg=self.colors["surface"],
                 fg=self.colors["text"], font=("Segoe UI", 11, "bold")).pack(side="left")
        self.manual_login_count_label = tk.Label(
            title_row, text="0 NEED ATTENTION", bg=self.colors["surface2"],
            fg=self.colors["muted"], font=("Segoe UI", 8, "bold"), padx=10, pady=5,
        )
        self.manual_login_count_label.pack(side="right")
        self.manual_login_message = tk.Label(
            panel, text="All accounts and programmes need no manual login.",
            bg=self.colors["surface"], fg=self.colors["muted"],
            font=("Segoe UI", 9), anchor="w", justify="left", wraplength=900,
        )
        self.manual_login_message.pack(fill="x", pady=(10, 12))

        self.manual_login_tabs_frame = tk.Frame(panel, bg=self.colors["surface"])
        self.manual_login_tab_buttons = {}
        for kind, label in (("SA", "Accounts"), ("PRG", "Programme")):
            button = self._button(
                self.manual_login_tabs_frame, label,
                lambda selected=kind: self._select_manual_login_tab(selected),
                kind="primary" if kind == self.manual_login_tab else "quiet",
            )
            button.pack(side="left", padx=(0, 7))
            self.manual_login_tab_buttons[kind] = button

        list_shell = tk.Frame(panel, bg=self.colors["surface"])
        list_shell.pack(fill="both", expand=True, pady=(8, 0))
        self.manual_login_list_shell = list_shell
        canvas = tk.Canvas(list_shell, bg=self.colors["surface"], highlightthickness=0, borderwidth=0)
        scrollbar = ttk.Scrollbar(list_shell, orient="vertical", command=canvas.yview)
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        self.manual_login_canvas = canvas
        self.manual_login_items_frame = tk.Frame(canvas, bg=self.colors["surface"])
        self.manual_login_canvas_window = canvas.create_window(
            (0, 0), window=self.manual_login_items_frame, anchor="nw",
        )
        self.manual_login_items_frame.bind(
            "<Configure>", lambda _event: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        canvas.bind(
            "<Configure>", lambda event: canvas.itemconfigure(self.manual_login_canvas_window, width=event.width)
        )
        self._refresh_manual_login_dashboard()

    def _select_manual_login_tab(self, kind: str) -> None:
        self.manual_login_tab = kind if kind in ("SA", "PRG") else "SA"
        self._refresh_manual_login_dashboard()

    @staticmethod
    def _manual_login_key(item: dict) -> tuple[str, str, str]:
        return (str(item.get("kind", "SA")).upper(),
                str(item.get("owner_id", "")), str(item.get("account_id", "")))

    def _refresh_manual_login_dashboard(self) -> None:
        if not hasattr(self, "manual_login_items_frame"):
            return
        with self.manual_login_lock:
            manual_items = list(self.manual_login_items.values())
        accounts = [item for item in manual_items if str(item.get("kind", "SA")).upper() == "SA"]
        programmes = [item for item in manual_items if str(item.get("kind", "SA")).upper() == "PRG"]
        total = len(accounts) + len(programmes)
        # Do not leave the user on an empty Accounts tab when the pending item
        # is a Programme request (or vice versa).
        if self.manual_login_tab == "SA" and not accounts and programmes:
            self.manual_login_tab = "PRG"
        elif self.manual_login_tab == "PRG" and not programmes and accounts:
            self.manual_login_tab = "SA"
        self.manual_login_count_label.configure(text=f"{total} NEED ATTENTION")
        tab_labels = {
            "SA": f"Accounts ({len(accounts)})",
            "PRG": f"Programme ({len(programmes)})",
        }
        for kind, button in getattr(self, "manual_login_tab_buttons", {}).items():
            button.configure(text=tab_labels.get(kind, kind))
        if hasattr(self, "manual_login_tabs_frame"):
            if total:
                if not self.manual_login_tabs_frame.winfo_manager():
                    list_shell = getattr(self, "manual_login_list_shell", None)
                    if list_shell is not None and list_shell.winfo_exists():
                        self.manual_login_tabs_frame.pack(fill="x", pady=(0, 3), before=list_shell)
                    else:
                        self.manual_login_tabs_frame.pack(fill="x", pady=(0, 3))
                self.manual_login_message.configure(
                    text="These accounts or programmes need a manual MT5 login. Open the terminal, sign in, then confirm the credentials here."
                )
            else:
                self.manual_login_tabs_frame.pack_forget()
                self.manual_login_message.configure(
                    text="All accounts and programmes need no manual login."
                )
        for kind, button in getattr(self, "manual_login_tab_buttons", {}).items():
            button.configure(bg=self.colors["gold"] if kind == self.manual_login_tab else self.colors["surface2"])
        nav_button = getattr(self, "nav_buttons", {}).get("brokerdetails")
        if nav_button:
            broker_item = self._program("brokerdetails") or {}
            nav_text = f"{broker_item.get('icon', '▣')}   {broker_item.get('label', 'Broker Details')}"
            if total:
                nav_text += f"   ·   {total} NEED ATTENTION"
            nav_button.configure(text=nav_text)
        for child in self.manual_login_items_frame.winfo_children():
            child.destroy()
        selected = accounts if self.manual_login_tab == "SA" else programmes
        if total and not selected:
            tk.Label(
                self.manual_login_items_frame,
                text=("No sub-accounts currently need manual login." if self.manual_login_tab == "SA"
                      else "No programmes currently need manual login."),
                bg=self.colors["surface"], fg=self.colors["muted"],
                font=("Segoe UI", 9), padx=14, pady=18,
            ).pack(anchor="w")
            return
        for index, item in enumerate(selected, start=1):
            self._render_manual_login_item(index, item)

    def _render_manual_login_item(self, index: int, item: dict) -> None:
        card = self._card(self.manual_login_items_frame, 13)
        card.pack(fill="x", expand=True, pady=(0, 8))
        kind, owner_id, account_id = self._manual_login_key(item)
        title = (f"{index}. Sub-account {account_id}  ·  Owner {owner_id}" if kind == "SA"
                 else f"{index}. Programme {account_id}  ·  Owner {owner_id}")
        tk.Label(card, text=title, bg=self.colors["surface"], fg=self.colors["text"],
                 font=("Segoe UI", 9, "bold")).pack(anchor="w", pady=(0, 7))
        fields = (
            ("Broker", item.get("broker", "N/A"), False),
            ("Server", item.get("server", "N/A"), False),
            ("Login ID", item.get("login", ""), True),
            ("Password", item.get("password", ""), True),
        )
        for label_text, value, copyable in fields:
            row = tk.Frame(card, bg=self.colors["surface"])
            row.pack(fill="x", pady=2)
            tk.Label(row, text=f"{label_text}:", width=11, anchor="w", bg=self.colors["surface"],
                     fg=self.colors["muted"], font=("Segoe UI", 8, "bold")).pack(side="left")
            display = "••••••••" if label_text == "Password" and value else (str(value) if value else "(empty)")
            tk.Label(row, text=display, bg=self.colors["surface"], fg=self.colors["text"],
                     font=("Segoe UI", 8), anchor="w").pack(side="left", fill="x", expand=True)
            if copyable:
                self._button(row, "COPY", lambda v=str(value): self._copy_text(v), kind="quiet").pack(side="right")
        actions = tk.Frame(card, bg=self.colors["surface"])
        actions.pack(fill="x", pady=(9, 0))
        terminal = str(item.get("terminal_exe") or "")
        self._button(
            actions, "▣  ACTIVATE TERMINAL",
            lambda path=terminal: self._activate_manual_terminal(path), kind="primary",
        ).pack(side="left", padx=(0, 7))
        self._button(
            actions, "INVALID CREDENTIALS",
            lambda k=(kind, owner_id, account_id): self._set_manual_login_decision(k, "invalid"),
            kind="danger",
        ).pack(side="right", padx=(7, 0))
        self._button(
            actions, "VALID CREDENTIALS",
            lambda k=(kind, owner_id, account_id): self._set_manual_login_decision(k, "valid"),
            kind="success",
        ).pack(side="right")

    def _activate_manual_terminal(self, terminal_exe: str) -> None:
        if not terminal_exe:
            messagebox.showerror("Terminal unavailable", "No terminal path was supplied for this login request.", parent=self.root)
            return
        try:
            if os.path.isfile(terminal_exe):
                os.startfile(terminal_exe)
            else:
                raise FileNotFoundError(terminal_exe)
            self._append_console("brokerdetails", f"[MANAGER] Activated MT5 terminal: {terminal_exe}", "success")
        except Exception as exc:
            messagebox.showerror("Could not activate terminal", str(exc), parent=self.root)

    def _set_manual_login_decision(self, key: tuple[str, str, str], decision: str) -> None:
        with self.manual_login_lock:
            self.manual_login_decisions[key] = decision
        kind, owner_id, account_id = key
        self._refresh_manual_login_dashboard()
        self._ui_log("brokerdetails", f"[MANAGER] {decision.title()} credentials selected for {kind} {account_id}; waiting for the broker monitor to verify.", "success" if decision == "valid" else "error")

    # ----------------------------- Local IPC ---------------------------------
    @staticmethod
    def _recv_exact(sock: socket.socket, size: int) -> bytes:
        chunks = bytearray()
        while len(chunks) < size:
            part = sock.recv(size - len(chunks))
            if not part:
                raise ConnectionError("socket closed before full frame arrived")
            chunks.extend(part)
        return bytes(chunks)

    def _control_server_loop(self) -> None:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((CONTROL_HOST, CONTROL_PORT))
            server.listen(16)
            server.settimeout(1.0)
            self.control_server_socket = server
            self._write_manager_log(f"Local manager control listener started at {CONTROL_HOST}:{CONTROL_PORT}.")
            while not self.control_server_stop.is_set():
                try:
                    conn, _address = server.accept()
                except socket.timeout:
                    continue
                except OSError:
                    break
                threading.Thread(target=self._handle_control_client, args=(conn,), daemon=True).start()
        except OSError as exc:
            self._write_manager_log(f"Local manager control listener could not bind {CONTROL_HOST}:{CONTROL_PORT}: {exc}")
        finally:
            try:
                server.close()
            except OSError:
                pass

    def _handle_control_client(self, conn: socket.socket) -> None:
        try:
            conn.settimeout(3.0)
            (length,) = struct.unpack(">I", self._recv_exact(conn, 4))
            if length <= 0 or length > 4 * 1024 * 1024:
                raise ValueError(f"invalid IPC request size: {length}")
            request = json.loads(self._recv_exact(conn, length).decode("utf-8"))
            response = self._handle_control_request(request)
            encoded = json.dumps(response, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            conn.sendall(struct.pack(">I", len(encoded)) + encoded)
        except Exception:
            try:
                encoded = b'{"ok":false,"error":"invalid manager IPC request"}'
                conn.sendall(struct.pack(">I", len(encoded)) + encoded)
            except Exception:
                pass
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def _handle_control_request(self, request: dict) -> dict:
        if not isinstance(request, dict):
            return {"ok": False, "error": "request must be an object"}
        op = str(request.get("op") or "").strip().lower()
        if op == "manual_login_upsert":
            item = request.get("item")
            if not isinstance(item, dict):
                return {"ok": False, "error": "item is missing"}
            key = self._manual_login_key(item)
            with self.manual_login_lock:
                self.manual_login_items[key] = dict(item)
            self.events.put(("manual_login_refresh", ()))
            return {"ok": True}
        if op == "manual_login_remove":
            key = (str(request.get("kind", "SA")).upper(),
                   str(request.get("owner_id", "")), str(request.get("account_id", "")))
            with self.manual_login_lock:
                self.manual_login_items.pop(key, None)
                self.manual_login_decisions.pop(key, None)
            self.events.put(("manual_login_refresh", ()))
            return {"ok": True}
        if op == "manual_login_take_decision":
            key = (str(request.get("kind", "SA")).upper(),
                   str(request.get("owner_id", "")), str(request.get("account_id", "")))
            with self.manual_login_lock:
                decision = self.manual_login_decisions.pop(key, None)
            return {"ok": True, "decision": decision}
        if op == "manager_ping":
            return {"ok": True, "name": APP_TITLE}
        return {"ok": False, "error": "unknown operation"}

    # ----------------------- MySQL bridge status polling ----------------------
    def _request_bridge_status(self) -> Optional[dict]:
        payload = json.dumps({"op": "bridge_status"}).encode("utf-8")
        try:
            with socket.create_connection((BRIDGE_HOST, BRIDGE_PORT), timeout=1.2) as sock:
                sock.settimeout(2.0)
                sock.sendall(struct.pack(">I", len(payload)) + payload)
                (length,) = struct.unpack(">I", self._recv_exact(sock, 4))
                if length <= 0 or length > 8 * 1024 * 1024:
                    return None
                response = json.loads(self._recv_exact(sock, length).decode("utf-8"))
                return response if isinstance(response, dict) else None
        except (OSError, ValueError, json.JSONDecodeError, ConnectionError):
            return None

    def _mysql_status_poll_loop(self) -> None:
        while not self.mysql_poll_stop.is_set() and not self.closing:
            status = self._request_bridge_status() if self._port_open() else None
            self.events.put(("mysql_metrics", status or {"status": "offline"}))
            self.mysql_poll_stop.wait(2.0)

    def _apply_mysql_metrics(self, status: Optional[dict]) -> None:
        if not hasattr(self, "mysql_metric_labels") or not self.mysql_metric_labels:
            self.mysql_metrics = status or {}
            return
        status = status if isinstance(status, dict) else {"status": "offline"}
        online = status.get("status") == "success"
        listener = "LISTENING" if online else "OFFLINE"
        portal = "READY" if online and status.get("browser_connected") else ("STARTING" if online else "OFFLINE")
        values = {
            "listener": listener,
            "portal": portal,
            "allocated": str(status.get("allocated_channels", 0)) if online else "0",
            "in_use": str(status.get("in_use_channels", 0)) if online else "0",
            "idle": str(status.get("idle_channels", 0)) if online else "0",
            "free": str(status.get("free_slots", MAX_BRIDGE_CHANNELS)) if online else str(MAX_BRIDGE_CHANNELS),
        }
        for key, value in values.items():
            label = self.mysql_metric_labels.get(key)
            if label is not None:
                label.configure(text=value)
        if hasattr(self, "mysql_listener_badge"):
            self.mysql_listener_badge.configure(
                text=("● LISTENING" if online else "● OFFLINE"),
                fg=(self.colors["green"] if online else self.colors["red"]),
            )
        if hasattr(self, "mysql_host_label"):
            self.mysql_host_label.configure(
                text=(f"Listener: {status.get('host', BRIDGE_HOST)}:{status.get('port', BRIDGE_PORT)}  ·  capacity {status.get('max_channels', MAX_BRIDGE_CHANNELS)}"
                      if online else f"Listener: {BRIDGE_HOST}:{BRIDGE_PORT}  ·  waiting for MySQL Remote"))
        if hasattr(self, "mysql_server_label"):
            self.mysql_server_label.configure(
                text=(f"Remote server: {status.get('selected_server') or 'not selected'}  ·  portal session: {portal}"
                      if online else "Remote server: not connected"))
        tree = self.mysql_channel_tree
        if tree is not None:
            for child in tree.get_children():
                tree.delete(child)
            for channel in status.get("channels", []) if online else []:
                channel_id = channel.get("channel_id", "—")
                owner = channel.get("owner", "unknown")
                state = "IN USE" if channel.get("in_use") else "IDLE / FREE"
                stamp = channel.get("last_used_at")
                try:
                    last_used = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(stamp)))
                except (TypeError, ValueError, OSError):
                    last_used = "—"
                tree.insert("", "end", values=(channel_id, owner, state, last_used))
        self.mysql_metrics = dict(status)

    def _build_program_page(self, page: tk.Frame, item: dict) -> None:
        if item["key"] == "brokerdetails":
            # Broker Details needs room for both the manual-login attention list
            # and its live console. Its content is taller than the fixed workspace
            # viewport, so make this one program page vertically scrollable.
            scroll_shell = tk.Frame(page, bg=self.colors["bg"])
            scroll_shell.pack(fill="both", expand=True)
            page_canvas = tk.Canvas(
                scroll_shell, bg=self.colors["bg"], highlightthickness=0,
                borderwidth=0, yscrollincrement=24,
            )
            page_scrollbar = ttk.Scrollbar(
                scroll_shell, orient="vertical", command=page_canvas.yview,
            )
            page_canvas.configure(yscrollcommand=page_scrollbar.set)
            page_canvas.pack(side="left", fill="both", expand=True)
            page_scrollbar.pack(side="right", fill="y")
            wrapper = tk.Frame(page_canvas, bg=self.colors["bg"])
            page_window = page_canvas.create_window((0, 0), window=wrapper, anchor="nw")
            wrapper.bind(
                "<Configure>",
                lambda _event, canvas=page_canvas: canvas.configure(scrollregion=canvas.bbox("all")),
            )
            page_canvas.bind(
                "<Configure>",
                lambda event, canvas=page_canvas, window=page_window:
                    canvas.itemconfigure(window, width=event.width),
            )
            self.program_page_canvases[item["key"]] = page_canvas
        else:
            wrapper = tk.Frame(page, bg=self.colors["bg"])
            wrapper.pack(fill="both", expand=True)
        header = self._card(wrapper, 18)
        header.pack(fill="x", pady=(0, 12))

        title_row = tk.Frame(header, bg=self.colors["surface"])
        title_row.pack(fill="x")
        icon_box = tk.Label(title_row, text=item["icon"], bg=self.colors["surface2"],
                            fg=self.colors["gold"], font=("Segoe UI Symbol", 22, "bold"),
                            width=3, height=2)
        icon_box.pack(side="left", padx=(0, 13))
        title_area = tk.Frame(title_row, bg=self.colors["surface"])
        title_area.pack(side="left", fill="x", expand=True)
        tk.Label(title_area, text=item["label"], bg=self.colors["surface"], fg=self.colors["text"],
                 font=("Segoe UI", 15, "bold")).pack(anchor="w")
        tk.Label(title_area, text=item["description"], bg=self.colors["surface"], fg=self.colors["muted"],
                 font=("Segoe UI", 9), wraplength=760, justify="left").pack(anchor="w", pady=(4, 0))
        status = tk.Label(title_row, text="STOPPED", bg=self.colors["surface2"], fg=self.colors["muted"],
                          padx=12, pady=7, font=("Segoe UI", 8, "bold"))
        status.pack(side="right", padx=(10, 0))
        self.status_widgets[item["key"]] = status

        controls = tk.Frame(header, bg=self.colors["surface"])
        controls.pack(fill="x", pady=(15, 0))
        tk.Label(
            controls, text="Execution type", bg=self.colors["surface"],
            fg=self.colors["muted"], font=("Segoe UI", 8, "bold"),
        ).pack(side="left", padx=(0, 6))
        execution_var = tk.StringVar(
            value=("Loop" if item["key"] == "mysqlremote" else
                   self.program_execution_types.get(item["key"], self.global_execution_type).title())
        )
        self.program_execution_vars[item["key"]] = execution_var
        execution_combo = ttk.Combobox(
            controls, textvariable=execution_var,
            values=(("Loop",) if item["key"] == "mysqlremote" else ("Loop", "Once")),
            state="readonly", width=7, style="HarvHub.TCombobox",
        )
        execution_combo.pack(side="left", padx=(0, 10))
        execution_combo.bind(
            "<<ComboboxSelected>>",
            lambda event, key=item["key"], var=execution_var:
                self._on_program_execution_changed(key, var),
        )
        play = self._button(controls, "▶  PLAY", lambda key=item["key"]: self.start_program(key), kind="success")
        play.pack(side="left", padx=(0, 7))
        stop = self._button(controls, "■  STOP", lambda key=item["key"]: self.stop_program(key), kind="danger")
        self.program_stop_buttons[item["key"]] = stop
        stop.pack_forget()
        toggle = self._button(controls, "SHOW CONSOLE", lambda key=item["key"]: self.toggle_console(key), kind="quiet")
        toggle.pack(side="left", padx=(0, 7))
        self.console_toggle_buttons[item["key"]] = toggle
        copy = self._button(controls, "COPY CONSOLE RESULT", lambda key=item["key"]: self.copy_console_result(key), kind="quiet")
        copy.pack(side="left", padx=(0, 7))
        self.console_copy_buttons[item["key"]] = copy
        clear = self._button(controls, "CLEAR", lambda key=item["key"]: self.clear_console(key), kind="quiet")
        clear.pack(side="right")

        placeholder = self._card(wrapper, 22)
        placeholder.pack(fill="both", expand=True, pady=(0, 10))
        if item["key"] == "mysqlremote":
            self._build_mysql_dashboard(placeholder)
        elif item["key"] == "brokerdetails":
            self._build_manual_login_dashboard(placeholder)
        else:
            emblem = tk.Frame(placeholder, bg=self.colors["surface"])
            emblem.place(relx=0.5, rely=0.44, anchor="center")
            tk.Label(emblem, text=item["icon"], bg=self.colors["surface2"], fg=self.colors["gold"],
                     font=("Segoe UI Symbol", 34, "bold"), width=3, height=1, pady=8).pack(pady=(0, 12))
            tk.Label(emblem, text="CONSOLE HIDDEN", bg=self.colors["surface"], fg=self.colors["text"],
                     font=("Segoe UI", 11, "bold")).pack()
            tk.Label(emblem, text="The console is hidden by default. Use Show Console to view the original output.",
                     bg=self.colors["surface"], fg=self.colors["muted"], font=("Segoe UI", 9),
                     wraplength=520, justify="center").pack(pady=(5, 0))

        console_card = self._card(wrapper, 10)
        self.console_cards[item["key"]] = console_card
        console_top = tk.Frame(console_card, bg=self.colors["surface"])
        console_top.pack(fill="x", pady=(0, 7))
        tk.Label(console_top, text="FULL CONSOLE OUTPUT", bg=self.colors["surface"], fg=self.colors["muted"],
                 font=("Segoe UI", 8, "bold")).pack(side="left")
        tk.Label(console_top, text="Original diagnostic text", bg=self.colors["surface"], fg=self.colors["muted"],
                 font=("Segoe UI", 8)).pack(side="right")
        console = ScrolledText(
            console_card, height=14, wrap="word", state="disabled",
            bg=self.colors["console_bg"], fg=self.colors["console_fg"],
            insertbackground=self.colors["console_fg"], selectbackground=self.colors["gold"],
            font=("Consolas", 9), relief="flat", padx=12, pady=10,
        )
        console.pack(fill="both", expand=True)
        console.tag_configure("dim", foreground=self.colors["console_dim"])
        console.tag_configure("error", foreground="#ff9388")
        console.tag_configure("success", foreground="#8fd5a8")
        self.console_widgets[item["key"]] = console
        self.console_visible[item["key"]] = False
        # Do not seed a fake "Ready." result. The console stays empty until
        # the manager or the actual child process produces a real log line.
        if item.get("warning") and not self.raw_logs.get(item["key"]):
            self.raw_logs[item["key"]] = ["NOTICE: " + item["warning"]]
        for line in self.raw_logs[item["key"]][-4000:]:
            tag = "error" if line.startswith("NOTICE:") or "Traceback" in line or "ERROR" in line else "dim" if line.startswith("[") else ""
            console.configure(state="normal")
            console.insert("end", line + "\n", tag)
            console.configure(state="disabled")
        console.see("end")
        self.console_placeholders = getattr(self, "console_placeholders", {})
        self.console_placeholders[item["key"]] = placeholder

    def _configure_global_header(
        self,
        header: tk.Frame,
        left: tk.Frame,
        right: tk.Frame,
    ) -> None:
        """Keep the app title and global actions together in one responsive header."""
        try:
            if not header.winfo_exists():
                return
            width = header.winfo_width()
            if width <= 1:
                width = self.root.winfo_width() - 36

            left.pack_forget()
            right.pack_forget()
            compact = width < 760
            if hasattr(self, "global_execution_combo"):
                self.global_execution_combo.configure(width=6 if compact else 7)
            for button in (
                self.global_run_all_button,
                self.global_stop_all_button,
                self.global_reset_all_button,
                self.global_update_button,
            ):
                if button is not None and button.winfo_exists():
                    button.configure(
                        padx=8 if compact else 12,
                        pady=7 if compact else 9,
                        font=("Segoe UI", 8 if compact else 9, "bold"),
                    )

            if compact:
                left.pack(fill="x", padx=18, pady=(10, 5))
                right.pack(fill="x", padx=18, pady=(0, 10))
            else:
                left.pack(side="left", fill="y", padx=18, pady=11)
                right.pack(side="right", padx=16, pady=12)
        except tk.TclError:
            return

    def _running_program_keys(self) -> list[str]:
        with self.process_lock:
            return [
                key for key, entry in self.processes.items()
                if entry.process.poll() is None
            ]

    def _is_process_running(self, key: str) -> bool:
        with self.process_lock:
            entry = self.processes.get(key)
            return bool(entry and entry.process.poll() is None)

    def _update_global_actions(self) -> None:
        """Show Stop All only while at least one managed program is running."""
        button = self.global_stop_all_button
        if button is None:
            return
        try:
            if not button.winfo_exists():
                return
            if self._running_program_keys():
                button.configure(state="normal")
                if button.winfo_manager() != "pack":
                    next_button = self.global_reset_all_button or self.global_update_button
                    button.pack(side="left", before=next_button, padx=(0, 7))
            else:
                button.pack_forget()
        except tk.TclError:
            return

    def _set_global_run_all_buttons(self, state: str, text: str) -> None:
        button = self.global_run_all_button
        if button is None:
            return
        try:
            if button.winfo_exists():
                button.configure(state=state, text=text)
        except tk.TclError:
            pass

    def _build_computer_ip_page(self, page: tk.Frame) -> None:
        card = self._card(page, 24)
        card.pack(fill="both", expand=True)
        top = tk.Frame(card, bg=self.colors["surface"])
        top.pack(fill="x")
        tk.Label(top, text="⌁", bg=self.colors["surface2"], fg=self.colors["gold"],
                 font=("Segoe UI Symbol", 25, "bold"), width=3, height=2).pack(side="left", padx=(0, 13))
        title = tk.Frame(top, bg=self.colors["surface"])
        title.pack(side="left", fill="x", expand=True)
        tk.Label(title, text="Computer IP", bg=self.colors["surface"], fg=self.colors["text"],
                 font=("Segoe UI", 16, "bold")).pack(anchor="w")
        tk.Label(title, text="Local network identity of this Windows computer.", bg=self.colors["surface"],
                 fg=self.colors["muted"], font=("Segoe UI", 9)).pack(anchor="w", pady=(4, 0))

        info = self._card(card, 18)
        info.pack(fill="x", pady=(24, 14))
        tk.Label(info, text="LOCAL IPV4 ADDRESS", bg=self.colors["surface"], fg=self.colors["muted"],
                 font=("Segoe UI", 8, "bold")).pack(anchor="w")
        self.ip_value_label = tk.Label(info, text="Checking...", bg=self.colors["surface"], fg=self.colors["text"],
                                       font=("Segoe UI", 23, "bold"))
        self.ip_value_label.pack(anchor="w", pady=(6, 14))
        tk.Label(info, text="COMPUTER NAME", bg=self.colors["surface"], fg=self.colors["muted"],
                 font=("Segoe UI", 8, "bold")).pack(anchor="w")
        self.computer_name_label = tk.Label(info, text=socket.gethostname(), bg=self.colors["surface"],
                                            fg=self.colors["text"], font=("Segoe UI", 11, "bold"))
        self.computer_name_label.pack(anchor="w", pady=(5, 0))

        actions = tk.Frame(card, bg=self.colors["surface"])
        actions.pack(fill="x")
        self._button(actions, "↻  REFRESH IP", self.refresh_computer_ip, kind="primary").pack(side="left", padx=(0, 8))
        self.ip_copy_button = self._button(actions, "COPY IP", self.copy_computer_ip, kind="quiet")
        self.ip_copy_button.pack(side="left")
        self.refresh_computer_ip()

    def get_computer_ipv4(self) -> str:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
                probe.settimeout(1.5)
                probe.connect(("8.8.8.8", 80))
                value = probe.getsockname()[0]
                if value and not value.startswith("127."):
                    return value
        except OSError:
            pass
        try:
            candidates = socket.gethostbyname_ex(socket.gethostname())[2]
            for value in candidates:
                if value and not value.startswith("127.") and not value.startswith("169.254."):
                    return value
        except OSError:
            pass
        return "Not available"

    def refresh_computer_ip(self) -> None:
        value = self.get_computer_ipv4()
        if self.ip_value_label and self.ip_value_label.winfo_exists():
            self.ip_value_label.configure(text=value)
        if hasattr(self, "ip_copy_button"):
            self.ip_copy_button.configure(state="normal" if value != "Not available" else "disabled")

    def copy_computer_ip(self) -> None:
        value = self.ip_value_label.cget("text") if self.ip_value_label else ""
        if value and value != "Not available":
            self._copy_text(value, getattr(self, "ip_copy_button", None))

    def _on_root_configure(self, event=None) -> None:
        if event is not None and getattr(event, "widget", None) is not self.root:
            return
        if not self.workspace_built:
            return
        if self._layout_after_id is not None:
            try:
                self.root.after_cancel(self._layout_after_id)
            except tk.TclError:
                pass
        self._layout_after_id = self.root.after(140, self._configure_workspace_layout)

    def _configure_workspace_layout(self, force: bool = False) -> None:
        if not self.workspace_built or not hasattr(self, "nav_panel"):
            return
        width = self.root.winfo_width()
        height = self.root.winfo_height()
        if width <= 1:
            width = int(self.root.winfo_reqwidth() or 760)
        if height <= 1:
            height = int(self.root.winfo_reqheight() or 600)
        landscape = width >= 720 and width >= height
        if not force and self._workspace_landscape == landscape:
            if not landscape:
                self._layout_mobile_nav(width)
            return
        self._workspace_landscape = landscape
        self.nav_panel.pack_forget()
        self.page_panel.pack_forget()
        for child in self.nav_list.winfo_children():
            child.grid_forget()
            child.pack_forget()
        if landscape:
            self.nav_panel.configure(width=238)
            self.nav_panel.pack(side="left", fill="y", padx=(0, 14))
            self.nav_panel.pack_propagate(False)
            for row, item in enumerate(NAV_ITEMS):
                button = self.nav_buttons[item["key"]]
                button.grid(row=row, column=0, sticky="ew", pady=3)
            self.nav_list.grid_columnconfigure(0, weight=1)
            self.page_panel.pack(side="left", fill="both", expand=True)
        else:
            self.nav_panel.configure(width=0)
            self.nav_panel.pack(side="top", fill="x", pady=(0, 10))
            self.nav_panel.pack_propagate(True)
            self._layout_mobile_nav(width)
            self.page_panel.pack(side="top", fill="both", expand=True)
        self._update_nav_styles()

    def _layout_mobile_nav(self, width: int) -> None:
        if not hasattr(self, "nav_list"):
            return
        for child in self.nav_list.winfo_children():
            child.grid_forget()
            child.pack_forget()
        columns = 1 if width < 480 else 2
        for index, item in enumerate(NAV_ITEMS):
            button = self.nav_buttons[item["key"]]
            button.configure(anchor="center" if columns > 1 else "w", padx=9, pady=9)
            button.grid(row=index // columns, column=index % columns, sticky="ew", padx=3, pady=3)
        for column in range(columns):
            self.nav_list.grid_columnconfigure(column, weight=1)

    def _update_nav_styles(self) -> None:
        for key, button in self.nav_buttons.items():
            selected = key == self.active_page_key
            if selected:
                button.configure(bg=self.colors["gold"], fg="#ffffff",
                                 activebackground=self.colors["gold_hover"], activeforeground="#ffffff")
            else:
                button.configure(bg=self.colors["surface"], fg=self.colors["text"],
                                 activebackground=self.colors["surface2"], activeforeground=self.colors["text"])

    def _show_page(self, key: str) -> None:
        if key not in self.pages:
            return
        for page in self.pages.values():
            page.pack_forget()
        self.active_page_key = key
        self.pages[key].pack(fill="both", expand=True)
        self._update_nav_styles()
        if key == "computer_ip":
            self.refresh_computer_ip()

    def toggle_console(self, key: str) -> None:
        """Show the console while keeping the Broker Details dashboard accessible.

        Broker Details is vertically scrollable so its manual-login list remains
        mounted while Show Console scrolls down to the live output. Other program
        pages swap their hidden placeholder for the console.
        """
        card = self.console_cards.get(key)
        button = self.console_toggle_buttons.get(key)
        placeholder = getattr(self, "console_placeholders", {}).get(key)
        if not card or not button:
            return
        visible = self.console_visible.get(key, False)
        keep_dashboard_visible = key == "brokerdetails"
        if visible:
            card.pack_forget()
            if placeholder and placeholder.winfo_exists() and not keep_dashboard_visible:
                placeholder.pack(fill="both", expand=True, pady=(0, 10))
            self.console_visible[key] = False
            button.configure(text="SHOW CONSOLE")
            if keep_dashboard_visible:
                canvas = self.program_page_canvases.get(key)
                if canvas is not None:
                    canvas.yview_moveto(0.0)
        else:
            if placeholder and placeholder.winfo_exists() and not keep_dashboard_visible:
                placeholder.pack_forget()
            card.pack(fill="both", expand=True, pady=(0, 8))
            self.console_visible[key] = True
            button.configure(text="HIDE CONSOLE")
            self._sync_console_widget(key)
            if keep_dashboard_visible:
                self.root.after_idle(lambda selected=key: self._scroll_program_console_into_view(selected))

    def _scroll_program_console_into_view(self, key: str) -> None:
        """Scroll the Broker Details page to its console without removing the dashboard."""
        canvas = self.program_page_canvases.get(key)
        card = self.console_cards.get(key)
        if canvas is None or card is None:
            return
        try:
            canvas.update_idletasks()
            bounds = canvas.bbox("all")
            if not bounds:
                return
            total_height = max(1, bounds[3] - bounds[1])
            target_y = max(0, card.winfo_y())
            canvas.yview_moveto(min(1.0, target_y / total_height))
        except tk.TclError:
            pass

    def _sync_console_widget(self, key: str) -> None:
        """Replay retained output efficiently without thousands of Tk calls."""
        widget = self.console_widgets.get(key)
        if not widget:
            return
        try:
            lines = self.raw_logs.get(key, [])[-4000:]
            widget.configure(state="normal")
            widget.delete("1.0", "end")
            # Group adjacent lines with the same style so a long console opens
            # in a small number of Tk insert operations rather than one per line.
            chunks = []
            current_tag = None
            current_lines = []
            for line in lines:
                lowered = line.lower()
                tag = (
                    "error" if "traceback" in lowered or "error" in lowered or "failed" in lowered
                    else "success" if "started pid" in lowered or "success" in lowered
                    else "dim" if line.startswith("[") else ""
                )
                if current_lines and tag != current_tag:
                    chunks.append((current_tag or "", "\n".join(current_lines) + "\n"))
                    current_lines = []
                current_tag = tag
                current_lines.append(line)
            if current_lines:
                chunks.append((current_tag or "", "\n".join(current_lines) + "\n"))
            for tag, value in chunks:
                widget.insert("end", value, tag)
            widget.see("end")
            widget.configure(state="disabled")
        except tk.TclError:
            pass

    def _copy_text(self, value: str, button: Optional[tk.Button] = None) -> None:
        try:
            self.root.clipboard_clear()
            self.root.clipboard_append(value)
            self.root.update_idletasks()
            if button:
                original = button.cget("text")
                button.configure(text="Copied")
                self.root.after(1600, lambda b=button, t=original: b.configure(text=t) if b.winfo_exists() else None)
        except tk.TclError:
            if button:
                button.configure(text="Copy failed")

    def copy_console_result(self, key: str) -> None:
        lines = self.raw_logs.get(key, [])
        if not lines:
            lines = [f"No console output has been recorded for {key} yet."]
        self._copy_text("\n".join(lines), self.console_copy_buttons.get(key))

    def copy_setup_report(self) -> None:
        lines = ["HarvHub AI — Setup Report", "=" * 52]
        lines.extend(self.setup_raw_lines or ["No setup output recorded yet."])
        self._copy_text("\n".join(lines), getattr(self, "setup_copy_button", None))

    # ----------------------------- Process control ---------------------------
    def update_application(self) -> None:
        """Close this instance, then run the installed latest-source updater."""
        active = self._running_program_keys()
        if active:
            messagebox.showwarning(
                "Stop programs before updating",
                "Stop all running HarvHub AI programs before updating the application and its Python files.\n\n"
                "Currently running: " + ", ".join((self._program(key) or {}).get("label", key) for key in active),
                parent=self.root,
            )
            return
        if not is_windows():
            messagebox.showinfo(
                "Update available on Windows",
                "The automatic updater runs on the installed Windows HarvHub AI computer.",
                parent=self.root,
            )
            return

        launcher_candidates = (
            MANAGER_DIR / "source" / "HarvHubAI.vbs",
            Path(__file__).resolve().parent / "HarvHubAI.vbs",
            HARVHUBAI_ROOT / "HarvHubAI.vbs",
        )
        launcher = next((path for path in launcher_candidates if path.is_file()), None)
        if launcher is None:
            messagebox.showerror(
                "Updater not found",
                "The installed HarvHub AI updater could not be found.\n\n"
                "Run the HarvHub AI launcher from Downloads to restore the updater files.",
                parent=self.root,
            )
            return

        if not messagebox.askyesno(
            "Update HarvHub AI?",
            "HarvHub AI will close and the updater will:\n\n"
            "• Find the newest complete HarvHub project in Downloads\n"
            "• Rebuild the application from that project\n"
            "• Replace the installed application and Python source files\n"
            "• Keep backups of replaced files\n\n"
            "Continue with the update?",
            parent=self.root,
        ):
            return

        try:
            MANAGER_DIR.mkdir(parents=True, exist_ok=True)
            waiter = MANAGER_DIR / "launch_update_after_exit.vbs"
            waiter_source = "\n".join((
                "Option Explicit",
                "Dim shell, service, processId, updaterPath, matches, command",
                'Set shell = CreateObject("WScript.Shell")',
                "processId = CLng(WScript.Arguments(0))",
                "updaterPath = WScript.Arguments(1)",
                r'Set service = GetObject("winmgmts:\\.\root\cimv2")',
                "Do",
                '    Set matches = service.ExecQuery("SELECT ProcessId FROM Win32_Process WHERE ProcessId=" & processId)',
                "    If matches.Count = 0 Then Exit Do",
                "    WScript.Sleep 500",
                "Loop",
                r'command = Chr(34) & shell.ExpandEnvironmentStrings("%WINDIR%") & "\System32\wscript.exe" & Chr(34) & " " & Chr(34) & updaterPath & Chr(34)',
                "shell.Run command, 0, False",
            )) + "\n"
            waiter.write_text(waiter_source, encoding="utf-8-sig")
            windir = Path(os.environ.get("WINDIR", r"C:\Windows"))
            wscript = windir / "System32" / "wscript.exe"
            if not wscript.is_file():
                raise FileNotFoundError(f"Windows Script Host was not found: {wscript}")
            subprocess.Popen(
                [str(wscript), str(waiter), str(os.getpid()), str(launcher)],
                cwd=str(HARVHUBAI_ROOT),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                close_fds=True,
            )
        except Exception as exc:
            messagebox.showerror(
                "Could not start update",
                f"The updater could not be scheduled. HarvHub AI will remain open.\n\n{exc}",
                parent=self.root,
            )
            return

        self.closing = True
        self.root.after(250, self.root.destroy)

    def run_all(self) -> None:
        execution_type = self.global_execution_type
        warning = (
            "Programme Signals Trading can manage live MT5 positions, and Signals Provision Challenge can update programme publication and approval statuses.\n\n"
            f"Execution type: {execution_type.upper()}. MySQL Remote starts first as the shared database dependency; each other program is then started as a separate process with its own tab console.\n\n"
            "Continue?"
        )
        if self.stop_all_in_progress or self.reset_lock.locked():
            return
        if not messagebox.askyesno("Run all programs?", warning, parent=self.root):
            return
        if not self.run_all_lock.acquire(blocking=False):
            return
        self.run_all_cancel.clear()
        self._set_global_run_all_buttons("disabled", "STARTING...")
        if self.global_reset_all_button is not None:
            self.global_reset_all_button.configure(state="disabled")
        threading.Thread(
            target=self._run_all_worker, args=(execution_type,),
            name="harvcore-run-all", daemon=True,
        ).start()

    def _run_all_worker(self, execution_type: str = "once") -> None:
        bridge_started_here = False
        newly_started_workers: list[str] = []
        attempted_workers: set[str] = set()
        reported_skips: set[str] = set()
        try:
            if self.run_all_cancel.is_set() or self.closing:
                return

            bridge_was_available = self._port_open()
            managed_bridge_running = self._is_process_running("mysqlremote")
            self._ui_log("mysqlremote", "[MANAGER] Run All: checking the MySQL Remote dependency.\n", "dim")

            if not bridge_was_available and not managed_bridge_running:
                self._ui_log("mysqlremote", "[MANAGER] Starting MySQL Remote bridge first.\n", "dim")
                bridge_started_here = self._launch_process("mysqlremote", execution_type="loop")
                if not bridge_started_here and not self._is_process_running("mysqlremote"):
                    raise RuntimeError("MySQL Remote could not be started; dependent programs were not launched.")
            elif bridge_was_available and not managed_bridge_running:
                self._ui_log(
                    "mysqlremote",
                    "[MANAGER] Port 5599 is already occupied. Checking whether it is an older HarvHub MySQL Remote process so this window can capture the real output.\n",
                    "dim",
                )
                stopped_count, cleanup_error = self._stop_unmanaged_harvhub_service("mysqlremote")
                if cleanup_error:
                    raise RuntimeError(f"Could not inspect the existing MySQL Remote process: {cleanup_error}")
                if stopped_count:
                    self._ui_log("mysqlremote", f"[MANAGER] Stopped {stopped_count} detached HarvHub MySQL Remote process(es); starting a console-managed instance.\n", "dim")
                    release_deadline = time.monotonic() + 8
                    while self._port_open() and time.monotonic() < release_deadline and not self.run_all_cancel.is_set():
                        time.sleep(0.2)
                    if self._port_open():
                        raise RuntimeError("Port 5599 remained occupied after stopping the detached HarvHub listener. Close the process using port 5599 and retry Run All.")
                    bridge_started_here = self._launch_process("mysqlremote", execution_type="loop")
                    if not bridge_started_here:
                        raise RuntimeError("Could not start a new MySQL Remote listener after cleaning the detached instance.")
                else:
                    raise RuntimeError("Port 5599 is being used by a process that is not the installed HarvHub mysqlremote.py. For safety it was not terminated; close that process and retry so MySQL Remote can stream its own console output.")
            else:
                self._ui_log("mysqlremote", "[MANAGER] MySQL Remote is already running under this manager.\n", "success")

            if not self._wait_for_bridge(timeout=150, cancel_event=self.run_all_cancel):
                if self.run_all_cancel.is_set() or self.closing:
                    return
                reason = "MySQL Remote did not open port 5599; this program needs the database bridge."
                for key in RUN_ALL_ORDER[1:]:
                    self._ui_log(key, f"[MANAGER] Not started by Run All: {reason}\n", "error")
                    self.events.put(("program_status", (key, "ERROR")))
                    reported_skips.add(key)
                raise RuntimeError("MySQL Remote did not become ready on port 5599.")

            self._ui_log("mysqlremote", "[MANAGER] Bridge port 5599 is accepting connections.\n", "success")
            for key in RUN_ALL_ORDER[1:]:
                if self.closing or self.run_all_cancel.is_set():
                    break
                was_running = self._is_process_running(key)
                attempted_workers.add(key)
                self._ui_log(
                    key,
                    "[MANAGER] Run All requested this program. Starting it in its own process; its output will appear only in this program's Show Console view.\n",
                    "dim",
                )
                started = self._launch_process(key, execution_type=execution_type)
                if started and not was_running and self._is_process_running(key):
                    newly_started_workers.append(key)
                elif not started:
                    self._ui_log(key, "[MANAGER] Run All could not start this program. See the error above in this program's console.\n", "error")
                    self.events.put(("program_status", (key, "ERROR")))
                time.sleep(0.25)

            if execution_type == "once" and not self.run_all_cancel.is_set():
                # Wait only for one-shot workers created by this Run All request.
                # Existing workers are left untouched.
                while not self.closing and not self.run_all_cancel.is_set():
                    active_workers = [
                        key for key in newly_started_workers
                        if self._is_process_running(key)
                    ]
                    if not active_workers:
                        break
                    time.sleep(0.35)
                if bridge_started_here and not self.run_all_cancel.is_set():
                    self._ui_log(
                        "mysqlremote",
                        "[MANAGER] One-shot workers completed; stopping the temporary database listener.\n",
                        "dim",
                    )
                    self._stop_process_worker("mysqlremote")
        except Exception as exc:
            self._ui_log("mysqlremote", f"[MANAGER] Run All failed: {exc}\n", "error")
            # Make the reason visible on every program page that was skipped,
            # not only in the coordinator's MySQL Remote console.
            for key in RUN_ALL_ORDER[1:]:
                if key not in attempted_workers and key not in reported_skips:
                    self._ui_log(key, f"[MANAGER] Run All did not start this program: {exc}\n", "error")
                    self.events.put(("program_status", (key, "ERROR")))
        finally:
            self.events.put(("run_all_finished", ()))
            try:
                self.run_all_lock.release()
            except RuntimeError:
                pass

    def start_program(self, key: str) -> None:
        item = self._program(key)
        if not item or not item["runnable"]:
            return
        if item.get("warning") and not messagebox.askyesno("Confirm program start", item["warning"] + "\n\nStart this program?", parent=self.root):
            return
        execution_type = self.program_execution_types.get(key, self.global_execution_type)
        if key == "mysqlremote":
            self._ui_log(
                key,
                "[MANAGER] MySQL Remote is a persistent database listener; starting in Loop mode so other programs can send it requests.\n",
                "dim",
            )
            threading.Thread(target=self._start_mysql_remote_with_capture, daemon=True, name="start-mysqlremote").start()
            return
        threading.Thread(
            target=self._start_with_bridge_worker, args=(key, execution_type), daemon=True
        ).start()

    def _start_mysql_remote_with_capture(self) -> None:
        if self._is_process_running("mysqlremote"):
            self._ui_log("mysqlremote", "[MANAGER] MySQL Remote is already running under this window.\n", "dim")
            return
        if self._port_open():
            count, error = self._stop_unmanaged_harvhub_service("mysqlremote")
            if error:
                self._ui_log("mysqlremote", f"[MANAGER][ERROR] Could not inspect the existing listener: {error}\n", "error")
                return
            if count:
                deadline = time.monotonic() + 8
                while self._port_open() and time.monotonic() < deadline:
                    time.sleep(0.2)
            if self._port_open():
                self._ui_log("mysqlremote", "[MANAGER][ERROR] Port 5599 belongs to another process. It was not terminated. Close it and press PLAY again.\n", "error")
                return
        self._launch_process("mysqlremote", execution_type="loop")

    def _start_with_bridge_worker(self, key: str, execution_type: str = "once") -> None:
        if not self._port_open():
            self._ui_log(key, "[MANAGER] MySQL Remote is not running. Starting it automatically...\n", "dim")
            self._launch_process("mysqlremote", execution_type="loop")
            if not self._wait_for_bridge(timeout=150):
                self._ui_log(key, "[MANAGER] Cannot start this program because the MySQL Remote bridge did not become ready.\n", "error")
                return
        self._launch_process(key, execution_type=execution_type)

    def _launch_process(self, key: str, execution_type: Optional[str] = None) -> bool:
        item = self._program(key)
        if not item or not item["runnable"]:
            return False
        with self.process_lock:
            existing = self.processes.get(key)
            if existing and existing.process.poll() is None:
                self.events.put(("program_status", (key, "RUNNING")))
                self._ui_log(key, "[MANAGER] Already running; no duplicate process started.\n", "dim")
                return True

            script = PYTHON_DIR / item["filename"]
            if not script.is_file():
                self._ui_log(key, f"[MANAGER] Program file not found: {script}\n", "error")
                self.events.put(("program_status", (key, "ERROR")))
                return False
            if not self.python_exe:
                self.python_exe = locate_python()
            if not self.python_exe:
                self._ui_log(key, "[MANAGER] Python 3.13 x64 is not available. Run Setup / Repair first.\n", "error")
                self.events.put(("program_status", (key, "ERROR")))
                return False

            selected_execution = execution_type or self.program_execution_types.get(
                key, self.global_execution_type
            )
            selected_execution = "loop" if str(selected_execution).lower() == "loop" else "once"
            interval = max(1, int(item.get("interval", DEFAULT_LOOP_INTERVAL)))
            command = build_service_command(
                python_exe=self.python_exe,
                script=script,
                key=key,
                execution_type=selected_execution,
                interval=interval,
                extra_args=item.get("args", ()),
            )
            env = os.environ.copy()
            env["PYTHONUNBUFFERED"] = "1"
            env["PYTHONUTF8"] = "1"
            env["HARVHUB_EXECUTION_TYPE"] = selected_execution
            env["HARVHUB_LOOP_INTERVAL"] = str(DEFAULT_LOOP_INTERVAL)
            env["HARVHUB_CONTROL_HOST"] = CONTROL_HOST
            env["HARVHUB_CONTROL_PORT"] = str(CONTROL_PORT)
            env["HARVHUB_MANAGER_DIR"] = str(MANAGER_DIR)
            env["PATH"] = str(self.python_exe.parent) + os.pathsep + str(self.python_exe.parent / "Scripts") + os.pathsep + env.get("PATH", "")
            flags = getattr(subprocess, "CREATE_NO_WINDOW", 0) | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
            try:
                process = subprocess.Popen(
                    command,
                    cwd=str(PYTHON_DIR),
                    env=env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    stdin=subprocess.DEVNULL,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    bufsize=1,
                    creationflags=flags,
                )
            except Exception as exc:
                self._ui_log(key, f"[MANAGER] Could not start process: {exc}\n", "error")
                self.events.put(("program_status", (key, "ERROR")))
                return False
            started_at = time.monotonic()
            self.processes[key] = ProgramProcess(process=process, started_at=started_at)
            watchdog_stop = threading.Event()
            self.output_watchdog_stops[key] = watchdog_stop
            self.service_output_at[key] = started_at

        self.events.put(("program_status", (key, "RUNNING")))
        self._ui_log(key, f"[MANAGER] STARTED: {item['label']} | PID={process.pid}\n", "success")
        self._ui_log(key, f"[MANAGER] Execution mode: {selected_execution.upper()} | interval={interval}s\n", "dim")
        self._ui_log(key, f"[MANAGER] Script: {script}\n", "dim")
        self._ui_log(key, "[MANAGER] Live stdout/stderr reader attached to this program only.\n", "dim")
        reader = threading.Thread(
            target=self._read_process_output, args=(key, process),
            daemon=True, name=f"{key}-output",
        )
        self.output_threads[key] = reader
        reader.start()
        watchdog = threading.Thread(
            target=self._watch_process_output,
            args=(key, process, started_at, watchdog_stop),
            daemon=True,
            name=f"{key}-output-watchdog",
        )
        watchdog.start()
        return True

    def _read_process_output(self, key: str, process: subprocess.Popen) -> None:
        """Forward the child process' combined stdout/stderr to its own tab."""
        received_output = False
        try:
            if process.stdout is None:
                self.events.put(("console_line", (key, "[MANAGER] No stdout pipe was created for this process.", "error")))
            else:
                self.events.put(("console_line", (key, "[MANAGER] Live output reader attached; waiting for service output.", "dim")))
                for line in iter(process.stdout.readline, ""):
                    received_output = True
                    self.service_output_at[key] = time.monotonic()
                    self.events.put(("console_line", (key, line.rstrip("\r\n"))))
        except Exception as exc:
            self.events.put(("console_line", (key, f"[MANAGER] Console reader error: {type(exc).__name__}: {exc}", "error")))
        finally:
            try:
                if process.stdout:
                    process.stdout.close()
            except Exception:
                pass
        try:
            return_code = process.wait(timeout=3)
        except Exception:
            return_code = process.poll()
        if not received_output and return_code is not None:
            self.events.put((
                "console_line",
                (key, f"[MANAGER] The service exited without writing any stdout/stderr. Exit code: {return_code}. Check that the deployed script is the updated source and that it does not redirect output.", "error" if return_code else "dim"),
            ))
        watchdog_stop = self.output_watchdog_stops.get(key)
        if watchdog_stop:
            watchdog_stop.set()
        self.events.put(("process_exit", (key, return_code)))

    def _watch_process_output(
        self,
        key: str,
        process: subprocess.Popen,
        started_at: float,
        stop_event: threading.Event,
    ) -> None:
        """Report process health when a service has not emitted a real log line."""
        next_report = 15.0
        while process.poll() is None and not stop_event.wait(1.0):
            now = time.monotonic()
            last_output = self.service_output_at.get(key, started_at)
            idle_seconds = now - last_output
            if idle_seconds >= next_report:
                uptime = int(now - started_at)
                self.events.put((
                    "console_line",
                    (key, f"[MANAGER][HEALTH] Process PID {process.pid} is still running; no service output for {int(idle_seconds)}s (uptime {uptime}s). This is a process-health notice, not a work result.", "dim"),
                ))
                next_report = idle_seconds + 30.0
        stop_event.set()

    def stop_program(self, key: str) -> None:
        threading.Thread(target=self._stop_process_worker, args=(key,), daemon=True).start()

    def stop_all(self) -> None:
        if self.stop_all_in_progress or self.reset_lock.locked():
            return
        if not messagebox.askyesno("Stop all programs?", "Stop all running programs and their child worker processes?", parent=self.root):
            return
        self.stop_all_in_progress = True
        self.run_all_cancel.set()
        self._set_global_run_all_buttons("disabled", "STOPPING...")
        if self.global_reset_all_button is not None:
            self.global_reset_all_button.configure(state="disabled")
        threading.Thread(target=self._stop_all_worker, daemon=True).start()

    def _stop_all_worker(self) -> None:
        self.run_all_cancel.set()
        # Let Run All observe cancellation before taking the final process snapshot.
        deadline = time.monotonic() + 12
        while self.run_all_lock.locked() and time.monotonic() < deadline:
            time.sleep(0.1)
        with self.process_lock:
            keys = list(self.processes.keys())
        for key in keys:
            self._stop_process_worker(key)
        orphan_count, orphan_error = self._stop_orphaned_harvhub_services()
        if orphan_count:
            self._ui_log(
                "mysqlremote",
                f"[MANAGER] Stop All also terminated {orphan_count} orphaned HarvHub service process(es).\n",
                "dim",
            )
        if orphan_error:
            self._ui_log("mysqlremote", f"[MANAGER] Orphan service cleanup warning: {orphan_error}\n", "error")
        self.events.put(("all_stopped", ()))

    def reset_all(self) -> None:
        if self.reset_lock.locked() or self.stop_all_in_progress:
            return
        if not messagebox.askyesno(
            "Reset HarvHub AI?",
            "This will stop all HarvHub services (including orphaned instances), terminate HarvHub MT5 terminals and child processes, clear all program consoles, and return the window to its initial Home view.\n\nPending manual-login requests will be kept so they are not lost.\n\nContinue?",
            parent=self.root,
        ):
            return
        if not self.reset_lock.acquire(blocking=False):
            return
        self.stop_all_in_progress = True
        self.run_all_cancel.set()
        if self.global_reset_all_button is not None:
            self.global_reset_all_button.configure(state="disabled", text="RESETTING...")
        self._set_global_run_all_buttons("disabled", "STOPPING...")
        if self.global_stop_all_button is not None:
            self.global_stop_all_button.configure(state="disabled")
        threading.Thread(
            target=self._reset_all_worker,
            name="harvhub-reset-all",
            daemon=True,
        ).start()

    def _reset_all_worker(self) -> None:
        """Bounded, staged reset; always returns a completion/failure event to Tk."""
        try:
            self.events.put(("reset_progress", ("CANCELLING RUN ALL...",)))
            # Run All must finish its cancellation path before Reset snapshots
            # the process table. Otherwise a late worker can launch a process
            # after Reset has already reported completion.
            deadline = time.monotonic() + 25
            while self.run_all_lock.locked() and time.monotonic() < deadline:
                time.sleep(0.1)
            if self.run_all_lock.locked():
                raise TimeoutError(
                    "Run All did not release its launch lock within 25 seconds. "
                    "Reset has not been reported as complete; retry after the active launch settles."
                )

            with self.process_lock:
                keys = list(self.processes.keys())
            total = max(1, len(keys))
            for index, key in enumerate(keys, start=1):
                self.events.put(("reset_progress", (f"STOPPING PROGRAMS {index}/{total}...",)))
                self._stop_process_worker(key)

            # Catch a process that was created as cancellation was requested.
            with self.process_lock:
                late_keys = list(self.processes.keys())
            for key in late_keys:
                if self._is_process_running(key):
                    self.events.put(("reset_progress", (f"STOPPING {key.upper()}...",)))
                    self._stop_process_worker(key)

            # Do not report a successful reset if Windows refused to stop a service.
            still_running = [key for key in RUN_ALL_ORDER if self._is_process_running(key)]
            if still_running:
                raise RuntimeError(
                    "Could not stop these managed services: " + ", ".join(still_running)
                )

            self.events.put(("reset_progress", ("FLUSHING CONSOLES...",)))
            for reader in list(self.output_threads.values()):
                if reader and reader.is_alive():
                    reader.join(timeout=0.8)

            self.events.put(("reset_progress", ("CLEANING DETACHED HARVHUB SERVICES...",)))
            orphan_count, orphan_error = self._stop_orphaned_harvhub_services()

            self.events.put(("reset_progress", ("CLOSING HARVHUB MT5 TERMINALS...",)))
            terminal_count, terminal_error = self._stop_harvhub_mt5_terminals()

            cleanup_errors = [value for value in (orphan_error, terminal_error) if value]
            self.events.put(("reset_progress", ("FINALISING RESET...",)))
            self.events.put(("reset_complete", (orphan_count, terminal_count, "\n\n".join(cleanup_errors))))
        except Exception as exc:
            self.events.put(("reset_failed", (f"{type(exc).__name__}: {exc}",)))
        finally:
            self.stop_all_in_progress = False
            try:
                self.reset_lock.release()
            except RuntimeError:
                pass

    def _stop_unmanaged_harvhub_service(self, key: str) -> tuple[int, str]:
        """Stop only an untracked Python process whose command line runs this exact service file."""
        if not is_windows():
            return 0, ""
        item = self._program(key)
        if not item:
            return 0, f"Unknown HarvHub program key: {key}"
        script_path = str((PYTHON_DIR / item["filename"]).resolve()).replace("'", "''")
        with self.process_lock:
            managed_pids = [entry.process.pid for entry in self.processes.values() if entry.process.poll() is None]
        excluded = ",".join(str(pid) for pid in managed_pids) or "-1"
        ps = f"""
$target = '{script_path}'
$excluded = @({excluded})
$items = @(Get-CimInstance Win32_Process -Filter "Name='python.exe' OR Name='pythonw.exe'" -ErrorAction SilentlyContinue | Where-Object {{
    $cmd = [string]$_.CommandLine
    ($cmd.IndexOf($target, [System.StringComparison]::OrdinalIgnoreCase) -ge 0) -and ($excluded -notcontains [int]$_.ProcessId)
}})
$count = 0
foreach ($item in $items) {{
    & (Join-Path (Join-Path $env:WINDIR 'System32') 'taskkill.exe') /PID $item.ProcessId /T /F | Out-Null
    $count++
}}
Write-Output $count
"""
        try:
            result = subprocess.run(
                ["powershell.exe", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", ps],
                capture_output=True, text=True, encoding="utf-8", errors="replace",
                timeout=8, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            if result.returncode != 0:
                return 0, (result.stderr or result.stdout or "PowerShell returned an error").strip()[-800:]
            lines = [line.strip() for line in (result.stdout or "").splitlines() if line.strip()]
            return (int(lines[-1]) if lines else 0), ""
        except Exception as exc:
            return 0, str(exc)

    def _stop_orphaned_harvhub_services(self) -> tuple[int, str]:
        """Stop stale service interpreters running HarvHub's exact script files."""
        if not is_windows():
            return 0, ""
        script_paths = []
        for key in RUN_ALL_ORDER:
            item = self._program(key)
            if item:
                script_paths.append(str((PYTHON_DIR / item["filename"]).resolve()).replace("'", "''"))
        if not script_paths:
            return 0, ""
        powershell_paths = ",\n    ".join(f"'{path}'" for path in script_paths)
        script = f"""
$scriptPaths = @(
    {powershell_paths}
)
$items = @(Get-CimInstance Win32_Process -Filter "Name='python.exe' OR Name='pythonw.exe'" -ErrorAction SilentlyContinue | Where-Object {{
    $commandLine = [string]$_.CommandLine
    $matched = $false
    foreach ($scriptPath in $scriptPaths) {{
        if ($commandLine.IndexOf($scriptPath, [System.StringComparison]::OrdinalIgnoreCase) -ge 0) {{
            $matched = $true
            break
        }}
    }}
    $matched
}})
$count = 0
foreach ($item in $items) {{
    & (Join-Path (Join-Path $env:WINDIR 'System32') 'taskkill.exe') /PID $item.ProcessId /T /F | Out-Null
    $count++
}}
Write-Output $count
"""
        try:
            result = subprocess.run(
                ["powershell.exe", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", script],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=10,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            if result.returncode != 0:
                detail = (result.stderr or result.stdout or "PowerShell returned an error").strip()
                return 0, detail[-1000:]
            lines = [line.strip() for line in (result.stdout or "").splitlines() if line.strip()]
            return (int(lines[-1]) if lines else 0), ""
        except Exception as exc:
            return 0, str(exc)

    def _stop_harvhub_mt5_terminals(self) -> tuple[int, str]:
        """Close only HarvHub-owned terminal64.exe processes under MT5_ROOT."""
        if not is_windows():
            return 0, ""
        root = str(MT5_ROOT.resolve()).replace("'", "''")
        script = f"""
$root = [System.IO.Path]::GetFullPath('{root}').TrimEnd([char]92) + [char]92
$items = @(Get-CimInstance Win32_Process -Filter "Name='terminal64.exe'" -ErrorAction SilentlyContinue | Where-Object {{
    $_.ExecutablePath -and $_.ExecutablePath.StartsWith($root, [System.StringComparison]::OrdinalIgnoreCase)
}})
$count = 0
foreach ($item in $items) {{
    & (Join-Path (Join-Path $env:WINDIR 'System32') 'taskkill.exe') /PID $item.ProcessId /T /F | Out-Null
    $count++
}}
Write-Output $count
"""
        try:
            result = subprocess.run(
                ["powershell.exe", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", script],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=10,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            if result.returncode != 0:
                detail = (result.stderr or result.stdout or "PowerShell returned an error").strip()
                return 0, detail[-1000:]
            lines = [line.strip() for line in (result.stdout or "").splitlines() if line.strip()]
            return (int(lines[-1]) if lines else 0), ""
        except Exception as exc:
            return 0, str(exc)

    def _stop_process_worker(self, key: str) -> None:
        with self.process_lock:
            entry = self.processes.get(key)
        if not entry:
            self.events.put(("program_status", (key, "STOPPED")))
            return
        process = entry.process
        if process.poll() is None:
            self._ui_log(key, "[MANAGER] Sending stop request...\n", "dim")
            if is_windows():
                try:
                    subprocess.run(
                        ["taskkill.exe", "/PID", str(process.pid), "/T", "/F"],
                        capture_output=True,
                        text=True,
                        timeout=6,
                        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                    )
                except Exception:
                    try:
                        process.kill()
                    except Exception:
                        pass
            else:
                try:
                    process.terminate()
                except Exception:
                    pass
            try:
                process.wait(timeout=2.5)
            except Exception:
                try:
                    process.kill()
                except Exception:
                    pass
                try:
                    process.wait(timeout=1.0)
                except Exception:
                    pass
            if process.poll() is None:
                self._ui_log(key, "[MANAGER][ERROR] Process is still alive after termination attempts.\n", "error")
                return
        watchdog_stop = self.output_watchdog_stops.get(key)
        if watchdog_stop:
            watchdog_stop.set()
        self.events.put(("program_status", (key, "STOPPED")))
        self._ui_log(key, "[MANAGER] Process stopped.\n", "dim")

    def _wait_for_bridge(self, timeout: int, cancel_event: Optional[threading.Event] = None) -> bool:
        started = time.monotonic()
        while time.monotonic() - started < timeout and not self.closing:
            if cancel_event is not None and cancel_event.is_set():
                return False
            if self._port_open():
                return True
            with self.process_lock:
                bridge = self.processes.get("mysqlremote")
            if bridge and bridge.process.poll() is not None:
                return False
            time.sleep(0.35)
        return self._port_open() and not (cancel_event and cancel_event.is_set())

    @staticmethod
    def _port_open() -> bool:
        try:
            with socket.create_connection((BRIDGE_HOST, BRIDGE_PORT), timeout=1.2):
                return True
        except OSError:
            return False

    def _program(self, key: str) -> Optional[dict]:
        for item in PROGRAMS:
            if item["key"] == key:
                return item
        return None

    def clear_console(self, key: str) -> None:
        self.raw_logs[key] = []
        widget = self.console_widgets.get(key)
        if widget:
            widget.configure(state="normal")
            widget.delete("1.0", "end")
            widget.configure(state="disabled")
        try:
            (LOG_DIR / f"{key}.hclog").unlink(missing_ok=True)
        except OSError:
            pass

    def _append_console(self, key: str, line: str, tag: Optional[str] = None) -> None:
        text = str(line)
        # Strip the legacy fake Ready line if an older protected log contains it.
        item = self._program(key)
        ready_placeholder = f"[{item['label']}] Ready." if item else None
        current_lines = self.raw_logs.setdefault(key, [])
        if ready_placeholder and ready_placeholder in current_lines:
            self.raw_logs[key] = [entry for entry in current_lines if entry != ready_placeholder]
        self.raw_logs.setdefault(key, []).append(text)
        if len(self.raw_logs[key]) > 30000:
            self.raw_logs[key] = self.raw_logs[key][-30000:]
        try:
            with self.log_lock:
                append_protected_log(LOG_DIR / f"{key}.hclog", f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {text}")
        except Exception:
            # Never fall back to a plaintext diagnostic file.
            pass
        widget = self.console_widgets.get(key)
        if not widget:
            return
        try:
            widget.configure(state="normal")
            widget.insert("end", text + ("" if text.endswith("\n") else "\n"), tag or "")
            line_count = int(widget.index("end-1c").split(".")[0])
            if line_count > 4000:
                widget.delete("1.0", f"{line_count - 4000}.0")
            widget.see("end")
            widget.configure(state="disabled")
        except tk.TclError:
            pass

    def _ui_log(self, key: str, line: str, tag: Optional[str] = None) -> None:
        self.events.put(("console_line", (key, line.rstrip("\n"), tag)))
        self._write_manager_log(f"{key}: {line.rstrip()}")

    # ------------------------------ UI events --------------------------------
    def _poll_events(self) -> None:
        # Bound work per Tk tick. During busy service cycles, an unbounded event
        # drain can starve button clicks and make the window appear frozen.
        processed_events = 0
        try:
            while processed_events < 250:
                event, data = self.events.get_nowait()
                processed_events += 1
                try:
                    if event == "setup_log":
                        self._append_setup_log(str(data[0]))
                    elif event == "setup_step":
                        self._set_setup_step(data[0], data[1], data[2])
                    elif event == "setup_download_progress":
                        label, percent, downloaded, total = data
                        if hasattr(self, "setup_subtitle"):
                            self.setup_subtitle.configure(text=f"Preparing components... {percent}%")
                        if hasattr(self, "setup_progress"):
                            completed = sum(1 for value in self.setup_step_states.values() if value == "done")
                            total_stages = max(1, len(self.setup_rows))
                            self.setup_progress["value"] = min(99, (completed + (max(0, min(100, percent)) / 100.0)) * 100 / total_stages)
                    elif event == "setup_done":
                        self._append_setup_log("SETUP COMPLETE — HarvHub AI environment verified.")
                        if hasattr(self, "setup_progress"):
                            self.setup_progress["value"] = 100
                        if hasattr(self, "setup_title"):
                            self.setup_title.configure(text="Setup complete")
                        if hasattr(self, "setup_subtitle"):
                            self.setup_subtitle.configure(text="Setup complete. Your programs are ready.")
                        self.root.after(650, self._show_workspace)
                    elif event == "setup_error":
                        self._append_setup_log(f"SETUP FAILED: {data[0]}")
                        if hasattr(self, "setup_title"):
                            self.setup_title.configure(text="Setup needs attention")
                        if hasattr(self, "setup_subtitle"):
                            self.setup_subtitle.configure(text="A component could not be prepared. Copy the setup report for the full diagnostic.")
                        if hasattr(self, "setup_status_label"):
                            self.setup_status_label.configure(text="Setup stopped before completion.\nDetailed error information is hidden. Click Copy Setup Report to copy the original result.")
                        if hasattr(self, "setup_action"):
                            self.setup_action.configure(state="normal", text="Retry setup")
                        self.setup_started = False
                    elif event == "console_line":
                        key, line, *rest = data
                        self._append_console(key, str(line), rest[0] if rest else None)
                    elif event == "mysql_metrics":
                        # _mysql_status_poll_loop queues the dict directly. Older
                        # manager builds (and any compatibility producer) may
                        # still send a one-item tuple/list.
                        if isinstance(data, dict):
                            metric_status = data
                        elif isinstance(data, (tuple, list)):
                            metric_status = data[0] if data else None
                        else:
                            metric_status = None
                        self._apply_mysql_metrics(metric_status)
                    elif event == "manual_login_refresh":
                        self._refresh_manual_login_dashboard()
                    elif event == "program_status":
                        self._set_program_status(data[0], data[1])
                    elif event == "process_exit":
                        key, code = data
                        self._set_program_status(key, "STOPPED")
                        self._append_console(key, f"[MANAGER] Process exited with code {code}.", "dim" if code in (0, None) else "error")
                    elif event == "run_all_finished":
                        if not self.stop_all_in_progress and not self.reset_lock.locked():
                            self._set_global_run_all_buttons("normal", "▶  RUN ALL")
                            if self.global_reset_all_button is not None:
                                self.global_reset_all_button.configure(state="normal", text="↺  RESET ALL")
                    elif event == "setup_cancelled":
                        self.setup_started = False
                        self.setup_view_active = False
                        self._append_setup_log("Setup stopped. Completed components were left in place; Setup / Repair can resume later.")
                        self._show_workspace()
                    elif event == "all_stopped":
                        self.stop_all_in_progress = False
                        for item in PROGRAMS:
                            key = item["key"]
                            self._set_program_status(key, "RUNNING" if self._is_process_running(key) else "STOPPED")
                        self._set_global_run_all_buttons("normal", "▶  RUN ALL")
                        if self.global_reset_all_button is not None and not self.reset_lock.locked():
                            self.global_reset_all_button.configure(state="normal", text="↺  RESET ALL")
                        self._update_global_actions()
                        self._update_summary()
                    elif event == "reset_progress":
                        if self.global_reset_all_button is not None:
                            self.global_reset_all_button.configure(state="disabled", text=str(data[0]))
                    elif event == "reset_complete":
                        orphan_count = int(data[0]) if data else 0
                        terminal_count = int(data[1]) if len(data) > 1 else 0
                        terminal_error = str(data[2]) if len(data) > 2 and data[2] else ""
                        self.stop_all_in_progress = False
                        for item in PROGRAMS:
                            key = item["key"]
                            self.clear_console(key)
                            self.console_visible[key] = False
                            card = self.console_cards.get(key)
                            if card:
                                card.pack_forget()
                            placeholder = getattr(self, "console_placeholders", {}).get(key)
                            if placeholder and placeholder.winfo_exists() and placeholder.winfo_manager() != "pack":
                                placeholder.pack(fill="both", expand=True, pady=(0, 10))
                            toggle = self.console_toggle_buttons.get(key)
                            if toggle:
                                toggle.configure(text="SHOW CONSOLE")
                            self._set_program_status(key, "STOPPED")
                        self._show_page("home")
                        self.manual_login_tab = "SA"
                        self._refresh_manual_login_dashboard()
                        self._set_global_run_all_buttons("normal", "▶  RUN ALL")
                        if self.global_reset_all_button is not None:
                            self.global_reset_all_button.configure(state="normal", text="↺  RESET ALL")
                        self.run_all_cancel.clear()
                        self._update_global_actions()
                        self._update_summary()
                        if terminal_error:
                            messagebox.showwarning(
                                "Reset completed with a terminal warning",
                                "The manager stopped the programs it owned, but one cleanup task reported an issue:\n\n"
                                + terminal_error,
                                parent=self.root,
                            )
                        else:
                            self.root.title(f"{APP_TITLE} — Reset complete")
                            self.root.after(2200, lambda: self.root.title(APP_TITLE) if self.root.winfo_exists() else None)
                    elif event == "reset_failed":
                        self.stop_all_in_progress = False
                        self._set_global_run_all_buttons("normal", "▶  RUN ALL")
                        if self.global_reset_all_button is not None:
                            self.global_reset_all_button.configure(state="normal", text="↺  RESET ALL")
                        self._update_global_actions()
                        messagebox.showerror("Reset failed", str(data[0]), parent=self.root)
                except Exception as exc:
                    # Attribute a UI event failure to its owner only when the
                    # event explicitly names a known program. Never mislabel a
                    # manager/dashboard error as MySQL Remote output.
                    event_key = None
                    if event in ("console_line", "program_status", "process_exit") and isinstance(data, (tuple, list)) and data:
                        candidate = str(data[0])
                        if candidate in self.raw_logs:
                            event_key = candidate
                    message = f"[MANAGER][EVENT ERROR] Could not process {event!r}: {type(exc).__name__}: {exc}"
                    if event_key:
                        self._append_console(event_key, message, "error")
                    else:
                        self._write_manager_log(message)
        except queue.Empty:
            pass
        if not self.closing:
            # If backlog remains, return control to Tk frequently so Show
            # Console, Stop, and Reset controls remain responsive.
            delay = 15 if not self.events.empty() else 120
            self.root.after(delay, self._poll_events)

    def _append_setup_log(self, line: str) -> None:
        text = str(line)
        self.setup_raw_lines.append(text)
        if len(self.setup_raw_lines) > 50000:
            self.setup_raw_lines = self.setup_raw_lines[-50000:]
        try:
            with self.log_lock:
                append_protected_log(LOG_DIR / "setup.hclog", f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {text}")
        except Exception:
            pass

    def _set_setup_step(self, key: str, status: str, detail: str) -> None:
        row = self.setup_rows.get(key)
        self.setup_step_states[key] = status
        if status == "running":
            self.active_setup_stage = key
        elif self.active_setup_stage == key:
            self.active_setup_stage = None
        if row:
            icon, label = row
            states = {
                "running": ("◉", self.colors["gold"]),
                "done": ("●", self.colors["green"]),
                "failed": ("●", self.colors["red"]),
            }
            icon_text, colour = states.get(status, ("○", self.colors["muted"]))
            icon.configure(text=icon_text, fg=colour)
            label.configure(fg=self.colors["green"] if status == "done" else self.colors["text"])
        if detail:
            self._append_setup_log(detail)
        labels = {
            "system": "Preparing Windows environment...",
            "python": "Preparing Python runtime...",
            "packages": "Installing required Python modules...",
            "chrome": "Preparing browser component...",
            "mt5": "Preparing MetaTrader 5...",
            "sources": "Updating HarvHub AI program files...",
            "verify": "Verifying installed components...",
        }
        if status == "running" and hasattr(self, "setup_subtitle"):
            self.setup_subtitle.configure(text=labels.get(key, "Working...") )
        completed = sum(1 for value in self.setup_step_states.values() if value == "done")
        total = max(1, len(self.setup_rows))
        if hasattr(self, "setup_progress"):
            self.setup_progress["value"] = min(99, int(completed * 100 / total))

    def _set_program_status(self, key: str, status: str) -> None:
        widget = self.status_widgets.get(key)
        managed_running = self._is_process_running(key)
        stop_button = self.program_stop_buttons.get(key)
        if stop_button is not None:
            try:
                if managed_running and stop_button.winfo_manager() != "pack":
                    stop_button.pack(side="left", padx=(0, 7), before=self.console_toggle_buttons.get(key))
                elif not managed_running:
                    stop_button.pack_forget()
            except tk.TclError:
                pass
        if widget:
            if status == "RUNNING":
                widget.configure(text="RUNNING", bg=self.colors["green"], fg="#ffffff")
            elif status == "ERROR":
                widget.configure(text="ERROR", bg=self.colors["red"], fg="#ffffff")
            else:
                item = self._program(key) or {}
                text = status if item.get("runnable") else item.get("role", "MODULE").upper()
                widget.configure(text=text, bg=self.colors["surface2"], fg=self.colors["muted"])
        self._update_summary()
        self._update_global_actions()

    def _refresh_all_statuses(self) -> None:
        with self.process_lock:
            values = list(self.processes.items())
        for key, entry in values:
            self._set_program_status(key, "RUNNING" if entry.process.poll() is None else "STOPPED")
        self._update_summary()
        if not self.closing:
            self.root.after(2500, self._refresh_all_statuses)

    def _update_summary(self) -> None:
        if not self.workspace_built:
            return
        active = 0
        with self.process_lock:
            for key, entry in self.processes.items():
                if entry.process.poll() is None:
                    active += 1
        self._update_global_actions()
        if hasattr(self, "summary_python"):
            version = "READY" if self.python_exe else "CHECK"
            self.summary_python.value_label.configure(text=version)  # type: ignore[attr-defined]
            self.summary_mt5.value_label.configure(text="INSTALLED" if (MT5_TEMPLATE / "terminal64.exe").is_file() else "MISSING")  # type: ignore[attr-defined]
            self.summary_services.value_label.configure(text=f"{active} ACTIVE")  # type: ignore[attr-defined]

    # ------------------------------ Shutdown ---------------------------------
    def on_close(self) -> None:
        self.run_all_cancel.set()
        active = []
        with self.process_lock:
            active = [key for key, entry in self.processes.items() if entry.process.poll() is None]
        if active:
            answer = messagebox.askyesno(
                "Close HarvHub AI?",
                "These programs are still running:\n\n"
                + ", ".join((self._program(key) or {}).get("label", key) for key in active)
                + "\n\nStop them and close the manager?",
                parent=self.root,
            )
            if not answer:
                return
            self.closing = True
            for key in active:
                self._stop_process_worker(key)
        self.closing = True
        self.control_server_stop.set()
        self.mysql_poll_stop.set()
        try:
            if self.control_server_socket is not None:
                self.control_server_socket.close()
        except OSError:
            pass
        self.root.destroy()


def main() -> None:
    # When this source file is launched directly, force the GitHub-first VBS
    # bootstrap. The frozen installed EXE and builder-launched source are exempt.
    if is_windows() and not getattr(sys, "frozen", False) and os.environ.get("HARVHUBAI_BOOTSTRAPPED") != "1":
        launcher = Path(__file__).resolve().parent / "HarvHubAI.vbs"
        if launcher.is_file():
            try:
                subprocess.Popen(
                    ["wscript.exe", str(launcher)],
                    cwd=str(launcher.parent),
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                )
            except Exception as exc:
                try:
                    ctypes.windll.user32.MessageBoxW(None, f"Could not start HarvHub AI GitHub bootstrap: {exc}", APP_TITLE, 0x10)
                except Exception:
                    pass
        else:
            try:
                ctypes.windll.user32.MessageBoxW(None, "Run HarvHubAI.vbs first. It downloads and validates the latest GitHub project before setup.", APP_TITLE, 0x10)
            except Exception:
                pass
        return

    if is_windows() and not is_admin():
        if not relaunch_as_admin():
            try:
                ctypes.windll.user32.MessageBoxW(
                    None,
                    "HarvHub AI needs Administrator privileges to install Python, Chrome, MetaTrader 5 and write to C:\\xampp\\htdocs\\harvhubAI.",
                    APP_TITLE,
                    0x10,
                )
            except Exception:
                pass
            return
        return
    root = tk.Tk()
    force_setup = any(str(arg).strip().lower() in {"--repair", "/repair"} for arg in sys.argv[1:])
    HarvCoreManager(root, force_setup=force_setup)
    root.mainloop()


if __name__ == "__main__":
    main()
