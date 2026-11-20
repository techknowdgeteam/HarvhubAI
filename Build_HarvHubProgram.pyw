from __future__ import annotations

import ctypes
import hashlib
import os
import queue
import shutil
import subprocess
import sys
import threading
import time
import traceback
from pathlib import Path
import tkinter as tk
from tkinter import ttk, messagebox
from tkinter.scrolledtext import ScrolledText

# The VBS launcher selects the newest complete HarvHub project in Downloads
# and passes its exact root to the GUI builder.
APP_DIR = Path(os.environ.get("HARVHUBAI_SOURCE_ROOT", str(Path(__file__).resolve().parent))).resolve()
BUILD_ENV = APP_DIR / ".build-venv"
BUILD_PYTHON = BUILD_ENV / "Scripts" / "python.exe"
OUTPUT_EXE = APP_DIR / "release" / "HarvHubAI.exe"

# Inputs that determine the customer EXE. Build products and Python caches are
# excluded so a completed build does not invalidate itself.
BUILD_EXCLUDED_DIRS = {".build-venv", "build", "release", "dist", "__pycache__", ".git"}
BUILD_INPUT_SUFFIXES = {".py", ".pyw", ".vbs", ".bat", ".txt", ".spec", ".md"}

REQUIRED_PAYLOAD_FILES = (
    "mysqlremote.py",
    "brokerdetails.py",
    "brokercharts.py",
    "programme_training.py",
    "signals_trading.py",
    "signals_provision_challenge.py",
    "ip.py",
    "notification_email.py",
    "notification_service.py",
    "harvhub_ipc.py",
    "harvcore_console_supervisor.py",
    "requirements-runtime.txt",
)
DEFAULT_RUNTIME_REQUIREMENTS = "# HarvHub AI runtime requirements\n" + "\n".join((
    "MetaTrader5", "pytz", "tzdata", "requests", "urllib3", "certifi",
    "selenium", "beautifulsoup4", "webdriver-manager",
)) + "\n"


def copy_file_with_progress(source: Path, destination: Path, label: str, progress=None) -> None:
    """Copy one payload file in chunks and report byte progress."""
    if not source.is_file():
        raise FileNotFoundError(f"Copy source for {label} is missing: {source}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    total = source.stat().st_size
    copied = 0
    last_report = time.monotonic()
    if callable(progress):
        progress(f"[COPY] {label}: starting {total:,}-byte copy to {destination}.")
    with source.open("rb") as src, destination.open("wb") as dst:
        while True:
            chunk = src.read(1024 * 1024)
            if not chunk:
                break
            dst.write(chunk)
            copied += len(chunk)
            now = time.monotonic()
            if callable(progress) and (now - last_report >= 5 or copied == total):
                progress(f"[COPY] {label}: {copied:,}/{total:,} bytes copied.")
                last_report = now
        dst.flush()
        os.fsync(dst.fileno())
    shutil.copystat(source, destination)
    if destination.stat().st_size != total:
        raise OSError(f"Copy size verification failed for {label}: expected {total:,} bytes, got {destination.stat().st_size:,} bytes.")
    if callable(progress):
        progress(f"[COPY] {label}: copy completed and size-verified.")


def prepare_payload_sources(progress=None) -> Path:
    """Stage and verify every runtime source required by HarvHub AI.

    A progress callback receives each package check/copy operation so setup
    never silently stops while preparing the embedded payload.
    """
    def report(message: str) -> None:
        if callable(progress):
            progress("[PAYLOAD] " + str(message))

    payload = APP_DIR / "payload"
    payload.mkdir(parents=True, exist_ok=True)
    report(f"Preparing payload directory: {payload}")

    for index, name in enumerate(REQUIRED_PAYLOAD_FILES, start=1):
        target = payload / name
        minimum_size = 1 if name == "requirements-runtime.txt" else 100
        report(f"[{index}/{len(REQUIRED_PAYLOAD_FILES)}] locating {name}")
        root_source = APP_DIR / name
        if root_source.is_file() and root_source.stat().st_size >= minimum_size:
            needs_copy = not target.is_file()
            if not needs_copy:
                try:
                    needs_copy = (
                        root_source.stat().st_size != target.stat().st_size
                        or sha256_file(root_source, progress, name + " root source") != sha256_file(target, progress, name + " payload copy")
                    )
                except OSError as exc:
                    report(f"Could not compare payload copy for {name}: {type(exc).__name__}: {exc}; recopying.")
                    needs_copy = True
            if needs_copy:
                report(f"Copying current project source into payload: {name}")
                target.parent.mkdir(parents=True, exist_ok=True)
                temporary = target.with_name(target.name + ".staging")
                try:
                    copy_file_with_progress(root_source, temporary, name, progress)
                    if sha256_file(root_source, progress, name + " source verify") != sha256_file(temporary, progress, name + " staged verify"):
                        raise OSError(f"Payload staging SHA-256 mismatch for {name}.")
                    temporary.replace(target)
                finally:
                    try:
                        temporary.unlink(missing_ok=True)
                    except OSError:
                        pass
            report(f"Verified payload source: {name} ({target.stat().st_size:,} bytes)")
            continue

        if target.is_file() and target.stat().st_size >= minimum_size:
            report(f"Using existing payload copy: {name} ({target.stat().st_size:,} bytes)")
            continue

        candidates = []
        for candidate in APP_DIR.rglob(name):
            if candidate == target or not candidate.is_file():
                continue
            relative_parts = {part.lower() for part in candidate.relative_to(APP_DIR).parts}
            if relative_parts.intersection({".build-venv", "build", "release", "dist", ".git"}):
                continue
            if "payload" in relative_parts:
                continue
            if candidate.stat().st_size >= minimum_size:
                candidates.append(candidate)
        if candidates:
            candidates.sort(key=lambda item: (len(item.relative_to(APP_DIR).parts), str(item).lower()))
            report(f"Found {name} at {candidates[0]}; staging into payload.")
            temporary = target.with_name(target.name + ".staging")
            try:
                copy_file_with_progress(candidates[0], temporary, name, progress)
                if sha256_file(candidates[0], progress, name + " nested source verify") != sha256_file(temporary, progress, name + " staged verify"):
                    raise OSError(f"Payload staging SHA-256 mismatch for {name}.")
                temporary.replace(target)
            finally:
                try:
                    temporary.unlink(missing_ok=True)
                except OSError:
                    pass
            report(f"Verified payload source: {name} ({target.stat().st_size:,} bytes)")
            continue

        if name == "requirements-runtime.txt":
            report("Runtime requirements file is absent; writing the documented default dependency list.")
            target.write_text(DEFAULT_RUNTIME_REQUIREMENTS, encoding="utf-8")
            continue

        raise FileNotFoundError(
            f"Required HarvHub source '{name}' is missing. The GitHub bootstrap must download it into "
            f"the project before building. Checked root '{APP_DIR}' and payload '{payload}'."
        )

    missing = [
        name for name in REQUIRED_PAYLOAD_FILES
        if not (payload / name).is_file()
        or (payload / name).stat().st_size < (1 if name == "requirements-runtime.txt" else 100)
    ]
    if missing:
        raise FileNotFoundError("Incomplete HarvHub AI payload after staging: " + ", ".join(missing))
    report(f"Payload ready: all {len(REQUIRED_PAYLOAD_FILES)} required files are present and non-empty.")
    return payload


def newest_build_input_mtime() -> float:
    newest = 0.0
    for current_root, dirs, files in os.walk(APP_DIR):
        dirs[:] = [name for name in dirs if name not in BUILD_EXCLUDED_DIRS and not name.startswith(".")]
        for name in files:
            path = Path(current_root) / name
            if path.suffix.lower() not in BUILD_INPUT_SUFFIXES:
                continue
            try:
                newest = max(newest, path.stat().st_mtime)
            except OSError:
                continue
    return newest


def output_is_current() -> bool:
    try:
        return OUTPUT_EXE.is_file() and OUTPUT_EXE.stat().st_size >= 1024 * 1024 and OUTPUT_EXE.stat().st_mtime >= newest_build_input_mtime()
    except OSError:
        return False


PALETTE = {
    "bg": "#faf7ef",
    "surface": "#fffdf8",
    "surface2": "#f7f0df",
    "text": "#302718",
    "muted": "#82745d",
    "border": "#e8dec8",
    "gold": "#b88720",
    "gold_hover": "#946b16",
    "green": "#247a4b",
    "red": "#b94335",
    "console": "#171915",
    "console_text": "#e6eadf",
}


def creation_flags() -> int:
    return getattr(subprocess, "CREATE_NO_WINDOW", 0)


def inspect_python(executable: Path) -> tuple[bool, str]:
    if not executable.is_file():
        return False, "not found"
    command = (
        "import sys,struct,tkinter; "
        "print(f'{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}'); "
        "print(struct.calcsize('P')*8)"
    )
    try:
        result = subprocess.run(
            [str(executable), "-c", command], capture_output=True, text=True,
            timeout=15, creationflags=creation_flags(),
        )
        lines = (result.stdout or "").strip().splitlines()
        if result.returncode != 0 or len(lines) < 2 or lines[1].strip() != "64":
            return False, "not a compatible 64-bit Python with Tkinter"
        version = tuple(int(x) for x in lines[0].split(".")[:2])
        if version < (3, 10) or version >= (3, 15):
            return False, f"unsupported Python {lines[0]}"
        return True, lines[0]
    except Exception as exc:
        return False, str(exc)


def find_build_python() -> Path | None:
    candidates: list[Path] = []
    # When launched with pythonw.exe, prefer the sibling python.exe.
    current = Path(sys.executable)
    if current.name.lower() == "pythonw.exe":
        candidates.append(current.with_name("python.exe"))
    elif current.name.lower() == "python.exe":
        candidates.append(current)

    roots = [
        Path(os.environ.get("ProgramFiles", r"C:\Program Files")),
        Path(os.environ.get("LOCALAPPDATA", str(Path.home() / "AppData/Local"))) / "Programs" / "Python",
    ]
    for root in roots:
        for version in ("Python314", "Python313", "Python312", "Python311", "Python310"):
            candidates.append(root / version / "python.exe")

    launcher = shutil_which("py.exe") or shutil_which("py")
    if launcher:
        for version in ("3.14", "3.13", "3.12", "3.11", "3.10"):
            try:
                result = subprocess.run(
                    [launcher, f"-{version}", "-c", "import sys; print(sys.executable)"],
                    capture_output=True, text=True, timeout=12, creationflags=creation_flags(),
                )
                found = (result.stdout or "").strip().splitlines()
                if result.returncode == 0 and found:
                    candidates.append(Path(found[-1].strip()))
            except Exception:
                pass

    for command in ("python.exe", "python", "python3.exe", "python3"):
        executable = shutil_which(command)
        if executable:
            candidates.append(Path(executable))

    seen: set[str] = set()
    for candidate in candidates:
        marker = str(candidate).lower()
        if marker in seen:
            continue
        seen.add(marker)
        compatible, _ = inspect_python(candidate)
        if compatible:
            return candidate.resolve()
    return None


def shutil_which(name: str) -> str | None:
    import shutil
    return shutil.which(name)


def sha256_file(path: Path, progress=None, label: str = "file") -> str:
    """Return SHA-256 using bounded chunks and optional progress reporting."""
    digest = hashlib.sha256()
    file_path = Path(path)
    total = file_path.stat().st_size
    processed = 0
    last_report = time.monotonic()
    with file_path.open("rb") as source:
        while True:
            chunk = source.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            processed += len(chunk)
            now = time.monotonic()
            if callable(progress) and (now - last_report >= 5 or processed == total):
                progress(f"[HASH] {label}: {processed:,}/{total:,} bytes hashed.")
                last_report = now
    return digest.hexdigest().lower()


class Builder:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.events: queue.Queue = queue.Queue()
        self.running = False
        self.log_lines: list[str] = []
        self.console_visible = False
        self.handed_off = False
        self.log_lock = threading.RLock()
        configured_log = os.environ.get("HARVHUB_BUILDER_LOG")
        self.log_file_path = Path(configured_log) if configured_log else (APP_DIR / ".manager" / "builder_output.log")
        try:
            self.log_file_path.parent.mkdir(parents=True, exist_ok=True)
            self.log_file_path.write_text("", encoding="utf-8")
        except OSError:
            pass
        self.root.title("HarvHub AI")
        self.root.geometry("900x650")
        self.root.minsize(520, 500)
        self.root.configure(bg=PALETTE["bg"])
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        self.watchdog_stop = threading.Event()
        self.cancel_event = threading.Event()
        self.activity_lock = threading.RLock()
        self.active_operation = "Builder startup"
        self.last_activity_message = "Creating setup interface."
        self.last_activity_at = time.monotonic()
        self.last_watchdog_bucket = 0
        self.stalled = False
        self.watchdog_thread = None
        self._build_ui()
        self.root.after(100, self._poll)
        # Rely on Windows initial process activation; never reassert focus later.
        self.root.after(250, self.start)


    def _watchdog_loop(self):
        while not self.watchdog_stop.wait(10):
            with self.activity_lock:
                idle = max(0.0, time.monotonic() - self.last_activity_at)
                operation = self.active_operation
                detail = self.last_activity_message
            if idle < 30:
                continue
            bucket = int(idle // 30)
            if bucket <= self.last_watchdog_bucket:
                continue
            self.last_watchdog_bucket = bucket
            message = (
                f"[WATCHDOG] NO PROGRESS for {int(idle)} seconds. Current operation: {operation}. "
                f"Last detail: {detail}. The operation has not emitted a new progress event."
            )
            self.log(message, activity=False)
            if idle >= 600:
                self.stalled = True
                self.cancel_event.set()
                self.events.put(("stalled", message + " The builder has requested cancellation after 10 minutes without verified progress. Any active child process will be terminated by the build runner."))
                return

    def button(self, parent, text, command, primary=False):
        return tk.Button(
            parent, text=text, command=command,
            bg=PALETTE["gold"] if primary else PALETTE["surface2"],
            fg="#fff" if primary else PALETTE["text"],
            activebackground=PALETTE["gold_hover"], activeforeground="#fff",
            relief="flat", bd=0, padx=16, pady=10,
            font=("Segoe UI", 9, "bold"), cursor="hand2",
        )

    def _build_ui(self):
        header = tk.Frame(self.root, bg=PALETTE["surface"], highlightbackground=PALETTE["border"], highlightthickness=1)
        header.pack(fill="x", padx=20, pady=(20, 12))
        mark = tk.Label(header, text="H", bg=PALETTE["gold"], fg="white", font=("Segoe UI", 18, "bold"), width=3, pady=7)
        mark.pack(side="left", padx=16, pady=14)
        title = tk.Frame(header, bg=PALETTE["surface"])
        title.pack(side="left", pady=14)
        tk.Label(title, text="HARVHUB AI", bg=PALETTE["surface"], fg=PALETTE["text"], font=("Segoe UI", 16, "bold")).pack(anchor="w")
        tk.Label(title, text="HARVHUB AI · AUTOMATIC SETUP", bg=PALETTE["surface"], fg=PALETTE["muted"], font=("Segoe UI", 8, "bold")).pack(anchor="w")

        card = tk.Frame(self.root, bg=PALETTE["surface"], padx=24, pady=22, highlightbackground=PALETTE["border"], highlightthickness=1)
        card.pack(fill="both", expand=True, padx=20, pady=(0, 12))
        tk.Label(card, text="Preparing HarvHub AI", bg=PALETTE["surface"], fg=PALETTE["text"], font=("Segoe UI", 18, "bold")).pack(anchor="w")
        tk.Label(card, text="HarvHub AI checks the existing application and automatically builds it when it is missing or out of date.", bg=PALETTE["surface"], fg=PALETTE["muted"], font=("Segoe UI", 9), wraplength=800, justify="left").pack(anchor="w", pady=(6, 18))
        self.status = tk.Label(card, text="Checking the HarvHub AI application...", bg=PALETTE["surface2"], fg=PALETTE["text"], padx=14, pady=12, anchor="w", font=("Segoe UI", 10, "bold"))
        self.status.pack(fill="x", pady=(0, 12))
        self.progress = ttk.Progressbar(card, mode="determinate", maximum=100)
        self.progress.pack(fill="x", pady=(0, 18))

        self.stage_labels = []
        for text in (
            "Detect build Python",
            "Create private build environment",
            "Install packaging tools",
            "Create HarvHubAI.exe",
            "Install app and desktop shortcuts",
        ):
            lbl = tk.Label(card, text="○  " + text, bg=PALETTE["surface"], fg=PALETTE["muted"], font=("Segoe UI", 9))
            lbl.pack(anchor="w", pady=4)
            self.stage_labels.append(lbl)

        controls = tk.Frame(card, bg=PALETTE["surface"])
        controls.pack(fill="x", pady=(22, 0))
        self.console_button = self.button(controls, "Show Build Details", self.toggle_console)
        self.console_button.pack(side="left")
        self.copy_button = self.button(controls, "Copy Build Report", self.copy_report)
        self.copy_button.pack(side="left", padx=(8, 0))
        self.open_button = self.button(controls, "Open Output Folder", self.open_output)
        self.open_button.pack(side="right")

        self.console = ScrolledText(
            card, height=10, wrap="word", state="disabled",
            bg=PALETTE["console"], fg=PALETTE["console_text"],
            insertbackground=PALETTE["console_text"], font=("Consolas", 9),
            relief="flat", padx=10, pady=8,
        )
        # Build detail is also hidden by default.
        self.console.pack_forget()
        footer = tk.Label(self.root, text="No command window or build button is required. The application is checked and built automatically.", bg=PALETTE["bg"], fg=PALETTE["muted"], font=("Segoe UI", 8))
        footer.pack(fill="x", padx=22, pady=(0, 12))

    def log(self, message: str, *, activity: bool = True):
        text = str(message)
        if activity:
            with self.activity_lock:
                self.last_activity_at = time.monotonic()
                self.last_activity_message = text
        try:
            with self.log_lock:
                with self.log_file_path.open("a", encoding="utf-8", errors="replace") as log_file:
                    log_file.write(text + "\n")
        except OSError:
            pass
        self.events.put(("log", text))

    def set_status(self, message: str):
        with self.activity_lock:
            self.active_operation = str(message)
            self.last_activity_message = str(message)
            self.last_activity_at = time.monotonic()
        self.events.put(("status", message))

    def set_stage(self, index: int, state: str):
        with self.activity_lock:
            stage_name = self.stage_labels[index].cget("text") if index < len(self.stage_labels) else f"Stage {index}"
            self.active_operation = f"{stage_name} ({state})"
            self.last_activity_message = self.active_operation
            self.last_activity_at = time.monotonic()
        self.events.put(("stage", index, state))

    def _run(self, command: list[str], label: str, timeout: int = 2400):
        self.log(f"[START] {label} | timeout={timeout}s")
        self.log("$ " + subprocess.list2cmdline(command))
        try:
            process = subprocess.Popen(
                command, cwd=str(APP_DIR), stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                text=True, encoding="utf-8", errors="replace", bufsize=1,
                creationflags=creation_flags(),
            )
        except Exception as exc:
            raise RuntimeError(f"Could not start {label}: {type(exc).__name__}: {exc}") from exc
        started = time.monotonic()
        last_output_at = started
        last_heartbeat = started
        last_output = "(no output received)"
        last_no_output_notice = 0
        output_queue: queue.Queue[str | None] = queue.Queue()

        def reader():
            try:
                if process.stdout:
                    for line in process.stdout:
                        output_queue.put(line.rstrip("\r\n"))
            finally:
                output_queue.put(None)

        thread = threading.Thread(target=reader, daemon=True)
        thread.start()
        reader_done = False
        while process.poll() is None:
            now = time.monotonic()
            elapsed = now - started
            if self.cancel_event.is_set():
                if os.name == "nt":
                    try:
                        subprocess.run(
                            ["taskkill.exe", "/PID", str(process.pid), "/T", "/F"],
                            capture_output=True, text=True, timeout=15,
                            creationflags=creation_flags(),
                        )
                    except Exception as exc:
                        self.log(f"[WARNING] Could not stop cancelled process tree for {label}: {exc}", activity=False)
                try:
                    process.kill()
                    process.wait(timeout=10)
                except Exception:
                    pass
                raise TimeoutError(
                    f"{label} was stopped because the build watchdog detected 10 minutes without verified output/progress. "
                    f"PID={process.pid}; elapsed={int(elapsed)}s; last output={last_output[:500]}"
                )
            no_output_idle = now - last_output_at
            if no_output_idle >= 30:
                notice_bucket = int(no_output_idle // 30)
                if notice_bucket > last_no_output_notice:
                    self.log(
                        f"[NO PROGRESS] {label}: child PID={process.pid}; elapsed={int(elapsed)}s; "
                        f"no new console output for {int(no_output_idle)}s; last output={last_output[:250]}",
                        activity=False,
                    )
                    last_no_output_notice = notice_bucket
            if elapsed > timeout:
                if os.name == "nt":
                    try:
                        subprocess.run(
                            ["taskkill.exe", "/PID", str(process.pid), "/T", "/F"],
                            capture_output=True, text=True, timeout=15,
                            creationflags=creation_flags(),
                        )
                    except Exception as exc:
                        self.log(f"[WARNING] Could not stop timed-out process tree for {label}: {exc}")
                try:
                    process.kill()
                    process.wait(timeout=10)
                except Exception:
                    pass
                raise TimeoutError(
                    f"{label} timed out after {int(elapsed)}s (PID {process.pid}). "
                    f"Last output: {last_output}"
                )
            try:
                line = output_queue.get(timeout=0.2)
                if line is None:
                    reader_done = True
                else:
                    last_output = line
                    last_output_at = time.monotonic()
                    self.log(line)
            except queue.Empty:
                pass
            if now - last_heartbeat >= 15 and process.poll() is None:
                silent = int(now - last_output_at)
                self.log(
                    f"[HEARTBEAT] {label} is still running (PID {process.pid}); "
                    f"elapsed={int(elapsed)}s; no child output for {silent}s; "
                    f"last output: {last_output[:250]}",
                    activity=False,
                )
                if silent >= 60:
                    self.log(f"[NO OUTPUT] {label} has produced no new console output for {silent}s. Waiting for process completion or the {timeout}s timeout.")
                last_heartbeat = now
        drain_deadline = time.monotonic() + 8
        while not reader_done and time.monotonic() < drain_deadline:
            try:
                line = output_queue.get(timeout=0.2)
                if line is None:
                    reader_done = True
                else:
                    last_output = line
                    self.log(line)
            except queue.Empty:
                if not thread.is_alive():
                    break
        if not reader_done and thread.is_alive():
            self.log(f"[WARNING] {label} exited, but inherited child handles kept its output pipe open. Stopped waiting after 8s; exit code={process.returncode}.")
            try:
                if process.stdout:
                    process.stdout.close()
            except Exception:
                pass
        if process.returncode != 0:
            raise RuntimeError(f"{label} failed (exit code {process.returncode}). Last output: {last_output}")
        self.log(f"[DONE] {label} completed successfully in {int(time.monotonic() - started)}s (exit code 0).")

    def start(self):
        if self.running:
            return
        self.running = True
        self.stalled = False
        self.cancel_event.clear()
        self.watchdog_stop.clear()
        with self.activity_lock:
            self.last_activity_at = time.monotonic()
            self.active_operation = "Build startup"
            self.last_activity_message = "Starting build worker."
        self.watchdog_thread = threading.Thread(target=self._watchdog_loop, name="harvhub-builder-watchdog", daemon=True)
        self.watchdog_thread.start()
        self.log_lines = []
        for label in self.stage_labels:
            text = label.cget("text").split("  ", 1)[-1]
            label.configure(text="○  " + text, fg=PALETTE["muted"])
        if self.console_visible:
            self.console.configure(state="normal")
            self.console.delete("1.0", "end")
            self.console.configure(state="disabled")
        self.progress.configure(mode="indeterminate")
        self.progress.start(12)
        threading.Thread(target=self._worker, name="harvhub-exe-build", daemon=True).start()

    def _worker(self):
        try:
            self.set_status("Detecting a compatible build interpreter...")
            self.set_stage(0, "running")
            python_exe = find_build_python()
            if not python_exe:
                raise RuntimeError("No compatible 64-bit Python 3.10–3.14 with Tkinter was found on this build computer. Install Python 3.12/3.13 x64 with Tcl/Tk, then reopen this builder.")
            ok, version = inspect_python(python_exe)
            self.log(f"Selected interpreter: {python_exe}")
            self.log(f"Python version: {version}")
            self.set_stage(0, "done")

            self.set_status("Creating the isolated build environment...")
            self.set_stage(1, "running")
            if not BUILD_PYTHON.is_file():
                self._run([str(python_exe), "-m", "venv", str(BUILD_ENV)], "Create build environment", timeout=600)
            ok, info = inspect_python(BUILD_PYTHON)
            if not ok:
                raise RuntimeError("The private build Python could not be verified: " + info)
            self.log("Private build interpreter verified: " + info)
            self.set_stage(1, "done")

            self.set_status("Installing the packaging tools...")
            self.set_stage(2, "running")
            self._run([str(BUILD_PYTHON), "-m", "pip", "install", "--disable-pip-version-check", "--upgrade", "pip", "pyinstaller", "certifi"], "Install PyInstaller and TLS certificate bundle", timeout=1800)
            self.set_stage(2, "done")

            self.set_status("Checking and staging all HarvHub AI source files...")
            self.log(f"Source payload ready: {prepare_payload_sources(progress=self.log)}")

            self.set_status("Creating HarvHubAI.exe...")
            self.set_stage(3, "running")
            (APP_DIR / "release").mkdir(exist_ok=True)
            command = [
                str(BUILD_PYTHON), "-m", "PyInstaller",
                "--noconfirm", "--clean", "--onefile", "--windowed", "--uac-admin",
                "--name", "HarvHubAI",
                "--collect-data", "certifi",
                "--distpath", str(APP_DIR / "release"),
                "--workpath", str(APP_DIR / "build"),
                "--specpath", str(APP_DIR / "build"),
                "--add-data", f"{APP_DIR / 'payload'};payload",
                str(APP_DIR / "harvcore_manager.py"),
            ]
            self._run(command, "Build HarvHubAI.exe", timeout=3600)
            if not OUTPUT_EXE.is_file() or OUTPUT_EXE.stat().st_size < 1024 * 1024:
                raise RuntimeError("The build ended but HarvHubAI.exe was not found or is unexpectedly small.")
            self.set_stage(3, "done")

            self.set_status("Installing HarvHub AI and creating shortcuts...")
            self.set_stage(4, "running")
            installed_exe = install_current_release_files(progress=self.log)
            self.log(f"INSTALLED: {installed_exe}")
            self.set_stage(4, "done")
            self.set_status("Opening HarvHub AI...")
            self.log(f"BUILD OUTPUT: {OUTPUT_EXE}")
            self.events.put(("complete", str(installed_exe)))
        except Exception as exc:
            diagnostic = traceback.format_exc().strip()
            self.log("BUILD FAILED: " + str(exc))
            self.log(diagnostic)
            self.set_status("Build needs attention. The error is shown in the console below.")
            self.events.put(("failed", str(exc)))

    def _poll(self):
        try:
            while True:
                event = self.events.get_nowait()
                kind = event[0]
                if kind == "log":
                    self.log_lines.append(str(event[1]))
                    if len(self.log_lines) > 30000:
                        self.log_lines = self.log_lines[-30000:]
                    if self.console_visible:
                        self.console.configure(state="normal")
                        self.console.insert("end", str(event[1]) + "\n")
                        self.console.see("end")
                        self.console.configure(state="disabled")
                elif kind == "status":
                    self.status.configure(text=event[1])
                elif kind == "stage":
                    index, state = event[1], event[2]
                    prefix = "●" if state == "done" else "◉" if state == "running" else "○"
                    color = PALETTE["green"] if state == "done" else PALETTE["gold"] if state == "running" else PALETTE["muted"]
                    text = self.stage_labels[index].cget("text").split("  ", 1)[-1]
                    self.stage_labels[index].configure(text=f"{prefix}  {text}", fg=color)
                elif kind == "stalled":
                    self.stalled = True
                    self.progress.stop()
                    self.running = False
                    self.status.configure(text="Setup stalled — the exact operation is shown in the console below.")
                    self.console_visible = True
                    self.console.pack(fill="both", expand=True, pady=(14, 0))
                    self.console.configure(state="normal")
                    self.console.insert("end", str(event[1]) + "\n")
                    self.console.see("end")
                    self.console.configure(state="disabled")
                    self.console_button.configure(text="Hide Build Details")
                elif kind == "complete":
                    if self.stalled:
                        continue
                    self.watchdog_stop.set()
                    self.progress.stop()
                    self.progress.configure(mode="determinate", value=100)
                    self.running = False
                    self.status.configure(text="Preparing HarvHub AI...")
                    # Do not start a second EXE process here. Transfer this exact
                    # Tk root into the real manager interface so build progress
                    # turns into the HarvHub AI in the same window.
                    self.root.after(250, self.open_manager_in_same_window)
                elif kind == "failed":
                    self.watchdog_stop.set()
                    self.progress.stop()
                    self.progress.configure(mode="determinate", value=0)
                    self.running = False
                    self.status.configure(text="Setup needs attention. The error is shown in the console below.")
                    # The builder reports its own error in its copyable console;
                    # no CMD window or external terminal is opened.
                    self.console_visible = True
                    self.console.pack(fill="both", expand=True, pady=(14, 0))
                    self.console.configure(state="normal")
                    self.console.delete("1.0", "end")
                    self.console.insert("end", "\n".join(self.log_lines))
                    self.console.see("end")
                    self.console.configure(state="disabled")
                    self.console_button.configure(text="Hide Build Details")
        except queue.Empty:
            pass
        if not self.handed_off:
            self.root.after(100, self._poll)

    def open_manager_in_same_window(self):
        """Install the built app, then replace the builder on this same Tk root."""
        try:
            # The worker has already installed the EXE and created shortcuts.
            # Transfer this exact root into the manager without launching a
            # second process or displaying a success modal.
            self.handed_off = True
            self.progress.stop()
            for child in self.root.winfo_children():
                try:
                    child.destroy()
                except tk.TclError:
                    pass

            if str(APP_DIR) not in sys.path:
                sys.path.insert(0, str(APP_DIR))
            import harvcore_manager

            # HarvCoreManager configures this existing root and starts its own
            # event polling. Tk.mainloop() remains the single original loop.
            harvcore_manager.HarvCoreManager(self.root)
        except Exception as exc:
            self.handed_off = False
            self.log("Could not open HarvHub AI in the current window: " + str(exc))
            self.log(traceback.format_exc().strip())
            # The builder widgets have already been removed. Recreate a compact
            # in-window recovery screen instead of displaying a modal dialog.
            for child in self.root.winfo_children():
                try:
                    child.destroy()
                except tk.TclError:
                    pass
            panel = tk.Frame(self.root, bg=PALETTE["surface"], padx=28, pady=26,
                             highlightbackground=PALETTE["border"], highlightthickness=1)
            panel.pack(fill="both", expand=True, padx=24, pady=24)
            tk.Label(panel, text="HarvHub AI needs attention", bg=PALETTE["surface"],
                     fg=PALETTE["text"], font=("Segoe UI", 17, "bold")).pack(anchor="w")
            tk.Label(panel, text="HarvHub AI could not be opened. The full technical report is ready to copy.",
                     bg=PALETTE["surface"], fg=PALETTE["muted"], font=("Segoe UI", 9),
                     wraplength=720, justify="left").pack(anchor="w", pady=(10, 18))
            self.console = ScrolledText(panel, height=14, wrap="word", state="normal",
                bg=PALETTE["console"], fg=PALETTE["console_text"], font=("Consolas", 9), relief="flat")
            self.console.insert("end", "\n".join(self.log_lines))
            self.console.configure(state="disabled")
            self.console.pack(fill="both", expand=True)
            controls = tk.Frame(panel, bg=PALETTE["surface"])
            controls.pack(fill="x", pady=(14, 0))
            self.copy_button = self.button(controls, "COPY BUILD REPORT", self.copy_report, primary=True)
            self.copy_button.pack(side="left")
            self.open_button = self.button(controls, "OPEN PROJECT FOLDER", self.open_output)
            self.open_button.pack(side="left", padx=(8, 0))

    def toggle_console(self):
        if self.console_visible:
            self.console.pack_forget()
            self.console_visible = False
            self.console_button.configure(text="Show Build Details")
        else:
            self.console.pack(fill="both", expand=True, pady=(14, 0))
            self.console_visible = True
            self.console_button.configure(text="Hide Build Details")
            self.console.configure(state="normal")
            self.console.delete("1.0", "end")
            self.console.insert("end", "\n".join(self.log_lines))
            self.console.configure(state="disabled")
            self.console.see("end")

    def copy_report(self):
        report = "HarvHub AI Build Report\n" + "=" * 45 + "\n" + "\n".join(self.log_lines)
        try:
            self.root.clipboard_clear()
            self.root.clipboard_append(report)
            self.root.update_idletasks()
            self.copy_button.configure(text="Copied")
            self.root.after(1500, lambda: self.copy_button.configure(text="Copy Build Report") if self.copy_button.winfo_exists() else None)
        except tk.TclError:
            pass

    def open_output(self):
        folder = APP_DIR / "release"
        folder.mkdir(exist_ok=True)
        if os.name == "nt":
            os.startfile(str(folder))

    def on_close(self):
        if self.running and not messagebox.askyesno("Close builder?", "The build is still running. Stop the active build process and close the builder?", parent=self.root):
            return
        if self.running:
            self.cancel_event.set()
        self.watchdog_stop.set()
        self.root.destroy()


def is_admin() -> bool:
    if os.name != "nt":
        return False
    try:
        return bool(ctypes.windll.shell32.IsUserAnAdmin())
    except Exception:
        return False


def relaunch_self_as_admin() -> bool:
    """Elevate the no-console bootstrap before installing to protected paths."""
    if is_admin():
        return True
    try:
        script = str(Path(__file__).resolve())
        params = subprocess.list2cmdline([script])
        result = ctypes.windll.shell32.ShellExecuteW(
            None, "runas", sys.executable, params, str(APP_DIR), 1
        )
        return int(result) > 32
    except Exception:
        return False


def launch_output_exe_elevated() -> None:
    """Use ShellExecute so the EXE's requireAdministrator manifest is honoured."""
    if not OUTPUT_EXE.is_file():
        raise FileNotFoundError(f"HarvHubAI.exe was not found: {OUTPUT_EXE}")
    result = ctypes.windll.shell32.ShellExecuteW(
        None, "runas", str(OUTPUT_EXE), None, str(OUTPUT_EXE.parent), 1
    )
    if int(result) <= 32:
        raise OSError(f"Windows could not launch HarvHub AI (ShellExecute code {int(result)}).")


BUNDLED_SOURCE_FILES = (
    "mysqlremote.py",
    "brokerdetails.py",
    "brokercharts.py",
    "programme_training.py",
    "signals_trading.py",
    "signals_provision_challenge.py",
    "ip.py",
    "notification_email.py",
    "notification_service.py",
    "harvhub_ipc.py",
    "harvcore_console_supervisor.py",
    "requirements-runtime.txt",
)


def _backup_destination_file(destination: Path, backup_root: Path, relative_name: str, progress=None) -> None:
    if not destination.is_file():
        return
    backup = backup_root / relative_name
    backup.parent.mkdir(parents=True, exist_ok=True)
    total = destination.stat().st_size
    copied = 0
    last_report = time.monotonic()
    if callable(progress):
        progress(f"[BACKUP] {relative_name}: backing up {total:,} bytes to {backup}.")
    with destination.open("rb") as source, backup.open("wb") as target:
        while True:
            chunk = source.read(1024 * 1024)
            if not chunk:
                break
            target.write(chunk)
            copied += len(chunk)
            now = time.monotonic()
            if callable(progress) and (now - last_report >= 5 or copied == total):
                progress(f"[BACKUP] {relative_name}: {copied:,}/{total:,} bytes copied.")
                last_report = now
        target.flush()
        os.fsync(target.fileno())
    shutil.copystat(destination, backup)
    if backup.stat().st_size != total:
        raise OSError(f"Backup size verification failed for {destination}: expected {total:,}, received {backup.stat().st_size:,}.")


def _atomic_copy_latest(source: Path, destination: Path, backup_root: Path, relative_name: str, *, force: bool = False, progress=None) -> bool:
    """Atomically install one file, reporting each check/copy/verification step."""
    def report(message: str) -> None:
        if callable(progress):
            progress(f"[DEPLOY] {relative_name}: {message}")

    if not source.is_file():
        raise FileNotFoundError(f"Latest HarvHub source file is missing: {source}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    report(f"checking source ({source.stat().st_size:,} bytes) and destination {destination}")
    same = False
    if destination.is_file():
        try:
            same = sha256_file(destination, progress, relative_name + " installed copy") == sha256_file(source, progress, relative_name + " source")
        except OSError as exc:
            report(f"hash comparison failed ({exc}); replacing the destination")
            same = False
    if same and not force:
        report("already current (SHA-256 verified)")
        return False
    if destination.is_file():
        report("backing up existing file")
        _backup_destination_file(destination, backup_root, relative_name, progress=progress)
    temporary = destination.with_name(destination.name + ".harvhub-new")
    try:
        report("copying to temporary file")
        with source.open("rb") as src, temporary.open("wb") as dst:
            copied = 0
            total = source.stat().st_size
            while True:
                chunk = src.read(1024 * 1024)
                if not chunk:
                    break
                dst.write(chunk)
                copied += len(chunk)
                if callable(progress) and (copied == total or copied % (16 * 1024 * 1024) == 0):
                    report(f"copied {copied:,}/{total:,} bytes")
            dst.flush()
            os.fsync(dst.fileno())
        shutil.copystat(source, temporary)
        report("verifying copied SHA-256")
        if sha256_file(temporary, progress, relative_name + " temporary copy") != sha256_file(source, progress, relative_name + " source verification"):
            raise OSError(f"Copied file verification failed: {destination.name}")
        temporary.replace(destination)
        report("installed and verified")
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
    return True


def install_current_release_files(progress=None) -> Path:
    """Always replace the installed EXE and deploy source from the selected latest project."""
    install_root = Path(os.environ.get("HARVHUBAI_ROOT", r"C:\xampp\htdocs\harvhubAI"))
    destination = install_root / "HarvHubAI.exe"
    install_root.mkdir(parents=True, exist_ok=True)
    if not OUTPUT_EXE.is_file():
        raise FileNotFoundError(f"The build output is missing: {OUTPUT_EXE}")

    stamp = time.strftime("%Y%m%d-%H%M%S")
    backup_root = install_root / ".manager" / "backups" / stamp
    if callable(progress):
        progress(f"[DEPLOY] Preparing install root {install_root}; backup folder {backup_root}.")

    # The VBS launcher checks that the installed app/bootstrap is not running
    # before it starts this forced update, so replacing the EXE is safe.
    _atomic_copy_latest(OUTPUT_EXE, destination, backup_root, "HarvHubAI.exe", force=True, progress=progress)
    if not destination.is_file() or sha256_file(destination) != sha256_file(OUTPUT_EXE):
        raise RuntimeError("The installed HarvHubAI.exe did not pass SHA-256 verification.")

    source_root = prepare_payload_sources()
    python_root = install_root / "python"
    python_root.mkdir(parents=True, exist_ok=True)
    source_manifest = {}
    copied_count = 0
    unchanged_count = 0
    for index, name in enumerate(BUNDLED_SOURCE_FILES, start=1):
        if callable(progress):
            progress(f"[DEPLOY] [{index}/{len(BUNDLED_SOURCE_FILES)}] Installing runtime file {name}.")
        source = source_root / name
        target = python_root / name
        changed = _atomic_copy_latest(source, target, backup_root, f"python/{name}", progress=progress)
        source_manifest[name] = sha256_file(source)
        if changed:
            copied_count += 1
        else:
            unchanged_count += 1

    # Preserve the complete selected project under HarvHub AI. The VBS remains
    # available for explicit updates; installed desktop shortcuts open the EXE.
    manager_dir = install_root / ".manager"
    manager_dir.mkdir(parents=True, exist_ok=True)
    installed_source = manager_dir / "source"
    installed_source.mkdir(parents=True, exist_ok=True)
    project_files = (
        "HarvHubAI.vbs",
        "Build_HarvHubProgram.vbs",
        "Build_HarvHubProgram.pyw",
        "harvcore_manager.py",
        "README.md",
        "START_HERE.txt",
    )
    for name in project_files:
        _atomic_copy_latest(
            APP_DIR / name, installed_source / name, backup_root,
            f".manager/source/{name}", progress=progress,
        )
    installed_payload = installed_source / "payload"
    installed_payload.mkdir(parents=True, exist_ok=True)
    for name in BUNDLED_SOURCE_FILES:
        _atomic_copy_latest(
            source_root / name, installed_payload / name, backup_root,
            f".manager/source/payload/{name}", progress=progress,
        )

    manifest = {
        "application": "HarvHub AI",
        "source_project": str(APP_DIR),
        "deployed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "files": source_manifest,
    }
    supervisor_target = python_root / "harvcore_console_supervisor.py"
    if not supervisor_target.is_file() or supervisor_target.stat().st_size < 100:
        raise FileNotFoundError(f"The required console supervisor was not installed: {supervisor_target}")
    if callable(progress):
        progress(f"[DEPLOY] Console supervisor verified at {supervisor_target}.")

    manifest_path = manager_dir / "source_manifest.json"
    manifest_tmp = manifest_path.with_suffix(".json.new")
    manifest_tmp.write_text(__import__("json").dumps(manifest, indent=2), encoding="utf-8")
    manifest_tmp.replace(manifest_path)

    installed_launcher = installed_source / "HarvHubAI.vbs"
    if not installed_launcher.is_file():
        raise RuntimeError(f"Installed HarvHub AI launcher is missing: {installed_launcher}")

    # Desktop and Start Menu shortcuts open the installed EXE directly. The
    # explicit HarvHubAI.vbs file remains the separate GitHub update/setup path.
    shortcut_script = r"""
$ErrorActionPreference = 'Stop'
$target = $env:HARVHUB_SHORTCUT_TARGET
$shortcutArgs = $env:HARVHUB_SHORTCUT_ARGS
$work = $env:HARVHUB_SHORTCUT_WORK
$icon = $env:HARVHUB_SHORTCUT_ICON
$desktop = [Environment]::GetFolderPath('Desktop')
$startMenuRoot = [Environment]::GetFolderPath('StartMenu')
$startMenu = Join-Path $startMenuRoot 'Programs'
$folders = @($desktop, $startMenu)
$w = New-Object -ComObject WScript.Shell
foreach ($folder in $folders) {
    if ([string]::IsNullOrWhiteSpace($folder)) { continue }
    New-Item -ItemType Directory -Force -Path $folder | Out-Null
    $shortcutPath = Join-Path $folder 'HarvHub AI.lnk'
    $shortcut = $w.CreateShortcut($shortcutPath)
    $shortcut.TargetPath = $target
    $shortcut.Arguments = $shortcutArgs
    $shortcut.WorkingDirectory = $work
    $shortcut.Description = 'HarvHub AI — open installed application'
    $shortcut.IconLocation = "$icon,0"
    $shortcut.Save()
}
"""
    env = os.environ.copy()
    env["HARVHUB_SHORTCUT_TARGET"] = str(destination)
    env["HARVHUB_SHORTCUT_ARGS"] = ""
    env["HARVHUB_SHORTCUT_WORK"] = str(install_root)
    env["HARVHUB_SHORTCUT_ICON"] = str(destination)
    result = subprocess.run(
        ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", shortcut_script],
        capture_output=True, text=True, timeout=45,
        creationflags=creation_flags(), env=env,
    )
    if result.returncode != 0:
        details = (result.stderr or result.stdout or "Shortcut creation failed.").strip()
        raise RuntimeError(
            f"HarvHub AI and latest Python sources were installed, but a shortcut could not be created: {details}"
        )

    print(
        f"Latest project deployed: {APP_DIR}; changed source files: {copied_count}; already current: {unchanged_count}; launcher: {installed_launcher}",
        flush=True,
    )
    return destination


def install_current_release_and_launch() -> None:
    """Install an already-current release EXE, create shortcuts, then open it."""
    destination = install_current_release_files()
    install_root = destination.parent
    result = ctypes.windll.shell32.ShellExecuteW(
        None, "runas", str(destination), None, str(install_root), 1
    )
    if int(result) <= 32:
        raise OSError(f"Windows could not install/open HarvHub AI (ShellExecute code {int(result)}).")


def main():
    if os.name != "nt":
        raise SystemExit("This builder is intended for Windows.")

    # The VBS launcher is the sole entry point on a fresh VPS. If this .pyw
    # file is started directly, hand control to the sibling VBS so GitHub is
    # downloaded and validated before Python setup/build continues.
    if os.environ.get("HARVHUBAI_BOOTSTRAPPED") != "1":
        launcher = Path(__file__).resolve().parent / "HarvHubAI.vbs"
        if not launcher.is_file():
            root = tk.Tk()
            root.withdraw()
            messagebox.showerror(
                "HarvHub AI bootstrap required",
                "Run HarvHubAI.vbs first. It downloads and validates the latest GitHub project before setup."
            )
            root.destroy()
            return
        try:
            subprocess.Popen(
                ["wscript.exe", str(launcher)],
                cwd=str(launcher.parent),
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
        except Exception as exc:
            root = tk.Tk()
            root.withdraw()
            messagebox.showerror("HarvHub AI", f"Could not start the GitHub-first launcher: {exc}")
            root.destroy()
        return

    args = {str(arg).strip().lower() for arg in sys.argv[1:]}
    install_mode = bool(args.intersection({"--install", "/install"}))
    force_rebuild = bool(args.intersection({"--force-rebuild", "/force-rebuild"}))

    # The VBS launcher deliberately requests --force-rebuild. Every click then
    # builds from the newest selected Downloads project and overwrites the installed
    # EXE plus changed HarvHub AI sources. No stale EXE can short-circuit the update.
    if not (install_mode and force_rebuild) and output_is_current():
        if install_mode:
            if not is_admin():
                if not relaunch_self_as_admin():
                    raise RuntimeError("Administrator permission is required to install HarvHub AI.")
                return
            install_current_release_and_launch()
        else:
            launch_output_exe_elevated()
        return

    # The progress window transitions directly into HarvCoreManager after build
    # and installation. Elevate first so all protected paths can be updated.
    if not is_admin():
        if not relaunch_self_as_admin():
            raise RuntimeError("Administrator permission is required to prepare HarvHub AI. Please approve the Windows permission prompt.")
        return

    root = tk.Tk()
    Builder(root)
    root.mainloop()


def _show_builder_startup_error(error_text: str) -> bool:
    """Show unexpected builder startup failures in a normal copyable window."""
    root = None
    try:
        root = tk.Tk()
        root.title("HarvHub AI — Startup Error")
        root.geometry("1000x680")
        root.minsize(680, 460)
        root.configure(bg=PALETTE["bg"])
        panel = tk.Frame(root, bg=PALETTE["surface"], padx=22, pady=20)
        panel.pack(fill="both", expand=True, padx=18, pady=18)
        tk.Label(panel, text="HarvHub AI setup could not start", bg=PALETTE["surface"],
                 fg=PALETTE["text"], font=("Segoe UI", 18, "bold")).pack(anchor="w")
        tk.Label(panel, text="The complete diagnostic is shown below and can be copied.",
                 bg=PALETTE["surface"], fg=PALETTE["muted"], font=("Segoe UI", 9)).pack(anchor="w", pady=(8, 12))
        console = ScrolledText(panel, wrap="word", bg=PALETTE["console"],
                               fg=PALETTE["console_text"], font=("Consolas", 9),
                               relief="flat", padx=12, pady=10)
        console.insert("end", error_text)
        console.configure(state="disabled")
        console.pack(fill="both", expand=True)
        controls = tk.Frame(panel, bg=PALETTE["surface"])
        controls.pack(fill="x", pady=(12, 0))
        def copy_error():
            try:
                root.clipboard_clear()
                root.clipboard_append(error_text)
                root.update_idletasks()
                copy_button.configure(text="COPIED")
            except tk.TclError:
                copy_button.configure(text="COPY FAILED")
        copy_button = tk.Button(controls, text="COPY ERROR CONSOLE", command=copy_error,
                                bg=PALETTE["gold"], fg="#fff", relief="flat",
                                padx=16, pady=9, font=("Segoe UI", 9, "bold"))
        copy_button.pack(side="left")
        root.mainloop()
        return True
    except Exception:
        try:
            if root is not None:
                root.destroy()
        except Exception:
            pass
        return False


def _launch_last_resort_builder_cmd(error_text: str) -> None:
    """Open CMD only if Tk cannot display the builder's own error window."""
    try:
        import tempfile
        stamp = time.strftime("%Y%m%d-%H%M%S")
        report_path = Path(tempfile.gettempdir()) / f"HarvHubAI-builder-error-{stamp}.txt"
        report_path.write_text(error_text, encoding="utf-8", errors="replace")
        command_file = report_path.with_suffix(".cmd")
        command_file.write_text(
            "@echo off\r\n"
            "title HarvHub AI - Last Resort Builder Error\r\n"
            f'type "{report_path}"\r\n'
            "echo.\r\n"
            "echo HarvHub AI could not display its own error console.\r\n"
            "pause\r\n",
            encoding="utf-8",
        )
        cmd_exe = os.path.join(os.environ.get("WINDIR", r"C:\Windows"), "System32", "cmd.exe")
        subprocess.Popen([cmd_exe, "/k", str(command_file)],
                         creationflags=getattr(subprocess, "CREATE_NEW_CONSOLE", 0))
    except Exception:
        pass


if __name__ == "__main__":
    try:
        main()
    except Exception:
        diagnostic = traceback.format_exc()
        if not _show_builder_startup_error(diagnostic):
            _launch_last_resort_builder_cmd(diagnostic)
