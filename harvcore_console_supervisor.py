#!/usr/bin/env python3
"""HarvHub AI service console supervisor.

Runs one service as a child process, forwards its complete console output to
stdout (the manager's per-service console), and restarts it after an unexpected
exit whenever execution mode is LOOP. Backoff is capped to avoid rapid crash
loops. A live worker is not killed merely because it logs a transient database
error; the worker's own bridge client is responsible for waiting/retrying safe
reads and its main loop handles uncertain writes without blind replay.
"""
from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

_STOP = False
_CHILD = None


def _log(message: str) -> None:
    print(f'[SUPERVISOR] {message}', flush=True)


def _handle_stop(_signum, _frame):
    global _STOP
    _STOP = True
    child = _CHILD
    if child is not None and child.poll() is None:
        try:
            child.terminate()
        except Exception:
            pass


def _stop_child(child) -> None:
    if child is None or child.poll() is not None:
        return
    try:
        child.terminate()
        child.wait(timeout=5)
    except Exception:
        try:
            child.kill()
        except Exception:
            pass
        try:
            child.wait(timeout=3)
        except Exception:
            pass


def main() -> int:
    global _CHILD, _STOP
    parser = argparse.ArgumentParser(description='Restart and monitor one HarvHub service console')
    parser.add_argument('--target', required=True)
    parser.add_argument('--execution-type', choices=('loop', 'once'), default='loop')
    parser.add_argument('--restart-min-delay', type=float, default=3.0)
    parser.add_argument('--restart-max-delay', type=float, default=30.0)
    parser.add_argument('--max-restarts', type=int, default=0, help='0 means unlimited')
    args, child_args = parser.parse_known_args()
    if child_args and child_args[0] == '--':
        child_args = child_args[1:]

    target = Path(args.target).expanduser().resolve()
    if not target.is_file():
        _log(f'ERROR: target service file does not exist: {target}')
        return 2

    try:
        signal.signal(signal.SIGINT, _handle_stop)
        if hasattr(signal, 'SIGTERM'):
            signal.signal(signal.SIGTERM, _handle_stop)
    except Exception:
        pass

    mode = args.execution_type.lower()
    delay = max(1.0, float(args.restart_min_delay))
    max_delay = max(delay, float(args.restart_max_delay))
    restarts = 0
    _log(f'watching {target.name} | mode={mode.upper()} | automatic restart={"ON" if mode == "loop" else "OFF"}')

    while not _STOP:
        env = os.environ.copy()
        env['PYTHONUNBUFFERED'] = '1'
        env['PYTHONUTF8'] = '1'
        command = [sys.executable, '-u', str(target), *child_args]
        started = time.monotonic()
        try:
            _CHILD = subprocess.Popen(
                command,
                cwd=str(target.parent),
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                text=True,
                encoding='utf-8',
                errors='replace',
                bufsize=1,
            )
            _log(f'STARTED child PID={_CHILD.pid} | {target.name}')
            assert _CHILD.stdout is not None
            for line in iter(_CHILD.stdout.readline, ''):
                if line:
                    sys.stdout.write(line)
                    sys.stdout.flush()
                if _STOP:
                    _stop_child(_CHILD)
                    break
            try:
                _CHILD.stdout.close()
            except Exception:
                pass
            return_code = _CHILD.wait()
        except KeyboardInterrupt:
            _STOP = True
            _stop_child(_CHILD)
            break
        except Exception as exc:
            return_code = 2
            _log(f'CHILD LAUNCH/CONSOLE ERROR: {type(exc).__name__}: {exc}')

        runtime = time.monotonic() - started
        _CHILD = None
        if _STOP:
            break
        if mode != 'loop':
            _log(f'{target.name} finished with exit code {return_code}; once mode will not restart it.')
            return int(return_code or 0)
        restarts += 1
        if args.max_restarts > 0 and restarts > args.max_restarts:
            _log(f'restart limit ({args.max_restarts}) reached; supervisor is stopping.')
            return int(return_code or 1)
        # Reset the backoff after a meaningful period of healthy execution.
        if runtime >= 300:
            delay = max(1.0, float(args.restart_min_delay))
        _log(f'{target.name} exited (code={return_code}, runtime={runtime:.1f}s). Restart {restarts} in {delay:.1f}s.')
        deadline = time.monotonic() + delay
        while not _STOP and time.monotonic() < deadline:
            time.sleep(min(0.5, max(0.05, deadline - time.monotonic())))
        delay = min(max_delay, delay * 2.0)

    _stop_child(_CHILD)
    _log(f'stopped {target.name}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
