"""Small localhost IPC client used by HarvHub services to notify the desktop manager.

The protocol is a four-byte big-endian length followed by UTF-8 JSON. It is
bound to loopback only; no account credentials are written to disk.
"""
from __future__ import annotations

import json
import os
import socket
import struct
from typing import Any

HOST = os.environ.get("HARVHUB_CONTROL_HOST", "127.0.0.1")
PORT = int(os.environ.get("HARVHUB_CONTROL_PORT", "5601"))
_HEADER = struct.Struct(">I")
_MAX_FRAME = 4 * 1024 * 1024


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise ConnectionError("HarvHub manager closed the IPC connection")
        data.extend(chunk)
    return bytes(data)


def manager_request(payload: dict[str, Any], timeout: float = 2.0) -> dict[str, Any] | None:
    """Send one request to the GUI manager. Failure is non-fatal to the service."""
    try:
        encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        if len(encoded) > _MAX_FRAME:
            raise ValueError("IPC frame exceeds the maximum supported size")
        with socket.create_connection((HOST, PORT), timeout=timeout) as sock:
            sock.settimeout(timeout)
            sock.sendall(_HEADER.pack(len(encoded)) + encoded)
            (length,) = _HEADER.unpack(_recv_exact(sock, _HEADER.size))
            if length <= 0 or length > _MAX_FRAME:
                raise ValueError(f"Invalid IPC response frame length: {length}")
            response = json.loads(_recv_exact(sock, length).decode("utf-8"))
            return response if isinstance(response, dict) else None
    except (OSError, ValueError, ConnectionError, json.JSONDecodeError) as exc:
        # Do not print credentials or payload contents in IPC diagnostics.
        print(f"[HarvHub Manager IPC] unavailable: {type(exc).__name__}: {exc}", flush=True)
        return None
