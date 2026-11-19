#!/usr/bin/env python3
"""Read-only diagnostic for the exact SQL bridge used by HarvHub Python workers.

It never prints the Brevo API key and never sends an email.
Run from the application Python directory while mysqlremote.py is running:
    python check_python_notification_mailer.py
"""
from __future__ import annotations

import re
import sys

from programme_training import BridgeClient
from notification_service import make_pdo
from notification_email import _fetch_mailer_credentials


def mask_email(value: str) -> str:
    value = (value or "").strip()
    if "@" not in value:
        return "(missing/invalid)"
    local, domain = value.split("@", 1)
    return (local[:1] + "***@" + domain) if local else "***@" + domain


def main() -> int:
    db = BridgeClient(owner="notification_mailer_diagnostic")
    try:
        print("[1/4] Connecting to mysqlremote bridge at 127.0.0.1:5599 ...", flush=True)
        db.acquire()
        print("      OK: bridge channel acquired.", flush=True)

        print("[2/4] Checking the database selected by the Python bridge ...", flush=True)
        db_info = db.fetch_all("SELECT DATABASE() AS db_name", timeout=30)
        if not db_info:
            print("      FAIL: database name query returned no row.", flush=True)
            return 2
        db_name = str(db_info[0].get("db_name") or "")
        print(f"      OK: database selected ({db_name or 'name not returned'}).", flush=True)

        print("[3/4] Checking server_account structure and row id=1 ...", flush=True)
        columns = db.fetch_all(
            "SELECT COLUMN_NAME AS column_name FROM information_schema.columns "
            "WHERE table_schema = DATABASE() AND table_name = 'server_account' "
            "AND column_name IN ('mailer_email','mailer_password')",
            timeout=30,
        )
        present = {str(row.get("column_name") or "").lower() for row in columns}
        missing = {"mailer_email", "mailer_password"} - present
        if missing:
            print("      FAIL: Python bridge database is missing: " + ", ".join(sorted(missing)), flush=True)
            return 3
        rows = db.fetch_all(
            "SELECT id, mailer_email, "
            "CASE WHEN TRIM(COALESCE(mailer_password,''))='' "
            "THEN 'MISSING' ELSE 'CONFIGURED' END AS api_key_status "
            "FROM server_account WHERE id=1 LIMIT 1",
            timeout=30,
        )
        if not rows:
            print("      FAIL: server_account row id=1 does not exist in the Python bridge database.", flush=True)
            return 4
        sender = str(rows[0].get("mailer_email") or "").strip()
        key_status = str(rows[0].get("api_key_status") or "MISSING").upper()
        print(f"      Sender: {mask_email(sender)}", flush=True)
        print(f"      API key: {key_status}", flush=True)

        print("[4/4] Calling the same credential loader used by notification_email.py ...", flush=True)
        pdo = make_pdo(db.query)
        loaded_sender, loaded_key = _fetch_mailer_credentials(pdo)
        print(f"      Sender loaded: {'YES' if loaded_sender else 'NO'}", flush=True)
        print(f"      API key loaded: {'YES' if loaded_key else 'NO'}", flush=True)
        if loaded_sender and loaded_key:
            print("RESULT: Python can read both mailer credentials from server_account.", flush=True)
            print("If notifications still do not arrive, inspect the BREVO FAILURE/CONNECTION logs from the worker.", flush=True)
            return 0
        print("RESULT: Python cannot load the mailer credentials. Compare the database name above with the database used by PHP.", flush=True)
        return 5
    except Exception as exc:
        print(f"RESULT: Python bridge diagnostic failed: {type(exc).__name__}: {exc}", flush=True)
        return 10
    finally:
        try:
            db.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
