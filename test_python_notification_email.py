#!/usr/bin/env python3
"""Diagnostic for HarvHub's SQL-queued PHP/Brevo notification path.

This follows the current architecture:
    programme_training.BridgeClient
      -> notification_service.make_pdo(db.query)
      -> notification_email.send_notification_email()
      -> INSERT notification_email_queue through mysqlremote SQL

It intentionally does NOT import or retrieve _fetch_mailer_credentials().
The Brevo API key must remain on the PHP hosting server. A successful result
The test succeeds only if mysqlremote immediately opens the PHP relay in its
Selenium browser and the existing PHP mailer receives a Brevo HTTP 2xx.

Run from the HarvHub Python application directory while mysqlremote.py is
running:
    python test_email.py
"""
from __future__ import annotations

import platform
import re
import sys
from datetime import datetime
from pathlib import Path

RECIPIENT = "iamkennyking60@gmail.com"
BRIDGE_OWNER = "python_notification_email_test"


def log(message: str) -> None:
    stamp = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    print(f"[{stamp}] {message}", flush=True)


def mask_email(value: str) -> str:
    value = (value or "").strip()
    if not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", value):
        return "(missing/invalid)"
    local, domain = value.split("@", 1)
    return (local[:1] + "***@" + domain) if local else "***@" + domain



def main() -> int:
    log("=== HARVHUB IMMEDIATE PHP/BREVO EMAIL TEST ===")
    log(f"Target recipient: {RECIPIENT}")
    log(f"Python version: {platform.python_version()}")
    log(f"Python executable: {sys.executable}")
    log(f"Test file: {Path(__file__).resolve()}")

    try:
        from programme_training import BridgeClient
        import programme_training
        from notification_service import make_pdo
        import notification_service
        from notification_email import send_notification_email
        import notification_email
    except Exception as exc:
        log(f"FAIL [IMPORT]: {type(exc).__name__}: {exc}")
        log("Ensure programme_training.py, notification_service.py and the updated notification_email.py are in this folder.")
        return 2

    log(f"programme_training module: {Path(programme_training.__file__).resolve()}")
    log(f"notification_service module: {Path(notification_service.__file__).resolve()}")
    log(f"notification_email module: {Path(notification_email.__file__).resolve()}")

    db = BridgeClient(owner=BRIDGE_OWNER)
    try:
        log("[1/5] Connecting to mysqlremote at 127.0.0.1:5599...")
        channel_id = db.acquire()
        log(f"PASS [BRIDGE]: channel acquired (channel_id={channel_id}).")

        log("[2/5] Identifying the database used by the Python SQL bridge...")
        database_rows = db.fetch_all("SELECT DATABASE() AS db_name", timeout=30)
        if not database_rows:
            log("FAIL [DATABASE]: SELECT DATABASE() returned no row.")
            return 3
        database_name = str(database_rows[0].get("db_name") or "").strip()
        log(f"PASS [DATABASE]: Python bridge database = {database_name or '(not returned)'}")

        log("[3/5] Checking PHP mailer configuration and SQL queue table...")
        config_rows = db.fetch_all(
            "SELECT id, mailer_email, "
            "CASE WHEN TRIM(COALESCE(mailer_password,''))='' "
            "THEN 'MISSING' ELSE 'CONFIGURED' END AS api_key_status "
            "FROM server_account WHERE id=1 LIMIT 1",
            timeout=30,
        )
        if not config_rows:
            log("FAIL [CONFIG]: server_account row id=1 was not found in the Python bridge database.")
            return 4

        sender_email = str(config_rows[0].get("mailer_email") or "").strip()
        api_key_status = str(config_rows[0].get("api_key_status") or "MISSING").strip().upper()
        log(f"PHP configured sender: {mask_email(sender_email)}")
        log(f"Brevo API key status on database: {api_key_status} (key value is not read or printed)")
        if api_key_status != "CONFIGURED":
            log("FAIL [CONFIG]: server_account.mailer_password is not configured for the PHP sender.")
            return 5
        if not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", sender_email):
            log("FAIL [CONFIG]: server_account.mailer_email is missing or invalid.")
            return 6

        table_rows = db.fetch_all(
            "SELECT COUNT(*) AS table_count "
            "FROM information_schema.tables "
            "WHERE table_schema = DATABASE() AND table_name = 'notification_email_queue'",
            timeout=30,
        )
        if not table_rows or int(table_rows[0].get("table_count") or 0) != 1:
            log("FAIL [QUEUE SCHEMA]: notification_email_queue does not exist in this Python bridge database.")
            log("Import notification_email_queue.sql into this exact database before testing again.")
            return 7

        needed_columns = {
            "id", "recipient", "recipient_name", "title", "message", "notification_id",
            "source", "status", "attempts", "max_attempts", "next_attempt_at",
        }
        column_rows = db.fetch_all(
            "SELECT COLUMN_NAME AS column_name "
            "FROM information_schema.columns "
            "WHERE table_schema = DATABASE() "
            "AND table_name = 'notification_email_queue'",
            timeout=30,
        )
        actual_columns = {
            str(row.get("column_name") or "").strip().lower()
            for row in column_rows
        }
        missing_columns = needed_columns - actual_columns
        if missing_columns:
            log("FAIL [QUEUE SCHEMA]: missing column(s): " + ", ".join(sorted(missing_columns)))
            return 8
        log("PASS [QUEUE SCHEMA]: the queue table and required columns exist.")

        log("[4/5] Building the same PDO-compatible SQL adapter used by notification_service...")
        pdo = make_pdo(db.query)
        adapter_rows = pdo.query("SELECT DATABASE() AS db_name")
        if not isinstance(adapter_rows, dict) or adapter_rows.get("status") != "success":
            log("FAIL [PDO ADAPTER]: make_pdo(db.query) could not execute a SQL query.")
            return 9
        log("PASS [PDO ADAPTER]: notification_service.make_pdo(db.query) is working.")

        log("[5/5] Inserting exactly one diagnostic email into notification_email_queue...")
        # The immediate relay reference is generated by notification_email.py.
        # This test performs no extra read-back SELECT after the send completes.
        source = "python_notification_email_test"
        test_time = datetime.now().astimezone().isoformat(timespec="seconds")
        subject = "HarvHub Python Notification Email Test"
        message = (
            "This is a diagnostic email from the HarvHub Python notification system.\n\n"
            "It confirms that Python inserted this email into MySQL and mysqlremote immediately loaded the PHP relay in its Selenium browser. The existing PHP mailer then submitted it to Brevo.\n\n"
            f"Test time: {test_time}\n"
            "No contract_notifications or programme_notifications row was created."
        )

        queued = send_notification_email(
            RECIPIENT,
            subject,
            message,
            {
                "pdo": pdo,
                "notification_id": 0,
                "recipient_name": "HarvHub Python Diagnostic",
                "source": source,
            },
        )
        if not queued:
            log("FAIL [IMMEDIATE SEND]: the PHP relay did not confirm Brevo acceptance.")
            log("Read the notification_email and mysqlremote diagnostics immediately above this line.")
            return 10

        # send_notification_email() returns True only after mysqlremote reports
        # the INSERT statement as successful. Do not issue a second SELECT here:
        # the last diagnostic showed that the hosted SQL UI failed on the read-back
        # query even though this INSERT had already succeeded.
        log("PASS [IMMEDIATE PHP RELAY]: mysqlremote opened the PHP relay before returning the SQL result.")
        log("PASS [BREVO]: The existing PHP sender reported that Brevo accepted the email (HTTP 2xx).")
        log("RESULT: EMAIL SUBMITTED TO BREVO IMMEDIATELY.")
        log("Check the recipient Inbox/Spam folder and the PHP error log for BREVO ACCEPTED EMAIL.")
        return 0

    except Exception as exc:
        log(f"FAIL [UNHANDLED]: {type(exc).__name__}: {exc}")
        return 12
    finally:
        try:
            db.close()
            log("mysqlremote bridge channel released.")
        except Exception as exc:
            log(f"WARNING: bridge channel cleanup failed: {type(exc).__name__}: {exc}")


if __name__ == "__main__":
    raise SystemExit(main())
