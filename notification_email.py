# notification_email.py
"""HarvHub Python notification email adapter.

Uses the same credential source as notification_email.php:
    server_account.id = 1
    server_account.mailer_email    -> verified Brevo sender
    server_account.mailer_password -> Brevo API key

This module deliberately does NOT read server_mailer_settings. The PHP and
Python notification pipelines must use the same server_account row.
"""
from __future__ import annotations

import json
import re
import urllib.error
import urllib.request
from typing import Any

_BREVO_URL = "https://api.brevo.com/v3/smtp/email"
_CREDENTIALS_SQL = (
    "SELECT mailer_email, mailer_password "
    "FROM server_account WHERE id = 1 LIMIT 1"
)


def _row_get(row: Any, key: str, default: Any = None) -> Any:
    if not isinstance(row, dict):
        return default
    wanted = key.strip().lower()
    for name, value in row.items():
        if isinstance(name, str) and name.strip().lower() == wanted:
            return value
    return default


def _query_result(pdo: Any, sql: str) -> dict:
    """Run a read-only query through the notification PDO/bridge adapter.

    The adapters in the programme workers accept query(sql, params), while
    some compatible adapters expose query(sql). Support both without changing
    the database bridge or sending credentials through IPC.
    """
    query = getattr(pdo, "query", None)
    if not callable(query):
        raise TypeError("The supplied notification database adapter has no query() method")
    try:
        result = query(sql)
    except TypeError:
        result = query(sql, None)
    if not isinstance(result, dict):
        raise TypeError(f"Database adapter returned {type(result).__name__}, expected a result dictionary")
    if result.get("status") != "success":
        message = str(result.get("message") or "database query failed")
        raise RuntimeError(message[:1200])
    return result


def _fetch_mailer_credentials(pdo: Any) -> tuple[str, str]:
    """Fetch (sender email, API key) from server_account; never log values."""
    try:
        result = _query_result(pdo, _CREDENTIALS_SQL)
    except Exception as exc:
        print(
            "[HarvHub Notification Email] Could not read server_account mailer credentials "
            f"through the Python database bridge: {type(exc).__name__}: {exc}",
            flush=True,
        )
        return "", ""

    rows = result.get("results") or []
    if not rows:
        print(
            "[HarvHub Notification Email] server_account row id=1 was not returned "
            "by the Python database bridge.",
            flush=True,
        )
        return "", ""

    sender = str(_row_get(rows[0], "mailer_email") or "").strip()
    api_key = str(_row_get(rows[0], "mailer_password") or "").strip()
    if not sender:
        print("[HarvHub Notification Email] server_account.mailer_email is empty.", flush=True)
    if not api_key:
        print("[HarvHub Notification Email] server_account.mailer_password is empty.", flush=True)
    return sender, api_key


def _html_escape(value: str) -> str:
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def _build_html(title: str, message: str) -> str:
    safe_title = _html_escape(title)
    safe_message = _html_escape(message).replace("\n", "<br>")
    return (
        "<!DOCTYPE html><html lang='en'><head><meta charset='UTF-8'>"
        "<meta name='viewport' content='width=device-width, initial-scale=1.0'>"
        f"<title>{safe_title}</title></head>"
        "<body style=\"margin:0;padding:0;background-color:#f4f5f7;"
        "font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;"
        "color:#111827;\">"
        "<table role='presentation' width='100%' cellspacing='0' cellpadding='0' border='0' "
        "style='background-color:#f4f5f7;padding:40px 16px;'><tr><td align='center'>"
        "<table role='presentation' width='100%' cellspacing='0' cellpadding='0' border='0' "
        "style='max-width:620px;background-color:#fff;border-radius:14px;overflow:hidden;'>"
        "<tr><td style='padding:32px 28px 28px;'>"
        f"<p style='font-size:15px;line-height:1.7;margin:0;color:#4b5563;'>{safe_message}</p>"
        "</td></tr></table></td></tr></table></body></html>"
    )


def send_notification_email(
    recipient: str,
    title: str,
    message: str,
    meta: dict | None = None,
) -> bool:
    meta = meta or {}
    recipient = str(recipient or "").strip().lower()
    if not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", recipient):
        print("[HarvHub Notification Email] Invalid or missing recipient email.", flush=True)
        return False

    pdo = meta.get("pdo")
    if pdo is None:
        print("[HarvHub Notification Email] Missing database adapter in email metadata.", flush=True)
        return False

    # Useful when a long-running worker imported a different module copy.
    print(f"[HarvHub Notification Email] Using sender module: {__file__}", flush=True)

    mailer_email, api_key = _fetch_mailer_credentials(pdo)
    if not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", mailer_email):
        print("[HarvHub Notification Email] Brevo sender address is missing or invalid.", flush=True)
        return False
    if not api_key:
        print("[HarvHub Notification Email] Brevo API key is missing from server_account.", flush=True)
        return False

    safe_title = str(title or "").strip() or "Notification"
    safe_message = str(message or "").strip()
    if not safe_message:
        print("[HarvHub Notification Email] Empty message; email was not sent.", flush=True)
        return False

    notification_id = int(meta.get("notification_id") or 0)
    recipient_name = str(meta.get("recipient_name") or "").strip()
    if not recipient_name:
        try:
            name_result = _query_result(
                pdo,
                "SELECT fullname, first_name FROM harvhub "
                "WHERE LOWER(email) = " + _sql_string(recipient) + " "
                "ORDER BY id ASC LIMIT 1",
            )
            name_rows = name_result.get("results") or []
            if name_rows:
                recipient_name = (
                    str(_row_get(name_rows[0], "first_name") or "").strip()
                    or str(_row_get(name_rows[0], "fullname") or "").strip()
                )
        except Exception as exc:
            print(
                "[HarvHub Notification Email] Recipient-name lookup failed: "
                f"{type(exc).__name__}: {exc}",
                flush=True,
            )

    to_entry = {"email": recipient}
    if recipient_name:
        to_entry["name"] = recipient_name

    payload = {
        "sender": {"name": "HarvHub", "email": mailer_email},
        "to": [to_entry],
        "subject": safe_title,
        "htmlContent": _build_html(safe_title, safe_message),
        "textContent": safe_message,
    }
    encoded = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        _BREVO_URL,
        data=encoded,
        method="POST",
        headers={
            "accept": "application/json",
            "api-key": api_key,
            "content-type": "application/json",
        },
    )

    try:
        with urllib.request.urlopen(request, timeout=25) as response:
            status = int(response.getcode())
            response_body = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        try:
            response_body = exc.read().decode("utf-8", errors="replace")
        except Exception:
            response_body = ""
        print(
            "[HarvHub Notification Email] BREVO FAILURE"
            f" | notification_id={notification_id} | recipient={recipient}"
            f" | http_code={exc.code} | response={response_body[:2000]}",
            flush=True,
        )
        return False
    except Exception as exc:
        print(
            "[HarvHub Notification Email] BREVO CONNECTION FAILURE"
            f" | notification_id={notification_id} | recipient={recipient}"
            f" | {type(exc).__name__}: {exc}",
            flush=True,
        )
        return False

    if not 200 <= status < 300:
        print(
            "[HarvHub Notification Email] BREVO FAILURE"
            f" | notification_id={notification_id} | recipient={recipient}"
            f" | http_code={status} | response={response_body[:2000]}",
            flush=True,
        )
        return False

    print(
        "[HarvHub Notification Email] BREVO ACCEPTED EMAIL"
        f" | notification_id={notification_id} | recipient={recipient}"
        f" | http_code={status} | response={response_body[:500]}",
        flush=True,
    )
    return True


def _sql_string(value: str) -> str:
    """Quote a value for the SQL-only browser bridge (not a DB-API driver)."""
    raw = value.encode("utf-8")
    return "CONVERT(X'" + raw.hex() + "' USING utf8mb4)"
