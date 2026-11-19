# notification_email.py
"""HarvHub SQL-queued PHP/Brevo notification-email adapter.

Flow:
    1. Insert the notification into MySQL through the existing SQL adapter.
    2. This module opens the signed PHP relay URL itself in headless Chrome.
    3. The PHP endpoint calls the existing notification_email.php mailer.
    4. This function returns True only when PHP confirms Brevo accepted it.

mysqlremote.py is not modified and does not know that email relay exists.
No Python requests/urllib API call is made to the PHP relay.
"""
from __future__ import annotations

import atexit
import hashlib
import hmac
import json
import os
import re
import tempfile
import threading
import time
import uuid
from typing import Any
from urllib.parse import urlencode

# Only this email module uses the new hosting domain.
_RELAY_URL = "https://harvhub.42web.io/notification_email_relay.php"
_RELAY_HMAC_SECRET = "b89f959383065f272cf891766307a2998801ba7a1016a9da0729d5b8f36344c5"
_RELAY_RESULT_MARKER = "HARVHUB_EMAIL_RELAY_RESULT:"
_RELAY_TIMEOUT_SECONDS = 30
_RELAY_MAX_NAVIGATIONS = 2

_QUEUE_TABLE = "notification_email_queue"
_EMAIL_RE = re.compile(r"[^\s@]+@[^\s@]+\.[^\s@]+")
_CHROME_PATH = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
_CHROMEDRIVER_PATH = os.path.expanduser(r"~\.chromedriver_cache\chromedriver.exe")

_DATABASE_NAME_LOGGED = False
_BROWSER_LOCK = threading.RLock()
_RELAY_DRIVER = None
_RELAY_PROFILE_DIR = None


def _query_result(pdo: Any, sql: str) -> dict:
    query = getattr(pdo, "query", None)
    if not callable(query):
        raise TypeError("The supplied notification database adapter has no query() method")
    try:
        result = query(sql)
    except TypeError:
        result = query(sql, None)
    if not isinstance(result, dict):
        raise TypeError(
            f"Database adapter returned {type(result).__name__}, expected a result dictionary"
        )
    if result.get("status") != "success":
        message = str(result.get("message") or "database query failed")
        raise RuntimeError(message[:1500])
    return result


def _sql_string(value: str) -> str:
    raw = str(value).encode("utf-8")
    return "CONVERT(X'" + raw.hex() + "' USING utf8mb4)"


def _sql_nullable_string(value: str | None) -> str:
    if value is None or str(value) == "":
        return "NULL"
    return _sql_string(str(value))


def _row_value(row: Any, wanted_key: str) -> Any:
    if not isinstance(row, dict):
        return None
    wanted = wanted_key.strip().lower()
    for key, value in row.items():
        if isinstance(key, str) and key.strip().lower() == wanted:
            return value
    return None


def _get_database_name(pdo: Any) -> str:
    result = _query_result(pdo, "SELECT DATABASE() AS database_name")
    rows = result.get("results") or []
    if not rows:
        raise RuntimeError("SELECT DATABASE() did not return a database name")
    database_name = str(_row_value(rows[0], "database_name") or "").strip()
    if not database_name:
        raise RuntimeError("The SQL bridge did not identify its active database")
    return database_name


def _log_database_name_once(database_name: str) -> None:
    global _DATABASE_NAME_LOGGED
    if _DATABASE_NAME_LOGGED:
        return
    print(
        "[HarvHub Notification Email] SQL bridge database: " + database_name,
        flush=True,
    )
    _DATABASE_NAME_LOGGED = True


def _build_signed_relay_url(database_name: str, relay_ref: str) -> str:
    timestamp = str(int(time.time()))
    signed_value = f"{timestamp}|{database_name}|{relay_ref}"
    signature = hmac.new(
        _RELAY_HMAC_SECRET.encode("utf-8"),
        signed_value.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    query = urlencode({
        "ts": timestamp,
        "db": database_name,
        "ref": relay_ref,
        "sig": signature,
    })
    return f"{_RELAY_URL}?{query}"


def _create_relay_browser():
    """Create this module's own headless browser; mysqlremote is not involved."""
    global _RELAY_PROFILE_DIR

    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
    except Exception as exc:
        raise RuntimeError(
            "Selenium is unavailable in the Python environment: "
            f"{type(exc).__name__}: {exc}"
        ) from exc

    options = Options()
    if os.path.isfile(_CHROME_PATH):
        options.binary_location = _CHROME_PATH

    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1280,900")
    options.add_argument("--log-level=3")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-proxy-server")
    options.add_argument("--disable-background-timer-throttling")
    options.add_argument("--disable-renderer-backgrounding")
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    options.add_experimental_option("useAutomationExtension", False)

    _RELAY_PROFILE_DIR = tempfile.mkdtemp(prefix="harvhub_email_relay_")
    options.add_argument(f"--user-data-dir={_RELAY_PROFILE_DIR}")

    # Reuse the ChromeDriver cache already populated by the SQL bridge when
    # available. Selenium Manager resolves the driver otherwise.
    if os.path.isfile(_CHROMEDRIVER_PATH):
        service = Service(executable_path=_CHROMEDRIVER_PATH)
        driver = webdriver.Chrome(service=service, options=options)
    else:
        driver = webdriver.Chrome(options=options)

    driver.set_page_load_timeout(_RELAY_TIMEOUT_SECONDS)
    return driver


def _get_relay_browser():
    global _RELAY_DRIVER
    if _RELAY_DRIVER is not None:
        try:
            _ = _RELAY_DRIVER.current_url
            return _RELAY_DRIVER
        except Exception:
            try:
                _RELAY_DRIVER.quit()
            except Exception:
                pass
            _RELAY_DRIVER = None

    _RELAY_DRIVER = _create_relay_browser()
    print(
        "[HarvHub Notification Email] Own headless Chrome relay browser started.",
        flush=True,
    )
    return _RELAY_DRIVER


def _read_relay_result(driver) -> tuple[dict | None, str]:
    try:
        body_text = driver.find_element("tag name", "body").text or ""
    except Exception:
        body_text = ""

    marker_position = body_text.find(_RELAY_RESULT_MARKER)
    if marker_position < 0:
        return None, body_text[:1200]

    encoded = body_text[marker_position + len(_RELAY_RESULT_MARKER):].strip()
    try:
        payload = json.loads(encoded)
    except json.JSONDecodeError as exc:
        return None, f"PHP relay returned an unreadable result: {exc}; body={body_text[:1200]}"

    if not isinstance(payload, dict):
        return None, "PHP relay result was not a JSON object"
    return payload, body_text[:1200]


def _load_php_relay(database_name: str, relay_ref: str) -> dict:
    """Navigate to the signed PHP relay URL in this module's browser."""
    with _BROWSER_LOCK:
        driver = _get_relay_browser()
        relay_url = _build_signed_relay_url(database_name, relay_ref)
        last_detail = "PHP relay did not return its completion marker."

        for attempt in range(1, _RELAY_MAX_NAVIGATIONS + 1):
            try:
                driver.get(relay_url)
            except Exception as navigation_error:
                # A page-load timeout can occur after PHP has already emitted
                # its result. Inspect the page before treating it as a failure.
                last_detail = (
                    f"{type(navigation_error).__name__}: {navigation_error}"
                )

            try:
                from selenium.webdriver.support.ui import WebDriverWait
                WebDriverWait(driver, 8).until(
                    lambda current: _RELAY_RESULT_MARKER in (
                        current.find_element("tag name", "body").text or ""
                    )
                )
            except Exception:
                pass

            payload, body_detail = _read_relay_result(driver)
            if payload is not None:
                payload["browser_url"] = str(getattr(driver, "current_url", "") or "")
                return payload

            current_url = str(getattr(driver, "current_url", "") or "")
            title = str(getattr(driver, "title", "") or "")
            last_detail = (
                f"No relay completion marker. url={current_url!r}; "
                f"title={title!r}; body={body_detail[:700]}"
            )

            # Retry one browser navigation in case the hosting provider first
            # returned its JavaScript/browser verification interstitial.
            if attempt < _RELAY_MAX_NAVIGATIONS:
                time.sleep(1.0)

        raise RuntimeError(last_detail[:1800])


def close_notification_email_browser() -> None:
    """Close the dedicated relay browser cleanly when the worker exits."""
    global _RELAY_DRIVER, _RELAY_PROFILE_DIR
    with _BROWSER_LOCK:
        driver, _RELAY_DRIVER = _RELAY_DRIVER, None
        profile_dir, _RELAY_PROFILE_DIR = _RELAY_PROFILE_DIR, None
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass
        if profile_dir:
            try:
                import shutil
                shutil.rmtree(profile_dir, ignore_errors=True)
            except Exception:
                pass


atexit.register(close_notification_email_browser)


def send_notification_email(
    recipient: str,
    title: str,
    message: str,
    meta: dict | None = None,
) -> bool:
    """Queue through SQL, then load the PHP relay directly from this module."""
    meta = meta if isinstance(meta, dict) else {}
    recipient = str(recipient or "").strip().lower()
    title = str(title or "").strip() or "Notification"
    message = str(message or "").strip()

    if not _EMAIL_RE.fullmatch(recipient):
        print(
            "[HarvHub Notification Email] Invalid or missing recipient email; not queued.",
            flush=True,
        )
        return False
    if not message:
        print("[HarvHub Notification Email] Empty message; not queued.", flush=True)
        return False

    pdo = meta.get("pdo")
    if pdo is None:
        print(
            "[HarvHub Notification Email] Missing SQL bridge adapter in email metadata.",
            flush=True,
        )
        return False

    try:
        notification_id = max(0, int(meta.get("notification_id") or 0))
    except (TypeError, ValueError, OverflowError):
        notification_id = 0

    source = str(meta.get("source") or "python_notification_service").strip()
    relay_ref = uuid.uuid4().hex
    source_tag = f"{source[:82]}|relay:{relay_ref}"
    recipient_name = str(meta.get("recipient_name") or "").strip()

    try:
        database_name = _get_database_name(pdo)
        _log_database_name_once(database_name)
    except Exception as exc:
        print(
            "[HarvHub Notification Email] Could not identify SQL database: "
            f"{type(exc).__name__}: {exc}",
            flush=True,
        )
        return False

    sql = (
        f"INSERT INTO `{_QUEUE_TABLE}` "
        "(`recipient`, `recipient_name`, `title`, `message`, `notification_id`, `source`, "
        "`status`, `attempts`, `max_attempts`, `next_attempt_at`, `created_at`, `updated_at`) "
        "VALUES ("
        f"{_sql_string(recipient)}, "
        f"{_sql_nullable_string(recipient_name)}, "
        f"{_sql_string(title)}, "
        f"{_sql_string(message)}, "
        f"{notification_id}, "
        f"{_sql_string(source_tag)}, "
        "'pending', 0, 3, NOW(), NOW(), NOW()"
        ")"
    )

    try:
        insert_result = _query_result(pdo, sql)
    except Exception as exc:
        print(
            "[HarvHub Notification Email] SQL QUEUE INSERT FAILED "
            f"| notification_id={notification_id} | recipient={recipient} "
            f"| {type(exc).__name__}: {exc}",
            flush=True,
        )
        return False

    print(
        "[HarvHub Notification Email] SQL INSERT CONFIRMED "
        f"| notification_id={notification_id} | recipient={recipient} "
        f"| relay_ref={relay_ref} | opening {_RELAY_URL} directly",
        flush=True,
    )

    try:
        relay = _load_php_relay(database_name, relay_ref)
    except Exception as exc:
        print(
            "[HarvHub Notification Email] PHP RELAY PAGE LOAD FAILED "
            f"| relay_ref={relay_ref} | {type(exc).__name__}: {exc}",
            flush=True,
        )
        return False

    target_status = str(relay.get("target_status") or "").lower()
    if target_status == "sent" and relay.get("success"):
        print(
            "[HarvHub Notification Email] PHP RELAY SENT EMAIL "
            f"| queue_id={relay.get('target_queue_id')} "
            f"| notification_id={notification_id} | recipient={recipient} "
            "| BREVO ACCEPTED EMAIL",
            flush=True,
        )
        return True

    reason = relay.get("message") or (
        f"PHP relay target status={target_status or 'unknown'}"
    )
    print(
        "[HarvHub Notification Email] PHP RELAY DID NOT SEND EMAIL "
        f"| queue_id={relay.get('target_queue_id')} "
        f"| notification_id={notification_id} | recipient={recipient} "
        f"| target_status={target_status or 'unknown'} "
        f"| {str(reason)[:1500]}",
        flush=True,
    )
    return False
