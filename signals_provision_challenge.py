#!/usr/bin/env python3
"""VPS-scoped Signals Provision challenge monitor and programme monetizer.

This worker mirrors the provision rules in signals_provision_request.php:
- only programmes bound to this VPS (direct owner or active follower) are read;
- only an interest whose Begin Test flag is 1 is evaluated as an active challenge;
- only programme_analytics rows with live_trades=1 and trade_configured=1 count;
- restricted dates/weekdays extend the required calendar window without
  reducing the number of eligible collection days;
- a checkpoint passes when minimum trades, expected wins and the consecutive
  loss gate are all satisfied; hitting the max-loss limit fails immediately;
- a failed challenge unpublishes the programme and revokes server approval;
- a completed challenge advertises the programme and grants server approval.

Notifications are sent through notification_service.py, which in turn uses
notification_email.py and the configured Brevo sender/API key. Deterministic
notification keys prevent duplicate emails across repeated monitoring scans.

Run continuously (recommended):
    python signals_provision_challenge.py --interval 30
Run one scan:
    python signals_provision_challenge.py --once
Override VPS detection when needed:
    python signals_provision_challenge.py --vps-ip 203.0.113.10

HARVHUB_VPS_IP may also be set in the environment. This monitor does not create
broker orders and does not change investor/sub-account trades.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import signal
import sys
import time
import traceback
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from programme_training import (
    BridgeClient,
    BridgeError,
    as_float,
    as_int,
    detect_vps_ip,
    get_columns,
    table_exists,
)

try:
    import notification_service as notifier
except Exception as notification_import_error:  # pragma: no cover - installation-specific
    notifier = None
    _NOTIFICATION_IMPORT_ERROR = notification_import_error
else:
    _NOTIFICATION_IMPORT_ERROR = None


BRIDGE_HOST = "127.0.0.1"
BRIDGE_PORT = 5599
BRIDGE_OWNER = "signals_provision_challenge"
DEFAULT_INTERVAL = 10
ANALYTICS_PAGE_SIZE = 500

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("signals_provision_challenge")


# -----------------------------------------------------------------------------
# Generic value/date helpers
# -----------------------------------------------------------------------------
def _clean(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    text = str(value).strip()
    return "" if text.lower() in {"null", "none", "nan"} else text


def _int(value: Any, default: int = 0) -> int:
    parsed = as_int(value, default)
    return int(default if parsed is None else parsed)


def _json_list(raw: Any) -> List[Any]:
    if isinstance(raw, list):
        return raw
    text = _clean(raw)
    if not text:
        return []
    try:
        value = json.loads(text)
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    return value if isinstance(value, list) else []


def _timezone(name: Any) -> ZoneInfo:
    requested = _clean(name) or "UTC"
    try:
        return ZoneInfo(requested)
    except (ZoneInfoNotFoundError, ValueError):
        log.warning("Unknown challenge timezone %r; falling back to UTC", requested)
        return ZoneInfo("UTC")


def _parse_dt(value: Any, tz: ZoneInfo) -> Optional[datetime]:
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = _clean(value)
        if not raw:
            return None
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except (TypeError, ValueError, OverflowError):
            # MySQL DATE/DATETIME values may contain a fractional part or a
            # space separator; fromisoformat handles both, this is a fallback.
            parsed = None
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y-%m-%d %H:%M:%S.%f"):
                try:
                    parsed = datetime.strptime(raw, fmt)
                    break
                except ValueError:
                    continue
            if parsed is None:
                return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=tz)
    return parsed.astimezone(tz)


def _dt_text(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _first_value(row: Dict[str, Any], candidates: Sequence[str]) -> Any:
    for key in candidates:
        value = row.get(key)
        if value is not None and _clean(value) != "":
            return value
    return None


def _normalise_status(value: Any) -> str:
    return " ".join(_clean(value).lower().replace("_", " ").replace("-", " ").split())


def _day_name(value: Any) -> str:
    raw = _clean(value).lower()
    aliases = {
        "0": "sunday", "sun": "sunday", "sunday": "sunday",
        "1": "monday", "mon": "monday", "monday": "monday",
        "2": "tuesday", "tue": "tuesday", "tues": "tuesday", "tuesday": "tuesday",
        "3": "wednesday", "wed": "wednesday", "weds": "wednesday", "wednesday": "wednesday",
        "4": "thursday", "thu": "thursday", "thur": "thursday", "thurs": "thursday", "thursday": "thursday",
        "5": "friday", "fri": "friday", "friday": "friday",
        "6": "saturday", "sat": "saturday", "saturday": "saturday",
    }
    return aliases.get(raw, "")


# -----------------------------------------------------------------------------
# VPS discovery — same association rule used by brokerdetails.py/programme_training.py
# -----------------------------------------------------------------------------
def _in_sql(values: Iterable[int]) -> Tuple[str, List[int]]:
    clean = sorted({int(value) for value in values if int(value) > 0})
    if not clean:
        return "", []
    return ",".join(["%s"] * len(clean)), clean


def discover_vps_programmes(
    db: BridgeClient,
    vps_ip: Optional[str] = None,
    programme_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Return every programme attached to this VPS, without challenge gating."""
    local_ip = _clean(vps_ip or detect_vps_ip())
    if not local_ip:
        log.error("VPS scope unavailable: VPS IP could not be detected; refusing an unscoped monitor run")
        return []
    if not table_exists(db, "programme_vps"):
        log.error("VPS scope unavailable: programme_vps table is missing; refusing an unscoped monitor run")
        return []

    vps_columns = {name.lower() for name in get_columns(db, "programme_vps")}
    required = {"programme_id", "user_id", "programme_vps_ip_address"}
    if not required.issubset(vps_columns):
        raise RuntimeError(
            "programme_vps is missing required column(s): " + ", ".join(sorted(required - vps_columns))
        )

    owner_rows = db.fetch_all(
        "SELECT `programme_id`,`user_id` FROM `programme_vps` "
        "WHERE `programme_vps_ip_address`=%s ORDER BY `id` ASC",
        [local_ip], timeout=90,
    )
    owner_user_by_programme: Dict[int, int] = {}
    for row in owner_rows:
        pid = _int(row.get("programme_id"))
        uid = _int(row.get("user_id"))
        if pid > 0 and uid > 0:
            owner_user_by_programme[pid] = uid

    roles: Dict[int, set[str]] = {pid: {"owner"} for pid in owner_user_by_programme}
    follower_owner: Dict[int, set[int]] = {}
    if owner_user_by_programme and table_exists(db, "programme_vps_hosts_followers"):
        follower_columns = {name.lower() for name in get_columns(db, "programme_vps_hosts_followers")}
        follower_required = {"owner_programme_id", "follower_programme_id", "host_status"}
        if follower_required.issubset(follower_columns):
            marks, params = _in_sql(owner_user_by_programme.keys())
            follower_rows = db.fetch_all(
                "SELECT `owner_programme_id`,`follower_programme_id`,`host_status` "
                "FROM `programme_vps_hosts_followers` "
                f"WHERE `owner_programme_id` IN ({marks}) AND `host_status`='active' "
                "ORDER BY `id` ASC",
                params, timeout=90,
            )
            for row in follower_rows:
                owner_pid = _int(row.get("owner_programme_id"))
                follower_pid = _int(row.get("follower_programme_id"))
                if owner_pid in owner_user_by_programme and follower_pid > 0:
                    roles.setdefault(follower_pid, set()).add("active follower")
                    follower_owner.setdefault(follower_pid, set()).add(owner_pid)

    associated_ids = sorted(roles)
    if not associated_ids:
        log.info("VPS %s has no direct programme or active follower bindings", local_ip)
        return []

    marks, params = _in_sql(associated_ids)
    programme_rows = db.fetch_all(
        f"SELECT * FROM `programme` WHERE `id` IN ({marks}) ORDER BY `id` ASC",
        params, timeout=120,
    )
    latest_interest: Dict[Tuple[int, int], Dict[str, Any]] = {}
    if table_exists(db, "signals_provider_interest"):
        interest_rows = db.fetch_all(
            f"SELECT * FROM `signals_provider_interest` WHERE `programme_id` IN ({marks}) ORDER BY `id` DESC",
            params, timeout=120,
        )
        for row in interest_rows:
            pid = _int(row.get("programme_id"))
            uid = _int(row.get("user_id"))
            if pid > 0 and uid > 0:
                latest_interest.setdefault((pid, uid), row)

    output: List[Dict[str, Any]] = []
    for programme in programme_rows:
        pid = _int(programme.get("id"))
        uid = _int(programme.get("userid"))
        if pid <= 0 or uid <= 0 or pid not in roles:
            continue
        # A direct owner VPS binding must still match the programme owner ID.
        # Followers are independently owned, but must be linked to an active host.
        if pid in owner_user_by_programme and owner_user_by_programme[pid] != uid:
            log.warning(
                "Ignoring mismatched VPS owner binding: programme=%s database_owner=%s vps_owner=%s",
                pid, uid, owner_user_by_programme[pid],
            )
            continue
        interest = latest_interest.get((pid, uid), {})
        programme["vps_associated"] = 1
        programme["vps_ip_address"] = local_ip
        programme["vps_role"] = ", ".join(sorted(roles[pid]))
        programme["begin_test_interest"] = interest
        programme["begin_test_interest_id"] = interest.get("id")
        programme["begin_test_flag"] = _int(interest.get("begin_test"))
        programme["begin_test_status"] = _normalise_status(interest.get("interest_status"))
        programme["begin_test_started_at"] = interest.get("signal_testing_started_at")
        programme["advertisement_flag"] = _int(programme.get("advertisement"))
        output.append(programme)

    if programme_id is not None:
        output = [item for item in output if _int(item.get("id")) == int(programme_id)]
    log.info(
        "VPS scope resolved: IP=%s | owner bindings=%s | programme rows=%s%s",
        local_ip, len(owner_user_by_programme), len(output),
        f" | programme filter={programme_id}" if programme_id else "",
    )
    return output


# -----------------------------------------------------------------------------
# Schema guards and settings
# -----------------------------------------------------------------------------
CHALLENGE_COLUMNS: Dict[str, str] = {
    "daily_progress_json": "LONGTEXT NULL",
    "weekly_progress_json": "LONGTEXT NULL",
    "monthly_progress_json": "LONGTEXT NULL",
    "challenge_restricted_dates_json": "LONGTEXT NULL",
    "challenge_restricted_days_json": "LONGTEXT NULL",
    "challenge_duration_days": "INT UNSIGNED NOT NULL DEFAULT 0",
    "challenge_end_at": "DATETIME NULL",
    "challenge_timezone": "VARCHAR(64) NULL",
    "challenge_total_periods": "INT UNSIGNED NOT NULL DEFAULT 0",
    "challenge_completed_periods": "INT UNSIGNED NOT NULL DEFAULT 0",
    "challenge_current_period_index": "INT NOT NULL DEFAULT -1",
    "challenge_failed_period_index": "INT NULL DEFAULT NULL",
    "challenge_failed_at": "DATETIME NULL",
    "challenge_failure_reason": "TEXT NULL",
    "challenge_completed_at": "DATETIME NULL",
    "challenge_last_evaluated_at": "DATETIME NULL",
}


def ensure_monitor_schema(db: BridgeClient) -> bool:
    if not table_exists(db, "signals_provider_interest"):
        log.error("signals_provider_interest table is missing")
        return False
    cols = {name.lower() for name in get_columns(db, "signals_provider_interest")}
    try:
        for column, definition in CHALLENGE_COLUMNS.items():
            if column not in cols:
                db.execute(
                    f"ALTER TABLE `signals_provider_interest` ADD COLUMN `{column}` {definition}",
                    timeout=90,
                )
                cols.add(column)
                log.info("Added missing challenge column: signals_provider_interest.%s", column)
        if not table_exists(db, "programme_analytics"):
            log.error("programme_analytics table is missing; challenge monitoring cannot count live signals")
            return False
        analytics_cols = {name.lower() for name in get_columns(db, "programme_analytics")}
        if "live_trades" not in analytics_cols:
            db.execute(
                "ALTER TABLE `programme_analytics` ADD COLUMN `live_trades` TINYINT(1) NOT NULL DEFAULT 0",
                timeout=90,
            )
            analytics_cols.add("live_trades")
            log.info("Added missing challenge source column: programme_analytics.live_trades")
        required_analytics = {"programme_id", "userid", "trade_configured", "live_trades"}
        if not required_analytics.issubset(analytics_cols):
            log.error(
                "programme_analytics lacks required challenge columns: %s",
                ", ".join(sorted(required_analytics - analytics_cols)),
            )
            return False
        return True
    except Exception as exc:
        log.error("Challenge schema verification/migration failed: %s", str(exc)[:1200])
        return False


def load_server_operational_settings(db: BridgeClient) -> Dict[str, Any]:
    """Load global challenge durations from the normalized operational settings row."""
    if not table_exists(db, "server_operational_settings"):
        log.error("Required configuration table server_operational_settings is missing")
        return {}
    rows = db.fetch_all(
        "SELECT `day_challenge_duration`, `weekly_challenge_duration`, `monthly_challenge_duration` "
        "FROM `server_operational_settings` WHERE `id`=1 LIMIT 1",
        timeout=30,
    )
    return rows[0] if rows else {}


def duration_days(period: str, operational_settings: Dict[str, Any], interest: Dict[str, Any]) -> int:
    configured = _int(interest.get("challenge_duration_days"))
    if configured > 0:
        return configured
    key = {
        "daily": "day_challenge_duration",
        "weekly": "weekly_challenge_duration",
        "monthly": "monthly_challenge_duration",
    }[period]
    return max(0, _int(operational_settings.get(key)))


# -----------------------------------------------------------------------------
# Restricted calendar and checkpoint construction
# -----------------------------------------------------------------------------
def restricted_calendar(
    db: BridgeClient, user_id: int, programme_id: int, email: str, interest: Dict[str, Any]
) -> Dict[str, List[str]]:
    raw_dates = _clean(interest.get("challenge_restricted_dates_json"))
    raw_days = _clean(interest.get("challenge_restricted_days_json"))
    # An explicit [] is a frozen empty snapshot. Only absent legacy snapshots
    # are re-read from Account Management.
    if raw_dates and raw_days:
        dates = sorted({str(value)[:10] for value in _json_list(raw_dates) if _clean(value)})
        days = sorted({day for value in _json_list(raw_days) if (day := _day_name(value))})
        return {"dates": dates, "days": days}

    developer_ids: List[int] = []
    if email:
        try:
            rows = db.fetch_all(
                "SELECT `id` FROM `developers` WHERE LOWER(`email`)=%s LIMIT 1",
                [email.lower()], timeout=30,
            )
            if rows:
                developer_id = _int(rows[0].get("id"))
                if developer_id > 0:
                    developer_ids.append(developer_id)
        except Exception as exc:
            log.warning("Could not resolve developer ID for restricted calendar: %s", str(exc)[:400])
    if not developer_ids and user_id > 0:
        developer_ids.append(user_id)

    dates: set[str] = set()
    days: set[str] = set()
    marks, params = _in_sql(developer_ids)
    if marks and table_exists(db, "accountmanagement_restricted_dates"):
        cols = {name.lower() for name in get_columns(db, "accountmanagement_restricted_dates")}
        if {"developerid", "programme_id", "restricted_date"}.issubset(cols):
            rows = db.fetch_all(
                "SELECT `restricted_date` FROM `accountmanagement_restricted_dates` "
                f"WHERE `developerid` IN ({marks}) AND `programme_id`=%s",
                params + [programme_id], timeout=60,
            )
            for row in rows:
                value = _clean(row.get("restricted_date"))[:10]
                if len(value) == 10:
                    dates.add(value)
    if marks and table_exists(db, "accountmanagement_restricted_days"):
        cols = {name.lower() for name in get_columns(db, "accountmanagement_restricted_days")}
        if {"developerid", "programme_id", "day"}.issubset(cols):
            rows = db.fetch_all(
                "SELECT `day` FROM `accountmanagement_restricted_days` "
                f"WHERE `developerid` IN ({marks}) AND `programme_id`=%s",
                params + [programme_id], timeout=60,
            )
            for row in rows:
                day = _day_name(row.get("day"))
                if day:
                    days.add(day)
    return {"dates": sorted(dates), "days": sorted(days)}


def period_count(period: str, days: int) -> int:
    if days <= 0:
        return 0
    if period == "weekly":
        return max(1, (days + 6) // 7)
    if period == "monthly":
        return max(1, (days + 29) // 30)
    return days


def period_label(period: str, index: int) -> str:
    prefix = "Week" if period == "weekly" else "Month" if period == "monthly" else "Day"
    return f"{prefix} {index + 1}"


def build_challenge_periods(
    start_at: datetime, period: str, configured_days: int, restricted: Dict[str, List[str]]
) -> List[Dict[str, Any]]:
    """Port of spc_buildChallengePeriods() from signals_provision_request.php."""
    date_set = set(restricted.get("dates") or [])
    day_set = set(restricted.get("days") or [])
    if len(day_set) >= 7:
        return []
    count = period_count(period, configured_days)
    cursor = start_at.replace(hour=0, minute=0, second=0, microsecond=0)
    periods: List[Dict[str, Any]] = []
    for index in range(count):
        if period == "daily":
            target_days = 1
        elif period == "weekly":
            target_days = min(7, max(0, configured_days - index * 7))
        else:
            target_days = min(30, max(0, configured_days - index * 30))
        if target_days <= 0:
            break

        active_dates: List[str] = []
        excluded_details: List[Dict[str, str]] = []
        calendar_days = 0
        while len(active_dates) < target_days and calendar_days < 5000:
            current = cursor + timedelta(days=calendar_days)
            current_date = current.strftime("%Y-%m-%d")
            weekday = current.strftime("%A").lower()
            date_restricted = current_date in date_set
            day_restricted = weekday in day_set
            if date_restricted or day_restricted:
                if date_restricted:
                    reasons = []
                    if date_restricted:
                        reasons.append("restricted date")
                    if day_restricted:
                        reasons.append("restricted day")
                    excluded_details.append({
                        "date": current_date,
                        "weekday": current.strftime("%A"),
                        "reason": " + ".join(reasons),
                        "label": f"{current_date} ({current.strftime('%A')} — {' + '.join(reasons)})",
                    })
            else:
                active_dates.append(current_date)
            calendar_days += 1
        if len(active_dates) != target_days:
            return []

        end_exclusive = cursor + timedelta(days=calendar_days)
        end_date = (end_exclusive - timedelta(days=1)).strftime("%Y-%m-%d")
        periods.append({
            "index": index,
            "label": period_label(period, index),
            "start_date": cursor.strftime("%Y-%m-%d"),
            "end_date": end_date,
            "start_at": cursor,
            "end_at": end_exclusive,
            "calendar_days": calendar_days,
            "eligible_days": len(active_dates),
            "active_dates": active_dates,
            "excluded_dates": excluded_details,
            "excluded_days": [day.title() for day in sorted(day_set)],
            "progress": 0,
            "status": "locked",
            "locked": True,
            "skipped": False,
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "consecutive_losses": 0,
            "trades_passed": False,
            "wins_passed": False,
            "loss_gate_passed": False,
            "requirements_met": False,
        })
        cursor = end_exclusive
    return periods


# -----------------------------------------------------------------------------
# Read live analytics and calculate checkpoint metrics
# -----------------------------------------------------------------------------
def _event_outcome(row: Dict[str, Any]) -> str:
    outcome = _normalise_status(row.get("outcome"))
    if outcome in {"take profit reached", "closed in profit", "closed profit", "profit", "profitable", "win", "won"}:
        return "win"
    if outcome in {"stoploss hit", "closed in loss", "closed loss", "loss", "losing", "lost"}:
        return "loss"
    if outcome in {
        "closed flat", "flat", "breakeven", "break even", "breakeven hit",
        "break even hit", "closed at breakeven", "closed breakeven", "breakeven stop hit",
    }:
        return "flat"
    if outcome in {"closed", "position closed", "manual close", "manually closed"}:
        pnl = as_float(_first_value(row, ("pnl", "profit_loss", "profitandloss")))
        if pnl is not None and pnl > 0:
            return "win"
        if pnl is not None and pnl < 0:
            return "loss"
        if pnl is not None and pnl == 0:
            return "flat"
    return ""


def _event_time_for_row(row: Dict[str, Any], event_type: str, trade_time: datetime, tz: ZoneInfo) -> datetime:
    if event_type == "loss":
        candidates = ("stoploss_trigger_time", "close_time", "closed_at", "event_time", "entry_trigger_time", "entry_anchor_time", "created_at")
    elif event_type == "win":
        candidates = ("take_profit_trigger_time", "close_time", "closed_at", "event_time", "entry_trigger_time", "entry_anchor_time", "created_at")
    elif event_type == "flat":
        candidates = ("breakeven_trigger_time", "close_time", "closed_at", "event_time", "entry_trigger_time", "entry_anchor_time", "created_at")
    else:
        return trade_time
    parsed = _parse_dt(_first_value(row, candidates), tz)
    return parsed or trade_time


def read_live_events(
    db: BridgeClient,
    user_id: int,
    programme_id: int,
    start_at: datetime,
    end_at: datetime,
    tz: ZoneInfo,
) -> Tuple[bool, List[Dict[str, Any]]]:
    """Read only live/configured Analytics rows; return tracking availability."""
    if not table_exists(db, "programme_analytics"):
        return False, []
    cols = {name.lower() for name in get_columns(db, "programme_analytics")}
    required = {"programme_id", "userid", "live_trades", "trade_configured"}
    if not required.issubset(cols):
        log.error("programme_analytics is missing challenge columns: %s", ", ".join(sorted(required - cols)))
        return False, []
    matched_filter = " AND COALESCE(`matched_family`,1)<>0" if "matched_family" in cols else ""
    has_id = "id" in cols
    rows: List[Dict[str, Any]] = []
    if has_id:
        last_id = 0
        while True:
            page = db.fetch_all(
                "SELECT * FROM `programme_analytics` WHERE `programme_id`=%s AND `userid`=%s "
                "AND `live_trades`=1 AND `trade_configured`=1" + matched_filter +
                " AND `id`>%s ORDER BY `id` ASC LIMIT " + str(ANALYTICS_PAGE_SIZE),
                [programme_id, user_id, last_id], timeout=120,
            )
            if not page:
                break
            rows.extend(page)
            page_last_id = max((_int(row.get("id")) for row in page), default=last_id)
            if page_last_id <= last_id or len(page) < ANALYTICS_PAGE_SIZE:
                break
            last_id = page_last_id
    else:
        rows = db.fetch_all(
            "SELECT * FROM `programme_analytics` WHERE `programme_id`=%s AND `userid`=%s "
            "AND `live_trades`=1 AND `trade_configured`=1" + matched_filter,
            [programme_id, user_id], timeout=180,
        )

    events: List[Dict[str, Any]] = []
    for index, row in enumerate(rows):
        if _int(row.get("live_trades")) != 1 or _int(row.get("trade_configured")) != 1:
            continue
        if "matched_family" in row and _int(row.get("matched_family"), 1) == 0:
            continue

        # Define the state before selecting the timestamp candidates. The
        # previous version referenced ``pending`` here without assigning it,
        # which produced the editor warning and raised NameError at runtime.
        outcome_text = re.sub(
            r"[\s_-]+", " ", str(row.get("outcome") or "").strip().lower()
        )
        pending = (
            _int(row.get("pending_entry"), 0) == 1
            or outcome_text == "pending entry"
        )

        # Pending orders use the stable time at which the new signal was first
        # identified. Triggered/closed orders use their actual entry timestamp.
        timestamp_candidates = (
            ("trade_time", "entry_trigger_time", "entry_anchor_time", "event_time", "created_at")
            if pending else
            ("entry_trigger_time", "entry_anchor_time", "event_time", "trade_time", "open_time", "created_at")
        )
        trade_dt = _parse_dt(_first_value(row, timestamp_candidates), tz)
        if trade_dt is None or trade_dt < start_at or trade_dt >= end_at:
            continue
        event_type = _event_outcome(row)
        event_dt = _event_time_for_row(row, event_type, trade_dt, tz)
        events.append({
            "type": event_type,
            "trade_date": trade_dt.strftime("%Y-%m-%d"),
            "trade_timestamp": trade_dt.timestamp(),
            "event_timestamp": event_dt.timestamp(),
            "row_id": _int(row.get("id"), index),
            "index": index,
        })
    return True, events


def compute_period_stats(events: Sequence[Dict[str, Any]], period: Dict[str, Any]) -> Dict[str, int]:
    date_set = set(period.get("active_dates") or [])
    selected = [event for event in events if event.get("trade_date") in date_set]
    outcomes = [event for event in selected if event.get("type") in {"win", "loss", "flat"}]
    outcomes.sort(key=lambda event: (
        event.get("event_timestamp", 0), event.get("row_id", 0), event.get("index", 0)
    ))
    wins = losses = run = max_run = 0
    for event in outcomes:
        kind = event.get("type")
        if kind == "win":
            wins += 1
            run = 0
        elif kind == "loss":
            losses += 1
            run += 1
            max_run = max(max_run, run)
        else:
            run = 0
    return {"trades": len(selected), "wins": wins, "losses": losses, "consecutive_losses": max_run}


def evaluate_period(
    period_row: Dict[str, Any],
    stats: Dict[str, int],
    now: datetime,
    minimum_trades: int,
    minimum_wins: int,
    maximum_losses: int,
) -> Dict[str, Any]:
    period_row.update(stats)
    period_row["trades_passed"] = minimum_trades > 0 and stats["trades"] >= minimum_trades
    period_row["wins_passed"] = minimum_wins > 0 and stats["wins"] >= minimum_wins
    period_row["loss_gate_passed"] = maximum_losses > 0 and stats["consecutive_losses"] < maximum_losses
    period_row["requirements_met"] = bool(
        period_row["trades_passed"] and period_row["wins_passed"] and period_row["loss_gate_passed"]
    )
    closed = now >= period_row["end_at"]
    immediate_loss_failure = maximum_losses > 0 and stats["consecutive_losses"] >= maximum_losses
    if immediate_loss_failure:
        period_row.update({"status": "failed", "progress": 0, "locked": False})
        return {"failed": True, "reason": f"The maximum consecutive-loss limit was reached in {period_row['label']}.", "closed": closed}
    if closed:
        if period_row["requirements_met"]:
            period_row.update({"status": "passed", "progress": 1, "locked": False})
            return {"failed": False, "reason": "", "closed": True}
        period_row.update({"status": "failed", "progress": 0, "locked": False})
        return {
            "failed": True,
            "reason": f"{period_row['label']} ended without meeting the minimum trades and expected-win requirements.",
            "closed": True,
        }
    if period_row["requirements_met"]:
        period_row.update({"status": "requirements_met", "progress": 1, "locked": False})
    else:
        period_row.update({"status": "active", "progress": 0, "locked": False})
    return {"failed": False, "reason": "", "closed": False}


# -----------------------------------------------------------------------------
# Persistence and notifications
# -----------------------------------------------------------------------------
def period_storage(periods: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for item in periods:
        out.append({
            "index": int(item["index"]),
            "label": str(item["label"]),
            "start_date": str(item["start_date"]),
            "end_date": str(item["end_date"]),
            "progress": int(item.get("progress") or 0),
            "status": str(item.get("status") or ""),
            "locked": bool(item.get("locked")),
            "skipped": bool(item.get("skipped")),
            "trades": int(item.get("trades") or 0),
            "wins": int(item.get("wins") or 0),
            "losses": int(item.get("losses") or 0),
            "consecutive_losses": int(item.get("consecutive_losses") or 0),
            "trades_passed": bool(item.get("trades_passed")),
            "wins_passed": bool(item.get("wins_passed")),
            "loss_gate_passed": bool(item.get("loss_gate_passed")),
            "requirements_met": bool(item.get("requirements_met")),
            "eligible_days": int(item.get("eligible_days") or 0),
            "excluded_dates": list(item.get("excluded_dates") or []),
            "excluded_days": list(item.get("excluded_days") or []),
        })
    return out


def save_progress(
    db: BridgeClient,
    interest: Dict[str, Any],
    user_id: int,
    programme_id: int,
    period: str,
    periods: Sequence[Dict[str, Any]],
    completed: int,
    total: int,
    current_index: int,
    now_text: str,
) -> None:
    payload = json.dumps(period_storage(periods), ensure_ascii=False, separators=(",", ":"))
    daily = payload if period == "daily" else "[]"
    weekly = payload if period == "weekly" else "[]"
    monthly = payload if period == "monthly" else "[]"
    db.execute(
        "UPDATE `signals_provider_interest` SET `daily_progress_json`=%s,`weekly_progress_json`=%s,"
        "`monthly_progress_json`=%s,`challenge_total_periods`=%s,`challenge_completed_periods`=%s,"
        "`challenge_current_period_index`=%s,`challenge_last_evaluated_at`=%s,`updated_at`=CURRENT_TIMESTAMP "
        "WHERE `id`=%s AND `user_id`=%s AND `programme_id`=%s",
        [daily, weekly, monthly, total, completed, current_index, now_text,
         _int(interest.get("id")), user_id, programme_id], timeout=90,
    )


def owner_contact(db: BridgeClient, user_id: int) -> Dict[str, str]:
    try:
        rows = db.fetch_all(
            "SELECT `email`,`fullname`,`first_name` FROM `harvhub` WHERE `id`=%s ORDER BY `id` ASC LIMIT 1",
            [user_id], timeout=30,
        )
        row = rows[0] if rows else {}
        return {
            "email": _clean(row.get("email")).lower(),
            "name": _clean(row.get("first_name")) or _clean(row.get("fullname")),
        }
    except Exception as exc:
        log.warning("Owner contact lookup failed for user %s: %s", user_id, str(exc)[:500])
        return {"email": "", "name": ""}


def send_programme_notice(
    db: BridgeClient,
    programme_id: int,
    email: str,
    key: str,
    title: str,
    message: str,
    notification_type: str = "success",
    recipient_name: str = "",
) -> None:
    email = _clean(email).lower()
    if not email or "@" not in email:
        log.warning("Notification skipped for programme %s: owner email is unavailable", programme_id)
        return
    if notifier is None:
        log.error("notification_service.py could not be imported: %s", _NOTIFICATION_IMPORT_ERROR)
        return
    try:
        pdo = notifier.make_pdo(db.query)
        result = notifier.record_programme_notification_once(
            pdo,
            programme_id,
            email,
            {
                "notification_key": key[:190],
                "title": title,
                "message": message,
                "type": notification_type,
                "section": "Signals",
                "action_tab": "signals_provision_request",
                "send_email": True,
                "recipient_name": recipient_name,
            },
        )
        if result:
            log.info("Notification recorded and emailed | programme=%s | key=%s | id=%s", programme_id, key, result)
        else:
            log.info("Notification already recorded or not sent | programme=%s | key=%s", programme_id, key)
    except Exception as exc:
        log.error("Programme notification failed | programme=%s | key=%s | error=%s", programme_id, key, str(exc)[:1000])


def notify_met_periods(
    db: BridgeClient,
    programme: Dict[str, Any],
    interest: Dict[str, Any],
    periods: Sequence[Dict[str, Any]],
    email: str,
    recipient_name: str,
) -> None:
    programme_id = _int(programme.get("id"))
    interest_id = _int(interest.get("id"))
    programme_name = _clean(programme.get("program_name")) or f"Programme {programme_id}"
    period = _clean(interest.get("trades_provision")).lower()
    period_word = {"daily": "daily", "weekly": "weekly", "monthly": "monthly"}.get(period, "daily")
    for item in periods:
        if item.get("skipped") or not item.get("progress"):
            continue
        label = _clean(item.get("label")) or period_label(period_word, _int(item.get("index")))
        key = f"signals-provision-period-met-{programme_id}-{interest_id}-{period_word}-{_int(item.get('index'))}"
        message = (
            f"The {period_word} provision {label} has been met for {programme_name}. "
            f"Recorded results: {int(item.get('trades') or 0)} eligible trades, "
            f"{int(item.get('wins') or 0)} expected wins, and "
            f"{int(item.get('consecutive_losses') or 0)} maximum consecutive losses. "
            "Your provision progress has been updated."
        )
        send_programme_notice(
            db, programme_id, email, key,
            f"{period_word.title()} Signals Provision Met — {label}",
            message, "success", recipient_name,
        )


def notify_failure(
    db: BridgeClient,
    programme: Dict[str, Any],
    interest: Dict[str, Any],
    periods: Sequence[Dict[str, Any]],
    email: str,
    recipient_name: str,
    failure_reason: str,
) -> None:
    programme_id = _int(programme.get("id"))
    interest_id = _int(interest.get("id"))
    failed_index = _int(interest.get("challenge_failed_period_index"), 0)
    failed_period = next((p for p in periods if _int(p.get("index"), -1) == failed_index), {})
    label = _clean(failed_period.get("label")) or period_label(
        _clean(interest.get("trades_provision")).lower() or "daily", failed_index
    )
    programme_name = _clean(programme.get("program_name")) or f"Programme {programme_id}"
    failure_at = _clean(interest.get("challenge_failed_at")) or "legacy-failure"
    fingerprint = hashlib.sha256(
        f"{programme_id}|{interest_id}|{failed_index}|{failure_at}".encode("utf-8")
    ).hexdigest()[:24]
    key = f"signals-provision-failed-{programme_id}-{interest_id}-{fingerprint}"
    message = (
        f"Your { _clean(interest.get('trades_provision')).title() or 'Daily' } signals provision challenge "
        f"for {programme_name} has failed. This is a contract breach. The programme has been removed "
        "from the market, and both advertisement and server advertisement approval have been disabled. "
        f"Failed checkpoint: {label}. Recorded results: {int(failed_period.get('trades') or 0)} eligible trades, "
        f"{int(failed_period.get('wins') or 0)} expected wins, and "
        f"{int(failed_period.get('consecutive_losses') or 0)} consecutive losses. "
        f"Failure reason: {failure_reason or 'The required provision conditions were not satisfied.'} "
        "Review the Signals Provision Request page for the recorded checkpoint results and next steps."
    )
    send_programme_notice(
        db, programme_id, email, key,
        "Signals Provision Challenge Failed", message, "danger", recipient_name,
    )


def notify_challenge_passed(
    db: BridgeClient,
    programme: Dict[str, Any],
    interest: Dict[str, Any],
    email: str,
    recipient_name: str,
) -> None:
    programme_id = _int(programme.get("id"))
    interest_id = _int(interest.get("id"))
    programme_name = _clean(programme.get("program_name")) or f"Programme {programme_id}"
    period = _clean(interest.get("trades_provision")).title() or "Daily"
    key = f"signals-provision-challenge-passed-{programme_id}-{interest_id}"
    message = (
        f"Congratulations. Your {period.lower()} signals provision challenge for {programme_name} "
        "has been completed successfully. All required checkpoints have been met. Your programme "
        "has been advertised and server advertisement approval has been enabled."
    )
    send_programme_notice(
        db, programme_id, email, key,
        "Signals Provision Challenge Passed", message, "success", recipient_name,
    )


def set_programme_monetization(db: BridgeClient, programme_id: int, user_id: int, approved: bool) -> None:
    advertisement = 1 if approved else 0
    approval = 1 if approved else 0
    db.execute(
        "UPDATE `programme` SET `advertisement`=%s,`server_advertisement_approval`=%s "
        "WHERE `id`=%s AND `userid`=%s",
        [advertisement, approval, programme_id, user_id], timeout=60,
    )
    log.warning(
        "PROGRAMME MONETIZATION UPDATED | programme=%s | advertisement=%s | server_advertisement_approval=%s",
        programme_id, advertisement, approval,
    )


def persist_failure(
    db: BridgeClient,
    programme: Dict[str, Any],
    interest: Dict[str, Any],
    periods: Sequence[Dict[str, Any]],
    failed_index: int,
    failure_reason: str,
    now: datetime,
    total: int,
    completed: int,
) -> Dict[str, Any]:
    programme_id = _int(programme.get("id"))
    user_id = _int(programme.get("userid"))
    now_text = _dt_text(now)
    failed_at = _clean(interest.get("challenge_failed_at")) or now_text
    # Mirror the PHP contract distinction: an advertised programme's failure
    # is a breach; an unadvertised programme's challenge failure is failed.
    failure_status = "breached" if _int(programme.get("advertisement")) == 1 else "failed"
    save_progress(
        db, interest, user_id, programme_id,
        _clean(interest.get("trades_provision")).lower() or "daily",
        periods, completed, total, failed_index, now_text,
    )
    db.execute(
        "UPDATE `signals_provider_interest` SET `interest_status`=%s,`begin_test`=0,"
        "`challenge_completed_periods`=%s,`challenge_total_periods`=%s,"
        "`challenge_failed_period_index`=%s,`challenge_failed_at`=%s,"
        "`challenge_failure_reason`=%s,`challenge_current_period_index`=%s,"
        "`challenge_completed_at`=NULL,`challenge_last_evaluated_at`=%s,`updated_at`=CURRENT_TIMESTAMP "
        "WHERE `id`=%s AND `user_id`=%s AND `programme_id`=%s AND `begin_test`=1",
        [failure_status, completed, total, failed_index, failed_at,
         failure_reason, failed_index, now_text, _int(interest.get("id")), user_id, programme_id],
        timeout=90,
    )
    interest.update({
        "interest_status": failure_status,
        "begin_test": 0,
        "challenge_completed_periods": completed,
        "challenge_total_periods": total,
        "challenge_failed_period_index": failed_index,
        "challenge_failed_at": failed_at,
        "challenge_failure_reason": failure_reason,
        "challenge_current_period_index": failed_index,
    })
    set_programme_monetization(db, programme_id, user_id, approved=False)
    programme["advertisement"] = 0
    programme["server_advertisement_approval"] = 0
    return interest


def persist_pass(
    db: BridgeClient,
    programme: Dict[str, Any],
    interest: Dict[str, Any],
    periods: Sequence[Dict[str, Any]],
    now: datetime,
    total: int,
) -> None:
    programme_id = _int(programme.get("id"))
    user_id = _int(programme.get("userid"))
    now_text = _dt_text(now)
    save_progress(
        db, interest, user_id, programme_id,
        _clean(interest.get("trades_provision")).lower() or "daily",
        periods, total, total, max(0, len(periods) - 1), now_text,
    )
    db.execute(
        "UPDATE `signals_provider_interest` SET `interest_status`='passed',`begin_test`=1,"
        "`challenge_completed_periods`=%s,`challenge_total_periods`=%s,"
        "`challenge_current_period_index`=%s,`challenge_completed_at`=%s,"
        "`challenge_last_evaluated_at`=%s,`updated_at`=CURRENT_TIMESTAMP "
        "WHERE `id`=%s AND `user_id`=%s AND `programme_id`=%s "
        "AND `begin_test`=1 AND `interest_status` NOT IN ('failed','breached')",
        [total, total, max(0, len(periods) - 1), now_text, now_text,
         _int(interest.get("id")), user_id, programme_id], timeout=90,
    )
    interest.update({
        "interest_status": "passed",
        "begin_test": 1,
        "challenge_completed_periods": total,
        "challenge_total_periods": total,
        "challenge_completed_at": now_text,
    })
    set_programme_monetization(db, programme_id, user_id, approved=True)
    programme["advertisement"] = 1
    programme["server_advertisement_approval"] = 1


# -----------------------------------------------------------------------------
# Challenge evaluation
# -----------------------------------------------------------------------------
def _saved_terminal_periods(interest: Dict[str, Any], period: str) -> List[Dict[str, Any]]:
    col = "weekly_progress_json" if period == "weekly" else "monthly_progress_json" if period == "monthly" else "daily_progress_json"
    return [item for item in _json_list(interest.get(col)) if isinstance(item, dict)]


def process_programme_challenge(
    db: BridgeClient,
    programme: Dict[str, Any],
    operational_settings: Dict[str, Any],
) -> Dict[str, Any]:
    programme_id = _int(programme.get("id"))
    user_id = _int(programme.get("userid"))
    interest = programme.get("begin_test_interest") or {}
    if not isinstance(interest, dict) or not interest:
        return {"status": "no_interest"}
    begin_test = _int(interest.get("begin_test")) == 1
    current_status = _normalise_status(interest.get("interest_status"))
    already_failed = current_status in {"failed", "breached"}
    already_passed = current_status == "passed"
    period = _clean(interest.get("trades_provision")).lower()
    if period not in {"daily", "weekly", "monthly"}:
        period = "daily"

    contact = owner_contact(db, user_id)
    email, recipient_name = contact["email"], contact["name"]

    # Terminal states are reconciled on every VPS scan. This also repairs a
    # manual/partial change to publication flags without changing frozen results.
    if already_failed:
        set_programme_monetization(db, programme_id, user_id, approved=False)
        saved = _saved_terminal_periods(interest, period)
        notify_failure(
            db, programme, interest, saved, email, recipient_name,
            _clean(interest.get("challenge_failure_reason")),
        )
        return {"status": current_status, "terminal": True}
    if already_passed:
        set_programme_monetization(db, programme_id, user_id, approved=True)
        saved = _saved_terminal_periods(interest, period)
        notify_met_periods(db, programme, interest, saved, email, recipient_name)
        notify_challenge_passed(db, programme, interest, email, recipient_name)
        return {"status": "passed", "terminal": True}

    if not begin_test:
        # A non-terminal interested/pending request is not a challenge run.
        return {"status": "not_started"}
    started_raw = _clean(interest.get("signal_testing_started_at"))
    tz = _timezone(interest.get("challenge_timezone") or programme.get("timezone"))
    start_at = _parse_dt(started_raw, tz)
    if start_at is None:
        log.error("Programme %s interest %s has begin_test=1 but no valid signal_testing_started_at; skipped", programme_id, interest.get("id"))
        return {"status": "invalid_start"}

    configured_days = duration_days(period, operational_settings, interest)
    if configured_days <= 0:
        log.error("Programme %s challenge duration is not configured for %s provision", programme_id, period)
        return {"status": "duration_unconfigured"}

    restricted = restricted_calendar(db, user_id, programme_id, email, interest)
    periods = build_challenge_periods(start_at, period, configured_days, restricted)
    if not periods:
        log.error("Programme %s challenge has no eligible checkpoints; restricted weekdays/dates may block all days", programme_id)
        return {"status": "no_periods"}

    now = datetime.now(tz)
    end_at = periods[-1]["end_at"]
    if _clean(interest.get("challenge_end_at")) != _dt_text(end_at):
        db.execute(
            "UPDATE `signals_provider_interest` SET `challenge_end_at`=%s,`updated_at`=CURRENT_TIMESTAMP "
            "WHERE `id`=%s AND `user_id`=%s AND `programme_id`=%s",
            [_dt_text(end_at), _int(interest.get("id")), user_id, programme_id], timeout=60,
        )
        interest["challenge_end_at"] = _dt_text(end_at)

    tracking_available, events = read_live_events(db, user_id, programme_id, start_at, end_at, tz)
    if not tracking_available:
        # Missing table/columns/query failure is not proof that a provider failed.
        log.error("Programme %s live analytics are unavailable; no challenge state changed", programme_id)
        return {"status": "tracking_unavailable"}

    minimum_trades = max(0, _int(interest.get("trades_count")))
    minimum_wins = max(0, _int(interest.get("expected_win_count")))
    maximum_losses = max(0, _int(interest.get("consecutive_expected_loss")))
    total_required = sum(1 for item in periods if int(item.get("eligible_days") or 0) > 0)
    completed_required = 0
    failed_index: Optional[int] = None
    failure_reason = ""
    current_index = -1

    for item in periods:
        item_start = item["start_at"]
        if now < item_start:
            item["locked"] = True
            item["status"] = "locked"
            continue
        current_index = int(item["index"])
        item["locked"] = False
        stats = compute_period_stats(events, item)
        evaluation = evaluate_period(
            item, stats, now, minimum_trades, minimum_wins, maximum_losses
        )
        if int(item.get("progress") or 0) == 1 and int(item.get("eligible_days") or 0) > 0:
            completed_required += 1
        if evaluation["failed"]:
            failed_index = int(item["index"])
            failure_reason = str(evaluation.get("reason") or "The provision requirements were not met.")
            break

    if failed_index is not None:
        # Future checkpoints stay locked exactly as they had not begun.
        for item in periods:
            if int(item["index"]) > failed_index:
                item.update({"status": "locked", "progress": 0, "locked": True})
        save_progress(
            db, interest, user_id, programme_id, period, periods,
            completed_required, total_required, failed_index, _dt_text(now),
        )
        # Failure status depends on whether the programme was advertised at the
        # moment the contract failed, exactly like the PHP request page.
        persist_failure(
            db, programme, interest, periods, failed_index, failure_reason,
            now, total_required, completed_required,
        )
        notify_met_periods(db, programme, interest, periods, email, recipient_name)
        notify_failure(db, programme, interest, periods, email, recipient_name, failure_reason)
        log.error(
            "CHALLENGE FAILED | programme=%s | period=%s | reason=%s",
            programme_id, periods[failed_index].get("label"), failure_reason,
        )
        return {"status": "failed", "failed_period": failed_index}

    all_done = now >= end_at and completed_required >= total_required
    if all_done:
        for item in periods:
            if item.get("eligible_days", 0) > 0:
                item.update({"progress": 1, "status": "passed", "locked": False})
            else:
                item.update({"progress": 1, "status": "restricted", "locked": False, "skipped": True})
        completed_required = total_required
        persist_pass(db, programme, interest, periods, now, total_required)
        notify_met_periods(db, programme, interest, periods, email, recipient_name)
        notify_challenge_passed(db, programme, interest, email, recipient_name)
        log.info("CHALLENGE PASSED | programme=%s | completed=%s/%s", programme_id, total_required, total_required)
        return {"status": "passed", "completed": completed_required, "total": total_required}

    save_progress(
        db, interest, user_id, programme_id, period, periods,
        completed_required, total_required, max(0, current_index), _dt_text(now),
    )
    db.execute(
        "UPDATE `signals_provider_interest` SET `challenge_completed_periods`=%s,"
        "`challenge_total_periods`=%s,`challenge_current_period_index`=%s,"
        "`challenge_last_evaluated_at`=%s,`updated_at`=CURRENT_TIMESTAMP "
        "WHERE `id`=%s AND `user_id`=%s AND `programme_id`=%s AND `begin_test`=1",
        [completed_required, total_required, max(0, current_index), _dt_text(now),
         _int(interest.get("id")), user_id, programme_id], timeout=90,
    )
    interest["challenge_completed_periods"] = completed_required
    interest["challenge_total_periods"] = total_required
    notify_met_periods(db, programme, interest, periods, email, recipient_name)
    log.info(
        "CHALLENGE ACTIVE | programme=%s | provision=%s | progress=%s/%s | live events=%s",
        programme_id, period, completed_required, total_required, len(events),
    )
    return {"status": "ongoing", "completed": completed_required, "total": total_required}


# -----------------------------------------------------------------------------
# Worker loop
# -----------------------------------------------------------------------------
def run_scan(db: BridgeClient, vps_ip: Optional[str], programme_id: Optional[int]) -> Dict[str, int]:
    programmes = discover_vps_programmes(db, vps_ip, programme_id)
    totals = {"vps_programmes": len(programmes), "checked": 0, "passed": 0, "failed": 0, "ongoing": 0, "skipped": 0}
    if not programmes:
        log.info("No programmes are currently associated with this VPS")
        return totals

    operational_settings = load_server_operational_settings(db)
    for programme in programmes:
        pid = _int(programme.get("id"))
        status = _normalise_status(programme.get("begin_test_status"))
        interest = programme.get("begin_test_interest") or {}
        begin_test = _int(interest.get("begin_test")) == 1 if isinstance(interest, dict) else False
        terminal = status in {"passed", "failed", "breached"}
        # The monitor is not a general programme poller. It only progresses a
        # started challenge or reconciles an already terminal challenge.
        if not begin_test and not terminal:
            totals["skipped"] += 1
            continue
        try:
            result = process_programme_challenge(db, programme, operational_settings)
            totals["checked"] += 1
            state = str(result.get("status") or "unknown")
            if state == "passed":
                totals["passed"] += 1
            elif state in {"failed", "breached"}:
                totals["failed"] += 1
            elif state == "ongoing":
                totals["ongoing"] += 1
            else:
                totals["skipped"] += 1
            log.info("Programme %s monitor result: %s", pid, state)
        except Exception:
            totals["skipped"] += 1
            log.error("Programme %s challenge evaluation failed:\n%s", pid, traceback.format_exc())
    return totals


def main() -> int:
    parser = argparse.ArgumentParser(
        description='VPS-scoped Signals Provision challenge monitor, publisher and server-approval controller'
    )
    parser.add_argument('--execution-type', choices=('loop', 'once'), default='loop', help='manager execution mode')
    parser.add_argument('--once', action='store_true', help='run one VPS scan and exit (legacy alias)')
    parser.add_argument('--interval', type=int, default=DEFAULT_INTERVAL, help=f'seconds between scans (default: {DEFAULT_INTERVAL})')
    parser.add_argument('--programme-id', type=int, default=None, help='limit to a programme only if it is still linked to this VPS')
    parser.add_argument('--vps-ip', default=None, help='override the local VPS IP; must match programme_vps.programme_vps_ip_address')
    parser.add_argument('--bridge-host', default=BRIDGE_HOST)
    parser.add_argument('--bridge-port', type=int, default=BRIDGE_PORT)
    args = parser.parse_args()
    args.interval = min(10, max(5, int(args.interval)))
    run_as_loop = args.execution_type == 'loop' and not args.once

    if notifier is None:
        log.error('notification_service.py import failed; email/in-app notifications will not be sent: %s', _NOTIFICATION_IMPORT_ERROR)
    log.info('Signals Provision Challenge Monitor starting')
    log.info('Execution mode: %s | scan interval: %ss', 'LOOP' if run_as_loop else 'ONCE', args.interval)
    log.info('VPS scope: programme_vps direct owner bindings + active programme_vps_hosts_followers')
    log.info('Challenge source: programme_analytics WHERE live_trades=1 AND trade_configured=1')
    log.info('Success action: interest_status=passed, advertisement=1, server_advertisement_approval=1')
    log.info('Failure action: interest_status=failed/breached, advertisement=0, server_advertisement_approval=0')
    log.info('Bridge recovery: SELECT/SHOW calls wait and retry; ambiguous writes are not automatically replayed')

    db = BridgeClient(args.bridge_host, args.bridge_port, BRIDGE_OWNER)
    stopping = False

    def stop(_signum: int, _frame: Any) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, stop)
    if hasattr(signal, 'SIGTERM'):
        signal.signal(signal.SIGTERM, stop)

    try:
        scan_number = 0
        while not stopping:
            scan_number += 1
            started = time.monotonic()
            log.info('SCAN #%s STARTED | resolving VPS membership before challenge evaluation', scan_number)
            totals = {'vps_programmes': 0, 'checked': 0, 'passed': 0, 'failed': 0, 'ongoing': 0, 'skipped': 0}
            try:
                vps_ip = args.vps_ip or detect_vps_ip()
                if not vps_ip:
                    log.error('No VPS IP detected. Set HARVHUB_VPS_IP or pass --vps-ip; refusing to scan all programmes.')
                else:
                    scoped = discover_vps_programmes(db, vps_ip, args.programme_id)
                    if not scoped:
                        log.info('No programmes are currently assigned to this VPS IP.')
                    elif not ensure_monitor_schema(db):
                        totals['vps_programmes'] = len(scoped)
                        totals['skipped'] = len(scoped)
                    else:
                        totals = run_scan(db, vps_ip, args.programme_id)
            except BridgeError as exc:
                log.error('Database bridge/query unavailable; challenge worker remains active: %s', str(exc)[:1800])
                if not run_as_loop:
                    return 2
                db.close()
                db = BridgeClient(args.bridge_host, args.bridge_port, BRIDGE_OWNER)
                log.warning('Challenge scan deferred; database health will be checked again on the next scan.')
            except Exception:
                log.error('Unexpected challenge scan error; worker remains active:\n%s', traceback.format_exc())
                if not run_as_loop:
                    return 1

            log.info(
                'SCAN COMPLETE | VPS programmes=%s | checked=%s | ongoing=%s | passed=%s | failed=%s | skipped=%s',
                totals['vps_programmes'], totals['checked'], totals['ongoing'],
                totals['passed'], totals['failed'], totals['skipped'],
            )
            if not run_as_loop:
                log.info('One-shot challenge scan finished; exiting')
                break
            sleep_for = args.interval
            log.info('Next challenge scan in %ss', sleep_for)
            for _ in range(sleep_for):
                if stopping:
                    break
                time.sleep(1)
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
