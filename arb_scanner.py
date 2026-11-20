"""
TFEX Real-time 1-Minute Intraday Arbitrage Radar (arb_scanner.py)
================================================================
Scans high-frequency 1-minute intraday snapshots from Turso Cloud SQLite to discover
real executable arbitrage opportunities across:
1. Put-Call Parity (Black-76 Futures Options: Conversion & Reversal)
2. Vertical Spread Monotonicity Violations (Call & Put)
3. Calendar Spread Spikes (Front vs Next Quarter Futures)

Modes:
- Real-time 1-Minute Worker (--session MORNING_WORKER / AFTERNOON_WORKER):
  Runs a continuous 1-minute loop synced with Turso (:06 offset / 3s polling).
  Anti-Spam: Alerts to Telegram LIVE when net profit > 0 is detected (Smart 15-min cooldown per strike/strategy, bypasses on +0.20 pts profit spike).
  At session end (12:35 / 17:05): Dispatches full session summary report to Telegram.
- Single-Shot (--session MIDDAY / EVENING):
  Runs full session scan and immediately outputs summary report.

Strict Fail-Loud / No Silent Fallback Rules:
- Real Turso Data Only: Never generate synthetic/mock quotes.
- Abort on 0 Rows: If Turso has 0 futures or options rows, send Telegram abort alert and exit.
- Strict THOR Rate: Must exist in eod_thor_rates; fail loud if missing/NULL/<=0.
- Strict DTE / T_years: Must calculate from actual series expiry; fail loud if DTE <= 0.
- Strict Bid/Ask: Never use 'last' in place of bid/ask. Drop any row with bid<=0, ask<=0,
  or ask<bid, and report dropped_invalid_quotes count.
- Commission Hurdle:
  - 2-leg strategies (Vertical / Calendar): 0.40 pts (40 THB/leg round-turn = 80 THB).
  - 3-leg strategies (Conversion / Reversal): 0.60 pts (120 THB).
- Multiplier: 200 THB/point.
"""

import os
import sys
import io
import re
import math
import calendar
import argparse
import datetime
import time as pytime
from datetime import date, time, timedelta
from typing import Dict, List, Tuple, Any, Optional

import requests
import pytz
import numpy as np
import pandas as pd
import openpyxl
from dotenv import load_dotenv

try:
    from settrade_v2 import Investor, config as settrade_config
except ImportError:
    Investor = None
    settrade_config = None

load_dotenv()

BANGKOK_TZ = pytz.timezone("Asia/Bangkok")
CONTRACT_MULTIPLIER = 200.0  # THB per SET50 index point

# Fee & Commission Settings (Lock 20.0 THB/side = 40.0 THB round-turn per leg = 0.20 index pts/leg)
FEE_PER_SIDE_THB = float(os.getenv("FEE_PER_SIDE_THB", "20.0"))
COMMISSION_PER_LEG_PTS = (2.0 * FEE_PER_SIDE_THB) / CONTRACT_MULTIPLIER  # 0.20 index pts
COMMISSION_2LEG_PTS = 2.0 * COMMISSION_PER_LEG_PTS   # 0.40 index pts (80 THB)
COMMISSION_3LEG_PTS = 3.0 * COMMISSION_PER_LEG_PTS   # 0.60 index pts (120 THB)
COMMISSION_4LEG_PTS = 4.0 * COMMISSION_PER_LEG_PTS   # 0.80 index pts (160 THB)

# THOR (BOT) publishes rates as PERCENT per annum (e.g. 0.994 == 0.994%).
# Never infer the unit from magnitude: 0.45% would be read as 45% and the
# discount factor exp(-r*T) collapses, fabricating parity edges of several
# index points on deep strikes.
THOR_MAX_PCT = 25.0

# How many of the top-ranked opportunities get re-quoted against Settrade REST
# before a live alert. Rank 1 alone used to be verified while ranks 2+ were
# shipped with DB prices up to 15 minutes old and no label.
MAX_REST_VERIFIED_OPPS = 3

# Quote staleness bound.
# LIVE alerting: 900s (15 นาที) ยาวเกินรอบรีเฟรช quote ของ TFEX options มาก —
# quote ที่หยุดขยับไป 14 นาทียังถูกใช้คำนวณและยิง alert ได้ เป็นต้นตอหลักของ
# opportunity ผี. 120s บังคับให้ quote ที่ใช้ยิง alert ต้องสดจริง
# BACKFILL (สรุปปิดรอบ): คง 900s เพื่อให้ strike ที่เทรดบางยังถูก forward-fill
QUOTE_STALENESS_LIVE_SEC = 120
QUOTE_STALENESS_BACKFILL_SEC = 900

MONTH_CODES = {3: "H", 6: "M", 9: "U", 12: "Z"}
REVERSE_MONTH_CODES = {"H": 3, "M": 6, "U": 9, "Z": 12}

# Official SET Holidays (2026 - 2028)
SET_HOLIDAYS = {
    # 2026
    "2026-01-01", "2026-01-02", "2026-03-03", "2026-04-06", "2026-04-13",
    "2026-04-14", "2026-04-15", "2026-05-01", "2026-05-04", "2026-06-01",
    "2026-06-03", "2026-07-28", "2026-07-29", "2026-08-12", "2026-10-13",
    "2026-10-16", "2026-10-23", "2026-12-07", "2026-12-10", "2026-12-31",
    # 2027
    "2027-01-01", "2027-02-22", "2027-04-06", "2027-04-13", "2027-04-14",
    "2027-04-15", "2027-05-03", "2027-05-04", "2027-05-20", "2027-06-03",
    "2027-07-19", "2027-07-28", "2027-08-12", "2027-10-13", "2027-10-25",
    "2027-12-06", "2027-12-10", "2027-12-31",
    # 2028
    "2028-01-03", "2028-02-10", "2028-04-06", "2028-04-13", "2028-04-14",
    "2028-05-01", "2028-05-05", "2028-06-05", "2028-07-17", "2028-07-28",
    "2028-08-14", "2028-10-13", "2028-10-23", "2028-12-05", "2028-12-11"
}


# ==============================================================================
# 1. TURSO HTTP PIPELINE CLIENT
# ==============================================================================

def to_turso_arg(val: Any) -> Dict[str, Any]:
    if val is None:
        return {"type": "null"}
    if isinstance(val, bool):
        return {"type": "integer", "value": str(int(val))}
    if isinstance(val, int):
        return {"type": "integer", "value": str(val)}
    if isinstance(val, float):
        if math.isnan(val) or math.isinf(val):
            return {"type": "null"}
        return {"type": "float", "value": val}
    return {"type": "text", "value": str(val)}


def parse_turso_result_rows(res: Dict[str, Any]) -> List[List[Any]]:
    rows = []
    for raw_row in res.get("rows", []):
        row_vals = []
        for cell in raw_row:
            v_type = cell.get("type")
            if v_type == "null":
                row_vals.append(None)
            elif v_type == "integer":
                row_vals.append(int(cell.get("value", 0)))
            elif v_type == "float":
                row_vals.append(float(cell.get("value", 0.0)))
            else:
                row_vals.append(cell.get("value"))
        rows.append(row_vals)
    return rows


def execute_turso_sql(
    pipeline_url: str,
    auth_token: str,
    sql: str,
    args: Optional[List[Any]] = None,
    max_attempts: int = 3
) -> Tuple[List[str], List[List[Any]]]:
    """
    Executes SQL via Turso v2 pipeline.

    Transient failures (network errors, HTTP 429/5xx) retry with exponential
    backoff, honouring Retry-After up to 30s — a 5xx from Turso used to kill the
    whole scan tick / the worker at startup. SQL errors (syntax, constraint)
    are NOT retried: they are deterministic and would just burn the timeout.
    """
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json"
    }
    stmt = {"sql": sql.strip()}
    if args:
        stmt["args"] = [to_turso_arg(a) for a in args]
    payload = {
        "requests": [
            {"type": "execute", "stmt": stmt},
            {"type": "close"}
        ]
    }

    last_exc: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            r = requests.post(pipeline_url, json=payload, headers=headers, timeout=25)
        except requests.RequestException as exc:
            last_exc = exc
            delay = min(2.0 ** (attempt - 1), 8.0)
        else:
            if r.status_code == 429 or r.status_code >= 500:
                last_exc = RuntimeError(f"Turso API HTTP error ({r.status_code}): {r.text[:200]}")
                retry_after = (r.headers.get("Retry-After") or "").strip()
                delay = min(float(retry_after), 30.0) if retry_after.replace(".", "", 1).isdigit() \
                    else min(2.0 ** (attempt - 1), 8.0)
            elif r.status_code != 200:
                raise RuntimeError(f"Turso API HTTP error ({r.status_code}): {r.text[:200]}")
            else:
                data = r.json()
                for resp_item in data.get("results", []):
                    if resp_item.get("type") == "error":
                        # deterministic — retrying cannot help
                        raise RuntimeError(f"Turso SQL execution error: {resp_item.get('error')}")
                    if resp_item.get("type") == "ok":
                        res = resp_item["response"]["result"]
                        cols = [c["name"] for c in res.get("cols", [])]
                        rows = parse_turso_result_rows(res)
                        return cols, rows
                return [], []

        if attempt >= max_attempts:
            break
        print(f"[TURSO] {type(last_exc).__name__} on attempt {attempt}/{max_attempts}: "
              f"{str(last_exc)[:120]} — retry in {delay:.0f}s")
        pytime.sleep(delay)

    if last_exc is not None:
        raise last_exc
    raise RuntimeError("Turso request failed without a captured error")


# ==============================================================================
# 2. CONCURRENCY GUARD & HEARTBEAT
# ==============================================================================

def ensure_arb_heartbeat_table(pipeline_url: str, auth_token: str) -> None:
    sql = """
    CREATE TABLE IF NOT EXISTS arb_worker_heartbeat (
        worker_session TEXT PRIMARY KEY,
        last_heartbeat_bkk TEXT,
        pid INTEGER
    );
    """
    execute_turso_sql(pipeline_url, auth_token, sql)


def is_arb_worker_active(
    pipeline_url: str,
    auth_token: str,
    session_name: str,
    now_bkk: datetime.datetime,
    max_age_seconds: int = 180
) -> bool:
    """
    Checks if another worker is running for the same session.
    Returns True if heartbeat was updated within max_age_seconds (default 3 min).
    """
    try:
        ensure_arb_heartbeat_table(pipeline_url, auth_token)
        sql = "SELECT last_heartbeat_bkk FROM arb_worker_heartbeat WHERE worker_session = ?;"
        _, rows = execute_turso_sql(pipeline_url, auth_token, sql, [session_name])
        if rows and rows[0][0]:
            last_ts_str = str(rows[0][0])
            last_dt = BANGKOK_TZ.localize(datetime.datetime.strptime(last_ts_str, "%Y-%m-%d %H:%M:%S"))
            diff_sec = (now_bkk - last_dt).total_seconds()
            if 0 <= diff_sec < max_age_seconds:
                print(f"[CONCURRENCY] Found active worker heartbeat at {last_ts_str} ({diff_sec:.0f}s ago < {max_age_seconds}s limit).")
                return True
    except Exception as e:
        print(f"[CONCURRENCY] Note checking arb heartbeat: {e}")
    return False


def record_arb_heartbeat(
    pipeline_url: str,
    auth_token: str,
    session_name: str,
    now_bkk: datetime.datetime
) -> None:
    try:
        ensure_arb_heartbeat_table(pipeline_url, auth_token)
        sql = """
        INSERT INTO arb_worker_heartbeat (worker_session, last_heartbeat_bkk, pid)
        VALUES (?, ?, ?)
        ON CONFLICT(worker_session) DO UPDATE SET
            last_heartbeat_bkk = excluded.last_heartbeat_bkk,
            pid = excluded.pid;
        """
        now_str = now_bkk.strftime("%Y-%m-%d %H:%M:%S")
        execute_turso_sql(pipeline_url, auth_token, sql, [session_name, now_str, os.getpid()])
    except Exception as e:
        print(f"[CONCURRENCY] Note recording arb heartbeat: {e}")


# ==============================================================================
# 3. DYNAMIC SERIES & EXPIRATION CALCULATION
# ==============================================================================

def get_penultimate_business_day(year: int, month: int, holidays: set = SET_HOLIDAYS) -> date:
    """
    Calculate the penultimate business day of a given month.
    (TFEX contract specification for SET50 Futures & Options expiration).
    """
    if not any(str(h).startswith(str(year)) for h in holidays):
        print(f"[CALENDAR] WARNING: SET_HOLIDAYS has no data for {year} — expiry date for "
              f"{year}-{month:02d} may be wrong. Update SET_HOLIDAYS before trading that year.")

    _, last_day = calendar.monthrange(year, month)
    biz_days = [
        date(year, month, d)
        for d in range(1, last_day + 1)
        if date(year, month, d).weekday() < 5 and date(year, month, d).strftime("%Y-%m-%d") not in holidays
    ]
    if len(biz_days) < 2:
        return biz_days[-1] if biz_days else date(year, month, last_day)
    return biz_days[-2]


def get_contract_last_trading_day(year: int, month: int) -> datetime.datetime:
    ltd = get_penultimate_business_day(year, month, SET_HOLIDAYS)
    return BANGKOK_TZ.localize(datetime.datetime(ltd.year, ltd.month, ltd.day, 16, 30, 0))


def resolve_active_series_candidates(now_bkk: datetime.datetime) -> Tuple[List[str], str, str]:
    curr_year = now_bkk.year
    candidates = []
    for y in [curr_year, curr_year + 1]:
        for m in [3, 6, 9, 12]:
            ltd = get_contract_last_trading_day(y, m)
            if ltd > now_bkk:
                candidates.append((ltd, f"{MONTH_CODES[m]}{str(y)[-2:]}"))

    candidates.sort(key=lambda x: x[0])
    series_list = [c[1] for c in candidates[:4]]
    front_series = series_list[0] if series_list else "Z26"
    next_series = series_list[1] if len(series_list) > 1 else "H27"
    return series_list, front_series, next_series


def parse_series_expiry(series_code: str, ref_date: date) -> datetime.datetime:
    month_char = series_code[0].upper()
    month = REVERSE_MONTH_CODES.get(month_char)
    if not month:
        raise ValueError(f"Invalid series month character: '{month_char}'")
    year_prefix = ref_date.year // 100
    year_suffix = int(series_code[1:])
    year = year_prefix * 100 + year_suffix
    return get_contract_last_trading_day(year, month)


# ==============================================================================
# 4. TURSO INTRADAY DATASET LOADER (STRICT VALIDATION)
# ==============================================================================

def _to_decimal_rate(val: Optional[float], label: str = "") -> Optional[float]:
    """
    Converts a BOT THOR reading into a decimal annual rate.

    THOR is ALWAYS published as percent per annum, so divide by 100
    unconditionally. The previous heuristic (`val / 100 if val > 0.5 else val`)
    silently turned 0.45% into 45%: exp(-r*T) then collapsed from 0.9996 to
    0.9637 at 30 DTE, which fabricates ~1.8 index points of parity edge
    (0.60 points of commission, so the alert fires) out of thin air.
    """
    if val is None:
        return None
    pct = float(val)
    if pct <= 0:
        return None
    if pct > THOR_MAX_PCT:
        raise ValueError(
            f"THOR rate out of range: {pct} ({label}) — expected percent p.a., "
            f"max {THOR_MAX_PCT}%"
        )
    return pct / 100.0


def select_dynamic_thor_rate(
    thor_record: Dict[str, Optional[float]],
    dte: int
) -> Tuple[float, str]:
    """
    Selects the dynamic THOR rate matching the option's DTE (Days to Expiration).
    
    Tenor Selection Rules:
    - dte <= 30 days:
        Prefer 1M Compounded ('1m')
        Fallback: Overnight ('overnight')
    - 30 < dte <= 90 days:
        Prefer 3M Compounded ('3m')
        Fallback: 1M Compounded ('1m') -> Overnight ('overnight')
    - 90 < dte <= 180 days:
        Prefer 6M Compounded ('6m')
        Fallback: 3M Compounded ('3m') -> 1M Compounded -> Overnight
    - dte > 180 days:
        Prefer 6M Compounded ('6m')
        Fallback: 3M Compounded -> Overnight
        
    Returns:
        (rate_decimal, tenor_label) where rate_decimal is in decimal form (e.g. 0.00995)
    """
    r_on = _to_decimal_rate(thor_record.get("overnight"), "overnight")
    r_1m = _to_decimal_rate(thor_record.get("1m"), "1m")
    r_3m = _to_decimal_rate(thor_record.get("3m"), "3m")
    r_6m = _to_decimal_rate(thor_record.get("6m"), "6m")

    if dte <= 30:
        candidates = [("1M", r_1m), ("ON", r_on), ("3M", r_3m), ("6M", r_6m)]
    elif dte <= 90:
        candidates = [("3M", r_3m), ("1M", r_1m), ("ON", r_on), ("6M", r_6m)]
    elif dte <= 180:
        candidates = [("6M", r_6m), ("3M", r_3m), ("1M", r_1m), ("ON", r_on)]
    else:
        candidates = [("6M", r_6m), ("3M", r_3m), ("ON", r_on), ("1M", r_1m)]

    for label, rate in candidates:
        if rate is not None and rate > 0:
            return rate, label

    raise ValueError(f"No valid THOR rate found in record: {thor_record}")


def fetch_thor_rates_record(pipeline_url: str, auth_token: str) -> Dict[str, Optional[float]]:
    sql_thor = "SELECT thor_rate, thor_1m_avg, thor_3m_avg, thor_6m_avg FROM eod_thor_rates ORDER BY trade_date DESC LIMIT 1;"
    _, rows_thor = execute_turso_sql(pipeline_url, auth_token, sql_thor)
    if not rows_thor or rows_thor[0][0] is None:
        raise ValueError("No record found in eod_thor_rates table or value is NULL.")
    
    row = rows_thor[0]
    return {
        "overnight": float(row[0]) if len(row) > 0 and row[0] is not None else None,
        "1m": float(row[1]) if len(row) > 1 and row[1] is not None else None,
        "3m": float(row[2]) if len(row) > 2 and row[2] is not None else None,
        "6m": float(row[3]) if len(row) > 3 and row[3] is not None else None,
    }


def fetch_thor_rate(pipeline_url: str, auth_token: str, dte: Optional[int] = None) -> float:
    thor_rec = fetch_thor_rates_record(pipeline_url, auth_token)
    if dte is not None:
        rate, _ = select_dynamic_thor_rate(thor_rec, dte)
        return rate
    rate, _ = select_dynamic_thor_rate(thor_rec, 42)
    return rate


def fetch_intraday_arb_dataset(
    pipeline_url: str,
    auth_token: str,
    trade_date: str,
    start_time_bkk: str,
    end_time_bkk: str
) -> Tuple[Dict[str, Dict[str, Any]], int, int]:
    ts_start = f"{trade_date} {start_time_bkk}"
    ts_end = f"{trade_date} {end_time_bkk}"

    sql_spot = """
    SELECT captured_at_bkk, last, volume, value 
    FROM spot_snapshots 
    WHERE captured_at_bkk >= ? AND captured_at_bkk <= ? 
    ORDER BY captured_at_bkk ASC;
    """
    _, rows_spot = execute_turso_sql(pipeline_url, auth_token, sql_spot, [ts_start, ts_end])

    sql_fut_depth = """
    SELECT captured_at_bkk, symbol, bid, ask, last, volume, open_interest, bid_vol, ask_vol
    FROM futures_snapshots 
    WHERE captured_at_bkk >= ? AND captured_at_bkk <= ? 
    ORDER BY captured_at_bkk ASC;
    """
    try:
        _, rows_fut = execute_turso_sql(pipeline_url, auth_token, sql_fut_depth, [ts_start, ts_end])
        has_fut_depth = True
    except Exception:
        sql_fut_fallback = """
        SELECT captured_at_bkk, symbol, bid, ask, last, volume, open_interest 
        FROM futures_snapshots 
        WHERE captured_at_bkk >= ? AND captured_at_bkk <= ? 
        ORDER BY captured_at_bkk ASC;
        """
        _, rows_fut = execute_turso_sql(pipeline_url, auth_token, sql_fut_fallback, [ts_start, ts_end])
        has_fut_depth = False

    sql_opt_depth = """
    SELECT captured_at_bkk, symbol, series, strike, option_type, bid, ask, last, volume, open_interest, bid_vol, ask_vol
    FROM options_snapshots 
    WHERE captured_at_bkk >= ? AND captured_at_bkk <= ? 
    ORDER BY captured_at_bkk ASC;
    """
    try:
        _, rows_opt = execute_turso_sql(pipeline_url, auth_token, sql_opt_depth, [ts_start, ts_end])
        has_opt_depth = True
    except Exception:
        sql_opt_fallback = """
        SELECT captured_at_bkk, symbol, series, strike, option_type, bid, ask, last, volume, open_interest 
        FROM options_snapshots 
        WHERE captured_at_bkk >= ? AND captured_at_bkk <= ? 
        ORDER BY captured_at_bkk ASC;
        """
        _, rows_opt = execute_turso_sql(pipeline_url, auth_token, sql_opt_fallback, [ts_start, ts_end])
        has_opt_depth = False

    # AS-OF Continuous Forward-Fill Engine
    # 1. Bucket raw updates chronologically into 1-minute time windows
    spot_by_minute: Dict[str, List[Any]] = {}
    for r in rows_spot:
        ts = str(r[0])
        m_key = ts[:16] + ":00" if len(ts) >= 16 else ts
        spot_by_minute.setdefault(m_key, []).append(r)

    fut_by_minute: Dict[str, List[Any]] = {}
    for r in rows_fut:
        ts = str(r[0])
        m_key = ts[:16] + ":00" if len(ts) >= 16 else ts
        fut_by_minute.setdefault(m_key, []).append(r)

    opt_by_minute: Dict[str, List[Any]] = {}
    for r in rows_opt:
        ts = str(r[0])
        m_key = ts[:16] + ":00" if len(ts) >= 16 else ts
        opt_by_minute.setdefault(m_key, []).append(r)

    all_minutes = sorted(list(set(spot_by_minute.keys()) | set(fut_by_minute.keys()) | set(opt_by_minute.keys())))

    running_spot: Optional[float] = None
    running_futures: Dict[str, Dict[str, Any]] = {}
    running_options: Dict[Tuple[str, float, str], Dict[str, Any]] = {}

    grouped: Dict[str, Dict[str, Any]] = {}

    for m_ts in all_minutes:
        # Update spot
        if m_ts in spot_by_minute:
            for r in spot_by_minute[m_ts]:
                if r[1] is not None:
                    running_spot = float(r[1])

        # Update futures (forward-fill unchanged contracts)
        if m_ts in fut_by_minute:
            for r in fut_by_minute[m_ts]:
                sym = str(r[1])
                bid, ask, last, vol, oi = r[2], r[3], r[4], r[5], r[6]
                bid_vol = float(r[7]) if (has_fut_depth and len(r) > 7 and r[7] is not None) else 0.0
                ask_vol = float(r[8]) if (has_fut_depth and len(r) > 8 and r[8] is not None) else 0.0
                running_futures[sym] = {
                    "symbol": sym,
                    "bid": float(bid) if bid is not None else None,
                    "ask": float(ask) if ask is not None else None,
                    "bid_vol": bid_vol,
                    "ask_vol": ask_vol,
                    "last": float(last) if last is not None else None,
                    "volume": float(vol) if vol is not None else 0.0,
                    "oi": float(oi) if oi is not None else 0.0,
                    "last_update_ts": str(r[0])
                }

        # Update options (forward-fill unchanged contracts)
        if m_ts in opt_by_minute:
            for r in opt_by_minute[m_ts]:
                sym, series, strike, otype = r[1], r[2], r[3], r[4]
                bid, ask, last, vol, oi = r[5], r[6], r[7], r[8], r[9]
                bid_vol = float(r[10]) if (has_opt_depth and len(r) > 10 and r[10] is not None) else 0.0
                ask_vol = float(r[11]) if (has_opt_depth and len(r) > 11 and r[11] is not None) else 0.0
                key = (str(series), float(strike), str(otype).upper())
                running_options[key] = {
                    "symbol": str(sym),
                    "series": str(series),
                    "strike": float(strike),
                    "option_type": str(otype).upper(),
                    "bid": float(bid) if bid is not None else None,
                    "ask": float(ask) if ask is not None else None,
                    "bid_vol": bid_vol,
                    "ask_vol": ask_vol,
                    "last": float(last) if last is not None else None,
                    "volume": float(vol) if vol is not None else 0.0,
                    "oi": float(oi) if oi is not None else 0.0,
                    "last_update_ts": str(r[0])
                }

        # Strict Staleness Guard: Prune any quote older than 15 minutes (900 seconds)
        try:
            m_dt = datetime.datetime.strptime(m_ts[:19], "%Y-%m-%d %H:%M:%S")
            for sym in list(running_futures.keys()):
                u_ts = running_futures[sym].get("last_update_ts")
                if u_ts:
                    try:
                        u_dt = datetime.datetime.strptime(u_ts[:19], "%Y-%m-%d %H:%M:%S")
                        if (m_dt - u_dt).total_seconds() > QUOTE_STALENESS_BACKFILL_SEC:
                            del running_futures[sym]
                    except Exception:
                        pass

            for key in list(running_options.keys()):
                u_ts = running_options[key].get("last_update_ts")
                if u_ts:
                    try:
                        u_dt = datetime.datetime.strptime(u_ts[:19], "%Y-%m-%d %H:%M:%S")
                        if (m_dt - u_dt).total_seconds() > QUOTE_STALENESS_BACKFILL_SEC:
                            del running_options[key]
                    except Exception:
                        pass
        except Exception:
            pass

        # Snapshot current AS-OF continuous state for this minute
        grouped[m_ts] = {
            "spot": running_spot,
            "futures": {k: dict(v) for k, v in running_futures.items()},
            "options": {k: dict(v) for k, v in running_options.items()}
        }

    return grouped, len(rows_fut), len(rows_opt)


# ==============================================================================
# 5. THREE INTRADAY ARBITRAGE SCAN MODULES (STRICT BLACK-76 VALIDATION)
# ==============================================================================

def _exec_sets(*depths: Any) -> int:
    """
    จำนวนชุดที่เคาะได้จริง = min(depth ของทุกขา) ปัดลง

    S1: เดิมใช้ int(min(...)) ตรง ๆ ทำให้ depth ที่ไม่ใช่จำนวนเต็มกลายเป็น 0 ชุด
    แต่ opp ยังถูกส่งออกไปพร้อมข้อความ 'Max: 0 ชุด' ซึ่งเป็น alert ที่สั่งให้
    เคาะออเดอร์ที่เคาะไม่ได้ ผู้เรียกต้องข้าม opp ที่ได้ 0
    """
    vals = []
    for d in depths:
        try:
            vals.append(float(d))
        except (TypeError, ValueError):
            return 0
    if not vals or any(v <= 0 for v in vals):
        return 0
    return int(min(vals))


def scan_put_call_parity(
    grouped_data: Dict[str, Dict[str, Any]],
    front_series: str,
    thor_rate: float,
    T_years: float
) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]], int, int, Dict[str, float]]:
    COMMISSION = COMMISSION_3LEG_PTS
    fut_sym = f"S50{front_series}"
    disc_factor = math.exp(-thor_rate * T_years)

    opportunities: List[Dict[str, Any]] = []
    best_near_miss: Optional[Dict[str, Any]] = None
    dropped_invalid_quotes = 0
    calc_error_count = 0
    minute_bests: Dict[str, float] = {}

    for ts in sorted(grouped_data.keys()):
        frame = grouped_data[ts]
        spot_ref = frame.get("spot")
        fut = frame["futures"].get(fut_sym)
        if not fut:
            continue

        f_bid = fut.get("bid")
        f_ask = fut.get("ask")
        f_vol = fut.get("volume", 0.0)

        if (f_bid is None or f_ask is None or f_bid <= 0 or f_ask <= 0 or
            f_ask < f_bid):
            dropped_invalid_quotes += 1
            continue

        ref_price = spot_ref or ((f_bid + f_ask) / 2.0)
        f_mid = (f_bid + f_ask) / 2.0

        strikes = set()
        for (ser, k, otype) in frame["options"].keys():
            if ser == front_series:
                strikes.add(k)

        for k in sorted(strikes):
            c_quote = frame["options"].get((front_series, k, "C"))
            p_quote = frame["options"].get((front_series, k, "P"))
            if not c_quote or not p_quote:
                continue

            c_bid, c_ask = c_quote.get("bid"), c_quote.get("ask")
            p_bid, p_ask = p_quote.get("bid"), p_quote.get("ask")
            c_vol, p_vol = c_quote.get("volume", 0.0), p_quote.get("volume", 0.0)

            if (c_bid is None or c_ask is None or c_bid <= 0 or c_ask <= 0 or
                c_ask < c_bid):
                dropped_invalid_quotes += 1
                continue
            if (p_bid is None or p_ask is None or p_bid <= 0 or p_ask <= 0 or
                p_ask < p_bid):
                dropped_invalid_quotes += 1
                continue

            c_mid = (c_bid + c_ask) / 2.0
            p_mid = (p_bid + p_ask) / 2.0
            c_bvol = c_quote.get("bid_vol", 0.0) or 0.0
            c_avol = c_quote.get("ask_vol", 0.0) or 0.0
            p_bvol = p_quote.get("bid_vol", 0.0) or 0.0
            p_avol = p_quote.get("ask_vol", 0.0) or 0.0
            f_bvol = fut.get("bid_vol", 0.0) or 0.0
            f_avol = fut.get("ask_vol", 0.0) or 0.0

            liq_tag = "[Liquid: Spot±30]" if abs(k - ref_price) <= 30.0 else "[Deep Strike: ถือจนหมดอายุ]"

            # 1. Conversion: (Call_Bid - Put_Ask) - (Fut_Ask - K) * exp(-r * T)
            # Requires strict depth: Sell Call @ Bid, Buy Put @ Ask, Buy Fut @ Ask
            if c_bvol > 0 and p_avol > 0 and f_avol > 0:
                max_sets_conv = _exec_sets(c_bvol, p_avol, f_avol)
                if max_sets_conv < 1:
                    continue
                try:
                    disc_diff_mid = (f_mid - k) * disc_factor
                    mid_edge_conv = (c_mid - p_mid) - disc_diff_mid

                    disc_diff_conv = (f_ask - k) * disc_factor
                    gross_conv = (c_bid - p_ask) - disc_diff_conv
                    if math.isnan(gross_conv) or math.isinf(gross_conv):
                        calc_error_count += 1
                        gross_conv = None
                except Exception:
                    calc_error_count += 1
                    gross_conv = None

                if gross_conv is not None:
                    net_conv = gross_conv - COMMISSION
                    spread_cost_conv = mid_edge_conv - gross_conv
                    c_sym = c_quote.get("symbol") or f"S50{front_series}C{k:.0f}"
                    p_sym = p_quote.get("symbol") or f"S50{front_series}P{k:.0f}"
                    item_conv = {
                        "strategy": "Conversion (Sell C, Buy P, Buy Fut)",
                        "target": f"Strike {k:.0f} & {fut_sym}",
                        "tag": liq_tag,
                        "time": ts,
                        "series": front_series,
                        "strike": k,
                        "gross_pts": gross_conv,
                        "net_pts": net_conv,
                        "mid_edge": mid_edge_conv,
                        "spread_cost": spread_cost_conv,
                        "commission_pts": COMMISSION,
                        "max_sets": max_sets_conv,
                        "disc_factor": disc_factor,
                        "legs": [
                            f"Sell Call {k:.0f} @ Bid <code>{c_bid:.2f}</code> (Depth: {c_bvol:,.0f})",
                            f"Buy Put {k:.0f} @ Ask <code>{p_ask:.2f}</code> (Depth: {p_avol:,.0f})",
                            f"Buy Futures {fut_sym} @ Ask <code>{f_ask:.2f}</code> (Depth: {f_avol:,.0f})"
                        ],
                        "order_legs": [
                            {"symbol": c_sym, "side": "S", "price": c_bid, "depth": c_bvol, "is_futures": False},
                            {"symbol": p_sym, "side": "L", "price": p_ask, "depth": p_avol, "is_futures": False},
                            {"symbol": fut_sym, "side": "L", "price": f_ask, "depth": f_avol, "is_futures": True}
                        ]
                    }
                    if ts not in minute_bests or net_conv > minute_bests[ts]:
                        minute_bests[ts] = net_conv

                    if net_conv > 0:
                        opportunities.append(item_conv)
                    if best_near_miss is None or net_conv > best_near_miss["net_pts"]:
                        best_near_miss = item_conv

            # 2. Reversal: (Fut_Bid - K) * exp(-r * T) - (Call_Ask - Put_Bid)
            # Requires strict depth: Buy Call @ Ask, Sell Put @ Bid, Sell Fut @ Bid
            if c_avol > 0 and p_bvol > 0 and f_bvol > 0:
                max_sets_rev = _exec_sets(c_avol, p_bvol, f_bvol)
                if max_sets_rev < 1:
                    continue
                try:
                    disc_diff_mid = (f_mid - k) * disc_factor
                    mid_edge_rev = disc_diff_mid - (c_mid - p_mid)

                    disc_diff_rev = (f_bid - k) * disc_factor
                    gross_rev = disc_diff_rev - (c_ask - p_bid)
                    if math.isnan(gross_rev) or math.isinf(gross_rev):
                        calc_error_count += 1
                        gross_rev = None
                except Exception:
                    calc_error_count += 1
                    gross_rev = None

                if gross_rev is not None:
                    net_rev = gross_rev - COMMISSION
                    spread_cost_rev = mid_edge_rev - gross_rev
                    item_rev = {
                        "strategy": "Reversal (Buy C, Sell P, Sell Fut)",
                        "target": f"Strike {k:.0f} & {fut_sym}",
                        "tag": liq_tag,
                        "time": ts,
                        "series": front_series,
                        "strike": k,
                        "gross_pts": gross_rev,
                        "net_pts": net_rev,
                        "mid_edge": mid_edge_rev,
                        "spread_cost": spread_cost_rev,
                        "commission_pts": COMMISSION,
                        "max_sets": max_sets_rev,
                        "disc_factor": disc_factor,
                        "legs": [
                            f"Buy Call {k:.0f} @ Ask <code>{c_ask:.2f}</code> (Depth: {c_avol:,.0f})",
                            f"Sell Put {k:.0f} @ Bid <code>{p_bid:.2f}</code> (Depth: {p_bvol:,.0f})",
                            f"Sell Futures {fut_sym} @ Bid <code>{f_bid:.2f}</code> (Depth: {f_bvol:,.0f})"
                        ],
                        "order_legs": [
                            {"symbol": c_sym, "side": "L", "price": c_ask, "depth": c_avol, "is_futures": False},
                            {"symbol": p_sym, "side": "S", "price": p_bid, "depth": p_bvol, "is_futures": False},
                            {"symbol": fut_sym, "side": "S", "price": f_bid, "depth": f_bvol, "is_futures": True}
                        ]
                    }
                    if ts not in minute_bests or net_rev > minute_bests[ts]:
                        minute_bests[ts] = net_rev

                    if net_rev > 0:
                        opportunities.append(item_rev)
                    if best_near_miss is None or net_rev > best_near_miss["net_pts"]:
                        best_near_miss = item_rev

    return opportunities, best_near_miss, dropped_invalid_quotes, calc_error_count, minute_bests


def scan_vertical_spreads(
    grouped_data: Dict[str, Dict[str, Any]],
    front_series: str
) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]], int, int, Dict[str, float]]:
    COMMISSION = COMMISSION_2LEG_PTS
    opportunities: List[Dict[str, Any]] = []
    best_near_miss: Optional[Dict[str, Any]] = None
    dropped_invalid_quotes = 0
    calc_error_count = 0
    minute_bests: Dict[str, float] = {}

    for ts in sorted(grouped_data.keys()):
        frame = grouped_data[ts]
        options = frame["options"]

        # Call Monotonicity
        calls = []
        for ((ser, k, otype), opt) in options.items():
            if ser == front_series and otype == "C":
                bid, ask, vol = opt.get("bid"), opt.get("ask"), opt.get("volume", 0.0)
                if (bid is None or ask is None or bid <= 0 or ask <= 0 or
                    ask < bid):
                    dropped_invalid_quotes += 1
                else:
                    calls.append(opt)

        calls.sort(key=lambda x: x["strike"])

        for i in range(len(calls)):
            for j in range(i + 1, len(calls)):
                c1, c2 = calls[i], calls[j]
                k1, k2 = c1["strike"], c2["strike"]
                c1_mid = (c1["bid"] + c1["ask"]) / 2.0
                c2_mid = (c2["bid"] + c2["ask"]) / 2.0
                mid_edge = c2_mid - c1_mid

                gross = c2["bid"] - c1["ask"]
                if math.isnan(gross) or math.isinf(gross):
                    calc_error_count += 1
                    continue
                net = gross - COMMISSION
                spread_cost = mid_edge - gross
                v_c1_ask = c1.get("ask_vol", 0.0) or 0.0
                v_c2_bid = c2.get("bid_vol", 0.0) or 0.0
                if v_c1_ask <= 0 or v_c2_bid <= 0:
                    continue
                max_sets = _exec_sets(v_c1_ask, v_c2_bid)
                if max_sets < 1:
                    continue

                c1_sym = c1.get("symbol") or f"S50{front_series}C{k1:.0f}"
                c2_sym = c2.get("symbol") or f"S50{front_series}C{k2:.0f}"
                item = {
                    "strategy": "Call Monotonicity Violation",
                    "target": f"Call {k1:.0f}/{k2:.0f} ({front_series})",
                    "tag": "[Vertical Spread]",
                    "time": ts,
                    "series": front_series,
                    "strike_pair": f"{k1:.0f}/{k2:.0f}",
                    "gross_pts": gross,
                    "net_pts": net,
                    "mid_edge": mid_edge,
                    "spread_cost": spread_cost,
                    "commission_pts": COMMISSION,
                    "max_sets": max_sets,
                    "legs": [
                        f"Buy Call {k1:.0f} @ Ask <code>{c1['ask']:.2f}</code> (Depth: {v_c1_ask:,.0f})",
                        f"Sell Call {k2:.0f} @ Bid <code>{c2['bid']:.2f}</code> (Depth: {v_c2_bid:,.0f})"
                    ],
                    "order_legs": [
                        {"symbol": c1_sym, "side": "L", "price": c1["ask"], "depth": v_c1_ask, "is_futures": False},
                        {"symbol": c2_sym, "side": "S", "price": c2["bid"], "depth": v_c2_bid, "is_futures": False}
                    ]
                }
                if ts not in minute_bests or net > minute_bests[ts]:
                    minute_bests[ts] = net

                if net > 0:
                    opportunities.append(item)
                if best_near_miss is None or net > best_near_miss["net_pts"]:
                    best_near_miss = item

        # Put Monotonicity
        puts = []
        for ((ser, k, otype), opt) in options.items():
            if ser == front_series and otype == "P":
                bid, ask, vol = opt.get("bid"), opt.get("ask"), opt.get("volume", 0.0)
                if (bid is None or ask is None or bid <= 0 or ask <= 0 or
                    ask < bid):
                    dropped_invalid_quotes += 1
                else:
                    puts.append(opt)

        puts.sort(key=lambda x: x["strike"])

        for i in range(len(puts)):
            for j in range(i + 1, len(puts)):
                p1, p2 = puts[i], puts[j]
                k1, k2 = p1["strike"], p2["strike"]
                p1_mid = (p1["bid"] + p1["ask"]) / 2.0
                p2_mid = (p2["bid"] + p2["ask"]) / 2.0
                mid_edge = p1_mid - p2_mid

                gross = p1["bid"] - p2["ask"]
                if math.isnan(gross) or math.isinf(gross):
                    calc_error_count += 1
                    continue
                net = gross - COMMISSION
                spread_cost = mid_edge - gross
                v_p2_ask = p2.get("ask_vol", 0.0) or 0.0
                v_p1_bid = p1.get("bid_vol", 0.0) or 0.0
                if v_p2_ask <= 0 or v_p1_bid <= 0:
                    continue
                max_sets = _exec_sets(v_p2_ask, v_p1_bid)
                if max_sets < 1:
                    continue

                p1_sym = p1.get("symbol") or f"S50{front_series}P{k1:.0f}"
                p2_sym = p2.get("symbol") or f"S50{front_series}P{k2:.0f}"
                item = {
                    "strategy": "Put Monotonicity Violation",
                    "target": f"Put {k1:.0f}/{k2:.0f} ({front_series})",
                    "tag": "[Vertical Spread]",
                    "time": ts,
                    "series": front_series,
                    "strike_pair": f"{k1:.0f}/{k2:.0f}",
                    "gross_pts": gross,
                    "net_pts": net,
                    "mid_edge": mid_edge,
                    "spread_cost": spread_cost,
                    "commission_pts": COMMISSION,
                    "max_sets": max_sets,
                    "legs": [
                        f"Buy Put {k2:.0f} @ Ask <code>{p2['ask']:.2f}</code> (Depth: {v_p2_ask:,.0f})",
                        f"Sell Put {k1:.0f} @ Bid <code>{p1['bid']:.2f}</code> (Depth: {v_p1_bid:,.0f})"
                    ],
                    "order_legs": [
                        {"symbol": p2_sym, "side": "L", "price": p2["ask"], "depth": v_p2_ask, "is_futures": False},
                        {"symbol": p1_sym, "side": "S", "price": p1["bid"], "depth": v_p1_bid, "is_futures": False}
                    ]
                }
                if ts not in minute_bests or net > minute_bests[ts]:
                    minute_bests[ts] = net

                if net > 0:
                    opportunities.append(item)
                if best_near_miss is None or net > best_near_miss["net_pts"]:
                    best_near_miss = item

    return opportunities, best_near_miss, dropped_invalid_quotes, calc_error_count, minute_bests


def scan_calendar_spreads(
    grouped_data: Dict[str, Dict[str, Any]],
    front_series: str,
    next_series: str
) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]], int, int, Dict[str, float]]:
    COMMISSION = COMMISSION_2LEG_PTS
    f1_sym = f"S50{front_series}"
    f2_sym = f"S50{next_series}"
    dropped_invalid_quotes = 0
    calc_error_count = 0
    minute_bests: Dict[str, float] = {}

    minute_spreads: Dict[str, Dict[str, Any]] = {}
    for ts in sorted(grouped_data.keys()):
        frame = grouped_data[ts]
        f1 = frame["futures"].get(f1_sym)
        f2 = frame["futures"].get(f2_sym)
        if not f1 or not f2:
            continue

        b1, a1 = f1.get("bid"), f1.get("ask")
        b2, a2 = f2.get("bid"), f2.get("ask")
        b1_vol = f1.get("bid_vol", 0.0) or 0.0
        a1_vol = f1.get("ask_vol", 0.0) or 0.0
        b2_vol = f2.get("bid_vol", 0.0) or 0.0
        a2_vol = f2.get("ask_vol", 0.0) or 0.0

        if (b1 is None or a1 is None or b1 <= 0 or a1 <= 0 or a1 < b1 or
            b2 is None or a2 is None or b2 <= 0 or a2 <= 0 or a2 < b2):
            dropped_invalid_quotes += 1
            continue

        mid1 = (b1 + a1) / 2.0
        mid2 = (b2 + a2) / 2.0
        mid_spread = mid1 - mid2

        # 1-minute bar bucket key to resample irregular CDC ticks into continuous bars
        m_key = ts[:16] + ":00" if len(ts) >= 16 else (ts[:5] + ":00" if len(ts) >= 5 else ts)
        minute_spreads[m_key] = {
            "ts": ts,
            "b1": b1, "a1": a1, "b1_vol": b1_vol, "a1_vol": a1_vol,
            "b2": b2, "a2": a2, "b2_vol": b2_vol, "a2_vol": a2_vol,
            "mid_spread": mid_spread
        }

    spread_records = [minute_spreads[k] for k in sorted(minute_spreads.keys())]

    if len(spread_records) < 5:
        return [], None, dropped_invalid_quotes, calc_error_count, {}

    df_sp = pd.DataFrame(spread_records)
    # W4: baseline ต้องไม่รวมแท่งที่กำลังทดสอบ (shift(1)) ไม่งั้น spike จะกลบตัวเองใน mu
    # และ z-score ถูก cap: ที่ min_periods=5 ค่า z สูงสุดที่เป็นไปได้คือ 1.789 < 2.0
    # ทำให้ detector ยิงไม่ได้เลยในช่วงต้นรอบ
    baseline_spread = df_sp["mid_spread"].shift(1)
    df_sp["rolling_mean"] = baseline_spread.rolling(window=30, min_periods=5).mean()
    df_sp["rolling_std"] = baseline_spread.rolling(window=30, min_periods=5).std()

    opportunities: List[Dict[str, Any]] = []
    best_near_miss: Optional[Dict[str, Any]] = None

    for _, row in df_sp.iterrows():
        ts = row["ts"]
        b1, a1 = row["b1"], row["a1"]
        b2, a2 = row["b2"], row["a2"]
        b1_vol, a1_vol = row["b1_vol"], row["a1_vol"]
        b2_vol, a2_vol = row["b2_vol"], row["a2_vol"]
        mu = row["rolling_mean"]
        std = row["rolling_std"]
        if pd.isna(mu) or pd.isna(std) or std <= 0.0001:
            continue

        # Case 1: Spread Spikes UP -> Sell Front @ b1, Buy Next @ a2
        # W3: เดิม `continue` ทำให้ Case 2 ไม่ถูกประเมินเลยเมื่อ bid ฝั่ง front ไม่มี depth
        if b1_vol > 0 and a2_vol > 0:
            max_sets_up = _exec_sets(b1_vol, a2_vol)

            mid1 = (b1 + a1) / 2.0
            mid2 = (b2 + a2) / 2.0
            mid_spread = mid1 - mid2
            mid_edge_up = mid_spread - mu

            exec_sell_spread = b1 - a2
            dev_up = exec_sell_spread - mu
            if math.isnan(dev_up) or math.isinf(dev_up):
                calc_error_count += 1
                continue
            net_up = dev_up - COMMISSION
            spread_cost_up = mid_edge_up - dev_up

            item_up = {
                "strategy": "Calendar Spread Spike (Overextended High)",
                "target": f"{f1_sym} - {f2_sym}",
                "tag": "[Statistical Spread]",
                "time": ts,
                "series": f"{front_series}-{next_series}",
                "gross_pts": dev_up,
                "net_pts": net_up,
                "mid_edge": mid_edge_up,
                "spread_cost": spread_cost_up,
                "commission_pts": COMMISSION,
                "max_sets": max_sets_up,
                "mu": mu,
                "std": std,
                "legs": [
                    f"Sell Front {f1_sym} @ Bid <code>{b1:.2f}</code> (Depth: {b1_vol:,.0f})",
                    f"Buy Next {f2_sym} @ Ask <code>{a2:.2f}</code> (Depth: {a2_vol:,.0f})"
                ],
                "order_legs": [
                    {"symbol": f1_sym, "side": "S", "price": b1, "depth": b1_vol, "is_futures": True},
                    {"symbol": f2_sym, "side": "L", "price": a2, "depth": a2_vol, "is_futures": True}
                ],
                "stats": f"Spread: {exec_sell_spread:+.2f} vs Baseline: {mu:+.2f} (+{dev_up:.2f} pts / +{(dev_up/std):.1f}σ)"
            }
            if ts not in minute_bests or net_up > minute_bests[ts]:
                minute_bests[ts] = net_up

            if max_sets_up >= 1 and exec_sell_spread > (mu + 2.0 * std) and net_up > 0:
                opportunities.append(item_up)
            if best_near_miss is None or net_up > best_near_miss["net_pts"]:
                best_near_miss = item_up

        # Case 2: Spread Spikes DOWN -> Buy Front @ a1, Sell Next @ b2
        if a1_vol > 0 and b2_vol > 0:
            max_sets_down = _exec_sets(a1_vol, b2_vol)

            exec_buy_spread = a1 - b2
            dev_down = mu - exec_buy_spread
            mid_edge_down = mu - mid_spread
            if math.isnan(dev_down) or math.isinf(dev_down):
                calc_error_count += 1
                continue
            net_down = dev_down - COMMISSION
            spread_cost_down = mid_edge_down - dev_down

            item_down = {
                "strategy": "Calendar Spread Spike (Overextended Low)",
                "target": f"{f1_sym} - {f2_sym}",
                "tag": "[Statistical Spread]",
                "time": ts,
                "series": f"{front_series}-{next_series}",
                "gross_pts": dev_down,
                "net_pts": net_down,
                "mid_edge": mid_edge_down,
                "spread_cost": spread_cost_down,
                "commission_pts": COMMISSION,
                "max_sets": max_sets_down,
                "mu": mu,
                "std": std,
                "legs": [
                    f"Buy Front {f1_sym} @ Ask <code>{a1:.2f}</code> (Depth: {a1_vol:,.0f})",
                    f"Sell Next {f2_sym} @ Bid <code>{b2:.2f}</code> (Depth: {b2_vol:,.0f})"
                ],
                "order_legs": [
                    {"symbol": f1_sym, "side": "L", "price": a1, "depth": a1_vol, "is_futures": True},
                    {"symbol": f2_sym, "side": "S", "price": b2, "depth": b2_vol, "is_futures": True}
                ],
                "stats": f"Spread: {exec_buy_spread:+.2f} vs Baseline: {mu:+.2f} (+{dev_down:.2f} pts / +{(dev_down/std):.1f}σ)"
            }
            if ts not in minute_bests or net_down > minute_bests[ts]:
                minute_bests[ts] = net_down

            if max_sets_down >= 1 and exec_buy_spread < (mu - 2.0 * std) and net_down > 0:
                opportunities.append(item_down)
            if best_near_miss is None or net_down > best_near_miss["net_pts"]:
                best_near_miss = item_down

    return opportunities, best_near_miss, dropped_invalid_quotes, calc_error_count, minute_bests


# ==============================================================================
# 6. TELEGRAM HTML FORMATTER & ALERT DISPATCHER
# ==============================================================================

def escape_telegram_html(text: str) -> str:
    pattern = r"</?(?:b|i|code|pre|a)(?:\s+[^>]+)?>"
    placeholder_map = {}

    def repl_tag(m):
        tok = f"___TAG_HOLD_{len(placeholder_map)}___"
        placeholder_map[tok] = m.group(0)
        return tok

    tokenized = re.sub(pattern, repl_tag, text, flags=re.IGNORECASE)
    tokenized = re.sub(r"&(?!amp;|lt;|gt;|quot;|apos;)", "&amp;", tokenized)
    tokenized = tokenized.replace("<", "&lt;").replace(">", "&gt;")
    for tok, orig in placeholder_map.items():
        tokenized = tokenized.replace(tok, orig)

    return tokenized


def send_telegram_alert(bot_token: str, chat_id: str, message_html: str) -> bool:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    sanitized_html = escape_telegram_html(message_html)
    payload = {
        "chat_id": chat_id,
        "text": sanitized_html,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        r = requests.post(url, json=payload, timeout=25)
        print(f"[TELEGRAM] HTTP Status: {r.status_code}")
        if r.status_code == 200 and r.json().get("ok"):
            print("[TELEGRAM] Arbitrage notification delivered successfully.")
            return True
        else:
            print(f"[TELEGRAM] Error: sendMessage failed with HTTP {r.status_code}: {r.text[:200]}")
            return False
    except Exception as e:
        # ห้าม log ตัว exception ดิบ: requests ใส่ URL เต็ม (ซึ่งมี bot token) ลงใน
        # ข้อความของ ConnectionError/Timeout และ repo นี้เป็น public
        # → log ของ GitHub Actions อ่านได้โดยใครก็ได้
        print(f"[TELEGRAM] Exception sending alert: {type(e).__name__} (URL/token redacted)")
        return False


def send_telegram_document(
    bot_token: str,
    chat_id: str,
    file_bytes: bytes,
    filename: str,
    caption: Optional[str] = None
) -> bool:
    """
    Sends a file (e.g. .xlsx Multiple Order template) to Telegram using sendDocument API.
    """
    url = f"https://api.telegram.org/bot{bot_token}/sendDocument"
    files = {
        "document": (filename, file_bytes, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    }
    data = {
        "chat_id": chat_id
    }
    if caption:
        data["caption"] = caption
        data["parse_mode"] = "HTML"

    try:
        r = requests.post(url, data=data, files=files, timeout=25)
        print(f"[TELEGRAM] sendDocument Status: {r.status_code}")
        if r.status_code == 200 and r.json().get("ok"):
            print(f"[TELEGRAM] Streaming Multi-Order document '{filename}' delivered successfully.")
            return True
        else:
            print(f"[TELEGRAM] Error: sendDocument failed with HTTP {r.status_code}: {r.text}")
            return False
    except Exception as e:
        print(f"[TELEGRAM] Exception sending document: {type(e).__name__} (URL/token redacted)")
        return False


def resolve_order_legs(opp: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extracts or resolves order legs for Settrade Streaming Multiple Order.
    Returns list of dicts: {'symbol': str, 'side': 'L'|'S', 'price': float, 'depth': float, 'is_futures': bool}
    """
    if "order_legs" in opp and opp["order_legs"]:
        return [dict(leg) for leg in opp["order_legs"]]

    # Fallback parser from text legs in opp.get("legs", [])
    resolved = []
    series = opp.get("series", "Z26")
    if "-" in str(series):
        series_parts = str(series).split("-")
        front_ser = series_parts[0]
        next_ser = series_parts[1]
    else:
        front_ser = str(series)
        next_ser = ""

    legs_text = opp.get("legs", [])
    for leg_str in legs_text:
        clean_text = leg_str.strip()
        side = "L" if (clean_text.startswith("Buy") or "Buy " in clean_text) else "S"
        is_fut = ("Futures" in clean_text) or ("Front" in clean_text) or ("Next" in clean_text)

        price_m = re.search(r"<code>([0-9.]+)</code>", clean_text)
        price = float(price_m.group(1)) if price_m else 0.0

        depth_m = re.search(r"Depth:\s*([0-9,.]+)", clean_text)
        depth = float(depth_m.group(1).replace(",", "")) if depth_m else 0.0

        sym = ""
        if is_fut:
            fut_m = re.search(r"(S50[A-Z0-9]+)", clean_text)
            if fut_m:
                sym = fut_m.group(1)
            elif "Next" in clean_text and next_ser:
                sym = f"S50{next_ser}"
            else:
                sym = f"S50{front_ser}"
        else:
            opt_m = re.search(r"(Call|Put)\s+([0-9.]+)", clean_text, re.IGNORECASE)
            if opt_m:
                otype = "C" if opt_m.group(1).lower() == "call" else "P"
                strike_num = float(opt_m.group(2))
                sym = f"S50{front_ser}{otype}{strike_num:.0f}"
            else:
                sym = f"S50{front_ser}"

        resolved.append({
            "symbol": sym,
            "side": side,
            "price": price,
            "depth": depth,
            "is_futures": is_fut
        })

    return resolved


def generate_streaming_multi_order_xlsx(opp: Dict[str, Any], allow_statistical: bool = False) -> Tuple[bytes, str]:
    """
    Generates a Settrade Streaming Multiple Order import Excel (.xlsx) file in-memory.
    Columns: Symbol, Side, Volume, Price, Position, Type, Validity, Iceberg

    Safety Constraints:
    - Volume: 1 (always 1 set)
    - Position: "A" (Auto)
    - Type: "Limit"
    - Validity: "FOK" (Fill or Kill)
    - Iceberg: None (empty)
    - Side: "L" for Buy (Ask), "S" for Sell (Bid)
    - Price: Execution price (Ask for Buy, Bid for Sell)
    - Restricted to Pure Arbitrage (Disallows Statistical Spreads like Calendar Spread by default).

    Critical Execution Order (Reduce Legging Risk):
    - Options sorted ascending by liquidity depth (least liquid first)
    - Futures (e.g. S50Z26) ALWAYS placed last

    Returns:
    - (file_bytes, filename)
    """
    strat = opp.get("strategy", "Arb")
    if "Calendar" in strat and not allow_statistical:
        raise ValueError(
            f"Calendar Spread ('{strat}') is a statistical spread, not pure arbitrage. "
            f"Streaming Multi-Order Excel generation is restricted to pure arbitrage only."
        )

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "MultipleOrder"

    headers = ["Symbol", "Side", "Volume", "Price", "Position", "Type", "Validity", "Iceberg"]
    ws.append(headers)

    legs = resolve_order_legs(opp)
    if not legs:
        raise ValueError(f"No order legs found for opportunity: {opp}")

    # Sort legs: Options first (is_futures=0) by depth ascending, Futures last (is_futures=1) by depth ascending
    sorted_legs = sorted(legs, key=lambda x: (1 if x.get("is_futures") else 0, float(x.get("depth", 0.0))))

    for leg in sorted_legs:
        sym = str(leg["symbol"])
        side_val = str(leg["side"]).upper()
        side = "L" if side_val in ("L", "BUY", "LONG") else "S"
        vol = 1
        price = float(leg["price"])
        pos = "A"
        ord_type = "Limit"
        validity = "FOK"
        iceberg = None

        ws.append([sym, side, vol, price, pos, ord_type, validity, iceberg])

    buf = io.BytesIO()
    wb.save(buf)
    file_bytes = buf.getvalue()
    buf.close()

    strat = opp.get("strategy", "Arb")
    target = opp.get("target", "Target")
    time_val = opp.get("time", "")
    time_tag = re.sub(r"[^0-9]", "", str(time_val))[-6:]
    if len(time_tag) < 6:
        time_tag = datetime.datetime.now(BANGKOK_TZ).strftime("%H%M%S")

    clean_strat = re.sub(r"[^a-zA-Z0-9]+", "_", strat).strip("_")
    clean_target = re.sub(r"[^a-zA-Z0-9]+", "_", target).strip("_")
    filename = f"ARB_{clean_strat}_{clean_target}_{time_tag}.xlsx"

    return file_bytes, filename


def format_trade_log_line(opp: Dict[str, Any]) -> str:
    """
    Formats a single-line summary for copying into Excel Trade_Log.
    Format:
    📋 Log: {Strategy} | {Leg1_Sym} {Side1} {Price1:.2f} | {Leg2_Sym} {Side2} {Price2:.2f} [| {Leg3_Sym} {Side3} {Price3:.2f}] | +{NetTHB:.0f}
    """
    strat = opp.get("strategy", "Arb")
    strat_clean = strat.split("(")[0].strip() or strat

    legs = opp.get("order_legs") or resolve_order_legs(opp)
    legs_parts = []
    for leg in legs:
        sym = leg.get("symbol", "")
        side = str(leg.get("side", "")).upper()
        price = float(leg.get("price", 0.0))
        legs_parts.append(f"{sym} {side} {price:.2f}")

    net_pts = float(opp.get("net_pts", 0.0))
    net_thb = net_pts * CONTRACT_MULTIPLIER
    net_thb_str = f"+{net_thb:.0f}" if net_thb >= 0 else f"{net_thb:.0f}"

    parts = [f"📋 Log: {strat_clean}"] + legs_parts + [net_thb_str]
    return " | ".join(parts)


_SETTRADE_INVESTOR = None
_SETTRADE_MARKET_DATA = None


def get_settrade_market_data(
    app_id: Optional[str] = None,
    app_secret: Optional[str] = None,
    broker_id: Optional[str] = None,
    app_code: Optional[str] = None,
    is_sandbox: Optional[bool] = None
) -> Any:
    """
    Initializes and caches Settrade Open API MarketData client using environment variables.
    Returns None if credentials are missing or settrade_v2 is unavailable.
    """
    global _SETTRADE_INVESTOR, _SETTRADE_MARKET_DATA
    if _SETTRADE_MARKET_DATA is not None:
        return _SETTRADE_MARKET_DATA

    app_id = app_id or os.getenv("SETTRADE_APP_ID", "").strip()
    app_secret = app_secret or os.getenv("SETTRADE_APP_SECRET", "").strip()
    broker_id = broker_id or os.getenv("SETTRADE_BROKER_ID", "").strip()
    app_code = app_code or os.getenv("SETTRADE_APP_CODE", "ALGO").strip() or "ALGO"

    if is_sandbox is None:
        is_sb_str = os.getenv("SETTRADE_IS_SANDBOX", "false").strip().lower()
        is_sandbox = is_sb_str in ("1", "true", "yes")

    if not app_id or not app_secret or not broker_id:
        return None

    if Investor is None:
        print("[SETTRADE] settrade_v2 library is not available.")
        return None

    try:
        broker = broker_id
        if is_sandbox:
            if settrade_config is not None:
                settrade_config["environment"] = "uat"
            if not broker or broker.upper() == "SANDBOX":
                broker = Investor.SANDBOX

        _SETTRADE_INVESTOR = Investor(
            app_id=app_id,
            app_secret=app_secret,
            broker_id=broker,
            app_code=app_code,
            is_auto_queue=False
        )
        _SETTRADE_MARKET_DATA = _SETTRADE_INVESTOR.MarketData()
        print("[SETTRADE] MarketData initialized successfully for REST verification.")
        return _SETTRADE_MARKET_DATA
    except Exception as e:
        print(f"[SETTRADE] Initialization failed: {e}")
        return None


def extract_rest_quote_depth(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extracts bid, ask, bid_vol, ask_vol, last, volume from Settrade REST quote dictionary.
    Supports both flat keys and nested bids/asks lists.
    """
    if not isinstance(raw, dict):
        return {
            "bid": None, "ask": None, "bid_vol": 0.0, "ask_vol": 0.0,
            "last": None, "volume": 0.0
        }

    bid = raw.get("bid") or raw.get("bid_price1") or raw.get("bidPrice1") or raw.get("best_bid")
    ask = raw.get("ask") or raw.get("ask_price1") or raw.get("askPrice1") or raw.get("offer") or raw.get("best_ask")
    bid_vol = raw.get("bid_vol") or raw.get("bid_volume1") or raw.get("bidVolume1") or raw.get("bid_size")
    ask_vol = raw.get("ask_vol") or raw.get("ask_volume1") or raw.get("askVolume1") or raw.get("ask_size")
    last = raw.get("last") or raw.get("last_price") or raw.get("lastPrice")
    vol = raw.get("volume") or raw.get("total_volume") or raw.get("totalVolume")

    if bid is None and "bids" in raw and isinstance(raw["bids"], list) and raw["bids"]:
        bid = raw["bids"][0].get("price")
        if bid_vol is None:
            bid_vol = raw["bids"][0].get("volume")
    if ask is None and "asks" in raw and isinstance(raw["asks"], list) and raw["asks"]:
        ask = raw["asks"][0].get("price")
        if ask_vol is None:
            ask_vol = raw["asks"][0].get("volume")

    def _to_f(v):
        try:
            return float(v) if v is not None else None
        except (ValueError, TypeError):
            return None

    b = _to_f(bid)
    a = _to_f(ask)
    bv = _to_f(bid_vol) or 0.0
    av = _to_f(ask_vol) or 0.0
    lp = _to_f(last)
    tv = _to_f(vol) or 0.0

    return {
        "bid": b if (b is not None and b > 0) else None,
        "ask": a if (a is not None and a > 0) else None,
        "bid_vol": bv if bv > 0 else 0.0,
        "ask_vol": av if av > 0 else 0.0,
        "last": lp,
        "volume": tv
    }


def _recalculate_opp_math(
    opp: Dict[str, Any],
    legs: List[Dict[str, Any]],
    live_quotes: Dict[str, Dict[str, Any]],
    thor_rate: Optional[float] = None,
    T_years: Optional[float] = None
) -> Tuple[Optional[float], Optional[float], int]:
    """
    Recalculates (gross_pts, net_pts, max_sets) from live quotes using exact strategy formulas.
    """
    strat = opp.get("strategy", "")
    comm = float(opp.get("commission_pts", COMMISSION_2LEG_PTS if len(legs) == 2 else COMMISSION_3LEG_PTS))

    # Conversion
    if "Conversion" in strat:
        c_leg = next((l for l in legs if not l.get("is_futures") and "C" in l["symbol"]), None)
        p_leg = next((l for l in legs if not l.get("is_futures") and "P" in l["symbol"]), None)
        f_leg = next((l for l in legs if l.get("is_futures")), None)
        if not c_leg or not p_leg or not f_leg:
            return None, None, 0
        c_bid = live_quotes[c_leg["symbol"]]["bid"]
        c_bvol = live_quotes[c_leg["symbol"]]["bid_vol"]
        p_ask = live_quotes[p_leg["symbol"]]["ask"]
        p_avol = live_quotes[p_leg["symbol"]]["ask_vol"]
        f_ask = live_quotes[f_leg["symbol"]]["ask"]
        f_avol = live_quotes[f_leg["symbol"]]["ask_vol"]
        if c_bid is None or p_ask is None or f_ask is None:
            return None, None, 0
        k = float(opp.get("strike", 0.0))
        disc = opp.get("disc_factor")
        if disc is None and thor_rate is not None and T_years is not None:
            disc = math.exp(-thor_rate * T_years)
        if disc is None:
            print("[RECALC] disc_factor missing and THOR/T unavailable — assuming 1.0 "
                  "(no discount); parity edge may be overstated on deep strikes")
            disc = 1.0
        gross = (c_bid - p_ask) - (f_ask - k) * disc
        net = gross - comm
        max_sets = _exec_sets(c_bvol, p_avol, f_avol)
        return (gross, net, max_sets) if max_sets >= 1 else (None, None, 0)

    # Reversal
    if "Reversal" in strat:
        c_leg = next((l for l in legs if not l.get("is_futures") and "C" in l["symbol"]), None)
        p_leg = next((l for l in legs if not l.get("is_futures") and "P" in l["symbol"]), None)
        f_leg = next((l for l in legs if l.get("is_futures")), None)
        if not c_leg or not p_leg or not f_leg:
            return None, None, 0
        c_ask = live_quotes[c_leg["symbol"]]["ask"]
        c_avol = live_quotes[c_leg["symbol"]]["ask_vol"]
        p_bid = live_quotes[p_leg["symbol"]]["bid"]
        p_bvol = live_quotes[p_leg["symbol"]]["bid_vol"]
        f_bid = live_quotes[f_leg["symbol"]]["bid"]
        f_bvol = live_quotes[f_leg["symbol"]]["bid_vol"]
        if c_ask is None or p_bid is None or f_bid is None:
            return None, None, 0
        k = float(opp.get("strike", 0.0))
        disc = opp.get("disc_factor")
        if disc is None and thor_rate is not None and T_years is not None:
            disc = math.exp(-thor_rate * T_years)
        if disc is None:
            print("[RECALC] disc_factor missing and THOR/T unavailable — assuming 1.0 "
                  "(no discount); parity edge may be overstated on deep strikes")
            disc = 1.0
        gross = (f_bid - k) * disc - (c_ask - p_bid)
        net = gross - comm
        max_sets = _exec_sets(c_avol, p_bvol, f_bvol)
        return (gross, net, max_sets) if max_sets >= 1 else (None, None, 0)

    # Call Monotonicity
    if "Call Monotonicity" in strat:
        buy_leg = next((l for l in legs if l.get("side", "").upper() in ("L", "BUY")), None)
        sell_leg = next((l for l in legs if l.get("side", "").upper() in ("S", "SELL")), None)
        if not buy_leg or not sell_leg:
            return None, None, 0
        b_ask = live_quotes[buy_leg["symbol"]]["ask"]
        b_avol = live_quotes[buy_leg["symbol"]]["ask_vol"]
        s_bid = live_quotes[sell_leg["symbol"]]["bid"]
        s_bvol = live_quotes[sell_leg["symbol"]]["bid_vol"]
        if b_ask is None or s_bid is None:
            return None, None, 0
        gross = s_bid - b_ask
        net = gross - comm
        max_sets = _exec_sets(b_avol, s_bvol)
        return (gross, net, max_sets) if max_sets >= 1 else (None, None, 0)

    # Put Monotonicity
    if "Put Monotonicity" in strat:
        sell_leg = next((l for l in legs if l.get("side", "").upper() in ("S", "SELL")), None)
        buy_leg = next((l for l in legs if l.get("side", "").upper() in ("L", "BUY")), None)
        if not sell_leg or not buy_leg:
            return None, None, 0
        s_bid = live_quotes[sell_leg["symbol"]]["bid"]
        s_bvol = live_quotes[sell_leg["symbol"]]["bid_vol"]
        b_ask = live_quotes[buy_leg["symbol"]]["ask"]
        b_avol = live_quotes[buy_leg["symbol"]]["ask_vol"]
        if s_bid is None or b_ask is None:
            return None, None, 0
        gross = s_bid - b_ask
        net = gross - comm
        max_sets = _exec_sets(s_bvol, b_avol)
        return (gross, net, max_sets) if max_sets >= 1 else (None, None, 0)

    # Calendar Spread Spike High
    if "Calendar" in strat and "High" in strat:
        sell_leg = next((l for l in legs if l.get("side", "").upper() in ("S", "SELL")), None)
        buy_leg = next((l for l in legs if l.get("side", "").upper() in ("L", "BUY")), None)
        if not sell_leg or not buy_leg:
            return None, None, 0
        s_bid = live_quotes[sell_leg["symbol"]]["bid"]
        s_bvol = live_quotes[sell_leg["symbol"]]["bid_vol"]
        b_ask = live_quotes[buy_leg["symbol"]]["ask"]
        b_avol = live_quotes[buy_leg["symbol"]]["ask_vol"]
        if s_bid is None or b_ask is None:
            return None, None, 0
        mu = float(opp.get("mu", 0.0))
        exec_sell_spread = s_bid - b_ask
        gross = exec_sell_spread - mu
        net = gross - comm
        max_sets = _exec_sets(s_bvol, b_avol)
        return (gross, net, max_sets) if max_sets >= 1 else (None, None, 0)

    # Calendar Spread Spike Low
    if "Calendar" in strat and "Low" in strat:
        buy_leg = next((l for l in legs if l.get("side", "").upper() in ("L", "BUY")), None)
        sell_leg = next((l for l in legs if l.get("side", "").upper() in ("S", "SELL")), None)
        if not buy_leg or not sell_leg:
            return None, None, 0
        b_ask = live_quotes[buy_leg["symbol"]]["ask"]
        b_avol = live_quotes[buy_leg["symbol"]]["ask_vol"]
        s_bid = live_quotes[sell_leg["symbol"]]["bid"]
        s_bvol = live_quotes[sell_leg["symbol"]]["bid_vol"]
        if b_ask is None or s_bid is None:
            return None, None, 0
        mu = float(opp.get("mu", 0.0))
        exec_buy_spread = b_ask - s_bid
        gross = mu - exec_buy_spread
        net = gross - comm
        max_sets = _exec_sets(b_avol, s_bvol)
        return (gross, net, max_sets) if max_sets >= 1 else (None, None, 0)

    # General 2-leg fallback
    if len(legs) == 2:
        buy_leg = next((l for l in legs if l.get("side", "").upper() in ("L", "BUY")), None)
        sell_leg = next((l for l in legs if l.get("side", "").upper() in ("S", "SELL")), None)
        if buy_leg and sell_leg:
            b_ask = live_quotes[buy_leg["symbol"]]["ask"]
            b_avol = live_quotes[buy_leg["symbol"]]["ask_vol"]
            s_bid = live_quotes[sell_leg["symbol"]]["bid"]
            s_bvol = live_quotes[sell_leg["symbol"]]["bid_vol"]
            if b_ask is not None and s_bid is not None:
                gross = s_bid - b_ask
                net = gross - comm
                max_sets = _exec_sets(b_avol, s_bvol)
                return (gross, net, max_sets) if max_sets >= 1 else (None, None, 0)

    return None, None, 0


def verify_live_quotes_before_alert(
    opp: Dict[str, Any],
    market_data: Any = None,
    running_futures: Optional[Dict[str, Dict[str, Any]]] = None,
    running_options: Optional[Dict[Tuple[str, float, str], Dict[str, Any]]] = None,
    thor_rate: Optional[float] = None,
    T_years: Optional[float] = None,
    require_rest: bool = True
) -> Tuple[bool, Dict[str, Any]]:
    """
    Pre-Alert REST Double-Check Gate:
    Fetches real-time live quotes (bid, ask, bid_vol, ask_vol) from Settrade REST API (get_quote_symbol)
    for the 2-3 legs of the top opportunity before sending Telegram alert or generating .xlsx.

    Rules:
    1. Overwrites old values in running_options / running_futures immediately with fresh REST data.
    2. Checks volume depth >= 1 for each leg and valid price.
    3. Re-calculates net_pts after commission using live REST prices.
    4. If net_pts > 0 and all legs have vol >= 1:
       - Updates order_legs, legs text checklist, gross_pts, net_pts, and max_sets.
       - Returns (True, updated_opp).
    5. If net_pts <= 0 or any leg vol < 1:
       - Logs [REST-GUARD] Filtered ghost/stale opportunity.
       - Suppresses alert (returns (False, opp)).
    """
    if market_data is None:
        market_data = get_settrade_market_data()

    if market_data is None:
        # Settrade is not configured / failed to initialize (expired secret, network,
        # library missing). Previously this "bypassed" the gate and returned True,
        # so alerts went out on DB quotes that can be 15 minutes stale while the
        # message still claimed "REST Verified ✅". Fail closed unless explicitly
        # opted out with --allow-unverified.
        reason = "[REST-GUARD] Settrade MarketData unavailable — live quotes NOT verified."
        if require_rest:
            print(f"{reason} Suppressing alert (fail-closed). Use --allow-unverified to override.")
            suppressed = dict(opp)
            suppressed["rest_verified"] = False
            return False, suppressed
        print(f"{reason} Continuing WITHOUT verification (--allow-unverified). Alert is labelled DB-only.")
        unverified = dict(opp)
        unverified["rest_verified"] = False
        return True, unverified

    legs = resolve_order_legs(opp)
    if not legs:
        print("[REST-GUARD] No order legs found for opportunity. Suppressing.")
        return False, opp

    now_bkk = datetime.datetime.now(BANGKOK_TZ)
    now_ts_str = now_bkk.strftime("%Y-%m-%d %H:%M:%S")

    live_quotes: Dict[str, Dict[str, Any]] = {}
    strat = opp.get("strategy", "")
    target = opp.get("target", "")

    # Step 1: Query REST quotes for each leg (max 3 calls)
    for leg in legs:
        sym = leg["symbol"]
        try:
            raw_quote = market_data.get_quote_symbol(sym)
            q_data = extract_rest_quote_depth(raw_quote)
        except Exception as e:
            print(f"[REST-GUARD] Error fetching quote for {sym}: {e}")
            return False, opp

        live_quotes[sym] = q_data

        # Update cache immediately to flush stale/ghost data
        if leg.get("is_futures") or not re.search(r"[CP]\d+$", sym):
            if running_futures is not None:
                running_futures[sym] = {
                    "symbol": sym,
                    "bid": q_data["bid"],
                    "ask": q_data["ask"],
                    "bid_vol": q_data["bid_vol"],
                    "ask_vol": q_data["ask_vol"],
                    "last": q_data["last"],
                    "volume": q_data["volume"],
                    "last_update_ts": now_ts_str
                }
        else:
            if running_options is not None:
                m = re.match(r"^S50([A-Z]\d{2})([CP])(\d+)$", sym)
                if m:
                    ser, otype, k_str = m.groups()
                    key = (str(ser), float(k_str), str(otype).upper())
                    running_options[key] = {
                        "symbol": sym,
                        "series": str(ser),
                        "strike": float(k_str),
                        "option_type": str(otype).upper(),
                        "bid": q_data["bid"],
                        "ask": q_data["ask"],
                        "bid_vol": q_data["bid_vol"],
                        "ask_vol": q_data["ask_vol"],
                        "last": q_data["last"],
                        "volume": q_data["volume"],
                        "last_update_ts": now_ts_str
                    }

    # Step 2: Validate execution price and depth >= 1 for each leg
    for leg in legs:
        sym = leg["symbol"]
        side = str(leg.get("side", "")).upper()
        q = live_quotes[sym]
        if side in ("L", "BUY", "LONG"):
            price = q["ask"]
            vol = q["ask_vol"]
            req_side = "Ask"
        else:
            price = q["bid"]
            vol = q["bid_vol"]
            req_side = "Bid"

        if price is None or price <= 0:
            print(f"[REST-GUARD] Filtered ghost/stale opportunity ({strat} - {target}): {sym} {req_side} price is missing/invalid ({price}).")
            return False, opp

        if vol < 1.0:
            print(f"[REST-GUARD] Filtered ghost/stale opportunity ({strat} - {target}): {sym} {req_side} depth is 0 or depleted (vol={vol}).")
            return False, opp

    # Step 3: Recalculate net profit from live REST prices
    gross, net, max_sets = _recalculate_opp_math(opp, legs, live_quotes, thor_rate, T_years)
    if gross is None or net is None:
        print(f"[REST-GUARD] Filtered ghost/stale opportunity ({strat} - {target}): Math calculation failed.")
        return False, opp

    if net <= 0.0:
        print(f"[REST-GUARD] Filtered ghost/stale opportunity ({strat} - {target}): Recalculated net_pts={net:.2f} <= 0.")
        return False, opp

    # Step 4: Construct verified, updated opportunity
    updated_opp = dict(opp)
    updated_opp["gross_pts"] = gross
    updated_opp["net_pts"] = net
    updated_opp["max_sets"] = max_sets

    updated_order_legs = []
    updated_text_legs = []
    for leg in legs:
        sym = leg["symbol"]
        side = str(leg.get("side", "")).upper()
        is_fut = bool(leg.get("is_futures", False))
        q = live_quotes[sym]
        if side in ("L", "BUY", "LONG"):
            p = q["ask"]
            d = q["ask_vol"]
            side_str = "L"
            side_label = "Buy"
            price_type = "Ask"
        else:
            p = q["bid"]
            d = q["bid_vol"]
            side_str = "S"
            side_label = "Sell"
            price_type = "Bid"

        updated_order_legs.append({
            "symbol": sym,
            "side": side_str,
            "price": p,
            "depth": d,
            "is_futures": is_fut
        })

        # Format text checklist leg
        m = re.match(r"^S50([A-Z]\d{2})([CP])(\d+)$", sym)
        if m:
            ser, otype, k_str = m.groups()
            opt_kind = "Call" if otype == "C" else "Put"
            updated_text_legs.append(f"{side_label} {opt_kind} {float(k_str):.0f} @ {price_type} <code>{p:.2f}</code> (Depth: {d:,.0f})")
        else:
            fut_label = "Futures"
            if "Front" in leg.get("label", "") or "f1" in sym.lower():
                fut_label = "Front"
            elif "Next" in leg.get("label", "") or "f2" in sym.lower():
                fut_label = "Next"
            updated_text_legs.append(f"{side_label} {fut_label} {sym} @ {price_type} <code>{p:.2f}</code> (Depth: {d:,.0f})")

    updated_opp["order_legs"] = updated_order_legs
    updated_opp["legs"] = updated_text_legs
    updated_opp["rest_verified"] = True

    print(f"[REST-GUARD] ✅ Verified live quotes via REST: {strat} {target} (Net: +{net:.2f} pts, Sets: {max_sets})")
    return True, updated_opp


def is_executable_market_hours(dt_or_time: Any) -> bool:
    """
    Market Hours Guard:
    Checks if given datetime or time is within actual executable trading hours:
    - Morning: 09:45:00 - 12:30:00
    - Afternoon: 13:45:00 - 16:55:00
    Returns False outside these windows (e.g. 16:55:01 - 17:05:00 is post-market when MMs withdraw quotes).
    """
    if isinstance(dt_or_time, datetime.datetime):
        t = dt_or_time.time()
    elif isinstance(dt_or_time, time):
        t = dt_or_time
    else:
        return False

    is_morning = (time(9, 45, 0) <= t <= time(12, 30, 0))
    is_afternoon = (time(13, 45, 0) <= t <= time(16, 55, 0))
    return is_morning or is_afternoon


class ArbSmartCooldown:
    """
    In-memory smart cooldown cache for real-time 1-minute arbitrage alerts.
    Key: f"{strategy}|{target}"
    Rules:
    - New opportunity: Alert immediately.
    - Existing opportunity within cooldown window (15 minutes / 900s):
      - Alert immediately if net profit improves by >= +0.20 pts.
      - Otherwise, suppress to prevent spamming.
    - If elapsed >= 15 minutes: Reset cooldown and alert.
    """
    def __init__(self, cooldown_seconds: int = 900, profit_hurdle_pts: float = 0.20):
        self.cooldown_seconds = cooldown_seconds
        self.profit_hurdle_pts = profit_hurdle_pts
        self.cache: Dict[str, Dict[str, Any]] = {}

    def is_alertable(self, opp: Dict[str, Any], now_dt: datetime.datetime) -> Tuple[bool, Optional[str]]:
        """
        Pure inspection: checks whether an opportunity is eligible for alerting.
        Does NOT modify cache or record cooldown state.
        Returns: (is_eligible, reason_or_spike_stat)
        """
        key = f"{opp['strategy']}|{opp['target']}"
        net_pts = float(opp.get("net_pts", 0.0))

        if key not in self.cache:
            return True, None

        entry = self.cache[key]
        elapsed = (now_dt - entry["last_alerted_at"]).total_seconds()
        profit_diff = net_pts - entry["last_net_pts"]

        if elapsed >= self.cooldown_seconds:
            return True, None
        elif profit_diff >= self.profit_hurdle_pts:
            return True, f"Profit Spike: +{profit_diff:.2f} pts vs previous alert"
        else:
            return False, None

    def filter_alertable(
        self,
        opps: List[Dict[str, Any]],
        now_dt: datetime.datetime,
        auto_record: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Filters list of opportunities to those eligible for alerting.
        If auto_record is False (default for decoupled REST verification),
        cache is NOT updated until record_alert() is called after verification succeeds.
        """
        alertable = []
        for opp in opps:
            eligible, spike_stat = self.is_alertable(opp, now_dt)
            if eligible:
                opp_copy = dict(opp)
                if spike_stat:
                    opp_copy["stats"] = spike_stat
                alertable.append(opp_copy)
                if auto_record:
                    self.record_alert(opp, now_dt)
        return alertable

    def record_alert(self, opp: Dict[str, Any], now_dt: datetime.datetime):
        """
        Explicitly records that an opportunity was verified by REST and alerted via Telegram.
        """
        key = f"{opp['strategy']}|{opp['target']}"
        net_pts = float(opp.get("net_pts", 0.0))
        if key not in self.cache:
            self.cache[key] = {
                "last_alerted_at": now_dt,
                "last_net_pts": net_pts,
                "alert_count": 1
            }
        else:
            self.cache[key]["last_alerted_at"] = now_dt
            self.cache[key]["last_net_pts"] = net_pts
            self.cache[key]["alert_count"] = self.cache[key].get("alert_count", 0) + 1


class OpportunityLifespanTracker:
    """
    In-memory tick-to-tick lifespan duration tracker for arbitrage opportunities.
    Key: f"{strategy}|{target}"
    Tracks duration in seconds from first_seen_ts until opportunity closes or session ends.
    """
    def __init__(self):
        self.active_opportunities: Dict[str, Dict[str, datetime.datetime]] = {}
        self.closed_durations: List[float] = []

    def _normalize_ts(self, ts_val: Any) -> datetime.datetime:
        """
        Normalizes any input timestamp (datetime, ISO string, HH:MM:SS) into a
        consistent timezone-naive Bangkok local datetime object (UTC+7).
        Guarantees that offset-aware and offset-naive comparisons never fail.
        """
        if isinstance(ts_val, datetime.datetime):
            if ts_val.tzinfo is not None:
                return ts_val.astimezone(BANGKOK_TZ).replace(tzinfo=None)
            return ts_val

        s = str(ts_val).strip()
        try:
            dt = datetime.datetime.fromisoformat(s)
            if dt.tzinfo is not None:
                return dt.astimezone(BANGKOK_TZ).replace(tzinfo=None)
            return dt
        except Exception:
            pass

        if len(s) >= 19 and (" " in s or "T" in s):
            s_clean = s[:19].replace("T", " ")
            try:
                return datetime.datetime.strptime(s_clean, "%Y-%m-%d %H:%M:%S")
            except Exception:
                pass
        elif len(s) == 8 and s.count(":") == 2:
            try:
                today = datetime.date.today()
                t = datetime.datetime.strptime(s, "%H:%M:%S").time()
                return datetime.datetime.combine(today, t)
            except Exception:
                pass
        return datetime.datetime.now(BANGKOK_TZ).replace(tzinfo=None)

    def _parse_ts(self, ts_val: Any) -> datetime.datetime:
        return self._normalize_ts(ts_val)

    def update(self, current_profitable_opps: List[Dict[str, Any]], current_ts: Any) -> None:
        curr_dt = self._normalize_ts(current_ts)
        seen_keys = set()

        for opp in current_profitable_opps:
            if opp.get("net_pts", 0.0) > 0:
                key = f"{opp['strategy']}|{opp['target']}"
                seen_keys.add(key)
                if key not in self.active_opportunities:
                    self.active_opportunities[key] = {
                        "first_seen_ts": curr_dt,
                        "last_seen_ts": curr_dt
                    }
                else:
                    self.active_opportunities[key]["last_seen_ts"] = curr_dt

        # Check for opportunities that disappeared / closed
        closed_keys = [k for k in self.active_opportunities if k not in seen_keys]
        for k in closed_keys:
            entry = self.active_opportunities.pop(k)
            first_ts = self._normalize_ts(entry["first_seen_ts"])
            last_ts = self._normalize_ts(entry["last_seen_ts"])
            if last_ts < first_ts:
                last_ts = first_ts
            dur = max(1.0, (last_ts - first_ts).total_seconds())
            self.closed_durations.append(dur)

    def finalize(self, end_ts: Any = None) -> None:
        """Closes all remaining active opportunities at session end."""
        end_dt = self._normalize_ts(end_ts) if end_ts is not None else None
        for k, entry in list(self.active_opportunities.items()):
            first_ts = self._normalize_ts(entry["first_seen_ts"])
            last_ts = end_dt if end_dt is not None else self._normalize_ts(entry["last_seen_ts"])
            if last_ts < first_ts:
                last_ts = first_ts
            dur = max(1.0, (last_ts - first_ts).total_seconds())
            self.closed_durations.append(dur)
        self.active_opportunities.clear()

    def get_stats(self) -> Dict[str, Any]:
        if not self.closed_durations:
            return {
                "has_data": False,
                "count": 0,
                "avg_sec": 0.0,
                "min_sec": 0.0,
                "max_sec": 0.0
            }
        arr = self.closed_durations
        return {
            "has_data": True,
            "count": len(arr),
            "avg_sec": float(sum(arr) / len(arr)),
            "min_sec": float(min(arr)),
            "max_sec": float(max(arr))
        }


def format_action_leg(leg: str) -> str:
    """
    Parses leg description and formats with high-visibility actionable color tags:
    - Buy -> 🟢 LONG (เคาะซื้อ):
    - Sell -> 🔴 SHORT (เคาะขาย):
    """
    clean_leg = leg.strip()
    if clean_leg.startswith("Buy "):
        return f"🟢 <b>LONG (เคาะซื้อ):</b> {clean_leg[4:]}"
    elif clean_leg.startswith("Sell "):
        return f"🔴 <b>SHORT (เคาะขาย):</b> {clean_leg[5:]}"
    return f"• {clean_leg}"


def format_live_1min_alert(
    trade_date: str,
    bar_ts: str,
    profitable_opps: List[Dict[str, Any]],
    thor_rate: float,
    dte: int,
    T_years: float,
    thor_tenor: str = ""
) -> str:
    """
    Formats instant real-time Telegram alert for 1-minute detected arbitrage opportunities.
    Compact design (< 3,800 chars):
    - Header: 🚨 LIVE ARB ALERT (HH:MM:SS | พบ N โอกาส)
    - Rank #1: Full execution details with legs ordered from thinnest to thickest liquidity + 📋 Log line.
    - Rank #2+: Compact 2 lines per opportunity (Line 1: Summary, Line 2: All legs on single line).
    - Safe Truncation: Truncates cleanly at 3,600 chars limit with notice of remaining opps.
    """
    if not profitable_opps:
        return ""

    ranked = sorted(profitable_opps, key=lambda x: x.get("net_pts", 0.0), reverse=True)
    total_cnt = len(ranked)

    tenor_str = f" ({thor_tenor})" if thor_tenor else ""
    header = (
        f"🚨 <b>LIVE ARB ALERT</b> (<code>{bar_ts} น.</code> | พบ {total_cnt} โอกาส)\n"
        f"📅 <i>{trade_date} | THOR{tenor_str}: {thor_rate*100.0:.2f}% | DTE: {dte} วัน</i>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━"
    )

    footer = (
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚡ <i>TFEX Live 1-Min Radar | Smart Cooldown 15 นาทีต่อคู่สัญญา</i>"
    )

    def _is_fut(leg: Dict[str, Any]) -> bool:
        if "is_futures" in leg:
            return bool(leg["is_futures"])
        sym = str(leg.get("symbol", ""))
        return not bool(re.search(r"[CP]\d+$", sym))

    def _format_legs_rank1(opp: Dict[str, Any]) -> List[str]:
        legs = opp.get("order_legs") or resolve_order_legs(opp)
        sorted_legs = sorted(legs, key=lambda x: (1 if _is_fut(x) else 0, float(x.get("depth", 0.0))))
        lines = []
        for leg in sorted_legs:
            side = "L" if str(leg.get("side", "")).upper() in ("L", "BUY", "LONG") else "S"
            sym = leg.get("symbol", "")
            p = float(leg.get("price", 0.0))
            d = float(leg.get("depth", 0.0))
            lines.append(f"• {side} {sym} @ {p:.2f} (Vol {d:,.0f})")
        return lines

    def _format_opp_rank1(opp: Dict[str, Any]) -> str:
        strat = opp.get("strategy", "Arb")
        target = opp.get("target", "")
        net_pts = float(opp.get("net_pts", 0.0))
        net_thb = net_pts * CONTRACT_MULTIPLIER
        max_sets = int(opp.get("max_sets", 1))

        legs_lines = _format_legs_rank1(opp)
        log_line = format_trade_log_line(opp)

        be_hedge_line = ""
        if "Conversion" in strat or "Reversal" in strat:
            k = float(opp.get("strike", 0.0))
            if k <= 0:
                m_k = re.search(r"Strike\s*(\d+)", target, re.IGNORECASE)
                if m_k:
                    k = float(m_k.group(1))
            order_legs = opp.get("order_legs") or resolve_order_legs(opp)
            fut_leg = next((l for l in order_legs if _is_fut(l)), None)
            c_leg = next((l for l in order_legs if not _is_fut(l) and "C" in str(l.get("symbol"))), None)
            p_leg = next((l for l in order_legs if not _is_fut(l) and "P" in str(l.get("symbol"))), None)

            if k > 0 and c_leg and p_leg and fut_leg:
                c_px = float(c_leg["price"])
                p_px = float(p_leg["price"])
                comm = float(opp.get("commission_pts", COMMISSION_3LEG_PTS))
                disc = float(opp.get("disc_factor") or math.exp(-thor_rate * max(T_years, 1e-6)))
                fut_side = str(fut_leg.get("side", "")).upper()
                if fut_side in ("L", "BUY", "LONG"):
                    f_be = k + (c_px - p_px - comm) / disc
                    be_hedge_line = f"\n  🎯 <b>Breakeven Fut Hedge:</b> <code>Long Fut &lt;= {f_be:.2f}</code> (ป้องกัน Legging Risk)"
                else:
                    f_be = k + (c_px - p_px + comm) / disc
                    be_hedge_line = f"\n  🎯 <b>Breakeven Fut Hedge:</b> <code>Short Fut &gt;= {f_be:.2f}</code> (ป้องกัน Legging Risk)"

        verify_label = "REST Verified ✅" if opp.get("rest_verified") else "⚠️ DB-only — ยังไม่ verified"

        return (
            f"<b>#1 {strat}</b> <code>{target}</code> | Net: 🟢 <b>+{net_pts:.2f} จุด (+{net_thb:,.0f} บ.)</b> | Max: <b>{max_sets}</b> ชุด ({verify_label})\n"
            + "\n".join(legs_lines)
            + be_hedge_line + "\n"
            + f"<code>{log_line}</code>"
        )

    def _format_opp_compact(idx: int, opp: Dict[str, Any]) -> str:
        strat = opp.get("strategy", "Arb")
        target = opp.get("target", "")
        net_pts = float(opp.get("net_pts", 0.0))
        net_thb = net_pts * CONTRACT_MULTIPLIER
        max_sets = int(opp.get("max_sets", 1))

        line1 = f"<b>#{idx} {strat}</b> <code>{target}</code> | Net: <b>+{net_pts:.2f} จุด (+{net_thb:,.0f} บ.)</b> | Max: {max_sets} ชุด"
        if not opp.get("rest_verified"):
            line1 += " ⚠️ <i>DB-only (ยังไม่ verified)</i>"

        legs = opp.get("order_legs") or resolve_order_legs(opp)
        sorted_legs = sorted(legs, key=lambda x: (1 if _is_fut(x) else 0, float(x.get("depth", 0.0))))
        legs_strs = []
        for leg in sorted_legs:
            side = "L" if str(leg.get("side", "")).upper() in ("L", "BUY", "LONG") else "S"
            sym = leg.get("symbol", "")
            p = float(leg.get("price", 0.0))
            d = float(leg.get("depth", 0.0))
            legs_strs.append(f"{side} {sym} @ {p:.2f} (Vol {d:,.0f})")
        line2 = "• " + " | ".join(legs_strs)

        return f"{line1}\n{line2}"

    # Build Rank #1 block
    body_blocks = [_format_opp_rank1(ranked[0])]
    truncated_count = 0

    # Build Rank #2+ blocks with Safe Truncation (< 3,600 chars limit)
    for idx, opp in enumerate(ranked[1:], start=2):
        opp_str = _format_opp_compact(idx, opp)
        remaining_after = total_cnt - idx
        test_trunc_notice = f"\n\n<i>...และอีก {remaining_after} โอกาส (ดูเพิ่มในสรุปปิดรอบ)</i>" if remaining_after > 0 else ""
        tentative_msg = f"{header}\n\n" + "\n\n".join(body_blocks + [opp_str]) + test_trunc_notice + f"\n\n{footer}"

        if len(tentative_msg) > 3600:
            truncated_count = total_cnt - (idx - 1)
            break
        body_blocks.append(opp_str)

    trunc_notice = f"\n\n<i>...และอีก {truncated_count} โอกาส (ดูเพิ่มในสรุปปิดรอบ)</i>" if truncated_count > 0 else ""
    return f"{header}\n\n" + "\n\n".join(body_blocks) + trunc_notice + f"\n\n{footer}"


def compute_best_of_minute_stats(
    grouped_data_keys: List[str],
    p_mins: Dict[str, float],
    v_mins: Dict[str, float],
    c_mins: Dict[str, float]
) -> Dict[str, Any]:
    """
    Coalesces sub-minute timestamps into 1-minute buckets (YYYY-MM-DD HH:MM or HH:MM)
    and extracts the best (maximum) net profit across parity, vertical, and calendar spreads
    for each distinct minute.
    Calculates summary metrics: Max, P90, Mean, SD, and near-breakeven percentage.
    """
    def to_minute_bucket(ts_str: str) -> str:
        # e.g. "2026-11-19 10:05:23" -> "2026-11-19 10:05"
        # e.g. "10:05:23" -> "10:05"
        if len(ts_str) >= 16 and (ts_str[10] == " " or ts_str[10] == "T"):
            return ts_str[:16]
        elif len(ts_str) >= 5 and ":" in ts_str:
            return ts_str[:5]
        return ts_str

    # Gather best per minute bucket
    bests_by_minute: Dict[str, float] = {}
    for source_dict in (p_mins, v_mins, c_mins):
        for ts, val in source_dict.items():
            m_bkt = to_minute_bucket(ts)
            if m_bkt not in bests_by_minute or val > bests_by_minute[m_bkt]:
                bests_by_minute[m_bkt] = val

    minute_bests = []
    if grouped_data_keys:
        # Preserve chronological order of distinct minute buckets
        distinct_minutes = []
        seen = set()
        for k in sorted(grouped_data_keys):
            m_bkt = to_minute_bucket(k)
            if m_bkt not in seen:
                seen.add(m_bkt)
                distinct_minutes.append(m_bkt)
        for m_bkt in distinct_minutes:
            if m_bkt in bests_by_minute:
                minute_bests.append(bests_by_minute[m_bkt])
    else:
        for m_bkt in sorted(bests_by_minute.keys()):
            minute_bests.append(bests_by_minute[m_bkt])

    if not minute_bests:
        return {
            "has_data": False,
            "bar_count": 0,
            "max": 0.0,
            "p90": 0.0,
            "avg": 0.0,
            "sd": 0.0,
            "near_be_cnt": 0,
            "near_be_pct": 0.0,
            "series": []
        }

    arr = np.array(minute_bests, dtype=float)
    bar_cnt = len(arr)
    max_val = float(np.max(arr))
    p90_val = float(np.percentile(arr, 90))
    avg_val = float(np.mean(arr))
    sd_val = float(np.std(arr, ddof=1)) if bar_cnt > 1 else 0.0
    near_be_cnt = int(np.sum(arr >= -0.25))
    near_be_pct = (near_be_cnt / bar_cnt * 100.0) if bar_cnt > 0 else 0.0

    return {
        "has_data": True,
        "bar_count": bar_cnt,
        "max": max_val,
        "p90": p90_val,
        "avg": avg_val,
        "sd": sd_val,
        "near_be_cnt": near_be_cnt,
        "near_be_pct": near_be_pct,
        "series": minute_bests
    }


def format_telegram_report(
    session_title: str,
    trade_date: str,
    profitable_opps: List[Dict[str, Any]],
    near_miss: Optional[Dict[str, Any]],
    thor_rate: float,
    dte: int,
    T_years: float,
    dropped_invalid_quotes: int,
    calc_error_count: int,
    valid_minutes: int,
    minute_stats: Optional[Dict[str, Any]] = None,
    strategy_nears: Optional[Dict[str, Any]] = None,
    thor_tenor: str = "",
    lifespan_stats: Optional[Dict[str, Any]] = None
) -> str:
    """Formats full session summary report for Telegram with market tightness stats and friction breakdown."""
    header = (
        f"⚡ <b>TFEX INTRADAY ARBITRAGE RADAR</b>\n"
        f"📅 <i>ข้อมูลวันที่ {trade_date} | รอบ {session_title}</i>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━"
    )

    if profitable_opps:
        ranked = sorted(profitable_opps, key=lambda x: x["net_pts"], reverse=True)
        top_3 = ranked[:3]

        parity_cnt = sum(1 for x in profitable_opps if "Conversion" in x["strategy"] or "Reversal" in x["strategy"])
        vertical_cnt = sum(1 for x in profitable_opps if "Monotonicity" in x["strategy"])
        calendar_cnt = sum(1 for x in profitable_opps if "Calendar" in x["strategy"])

        lifespan_str = ""
        if lifespan_stats and lifespan_stats.get("has_data"):
            l_avg = lifespan_stats["avg_sec"]
            l_min = lifespan_stats["min_sec"]
            l_max = lifespan_stats["max_sec"]
            l_cnt = lifespan_stats["count"]
            lifespan_str = f"• ⏱ <b>อายุโอกาสทำกำไร (Lifespan):</b> เฉลี่ย {l_avg:.0f} วิ | สั้นสุด {l_min:.0f} วิ | นานสุด {l_max:.0f} วิ (จาก {l_cnt} รอบที่เกิด)\n"

        summary_sec = (
            f"🎯 <b>พบโอกาส Arbitrage กำไรสุทธิ ({len(profitable_opps)} ครั้ง):</b>\n"
            f"• <b>Put-Call Parity:</b> {parity_cnt} ครั้ง | <b>Vertical:</b> {vertical_cnt} ครั้ง | <b>Calendar:</b> {calendar_cnt} ครั้ง\n"
            f"{lifespan_str}\n"
            f"🏆 <b>TOP {len(top_3)} โอกาสกำไรสูงสุด:</b>"
        )

        medals = ["🥇 [1]", "🥈 [2]", "🥉 [3]"]
        opp_blocks = []
        for idx, opp in enumerate(top_3):
            medal = medals[idx] if idx < len(medals) else f"• [{idx+1}]"
            gross_pts = opp["gross_pts"]
            comm_pts = opp["commission_pts"]
            net_pts = opp["net_pts"]
            net_thb_per_set = net_pts * CONTRACT_MULTIPLIER
            m_sets = opp["max_sets"]
            tot_thb = net_thb_per_set * m_sets if m_sets > 0 else net_thb_per_set

            action_checklist = "\n".join([f"    {format_action_leg(leg)}" for leg in opp["legs"]])
            legs_count = len(opp["legs"])
            stats_line = f"\n  📊 <b>สถิติ:</b> <i>{opp['stats']}</i>" if "stats" in opp else ""

            block = (
                f"{medal} <b>{opp['strategy']} | {opp['target']}</b> (<code>{opp['time']}</code>, {m_sets} ชุด)\n"
                f"{action_checklist}\n"
                f"  ➔ Gross: <code>+{gross_pts:.2f}</code> | คอม ({legs_count} ขา): <code>-{comm_pts:.2f}</code> | <b>Net: 🟢 +{net_pts:.2f} จุด (+{net_thb_per_set:,.0f} บ./ชุด) | รวม: 💰 +{tot_thb:,.0f} บาท</b>{stats_line}"
            )
            opp_blocks.append(block)

        status_block = summary_sec + "\n\n" + "\n\n".join(opp_blocks)
    else:
        status_block = (
            f"ℹ️ <b>ไม่พบช่องว่าง Arbitrage ที่คุ้มค่าคอม (0.40–0.60 จุด) ในรอบนี้</b>\n"
            f"<i>ตลาด SET50 Options & Futures มีประสิทธิภาพสูง (Efficient Market) สเปรดสะท้อนต้นทุนครบถ้วน</i>"
        )

    # Best-of-Minute Distribution Stats
    if minute_stats and minute_stats.get("has_data"):
        m_cnt = minute_stats["bar_count"]
        m_max = minute_stats["max"]
        m_p90 = minute_stats["p90"]
        m_avg = minute_stats["avg"]
        m_sd = minute_stats["sd"]
        m_near_cnt = minute_stats["near_be_cnt"]
        m_near_pct = minute_stats["near_be_pct"]
        dist_sec = (
            f"───────────────────────\n"
            f"📊 <b>พฤติกรรมความตึงของตลาด (Best-of-Minute Stats):</b>\n"
            f"• <b>สถิติ Net Profit สูงสุดรายนาที ({m_cnt} บาร์):</b>\n"
            f"  - <b>Max:</b> <code>{m_max:+.2f} จุด</code> | <b>P90:</b> <code>{m_p90:+.2f} จุด</code>\n"
            f"  - <b>Avg:</b> <code>{m_avg:+.2f} จุด</code> | <b>SD:</b> <code>{m_sd:.2f} จุด</code>\n"
            f"• <b>เข้าใกล้จุดคุ้มทุน (Net ≥ -0.25 จุด):</b> <code>{m_near_cnt}/{m_cnt} นาที ({m_near_pct:.1f}%)</code>"
        )
    else:
        dist_sec = ""

    # Strategy Breakdown (Near-Miss Top 1 per Strategy)
    if strategy_nears:
        np_item = strategy_nears.get("parity")
        nv_item = strategy_nears.get("vertical")
        nc_item = strategy_nears.get("calendar")

        if np_item:
            st_name = "Conversion" if "Conversion" in np_item["strategy"] else ("Reversal" if "Reversal" in np_item["strategy"] else np_item["strategy"])
            p_line = f"{st_name} {np_item.get('series', '')} Strike <code>{np_item.get('strike', 0):.0f}</code> (<code>{np_item['time']}</code>, <code>{np_item.get('max_sets', 0)}</code> ชุด) ➔ Net: <code>{np_item['net_pts']:+.2f} จุด</code>"
        else:
            p_line = "<i>N/A (ไม่มีสภาพคล่องครบขา)</i>"

        if nv_item:
            st_name = "Call Spread" if "Call" in nv_item["strategy"] else ("Put Spread" if "Put" in nv_item["strategy"] else nv_item["strategy"])
            v_line = f"{st_name} <code>{nv_item.get('strike_pair', '')}</code> ({nv_item.get('series', '')}) (<code>{nv_item['time']}</code>, <code>{nv_item.get('max_sets', 0)}</code> ชุด) ➔ Net: <code>{nv_item['net_pts']:+.2f} จุด</code>"
        else:
            v_line = "<i>N/A (ไม่มีสภาพคล่องครบขา)</i>"

        if nc_item:
            c_line = f"{nc_item.get('series', nc_item['target'])} (<code>{nc_item['time']}</code>, <code>{nc_item.get('max_sets', 0)}</code> ชุด) ➔ Net: <code>{nc_item['net_pts']:+.2f} จุด</code>"
        else:
            c_line = "<i>N/A (ไม่มีสภาพคล่องครบขา)</i>"

        strat_sec = (
            f"───────────────────────\n"
            f"🎯 <b>Near-Miss Top 1 รายกลยุทธ์ (คู่ที่ใกล้จุดคุ้มทุนที่สุด):</b>\n"
            f"• <b>Put-Call Parity:</b> {p_line}\n"
            f"• <b>Vertical Spread:</b> {v_line}\n"
            f"• <b>Calendar Spread:</b> {c_line}"
        )
    else:
        strat_sec = ""

    # Friction Breakdown of Top 1 Pair
    if near_miss:
        mid_edge = near_miss.get("mid_edge", 0.0)
        comm_pts = near_miss.get("commission_pts", 0.60)
        net_pts = near_miss.get("net_pts", 0.0)
        spread_cost = near_miss.get("spread_cost", abs(net_pts + comm_pts - mid_edge))
        legs_str = "\n".join([f"    {format_action_leg(leg)}" for leg in near_miss.get("legs", [])])

        if net_pts < 0:
            shortfall = abs(net_pts)
            net_desc = f"ขาดอีก {shortfall:.2f} จุด ถึงจุดคุ้มทุน"
        else:
            net_desc = f"กำไรสุทธิ +{net_pts:.2f} จุด (+{net_pts * CONTRACT_MULTIPLIER:,.0f} บ./ชุด)"

        friction_sec = (
            f"───────────────────────\n"
            f"🔬 <b>ผ่าโครงสร้างต้นทุนคู่ที่ดีที่สุดอันดับ 1 (Friction Breakdown):</b>\n"
            f"• <b>คู่ที่ดีที่สุด:</b> {near_miss['strategy']} | {near_miss['target']}\n"
            f"• <b>เวลา:</b> <code>{near_miss['time']}</code> | <b>ปริมาณรองรับ:</b> <b>{near_miss.get('max_sets', 0)}</b> ชุด\n"
            f"• <b>ราคาหน้ากระดาน (Strict Bid/Ask):</b>\n{legs_str}\n"
            f"• <b>Theoretical Edge (Mid-Price):</b> <code>{mid_edge:+.2f} จุด</code> <i>(ความเพี้ยนราคาก่อนหักค่าถ่าง)</i>\n"
            f"• <b>Spread Slippage Cost:</b> <code>-{spread_cost:.2f} จุด</code> <i>(ต้นทุนเคาะข้าม Bid-Ask)</i>\n"
            f"• <b>Commission Cost:</b> <code>-{comm_pts:.2f} จุด</code> <i>(ค่าคอมมิชชันไป-กลับ)</i>\n"
            f"➔ <b>Net Profit สุทธิ:</b> <code>{net_pts:+.2f} จุด</code> <i>({net_desc})</i>"
        )
    else:
        friction_sec = (
            f"───────────────────────\n"
            f"🔬 <b>ผ่าโครงสร้างต้นทุนคู่ที่ดีที่สุดอันดับ 1 (Friction Breakdown):</b>\n"
            f"• <i>N/A (ไม่มีคู่ที่มีสภาพคล่องสมบูรณ์ในเซสชันนี้)</i>"
        )

    # Data Audit (Condensed into 2 lines)
    tenor_str = f" ({thor_tenor})" if thor_tenor else ""
    audit_sec = (
        f"───────────────────────\n"
        f"🔍 <b>Data Audit:</b> ประมวลผล <code>{valid_minutes}</code> บาร์ | กรอง Bid/Ask ผิดปกติ: <code>{dropped_invalid_quotes:,}</code> | Error: <code>{calc_error_count}</code>\n"
        f"📌 <b>ตัวแปรอ้างอิง:</b> THOR{tenor_str} <code>{thor_rate*100.0:.2f}%</code> | DTE <code>{dte} วัน</code> (T = {T_years:.4f} ปี) | คอม 40 บ./สัญญา/ขา"
    )

    footer = (
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🤖 <i>TFEX Arbitrage Radar | คำนวณจาก Bid/Ask จริง และหักค่าคอมมิชชันครบทุกขา</i>"
    )

    body_parts = [header, status_block]
    if dist_sec:
        body_parts.append(dist_sec)
    if strat_sec:
        body_parts.append(strat_sec)
    if friction_sec:
        body_parts.append(friction_sec)
    body_parts.append(audit_sec)
    body_parts.append(footer)

    return "\n\n".join(body_parts)


# ==============================================================================
# 7. WORKER LOOP & DISPATCH CONTROLLER
# ==============================================================================

def run_arb_worker(session_mode: str, target_date: Optional[str] = None, no_telegram: bool = False,
                   allow_unverified: bool = False) -> None:
    """
    Persistent 1-Minute Real-time Session Worker:
    - Runs a continuous 1-minute loop synced with Turso (:06 offset / 3s polling).
    - Immediate Execution Principle: Strict Bid/Ask, volume-matched sets, full round-turn commissions.
    - Smart Anti-Spam: In-memory cooldown (15 min per key, bypassed on >= +0.20 pts profit spike).
    - End of session: Dispatches comprehensive session summary report to Telegram.
    """
    now_bkk = datetime.datetime.now(BANGKOK_TZ)
    trade_date = target_date or now_bkk.strftime("%Y-%m-%d")
    trade_date_obj = datetime.datetime.strptime(trade_date, "%Y-%m-%d").date()

    turso_url = os.environ.get("TURSO_DATABASE_URL")
    turso_token = os.environ.get("TURSO_AUTH_TOKEN")
    tg_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    tg_chat_id = os.environ.get("TELEGRAM_CHAT_ID")

    if not turso_url or not turso_token:
        print("[ERROR] TURSO_DATABASE_URL or TURSO_AUTH_TOKEN not configured in environment.")
        sys.exit(1)

    pipeline_url = f"{turso_url.replace('libsql://', 'https://').rstrip('/')}/v2/pipeline"

    # Concurrency Guard
    if is_arb_worker_active(pipeline_url, turso_token, session_mode, now_bkk):
        print(f"Another {session_mode} worker is already active. Exiting cleanly.")
        sys.exit(0)

    record_arb_heartbeat(pipeline_url, turso_token, session_mode, now_bkk)

    # Session boundaries
    if session_mode == "MORNING_WORKER":
        session_start_time = time(9, 45, 0)
        session_end_time = time(12, 30, 5)
        summary_start_str = "09:45:00"
        summary_end_str = "12:30:05"
        summary_title = "สรุปภาพรวมช่วงเช้า (09:45–12:30 น.)"
    elif session_mode == "AFTERNOON_WORKER":
        session_start_time = time(13, 45, 0)
        session_end_time = time(16, 55, 5)
        summary_start_str = "09:45:00"
        summary_end_str = "16:55:05"
        summary_title = "สรุปภาพรวมทั้งวัน (09:45–16:55 น.)"
    else:
        print(f"[ERROR] Invalid worker session: '{session_mode}'. Must be MORNING_WORKER or AFTERNOON_WORKER.")
        sys.exit(1)

    # Contract & Rate Validation
    cand_fut, front_series, next_series = resolve_active_series_candidates(now_bkk)
    expiry_dt = parse_series_expiry(front_series, trade_date_obj)
    dte = (expiry_dt.date() - trade_date_obj).days
    if dte < 0:
        print(f"[ERROR] Invalid DTE ({dte} days) for expired series {front_series}. Exiting.")
        sys.exit(1)
    elif dte == 0:
        # Expiry Day (0 DTE): Trading halts at 16:30:00 ICT for expiring series.
        close_today = BANGKOK_TZ.localize(datetime.datetime(trade_date_obj.year, trade_date_obj.month, trade_date_obj.day, 16, 30, 0))
        rem_seconds = (close_today - now_bkk).total_seconds()
        if rem_seconds <= 0:
            print(f"[INFO] Front series {front_series} trading ceased at 16:30:00 on expiry day {expiry_dt.date()}.")
            sys.exit(0)
        rem_hours = rem_seconds / 3600.0
        T_years = max(rem_hours, 0.1) / (365.0 * 24.0)
        if session_mode == "AFTERNOON_WORKER":
            session_end_time = time(16, 30, 5)
            summary_end_str = "16:30:05"
            summary_title = "สรุปภาพรวมวันหมดอายุ (09:45–16:30 น.)"
    else:
        T_years = dte / 365.0
    thor_record = fetch_thor_rates_record(pipeline_url, turso_token)
    thor_rate, thor_tenor = select_dynamic_thor_rate(thor_record, dte)

    print("=" * 75)
    print(f"TFEX REAL-TIME SUB-MINUTE ARBITRAGE WORKER STARTED: {session_mode}")
    print(f"Date: {trade_date} | Front: {front_series} (DTE: {dte}d) | THOR ({thor_tenor}): {thor_rate*100.0:.2f}%")
    print(f"Trading Hours: {session_start_time.strftime('%H:%M:%S')} – {session_end_time.strftime('%H:%M:%S')}")
    print("=" * 75)

    # Pre-market Standby
    while datetime.datetime.now(BANGKOK_TZ).time() < session_start_time:
        now_sb = datetime.datetime.now(BANGKOK_TZ)
        record_arb_heartbeat(pipeline_url, turso_token, session_mode, now_sb)
        target_start_dt = now_sb.replace(
            hour=session_start_time.hour,
            minute=session_start_time.minute,
            second=session_start_time.second,
            microsecond=0
        )
        wait_sec = (target_start_dt - now_sb).total_seconds()
        sleep_dur = min(15.0, max(1.0, wait_sec))
        print(f"[WORKER] Pre-market standby until {session_start_time.strftime('%H:%M:%S')} ({wait_sec:.0f}s remaining)...")
        pytime.sleep(sleep_dur)

    # Initial session dataset backfill
    curr_now = datetime.datetime.now(BANGKOK_TZ)
    curr_time_str = curr_now.strftime("%H:%M:%S")
    print(f"[WORKER] Initializing session dataset ({summary_start_str} to {curr_time_str})...")
    session_dataset, fut_cnt, opt_cnt = fetch_intraday_arb_dataset(
        pipeline_url=pipeline_url,
        auth_token=turso_token,
        trade_date=trade_date,
        start_time_bkk=summary_start_str,
        end_time_bkk=curr_time_str
    )
    last_scanned_ts = max(session_dataset.keys()) if session_dataset else None
    print(f"[WORKER] Initial dataset ready: {len(session_dataset)} bars loaded. Last bar: {last_scanned_ts}")

    # Seed running AS-OF continuous state
    if last_scanned_ts and last_scanned_ts in session_dataset:
        latest_seed = session_dataset[last_scanned_ts]
        running_spot: Optional[float] = latest_seed.get("spot")
        running_futures: Dict[str, Dict[str, Any]] = {k: dict(v) for k, v in latest_seed.get("futures", {}).items()}
        running_options: Dict[Tuple[str, float, str], Dict[str, Any]] = {k: dict(v) for k, v in latest_seed.get("options", {}).items()}
    else:
        running_spot = None
        running_futures = {}
        running_options = {}

    # Initialize Smart Anti-Spam Cooldown & Lifespan Tracker
    cooldown = ArbSmartCooldown(cooldown_seconds=900, profit_hurdle_pts=0.20)
    lifespan_tracker = OpportunityLifespanTracker()
    last_heartbeat_time = pytime.time()
    st_market_data = get_settrade_market_data()
    require_rest = not allow_unverified
    if st_market_data is None:
        if require_rest:
            print("[REST-GUARD] Settrade MarketData unavailable — live alerts will be SUPPRESSED "
                  "(fail-closed). Pass --allow-unverified to alert on DB quotes only.")
        else:
            print("[REST-GUARD] --allow-unverified set: alerts will be labelled DB-only.")

    # --------------------------------------------------------------------------
    # Real-time Sub-Minute Live Loop (Polls Turso every 5 seconds)
    # --------------------------------------------------------------------------
    bound_start = f"{trade_date} {summary_start_str}"
    bound_end = f"{trade_date} {summary_end_str}"
    sql_check = """
    SELECT MAX(ts) FROM (
        SELECT MAX(captured_at_bkk) AS ts FROM futures_snapshots WHERE captured_at_bkk >= ? AND captured_at_bkk <= ?
        UNION ALL
        SELECT MAX(captured_at_bkk) AS ts FROM options_snapshots WHERE captured_at_bkk >= ? AND captured_at_bkk <= ?
    );
    """

    while True:
        curr_bkk = datetime.datetime.now(BANGKOK_TZ)
        if curr_bkk.time() >= session_end_time:
            print(f"[WORKER] Reached session end time ({session_end_time.strftime('%H:%M:%S')}). Commencing session summary...")
            break

        # Record heartbeat periodically every ~60 seconds
        if pytime.time() - last_heartbeat_time >= 60:
            record_arb_heartbeat(pipeline_url, turso_token, session_mode, curr_bkk)
            last_heartbeat_time = pytime.time()

        # Check for newly written event in Turso (Futures or Options)
        new_ts_found = None
        try:
            _, check_rows = execute_turso_sql(pipeline_url, turso_token, sql_check, [bound_start, bound_end, bound_start, bound_end])
            turso_max_ts = str(check_rows[0][0]) if check_rows and check_rows[0][0] else None
        except Exception as e:
            print(f"[POLL] Error checking Turso max timestamp: {e}")
            turso_max_ts = None

        if turso_max_ts and (last_scanned_ts is None or turso_max_ts > last_scanned_ts):
            new_ts_found = turso_max_ts

        if new_ts_found:
            delta_start = last_scanned_ts if last_scanned_ts else bound_start

            # Fetch delta spot
            sql_delta_spot = "SELECT captured_at_bkk, last FROM spot_snapshots WHERE captured_at_bkk > ? AND captured_at_bkk <= ? ORDER BY captured_at_bkk ASC;"
            try:
                _, rows_spot = execute_turso_sql(pipeline_url, turso_token, sql_delta_spot, [delta_start, new_ts_found])
                for r in rows_spot:
                    if r[1] is not None:
                        running_spot = float(r[1])
            except Exception as e:
                print(f"[DELTA] Error fetching spot delta: {e}")

            # Fetch delta futures
            sql_delta_fut_depth = "SELECT captured_at_bkk, symbol, bid, ask, last, volume, open_interest, bid_vol, ask_vol FROM futures_snapshots WHERE captured_at_bkk > ? AND captured_at_bkk <= ? ORDER BY captured_at_bkk ASC;"
            try:
                _, rows_fut = execute_turso_sql(pipeline_url, turso_token, sql_delta_fut_depth, [delta_start, new_ts_found])
                has_fut_depth = True
            except Exception:
                sql_delta_fut_fb = "SELECT captured_at_bkk, symbol, bid, ask, last, volume, open_interest FROM futures_snapshots WHERE captured_at_bkk > ? AND captured_at_bkk <= ? ORDER BY captured_at_bkk ASC;"
                try:
                    _, rows_fut = execute_turso_sql(pipeline_url, turso_token, sql_delta_fut_fb, [delta_start, new_ts_found])
                except Exception as e:
                    print(f"[DELTA] Error fetching futures delta: {e}")
                    rows_fut = []
                has_fut_depth = False

            for r in rows_fut:
                sym = str(r[1])
                bid, ask, last, vol, oi = r[2], r[3], r[4], r[5], r[6]
                bid_vol = float(r[7]) if (has_fut_depth and len(r) > 7 and r[7] is not None) else 0.0
                ask_vol = float(r[8]) if (has_fut_depth and len(r) > 8 and r[8] is not None) else 0.0
                running_futures[sym] = {
                    "symbol": sym,
                    "bid": float(bid) if bid is not None else None,
                    "ask": float(ask) if ask is not None else None,
                    "bid_vol": bid_vol,
                    "ask_vol": ask_vol,
                    "last": float(last) if last is not None else None,
                    "volume": float(vol) if vol is not None else 0.0,
                    "oi": float(oi) if oi is not None else 0.0,
                    "last_update_ts": str(r[0])
                }

            # Fetch delta options
            sql_delta_opt_depth = "SELECT captured_at_bkk, symbol, series, strike, option_type, bid, ask, last, volume, open_interest, bid_vol, ask_vol FROM options_snapshots WHERE captured_at_bkk > ? AND captured_at_bkk <= ? ORDER BY captured_at_bkk ASC;"
            try:
                _, rows_opt = execute_turso_sql(pipeline_url, turso_token, sql_delta_opt_depth, [delta_start, new_ts_found])
                has_opt_depth = True
            except Exception:
                sql_delta_opt_fb = "SELECT captured_at_bkk, symbol, series, strike, option_type, bid, ask, last, volume, open_interest FROM options_snapshots WHERE captured_at_bkk > ? AND captured_at_bkk <= ? ORDER BY captured_at_bkk ASC;"
                try:
                    _, rows_opt = execute_turso_sql(pipeline_url, turso_token, sql_delta_opt_fb, [delta_start, new_ts_found])
                except Exception as e:
                    print(f"[DELTA] Error fetching options delta: {e}")
                    rows_opt = []
                has_opt_depth = False

            for r in rows_opt:
                sym, series, strike, otype = r[1], r[2], r[3], r[4]
                bid, ask, last, vol, oi = r[5], r[6], r[7], r[8], r[9]
                bid_vol = float(r[10]) if (has_opt_depth and len(r) > 10 and r[10] is not None) else 0.0
                ask_vol = float(r[11]) if (has_opt_depth and len(r) > 11 and r[11] is not None) else 0.0
                key = (str(series), float(strike), str(otype).upper())
                running_options[key] = {
                    "symbol": str(sym),
                    "series": str(series),
                    "strike": float(strike),
                    "option_type": str(otype).upper(),
                    "bid": float(bid) if bid is not None else None,
                    "ask": float(ask) if ask is not None else None,
                    "bid_vol": bid_vol,
                    "ask_vol": ask_vol,
                    "last": float(last) if last is not None else None,
                    "volume": float(vol) if vol is not None else 0.0,
                    "oi": float(oi) if oi is not None else 0.0,
                    "last_update_ts": str(r[0])
                }

            # Strict Staleness Guard: Prune quotes older than 15 minutes (900 seconds)
            try:
                curr_dt = datetime.datetime.strptime(new_ts_found[:19], "%Y-%m-%d %H:%M:%S")
                for sym in list(running_futures.keys()):
                    u_ts = running_futures[sym].get("last_update_ts")
                    if u_ts:
                        try:
                            u_dt = datetime.datetime.strptime(u_ts[:19], "%Y-%m-%d %H:%M:%S")
                            if (curr_dt - u_dt).total_seconds() > QUOTE_STALENESS_LIVE_SEC:
                                del running_futures[sym]
                        except Exception:
                            pass

                for key in list(running_options.keys()):
                    u_ts = running_options[key].get("last_update_ts")
                    if u_ts:
                        try:
                            u_dt = datetime.datetime.strptime(u_ts[:19], "%Y-%m-%d %H:%M:%S")
                            if (curr_dt - u_dt).total_seconds() > QUOTE_STALENESS_LIVE_SEC:
                                del running_options[key]
                        except Exception:
                            pass
            except Exception:
                pass

            # Snapshot AS-OF state
            current_snapshot = {
                "spot": running_spot,
                "futures": {k: dict(v) for k, v in running_futures.items()},
                "options": {k: dict(v) for k, v in running_options.items()}
            }
            session_dataset[new_ts_found] = current_snapshot
            last_scanned_ts = new_ts_found

            # Scan opportunities
            single_bar = {new_ts_found: current_snapshot}
            p_opps, _, d1, e1, _ = scan_put_call_parity(single_bar, front_series, thor_rate, T_years)
            v_opps, _, d2, e2, _ = scan_vertical_spreads(single_bar, front_series)
            c_opps, _, d3, e3, _ = scan_calendar_spreads(session_dataset, front_series, next_series)
            c_curr = [c for c in c_opps if c.get("time") == new_ts_found]

            bar_all_opps = p_opps + v_opps + c_curr
            lifespan_tracker.update(bar_all_opps, new_ts_found)
            bar_time_str = new_ts_found[11:19]
            print(f"[{bar_time_str}] Event Scanned ({new_ts_found}). Found {len(bar_all_opps)} profitable opps (Parity={len(p_opps)}, Vert={len(v_opps)}, Cal={len(c_curr)}).")

            if bar_all_opps:
                if not is_executable_market_hours(curr_bkk):
                    print(f"[MARKET-HOURS] Outside executable trading hours ({curr_bkk.strftime('%H:%M:%S')}). Suppressing LIVE Telegram alert ({len(bar_all_opps)} opps held).")
                else:
                    alertable = cooldown.filter_alertable(bar_all_opps, curr_bkk, auto_record=False)
                    if alertable:
                        ranked_alertable = sorted(alertable, key=lambda x: x.get("net_pts", 0.0), reverse=True)

                        # Pre-Alert REST Double-Check Gate.
                        # W1: เดิม verify เฉพาะอันดับ #1 แต่ส่ง alert ทุกอันดับ ที่เหลือจึง
                        # ออกไปด้วยราคา DB ที่เก่าได้ถึง 15 นาที โดยไม่มีป้ายบอก
                        # แก้: verify อันดับต้น ๆ ทุกตัว (สูงสุด MAX_REST_VERIFIED_OPPS)
                        # และติดป้าย DB-only ให้ตัวที่ยังไม่ได้ verify
                        verified_ranked: List[Dict[str, Any]] = []
                        for cand in ranked_alertable[:MAX_REST_VERIFIED_OPPS]:
                            ok, upd = verify_live_quotes_before_alert(
                                opp=cand,
                                market_data=st_market_data,
                                running_futures=running_futures,
                                running_options=running_options,
                                thor_rate=thor_rate,
                                T_years=T_years,
                                require_rest=require_rest
                            )
                            if ok:
                                verified_ranked.append(upd)
                            else:
                                print(f"[REST-GUARD] Filtered ghost/stale opportunity ({cand.get('strategy')} - {cand.get('target')}).")

                        if not verified_ranked:
                            print(f"[REST-GUARD] No candidate survived REST verification ({len(ranked_alertable)} suppressed). Suppressing LIVE Telegram alert.")
                        else:
                            tail = [
                                dict(o, rest_verified=bool(o.get("rest_verified")))
                                for o in ranked_alertable[MAX_REST_VERIFIED_OPPS:]
                            ]
                            for o in tail:
                                o["rest_verified"] = False
                            ranked_alertable = sorted(
                                verified_ranked + tail,
                                key=lambda x: x.get("net_pts", 0.0),
                                reverse=True
                            )
                            top_1_opp = ranked_alertable[0]
                            print(f"[TELEGRAM] 🚨 Dispatching LIVE Event Alert for {len(ranked_alertable)} opportunities...")
                            live_msg = format_live_1min_alert(
                                trade_date=trade_date,
                                bar_ts=bar_time_str,
                                profitable_opps=ranked_alertable,
                                thor_rate=thor_rate,
                                dte=dte,
                                T_years=T_years,
                                thor_tenor=thor_tenor
                            )
                            if not no_telegram and tg_token and tg_chat_id:
                                send_telegram_alert(tg_token, tg_chat_id, live_msg)
                                # Record alert cooldown only after REST verification succeeds and alert is dispatched
                                for opp_alerted in ranked_alertable:
                                    cooldown.record_alert(opp_alerted, curr_bkk)

                                # Generate & attach Settrade Streaming Multiple Order Excel file for Top 1 pure arbitrage opportunity
                                if "Calendar" not in top_1_opp.get("strategy", "") and top_1_opp.get("rest_verified"):
                                    try:
                                        xlsx_bytes, xlsx_filename = generate_streaming_multi_order_xlsx(top_1_opp)
                                        doc_caption = (
                                            f"📥 <b>Streaming Multi-Order (Top 1):</b> <code>{top_1_opp['strategy']}</code> | "
                                            f"<code>{top_1_opp['target']}</code> (Net: <b>+{top_1_opp['net_pts']:.2f} pts</b>)\n"
                                            f"⚡ <i>นำเข้าเมนู Multiple Order บน Settrade Streaming เพื่อส่งคำสั่งพร้อมกัน</i>"
                                        )
                                        send_telegram_document(tg_token, tg_chat_id, xlsx_bytes, xlsx_filename, caption=doc_caption)
                                    except Exception as e:
                                        print(f"[EXCEL] Error generating/sending Streaming Multi-Order xlsx: {e}")
                    else:
                        print(f"[COOLDOWN] {len(bar_all_opps)} opps detected but held in anti-spam cooldown.")

        # Event-driven polling interval: 5 seconds
        pytime.sleep(5)

    # Finalize any ongoing opportunities in tracker
    lifespan_tracker.finalize(datetime.datetime.now(BANGKOK_TZ))
    lifespan_stats = lifespan_tracker.get_stats()

    # --------------------------------------------------------------------------
    # Final Session Summary Execution
    # --------------------------------------------------------------------------
    print("\n" + "=" * 75)
    print(f"GENERATING FINAL SESSION SUMMARY: {summary_title}")
    print("=" * 75)

    grouped_data, fut_cnt, opt_cnt = fetch_intraday_arb_dataset(
        pipeline_url=pipeline_url,
        auth_token=turso_token,
        trade_date=trade_date,
        start_time_bkk=summary_start_str,
        end_time_bkk=summary_end_str
    )

    if fut_cnt == 0 or opt_cnt == 0 or len(grouped_data) == 0:
        abort_msg = (
            f"⚠️ <b>[ABORT] ไม่พบข้อมูล Intraday ใน Turso สำหรับวันที่ {trade_date} ({summary_title}) — งดการคำนวณ</b>\n\n"
            f"• <b>ตาราง Futures:</b> {fut_cnt} แถว\n"
            f"• <b>ตาราง Options:</b> {opt_cnt} แถว\n"
            f"• <b>ช่วงเวลาที่ค้นหา:</b> {summary_start_str} ถึง {summary_end_str} น."
        )
        print(f"[ABORT] Zero records for session summary.")
        if not no_telegram and tg_token and tg_chat_id:
            send_telegram_alert(tg_token, tg_chat_id, abort_msg)
        sys.exit(1)

    p_opps, near_p, d1, e1, p_mins = scan_put_call_parity(grouped_data, front_series, thor_rate, T_years)
    v_opps, near_v, d2, e2, v_mins = scan_vertical_spreads(grouped_data, front_series)
    c_opps, near_c, d3, e3, c_mins = scan_calendar_spreads(grouped_data, front_series, next_series)

    all_opps = p_opps + v_opps + c_opps
    all_nears = [n for n in [near_p, near_v, near_c] if n is not None]
    best_near = max(all_nears, key=lambda x: x["net_pts"]) if all_nears else None

    minute_stats = compute_best_of_minute_stats(
        grouped_data_keys=list(grouped_data.keys()),
        p_mins=p_mins,
        v_mins=v_mins,
        c_mins=c_mins
    )

    strategy_nears = {
        "parity": near_p,
        "vertical": near_v,
        "calendar": near_c
    }

    summary_msg = format_telegram_report(
        session_title=summary_title,
        trade_date=trade_date,
        profitable_opps=all_opps,
        near_miss=best_near,
        thor_rate=thor_rate,
        dte=dte,
        T_years=T_years,
        dropped_invalid_quotes=(d1 + d2 + d3),
        calc_error_count=(e1 + e2 + e3),
        valid_minutes=len(grouped_data),
        minute_stats=minute_stats,
        strategy_nears=strategy_nears,
        thor_tenor=thor_tenor,
        lifespan_stats=lifespan_stats
    )

    print("\n" + "=" * 75)
    print("SESSION SUMMARY NOTIFICATION:")
    print("=" * 75)
    print(summary_msg)
    print("=" * 75)

    if not no_telegram and tg_token and tg_chat_id:
        send_telegram_alert(tg_token, tg_chat_id, summary_msg)
    else:
        print("[TELEGRAM] --no-telegram supplied or credentials missing. Summary dispatch skipped.")

    print(f"=== {session_mode} WORKER COMPLETED SUCCESSFULLY ===")
    sys.exit(0)


def run_single_scan(session_name: str, target_date: Optional[str] = None, no_telegram: bool = False) -> None:
    """Single-shot scan for MIDDAY (09:45-12:35) or EVENING (09:45-17:05)."""
    now_bkk = datetime.datetime.now(BANGKOK_TZ)
    trade_date = target_date or now_bkk.strftime("%Y-%m-%d")
    trade_date_obj = datetime.datetime.strptime(trade_date, "%Y-%m-%d").date()

    turso_url = os.environ.get("TURSO_DATABASE_URL")
    turso_token = os.environ.get("TURSO_AUTH_TOKEN")
    tg_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    tg_chat_id = os.environ.get("TELEGRAM_CHAT_ID")

    if not turso_url or not turso_token:
        print("[ERROR] TURSO_DATABASE_URL or TURSO_AUTH_TOKEN not configured in environment.")
        sys.exit(1)

    pipeline_url = f"{turso_url.replace('libsql://', 'https://').rstrip('/')}/v2/pipeline"

    sess_upper = session_name.upper()
    if sess_upper == "MIDDAY":
        t_start_bkk = "09:45:00"
        t_end_bkk = "12:30:05"
        sess_title = "พักเที่ยง (09:45–12:30 น.)"
    elif sess_upper == "EVENING":
        t_start_bkk = "09:45:00"
        t_end_bkk = "16:55:05"
        sess_title = "ปิดตลาดเย็น (09:45–16:55 น.)"
    else:
        print(f"[ERROR] Unsupported single scan session: '{session_name}'. Expected 'MIDDAY' or 'EVENING'.")
        sys.exit(1)

    cand_fut, front_series, next_series = resolve_active_series_candidates(now_bkk)
    expiry_dt = parse_series_expiry(front_series, trade_date_obj)
    dte = (expiry_dt.date() - trade_date_obj).days
    if dte < 0:
        print(f"[ERROR] Invalid DTE ({dte} days) for expired series {front_series}. Exiting.")
        sys.exit(1)
    elif dte == 0:
        close_today = BANGKOK_TZ.localize(datetime.datetime(trade_date_obj.year, trade_date_obj.month, trade_date_obj.day, 16, 30, 0))
        rem_seconds = (close_today - now_bkk).total_seconds()
        rem_hours = max(rem_seconds, 360.0) / 3600.0
        T_years = max(rem_hours, 0.1) / (365.0 * 24.0)
        if sess_upper == "EVENING":
            t_end_bkk = "16:30:05"
            sess_title = "ปิดตลาดวันหมดอายุ (09:45–16:30 น.)"
    else:
        T_years = dte / 365.0
    thor_record = fetch_thor_rates_record(pipeline_url, turso_token)
    thor_rate, thor_tenor = select_dynamic_thor_rate(thor_record, dte)

    grouped_data, fut_cnt, opt_cnt = fetch_intraday_arb_dataset(
        pipeline_url=pipeline_url,
        auth_token=turso_token,
        trade_date=trade_date,
        start_time_bkk=t_start_bkk,
        end_time_bkk=t_end_bkk
    )

    if fut_cnt == 0 or opt_cnt == 0 or len(grouped_data) == 0:
        abort_msg = (
            f"⚠️ <b>[ABORT] ไม่พบข้อมูล Intraday ใน Turso สำหรับวันที่ {trade_date} ({sess_title}) — งดการคำนวณ</b>\n\n"
            f"• <b>ตาราง Futures:</b> {fut_cnt} แถว\n"
            f"• <b>ตาราง Options:</b> {opt_cnt} แถว\n"
            f"• <b>ช่วงเวลาที่ค้นหา:</b> {t_start_bkk} ถึง {t_end_bkk} น."
        )
        print(f"[ABORT] Zero records for session {sess_title}.")
        if not no_telegram and tg_token and tg_chat_id:
            send_telegram_alert(tg_token, tg_chat_id, abort_msg)
        sys.exit(1)

    p_opps, near_p, d1, e1, p_mins = scan_put_call_parity(grouped_data, front_series, thor_rate, T_years)
    v_opps, near_v, d2, e2, v_mins = scan_vertical_spreads(grouped_data, front_series)
    c_opps, near_c, d3, e3, c_mins = scan_calendar_spreads(grouped_data, front_series, next_series)

    all_opps = p_opps + v_opps + c_opps
    all_nears = [n for n in [near_p, near_v, near_c] if n is not None]
    best_near = max(all_nears, key=lambda x: x["net_pts"]) if all_nears else None

    minute_stats = compute_best_of_minute_stats(
        grouped_data_keys=list(grouped_data.keys()),
        p_mins=p_mins,
        v_mins=v_mins,
        c_mins=c_mins
    )

    strategy_nears = {
        "parity": near_p,
        "vertical": near_v,
        "calendar": near_c
    }

    report_msg = format_telegram_report(
        session_title=sess_title,
        trade_date=trade_date,
        profitable_opps=all_opps,
        near_miss=best_near,
        thor_rate=thor_rate,
        dte=dte,
        T_years=T_years,
        dropped_invalid_quotes=(d1 + d2 + d3),
        calc_error_count=(e1 + e2 + e3),
        valid_minutes=len(grouped_data),
        minute_stats=minute_stats,
        strategy_nears=strategy_nears,
        thor_tenor=thor_tenor,
        lifespan_stats=None
    )

    print("\n" + "=" * 75)
    print("SESSION REPORT NOTIFICATION:")
    print("=" * 75)
    print(report_msg)
    print("=" * 75)

    if not no_telegram and tg_token and tg_chat_id:
        send_telegram_alert(tg_token, tg_chat_id, report_msg)
    else:
        print("[TELEGRAM] --no-telegram supplied or credentials missing. Dispatch skipped.")

    sys.exit(0)


# ==============================================================================
# ENTRY POINT
# ==============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TFEX Intraday Arbitrage Radar")
    parser.add_argument(
        "--session",
        type=str.upper,
        choices=["MIDDAY", "EVENING", "MORNING_WORKER", "AFTERNOON_WORKER"],
        required=True,
        help="Execution session mode (MIDDAY, EVENING, MORNING_WORKER, AFTERNOON_WORKER)"
    )
    parser.add_argument("--date", type=str, default=None, help="Target trade date (YYYY-MM-DD, default today)")
    parser.add_argument("--no-telegram", action="store_true", help="Skip sending Telegram alert")
    parser.add_argument(
        "--allow-unverified",
        action="store_true",
        help="ยอมให้แจ้งเตือนด้วยราคาจาก DB โดยไม่ผ่าน Settrade REST verification "
             "(alert จะติดป้าย DB-only); ค่าเริ่มต้นคือ fail-closed"
    )
    args = parser.parse_args()

    if args.session in ["MORNING_WORKER", "AFTERNOON_WORKER"]:
        run_arb_worker(session_mode=args.session, target_date=args.date,
                       no_telegram=args.no_telegram, allow_unverified=args.allow_unverified)
    elif args.session in ["MIDDAY", "EVENING"]:
        run_single_scan(session_name=args.session, target_date=args.date, no_telegram=args.no_telegram)
    else:
        print(f"[ERROR] Invalid session: {args.session}. Must be one of MIDDAY, EVENING, MORNING_WORKER, AFTERNOON_WORKER.")
        sys.exit(1)
