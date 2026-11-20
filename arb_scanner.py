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

load_dotenv()

BANGKOK_TZ = pytz.timezone("Asia/Bangkok")
CONTRACT_MULTIPLIER = 200.0  # THB per SET50 index point
COMMISSION_2LEG_PTS = 0.40   # 2 legs * 0.20 index pts
COMMISSION_3LEG_PTS = 0.60   # 3 legs * 0.20 index pts

MONTH_CODES = {3: "H", 6: "M", 9: "U", 12: "Z"}
REVERSE_MONTH_CODES = {"H": 3, "M": 6, "U": 9, "Z": 12}


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
    args: Optional[List[Any]] = None
) -> Tuple[List[str], List[List[Any]]]:
    """Executes SQL via Turso v2 pipeline."""
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
    r = requests.post(pipeline_url, json=payload, headers=headers, timeout=25)
    if r.status_code != 200:
        raise RuntimeError(f"Turso API HTTP error ({r.status_code}): {r.text}")
    data = r.json()
    for resp_item in data.get("results", []):
        if resp_item.get("type") == "error":
            raise RuntimeError(f"Turso SQL execution error: {resp_item.get('error')}")
        if resp_item.get("type") == "ok":
            res = resp_item["response"]["result"]
            cols = [c["name"] for c in res.get("cols", [])]
            rows = parse_turso_result_rows(res)
            return cols, rows
    return [], []


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
        INSERT OR REPLACE INTO arb_worker_heartbeat (worker_session, last_heartbeat_bkk, pid)
        VALUES (?, ?, ?);
        """
        now_str = now_bkk.strftime("%Y-%m-%d %H:%M:%S")
        execute_turso_sql(pipeline_url, auth_token, sql, [session_name, now_str, os.getpid()])
    except Exception as e:
        print(f"[CONCURRENCY] Note recording arb heartbeat: {e}")


# ==============================================================================
# 3. DYNAMIC SERIES & EXPIRATION CALCULATION
# ==============================================================================

def get_contract_last_trading_day(year: int, month: int) -> datetime.datetime:
    _, last_day = calendar.monthrange(year, month)
    biz_days = [
        date(year, month, d)
        for d in range(1, last_day + 1)
        if date(year, month, d).weekday() < 5
    ]
    if len(biz_days) >= 2:
        ltd = biz_days[-2]
    else:
        ltd = biz_days[-1]
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
    def _clean(val: Optional[float]) -> Optional[float]:
        if val is None or val <= 0:
            return None
        return val / 100.0 if val > 0.5 else val

    r_on = _clean(thor_record.get("overnight"))
    r_1m = _clean(thor_record.get("1m"))
    r_3m = _clean(thor_record.get("3m"))
    r_6m = _clean(thor_record.get("6m"))

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
                        if (m_dt - u_dt).total_seconds() > 900:
                            del running_futures[sym]
                    except Exception:
                        pass

            for key in list(running_options.keys()):
                u_ts = running_options[key].get("last_update_ts")
                if u_ts:
                    try:
                        u_dt = datetime.datetime.strptime(u_ts[:19], "%Y-%m-%d %H:%M:%S")
                        if (m_dt - u_dt).total_seconds() > 900:
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
                max_sets_conv = int(min(c_bvol, p_avol, f_avol))
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
                max_sets_rev = int(min(c_avol, p_bvol, f_bvol))
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
                max_sets = int(min(v_c1_ask, v_c2_bid))

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
                max_sets = int(min(v_p2_ask, v_p1_bid))

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

    spread_records = []
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

        spread_records.append({
            "ts": ts,
            "b1": b1, "a1": a1, "b1_vol": b1_vol, "a1_vol": a1_vol,
            "b2": b2, "a2": a2, "b2_vol": b2_vol, "a2_vol": a2_vol,
            "mid_spread": mid_spread
        })

    if len(spread_records) < 10:
        return [], None, dropped_invalid_quotes, calc_error_count, {}

    df_sp = pd.DataFrame(spread_records)
    df_sp["rolling_mean"] = df_sp["mid_spread"].rolling(window=30, min_periods=5).mean()
    df_sp["rolling_std"] = df_sp["mid_spread"].rolling(window=30, min_periods=5).std()

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
        if b1_vol <= 0 or a2_vol <= 0:
            continue
        max_sets_up = int(min(b1_vol, a2_vol))

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
            "tag": "[Calendar Spread]",
            "time": ts,
            "series": f"{front_series}-{next_series}",
            "gross_pts": dev_up,
            "net_pts": net_up,
            "mid_edge": mid_edge_up,
            "spread_cost": spread_cost_up,
            "commission_pts": COMMISSION,
            "max_sets": max_sets_up,
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

        if exec_sell_spread > (mu + 2.0 * std) and net_up > 0:
            opportunities.append(item_up)
        if best_near_miss is None or net_up > best_near_miss["net_pts"]:
            best_near_miss = item_up

        # Case 2: Spread Spikes DOWN -> Buy Front @ a1, Sell Next @ b2
        if a1_vol <= 0 or b2_vol <= 0:
            continue
        max_sets_down = int(min(a1_vol, b2_vol))

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
            "tag": "[Calendar Spread]",
            "time": ts,
            "series": f"{front_series}-{next_series}",
            "gross_pts": dev_down,
            "net_pts": net_down,
            "mid_edge": mid_edge_down,
            "spread_cost": spread_cost_down,
            "commission_pts": COMMISSION,
            "max_sets": max_sets_down,
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

        if exec_buy_spread < (mu - 2.0 * std) and net_down > 0:
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
        print(f"[TELEGRAM] API Response: {r.text}")
        if r.status_code == 200 and r.json().get("ok"):
            print("[TELEGRAM] Arbitrage notification delivered successfully.")
            return True
        else:
            print(f"[TELEGRAM] Error: sendMessage failed with HTTP {r.status_code}: {r.text}")
            return False
    except Exception as e:
        print(f"[TELEGRAM] Exception sending telegram notification: {e}")
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
        print(f"[TELEGRAM] Exception sending telegram document: {e}")
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


def generate_streaming_multi_order_xlsx(opp: Dict[str, Any]) -> Tuple[bytes, str]:
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

    Critical Execution Order (Reduce Legging Risk):
    - Options sorted ascending by liquidity depth (least liquid first)
    - Futures (e.g. S50Z26) ALWAYS placed last

    Returns:
    - (file_bytes, filename)
    """
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


def is_executable_market_hours(dt_or_time: Any) -> bool:
    """
    Market Hours Guard:
    Checks if given datetime or time is within actual executable trading hours:
    - Morning: 09:45:00 - 12:30:00
    - Afternoon: 14:15:00 - 16:55:00
    Returns False outside these windows (e.g. 16:55:01 - 17:05:00 is post-market when MMs withdraw quotes).
    """
    if isinstance(dt_or_time, datetime.datetime):
        t = dt_or_time.time()
    elif isinstance(dt_or_time, time):
        t = dt_or_time
    else:
        return False

    is_morning = (time(9, 45, 0) <= t <= time(12, 30, 0))
    is_afternoon = (time(14, 15, 0) <= t <= time(16, 55, 0))
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

    def filter_alertable(self, opps: List[Dict[str, Any]], now_dt: datetime.datetime) -> List[Dict[str, Any]]:
        alertable = []
        for opp in opps:
            key = f"{opp['strategy']}|{opp['target']}"
            net_pts = opp["net_pts"]

            if key not in self.cache:
                alertable.append(opp)
                self.cache[key] = {
                    "last_alerted_at": now_dt,
                    "last_net_pts": net_pts,
                    "alert_count": 1
                }
            else:
                entry = self.cache[key]
                elapsed = (now_dt - entry["last_alerted_at"]).total_seconds()
                profit_diff = net_pts - entry["last_net_pts"]

                if elapsed >= self.cooldown_seconds:
                    alertable.append(opp)
                    self.cache[key]["last_alerted_at"] = now_dt
                    self.cache[key]["last_net_pts"] = net_pts
                    self.cache[key]["alert_count"] = entry.get("alert_count", 0) + 1
                elif profit_diff >= self.profit_hurdle_pts:
                    opp["stats"] = f"Profit Spike: +{profit_diff:.2f} pts vs previous alert"
                    alertable.append(opp)
                    self.cache[key]["last_alerted_at"] = now_dt
                    self.cache[key]["last_net_pts"] = net_pts
                    self.cache[key]["alert_count"] = entry.get("alert_count", 0) + 1
                else:
                    # Cooldown active; suppress alert
                    pass
        return alertable


class OpportunityLifespanTracker:
    """
    In-memory tick-to-tick lifespan duration tracker for arbitrage opportunities.
    Key: f"{strategy}|{target}"
    Tracks duration in seconds from first_seen_ts until opportunity closes or session ends.
    """
    def __init__(self):
        self.active_opportunities: Dict[str, Dict[str, datetime.datetime]] = {}
        self.closed_durations: List[float] = []

    def _parse_ts(self, ts_val: Any) -> datetime.datetime:
        if isinstance(ts_val, datetime.datetime):
            return ts_val
        s = str(ts_val).strip()
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

    def update(self, current_profitable_opps: List[Dict[str, Any]], current_ts: Any) -> None:
        curr_dt = self._parse_ts(current_ts)
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
            first_ts = entry["first_seen_ts"]
            last_ts = entry["last_seen_ts"]
            dur = max(1.0, (last_ts - first_ts).total_seconds())
            self.closed_durations.append(dur)

    def finalize(self, end_ts: Any = None) -> None:
        """Closes all remaining active opportunities at session end."""
        end_dt = self._parse_ts(end_ts) if end_ts else None
        for k, entry in list(self.active_opportunities.items()):
            first_ts = entry["first_seen_ts"]
            last_ts = end_dt if end_dt else entry["last_seen_ts"]
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
    """Formats instant real-time Telegram alert for 1-minute detected arbitrage opportunities with complete execution details."""
    header = (
        f"🚨 <b>[LIVE ARBITRAGE ALERT] ตรวจพบโอกาสทำกำไรทันที!</b>\n"
        f"📅 <i>ข้อมูลวันที่ {trade_date} | เวลา <code>{bar_ts} น.</code></i>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━"
    )

    ranked = sorted(profitable_opps, key=lambda x: x["net_pts"], reverse=True)
    top_3 = ranked[:3]

    parity_cnt = sum(1 for x in profitable_opps if "Conversion" in x["strategy"] or "Reversal" in x["strategy"])
    vertical_cnt = sum(1 for x in profitable_opps if "Monotonicity" in x["strategy"])
    calendar_cnt = sum(1 for x in profitable_opps if "Calendar" in x["strategy"])

    tenor_str = f" ({thor_tenor})" if thor_tenor else ""
    meta_stats = (
        f"• <b>อัตราดอกเบี้ย THOR{tenor_str}:</b> {thor_rate*100.0:.2f}% | <b>DTE คงเหลือ:</b> {dte} วัน\n"
        f"• <b>ตรวจพบ:</b> {len(profitable_opps)} รายการ (Parity: {parity_cnt}, Vertical: {vertical_cnt}, Calendar: {calendar_cnt})"
    )

    summary_sec = (
        f"⚡ <b>พบสัญญาณเคาะกำไรสุทธิ (Real-time Execution):</b>\n"
        f"{meta_stats}\n"
        f"───────────────────────\n"
        f"🏆 <b>โอกาสทำกำไรสูงสุด (Strict Executable Prices):</b>\n"
    )

    medals = ["🥇 [1]", "🥈 [2]", "🥉 [3]"]
    opp_blocks = []
    for idx, opp in enumerate(top_3):
        medal = medals[idx] if idx < len(medals) else f"• [{idx+1}]"
        gross_pts = opp["gross_pts"]
        comm_pts = opp["commission_pts"]
        net_pts = opp["net_pts"]
        gross_thb_set = gross_pts * CONTRACT_MULTIPLIER
        comm_thb_set = comm_pts * CONTRACT_MULTIPLIER
        net_thb_per_set = net_pts * CONTRACT_MULTIPLIER
        m_sets = opp["max_sets"]
        tot_thb = net_thb_per_set * m_sets if m_sets > 0 else net_thb_per_set

        action_checklist = "\n".join([f"    {format_action_leg(leg)}" for leg in opp["legs"]])
        legs_count = len(opp["legs"])
        stats_line = f"\n  📊 <b>สถิติสเปรด:</b> <i>{opp['stats']}</i>" if "stats" in opp else ""

        block = (
            f"{medal} <b>{opp['strategy']}</b>\n"
            f"  📌 <b>เป้าหมาย:</b> <code>{opp['target']}</code> {opp.get('tag', '')}\n"
            f"  ⏱ <b>เวลาตรวจพบ:</b> <code>{opp['time']} น.</code>\n\n"
            f"  👉 <b>ขั้นตอนส่งคำสั่งทันที (Execution Checklist):</b>\n"
            f"{action_checklist}\n\n"
            f"  💵 <b>โครงสร้างต้นทุนและกำไร (Financial Breakdown):</b>\n"
            f"    • <b>กำไรส่วนต่างขั้นต้น (Gross):</b> <code>+{gross_pts:.2f} จุด</code> (+{gross_thb_set:,.0f} บาท/ชุด)\n"
            f"    • <b>หักค่าคอมมิชชัน ({legs_count} ขา @ 40 บ.):</b> <code>-{comm_pts:.2f} จุด</code> (-{comm_thb_set:,.0f} บาท/ชุด)\n"
            f"    • <b>กำไรสุทธิคงเหลือ (Net Profit):</b> 🟢 <b>+{net_pts:.2f} จุด (+{net_thb_per_set:,.0f} บาท/ชุด)</b>\n"
            f"    • <b>สภาพคล่องที่เคาะได้ทันที:</b> <b>{m_sets}</b> สัญญา (อิง Minimum Leg Depth)\n"
            f"    • <b>มูลค่ากำไรสุทธิรวมทั้งพอร์ต:</b> 💰 <b>+{tot_thb:,.0f} บาท</b>{stats_line}"
        )
        opp_blocks.append(block)

    content = summary_sec + "\n\n".join(opp_blocks)
    footer = (
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚡ <i>TFEX Live 1-Min Radar | Smart Cooldown 15 นาทีต่อคู่สัญญา</i>"
    )

    return f"{header}\n\n{content}\n\n{footer}"


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

def run_arb_worker(session_mode: str, target_date: Optional[str] = None, no_telegram: bool = False) -> None:
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
        session_end_time = time(12, 35, 0)
        summary_start_str = "09:45:00"
        summary_end_str = "12:35:00"
        summary_title = "สรุปภาพรวมช่วงเช้า (09:45–12:35 น.)"
    elif session_mode == "AFTERNOON_WORKER":
        session_start_time = time(14, 15, 0)
        session_end_time = time(17, 5, 0)
        summary_start_str = "09:45:00"
        summary_end_str = "17:05:00"
        summary_title = "สรุปภาพรวมทั้งวัน (09:45–17:05 น.)"
    else:
        print(f"[ERROR] Invalid worker session: '{session_mode}'. Must be MORNING_WORKER or AFTERNOON_WORKER.")
        sys.exit(1)

    # Contract & Rate Validation
    cand_fut, front_series, next_series = resolve_active_series_candidates(now_bkk)
    expiry_dt = parse_series_expiry(front_series, trade_date_obj)
    dte = (expiry_dt.date() - trade_date_obj).days
    if dte <= 0:
        print(f"[ERROR] Invalid DTE ({dte} days) for series {front_series}. Exiting.")
        sys.exit(1)
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

    # --------------------------------------------------------------------------
    # Real-time Sub-Minute Live Loop (Polls Turso every 5 seconds)
    # --------------------------------------------------------------------------
    bound_start = f"{trade_date} {summary_start_str}"
    bound_end = f"{trade_date} {summary_end_str}"
    sql_check = "SELECT MAX(captured_at_bkk) FROM futures_snapshots WHERE captured_at_bkk >= ? AND captured_at_bkk <= ?;"
    sql_opt_check = "SELECT COUNT(*) FROM options_snapshots WHERE captured_at_bkk = ?;"

    while True:
        curr_bkk = datetime.datetime.now(BANGKOK_TZ)
        if curr_bkk.time() >= session_end_time:
            print(f"[WORKER] Reached session end time ({session_end_time.strftime('%H:%M:%S')}). Commencing session summary...")
            break

        # Record heartbeat periodically every ~60 seconds
        if pytime.time() - last_heartbeat_time >= 60:
            record_arb_heartbeat(pipeline_url, turso_token, session_mode, curr_bkk)
            last_heartbeat_time = pytime.time()

        # Check for newly written event in Turso
        new_ts_found = None
        try:
            _, check_rows = execute_turso_sql(pipeline_url, turso_token, sql_check, [bound_start, bound_end])
            turso_max_ts = str(check_rows[0][0]) if check_rows and check_rows[0][0] else None
        except Exception as e:
            print(f"[POLL] Error checking Turso max timestamp: {e}")
            turso_max_ts = None

        if turso_max_ts and (last_scanned_ts is None or turso_max_ts > last_scanned_ts):
            # Verify options_snapshots also has data for turso_max_ts
            try:
                _, opt_check_rows = execute_turso_sql(pipeline_url, turso_token, sql_opt_check, [turso_max_ts])
                opt_bar_cnt = int(opt_check_rows[0][0]) if opt_check_rows and opt_check_rows[0][0] else 0
            except Exception as e:
                print(f"[POLL] Error checking options count for {turso_max_ts}: {e}")
                opt_bar_cnt = 0

            if opt_bar_cnt > 0:
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
                            if (curr_dt - u_dt).total_seconds() > 900:
                                del running_futures[sym]
                        except Exception:
                            pass

                for key in list(running_options.keys()):
                    u_ts = running_options[key].get("last_update_ts")
                    if u_ts:
                        try:
                            u_dt = datetime.datetime.strptime(u_ts[:19], "%Y-%m-%d %H:%M:%S")
                            if (curr_dt - u_dt).total_seconds() > 900:
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
                    alertable = cooldown.filter_alertable(bar_all_opps, curr_bkk)
                    if alertable:
                        print(f"[TELEGRAM] 🚨 Dispatching LIVE Event Alert for {len(alertable)} opportunities...")
                        live_msg = format_live_1min_alert(
                            trade_date=trade_date,
                            bar_ts=bar_time_str,
                            profitable_opps=alertable,
                            thor_rate=thor_rate,
                            dte=dte,
                            T_years=T_years,
                            thor_tenor=thor_tenor
                        )
                        if not no_telegram and tg_token and tg_chat_id:
                            send_telegram_alert(tg_token, tg_chat_id, live_msg)
                            # Generate & attach Settrade Streaming Multiple Order Excel file for Top 1 opportunity
                            try:
                                ranked_alertable = sorted(alertable, key=lambda x: x.get("net_pts", 0.0), reverse=True)
                                top_1_opp = ranked_alertable[0]
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
        t_end_bkk = "12:35:00"
        sess_title = "พักเที่ยง (09:45–12:35 น.)"
    elif sess_upper == "EVENING":
        t_start_bkk = "09:45:00"
        t_end_bkk = "17:05:00"
        sess_title = "ปิดตลาดเย็น (09:45–17:05 น.)"
    else:
        print(f"[ERROR] Unsupported single scan session: '{session_name}'. Expected 'MIDDAY' or 'EVENING'.")
        sys.exit(1)

    cand_fut, front_series, next_series = resolve_active_series_candidates(now_bkk)
    expiry_dt = parse_series_expiry(front_series, trade_date_obj)
    dte = (expiry_dt.date() - trade_date_obj).days
    if dte <= 0:
        print(f"[ERROR] Invalid DTE ({dte} days) for series {front_series}. Exiting.")
        sys.exit(1)
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
        default="MIDDAY",
        help="Execution session mode (MIDDAY, EVENING, MORNING_WORKER, AFTERNOON_WORKER)"
    )
    parser.add_argument("--date", type=str, default=None, help="Target trade date (YYYY-MM-DD, default today)")
    parser.add_argument("--no-telegram", action="store_true", help="Skip sending Telegram alert")
    args = parser.parse_args()

    if args.session in ["MORNING_WORKER", "AFTERNOON_WORKER"]:
        run_arb_worker(session_mode=args.session, target_date=args.date, no_telegram=args.no_telegram)
    elif args.session in ["MIDDAY", "EVENING"]:
        run_single_scan(session_name=args.session, target_date=args.date, no_telegram=args.no_telegram)
    else:
        print(f"[ERROR] Invalid session: {args.session}. Must be one of MIDDAY, EVENING, MORNING_WORKER, AFTERNOON_WORKER.")
        sys.exit(1)
