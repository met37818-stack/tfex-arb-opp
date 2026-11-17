"""
TFEX Intraday Arbitrage Radar (arb_scanner.py)
==============================================
Scans high-frequency intraday snapshots from Turso Cloud SQLite to discover
real executable arbitrage opportunities across:
1. Put-Call Parity (Black-76 Futures Options: Conversion & Reversal)
2. Vertical Spread Monotonicity Violations (Call & Put)
3. Calendar Spread Spikes (Front vs Next Quarter Futures)

Modes:
- Real-time Worker (--session MORNING_WORKER / AFTERNOON_WORKER):
  Runs a 15-minute pulse loop (:00, :15, :30, :45 + 10s offset).
  Anti-Spam: Alerts to Telegram ONLY when net profit > 0 is detected.
  At session end (12:35 / 17:05): Dispatches full session summary report to Telegram.
- Single-Shot (--session MIDDAY / EVENING):
  Runs full session scan and immediately outputs summary report.

Strict Fail-Loud / No Silent Fallback Rules:
- Real Turso Data Only: Never generate synthetic/mock quotes.
- Abort on 0 Rows: If Turso has 0 futures or options rows, send Telegram abort alert and exit.
- Strict THOR Rate: Must exist in eod_thor_rates; fail loud if missing/NULL/<=0.
- Strict DTE / T_years: Must calculate from actual series expiry; fail loud if DTE <= 0.
- Strict Bid/Ask: Never use 'last' in place of bid/ask. Drop any row with bid<=0, ask<=0,
  vol<=0, or ask<bid, and report dropped_invalid_quotes count.
- Commission Hurdle:
  - 2-leg strategies (Vertical / Calendar): 0.40 pts (40 THB/leg round-turn = 80 THB).
  - 3-leg strategies (Conversion / Reversal): 0.60 pts (120 THB).
- Multiplier: 200 THB/point.
"""

import os
import sys
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

def fetch_thor_rate(pipeline_url: str, auth_token: str) -> float:
    sql_thor = "SELECT thor_rate FROM eod_thor_rates ORDER BY trade_date DESC LIMIT 1;"
    _, rows_thor = execute_turso_sql(pipeline_url, auth_token, sql_thor)
    if not rows_thor or rows_thor[0][0] is None:
        raise ValueError("No record found in eod_thor_rates table or value is NULL.")
    
    val = float(rows_thor[0][0])
    if val <= 0:
        raise ValueError(f"Invalid THOR rate in eod_thor_rates: {val} (must be > 0)")
    
    rate = val / 100.0 if val > 0.5 else val
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

    sql_fut = """
    SELECT captured_at_bkk, symbol, bid, ask, last, volume, open_interest 
    FROM futures_snapshots 
    WHERE captured_at_bkk >= ? AND captured_at_bkk <= ? 
    ORDER BY captured_at_bkk ASC;
    """
    _, rows_fut = execute_turso_sql(pipeline_url, auth_token, sql_fut, [ts_start, ts_end])

    sql_opt = """
    SELECT captured_at_bkk, symbol, series, strike, option_type, bid, ask, last, volume, open_interest 
    FROM options_snapshots 
    WHERE captured_at_bkk >= ? AND captured_at_bkk <= ? 
    ORDER BY captured_at_bkk ASC;
    """
    _, rows_opt = execute_turso_sql(pipeline_url, auth_token, sql_opt, [ts_start, ts_end])

    grouped: Dict[str, Dict[str, Any]] = {}

    for r in rows_spot:
        ts = str(r[0])
        if ts not in grouped:
            grouped[ts] = {"spot": None, "futures": {}, "options": {}}
        if r[1] is not None:
            grouped[ts]["spot"] = float(r[1])

    for r in rows_fut:
        ts, sym, bid, ask, last, vol, oi = r[0], r[1], r[2], r[3], r[4], r[5], r[6]
        if ts not in grouped:
            grouped[ts] = {"spot": None, "futures": {}, "options": {}}
        grouped[ts]["futures"][sym] = {
            "symbol": sym,
            "bid": float(bid) if bid is not None else None,
            "ask": float(ask) if ask is not None else None,
            "last": float(last) if last is not None else None,
            "volume": float(vol) if vol is not None else 0.0,
            "oi": float(oi) if oi is not None else 0.0
        }

    for r in rows_opt:
        ts, sym, series, strike, otype, bid, ask, last, vol, oi = r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9]
        if ts not in grouped:
            grouped[ts] = {"spot": None, "futures": {}, "options": {}}
        key = (series, float(strike), str(otype).upper())
        grouped[ts]["options"][key] = {
            "symbol": sym,
            "series": series,
            "strike": float(strike),
            "option_type": str(otype).upper(),
            "bid": float(bid) if bid is not None else None,
            "ask": float(ask) if ask is not None else None,
            "last": float(last) if last is not None else None,
            "volume": float(vol) if vol is not None else 0.0,
            "oi": float(oi) if oi is not None else 0.0
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
) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]], int, int]:
    COMMISSION = COMMISSION_3LEG_PTS
    fut_sym = f"S50{front_series}"
    disc_factor = math.exp(-thor_rate * T_years)

    opportunities: List[Dict[str, Any]] = []
    best_near_miss: Optional[Dict[str, Any]] = None
    dropped_invalid_quotes = 0
    calc_error_count = 0

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
            f_vol <= 0 or f_ask < f_bid):
            dropped_invalid_quotes += 1
            continue

        ref_price = spot_ref or ((f_bid + f_ask) / 2.0)

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
                c_vol <= 0 or c_ask < c_bid):
                dropped_invalid_quotes += 1
                continue
            if (p_bid is None or p_ask is None or p_bid <= 0 or p_ask <= 0 or
                p_vol <= 0 or p_ask < p_bid):
                dropped_invalid_quotes += 1
                continue

            max_sets = int(min(c_vol, p_vol, f_vol))
            liq_tag = "[Liquid: Spot±30]" if abs(k - ref_price) <= 30.0 else "[Deep Strike: ถือจนหมดอายุ]"

            # 1. Conversion: (Call_Bid - Put_Ask) - (Fut_Ask - K) * exp(-r * T)
            try:
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
                item_conv = {
                    "strategy": "Conversion (Sell C, Buy P, Buy Fut)",
                    "target": f"Strike {k:.0f} & {fut_sym}",
                    "tag": liq_tag,
                    "time": ts,
                    "strike": k,
                    "gross_pts": gross_conv,
                    "net_pts": net_conv,
                    "commission_pts": COMMISSION,
                    "max_sets": max_sets,
                    "legs": [
                        f"Sell Call {k:.0f} @ Bid <code>{c_bid:.2f}</code> (Vol: {c_vol:,.0f})",
                        f"Buy Put {k:.0f} @ Ask <code>{p_ask:.2f}</code> (Vol: {p_vol:,.0f})",
                        f"Buy Futures {fut_sym} @ Ask <code>{f_ask:.2f}</code> (Vol: {f_vol:,.0f})"
                    ]
                }
                if net_conv > 0:
                    opportunities.append(item_conv)
                else:
                    if best_near_miss is None or net_conv > best_near_miss["net_pts"]:
                        best_near_miss = item_conv

            # 2. Reversal: (Fut_Bid - K) * exp(-r * T) - (Call_Ask - Put_Bid)
            try:
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
                item_rev = {
                    "strategy": "Reversal (Buy C, Sell P, Sell Fut)",
                    "target": f"Strike {k:.0f} & {fut_sym}",
                    "tag": liq_tag,
                    "time": ts,
                    "strike": k,
                    "gross_pts": gross_rev,
                    "net_pts": net_rev,
                    "commission_pts": COMMISSION,
                    "max_sets": max_sets,
                    "legs": [
                        f"Buy Call {k:.0f} @ Ask <code>{c_ask:.2f}</code> (Vol: {c_vol:,.0f})",
                        f"Sell Put {k:.0f} @ Bid <code>{p_bid:.2f}</code> (Vol: {p_vol:,.0f})",
                        f"Sell Futures {fut_sym} @ Bid <code>{f_bid:.2f}</code> (Vol: {f_vol:,.0f})"
                    ]
                }
                if net_rev > 0:
                    opportunities.append(item_rev)
                else:
                    if best_near_miss is None or net_rev > best_near_miss["net_pts"]:
                        best_near_miss = item_rev

    return opportunities, best_near_miss, dropped_invalid_quotes, calc_error_count


def scan_vertical_spreads(
    grouped_data: Dict[str, Dict[str, Any]],
    front_series: str
) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]], int, int]:
    COMMISSION = COMMISSION_2LEG_PTS
    opportunities: List[Dict[str, Any]] = []
    best_near_miss: Optional[Dict[str, Any]] = None
    dropped_invalid_quotes = 0
    calc_error_count = 0

    for ts in sorted(grouped_data.keys()):
        frame = grouped_data[ts]
        options = frame["options"]

        # Call Monotonicity
        calls = []
        for ((ser, k, otype), opt) in options.items():
            if ser == front_series and otype == "C":
                bid, ask, vol = opt.get("bid"), opt.get("ask"), opt.get("volume", 0.0)
                if (bid is None or ask is None or bid <= 0 or ask <= 0 or
                    vol <= 0 or ask < bid):
                    dropped_invalid_quotes += 1
                else:
                    calls.append(opt)

        calls.sort(key=lambda x: x["strike"])

        for i in range(len(calls)):
            for j in range(i + 1, len(calls)):
                c1, c2 = calls[i], calls[j]
                k1, k2 = c1["strike"], c2["strike"]
                gross = c2["bid"] - c1["ask"]
                if math.isnan(gross) or math.isinf(gross):
                    calc_error_count += 1
                    continue
                net = gross - COMMISSION
                max_sets = int(min(c1.get("volume", 0.0), c2.get("volume", 0.0)))

                item = {
                    "strategy": "Call Monotonicity Violation",
                    "target": f"Call {k1:.0f}/{k2:.0f} ({front_series})",
                    "tag": "[Vertical Spread]",
                    "time": ts,
                    "gross_pts": gross,
                    "net_pts": net,
                    "commission_pts": COMMISSION,
                    "max_sets": max_sets,
                    "legs": [
                        f"Buy Call {k1:.0f} @ Ask <code>{c1['ask']:.2f}</code> (Vol: {c1.get('volume', 0):,.0f})",
                        f"Sell Call {k2:.0f} @ Bid <code>{c2['bid']:.2f}</code> (Vol: {c2.get('volume', 0):,.0f})"
                    ]
                }
                if net > 0:
                    opportunities.append(item)
                else:
                    if best_near_miss is None or net > best_near_miss["net_pts"]:
                        best_near_miss = item

        # Put Monotonicity
        puts = []
        for ((ser, k, otype), opt) in options.items():
            if ser == front_series and otype == "P":
                bid, ask, vol = opt.get("bid"), opt.get("ask"), opt.get("volume", 0.0)
                if (bid is None or ask is None or bid <= 0 or ask <= 0 or
                    vol <= 0 or ask < bid):
                    dropped_invalid_quotes += 1
                else:
                    puts.append(opt)

        puts.sort(key=lambda x: x["strike"])

        for i in range(len(puts)):
            for j in range(i + 1, len(puts)):
                p1, p2 = puts[i], puts[j]
                k1, k2 = p1["strike"], p2["strike"]
                gross = p1["bid"] - p2["ask"]
                if math.isnan(gross) or math.isinf(gross):
                    calc_error_count += 1
                    continue
                net = gross - COMMISSION
                max_sets = int(min(p1.get("volume", 0.0), p2.get("volume", 0.0)))

                item = {
                    "strategy": "Put Monotonicity Violation",
                    "target": f"Put {k1:.0f}/{k2:.0f} ({front_series})",
                    "tag": "[Vertical Spread]",
                    "time": ts,
                    "gross_pts": gross,
                    "net_pts": net,
                    "commission_pts": COMMISSION,
                    "max_sets": max_sets,
                    "legs": [
                        f"Buy Put {k2:.0f} @ Ask <code>{p2['ask']:.2f}</code> (Vol: {p2.get('volume', 0):,.0f})",
                        f"Sell Put {k1:.0f} @ Bid <code>{p1['bid']:.2f}</code> (Vol: {p1.get('volume', 0):,.0f})"
                    ]
                }
                if net > 0:
                    opportunities.append(item)
                else:
                    if best_near_miss is None or net > best_near_miss["net_pts"]:
                        best_near_miss = item

    return opportunities, best_near_miss, dropped_invalid_quotes, calc_error_count


def scan_calendar_spreads(
    grouped_data: Dict[str, Dict[str, Any]],
    front_series: str,
    next_series: str
) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]], int, int]:
    COMMISSION = COMMISSION_2LEG_PTS
    f1_sym = f"S50{front_series}"
    f2_sym = f"S50{next_series}"
    dropped_invalid_quotes = 0
    calc_error_count = 0

    spread_records = []
    for ts in sorted(grouped_data.keys()):
        frame = grouped_data[ts]
        f1 = frame["futures"].get(f1_sym)
        f2 = frame["futures"].get(f2_sym)
        if not f1 or not f2:
            continue

        b1, a1, v1 = f1.get("bid"), f1.get("ask"), f1.get("volume", 0.0)
        b2, a2, v2 = f2.get("bid"), f2.get("ask"), f2.get("volume", 0.0)

        if (b1 is None or a1 is None or b1 <= 0 or a1 <= 0 or v1 <= 0 or a1 < b1 or
            b2 is None or a2 is None or b2 <= 0 or a2 <= 0 or v2 <= 0 or a2 < b2):
            dropped_invalid_quotes += 1
            continue

        mid1 = (b1 + a1) / 2.0
        mid2 = (b2 + a2) / 2.0
        mid_spread = mid1 - mid2

        spread_records.append({
            "ts": ts,
            "b1": b1, "a1": a1, "v1": v1,
            "b2": b2, "a2": a2, "v2": v2,
            "mid_spread": mid_spread
        })

    if len(spread_records) < 10:
        return [], None, dropped_invalid_quotes, calc_error_count

    df_sp = pd.DataFrame(spread_records)
    df_sp["rolling_mean"] = df_sp["mid_spread"].rolling(window=30, min_periods=5).mean()
    df_sp["rolling_std"] = df_sp["mid_spread"].rolling(window=30, min_periods=5).std().fillna(0.15)
    df_sp["rolling_std"] = df_sp["rolling_std"].clip(lower=0.10)

    opportunities: List[Dict[str, Any]] = []
    best_near_miss: Optional[Dict[str, Any]] = None

    for _, row in df_sp.iterrows():
        ts = row["ts"]
        b1, a1, v1 = row["b1"], row["a1"], row["v1"]
        b2, a2, v2 = row["b2"], row["a2"], row["v2"]
        mu = row["rolling_mean"]
        std = row["rolling_std"]
        max_sets = int(min(v1, v2))

        # Case 1: Spread Spikes UP -> Sell Front @ b1, Buy Next @ a2
        exec_sell_spread = b1 - a2
        dev_up = exec_sell_spread - mu
        if math.isnan(dev_up) or math.isinf(dev_up):
            calc_error_count += 1
            continue
        net_up = dev_up - COMMISSION

        item_up = {
            "strategy": "Calendar Spread Spike (Overextended High)",
            "target": f"{f1_sym} - {f2_sym}",
            "tag": "[Calendar Spread]",
            "time": ts,
            "gross_pts": dev_up,
            "net_pts": net_up,
            "commission_pts": COMMISSION,
            "max_sets": max_sets,
            "legs": [
                f"Sell Front {f1_sym} @ Bid <code>{b1:.2f}</code> (Vol: {v1:,.0f})",
                f"Buy Next {f2_sym} @ Ask <code>{a2:.2f}</code> (Vol: {v2:,.0f})"
            ],
            "stats": f"Spread: {exec_sell_spread:+.2f} vs Baseline: {mu:+.2f} (+{dev_up:.2f} pts / +{(dev_up/std):.1f}σ)"
        }

        if exec_sell_spread > (mu + 2.0 * std) and net_up > 0:
            opportunities.append(item_up)
        else:
            if best_near_miss is None or net_up > best_near_miss["net_pts"]:
                best_near_miss = item_up

        # Case 2: Spread Spikes DOWN -> Buy Front @ a1, Sell Next @ b2
        exec_buy_spread = a1 - b2
        dev_down = mu - exec_buy_spread
        if math.isnan(dev_down) or math.isinf(dev_down):
            calc_error_count += 1
            continue
        net_down = dev_down - COMMISSION

        item_down = {
            "strategy": "Calendar Spread Spike (Overextended Low)",
            "target": f"{f1_sym} - {f2_sym}",
            "tag": "[Calendar Spread]",
            "time": ts,
            "gross_pts": dev_down,
            "net_pts": net_down,
            "commission_pts": COMMISSION,
            "max_sets": max_sets,
            "legs": [
                f"Buy Front {f1_sym} @ Ask <code>{a1:.2f}</code> (Vol: {v1:,.0f})",
                f"Sell Next {f2_sym} @ Bid <code>{b2:.2f}</code> (Vol: {v2:,.0f})"
            ],
            "stats": f"Spread: {exec_buy_spread:+.2f} vs Baseline: {mu:+.2f} (+{dev_down:.2f} pts / +{(dev_down/std):.1f}σ)"
        }

        if exec_buy_spread < (mu - 2.0 * std) and net_down > 0:
            opportunities.append(item_down)
        else:
            if best_near_miss is None or net_down > best_near_miss["net_pts"]:
                best_near_miss = item_down

    return opportunities, best_near_miss, dropped_invalid_quotes, calc_error_count


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


def format_15min_pulse_alert(
    trade_date: str,
    window_start: str,
    window_end: str,
    profitable_opps: List[Dict[str, Any]],
    thor_rate: float,
    dte: int,
    T_years: float,
    dropped_invalid_quotes: int,
    calc_error_count: int,
    valid_minutes: int
) -> str:
    """Formats 15-minute real-time alert for Telegram (only when net profit > 0)."""
    header = (
        f"🚨 <b>[15-MIN ARB ALERT] พบโอกาส Arbitrage ระหว่างวัน!</b>\n"
        f"📅 <i>ข้อมูลวันที่ {trade_date} | สแกนช่วง {window_start}–{window_end} น.</i>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━"
    )

    ranked = sorted(profitable_opps, key=lambda x: x["net_pts"], reverse=True)
    top_3 = ranked[:3]

    parity_cnt = sum(1 for x in profitable_opps if "Conversion" in x["strategy"] or "Reversal" in x["strategy"])
    vertical_cnt = sum(1 for x in profitable_opps if "Monotonicity" in x["strategy"])
    calendar_cnt = sum(1 for x in profitable_opps if "Calendar" in x["strategy"])

    meta_stats = (
        f"• <b>บาร์ข้อมูลที่ประมวลผล:</b> {valid_minutes} นาที\n"
        f"• <b>กรองข้อมูลไม่สมบูรณ์:</b> {dropped_invalid_quotes:,} รายการ\n"
        f"• <b>อัตราดอกเบี้ย THOR:</b> {thor_rate*100.0:.2f}% | <b>DTE:</b> {dte} วัน"
    )

    summary_sec = (
        f"🎯 <b>สรุปโอกาสที่พบ ({len(profitable_opps)} ครั้ง ใน 15 นาทีล่าสุด):</b>\n"
        f"• <b>Put-Call Parity (Black-76):</b> {parity_cnt} ครั้ง\n"
        f"• <b>Vertical Spread:</b> {vertical_cnt} ครั้ง\n"
        f"• <b>Calendar Spread Spike:</b> {calendar_cnt} ครั้ง\n"
        f"{meta_stats}\n"
        f"───────────────────────\n"
        f"🏆 <b>โอกาสกำไรสุทธิสูงสุด:</b>\n"
    )

    medals = ["🥇 [1]", "🥈 [2]", "🥉 [3]"]
    opp_blocks = []
    for idx, opp in enumerate(top_3):
        medal = medals[idx] if idx < len(medals) else f"• [{idx+1}]"
        net_pts = opp["net_pts"]
        net_thb_per_set = net_pts * CONTRACT_MULTIPLIER
        m_sets = opp["max_sets"]
        tot_thb = net_thb_per_set * m_sets if m_sets > 0 else net_thb_per_set

        legs_str = "\n".join([f"    - {leg}" for leg in opp["legs"]])
        stats_line = f"\n  • <b>สถิติ:</b> <i>{opp['stats']}</i>" if "stats" in opp else ""

        block = (
            f"{medal} <b>{opp['strategy']} | {opp['target']} {opp.get('tag', '')}</b>\n"
            f"  • <b>เวลาที่เกิด:</b> <code>{opp['time']}</code>\n"
            f"  • <b>ขาการเทรด (Strict Bid/Ask):</b>\n{legs_str}\n"
            f"  • <b>ปริมาณที่เปิดได้ (Executable):</b> <b>{m_sets}</b> ชุด (min leg vol){stats_line}\n"
            f"  • <b>กำไรสุทธิต่อชุด:</b> 🟢 <b>+{net_pts:.2f} จุด</b> (+{net_thb_per_set:,.0f} บาท/ชุด)\n"
            f"  • <b>มูลค่ากำไรรวม:</b> <b>+{tot_thb:,.0f} บาท</b> (หักค่าคอม {opp['commission_pts']:.2f} จุดแล้ว)"
        )
        opp_blocks.append(block)

    content = summary_sec + "\n\n".join(opp_blocks)
    footer = (
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🤖 <i>TFEX Arbitrage Real-time Pulse | แจ้งเตือนเฉพาะเมื่อพบ Net Profit > 0</i>"
    )

    return f"{header}\n\n{content}\n\n{footer}"


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
    valid_minutes: int
) -> str:
    """Formats full session summary report for Telegram (always delivered)."""
    header = (
        f"⚡ <b>TFEX INTRADAY ARBITRAGE RADAR</b>\n"
        f"📅 <i>ข้อมูลวันที่ {trade_date} | รอบ {session_title}</i>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━"
    )

    meta_stats = (
        f"• <b>บาร์ข้อมูลที่ประมวลผล:</b> {valid_minutes} นาที\n"
        f"• <b>กรองข้อมูลไม่สมบูรณ์ (Bid/Ask/Vol ≤ 0):</b> {dropped_invalid_quotes:,} รายการ\n"
        f"• <b>ข้อผิดพลาดในการคำนวณ (NaN/Inf):</b> {calc_error_count} รายการ\n"
        f"• <b>อัตราดอกเบี้ย THOR อ้างอิง:</b> {thor_rate*100.0:.2f}% <i>(จาก eod_thor_rates)</i>\n"
        f"• <b>อายุคงเหลือ (DTE):</b> {dte} วัน (T = {T_years:.4f} ปี)\n"
    )

    if profitable_opps:
        ranked = sorted(profitable_opps, key=lambda x: x["net_pts"], reverse=True)
        top_3 = ranked[:3]

        parity_cnt = sum(1 for x in profitable_opps if "Conversion" in x["strategy"] or "Reversal" in x["strategy"])
        vertical_cnt = sum(1 for x in profitable_opps if "Monotonicity" in x["strategy"])
        calendar_cnt = sum(1 for x in profitable_opps if "Calendar" in x["strategy"])

        summary_sec = (
            f"🎯 <b>สรุปโอกาส Arbitrage ที่พบ ({len(profitable_opps)} ครั้ง):</b>\n"
            f"• <b>Put-Call Parity (Black-76):</b> {parity_cnt} ครั้ง\n"
            f"• <b>Vertical Spread:</b> {vertical_cnt} ครั้ง\n"
            f"• <b>Calendar Spread Spike:</b> {calendar_cnt} ครั้ง\n"
            f"{meta_stats}"
            f"───────────────────────\n"
            f"🏆 <b>TOP {len(top_3)} โอกาสกำไรสุทธิสูงสุด:</b>\n"
        )

        medals = ["🥇 [1]", "🥈 [2]", "🥉 [3]"]
        opp_blocks = []
        for idx, opp in enumerate(top_3):
            medal = medals[idx] if idx < len(medals) else f"• [{idx+1}]"
            net_pts = opp["net_pts"]
            net_thb_per_set = net_pts * CONTRACT_MULTIPLIER
            m_sets = opp["max_sets"]
            tot_thb = net_thb_per_set * m_sets if m_sets > 0 else net_thb_per_set

            legs_str = "\n".join([f"    - {leg}" for leg in opp["legs"]])
            stats_line = f"\n  • <b>สถิติ:</b> <i>{opp['stats']}</i>" if "stats" in opp else ""

            block = (
                f"{medal} <b>{opp['strategy']} | {opp['target']} {opp.get('tag', '')}</b>\n"
                f"  • <b>เวลาที่เกิด:</b> <code>{opp['time']}</code>\n"
                f"  • <b>ขาการเทรด (Strict Bid/Ask):</b>\n{legs_str}\n"
                f"  • <b>ปริมาณที่เปิดได้ (Executable):</b> <b>{m_sets}</b> ชุด (min leg vol){stats_line}\n"
                f"  • <b>กำไรสุทธิต่อชุด:</b> 🟢 <b>+{net_pts:.2f} จุด</b> (+{net_thb_per_set:,.0f} บาท/ชุด)\n"
                f"  • <b>มูลค่ากำไรรวม:</b> <b>+{tot_thb:,.0f} บาท</b> (หักค่าคอม {opp['commission_pts']:.2f} จุดแล้ว)"
            )
            opp_blocks.append(block)

        content = summary_sec + "\n\n".join(opp_blocks)
    else:
        near_text = ""
        if near_miss:
            shortfall = abs(near_miss["net_pts"])
            legs_str = "\n".join([f"  - {leg}" for leg in near_miss["legs"]])
            near_text = (
                f"───────────────────────\n"
                f"🔍 <b>Near-Miss (คู่ที่เข้าใกล้จุดคุ้มทุนที่สุด):</b>\n"
                f"• <b>กลยุทธ์:</b> {near_miss['strategy']} | {near_miss['target']}\n"
                f"• <b>เวลา:</b> <code>{near_miss['time']}</code>\n"
                f"• <b>ราคาหน้ากระดาน:</b>\n{legs_str}\n"
                f"• <b>ส่วนต่างสุทธิ:</b> <code>{near_miss['net_pts']:+.2f} จุด</code> "
                f"<i>(ขาดอีก {shortfall:.2f} จุด ถึงจุดคุ้มทุนหลังหักค่าคอม {near_miss['commission_pts']:.2f} จุด)</i>\n"
            )

        content = (
            f"ℹ️ <b>ไม่พบช่องว่าง Arbitrage ที่คุ้มค่าคอม (0.40–0.60 จุด) ในรอบนี้</b>\n"
            f"<i>ตลาด SET50 Options & Futures มีประสิทธิภาพสูง (Efficient Market) สเปรดสะท้อนต้นทุนครบถ้วน</i>\n"
            f"{near_text}"
            f"───────────────────────\n"
            f"📊 <b>สถิติการตรวจสอบข้อมูล:</b>\n"
            f"{meta_stats}"
        )

    footer = (
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🤖 <i>TFEX Arbitrage Radar | คำนวณจาก Bid/Ask จริง และหักค่าคอม 40 บ./สัญญา/ขา</i>"
    )

    return f"{header}\n\n{content}\n\n{footer}"


# ==============================================================================
# 7. WORKER LOOP & DISPATCH CONTROLLER
# ==============================================================================

def get_next_pulse_time(now_dt: datetime.datetime, min_start: datetime.time) -> datetime.datetime:
    """Computes next target pulse time at :00, :15, :30, :45 (+10 seconds offset)."""
    # If now is before the minimum start time, target the minimum start time
    min_start_dt = now_dt.replace(
        hour=min_start.hour,
        minute=min_start.minute,
        second=min_start.second,
        microsecond=0
    )
    if now_dt < min_start_dt:
        return min_start_dt

    for m in [0, 15, 30, 45]:
        cand = now_dt.replace(minute=m, second=10, microsecond=0)
        if cand > now_dt:
            return cand
    next_hr = now_dt + timedelta(hours=1)
    return next_hr.replace(minute=0, second=10, microsecond=0)


def run_arb_worker(session_mode: str, target_date: Optional[str] = None, no_telegram: bool = False) -> None:
    """
    Persistent Session Worker:
    - Runs 15-minute pulses (:00, :15, :30, :45 +10s).
    - Anti-Spam: Alerts only when profitable arb is detected.
    - End of session: Dispatches comprehensive session summary report.
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
        first_pulse_time = time(10, 0, 10)
        session_end_time = time(12, 35, 0)
        summary_start_str = "09:45:00"
        summary_end_str = "12:35:00"
        summary_title = "สรุปภาพรวมช่วงเช้า (09:45–12:35 น.)"
    else:  # AFTERNOON_WORKER
        first_pulse_time = time(14, 30, 10)
        session_end_time = time(17, 5, 0)
        summary_start_str = "09:45:00"
        summary_end_str = "17:05:00"
        summary_title = "สรุปภาพรวมทั้งวัน (09:45–17:05 น.)"

    # Contract & Rate Validation
    cand_fut, front_series, next_series = resolve_active_series_candidates(now_bkk)
    expiry_dt = parse_series_expiry(front_series, trade_date_obj)
    dte = (expiry_dt.date() - trade_date_obj).days
    if dte <= 0:
        print(f"[ERROR] Invalid DTE ({dte} days) for series {front_series}. Exiting.")
        sys.exit(1)
    T_years = dte / 365.0
    thor_rate = fetch_thor_rate(pipeline_url, turso_token)

    print("=" * 75)
    print(f"TFEX ARBITRAGE WORKER STARTED: {session_mode}")
    print(f"Date: {trade_date} | Front: {front_series} (DTE: {dte}d) | THOR: {thor_rate*100.0:.2f}%")
    print(f"First Pulse Target: {first_pulse_time.strftime('%H:%M:%S')} | End Time: {session_end_time.strftime('%H:%M:%S')}")
    print("=" * 75)

    # --------------------------------------------------------------------------
    # 15-Minute Pulse Loop
    # --------------------------------------------------------------------------
    while True:
        curr_bkk = datetime.datetime.now(BANGKOK_TZ)
        if curr_bkk.time() >= session_end_time:
            print(f"[WORKER] Reached session end time ({session_end_time.strftime('%H:%M:%S')}). Commencing session summary...")
            break

        next_pulse = get_next_pulse_time(curr_bkk, first_pulse_time)
        if next_pulse.time() >= session_end_time:
            # Sleep until session end time for final summary
            target_end_dt = curr_bkk.replace(
                hour=session_end_time.hour,
                minute=session_end_time.minute,
                second=session_end_time.second,
                microsecond=0
            )
            sleep_sec = (target_end_dt - curr_bkk).total_seconds()
            if sleep_sec > 0:
                print(f"[WORKER] Standby for session end at {session_end_time.strftime('%H:%M:%S')} ({sleep_sec:.0f}s)...")
                pytime.sleep(sleep_sec)
            break

        sleep_sec = (next_pulse - curr_bkk).total_seconds()
        print(f"[WORKER] Next 15-min pulse at {next_pulse.strftime('%H:%M:%S')} (sleeping {sleep_sec:.0f}s)...")
        if sleep_sec > 0:
            pytime.sleep(sleep_sec)

        # Pulse Execution
        pulse_now = datetime.datetime.now(BANGKOK_TZ)
        record_arb_heartbeat(pipeline_url, turso_token, session_mode, pulse_now)

        w_start = (pulse_now - timedelta(minutes=15)).strftime("%H:%M:00")
        w_end = pulse_now.strftime("%H:%M:00")
        print(f"\n--- [15-MIN PULSE] Scanning window: {w_start} – {w_end} ---")

        grouped_data, fut_cnt, opt_cnt = fetch_intraday_arb_dataset(
            pipeline_url=pipeline_url,
            auth_token=turso_token,
            trade_date=trade_date,
            start_time_bkk=w_start,
            end_time_bkk=w_end
        )

        if not grouped_data:
            print(f"[PULSE] No synchronized data found in window {w_start}–{w_end}. Skipping pulse.")
            continue

        p_opps, _, d1, e1 = scan_put_call_parity(grouped_data, front_series, thor_rate, T_years)
        v_opps, _, d2, e2 = scan_vertical_spreads(grouped_data, front_series)
        c_opps, _, d3, e3 = scan_calendar_spreads(grouped_data, front_series, next_series)

        pulse_opps = p_opps + v_opps + c_opps
        tot_drop = d1 + d2 + d3
        tot_err = e1 + e2 + e3

        print(f"[PULSE] Found {len(pulse_opps)} profitable opportunities (Parity={len(p_opps)}, Vert={len(v_opps)}, Cal={len(c_opps)}).")

        # Anti-Spam: Dispatch ONLY if profitable opportunities exist
        if pulse_opps:
            print(f"[TELEGRAM] Profitable opportunities detected! Dispatching 15-min alert...")
            pulse_msg = format_15min_pulse_alert(
                trade_date=trade_date,
                window_start=w_start[:5],
                window_end=w_end[:5],
                profitable_opps=pulse_opps,
                thor_rate=thor_rate,
                dte=dte,
                T_years=T_years,
                dropped_invalid_quotes=tot_drop,
                calc_error_count=tot_err,
                valid_minutes=len(grouped_data)
            )
            if not no_telegram and tg_token and tg_chat_id:
                send_telegram_alert(tg_token, tg_chat_id, pulse_msg)
        else:
            print(f"[PULSE] ({w_start}–{w_end}) No profitable opportunities. Anti-spam: Telegram alert skipped.")

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

    p_opps, near_p, d1, e1 = scan_put_call_parity(grouped_data, front_series, thor_rate, T_years)
    v_opps, near_v, d2, e2 = scan_vertical_spreads(grouped_data, front_series)
    c_opps, near_c, d3, e3 = scan_calendar_spreads(grouped_data, front_series, next_series)

    all_opps = p_opps + v_opps + c_opps
    all_nears = [n for n in [near_p, near_v, near_c] if n is not None]
    best_near = max(all_nears, key=lambda x: x["net_pts"]) if all_nears else None

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
        valid_minutes=len(grouped_data)
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
    else:
        t_start_bkk = "09:45:00"
        t_end_bkk = "17:05:00"
        sess_title = "ปิดตลาดเย็น (09:45–17:05 น.)"

    cand_fut, front_series, next_series = resolve_active_series_candidates(now_bkk)
    expiry_dt = parse_series_expiry(front_series, trade_date_obj)
    dte = (expiry_dt.date() - trade_date_obj).days
    if dte <= 0:
        print(f"[ERROR] Invalid DTE ({dte} days) for series {front_series}. Exiting.")
        sys.exit(1)
    T_years = dte / 365.0
    thor_rate = fetch_thor_rate(pipeline_url, turso_token)

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

    p_opps, near_p, d1, e1 = scan_put_call_parity(grouped_data, front_series, thor_rate, T_years)
    v_opps, near_v, d2, e2 = scan_vertical_spreads(grouped_data, front_series)
    c_opps, near_c, d3, e3 = scan_calendar_spreads(grouped_data, front_series, next_series)

    all_opps = p_opps + v_opps + c_opps
    all_nears = [n for n in [near_p, near_v, near_c] if n is not None]
    best_near = max(all_nears, key=lambda x: x["net_pts"]) if all_nears else None

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
        valid_minutes=len(grouped_data)
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
        choices=["MORNING_WORKER", "AFTERNOON_WORKER", "MIDDAY", "EVENING"],
        default="MORNING_WORKER",
        help="Execution session mode"
    )
    parser.add_argument("--date", type=str, default=None, help="Target trade date (YYYY-MM-DD, default today)")
    parser.add_argument("--no-telegram", action="store_true", help="Skip sending Telegram alert")
    args = parser.parse_args()

    if args.session in ["MORNING_WORKER", "AFTERNOON_WORKER"]:
        run_arb_worker(session_mode=args.session, target_date=args.date, no_telegram=args.no_telegram)
    else:
        run_single_scan(session_name=args.session, target_date=args.date, no_telegram=args.no_telegram)
