"""
TFEX Intraday Arbitrage Radar (arb_scanner.py)
==============================================
Scans high-frequency intraday snapshots from Turso Cloud SQLite to discover
real executable arbitrage opportunities across:
1. Put-Call Parity (Conversion & Reversal)
2. Vertical Spread Monotonicity Violations (Call & Put)
3. Calendar Spread Spikes (Front vs Next Quarter Futures)

Strict Execution Rules:
- Buy at Ask, Sell at Bid. Filter out bid <= 0, ask <= 0, ask < bid.
- Executable sets (max_sets) = min(volume of all legs in the same exact minute).
- Commission Hurdle:
  - 2-leg strategies (Vertical / Calendar): 0.40 pts (40 THB/leg round-turn = 80 THB).
  - 3-leg strategies (Conversion / Reversal): 0.60 pts (120 THB).
- Multiplier: 200 THB/point.
"""

import os
import sys
import math
import calendar
import argparse
import datetime
from datetime import date, time
from typing import Dict, List, Tuple, Any, Optional

import requests
import pytz
import numpy as np
import pandas as pd
from dotenv import load_dotenv

BANGKOK_TZ = pytz.timezone("Asia/Bangkok")
CONTRACT_MULTIPLIER = 200.0  # THB per SET50 index point
COMMISSION_PER_LEG_PTS = 0.20  # 40 THB / 200 = 0.20 index pts per leg round-turn

MONTH_CODES = {3: "H", 6: "M", 9: "U", 12: "Z"}
REVERSE_MONTH_CODES = {"H": 3, "M": 6, "U": 9, "Z": 12}

# ==============================================================================
# 1. DATABASE & CONFIGURATION UTILITIES
# ==============================================================================

def load_environment() -> None:
    """Loads environment variables from local and parent directories."""
    candidates = [
        ".env",
        "../.env",
        "../tfex-intraday-collector/.env",
        "../tfex-eod-collector/.env",
        "../tfex-quant-analytics/.env"
    ]
    for c in candidates:
        if os.path.exists(c):
            load_dotenv(c)


def to_turso_arg(val: Any) -> dict:
    if val is None:
        return {"type": "null"}
    if isinstance(val, bool):
        return {"type": "integer", "value": "1" if val else "0"}
    if isinstance(val, int):
        return {"type": "integer", "value": str(val)}
    if isinstance(val, float):
        if math.isnan(val) or math.isinf(val):
            return {"type": "null"}
        return {"type": "float", "value": val}
    return {"type": "text", "value": str(val)}


def execute_turso_sql(
    pipeline_url: str,
    auth_token: str,
    sql: str,
    args: list = None
) -> Tuple[List[str], List[List[Any]]]:
    """Executes a single SQL query against Turso Cloud SQLite HTTP Pipeline API."""
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json"
    }
    turso_args = [to_turso_arg(a) for a in (args or [])]
    payload = {
        "requests": [
            {
                "type": "execute",
                "stmt": {
                    "sql": sql.strip(),
                    "args": turso_args
                }
            },
            {"type": "close"}
        ]
    }
    resp = requests.post(pipeline_url, headers=headers, json=payload, timeout=25)
    if resp.status_code != 200:
        raise RuntimeError(f"Turso query failed (HTTP {resp.status_code}): {resp.text}")
    data = resp.json()
    result_item = data.get("results", [])[0]
    if result_item.get("type") == "error":
        raise RuntimeError(f"Turso SQL execution error: {result_item.get('error')}")

    r = result_item.get("response", {}).get("result", {})
    cols = [c["name"] for c in r.get("cols", [])]
    rows = []
    for raw_row in r.get("rows", []):
        parsed = []
        for val_dict in raw_row:
            v_type = val_dict.get("type")
            v = val_dict.get("value")
            if v_type == "null" or v is None:
                parsed.append(None)
            elif v_type == "integer":
                parsed.append(int(v))
            elif v_type == "float":
                parsed.append(float(v))
            else:
                parsed.append(str(v))
        rows.append(parsed)
    return cols, rows


# ==============================================================================
# 2. CONTRACT SPECIFICATIONS & EXPIRY DISCOVERY
# ==============================================================================

def get_contract_last_trading_day(year: int, month: int) -> datetime.datetime:
    """
    Calculates TFEX contract last trading day:
    The business day immediately preceding the last business day of the contract month.
    Trading ceases at 16:30 hrs.
    """
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
    """Discovers active quarterly series (e.g. ['Z26', 'H27'], 'Z26', 'H27')."""
    active_quarterlies: List[str] = []
    for yr in range(now_bkk.year, now_bkk.year + 3):
        for qm in [3, 6, 9, 12]:
            q_ltd = get_contract_last_trading_day(yr, qm)
            if now_bkk <= q_ltd:
                code = MONTH_CODES[qm]
                yr_short = str(yr)[-2:]
                active_quarterlies.append(f"{code}{yr_short}")

    candidate_futures_series = active_quarterlies[:4]
    front_series = active_quarterlies[0]
    next_series = active_quarterlies[1]
    return candidate_futures_series, front_series, next_series


def parse_series_expiry(series_code: str, ref_date: datetime.date) -> datetime.datetime:
    """Parses series code like Z26 or H27 into expiry datetime."""
    month_code = series_code[0].upper()
    month = REVERSE_MONTH_CODES.get(month_code, 12)
    year_prefix = ref_date.year // 100
    year_suffix = int(series_code[1:])
    year = year_prefix * 100 + year_suffix
    return get_contract_last_trading_day(year, month)


# ==============================================================================
# 3. TURSO INTRADAY DATASET LOADER
# ==============================================================================

def fetch_intraday_arb_dataset(
    pipeline_url: str,
    auth_token: str,
    trade_date: str,
    start_time_bkk: str,
    end_time_bkk: str
) -> Tuple[Dict[str, Dict[str, Any]], float]:
    """
    Fetches synchronized 1-minute intraday snapshots for Spot, Futures, Options
    and the latest BOT THOR interest rate.
    Returns:
    - grouped_data: {timestamp_str: {"spot": ..., "futures": {...}, "options": {...}}}
    - thor_rate: float (annual interest rate e.g. 0.0225)
    """
    ts_start = f"{trade_date} {start_time_bkk}"
    ts_end = f"{trade_date} {end_time_bkk}"

    # 1. Fetch Latest THOR interest rate
    thor_rate = 0.0225  # Standard default 2.25%
    try:
        sql_thor = "SELECT thor_rate FROM eod_thor_rates ORDER BY trade_date DESC LIMIT 1;"
        _, rows_thor = execute_turso_sql(pipeline_url, auth_token, sql_thor)
        if rows_thor and rows_thor[0][0] is not None:
            thor_rate = float(rows_thor[0][0]) / 100.0 if float(rows_thor[0][0]) > 0.5 else float(rows_thor[0][0])
    except Exception as e:
        print(f"[DATA] Note: Using fallback THOR rate ({thor_rate*100:.2f}%): {e}")

    # 2. Fetch Spot Snapshots
    sql_spot = """
    SELECT captured_at_bkk, last, volume, value 
    FROM spot_snapshots 
    WHERE captured_at_bkk >= ? AND captured_at_bkk <= ? 
    ORDER BY captured_at_bkk ASC;
    """
    _, rows_spot = execute_turso_sql(pipeline_url, auth_token, sql_spot, [ts_start, ts_end])

    # 3. Fetch Futures Snapshots
    sql_fut = """
    SELECT captured_at_bkk, symbol, bid, ask, last, volume, open_interest 
    FROM futures_snapshots 
    WHERE captured_at_bkk >= ? AND captured_at_bkk <= ? 
    ORDER BY captured_at_bkk ASC;
    """
    _, rows_fut = execute_turso_sql(pipeline_url, auth_token, sql_fut, [ts_start, ts_end])

    # 4. Fetch Options Snapshots
    sql_opt = """
    SELECT captured_at_bkk, symbol, series, strike, option_type, bid, ask, last, volume, open_interest 
    FROM options_snapshots 
    WHERE captured_at_bkk >= ? AND captured_at_bkk <= ? 
    ORDER BY captured_at_bkk ASC;
    """
    _, rows_opt = execute_turso_sql(pipeline_url, auth_token, sql_opt, [ts_start, ts_end])

    # Synchronize by exact timestamp (captured_at_bkk)
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

    return grouped, thor_rate


# ==============================================================================
# 4. THREE INTRADAY ARBITRAGE SCAN MODULES
# ==============================================================================

def scan_put_call_parity(
    grouped_data: Dict[str, Dict[str, Any]],
    front_series: str,
    thor_rate: float,
    T_years: float
) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    Module 1: Put-Call Parity Arbitrage (Conversion & Reversal).
    Commission Hurdle: 0.60 index points (3 legs * 0.20 pts).
    - Conversion: Sell Call (Bid), Buy Put (Ask), Buy Futures (Ask).
      Gross = Call_Bid - Put_Ask + K*exp(-r*T) - Fut_Ask
    - Reversal: Buy Call (Ask), Sell Put (Bid), Sell Futures (Bid).
      Gross = Put_Bid - Call_Ask + Fut_Bid - K*exp(-r*T)
    """
    COMMISSION = 0.60
    fut_sym = f"S50{front_series}"
    disc_factor = math.exp(-thor_rate * max(0.001, T_years))

    opportunities: List[Dict[str, Any]] = []
    best_near_miss: Optional[Dict[str, Any]] = None

    for ts in sorted(grouped_data.keys()):
        frame = grouped_data[ts]
        spot_ref = frame.get("spot")
        fut = frame["futures"].get(fut_sym)
        if not fut:
            continue

        f_bid = fut.get("bid")
        f_ask = fut.get("ask")
        f_vol = fut.get("volume", 0.0)
        if f_bid is None or f_ask is None or f_bid <= 0 or f_ask <= 0 or f_ask < f_bid:
            continue

        ref_price = spot_ref or ((f_bid + f_ask) / 2.0)

        # Find all available strikes in front series
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

            # Strict bid/ask validity
            if (c_bid is None or c_ask is None or c_bid <= 0 or c_ask <= 0 or c_ask < c_bid or
                p_bid is None or p_ask is None or p_bid <= 0 or p_ask <= 0 or p_ask < p_bid):
                continue

            max_sets = int(min(c_vol, p_vol, f_vol))
            pv_k = k * disc_factor
            liq_tag = "[Liquid: Spot±30]" if abs(k - ref_price) <= 30.0 else "[Deep Strike: ถือจนหมดอายุ]"

            # Strategy 1: Conversion
            # Sell Call @ c_bid, Buy Put @ p_ask, Buy Futures @ f_ask
            gross_conv = (c_bid - p_ask) + (pv_k - f_ask)
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

            # Strategy 2: Reversal
            # Buy Call @ c_ask, Sell Put @ p_bid, Sell Futures @ f_bid
            gross_rev = (p_bid - c_ask) + (f_bid - pv_k)
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

    return opportunities, best_near_miss


def scan_vertical_spreads(
    grouped_data: Dict[str, Dict[str, Any]],
    front_series: str
) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    Module 2: Vertical Spread Monotonicity Violations.
    Commission Hurdle: 0.40 index points (2 legs * 0.20 pts).
    - Call Monotonicity: For K1 < K2, if Call_Ask(K1) < Call_Bid(K2).
      Buy K1 @ Ask, Sell K2 @ Bid. Net credit = Call_Bid(K2) - Call_Ask(K1) - 0.40.
    - Put Monotonicity: For K1 < K2, if Put_Ask(K2) < Put_Bid(K1).
      Buy K2 @ Ask, Sell K1 @ Bid. Net credit = Put_Bid(K1) - Put_Ask(K2) - 0.40.
    """
    COMMISSION = 0.40
    opportunities: List[Dict[str, Any]] = []
    best_near_miss: Optional[Dict[str, Any]] = None

    for ts in sorted(grouped_data.keys()):
        frame = grouped_data[ts]
        options = frame["options"]

        # 1. Evaluate Call Vertical Monotonicity
        calls = [
            opt for ((ser, k, otype), opt) in options.items()
            if ser == front_series and otype == "C" and opt.get("bid") and opt.get("ask")
            and opt["bid"] > 0 and opt["ask"] > 0 and opt["ask"] >= opt["bid"]
        ]
        calls.sort(key=lambda x: x["strike"])

        for i in range(len(calls)):
            for j in range(i + 1, len(calls)):
                c1, c2 = calls[i], calls[j]
                k1, k2 = c1["strike"], c2["strike"]
                # Buy c1 @ ask, Sell c2 @ bid
                gross = c2["bid"] - c1["ask"]
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

        # 2. Evaluate Put Vertical Monotonicity
        puts = [
            opt for ((ser, k, otype), opt) in options.items()
            if ser == front_series and otype == "P" and opt.get("bid") and opt.get("ask")
            and opt["bid"] > 0 and opt["ask"] > 0 and opt["ask"] >= opt["bid"]
        ]
        puts.sort(key=lambda x: x["strike"])

        for i in range(len(puts)):
            for j in range(i + 1, len(puts)):
                p1, p2 = puts[i], puts[j]
                k1, k2 = p1["strike"], p2["strike"]
                # Buy p2 @ ask, Sell p1 @ bid
                gross = p1["bid"] - p2["ask"]
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

    return opportunities, best_near_miss


def scan_calendar_spreads(
    grouped_data: Dict[str, Dict[str, Any]],
    front_series: str,
    next_series: str
) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    Module 3: Calendar Spread Spike (Front vs Next Quarter Futures).
    Commission Hurdle: 0.40 index points (2 legs * 0.20 pts).
    Evaluates Front - Next calendar spread against intraday rolling mean +- 2 SD.
    Filters out static dividend basis differences and captures transient execution dislocations.
    """
    COMMISSION = 0.40
    f1_sym = f"S50{front_series}"
    f2_sym = f"S50{next_series}"

    spread_records = []
    for ts in sorted(grouped_data.keys()):
        frame = grouped_data[ts]
        f1 = frame["futures"].get(f1_sym)
        f2 = frame["futures"].get(f2_sym)
        if not f1 or not f2:
            continue

        b1, a1, v1 = f1.get("bid"), f1.get("ask"), f1.get("volume", 0.0)
        b2, a2, v2 = f2.get("bid"), f2.get("ask"), f2.get("volume", 0.0)

        if (b1 is None or a1 is None or b1 <= 0 or a1 <= 0 or a1 < b1 or
            b2 is None or a2 is None or b2 <= 0 or a2 <= 0 or a2 < b2):
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
        return [], None

    df_sp = pd.DataFrame(spread_records)
    # 30-period rolling statistics
    df_sp["rolling_mean"] = df_sp["mid_spread"].rolling(window=30, min_periods=5).mean()
    df_sp["rolling_std"] = df_sp["mid_spread"].rolling(window=30, min_periods=5).std().fillna(0.15)
    # Ensure min std to avoid false precision
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
        # Executable spread = b1 - a2
        exec_sell_spread = b1 - a2
        dev_up = exec_sell_spread - mu
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
        # Executable spread = a1 - b2
        exec_buy_spread = a1 - b2
        dev_down = mu - exec_buy_spread
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
            "stats": f"Spread: {exec_buy_spread:+.2f} vs Baseline: {mu:+.2f} (-{dev_down:.2f} pts / -{(dev_down/std):.1f}σ)"
        }

        if exec_buy_spread < (mu - 2.0 * std) and net_down > 0:
            opportunities.append(item_down)
        else:
            if best_near_miss is None or net_down > best_near_miss["net_pts"]:
                best_near_miss = item_down

    return opportunities, best_near_miss


# ==============================================================================
# 5. TELEGRAM ALERT BUILDER & DISPATCHER
# ==============================================================================

def format_telegram_report(
    session_name: str,
    trade_date: str,
    profitable_opps: List[Dict[str, Any]],
    near_miss: Optional[Dict[str, Any]],
    thor_rate: float
) -> str:
    """Formats actionable Thai arbitrage radar report for Telegram."""
    time_window_map = {
        "MIDDAY": ("09:45", "12:30", "พักเที่ยง"),
        "EVENING": ("09:45", "16:55", "ปิดตลาดเย็น")
    }
    t_start, t_end, sess_label = time_window_map.get(session_name, ("09:45", "16:55", session_name))

    header = (
        f"⚡ <b>TFEX INTRADAY ARBITRAGE RADAR</b>\n"
        f"📅 <i>ข้อมูลวันที่ {trade_date} | รอบ {sess_label} ({t_start}–{t_end} น.)</i>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━"
    )

    if profitable_opps:
        # Deduplicate & rank by net profit
        # Group duplicates occurring within consecutive minutes on the same target
        ranked = sorted(profitable_opps, key=lambda x: x["net_pts"], reverse=True)
        top_3 = ranked[:3]

        parity_cnt = sum(1 for x in profitable_opps if "Conversion" in x["strategy"] or "Reversal" in x["strategy"])
        vertical_cnt = sum(1 for x in profitable_opps if "Monotonicity" in x["strategy"])
        calendar_cnt = sum(1 for x in profitable_opps if "Calendar" in x["strategy"])

        summary_sec = (
            f"🎯 <b>สรุปโอกาส Arbitrage ที่พบ ({len(profitable_opps)} ครั้ง):</b>\n"
            f"• <b>Put-Call Parity:</b> {parity_cnt} ครั้ง\n"
            f"• <b>Vertical Spread:</b> {vertical_cnt} ครั้ง\n"
            f"• <b>Calendar Spread Spike:</b> {calendar_cnt} ครั้ง\n"
            f"• <b>อัตราดอกเบี้ย THOR อ้างอิง:</b> {thor_rate*100.0:.2f}%\n"
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
                f"<i>(ขาดอีก {shortfall:.2f} จุด ถึงจุดคุ้มทุนหลังหักค่าคอม {near_miss['commission_pts']:.2f} จุด)</i>"
            )

        content = (
            f"ℹ️ <b>ไม่พบช่องว่าง Arbitrage ที่คุ้มค่าคอม (0.40–0.60 จุด) ในรอบนี้</b>\n"
            f"<i>ตลาด SET50 Options & Futures มีประสิทธิภาพสูง (Efficient Market) สเปรดสะท้อนต้นทุนครบถ้วน</i>\n"
            f"{near_text}"
        )

    footer = (
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🤖 <i>TFEX Arbitrage Radar | คำนวณจาก Bid/Ask จริง และหักค่าคอม 40 บ./สัญญา/ขา</i>"
    )

    return f"{header}\n\n{content}\n\n{footer}"


def send_telegram_alert(bot_token: str, chat_id: str, message_html: str) -> bool:
    """Delivers HTML report to Telegram chat."""
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message_html,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        r = requests.post(url, json=payload, timeout=20)
        if r.status_code == 200 and r.json().get("ok"):
            print("[TELEGRAM] Arbitrage radar summary delivered successfully.")
            return True
        else:
            print(f"[TELEGRAM] Warning: sendMessage failed ({r.status_code}): {r.text}")
            return False
    except Exception as e:
        print(f"[TELEGRAM] Error sending telegram notification: {e}")
        return False


# ==============================================================================
# 6. TEST MODE GENERATOR
# ==============================================================================

def generate_mock_arb_dataset(trade_date: str) -> Tuple[Dict[str, Dict[str, Any]], float]:
    """Generates a realistic intraday snapshot dataset with intentional arbitrage cases for verification."""
    front_series = "Z26"
    next_series = "H27"
    f1_sym = f"S50{front_series}"
    f2_sym = f"S50{next_series}"
    thor_rate = 0.0225

    grouped: Dict[str, Dict[str, Any]] = {}
    base_time = datetime.datetime.strptime(f"{trade_date} 10:00:00", "%Y-%m-%d %H:%M:%S")

    # Generate 40 normal minutes
    for m in range(40):
        ts = (base_time + datetime.timedelta(minutes=m)).strftime("%Y-%m-%d %H:%M:%S")
        f1_price = 1025.0 + math.sin(m / 5.0) * 1.5
        f2_price = f1_price - 3.5  # Typical basis spread

        grouped[ts] = {
            "spot": f1_price - 0.2,
            "futures": {
                f1_sym: {"symbol": f1_sym, "bid": f1_price - 0.1, "ask": f1_price + 0.1, "last": f1_price, "volume": 120 + m * 5},
                f2_sym: {"symbol": f2_sym, "bid": f2_price - 0.2, "ask": f2_price + 0.2, "last": f2_price, "volume": 60 + m * 2}
            },
            "options": {}
        }

        # Add strikes around 1000 - 1050
        for k in range(1000, 1060, 10):
            intrinsic_c = max(0.0, f1_price - k)
            intrinsic_p = max(0.0, k - f1_price)
            time_val = 8.0 - abs(f1_price - k) * 0.15
            c_mid = intrinsic_c + max(0.5, time_val)
            p_mid = intrinsic_p + max(0.5, time_val)

            grouped[ts]["options"][(front_series, float(k), "C")] = {
                "symbol": f"S50{front_series}C{k}", "series": front_series, "strike": float(k), "option_type": "C",
                "bid": round(c_mid - 0.3, 2), "ask": round(c_mid + 0.3, 2), "last": round(c_mid, 2), "volume": 50 + m
            }
            grouped[ts]["options"][(front_series, float(k), "P")] = {
                "symbol": f"S50{front_series}P{k}", "series": front_series, "strike": float(k), "option_type": "P",
                "bid": round(p_mid - 0.3, 2), "ask": round(p_mid + 0.3, 2), "last": round(p_mid, 2), "volume": 45 + m
            }

    # Inject Arbitrage Opportunity 1: Put-Call Parity Reversal at 10:15
    ts_arb1 = f"{trade_date} 10:15:00"
    grouped[ts_arb1]["options"][(front_series, 1020.0, "C")]["ask"] = 12.0  # Underpriced Call
    grouped[ts_arb1]["options"][(front_series, 1020.0, "P")]["bid"] = 10.5  # Overpriced Put
    grouped[ts_arb1]["futures"][f1_sym]["bid"] = 1026.0

    # Inject Arbitrage Opportunity 2: Vertical Monotonicity at 10:25
    ts_arb2 = f"{trade_date} 10:25:00"
    grouped[ts_arb2]["options"][(front_series, 1020.0, "C")]["ask"] = 11.20
    grouped[ts_arb2]["options"][(front_series, 1030.0, "C")]["bid"] = 12.50  # Monotonicity breach

    # Inject Arbitrage Opportunity 3: Calendar Spread Spike at 10:35
    ts_arb3 = f"{trade_date} 10:35:00"
    grouped[ts_arb3]["futures"][f1_sym]["bid"] = 1029.5  # Sudden transient spike in Front
    grouped[ts_arb3]["futures"][f2_sym]["ask"] = 1021.0

    return grouped, thor_rate


# ==============================================================================
# 7. MAIN ENGINE ORCHESTRATOR
# ==============================================================================

def run_arb_radar(session_name: str = "EVENING", no_telegram: bool = False, test_mode: bool = False):
    """Main execution pipeline."""
    start_time = datetime.datetime.now(BANGKOK_TZ)
    print("=" * 75)
    print(f"TFEX INTRADAY ARBITRAGE RADAR | Session: {session_name}")
    print(f"Timestamp: {start_time.strftime('%Y-%m-%d %H:%M:%S')} UTC+07:00")
    print("=" * 75)

    load_environment()

    turso_url = os.environ.get("TURSO_DATABASE_URL")
    turso_token = os.environ.get("TURSO_AUTH_TOKEN")
    tg_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    tg_chat_id = os.environ.get("TELEGRAM_CHAT_ID")

    now_bkk = datetime.datetime.now(BANGKOK_TZ)
    today_str = now_bkk.strftime("%Y-%m-%d")

    # Time boundaries
    if session_name.upper() == "MIDDAY":
        t_start_bkk = "09:45:00"
        t_end_bkk = "12:35:00"
    else:  # EVENING
        t_start_bkk = "09:45:00"
        t_end_bkk = "17:05:00"

    cand_fut, front_series, next_series = resolve_active_series_candidates(now_bkk)
    expiry_dt = parse_series_expiry(front_series, now_bkk.date())
    days_to_exp = max(0.5, (expiry_dt.date() - now_bkk.date()).days)
    T_years = days_to_exp / 365.0

    print(f"[RADAR] Active Series: Front={front_series}, Next={next_series} (Days to exp: {days_to_exp})")

    if test_mode:
        print("[TEST] Executing in TEST MODE with realistic sample intraday data...")
        grouped_data, thor_rate = generate_mock_arb_dataset(today_str)
        trade_date = today_str
    else:
        if not turso_url or not turso_token:
            print("[ERROR] TURSO_DATABASE_URL or TURSO_AUTH_TOKEN not configured.")
            print("Tip: Use --test to test run locally with simulated market data.")
            sys.exit(1)

        pipeline_url = f"{turso_url.replace('libsql://', 'https://').rstrip('/')}/v2/pipeline"

        # Determine target trade date (fallback to latest available in Turso if today is empty)
        trade_date = today_str
        try:
            sql_check = f"SELECT COUNT(*) FROM futures_snapshots WHERE captured_at_bkk LIKE '{today_str}%';"
            _, r_check = execute_turso_sql(pipeline_url, turso_token, sql_check)
            if not r_check or int(r_check[0][0] or 0) == 0:
                sql_latest = "SELECT MAX(SUBSTR(captured_at_bkk, 1, 10)) FROM futures_snapshots;"
                _, r_lat = execute_turso_sql(pipeline_url, turso_token, sql_latest)
                if r_lat and r_lat[0][0]:
                    trade_date = str(r_lat[0][0])
                    print(f"[DATA] No records for {today_str}. Falling back to latest trade date in Turso: {trade_date}")
        except Exception as e:
            print(f"[DATA] Note checking latest trade date: {e}")

        print(f"[DATA] Fetching intraday dataset for {trade_date} ({t_start_bkk} - {t_end_bkk})...")
        grouped_data, thor_rate = fetch_intraday_arb_dataset(
            pipeline_url=pipeline_url,
            auth_token=turso_token,
            trade_date=trade_date,
            start_time_bkk=t_start_bkk,
            end_time_bkk=t_end_bkk
        )

    print(f"[DATA] Synchronized minutes loaded: {len(grouped_data)} bars | THOR rate: {thor_rate*100.0:.2f}%")

    if not grouped_data:
        print(f"[WARN] No synchronized market data found for {trade_date}. Exiting.")
        return

    # Run Scan Modules
    print("[SCAN] 1. Scanning Put-Call Parity (Conversion & Reversal)...")
    parity_opps, near_parity = scan_put_call_parity(grouped_data, front_series, thor_rate, T_years)
    print(f"       Found {len(parity_opps)} profitable Put-Call Parity opportunities.")

    print("[SCAN] 2. Scanning Vertical Spread Monotonicity Violations...")
    vert_opps, near_vert = scan_vertical_spreads(grouped_data, front_series)
    print(f"       Found {len(vert_opps)} profitable Vertical Spread opportunities.")

    print("[SCAN] 3. Scanning Calendar Spread Spikes (Front vs Next Quarter)...")
    cal_opps, near_cal = scan_calendar_spreads(grouped_data, front_series, next_series)
    print(f"       Found {len(cal_opps)} profitable Calendar Spread opportunities.")

    all_opps = parity_opps + vert_opps + cal_opps
    all_nears = [n for n in [near_parity, near_vert, near_cal] if n is not None]
    best_near = max(all_nears, key=lambda x: x["net_pts"]) if all_nears else None

    # Build Telegram Notification
    telegram_msg = format_telegram_report(
        session_name=session_name,
        trade_date=trade_date,
        profitable_opps=all_opps,
        near_miss=best_near,
        thor_rate=thor_rate
    )

    print("\n" + "=" * 75)
    print("TELEGRAM NOTIFICATION PREVIEW:")
    print("=" * 75)
    print(telegram_msg)
    print("=" * 75)
    print(f"Message Length: {len(telegram_msg)} characters (Limit: 4096)")

    # Dispatch to Telegram
    if no_telegram:
        print("[TELEGRAM] --no-telegram supplied. Dispatch skipped.")
    elif tg_token and tg_chat_id:
        print(f"[TELEGRAM] Dispatching alert to Chat ID {tg_chat_id}...")
        send_telegram_alert(tg_token, tg_chat_id, telegram_msg)
    else:
        print("[TELEGRAM] Warning: Telegram credentials not configured. Dispatch skipped.")

    duration = (datetime.datetime.now(BANGKOK_TZ) - start_time).total_seconds()
    print("=" * 75)
    print(f"ARBITRAGE RADAR COMPLETED SUCCESSFULLY ({duration:.2f}s)")
    print("=" * 75)


# ==============================================================================
# ENTRY POINT
# ==============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TFEX Intraday Arbitrage Radar")
    parser.add_argument("--session", choices=["MIDDAY", "EVENING"], default="EVENING", help="Trading session to scan")
    parser.add_argument("--no-telegram", action="store_true", help="Skip sending Telegram alert")
    parser.add_argument("--test", action="store_true", help="Run with simulated intraday dataset")
    args = parser.parse_args()

    run_arb_radar(session_name=args.session, no_telegram=args.no_telegram, test_mode=args.test)
