"""
Unit test for enhanced session summary in arb_scanner.py:
- Verifies Best-of-Minute Distribution stats (Max, P90, Avg, SD, % Bars near breakeven)
- Verifies Strategy Breakdown (Near-Miss Top 1 for Parity, Vertical, Calendar)
- Verifies Friction Breakdown math (Theoretical Mid Edge, Spread Slippage Cost, Commission, Net Profit)
- Verifies Telegram message length (< 3,800 characters)
"""

import datetime
import math
import numpy as np
import pytest

from arb_scanner import (
    scan_put_call_parity,
    scan_vertical_spreads,
    scan_calendar_spreads,
    compute_best_of_minute_stats,
    format_telegram_report,
    COMMISSION_3LEG_PTS,
    COMMISSION_2LEG_PTS,
    select_dynamic_thor_rate,
    is_executable_market_hours,
    OpportunityLifespanTracker,
    BANGKOK_TZ
)


def build_mock_intraday_data(num_bars: int = 60, front_series: str = "Z26", next_series: str = "H27"):
    """Generates synthetic multi-minute intraday dataset for testing."""
    grouped = {}
    base_f1 = 920.0
    base_f2 = 922.5

    for i in range(num_bars):
        hh = 9 + (45 + i) // 60
        mm = (45 + i) % 60
        ts = f"{hh:02d}:{mm:02d}:00"

        # Slight random variation per bar
        f1_bid = base_f1 + (i % 5) * 0.2
        f1_ask = f1_bid + 0.4
        f1_vol = 50.0

        f2_bid = base_f2 + (i % 7) * 0.2
        f2_ask = f2_bid + 0.4
        f2_vol = 30.0

        futures = {
            f"S50{front_series}": {"bid": f1_bid, "ask": f1_ask, "volume": f1_vol, "bid_vol": 10.0, "ask_vol": 10.0},
            f"S50{next_series}": {"bid": f2_bid, "ask": f2_ask, "volume": f2_vol, "bid_vol": 10.0, "ask_vol": 10.0}
        }

        # Options chain: Strikes 900, 910, 920, 930, 940
        options = {}
        for k in [900.0, 910.0, 920.0, 930.0, 940.0]:
            # Call
            c_mid = max(0.5, 920.0 - k + 10.0)
            c_bid = c_mid - 0.3
            c_ask = c_mid + 0.3
            # Put
            p_mid = max(0.5, k - 920.0 + 10.0)
            p_bid = p_mid - 0.3
            p_ask = p_mid + 0.3

            options[(front_series, k, "C")] = {"bid": c_bid, "ask": c_ask, "volume": 20.0, "bid_vol": 10.0, "ask_vol": 10.0, "strike": k, "series": front_series, "option_type": "C"}
            options[(front_series, k, "P")] = {"bid": p_bid, "ask": p_ask, "volume": 20.0, "bid_vol": 10.0, "ask_vol": 10.0, "strike": k, "series": front_series, "option_type": "P"}

        grouped[ts] = {
            "futures": futures,
            "options": options,
            "spot": 920.0
        }

    return grouped


def test_scan_and_friction_breakdown():
    front_series = "Z26"
    next_series = "H27"
    thor_rate = 0.0225
    T_years = 42.0 / 365.0

    grouped = build_mock_intraday_data(num_bars=30, front_series=front_series, next_series=next_series)

    # 1. Run Scanners
    p_opps, near_p, d1, e1, p_mins = scan_put_call_parity(grouped, front_series, thor_rate, T_years)
    v_opps, near_v, d2, e2, v_mins = scan_vertical_spreads(grouped, front_series)
    c_opps, near_c, d3, e3, c_mins = scan_calendar_spreads(grouped, front_series, next_series)

    # Verify return types
    assert isinstance(p_mins, dict)
    assert isinstance(v_mins, dict)
    assert isinstance(c_mins, dict)

    # Verify Friction Breakdown mathematical identity for near_p
    assert near_p is not None
    assert "mid_edge" in near_p
    assert "spread_cost" in near_p
    assert "net_pts" in near_p
    assert "commission_pts" in near_p

    # Identity: Net Profit = Mid Edge - Spread Slippage - Commission
    expected_net = near_p["mid_edge"] - near_p["spread_cost"] - near_p["commission_pts"]
    assert math.isclose(near_p["net_pts"], expected_net, abs_tol=1e-7)

    # Verify Friction Breakdown for near_v
    assert near_v is not None
    expected_v_net = near_v["mid_edge"] - near_v["spread_cost"] - near_v["commission_pts"]
    assert math.isclose(near_v["net_pts"], expected_v_net, abs_tol=1e-7)

    # Verify Friction Breakdown for near_c
    if near_c is not None:
        expected_c_net = near_c["mid_edge"] - near_c["spread_cost"] - near_c["commission_pts"]
        assert math.isclose(near_c["net_pts"], expected_c_net, abs_tol=1e-7)

    print("✅ Friction Breakdown Identity Verified (Net = Mid - Spread - Comm)")


def test_best_of_minute_stats():
    front_series = "Z26"
    next_series = "H27"
    thor_rate = 0.0225
    T_years = 42.0 / 365.0

    grouped = build_mock_intraday_data(num_bars=60, front_series=front_series, next_series=next_series)

    p_opps, near_p, d1, e1, p_mins = scan_put_call_parity(grouped, front_series, thor_rate, T_years)
    v_opps, near_v, d2, e2, v_mins = scan_vertical_spreads(grouped, front_series)
    c_opps, near_c, d3, e3, c_mins = scan_calendar_spreads(grouped, front_series, next_series)

    stats = compute_best_of_minute_stats(
        grouped_data_keys=list(grouped.keys()),
        p_mins=p_mins,
        v_mins=v_mins,
        c_mins=c_mins
    )

    assert stats["has_data"] is True
    assert stats["bar_count"] == 60
    assert "max" in stats
    assert "p90" in stats
    assert "avg" in stats
    assert "sd" in stats
    assert "near_be_cnt" in stats
    assert "near_be_pct" in stats

    assert stats["max"] >= stats["p90"]
    assert stats["p90"] >= stats["avg"] or math.isclose(stats["p90"], stats["avg"], abs_tol=0.5)
    assert stats["sd"] >= 0.0
    assert 0 <= stats["near_be_cnt"] <= 60
    assert 0.0 <= stats["near_be_pct"] <= 100.0

    print(f"✅ Best-of-Minute Stats Verified: Max={stats['max']:+.2f}, P90={stats['p90']:+.2f}, Avg={stats['avg']:+.2f}, SD={stats['sd']:.2f}")


def test_telegram_message_formatting_and_length():
    front_series = "Z26"
    next_series = "H27"
    thor_rate = 0.0225
    T_years = 42.0 / 365.0

    grouped = build_mock_intraday_data(num_bars=170, front_series=front_series, next_series=next_series)

    p_opps, near_p, d1, e1, p_mins = scan_put_call_parity(grouped, front_series, thor_rate, T_years)
    v_opps, near_v, d2, e2, v_mins = scan_vertical_spreads(grouped, front_series)
    c_opps, near_c, d3, e3, c_mins = scan_calendar_spreads(grouped, front_series, next_series)

    all_opps = p_opps + v_opps + c_opps
    all_nears = [n for n in [near_p, near_v, near_c] if n is not None]
    best_near = max(all_nears, key=lambda x: x["net_pts"]) if all_nears else None

    minute_stats = compute_best_of_minute_stats(
        grouped_data_keys=list(grouped.keys()),
        p_mins=p_mins,
        v_mins=v_mins,
        c_mins=c_mins
    )

    strategy_nears = {
        "parity": near_p,
        "vertical": near_v,
        "calendar": near_c
    }

    report_text = format_telegram_report(
        session_title="สรุปภาพรวมช่วงเช้า (09:45–12:35 น.)",
        trade_date="2026-11-18",
        profitable_opps=all_opps,
        near_miss=best_near,
        thor_rate=thor_rate,
        dte=42,
        T_years=T_years,
        dropped_invalid_quotes=d1 + d2 + d3,
        calc_error_count=e1 + e2 + e3,
        valid_minutes=len(grouped),
        minute_stats=minute_stats,
        strategy_nears=strategy_nears
    )

    # Strict assertion on Telegram message length
    msg_len = len(report_text)
    print(f"\nGenerated Telegram Report Preview:\n{report_text}\n")
    print(f"Message length: {msg_len} characters (Target: < 3,800, Limit: 4,096)")

    assert msg_len < 3800, f"Telegram message length {msg_len} exceeds 3,800 limit!"
    assert "พฤติกรรมความตึงของตลาด (Best-of-Minute Stats)" in report_text
    assert "Near-Miss Top 1 รายกลยุทธ์" in report_text
    assert "ผ่าโครงสร้างต้นทุนคู่ที่ดีที่สุดอันดับ 1 (Friction Breakdown)" in report_text
    assert "Data Audit" in report_text
    print("✅ Telegram Report Formatting & Length Verified!")


def test_telegram_message_no_profitable_opps():
    """Tests the standard efficient market scenario where net profit is negative."""
    thor_rate = 0.0225
    T_years = 42.0 / 365.0

    minute_stats = {
        "has_data": True,
        "bar_count": 170,
        "max": -0.12,
        "p90": -0.25,
        "avg": -0.35,
        "sd": 0.08,
        "near_be_cnt": 18,
        "near_be_pct": 10.6
    }

    near_p = {
        "strategy": "Conversion (Sell C, Buy P, Buy Fut)",
        "target": "Strike 920 & S50Z26",
        "series": "S50Z26",
        "strike": 920.0,
        "time": "10:42:00",
        "max_sets": 15,
        "gross_pts": 0.48,
        "net_pts": -0.12,
        "mid_edge": 1.15,
        "spread_cost": 0.67,
        "commission_pts": 0.60,
        "legs": [
            "Sell Call 920 @ Bid <code>12.00</code> (Vol: 25)",
            "Buy Put 920 @ Ask <code>8.50</code> (Vol: 15)",
            "Buy Futures S50Z26 @ Ask <code>920.20</code> (Vol: 40)"
        ]
    }

    near_v = {
        "strategy": "Call Monotonicity Violation",
        "target": "Call 910/920 (S50Z26)",
        "series": "S50Z26",
        "strike_pair": "910/920",
        "time": "11:15:00",
        "max_sets": 8,
        "gross_pts": 0.22,
        "net_pts": -0.18,
        "mid_edge": 0.62,
        "spread_cost": 0.40,
        "commission_pts": 0.40,
        "legs": [
            "Buy Call 910 @ Ask <code>18.20</code> (Vol: 10)",
            "Sell Call 920 @ Bid <code>18.42</code> (Vol: 8)"
        ]
    }

    strategy_nears = {
        "parity": near_p,
        "vertical": near_v,
        "calendar": None
    }

    report_text = format_telegram_report(
        session_title="สรุปภาพรวมช่วงเช้า (09:45–12:35 น.)",
        trade_date="2026-11-18",
        profitable_opps=[],
        near_miss=near_p,
        thor_rate=thor_rate,
        dte=42,
        T_years=T_years,
        dropped_invalid_quotes=1500,
        calc_error_count=0,
        valid_minutes=170,
        minute_stats=minute_stats,
        strategy_nears=strategy_nears
    )

    msg_len = len(report_text)
    print(f"\n[No Profitable Opps] Telegram Report Preview:\n{report_text}\n")
    print(f"Message length: {msg_len} characters (Target: < 3,800, Limit: 4,096)")

    assert msg_len < 3800
    assert "ไม่พบช่องว่าง Arbitrage ที่คุ้มค่าคอม" in report_text
    assert "<b>Calendar Spread:</b> <i>N/A (ไม่มีสภาพคล่องครบขา)</i>" in report_text
    assert "ขาดอีก 0.12 จุด ถึงจุดคุ้มทุน" in report_text
    print("✅ Efficient Market Scenario Verified!")


def test_zero_traded_volume_with_valid_quotes():
    """Verifies that quotes with volume=0 (no trades yet today) but valid Bid/Ask are NOT dropped."""
    front_series = "Z26"
    next_series = "H27"
    thor_rate = 0.0225
    T_years = 42.0 / 365.0

    # Build data where all volume=0.0
    grouped = {}
    for i in range(15):
        ts = f"10:{i:02d}:00"
        f2_shift = (i % 3) * 0.2
        grouped[ts] = {
            "spot": 920.0,
            "futures": {
                f"S50{front_series}": {"bid": 920.0, "ask": 920.4, "volume": 0.0, "bid_vol": 10.0, "ask_vol": 10.0},
                f"S50{next_series}": {"bid": 922.0 + f2_shift, "ask": 922.5 + f2_shift, "volume": 0.0, "bid_vol": 10.0, "ask_vol": 10.0}
            },
            "options": {
                (front_series, 920.0, "C"): {"bid": 10.0, "ask": 10.5, "volume": 0.0, "bid_vol": 10.0, "ask_vol": 10.0, "strike": 920.0, "series": front_series, "option_type": "C"},
                (front_series, 920.0, "P"): {"bid": 9.0, "ask": 9.5, "volume": 0.0, "bid_vol": 10.0, "ask_vol": 10.0, "strike": 920.0, "series": front_series, "option_type": "P"},
                (front_series, 930.0, "C"): {"bid": 5.0, "ask": 5.5, "volume": 0.0, "bid_vol": 10.0, "ask_vol": 10.0, "strike": 930.0, "series": front_series, "option_type": "C"},
                (front_series, 930.0, "P"): {"bid": 14.0, "ask": 14.5, "volume": 0.0, "bid_vol": 10.0, "ask_vol": 10.0, "strike": 930.0, "series": front_series, "option_type": "P"},
            }
        }

    p_opps, near_p, d1, e1, p_mins = scan_put_call_parity(grouped, front_series, thor_rate, T_years)
    v_opps, near_v, d2, e2, v_mins = scan_vertical_spreads(grouped, front_series)
    c_opps, near_c, d3, e3, c_mins = scan_calendar_spreads(grouped, front_series, next_series)

    # ZERO dropped quotes because all bid/ask are valid even though volume=0!
    assert d1 == 0, f"Expected 0 dropped in parity, got {d1}"
    assert d2 == 0, f"Expected 0 dropped in vertical, got {d2}"
    assert d3 == 0, f"Expected 0 dropped in calendar, got {d3}"

    # Near misses must NOT be None
    assert near_p is not None, "near_p must not be None when quotes are valid"
    assert near_v is not None, "near_v must not be None when quotes are valid"
    assert near_c is not None, "near_c must not be None when quotes are valid"

    # Default max_sets should be >= 1
    assert near_p["max_sets"] >= 1
    assert near_v["max_sets"] >= 1
    assert near_c["max_sets"] >= 1
    print("✅ Zero Traded Volume with Valid Bids/Asks Correctly Evaluated (Not Dropped)!")


def test_dynamic_thor_rate_selection():
    """Verifies Tenor-Matching logic for dynamic THOR rates across DTE horizons."""
    thor_record = {
        "overnight": 0.99479,
        "1m": 0.99447,
        "3m": 0.99564,
        "6m": 0.99600
    }

    # 1. DTE <= 30 -> 1M
    r1, label1 = select_dynamic_thor_rate(thor_record, dte=15)
    assert label1 == "1M"
    assert math.isclose(r1, 0.0099447, abs_tol=1e-7)

    # 2. 30 < DTE <= 90 -> 3M
    r2, label2 = select_dynamic_thor_rate(thor_record, dte=42)
    assert label2 == "3M"
    assert math.isclose(r2, 0.0099564, abs_tol=1e-7)

    # 3. 90 < DTE <= 180 -> 6M
    r3, label3 = select_dynamic_thor_rate(thor_record, dte=120)
    assert label3 == "6M"
    assert math.isclose(r3, 0.0099600, abs_tol=1e-7)

    # 4. DTE > 180 -> 6M
    r4, label4 = select_dynamic_thor_rate(thor_record, dte=250)
    assert label4 == "6M"
    assert math.isclose(r4, 0.0099600, abs_tol=1e-7)

    # 5. Fallback test: when 3M is missing for DTE=42, fall back to 1M
    partial_rec = {"overnight": 0.99479, "1m": 0.99447, "3m": None, "6m": 0.99600}
    r_fb, label_fb = select_dynamic_thor_rate(partial_rec, dte=42)
    assert label_fb == "1M"
    assert math.isclose(r_fb, 0.0099447, abs_tol=1e-7)

    # 6. Fallback test: when all averages missing, fall back to Overnight
    on_only_rec = {"overnight": 0.99479, "1m": None, "3m": None, "6m": None}
    r_on, label_on = select_dynamic_thor_rate(on_only_rec, dte=42)
    assert label_on == "ON"
    assert math.isclose(r_on, 0.0099479, abs_tol=1e-7)

    print("✅ Dynamic THOR Rate Tenor Matching & Fallbacks Verified!")


def test_is_executable_market_hours():
    """Verifies Market Hours Guard for executable trading windows vs post-market/lunch suppression."""
    from datetime import time as dt_time, datetime as dt_datetime

    # Morning session: 09:45:00 - 12:30:00
    assert is_executable_market_hours(dt_time(9, 44, 59)) is False
    assert is_executable_market_hours(dt_time(9, 45, 0)) is True
    assert is_executable_market_hours(dt_time(10, 30, 15)) is True
    assert is_executable_market_hours(dt_time(12, 30, 0)) is True
    assert is_executable_market_hours(dt_time(12, 30, 1)) is False

    # Lunch break
    assert is_executable_market_hours(dt_time(13, 0, 0)) is False
    assert is_executable_market_hours(dt_time(14, 14, 59)) is False

    # Afternoon session: 14:15:00 - 16:55:00
    assert is_executable_market_hours(dt_time(14, 15, 0)) is True
    assert is_executable_market_hours(dt_time(15, 45, 30)) is True
    assert is_executable_market_hours(dt_time(16, 55, 0)) is True

    # Post-market: 16:55:01 onwards (MMs withdraw quotes -> MUST suppress live alerts)
    assert is_executable_market_hours(dt_time(16, 55, 1)) is False
    assert is_executable_market_hours(dt_time(17, 0, 0)) is False
    assert is_executable_market_hours(dt_time(17, 5, 0)) is False
    assert is_executable_market_hours(dt_time(18, 0, 0)) is False

    # Datetime inputs
    dt_active = dt_datetime(2026, 11, 19, 10, 15, 0)
    dt_post = dt_datetime(2026, 11, 19, 16, 56, 0)
    assert is_executable_market_hours(dt_active) is True
    assert is_executable_market_hours(dt_post) is False

    print("✅ Market Hours Guard Verified (Active 09:45-12:30 & 14:15-16:55, Suppressed 16:55-17:05)")


def test_sub_minute_coalescing_in_best_of_minute_stats():
    """Verifies that multiple sub-minute ticks within the same minute are coalesced into 1-minute buckets."""
    # 3 sub-minute events in minute 10:05, 2 in 10:06
    p_mins = {
        "2026-11-19 10:05:05": 1.10,
        "2026-11-19 10:05:25": 1.50,
        "2026-11-19 10:05:45": 0.90,
        "2026-11-19 10:06:10": 2.00,
        "2026-11-19 10:06:40": 2.40,
    }
    v_mins = {
        "2026-11-19 10:05:15": 1.30,
        "2026-11-19 10:06:20": 2.20,
    }
    c_mins = {}

    keys = list(p_mins.keys()) + list(v_mins.keys())

    stats = compute_best_of_minute_stats(
        grouped_data_keys=keys,
        p_mins=p_mins,
        v_mins=v_mins,
        c_mins=c_mins
    )

    # Should coalesce to exactly 2 distinct minute buckets: 10:05 and 10:06
    assert stats["has_data"] is True
    assert stats["bar_count"] == 2
    # Minute 10:05 best = max(1.10, 1.50, 0.90, 1.30) = 1.50
    # Minute 10:06 best = max(2.00, 2.40, 2.20) = 2.40
    # Overall max = 2.40
    assert math.isclose(stats["max"], 2.40, abs_tol=1e-7)
    # Average = (1.50 + 2.40) / 2 = 1.95
    assert math.isclose(stats["avg"], 1.95, abs_tol=1e-7)

    print("✅ Sub-Minute Timestamp Coalescing to 1-Minute Buckets Verified!")


def test_opportunity_lifespan_tracker():
    """Verifies Tick-to-Tick Opportunity Lifespan Tracker (first_seen, last_seen, duration, finalize)."""
    import datetime
    tracker = OpportunityLifespanTracker()

    # Initial state
    stats0 = tracker.get_stats()
    assert stats0["has_data"] is False
    assert stats0["count"] == 0

    # Tick 1: 10:00:00 - Opp A arrives
    opp_a = {"strategy": "Conversion", "target": "Strike 920 & S50Z26", "net_pts": 0.50}
    tracker.update([opp_a], "2026-11-19 10:00:00")
    assert len(tracker.active_opportunities) == 1
    assert len(tracker.closed_durations) == 0

    # Tick 2: 10:00:05 - Opp A still active, Opp B arrives
    opp_b = {"strategy": "Vertical Call", "target": "Call 900/910", "net_pts": 0.30}
    tracker.update([opp_a, opp_b], "2026-11-19 10:00:05")
    assert len(tracker.active_opportunities) == 2
    assert len(tracker.closed_durations) == 0

    # Tick 3: 10:00:10 - Opp A still active, Opp B disappears (closed)
    tracker.update([opp_a], "2026-11-19 10:00:10")
    # Opp B was first seen 10:00:05, last seen 10:00:05 -> duration = max(1.0, 0.0) = 1.0s
    assert len(tracker.active_opportunities) == 1
    assert len(tracker.closed_durations) == 1
    assert tracker.closed_durations[0] == 1.0

    # Tick 4: 10:00:20 - Opp A disappears (closed)
    # Opp A was first seen 10:00:00, last seen 10:00:10 -> duration = 10.0s
    tracker.update([], "2026-11-19 10:00:20")
    assert len(tracker.active_opportunities) == 0
    assert len(tracker.closed_durations) == 2
    assert tracker.closed_durations[1] == 10.0

    # Tick 5: 10:01:00 - Opp C arrives, stays active until session end
    opp_c = {"strategy": "Calendar", "target": "S50Z26/S50H27", "net_pts": 0.70}
    tracker.update([opp_c], "2026-11-19 10:01:00")
    assert len(tracker.active_opportunities) == 1

    # Session end at 10:01:45
    tracker.finalize("2026-11-19 10:01:45")
    assert len(tracker.active_opportunities) == 0
    assert len(tracker.closed_durations) == 3
    # Opp C duration: 10:01:45 - 10:01:00 = 45.0s
    assert tracker.closed_durations[2] == 45.0

    stats = tracker.get_stats()
    assert stats["has_data"] is True
    assert stats["count"] == 3
    assert math.isclose(stats["min_sec"], 1.0, abs_tol=1e-7)
    assert math.isclose(stats["max_sec"], 45.0, abs_tol=1e-7)
    # Average: (1.0 + 10.0 + 45.0) / 3 = 18.666...
    assert math.isclose(stats["avg_sec"], 56.0 / 3.0, abs_tol=1e-7)

    print(f"✅ Opportunity Lifespan Tracker Verified: Count={stats['count']}, Min={stats['min_sec']:.0f}s, Max={stats['max_sec']:.0f}s, Avg={stats['avg_sec']:.1f}s")


def test_telegram_message_with_lifespan():
    """Verifies that format_telegram_report renders lifespan stats correctly and stays under 3,800 chars."""
    thor_rate = 0.0225
    T_years = 42.0 / 365.0

    opp = {
        "strategy": "Conversion (Sell C, Buy P, Buy Fut)",
        "target": "Strike 920 & S50Z26",
        "series": "S50Z26",
        "strike": 920.0,
        "time": "10:15:00",
        "max_sets": 10,
        "gross_pts": 1.20,
        "net_pts": 0.60,
        "mid_edge": 1.80,
        "spread_cost": 0.60,
        "commission_pts": 0.60,
        "legs": [
            "Sell Call 920 @ Bid <code>12.00</code> (Vol: 20)",
            "Buy Put 920 @ Ask <code>10.00</code> (Vol: 20)",
            "Buy Futures S50Z26 @ Ask <code>921.00</code> (Vol: 20)"
        ]
    }

    lifespan_stats = {
        "has_data": True,
        "count": 5,
        "avg_sec": 18.4,
        "min_sec": 3.0,
        "max_sec": 42.0
    }

    report = format_telegram_report(
        session_title="ทดสอบ Lifespan",
        trade_date="2026-11-19",
        profitable_opps=[opp],
        near_miss=opp,
        thor_rate=thor_rate,
        dte=42,
        T_years=T_years,
        dropped_invalid_quotes=0,
        calc_error_count=0,
        valid_minutes=10,
        lifespan_stats=lifespan_stats
    )

    assert "⏱ <b>อายุโอกาสทำกำไร (Lifespan):</b> เฉลี่ย 18 วิ | สั้นสุด 3 วิ | นานสุด 42 วิ (จาก 5 รอบที่เกิด)" in report
    assert len(report) < 3800
    print("✅ Telegram Report Lifespan Rendering & Character Limit (< 3,800) Verified!")


def test_lifespan_tracker_aware_vs_naive_finalize():
    """
    Verifies that OpportunityLifespanTracker handles comparisons between
    offset-naive timestamps (e.g. from Turso string logs or strptime) and
    offset-aware timestamps (e.g. datetime.datetime.now(BANGKOK_TZ)) without
    throwing TypeError: can't compare offset-naive and offset-aware datetimes.
    """
    tracker = OpportunityLifespanTracker()

    # 1. Update with naive string timestamp
    opp = {"strategy": "Conversion", "target": "Strike 920 & S50Z26", "net_pts": 0.50}
    tracker.update([opp], "2026-11-20 10:00:00")
    assert len(tracker.active_opportunities) == 1

    # 2. Update with naive datetime object
    naive_dt = datetime.datetime(2026, 11, 20, 10, 0, 15)
    tracker.update([opp], naive_dt)

    # 3. Finalize with offset-aware datetime (BANGKOK_TZ)
    # Simulate session end 30 seconds after first_seen
    end_dt_aware = datetime.datetime(2026, 11, 20, 10, 0, 30, tzinfo=BANGKOK_TZ)
    tracker.finalize(end_dt_aware)

    assert len(tracker.active_opportunities) == 0
    assert len(tracker.closed_durations) == 1
    assert math.isclose(tracker.closed_durations[0], 30.0, abs_tol=1e-7)

    # 4. Also test finalize with live datetime.datetime.now(BANGKOK_TZ) directly
    tracker.update([opp], "2026-11-20 00:00:00")
    tracker.finalize(datetime.datetime.now(BANGKOK_TZ))
    assert len(tracker.active_opportunities) == 0
    assert len(tracker.closed_durations) == 2
    assert tracker.closed_durations[1] >= 1.0

    stats = tracker.get_stats()
    assert stats["has_data"] is True
    assert stats["count"] == 2
    print("✅ Opportunity Lifespan Tracker Aware vs Naive Finalize Verified!")


if __name__ == "__main__":
    test_scan_and_friction_breakdown()
    test_best_of_minute_stats()
    test_telegram_message_formatting_and_length()
    test_telegram_message_no_profitable_opps()
    test_zero_traded_volume_with_valid_quotes()
    test_dynamic_thor_rate_selection()
    test_opportunity_lifespan_tracker()
    test_telegram_message_with_lifespan()
    test_lifespan_tracker_aware_vs_naive_finalize()
    print("\n🎉 ALL UNIT TESTS PASSED SUCCESSFULLY!")
