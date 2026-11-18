"""
Unit test for enhanced session summary in arb_scanner.py:
- Verifies Best-of-Minute Distribution stats (Max, P90, Avg, SD, % Bars near breakeven)
- Verifies Strategy Breakdown (Near-Miss Top 1 for Parity, Vertical, Calendar)
- Verifies Friction Breakdown math (Theoretical Mid Edge, Spread Slippage Cost, Commission, Net Profit)
- Verifies Telegram message length (< 3,800 characters)
"""

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
    COMMISSION_2LEG_PTS
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
            f"S50{front_series}": {"bid": f1_bid, "ask": f1_ask, "volume": f1_vol},
            f"S50{next_series}": {"bid": f2_bid, "ask": f2_ask, "volume": f2_vol}
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

            options[(front_series, k, "C")] = {"bid": c_bid, "ask": c_ask, "volume": 20.0, "strike": k, "series": front_series, "option_type": "C"}
            options[(front_series, k, "P")] = {"bid": p_bid, "ask": p_ask, "volume": 20.0, "strike": k, "series": front_series, "option_type": "P"}

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


if __name__ == "__main__":
    test_scan_and_friction_breakdown()
    test_best_of_minute_stats()
    test_telegram_message_formatting_and_length()
    test_telegram_message_no_profitable_opps()
    print("\n🎉 ALL UNIT TESTS PASSED SUCCESSFULLY!")
