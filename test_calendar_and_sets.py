"""
Unit tests for Calendar Spread detection fixes (W3/W4) และ executable-set guard (S1)

W3: เดิม `continue` ตอน Case 1 ไม่ผ่านเงื่อนไข depth ทำให้ Case 2 ไม่ถูกประเมินเลย
W4: เดิม baseline (rolling mean/std) รวมแท่งที่กำลังทดสอบ → spike กลบตัวเองใน mu
    และ z-score ถูก cap ที่ ~1.79 เมื่อ min_periods=5 → detector ยิงไม่ได้ตอนต้นรอบ
S1: เดิม int(min(depth)) ทำให้ max_sets = 0 แต่ opp ยังถูกส่งออกไป
"""
import unittest

from arb_scanner import _exec_sets, scan_calendar_spreads

TICK = 0.5      # ระยะ bid/ask จาก mid ของ futures แต่ละซีรีส์


def _make_minute(ts, mid_front, mid_next, f1_bid_vol=50.0, f1_ask_vol=50.0,
                 f2_bid_vol=50.0, f2_ask_vol=50.0):
    return ts, {
        "spot": 920.0,
        "futures": {
            "S50Z26": {"symbol": "S50Z26", "bid": mid_front - TICK, "ask": mid_front + TICK,
                       "bid_vol": f1_bid_vol, "ask_vol": f1_ask_vol,
                       "last": mid_front, "volume": 10.0, "oi": 100.0, "last_update_ts": ts},
            "S50H27": {"symbol": "S50H27", "bid": mid_next - TICK, "ask": mid_next + TICK,
                       "bid_vol": f2_bid_vol, "ask_vol": f2_ask_vol,
                       "last": mid_next, "volume": 10.0, "oi": 100.0, "last_update_ts": ts},
        },
        "options": {},
    }


def _flat_history(n=20):
    """สร้าง n นาทีที่ spread (~10 จุด) นิ่ง มี noise เล็กน้อยให้ std > 0"""
    grouped = {}
    for i in range(n):
        noise = 0.05 if i % 2 == 0 else -0.05
        ts = f"2026-11-20 10:{i:02d}:03"
        ts, frame = _make_minute(ts, 920.0 + noise, 910.0)
        grouped[ts] = frame
    return grouped


class TestExecutableSets(unittest.TestCase):

    def test_fractional_depth_is_not_executable(self):
        self.assertEqual(_exec_sets(0.5, 3.0), 0)

    def test_normal_case_returns_min_depth(self):
        self.assertEqual(_exec_sets(2.0, 5.0, 3.0), 2)

    def test_zero_or_none_depth_is_not_executable(self):
        self.assertEqual(_exec_sets(0.0, 5.0), 0)
        self.assertEqual(_exec_sets(None, 5.0), 0)


class TestCalendarSpreadFix(unittest.TestCase):

    def test_spike_high_is_detected_with_excluding_baseline(self):
        grouped = _flat_history()
        ts = "2026-11-20 10:20:03"
        _, frame = _make_minute(ts, 925.0, 910.0)   # spread 15 vs baseline 10
        grouped[ts] = frame

        opps, _, _, _, _ = scan_calendar_spreads(grouped, "Z26", "H27")
        highs = [o for o in opps if "Overextended High" in o["strategy"]]
        self.assertEqual(len(highs), 1, "spike ขึ้นต้องถูกตรวจพบ")
        self.assertGreater(highs[0]["net_pts"], 0)
        self.assertEqual(highs[0]["time"], ts)

    def test_case2_still_evaluated_when_front_bid_depth_is_zero(self):
        """
        W3 regression: front bid ไม่มี depth (ขาย front ไม่ได้) แต่ Case 2
        (buy front @ ask, sell next @ bid) ยังเคาะได้ → ต้องยังถูกประเมิน
        """
        grouped = _flat_history()
        ts = "2026-11-20 10:20:03"
        # spread แคบลง (spike low) + front bid depth = 0
        _, frame = _make_minute(ts, 915.0, 910.0, f1_bid_vol=0.0, f1_ask_vol=80.0)
        grouped[ts] = frame

        opps, _, _, _, _ = scan_calendar_spreads(grouped, "Z26", "H27")
        lows = [o for o in opps if "Overextended Low" in o["strategy"]]
        self.assertEqual(len(lows), 1,
                         "Case 2 ต้องถูกประเมินแม้ bid ของ front ไม่มี depth (บั๊ก continue เดิม)")
        self.assertGreater(lows[0]["net_pts"], 0)

    def test_baseline_excludes_bar_under_test(self):
        """
        W4: ถ้า baseline รวมแท่งที่ทดสอบ ค่า z สูงสุดที่ min_periods=5 ทำได้คือ 1.789
        (< 2.0) ทำให้ยิงไม่ได้เลย — ทดสอบว่ามี ≥6 แท่งแล้ว spike ยังตรวจพบ
        """
        grouped = _flat_history(6)
        ts = "2026-11-20 10:06:03"
        _, frame = _make_minute(ts, 925.0, 910.0)
        grouped[ts] = frame

        opps, _, _, _, _ = scan_calendar_spreads(grouped, "Z26", "H27")
        self.assertTrue(
            [o for o in opps if "Overextended High" in o["strategy"]],
            "spike ที่แท่งที่ 7 ต้องตรวจพบได้ (เดิม z ถูก cap จาก baseline ที่รวมแท่งตัวเอง)"
        )


if __name__ == "__main__":
    unittest.main()
