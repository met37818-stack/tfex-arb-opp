"""
Unit tests for THOR unit handling (Critical C2) and REST verification gating (Critical C1).

ที่มา: THOR จาก BOT เป็น percent p.a. เสมอ ตรรกะเดิมที่เดาหน่วยจาก magnitude
(`val/100 if val > 0.5 else val`) ทำให้ THOR 0.45% กลายเป็น 45% → exp(-r*T) เพี้ยน
→ ระบบยิง parity arbitrage ปลอมหลายจุดบน deep strike
"""
import math
import unittest

from arb_scanner import (
    COMMISSION_3LEG_PTS,
    THOR_MAX_PCT,
    _to_decimal_rate,
    select_dynamic_thor_rate,
    verify_live_quotes_before_alert,
)


class TestTHORRateUnit(unittest.TestCase):

    def test_below_half_percent_still_treated_as_percent(self):
        """0.45% ต้องกลายเป็น 0.0045 — ไม่ใช่ 0.45 (บั๊กเดิมที่ทำให้ disc เพี้ยน)"""
        record = {"overnight": 1.50, "1m": 0.45, "3m": 0.90, "6m": 1.20}
        rate, tenor = select_dynamic_thor_rate(record, dte=30)
        self.assertEqual(tenor, "1M")
        self.assertAlmostEqual(rate, 0.0045, places=9)
        self.assertLess(rate, 0.01)

    def test_normal_rate_unchanged(self):
        record = {"overnight": 1.50, "1m": 0.994, "3m": 1.10, "6m": 1.30}
        rate, _ = select_dynamic_thor_rate(record, dte=30)
        self.assertAlmostEqual(rate, 0.00994, places=9)

    def test_tiny_rate_not_inflated(self):
        record = {"overnight": 0.10, "1m": 0.05, "3m": 0.08, "6m": 0.09}
        rate, _ = select_dynamic_thor_rate(record, dte=200)
        self.assertAlmostEqual(rate, 0.0009, places=9)  # 0.09% -> 0.0009
        self.assertLess(rate, 0.001)

    def test_absurd_rate_raises_instead_of_fabricating_edges(self):
        with self.assertRaises(ValueError):
            select_dynamic_thor_rate({"overnight": 1.0, "1m": 45.0}, dte=30)
        with self.assertRaises(ValueError):
            _to_decimal_rate(THOR_MAX_PCT + 0.1, "1m")

    def test_none_and_zero_become_none(self):
        self.assertIsNone(_to_decimal_rate(None))
        self.assertIsNone(_to_decimal_rate(0.0))
        self.assertIsNone(_to_decimal_rate(-1.0))

    def test_discount_factor_error_was_material(self):
        """
        ยืนยันขนาดความเสียหายของบั๊กเดิม: ที่ |F-K|=50, DTE=90, THOR 0.45%
        disc ที่ผิดทำให้ edge ปลอม > commission 3 ขา (0.60 จุด)
        """
        T = 90 / 365.0
        disc_ok = math.exp(-(0.45 / 100.0) * T)
        disc_buggy = math.exp(-0.45 * T)
        fake_edge = (disc_ok - disc_buggy) * 50.0
        self.assertGreater(fake_edge, 5.0)
        self.assertGreater(fake_edge, COMMISSION_3LEG_PTS)


class TestRESTGateFailsClosed(unittest.TestCase):
    """
    Critical C1: เดิมถ้า Settrade ไม่พร้อม (secret หมดอายุ/เน็ตหลุด) gate จะ
    'bypass' แล้วคืน True ทำให้ alert ออกจากราคา DB ที่เก่าได้ 15 นาที
    ทั้งที่ข้อความยังเขียนว่า 'REST Verified ✅'
    """

    OPP = {
        "strategy": "Conversion (Sell C, Buy P, Buy Fut)",
        "target": "Strike 900 & S50Z26",
        "strike": 900.0,
        "series": "Z26",
        "net_pts": 0.75,
        "gross_pts": 1.35,
        "commission_pts": COMMISSION_3LEG_PTS,
        "disc_factor": 0.9992,
        "max_sets": 5,
        "order_legs": [
            {"symbol": "S50Z26C900", "side": "S", "price": 20.0, "depth": 5.0, "is_futures": False},
            {"symbol": "S50Z26P900", "side": "L", "price": 18.0, "depth": 5.0, "is_futures": False},
            {"symbol": "S50Z26", "side": "L", "price": 920.0, "depth": 5.0, "is_futures": True},
        ],
    }

    def test_missing_market_data_suppresses_instead_of_bypassing(self):
        verified, opp = verify_live_quotes_before_alert(
            opp=dict(self.OPP), market_data=None, require_rest=True
        )
        self.assertFalse(verified, "ต้องไม่ยิง alert ที่ยังไม่ผ่าน REST verification")
        self.assertFalse(opp.get("rest_verified", True))

    def test_explicit_opt_out_is_possible_and_labelled(self):
        verified, opp = verify_live_quotes_before_alert(
            opp=dict(self.OPP), market_data=None, require_rest=False
        )
        self.assertTrue(verified, "โหมด replay/offline ต้องยังรันได้เมื่อสั่ง opt-out ชัดเจน")
        self.assertFalse(opp.get("rest_verified", True), "ต้องทำเครื่องหมายว่าไม่ได้ verify")

    def test_verified_opp_is_flagged(self):
        class _MD:
            def get_quote_symbol(self, sym):
                return {"bid": 20.0, "ask": 18.0, "bid_vol": 5, "ask_vol": 5,
                        "last": 19.0, "volume": 100}
        # ใช้ quote ที่ยืนยันแล้วว่า net > 0 ผ่าน _recalculate_opp_math
        class _MD2(_MD):
            def get_quote_symbol(self, sym):
                return {"bid": 30.0, "ask": 10.0, "bid_vol": 5, "ask_vol": 5,
                        "last": 19.0, "volume": 100}
        verified, opp = verify_live_quotes_before_alert(
            opp=dict(self.OPP), market_data=_MD2(), require_rest=False
        )
        self.assertTrue(verified)
        self.assertTrue(opp.get("rest_verified"))


if __name__ == "__main__":
    unittest.main()
