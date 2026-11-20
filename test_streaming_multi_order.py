"""
Unit tests for Settrade Streaming Multiple Order Excel (.xlsx) generation in arb_scanner.py:
- Verifies exact columns: ["Symbol", "Side", "Volume", "Price", "Position", "Type", "Validity", "Iceberg"]
- Verifies safety constraints: Volume = 1, Position = 'A', Type = 'Limit', Validity = 'FOK', Iceberg is None
- Verifies critical execution order: Options sorted ascending by depth (lowest depth first), Futures ALWAYS last
- Verifies filename pattern: ARB_{Strategy}_{Target}_{HHMMSS}.xlsx
- Verifies fallback leg resolution from text strings
"""

import io
import unittest
from unittest.mock import MagicMock
import openpyxl

from arb_scanner import (
    generate_streaming_multi_order_xlsx,
    resolve_order_legs,
    format_trade_log_line,
    verify_live_quotes_before_alert,
    format_live_1min_alert
)


class TestStreamingMultiOrder(unittest.TestCase):

    def test_parity_conversion_ordering_and_values(self):
        """
        Conversion: Sell Call, Buy Put, Buy Futures.
        Put is least liquid (Depth: 5), Call is next (Depth: 15), Futures is most liquid (Depth: 50).
        Execution Order MUST BE:
        1. Put (Depth: 5)
        2. Call (Depth: 15)
        3. Futures (Depth: 50) - ALWAYS LAST
        """
        opp = {
            "strategy": "Conversion (Sell C, Buy P, Buy Fut)",
            "target": "Strike 920 & S50Z26",
            "series": "Z26",
            "strike": 920.0,
            "net_pts": 0.80,
            "time": "10:15:30",
            "order_legs": [
                {"symbol": "S50Z26C920", "side": "S", "price": 12.50, "depth": 15.0, "is_futures": False},
                {"symbol": "S50Z26P920", "side": "L", "price": 10.20, "depth": 5.0, "is_futures": False},
                {"symbol": "S50Z26", "side": "L", "price": 921.40, "depth": 50.0, "is_futures": True}
            ]
        }

        file_bytes, filename = generate_streaming_multi_order_xlsx(opp)

        # 1. Verify filename
        self.assertTrue(filename.startswith("ARB_Conversion_Sell_C_Buy_P_Buy_Fut_Strike_920_S50Z26_"))
        self.assertTrue(filename.endswith(".xlsx"))
        self.assertIn("101530", filename)

        # 2. Inspect Excel content
        wb = openpyxl.load_workbook(io.BytesIO(file_bytes))
        ws = wb.active
        self.assertEqual(ws.title, "MultipleOrder")

        rows = list(ws.iter_rows(values_only=True))
        # Header + 3 legs = 4 rows
        self.assertEqual(len(rows), 4)

        expected_headers = ("Symbol", "Side", "Volume", "Price", "Position", "Type", "Validity", "Iceberg")
        self.assertEqual(rows[0], expected_headers)

        # Row 1: Put (least liquid option, depth 5)
        self.assertEqual(rows[1][0], "S50Z26P920")
        self.assertEqual(rows[1][1], "L")
        self.assertEqual(rows[1][2], 1)
        self.assertAlmostEqual(rows[1][3], 10.20)
        self.assertEqual(rows[1][4], "A")
        self.assertEqual(rows[1][5], "Limit")
        self.assertEqual(rows[1][6], "FOK")
        self.assertIsNone(rows[1][7])

        # Row 2: Call (depth 15)
        self.assertEqual(rows[2][0], "S50Z26C920")
        self.assertEqual(rows[2][1], "S")
        self.assertEqual(rows[2][2], 1)
        self.assertAlmostEqual(rows[2][3], 12.50)
        self.assertEqual(rows[2][4], "A")
        self.assertEqual(rows[2][5], "Limit")
        self.assertEqual(rows[2][6], "FOK")
        self.assertIsNone(rows[2][7])

        # Row 3: Futures (MUST BE LAST, depth 50)
        self.assertEqual(rows[3][0], "S50Z26")
        self.assertEqual(rows[3][1], "L")
        self.assertEqual(rows[3][2], 1)
        self.assertAlmostEqual(rows[3][3], 921.40)
        self.assertEqual(rows[3][4], "A")
        self.assertEqual(rows[3][5], "Limit")
        self.assertEqual(rows[3][6], "FOK")
        self.assertIsNone(rows[3][7])

    def test_parity_reversal_ordering_and_values(self):
        """
        Reversal: Buy Call, Sell Put, Sell Futures.
        Call depth: 8, Put depth: 25, Futures depth: 100.
        Execution Order: Call (depth 8) -> Put (depth 25) -> Futures (depth 100, last).
        """
        opp = {
            "strategy": "Reversal (Buy C, Sell P, Sell Fut)",
            "target": "Strike 930 & S50Z26",
            "series": "Z26",
            "strike": 930.0,
            "net_pts": 0.50,
            "time": "14:20:00",
            "order_legs": [
                {"symbol": "S50Z26C930", "side": "L", "price": 8.00, "depth": 8.0, "is_futures": False},
                {"symbol": "S50Z26P930", "side": "S", "price": 14.50, "depth": 25.0, "is_futures": False},
                {"symbol": "S50Z26", "side": "S", "price": 923.00, "depth": 100.0, "is_futures": True}
            ]
        }

        file_bytes, filename = generate_streaming_multi_order_xlsx(opp)
        wb = openpyxl.load_workbook(io.BytesIO(file_bytes))
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))

        # Row 1: Call (depth 8)
        self.assertEqual(rows[1][0], "S50Z26C930")
        self.assertEqual(rows[1][1], "L")
        self.assertEqual(rows[1][2], 1)
        self.assertAlmostEqual(rows[1][3], 8.00)

        # Row 2: Put (depth 25)
        self.assertEqual(rows[2][0], "S50Z26P930")
        self.assertEqual(rows[2][1], "S")
        self.assertEqual(rows[2][2], 1)
        self.assertAlmostEqual(rows[2][3], 14.50)

        # Row 3: Futures (MUST BE LAST)
        self.assertEqual(rows[3][0], "S50Z26")
        self.assertEqual(rows[3][1], "S")
        self.assertEqual(rows[3][2], 1)
        self.assertAlmostEqual(rows[3][3], 923.00)

    def test_vertical_spread_ordering(self):
        """
        Vertical Spread: 2 Options legs.
        Call 910 has depth 3, Call 900 has depth 12.
        Order must be Call 910 (3) -> Call 900 (12).
        """
        opp = {
            "strategy": "Call Monotonicity Violation",
            "target": "Call 900/910 (Z26)",
            "series": "Z26",
            "net_pts": 0.45,
            "time": "11:05:15",
            "order_legs": [
                {"symbol": "S50Z26C900", "side": "L", "price": 20.00, "depth": 12.0, "is_futures": False},
                {"symbol": "S50Z26C910", "side": "S", "price": 21.00, "depth": 3.0, "is_futures": False}
            ]
        }

        file_bytes, filename = generate_streaming_multi_order_xlsx(opp)
        wb = openpyxl.load_workbook(io.BytesIO(file_bytes))
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))

        self.assertEqual(len(rows), 3)
        # Row 1: least liquid (depth 3)
        self.assertEqual(rows[1][0], "S50Z26C910")
        self.assertEqual(rows[1][1], "S")
        # Row 2: depth 12
        self.assertEqual(rows[2][0], "S50Z26C900")
        self.assertEqual(rows[2][1], "L")

    def test_calendar_spread_ordering(self):
        """
        Calendar Spread: 2 Futures legs.
        Next quarter H27 has depth 4, Front quarter Z26 has depth 30.
        Order must be H27 (depth 4) -> Z26 (depth 30).
        """
        opp = {
            "strategy": "Calendar Spread Spike (Overextended High)",
            "target": "S50Z26 - S50H27",
            "series": "Z26-H27",
            "net_pts": 0.35,
            "time": "15:45:00",
            "order_legs": [
                {"symbol": "S50Z26", "side": "S", "price": 922.00, "depth": 30.0, "is_futures": True},
                {"symbol": "S50H27", "side": "L", "price": 924.50, "depth": 4.0, "is_futures": True}
            ]
        }

        file_bytes, filename = generate_streaming_multi_order_xlsx(opp)
        wb = openpyxl.load_workbook(io.BytesIO(file_bytes))
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))

        self.assertEqual(len(rows), 3)
        # S50H27 is less liquid (depth 4) -> first
        self.assertEqual(rows[1][0], "S50H27")
        self.assertEqual(rows[1][1], "L")
        # S50Z26 is more liquid (depth 30) -> second
        self.assertEqual(rows[2][0], "S50Z26")
        self.assertEqual(rows[2][1], "S")

    def test_fallback_leg_resolution_from_text(self):
        """
        Tests resolve_order_legs when opportunity does not contain 'order_legs'
        and must parse from human-readable 'legs' list.
        """
        opp = {
            "strategy": "Conversion (Sell C, Buy P, Buy Fut)",
            "target": "Strike 920 & S50Z26",
            "series": "Z26",
            "strike": 920.0,
            "legs": [
                "Sell Call 920 @ Bid <code>12.00</code> (Depth: 25)",
                "Buy Put 920 @ Ask <code>10.00</code> (Depth: 6)",
                "Buy Futures S50Z26 @ Ask <code>921.00</code> (Depth: 50)"
            ]
        }

        legs = resolve_order_legs(opp)
        self.assertEqual(len(legs), 3)

        file_bytes, filename = generate_streaming_multi_order_xlsx(opp)
        wb = openpyxl.load_workbook(io.BytesIO(file_bytes))
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))

        # Put has Depth 6 -> Row 1
        self.assertEqual(rows[1][0], "S50Z26P920")
        self.assertEqual(rows[1][1], "L")
        self.assertAlmostEqual(rows[1][3], 10.00)

        # Call has Depth 25 -> Row 2
        self.assertEqual(rows[2][0], "S50Z26C920")
        self.assertEqual(rows[2][1], "S")
        self.assertAlmostEqual(rows[2][3], 12.00)

        # Futures has Depth 50 -> Row 3 (LAST)
        self.assertEqual(rows[3][0], "S50Z26")
        self.assertEqual(rows[3][1], "L")
        self.assertAlmostEqual(rows[3][3], 921.00)

    def test_format_trade_log_line_2leg(self):
        """
        Tests format_trade_log_line for 2-leg opportunity (Vertical Spread / Call Monotonicity).
        Format: 📋 Log: {Strategy} | {Leg1_Sym} {Side1} {Price1:.2f} | {Leg2_Sym} {Side2} {Price2:.2f} | +{NetTHB:.0f}
        """
        opp = {
            "strategy": "Call Monotonicity Violation",
            "net_pts": 1.60,
            "order_legs": [
                {"symbol": "S50Z26C910", "side": "L", "price": 25.00, "depth": 10.0, "is_futures": False},
                {"symbol": "S50Z26C920", "side": "S", "price": 27.00, "depth": 8.0, "is_futures": False}
            ]
        }
        line = format_trade_log_line(opp)
        self.assertEqual(line, "📋 Log: Call Monotonicity Violation | S50Z26C910 L 25.00 | S50Z26C920 S 27.00 | +320")

    def test_format_trade_log_line_3leg(self):
        """
        Tests format_trade_log_line for 3-leg opportunity (Put-Call Parity Conversion).
        Format: 📋 Log: {Strategy} | {Leg1_Sym} {Side1} {Price1:.2f} | {Leg2_Sym} {Side2} {Price2:.2f} | {Leg3_Sym} {Side3} {Price3:.2f} | +{NetTHB:.0f}
        """
        opp = {
            "strategy": "Conversion (Sell C, Buy P, Buy Fut)",
            "net_pts": 1.50,
            "order_legs": [
                {"symbol": "S50Z26C920", "side": "S", "price": 22.50, "depth": 15.0, "is_futures": False},
                {"symbol": "S50Z26P920", "side": "L", "price": 15.00, "depth": 5.0, "is_futures": False},
                {"symbol": "S50Z26", "side": "L", "price": 920.00, "depth": 50.0, "is_futures": True}
            ]
        }
        line = format_trade_log_line(opp)
        self.assertEqual(line, "📋 Log: Conversion | S50Z26C920 S 22.50 | S50Z26P920 L 15.00 | S50Z26 L 920.00 | +300")

    def test_format_live_1min_alert_includes_trade_log(self):
        """
        Tests that format_live_1min_alert includes the 📋 Log: trade line in its footer.
        """
        opp = {
            "strategy": "Conversion (Sell C, Buy P, Buy Fut)",
            "target": "Strike 920 & S50Z26",
            "time": "10:15:00",
            "gross_pts": 2.10,
            "commission_pts": 0.60,
            "net_pts": 1.50,
            "max_sets": 5,
            "legs": [
                "Sell Call 920 @ Bid <code>22.50</code> (Depth: 15)",
                "Buy Put 920 @ Ask <code>15.00</code> (Depth: 5)",
                "Buy Futures S50Z26 @ Ask <code>920.00</code> (Depth: 50)"
            ],
            "order_legs": [
                {"symbol": "S50Z26C920", "side": "S", "price": 22.50, "depth": 15.0, "is_futures": False},
                {"symbol": "S50Z26P920", "side": "L", "price": 15.00, "depth": 5.0, "is_futures": False},
                {"symbol": "S50Z26", "side": "L", "price": 920.00, "depth": 50.0, "is_futures": True}
            ]
        }
        alert_msg = format_live_1min_alert(
            trade_date="2026-11-20",
            bar_ts="10:15:00",
            profitable_opps=[opp],
            thor_rate=0.0225,
            dte=35,
            T_years=35 / 365.0
        )
        self.assertIn("<code>📋 Log: Conversion | S50Z26C920 S 22.50 | S50Z26P920 L 15.00 | S50Z26 L 920.00 | +300</code>", alert_msg)

    def test_verify_live_quotes_success_and_cache_update(self):
        """
        Case 1: REST confirms real profit:
        - REST returns valid bid/ask and vol >= 1
        - net_pts > 0 after recalculation
        - Returns True, updated_opp with fresh prices and depths
        - running_options and running_futures are updated in memory
        - generate_streaming_multi_order_xlsx outputs updated prices
        """
        opp = {
            "strategy": "Conversion (Sell C, Buy P, Buy Fut)",
            "target": "Strike 920 & S50Z26",
            "series": "Z26",
            "strike": 920.0,
            "gross_pts": 2.10,
            "commission_pts": 0.60,
            "net_pts": 1.50,
            "max_sets": 5,
            "disc_factor": 1.0,
            "order_legs": [
                {"symbol": "S50Z26C920", "side": "S", "price": 22.00, "depth": 5.0, "is_futures": False},
                {"symbol": "S50Z26P920", "side": "L", "price": 15.00, "depth": 5.0, "is_futures": False},
                {"symbol": "S50Z26", "side": "L", "price": 920.00, "depth": 10.0, "is_futures": True}
            ]
        }

        # Mock REST API returns slightly shifted prices but still profitable:
        # Call Bid = 23.00 (vol = 12), Put Ask = 14.80 (vol = 8), Fut Ask = 920.20 (vol = 20)
        # Recalculated Gross = (23.00 - 14.80) - (920.20 - 920.0) * 1.0 = 8.20 - 0.20 = 8.00
        # Recalculated Net = 8.00 - 0.60 = 7.40 pts
        mock_market_data = MagicMock()
        quotes_map = {
            "S50Z26C920": {"bid": 23.00, "ask": 23.50, "bid_vol": 12.0, "ask_vol": 15.0, "last": 23.0, "volume": 100},
            "S50Z26P920": {"bid": 14.50, "ask": 14.80, "bid_vol": 10.0, "ask_vol": 8.0, "last": 14.8, "volume": 50},
            "S50Z26": {"bid": 920.10, "ask": 920.20, "bid_vol": 30.0, "ask_vol": 20.0, "last": 920.1, "volume": 500}
        }
        mock_market_data.get_quote_symbol.side_effect = lambda sym: quotes_map[sym]

        running_futures = {
            "S50Z26": {"symbol": "S50Z26", "bid": 919.0, "ask": 920.0, "bid_vol": 5.0, "ask_vol": 5.0, "last_update_ts": "old_ts"}
        }
        running_options = {
            ("Z26", 920.0, "C"): {"symbol": "S50Z26C920", "bid": 21.0, "ask": 22.0, "bid_vol": 2.0, "ask_vol": 2.0, "last_update_ts": "old_ts"},
            ("Z26", 920.0, "P"): {"symbol": "S50Z26P920", "bid": 14.0, "ask": 16.0, "bid_vol": 2.0, "ask_vol": 2.0, "last_update_ts": "old_ts"}
        }

        verified, updated_opp = verify_live_quotes_before_alert(
            opp=opp,
            market_data=mock_market_data,
            running_futures=running_futures,
            running_options=running_options
        )

        self.assertTrue(verified)
        self.assertAlmostEqual(updated_opp["gross_pts"], 8.00)
        self.assertAlmostEqual(updated_opp["net_pts"], 7.40)
        self.assertEqual(updated_opp["max_sets"], 8)  # min(12, 8, 20)

        # Check in-memory caches updated with fresh REST quotes
        self.assertEqual(running_futures["S50Z26"]["ask"], 920.20)
        self.assertEqual(running_futures["S50Z26"]["ask_vol"], 20.0)
        self.assertEqual(running_options[("Z26", 920.0, "C")]["bid"], 23.00)
        self.assertEqual(running_options[("Z26", 920.0, "C")]["bid_vol"], 12.0)
        self.assertEqual(running_options[("Z26", 920.0, "P")]["ask"], 14.80)
        self.assertEqual(running_options[("Z26", 920.0, "P")]["ask_vol"], 8.0)

        # Check Streaming Multiple Order xlsx contains updated REST prices
        file_bytes, filename = generate_streaming_multi_order_xlsx(updated_opp)
        wb = openpyxl.load_workbook(io.BytesIO(file_bytes))
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        # Put Ask 14.80 (least depth 8)
        self.assertEqual(rows[1][0], "S50Z26P920")
        self.assertAlmostEqual(rows[1][3], 14.80)
        # Call Bid 23.00 (depth 12)
        self.assertEqual(rows[2][0], "S50Z26C920")
        self.assertAlmostEqual(rows[2][3], 23.00)
        # Fut Ask 920.20 (depth 20)
        self.assertEqual(rows[3][0], "S50Z26")
        self.assertAlmostEqual(rows[3][3], 920.20)

    def test_verify_live_quotes_ghost_zero_volume_filtered(self):
        """
        Case 2a: REST detects ghost quote due to 0 volume depth (liquidity already taken):
        - One of the legs has vol == 0
        - Returns False (suppress alert)
        - Cache is overwritten with live REST data (flushing the ghost price)
        """
        opp = {
            "strategy": "Call Monotonicity Violation",
            "target": "Call 910/920 (Z26)",
            "series": "Z26",
            "gross_pts": 2.00,
            "commission_pts": 0.40,
            "net_pts": 1.60,
            "order_legs": [
                {"symbol": "S50Z26C910", "side": "L", "price": 25.00, "depth": 10.0, "is_futures": False},
                {"symbol": "S50Z26C920", "side": "S", "price": 27.00, "depth": 8.0, "is_futures": False}
            ]
        }

        # Mock REST API: S50Z26C920 Bid volume is 0.0 (someone took it)
        mock_market_data = MagicMock()
        quotes_map = {
            "S50Z26C910": {"bid": 24.50, "ask": 25.00, "bid_vol": 10.0, "ask_vol": 10.0, "last": 25.0, "volume": 100},
            "S50Z26C920": {"bid": 27.00, "ask": 27.50, "bid_vol": 0.0, "ask_vol": 5.0, "last": 27.0, "volume": 50}
        }
        mock_market_data.get_quote_symbol.side_effect = lambda sym: quotes_map[sym]

        running_options = {
            ("Z26", 920.0, "C"): {"symbol": "S50Z26C920", "bid": 27.0, "ask": 27.5, "bid_vol": 8.0, "ask_vol": 5.0, "last_update_ts": "old_ts"}
        }

        verified, _ = verify_live_quotes_before_alert(
            opp=opp,
            market_data=mock_market_data,
            running_options=running_options
        )

        # Verification must fail and suppress alert
        self.assertFalse(verified)

        # But cache MUST be overwritten with live REST data (bid_vol = 0) to flush ghost price
        self.assertEqual(running_options[("Z26", 920.0, "C")]["bid_vol"], 0.0)

    def test_verify_live_quotes_ghost_unprofitable_filtered(self):
        """
        Case 2b: REST prices moved such that net_pts <= 0 after commission:
        - Market quote moved before alert could be dispatched
        - Recalculated net_pts <= 0
        - Returns False (suppress alert)
        - Cache is updated with fresh quotes
        """
        opp = {
            "strategy": "Call Monotonicity Violation",
            "target": "Call 910/920 (Z26)",
            "series": "Z26",
            "gross_pts": 2.00,
            "commission_pts": 0.40,
            "net_pts": 1.60,
            "order_legs": [
                {"symbol": "S50Z26C910", "side": "L", "price": 25.00, "depth": 10.0, "is_futures": False},
                {"symbol": "S50Z26C920", "side": "S", "price": 27.00, "depth": 8.0, "is_futures": False}
            ]
        }

        # Mock REST API: S50Z26C920 Bid dropped to 25.20, S50Z26C910 Ask rose to 25.10
        # Recalculated Gross = 25.20 - 25.10 = 0.10 pts
        # Recalculated Net = 0.10 - 0.40 = -0.30 pts <= 0
        mock_market_data = MagicMock()
        quotes_map = {
            "S50Z26C910": {"bid": 24.80, "ask": 25.10, "bid_vol": 10.0, "ask_vol": 10.0, "last": 25.0, "volume": 100},
            "S50Z26C920": {"bid": 25.20, "ask": 25.60, "bid_vol": 8.0, "ask_vol": 5.0, "last": 25.2, "volume": 50}
        }
        mock_market_data.get_quote_symbol.side_effect = lambda sym: quotes_map[sym]

        running_options = {
            ("Z26", 920.0, "C"): {"symbol": "S50Z26C920", "bid": 27.0, "ask": 27.5, "bid_vol": 8.0, "ask_vol": 5.0, "last_update_ts": "old_ts"}
        }

        verified, _ = verify_live_quotes_before_alert(
            opp=opp,
            market_data=mock_market_data,
            running_options=running_options
        )

        self.assertFalse(verified)
        # In-memory cache is updated with new price 25.20
        self.assertEqual(running_options[("Z26", 920.0, "C")]["bid"], 25.20)


if __name__ == "__main__":
    unittest.main()
