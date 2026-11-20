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
import openpyxl

from arb_scanner import (
    generate_streaming_multi_order_xlsx,
    resolve_order_legs
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


if __name__ == "__main__":
    unittest.main()
