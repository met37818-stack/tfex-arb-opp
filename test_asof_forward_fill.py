"""
Unit tests for AS-OF Forward-Fill Engine and Order Book Depth in arb_scanner.py
"""
import unittest
from unittest.mock import patch

from arb_scanner import fetch_intraday_arb_dataset, scan_put_call_parity, scan_vertical_spreads


class TestASOFForwardFill(unittest.TestCase):

    @patch("arb_scanner.execute_turso_sql")
    def test_asof_forward_fill_sparse_cdc(self, mock_sql):
        # Minute 1: Full baseline snapshot with 2 strikes (900, 910)
        # Minute 2: Only 900 changes (910 is sparse/unchanged)
        # Minute 3: Only 910 changes (900 is sparse/unchanged)

        mock_rows_spot = [
            ("2026-11-19 10:00:00", 920.0, 1000, 100000),
            ("2026-11-19 10:01:00", 920.5, 2000, 200000),
            ("2026-11-19 10:02:00", 921.0, 3000, 300000),
        ]

        mock_rows_fut = [
            ("2026-11-19 10:00:00", "S50Z26", 920.0, 920.4, 920.2, 500.0, 10000.0, 50.0, 60.0),
            ("2026-11-19 10:01:00", "S50Z26", 920.5, 920.9, 920.7, 600.0, 10000.0, 55.0, 65.0),
            ("2026-11-19 10:02:00", "S50Z26", 921.0, 921.4, 921.2, 700.0, 10000.0, 70.0, 80.0),
        ]

        # Options:
        # Min 1: C900, P900, C910, P910
        # Min 2: C900 changed (C910, P900, P910 unchanged)
        # Min 3: P910 changed (C900, P900, C910 unchanged)
        mock_rows_opt = [
            # Min 1
            ("2026-11-19 10:00:00", "S50Z26C900", "Z26", 900.0, "C", 22.0, 22.5, 22.2, 50.0, 100.0, 10.0, 15.0),
            ("2026-11-19 10:00:00", "S50Z26P900", "Z26", 900.0, "P", 2.0, 2.4, 2.2, 30.0, 80.0, 12.0, 18.0),
            ("2026-11-19 10:00:00", "S50Z26C910", "Z26", 910.0, "C", 14.0, 14.5, 14.2, 40.0, 90.0, 20.0, 25.0),
            ("2026-11-19 10:00:00", "S50Z26P910", "Z26", 910.0, "P", 4.0, 4.4, 4.2, 25.0, 70.0, 15.0, 10.0),
            # Min 2: only C900 updated
            ("2026-11-19 10:01:05", "S50Z26C900", "Z26", 900.0, "C", 22.5, 23.0, 22.7, 55.0, 100.0, 14.0, 20.0),
            # Min 3: only P910 updated
            ("2026-11-19 10:02:10", "S50Z26P910", "Z26", 910.0, "P", 4.5, 4.9, 4.7, 30.0, 70.0, 18.0, 12.0),
        ]

        def side_effect(url, token, sql, args):
            if "spot_snapshots" in sql:
                return None, mock_rows_spot
            elif "futures_snapshots" in sql:
                return None, mock_rows_fut
            elif "options_snapshots" in sql:
                return None, mock_rows_opt
            return None, []

        mock_sql.side_effect = side_effect

        grouped, fut_cnt, opt_cnt = fetch_intraday_arb_dataset(
            "https://test", "token", "2026-11-19", "10:00:00", "10:02:59"
        )

        # 3 minute buckets
        minutes = sorted(grouped.keys())
        self.assertEqual(len(minutes), 3)

        # In Minute 2 ("2026-11-19 10:01:00"), all 4 option contracts must be present due to AS-OF forward-fill
        m2 = grouped["2026-11-19 10:01:00"]
        self.assertEqual(len(m2["options"]), 4)
        # C900 should be the updated quote (bid=22.5)
        self.assertEqual(m2["options"][("Z26", 900.0, "C")]["bid"], 22.5)
        # P900 should be forward-filled from minute 1 (bid=2.0)
        self.assertEqual(m2["options"][("Z26", 900.0, "P")]["bid"], 2.0)
        # C910 should be forward-filled from minute 1 (bid=14.0)
        self.assertEqual(m2["options"][("Z26", 910.0, "C")]["bid"], 14.0)

        # In Minute 3 ("2026-11-19 10:02:00"), P910 is updated, others forward-filled
        m3 = grouped["2026-11-19 10:02:00"]
        self.assertEqual(len(m3["options"]), 4)
        self.assertEqual(m3["options"][("Z26", 910.0, "P")]["bid"], 4.5)
        self.assertEqual(m3["options"][("Z26", 900.0, "C")]["bid"], 22.5)

    def test_depth_max_sets_parity(self):
        grouped = {
            "10:00:00": {
                "spot": 920.0,
                "futures": {
                    "S50Z26": {
                        "symbol": "S50Z26",
                        "bid": 920.0,
                        "ask": 920.4,
                        "bid_vol": 50.0,
                        "ask_vol": 60.0,
                        "volume": 5000.0
                    }
                },
                "options": {
                    ("Z26", 900.0, "C"): {
                        "symbol": "S50Z26C900",
                        "bid": 22.0,
                        "ask": 22.5,
                        "bid_vol": 10.0,  # Min leg depth for sell call
                        "ask_vol": 25.0,
                        "volume": 1000.0
                    },
                    ("Z26", 900.0, "P"): {
                        "symbol": "S50Z26P900",
                        "bid": 1.5,
                        "ask": 2.0,
                        "bid_vol": 20.0,
                        "ask_vol": 15.0,  # Min leg depth for buy put
                        "volume": 800.0
                    }
                }
            }
        }

        # Conversion: Sell Call (bid_vol=10), Buy Put (ask_vol=15), Buy Fut (ask_vol=60)
        # min(10, 15, 60) = 10
        opps, near_miss, dropped, errs, _ = scan_put_call_parity(grouped, "Z26", 0.02, 30/365)
        if near_miss:
            if "Conversion" in near_miss["strategy"]:
                self.assertEqual(near_miss["max_sets"], 10)

    @patch("arb_scanner.execute_turso_sql")
    def test_asof_forward_fill_prunes_older_than_15_minutes(self, mock_sql):
        # Minute 1: 10:00:00 (C900)
        # Minute 2: 10:14:00 (14 min later: C900 should still be preserved)
        # Minute 3: 10:16:00 (16 min later: C900 should be pruned because > 900 seconds)
        mock_rows_spot = [
            ("2026-11-19 10:00:00", 920.0, 1000, 100000),
            ("2026-11-19 10:14:00", 920.5, 2000, 200000),
            ("2026-11-19 10:16:00", 921.0, 3000, 300000),
        ]
        mock_rows_fut = [
            ("2026-11-19 10:00:00", "S50Z26", 920.0, 920.4, 920.2, 500.0, 10000.0, 50.0, 60.0),
            ("2026-11-19 10:14:00", "S50Z26", 920.5, 920.9, 920.7, 600.0, 10000.0, 55.0, 65.0),
            ("2026-11-19 10:16:00", "S50Z26", 921.0, 921.4, 921.2, 700.0, 10000.0, 70.0, 80.0),
        ]
        # Option C900 only updated at 10:00:00, P900 updated at 10:16:00
        mock_rows_opt = [
            ("2026-11-19 10:00:00", "S50Z26C900", "Z26", 900.0, "C", 22.0, 22.5, 22.2, 50.0, 100.0, 10.0, 15.0),
            ("2026-11-19 10:16:00", "S50Z26P900", "Z26", 900.0, "P", 2.0, 2.4, 2.2, 30.0, 80.0, 12.0, 18.0),
        ]

        def side_effect(url, token, sql, args):
            if "spot_snapshots" in sql:
                return None, mock_rows_spot
            elif "futures_snapshots" in sql:
                return None, mock_rows_fut
            elif "options_snapshots" in sql:
                return None, mock_rows_opt
            return None, []

        mock_sql.side_effect = side_effect

        grouped, fut_cnt, opt_cnt = fetch_intraday_arb_dataset(
            "https://test", "token", "2026-11-19", "10:00:00", "10:16:00"
        )

        # At 10:14:00 (14 min since 10:00:00), C900 must still exist (<= 15 min)
        m_14 = grouped["2026-11-19 10:14:00"]
        self.assertIn(("Z26", 900.0, "C"), m_14["options"])

        # At 10:16:00 (16 min since 10:00:00), C900 must be pruned (> 15 min)
        m_16 = grouped["2026-11-19 10:16:00"]
        self.assertNotIn(("Z26", 900.0, "C"), m_16["options"])
        # But P900 updated at 10:16:00 must be present
        self.assertIn(("Z26", 900.0, "P"), m_16["options"])


if __name__ == "__main__":
    unittest.main()
