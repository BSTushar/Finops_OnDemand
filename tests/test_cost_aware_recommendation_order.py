from __future__ import annotations

import unittest

import pandas as pd

from data_loader import ColumnBinding
from processor import apply_na_fill, process
from pricing_engine import get_price, get_rds_hourly


class TestCostAwareRecommendationOrder(unittest.TestCase):
    def test_ec2_alt1_prefers_n_plus_1_with_amd_if_cheaper(self) -> None:
        df = pd.DataFrame({"i": ["m5.large"], "o": ["linux"], "c": [1.0]})
        b = ColumnBinding(instance="i", os="o", actual_cost="c")
        out = apply_na_fill(process(df, b, region="eu-west-1", service="ec2", cpu_filter="both"))
        self.assertEqual(out["Alt1 Instance"].iloc[0], "m6a.large")
        self.assertEqual(
            float(out["Alt1 Price ($/hr)"].iloc[0]),
            float(get_price("m6a.large", region="eu-west-1", os="linux")),
        )
        # Alt2 remains the newer-generation candidate from n+2 pool.
        self.assertEqual(out["Alt2 Instance"].iloc[0], "m7g.large")
        self.assertEqual(
            float(out["Alt2 Price ($/hr)"].iloc[0]),
            float(get_price("m7g.large", region="eu-west-1", os="linux")),
        )

    def test_rds_prefers_priced_cheapest_alt_and_hides_missing_priced_alt(self) -> None:
        df = pd.DataFrame(
            {"i": ["db.m5.large"], "o": ["linux"], "db_engine": ["mysql"], "c": [1.0]}
        )
        b = ColumnBinding(instance="i", os="o", actual_cost="c")
        out = apply_na_fill(process(df, b, region="eu-west-1", service="rds", cpu_filter="both"))

        # db.m6a.large is not in bundled RDS table; Alt1 stays db.m6i.large.
        self.assertEqual(out["Alt1 Instance"].iloc[0], "db.m6i.large")
        # Alt2 keeps n+2 candidate when priced and distinct.
        self.assertEqual(out["Alt2 Instance"].iloc[0], "db.m7g.large")
        self.assertEqual(
            float(out["Alt1 Price ($/hr)"].iloc[0]),
            float(get_rds_hourly("db.m6i.large", region="eu-west-1", os="linux")),
        )
        self.assertEqual(
            float(out["Alt2 Price ($/hr)"].iloc[0]),
            float(get_rds_hourly("db.m7g.large", region="eu-west-1", os="linux")),
        )

    def test_alt2_is_suppressed_when_costlier_than_alt1(self) -> None:
        df = pd.DataFrame({"i": ["m5.large"], "o": ["linux"], "c": [1.0]})
        b = ColumnBinding(instance="i", os="o", actual_cost="c")
        out = apply_na_fill(process(df, b, region="eu-west-1", service="ec2", cpu_filter="intel"))
        # intel mode on m5.large typically yields m6i (n+1) and m7i (n+2), where m7i is costlier.
        self.assertEqual(out["Alt1 Instance"].iloc[0], "m6i.large")
        self.assertEqual(out["Alt2 Instance"].iloc[0], "N/A")
        self.assertEqual(out["Alt2 Price ($/hr)"].iloc[0], "N/A")


if __name__ == "__main__":
    unittest.main()
