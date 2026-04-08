from __future__ import annotations
import unittest
import pandas as pd
from data_loader import ColumnBinding
from pricing_engine import get_price, get_rds_hourly
from processor import apply_na_fill, process


class TestRowRegionPricing(unittest.TestCase):
    def test_ec2_row_uses_row_region_when_present(self):
        df = pd.DataFrame(
            {
                'Instance': ['m5.large'],
                'Product': ['linux'],
                'Region': ['us-east-1'],
                'Cost': [1.0],
            }
        )
        b = ColumnBinding(instance='Instance', os='Product', actual_cost='Cost')
        out = apply_na_fill(process(df, b, region='eu-west-1', service='both'))
        want = get_price('m5.large', region='us-east-1', os='linux')
        self.assertIsNotNone(want)
        self.assertAlmostEqual(float(out['Current Price ($/hr)'].iloc[0]), float(want), places=6)

    def test_ec2_unsupported_row_region_falls_back_to_default(self):
        df = pd.DataFrame(
            {
                'Instance': ['m5.large'],
                'Product': ['linux'],
                'Region': ['ca-central-1'],
                'Cost': [1.0],
            }
        )
        b = ColumnBinding(instance='Instance', os='Product', actual_cost='Cost')
        out = apply_na_fill(process(df, b, region='eu-west-1', service='both'))
        # ca-central-1 not in bundled dataset -> fallback to eu-west-1
        want = get_price('m5.large', region='eu-west-1', os='linux')
        self.assertIsNotNone(want)
        self.assertAlmostEqual(float(out['Current Price ($/hr)'].iloc[0]), float(want), places=6)

    def test_rds_row_uses_row_region_when_context_supported(self):
        df = pd.DataFrame(
            {
                'Instance': ['db.r5.large'],
                'DB Engine': ['mysql'],
                'Availability_Zone': ['us-east-1a'],
                'Region': ['us-east-1'],
                'Cost': [1.0],
            }
        )
        b = ColumnBinding(instance='Instance', os='DB Engine', actual_cost='Cost')
        out = apply_na_fill(process(df, b, region='eu-west-1', service='both'))
        want = get_rds_hourly('db.r5.large', region='us-east-1', os='linux')
        self.assertIsNotNone(want)
        self.assertAlmostEqual(float(out['Current Price ($/hr)'].iloc[0]), float(want), places=6)

    def test_rds_context_unsupported_still_suppresses(self):
        df = pd.DataFrame(
            {
                'Instance': ['db.r5.large'],
                'DB Engine': ['postgres'],
                'Availability_Zone': ['ca-central-1a'],
                'Region': ['ca-central-1'],
                'Cost': [1.0],
            }
        )
        b = ColumnBinding(instance='Instance', os='DB Engine', actual_cost='Cost')
        out = apply_na_fill(process(df, b, region='eu-west-1', service='both'))
        self.assertEqual(out['Current Price ($/hr)'].iloc[0], 'N/A')
        self.assertEqual(out['Alt1 Instance'].iloc[0], 'N/A')
        self.assertEqual(out['Alt2 Instance'].iloc[0], 'N/A')


if __name__ == '__main__':
    unittest.main()
