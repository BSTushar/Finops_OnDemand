"""Windows + Graviton guardrails and RDS hourly when Product says Windows."""
from __future__ import annotations
import unittest
import pandas as pd
from data_loader import ColumnBinding
from pricing_engine import PRICING_LOOKUP_REGION, get_rds_hourly
from processor import ALT2_INCOMPATIBLE_OS, apply_na_fill, process


class TestRdsWindowsGetsClassPrice(unittest.TestCase):
    """RDS list table is class-based (MySQL SA–style); Windows CUR rows still get hourly for comparison."""

    def test_db_m5_large_windows_product_has_current_price(self) -> None:
        df = pd.DataFrame({'i': ['db.m5.large'], 'p': ['Windows Server'], 'c': [1.0]})
        b = ColumnBinding(instance='i', os='p', actual_cost='c')
        out = apply_na_fill(process(df, b, region='eu-west-1', service='both'))
        want = get_rds_hourly('db.m5.large', region=PRICING_LOOKUP_REGION, os='linux')
        self.assertIsNotNone(want)
        cp = float(out['Current Price ($/hr)'].iloc[0])
        self.assertAlmostEqual(cp, want, places=4)
        self.assertEqual(out['Pricing OS'].iloc[0], 'Windows')
        self.assertIn('db.m6i', str(out['Alt1 Instance'].iloc[0]))


class TestWindowsNoGravitonAlts(unittest.TestCase):
    def test_ec2_windows_blocks_graviton_alt2(self) -> None:
        df = pd.DataFrame({'i': ['c5.xlarge'], 'p': ['windows'], 'c': [1.0]})
        b = ColumnBinding(instance='i', os='p', actual_cost='c')
        out = apply_na_fill(process(df, b, region='eu-west-1', service='ec2', cpu_filter='both'))
        self.assertEqual(out['Alt2 Instance'].iloc[0], ALT2_INCOMPATIBLE_OS)

    def test_ec2_windows_graviton_cpu_mode_no_graviton_alts(self) -> None:
        df = pd.DataFrame({'i': ['m6g.large'], 'p': ['Windows'], 'c': [1.0]})
        b = ColumnBinding(instance='i', os='p', actual_cost='c')
        out = apply_na_fill(process(df, b, region='eu-west-1', service='ec2', cpu_filter='graviton'))
        self.assertEqual(out['Alt1 Instance'].iloc[0], 'N/A')
        self.assertIn(out['Alt2 Instance'].iloc[0], (ALT2_INCOMPATIBLE_OS, 'N/A'))


if __name__ == '__main__':
    unittest.main()
