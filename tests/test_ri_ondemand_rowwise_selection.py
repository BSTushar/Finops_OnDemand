from __future__ import annotations
import unittest
import pandas as pd
from data_loader import ColumnBinding
from processor import process


class TestRiOnDemandRowwiseSelection(unittest.TestCase):
    def test_uses_ri_when_ondemand_zero(self):
        df = pd.DataFrame(
            {
                'API Name': ['m5.large'],
                'Product': ['linux'],
                'On Demand Cost': [0.0],
                'RI Cost': [120.0],
            }
        )
        b = ColumnBinding(instance='API Name', os='Product', actual_cost='On Demand Cost')
        out = process(df, b, region='eu-west-1', service='both')
        self.assertAlmostEqual(float(out['Actual Cost ($)'].iloc[0]), 120.0, places=8)

    def test_uses_ondemand_when_ri_zero(self):
        df = pd.DataFrame(
            {
                'API Name': ['m5.large'],
                'Product': ['linux'],
                'On Demand Cost': [130.0],
                'RI Cost': [0.0],
            }
        )
        b = ColumnBinding(instance='API Name', os='Product', actual_cost='RI Cost')
        out = process(df, b, region='eu-west-1', service='both')
        self.assertAlmostEqual(float(out['Actual Cost ($)'].iloc[0]), 130.0, places=8)

    def test_both_non_zero_prefers_selected_column(self):
        df = pd.DataFrame(
            {
                'API Name': ['m5.large'],
                'Product': ['linux'],
                'On Demand Cost': [140.0],
                'RI Cost': [110.0],
            }
        )
        b1 = ColumnBinding(instance='API Name', os='Product', actual_cost='RI Cost')
        b2 = ColumnBinding(instance='API Name', os='Product', actual_cost='On Demand Cost')
        out1 = process(df, b1, region='eu-west-1', service='both')
        out2 = process(df, b2, region='eu-west-1', service='both')
        self.assertAlmostEqual(float(out1['Actual Cost ($)'].iloc[0]), 110.0, places=8)
        self.assertAlmostEqual(float(out2['Actual Cost ($)'].iloc[0]), 140.0, places=8)


if __name__ == '__main__':
    unittest.main()
