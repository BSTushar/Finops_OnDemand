from __future__ import annotations
import unittest
import pandas as pd
from data_loader import ColumnBinding, analyze_load, finalize_binding
from os_resolve import cell_matches_valid_os_pattern, engine_os_for_pricing, normalize_pricing_os_display
from processor import INSERT_COLS, apply_na_fill, process


class TestOsValuePatterns(unittest.TestCase):

    def test_detection_linux_variants(self):
        for v in ('linux', 'Ubuntu 22', 'DEBIAN', 'rhel 8', 'amazon linux 2', 'AMAZON LINUX'):
            self.assertTrue(cell_matches_valid_os_pattern(v), msg=v)

    def test_detection_windows_variants(self):
        for v in ('windows', 'Windows Server', 'win', 'WIN2019'):
            self.assertTrue(cell_matches_valid_os_pattern(v), msg=v)

    def test_normalize_display_buckets(self):
        self.assertEqual(normalize_pricing_os_display('ubuntu'), 'Linux')
        self.assertEqual(normalize_pricing_os_display('Win10'), 'Windows')
        self.assertEqual(normalize_pricing_os_display(''), 'Linux')
        self.assertEqual(normalize_pricing_os_display(None), 'Linux')

    def test_engine_default_linux(self):
        self.assertEqual(engine_os_for_pricing(''), 'linux')
        self.assertEqual(engine_os_for_pricing('garbage xyz'), 'linux')


class TestDynamicOsColumnDetection(unittest.TestCase):

    def test_os_in_arbitrary_column_name(self):
        df = pd.DataFrame(
            {
                'VM': ['m5.large', 'm5.large'],
                'Totally_Not_OS': ['linux', 'ubuntu'],
            }
        )
        lr = analyze_load(df, [])
        self.assertIsNotNone(lr.binding)
        self.assertEqual(lr.binding.os, 'Totally_Not_OS')
        self.assertFalse(lr.needs_os_pick)

    def test_mixed_values_normalized(self):
        df = pd.DataFrame({'i': ['m5.large', 'm5.large'], 'x': ['Win', 'debian']})
        lr = analyze_load(df, [])
        self.assertEqual(lr.binding.instance, 'i')
        self.assertEqual(lr.binding.os, 'x')
        out = apply_na_fill(process(lr.df, lr.binding, region='eu-west-1', service='both'))
        ins = list(out.columns).index('i')
        self.assertEqual(out.columns[ins + 1], 'Pricing OS')
        self.assertEqual(out['Pricing OS'].iloc[0], 'Windows')
        self.assertEqual(out['Pricing OS'].iloc[1], 'Linux')

    def test_missing_os_values_fallback_linux_column(self):
        df = pd.DataFrame({'i': ['m5.large', 'm5.large'], 'x': ['linux', '']})
        lr = analyze_load(df, [])
        self.assertEqual(lr.binding.os, 'x')
        out = apply_na_fill(process(lr.df, lr.binding, region='eu-west-1'))
        self.assertEqual(out['Pricing OS'].iloc[0], 'Linux')
        self.assertEqual(out['Pricing OS'].iloc[1], 'Linux')

    def test_no_os_column_auto_binding(self):
        df = pd.DataFrame({'shape': ['m5.large'], 'note': ['prod'], 'amt': [10.0]})
        lr = analyze_load(df, [])
        self.assertIsNone(lr.binding.os)
        self.assertFalse(lr.needs_os_pick)
        out = apply_na_fill(process(lr.df, lr.binding, region='eu-west-1', service='both'))
        self.assertEqual(out['Pricing OS'].tolist(), ['Linux'])

    def test_ambiguous_two_os_columns(self):
        df = pd.DataFrame(
            {
                'inst': ['m5.large'],
                'col_a': ['linux'],
                'col_b': ['ubuntu'],
            }
        )
        lr = analyze_load(df, [])
        self.assertTrue(lr.needs_os_pick)
        self.assertIsNone(lr.binding)
        self.assertGreaterEqual(len(lr.os_candidates), 2)

    def test_binding_os_none_process(self):
        df = pd.DataFrame({'i': ['m5.large'], 'c': [100.0]})
        b = ColumnBinding(instance='i', os=None, actual_cost='c')
        out = apply_na_fill(process(df, b, region='eu-west-1'))
        self.assertEqual(out['Pricing OS'].iloc[0], 'Linux')
        self.assertIn('Alt1 Instance', out.columns)


class TestPricingOsInsertionOrder(unittest.TestCase):

    def test_after_instance_column(self):
        cols = ['A', 'Instance', 'Z']
        df = pd.DataFrame([['a', 'm5.large', 'z']], columns=cols)
        b = ColumnBinding(instance='Instance', os='Z', actual_cost=None)
        out = process(df, b, region='eu-west-1')
        idx = list(out.columns).index('Instance')
        self.assertEqual(list(out.columns[idx : idx + 1 + len(INSERT_COLS)][:2]), ['Instance', 'Pricing OS'])
        self.assertEqual(list(out.columns[idx + 1 : idx + 1 + len(INSERT_COLS)]), INSERT_COLS)


class TestFinalizeBindingOptionalOs(unittest.TestCase):

    def test_finalize_none_os(self):
        df = pd.DataFrame({'i': ['m5.large'], 'c': [1.0]})
        lr = analyze_load(df, [])
        lr2 = finalize_binding(lr, 'i', None, 'c')
        self.assertIsNone(lr2.binding.os)


if __name__ == '__main__':
    unittest.main()
