from __future__ import annotations
import unittest
import pandas as pd
from sheet_merger import merge_primary_with_secondary, suggest_key_pairs

_MERGE_FLAGS = [
    'FinOps_Merge_DuplicateSecondaryRows',
    'FinOps_Merge_SecondaryRowGroupIndex',
    'FinOps_Merge_DuplicatePrimaryKey',
]


class TestSuggestKeyPairs(unittest.TestCase):
    def test_same_name_resource_id_first(self):
        c1 = ['resource_id', 'Instance', 'OS']
        c2 = ['resource_id', 'Cost', 'Spend']
        pairs = suggest_key_pairs(c1, c2)
        self.assertTrue(any((p == ('resource_id', 'resource_id') for p in pairs)))

    def test_cross_name_instance_id(self):
        c1 = ['instance_id', 'vm', 'linux']
        c2 = ['resource_id', 'amount']
        pairs = suggest_key_pairs(c1, c2)
        self.assertTrue(any((p[0] == 'instance_id' and p[1] == 'resource_id' for p in pairs)))


class TestMergePrimaryWithSecondary(unittest.TestCase):
    def test_missing_cost_in_d1_filled_from_d2(self):
        d1 = pd.DataFrame({'resource_id': ['a', 'b'], 'Instance': ['m5.large', 'c5.xlarge'], 'OS': ['linux', 'linux'], 'Cost': [pd.NA, pd.NA]})
        d2 = pd.DataFrame({'resource_id': ['a', 'b'], 'Cost': [100.0, 200.0]})
        out, w = merge_primary_with_secondary(d1, d2, 'resource_id', 'resource_id')
        self.assertEqual(list(out.columns), ['resource_id', 'Instance', 'OS', 'Cost'] + _MERGE_FLAGS)
        self.assertEqual(list(out['Cost']), [100.0, 200.0])

    def test_d1_column_order_preserved_d2_only_appended(self):
        d1 = pd.DataFrame({'id': [1], 'A': ['x'], 'B': ['y']})
        d2 = pd.DataFrame({'id': [1], 'B': ['ignored'], 'Z': ['new']})
        out, _ = merge_primary_with_secondary(d1, d2, 'id', 'id')
        self.assertEqual(list(out.columns), ['id', 'A', 'B', 'Z'] + _MERGE_FLAGS)
        self.assertEqual(out.iloc[0]['B'], 'y')
        self.assertEqual(out.iloc[0]['Z'], 'new')

    def test_prefer_d1_when_non_empty(self):
        d1 = pd.DataFrame({'id': [1], 'Cost': [50.0]})
        d2 = pd.DataFrame({'id': [1], 'Cost': [999.0]})
        out, _ = merge_primary_with_secondary(d1, d2, 'id', 'id')
        self.assertEqual(float(out.iloc[0]['Cost']), 50.0)

    def test_partial_key_overlap(self):
        d1 = pd.DataFrame({'resource_id': ['r1', 'r2', 'r3'], 'Instance': ['m5.large', 'm5.large', 'm5.large']})
        d2 = pd.DataFrame({'resource_id': ['r1', 'r3'], 'Spend': [10.0, 30.0]})
        out, w = merge_primary_with_secondary(d1, d2, 'resource_id', 'resource_id')
        self.assertEqual(list(out.columns), ['resource_id', 'Instance', 'Spend'] + _MERGE_FLAGS)
        self.assertTrue(any('no secondary match' in x for x in w))
        self.assertEqual(float(out.loc[out['resource_id'] == 'r1', 'Spend'].iloc[0]), 10.0)
        self.assertTrue(pd.isna(out.loc[out['resource_id'] == 'r2', 'Spend'].iloc[0]))

    def test_duplicate_d2_keys_includes_all_rows(self):
        d1 = pd.DataFrame({'id': ['x'], 'Instance': ['m5.large']})
        d2 = pd.DataFrame({'id': ['x', 'x'], 'Spend': [1.0, 2.0]})
        out, w = merge_primary_with_secondary(d1, d2, 'id', 'id')
        self.assertEqual(len(out), 2)
        self.assertEqual(sorted(float(out.iloc[i]['Spend']) for i in range(2)), [1.0, 2.0])
        self.assertTrue((out['FinOps_Merge_DuplicateSecondaryRows'] == 'Yes').all())
        self.assertEqual(set(out['FinOps_Merge_SecondaryRowGroupIndex']), {'1/2', '2/2'})
        self.assertTrue(any('secondary' in x.lower() and 'repeat' in x.lower() for x in w))

    def test_missing_instance_in_d2_does_not_drop_d1_columns(self):
        d1 = pd.DataFrame({'arn': ['arn:1'], 'Instance': ['m5.large'], 'OS': ['linux']})
        d2 = pd.DataFrame({'arn': ['arn:1'], 'Spend': [42.0]})
        out, _ = merge_primary_with_secondary(d1, d2, 'arn', 'arn')
        self.assertEqual(list(out.columns), ['arn', 'Instance', 'OS', 'Spend'] + _MERGE_FLAGS)
        self.assertEqual(out.iloc[0]['Instance'], 'm5.large')
        self.assertEqual(float(out.iloc[0]['Spend']), 42.0)

    def test_fuzzy_key_embedded_short_code_in_long_secondary(self):
        d1 = pd.DataFrame({'app_code': ['a1105'], 'Name': ['App A']})
        d2 = pd.DataFrame({'application_type': ['asdsd_asa_a1105'], 'Spend': [12.5]})
        out, w = merge_primary_with_secondary(d1, d2, 'app_code', 'application_type')
        self.assertEqual(float(out.iloc[0]['Spend']), 12.5)
        self.assertTrue(any('fuzzy' in x.lower() for x in w))

    def test_case_insensitive_exact_key(self):
        d1 = pd.DataFrame({'k': ['A1105'], 'x': [1]})
        d2 = pd.DataFrame({'k': ['a1105'], 'y': ['ok']})
        out, w = merge_primary_with_secondary(d1, d2, 'k', 'k')
        self.assertEqual(out.iloc[0]['y'], 'ok')
        self.assertFalse(any('fuzzy' in x.lower() for x in w))


if __name__ == '__main__':
    unittest.main()
