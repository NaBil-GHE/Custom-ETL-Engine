import unittest
import pandas as pd
import numpy as np
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from etl_engine.transformer import DataCleaner, Deduplicator, DataValidator


class TestDataCleaner(unittest.TestCase):

    def _sample_df(self):
        return pd.DataFrame({
            "id":    ["1", "2", "3", "4"],
            "name":  ["  Ahmed  ", "Sara", "NULL", ""],
            "email": ["a@b.com", "N/A", "c@d.com", "e@f.com"],
            "age":   ["25", "30", "", "22"],
        })

    def test_column_names_standardized(self):
        df = pd.DataFrame({"First Name": [1], "Last-Name": [2]})
        cleaner = DataCleaner()
        result = cleaner.clean(df)
        self.assertIn("first_name", result.columns)
        self.assertIn("last_name", result.columns)

    def test_whitespace_stripped(self):
        cleaner = DataCleaner()
        result = cleaner.clean(self._sample_df())
        self.assertEqual(result["name"].iloc[0], "Ahmed")

    def test_null_strings_replaced(self):
        cleaner = DataCleaner(config={"null_strategy": "report"})
        result = cleaner.clean(self._sample_df())
        self.assertTrue(result["email"].iloc[1] != "N/A")

    def test_quality_report_structure(self):
        df = pd.DataFrame({"a": [1, None, 3], "b": ["x", "x", "y"]})
        report = DataCleaner.generate_quality_report(df)
        self.assertIn("total_rows", report)
        self.assertIn("null_counts", report)
        self.assertIn("duplicate_rows", report)
        self.assertEqual(report["total_rows"], 3)


class TestDeduplicator(unittest.TestCase):

    def _dup_df(self):
        return pd.DataFrame({
            "id":    [1, 2, 2, 3],
            "name":  ["A", "B", "B", "C"],
            "value": [10, 20, 20, 30],
        })

    def test_removes_exact_duplicates(self):
        dedup = Deduplicator()
        result = dedup.deduplicate(self._dup_df())
        self.assertEqual(len(result), 3)

    def test_keep_last(self):
        dedup = Deduplicator(keep="last")
        result = dedup.deduplicate(self._dup_df())
        self.assertEqual(len(result), 3)

    def test_row_hash_added(self):
        dedup = Deduplicator(use_hash=True)
        result = dedup.deduplicate(self._dup_df())
        self.assertIn("_row_hash", result.columns)

    def test_duplicate_report(self):
        report = Deduplicator.get_duplicate_report(self._dup_df())
        self.assertEqual(report["duplicate_rows"], 2)
        self.assertEqual(report["total_rows"], 4)


class TestDataValidator(unittest.TestCase):

    def _sample_df(self):
        return pd.DataFrame({
            "email": ["valid@test.com", "invalid-email", "another@ok.com"],
            "age":   [25, 150, 30],
            "status": ["active", "active", "unknown"],
        })

    def test_regex_violation_detected(self):
        rules = {"email": {"regex": r"^[\w.+-]+@[\w-]+\.\w+$"}}
        validator = DataValidator(rules)
        report = validator.validate(self._sample_df())
        self.assertFalse(report["passed"])
        self.assertIn("email", report["violations"])

    def test_max_value_violation(self):
        rules = {"age": {"max_value": 120}}
        validator = DataValidator(rules)
        report = validator.validate(self._sample_df())
        self.assertIn("age", report["violations"])

    def test_allowed_values_violation(self):
        rules = {"status": {"allowed_values": ["active", "inactive"]}}
        validator = DataValidator(rules)
        report = validator.validate(self._sample_df())
        self.assertIn("status", report["violations"])

    def test_valid_data_passes(self):
        df = pd.DataFrame({"age": [25, 30, 22], "status": ["active", "inactive", "active"]})
        rules = {"age": {"min_value": 0, "max_value": 120},
                 "status": {"allowed_values": ["active", "inactive"]}}
        validator = DataValidator(rules)
        report = validator.validate(df)
        self.assertTrue(report["passed"])


if __name__ == "__main__":
    unittest.main()
