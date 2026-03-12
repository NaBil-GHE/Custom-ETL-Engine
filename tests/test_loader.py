import unittest
import pandas as pd
import os
import sys
import tempfile

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from etl_engine.loader import DBLoader


class TestDBLoader(unittest.TestCase):

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmp_dir, "test.db")
        self.loader = DBLoader(db_path=self.db_path)
        self.sample_df = pd.DataFrame({
            "id":    [1, 2, 3],
            "name":  ["Ahmed", "Sara", "Omar"],
            "score": [90.5, 85.0, 78.3],
            "_source_type": ["CSV", "JSON", "CSV"],  # metadata – should be excluded
        })

    def test_load_creates_table(self):
        self.loader.load(self.sample_df, "test_table")
        self.assertTrue(self.loader.table_exists("test_table"))

    def test_load_correct_row_count(self):
        self.loader.load(self.sample_df, "test_table")
        self.assertEqual(self.loader.get_row_count("test_table"), 3)

    def test_metadata_columns_excluded(self):
        self.loader.load(self.sample_df, "test_table")
        df_back = self.loader.read_table("test_table")
        self.assertNotIn("_source_type", df_back.columns)

    def test_export_csv(self):
        csv_path = os.path.join(self.tmp_dir, "output.csv")
        self.loader.export_csv(self.sample_df, csv_path)
        self.assertTrue(os.path.exists(csv_path))
        loaded = pd.read_csv(csv_path)
        self.assertEqual(len(loaded), 3)

    def test_export_json(self):
        json_path = os.path.join(self.tmp_dir, "output.json")
        self.loader.export_json(self.sample_df, json_path)
        self.assertTrue(os.path.exists(json_path))

    def test_append_mode(self):
        self.loader.load(self.sample_df, "test_table", if_exists="replace")
        self.loader.load(self.sample_df, "test_table", if_exists="append")
        self.assertEqual(self.loader.get_row_count("test_table"), 6)


if __name__ == "__main__":
    unittest.main()
