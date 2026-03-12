import unittest
import pandas as pd
import os
import sys
import tempfile
import json

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from etl_engine.extractor import CSVExtractor, JSONExtractor


class TestCSVExtractor(unittest.TestCase):

    def setUp(self):
        # Create a temporary CSV file
        self.tmp = tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w", encoding="utf-8")
        self.tmp.write("id,name,age\n1,Ahmed,25\n2,Sara,30\n")
        self.tmp.close()

    def tearDown(self):
        os.unlink(self.tmp.name)

    def test_extract_returns_dataframe(self):
        extractor = CSVExtractor()
        df = extractor.extract(self.tmp.name)
        self.assertIsInstance(df, pd.DataFrame)

    def test_correct_row_count(self):
        extractor = CSVExtractor()
        df = extractor.extract(self.tmp.name)
        self.assertEqual(len(df), 2)

    def test_source_metadata_added(self):
        extractor = CSVExtractor()
        df = extractor.extract(self.tmp.name)
        self.assertIn("_source_type", df.columns)
        self.assertEqual(df["_source_type"].iloc[0], "CSV")

    def test_missing_file_raises(self):
        extractor = CSVExtractor()
        with self.assertRaises(FileNotFoundError):
            extractor.extract("non_existent.csv")


class TestJSONExtractor(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w", encoding="utf-8")
        json.dump([{"id": 1, "product": "Laptop"}, {"id": 2, "product": "Mouse"}], self.tmp)
        self.tmp.close()

    def tearDown(self):
        os.unlink(self.tmp.name)

    def test_extract_returns_dataframe(self):
        extractor = JSONExtractor()
        df = extractor.extract(self.tmp.name)
        self.assertIsInstance(df, pd.DataFrame)

    def test_correct_row_count(self):
        extractor = JSONExtractor()
        df = extractor.extract(self.tmp.name)
        self.assertEqual(len(df), 2)

    def test_source_metadata_added(self):
        extractor = JSONExtractor()
        df = extractor.extract(self.tmp.name)
        self.assertEqual(df["_source_type"].iloc[0], "JSON")

    def test_missing_file_raises(self):
        extractor = JSONExtractor()
        with self.assertRaises(FileNotFoundError):
            extractor.extract("non_existent.json")


if __name__ == "__main__":
    unittest.main()
