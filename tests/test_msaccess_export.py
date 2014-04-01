from unittest import TestCase
from scripts.msaccess_export import tables, columns, dump_mongodb_json, export_mongodb


class TestMsAccessExport(TestCase):
    def test_tables(self):
        tables("test_data.mdb")

    def test_columns(self):
        columns("test_data.mdb")

    def test_dump_mongodb_json(self):
        dump_mongodb_json("dump_database.json", "test_data.mdb", "translation_words.yaml")

    def test_export_mongodb(self):
        export_mongodb("kusado", "test_data.mdb", "translation_words.yaml")
