from unittest import TestCase
from scripts.msaccess_export import tables, columns


class TestMsAccessExport(TestCase):
    def test_tables(self):
        tables("testdata.mdb")

    def test_columns(self):
        columns("testdata.mdb")

