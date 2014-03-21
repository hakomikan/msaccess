from unittest import TestCase
from scripts.msaccess_export import tables


class TestMsAccessExport(TestCase):
    def test_tables(self):
        tables("testdata.mdb")
