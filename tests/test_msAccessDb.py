from unittest import TestCase
import os
import msaccess
from pprint import pprint


class TestMsAccessDb(TestCase):
    def setUp(self):
        self.db = msaccess.MsAccessDb("testdata.mdb")

    def test_get_schema_names(self):
        pprint(self.db.get_schema_names(msaccess.SchemaTypes.TABLE.name))
        self.assertIn("setting", self.db.get_schema_names("TABLE"))
