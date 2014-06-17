from unittest import TestCase
import os
import msaccess
from pprint import pprint


class TestMsAccessDb(TestCase):
    def setUp(self):
        self.db = msaccess.MsAccessDb("test_data.mdb")

    def test_get_schema_names(self):
        pprint(self.db.get_schema_names(msaccess.SchemaTypes.TABLE.name))
        self.assertIn("setting", self.db.get_schema_names("TABLE"))

    def test_get_field_attributes(self):
        db = self.db
        pprint(db.get_field_attributes(list(db.get_table_names())[0]))