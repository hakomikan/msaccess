#!python
# encoding: utf-8
import begin
import msaccess
import locale
import codecs
import sys
import re
import bson.json_util
from collections import defaultdict
import jinja2
from jinja2 import Template, environmentfilter

OUTPUT_ENCODING = "utf-8"
INPUT_ENCODING = "utf-8"

def open_output_stream(filename):
    if filename == "-":
        sys.stdout.close = lambda: None
        return sys.stdout
    else:
        return codecs.open(filename, "w", OUTPUT_ENCODING)


def open_input_stream(filename):
    if filename == "-":
        return sys.stdin
    else:
        return codecs.open(filename, "r", INPUT_ENCODING)


def read_yaml(filename):
    with open_input_stream(filename) as f:
        import yaml
        return yaml.load(f.read())


def dump_yaml(filename, data):
    import yaml
    f = open_output_stream(filename)
    f.write(
        yaml.safe_dump(
            data,
            allow_unicode=True,
            default_flow_style=False))


def read_list(filename):
    with open_input_stream(filename) as f:
        return [x.strip() for x in f.readlines()]


def write_list(filename, lst):
    with open_output_stream(filename) as f:
        for x in lst:
            print(x, file=f)


def to_member_variable(value):
    tmp = re.sub("(?!^)[A-Z]",lambda x: "_"+x.group(0).lower(), value)
    return re.sub("^[A-Z]", lambda x: x.group(0).lower(), tmp)


jinja2.filters.FILTERS['to_member_variable'] = to_member_variable

models_template = """
from flaskext.mongoalchemy import MongoAlchemy
from kusado import db

{% for table_name, fields in schema.items() %}
class {{table_name}}(db.Document):
    config_collection_name = "{{table_name}}"
    config_extra_fields = "ignore"
    {%- for field_name, attributes in fields["Members"].items() %}
    {{field_name|to_member_variable}} = db.StringField(db_field="{{field_name}}")
    {%- endfor %}

    def to_json(self):
        return {
        {%- for field_name in fields["Members"] %}
            "{{field_name|to_member_variable}}": self.{{field_name|to_member_variable}},
        {%- endfor %}
        }
{% endfor %}
"""

@begin.subcommand
def mongoalchemy(schema_file, output="-"):
    """Export models of MongoAlchemy."""
    schema = read_yaml(schema_file)
    with open_output_stream(output) as f:
        print(Template(models_template).render(schema=schema), f)


class JsonSchemaConverterFromAccessSchema:
    def __init__(self):
        pass

    @classmethod
    def convert_schemas(cls, schemas):
        return [cls.convert_schema(name, schema) for name, schema in schemas.items()]

    @classmethod
    def convert_schema(cls, name, schema):
        return {
            "title": name,
            "description": schema["OriginalTableName"],
            "type": "object",
            "required": cls.get_required_members(schema["Members"]),
            "properties":  dict( (member_name, cls.convert_member(member_name, attributes)) for member_name, attributes in schema["Members"].items())
        }

    @classmethod
    def convert_type(cls, type_name):
        convert_table = {
            "adWChar": "string",
            "adInteger": "integer",
            "adDate": "date",
            "adBoolean": "boolean",
            "adSmallInt": "integer",
            "adCurrency": "decimal",
            "adNumeric": "number",
            "adDouble": "number",
            "adUnsignedTinyInt": "integer"
        }
        return convert_table[type_name]

    @classmethod
    def get_required_members(cls, members):
        return [ k for k,v in members.items() if not v["IS_NULLABLE"]]

    @classmethod
    def convert_member(cls, member_name, attributes):
        member_type = cls.convert_type(attributes["DATA_TYPE_NAME"])
        ret = {
            "originalName": attributes["ORIGINAL_NAME"],
            "description": attributes["DESCRIPTION"],
            "isNullable": attributes["IS_NULLABLE"],
            "default": attributes["COLUMN_DEFAULT"],
            "type": member_type,
            "ordinalPosition": attributes["ORDINAL_POSITION"]
        }

        if member_type == "string":
            ret["maxLength"] = attributes["CHARACTER_MAXIMUM_LENGTH"]

        del attributes["CHARACTER_MAXIMUM_LENGTH"]
        del attributes["DATA_TYPE"]
        del attributes["DATA_TYPE_NAME"]
        del attributes["TABLE_NAME"]
        del attributes["TYPE_GUID"]
        del attributes["ORIGINAL_NAME"]
        del attributes["DESCRIPTION"]
        del attributes["COLUMN_NAME"]
        del attributes["IS_NULLABLE"]
        del attributes["COLUMN_DEFAULT"]
        del attributes["COLUMN_GUID"]
        del attributes["ORDINAL_POSITION"]
        del attributes["NUMERIC_PRECISION"]
        del attributes["COLUMN_FLAGS"]
        del attributes["COLUMN_HASDEFAULT"]
        del attributes["CHARACTER_OCTET_LENGTH"]
        del attributes["NUMERIC_SCALE"]
        for k in list(attributes):
            if not attributes[k]:
                del attributes[k]

        if attributes:
            raise Exception("unknown attributes: " + str(attributes))

        return ret

@begin.subcommand
def json_schema(schema_file, output="-"):
    """Export json schemas from access schemas"""
    schemas = read_yaml(schema_file)
    dump_yaml(output, JsonSchemaConverterFromAccessSchema.convert_schemas(schemas))

@begin.start
def run():
    sys.stdout = codecs.getwriter(OUTPUT_ENCODING)(sys.stdout.detach())

