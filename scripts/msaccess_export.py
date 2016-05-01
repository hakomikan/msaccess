#!python
# encoding: utf-8
import begin
import msaccess
import locale
import codecs
import sys
import bson.json_util
from collections import defaultdict

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


@begin.subcommand
def tables(mdb):
    """Show table names."""
    db = msaccess.MsAccessDb(mdb)
    for table_name in db.get_table_names():
        print(table_name)


@begin.subcommand
def columns(mdb):
    """Show column names."""
    db = msaccess.MsAccessDb(mdb)
    for table_name, field_name in db.get_table_and_field_names():
        print("/".join([table_name,field_name]))


def translate_database_entry(table_name, table_entry, translation_dict):
    return translation_dict[table_name+"/"+table_entry].split("/")[1]


def translate_mongo_document(table_name, doc, translation_dict):
    translated_entry = {}
    for doc_key, doc_value in doc.items():
        translated_entry[translate_database_entry(table_name, doc_key, translation_dict)] = doc_value
    return translated_entry


def translate_database(db, translation_dict):
    ret = {}
    for table_key, table_value in db.items():
        translated_table = translation_dict[table_key]
        table_data = []
        for table_entry in table_value:
            translated_entry = {}
            for entry_key, entry_value in table_entry.items():
                translated_entry[translate_database_entry(table_key, entry_key, translation_dict)] = entry_value
            table_data.append(translated_entry)
        ret[translated_table] = table_data
    return ret

@begin.subcommand
def list_tables(mdb=None, translation_words=None):
    """Dump database as json.
    """
    db = msaccess.MsAccessDb(mdb)
    db_data = dict()

    translation_dict = {}
    if translation_words:
        translation_dict = read_yaml(translation_words)

    for table_name in db.get_table_names():
        print("{0}: {1}".format(translation_dict[table_name], table_name))

@begin.subcommand
def dump_table(table_name=None, output="-", mdb=None, translation_words=None):
    """Dump a table as json.
    """
    db = msaccess.MsAccessDb(mdb)
    db_data = dict()

    translation_dict = {}
    if translation_words:
        translation_dict = read_yaml(translation_words)

    original_table_names = [t_name for t_name in db.get_table_names() if translation_dict[t_name] == table_name ]
    
    if len(original_table_names) != 1:
        raise Exception("not found table: {0}".format(table_name))
    
    original_table_name = original_table_names[0]

    db_data = dict()
    table_data = []

    print("extract {0}".format(table_name))
    field_names = [field_name for field_name in db.get_field_names(original_table_name)]
    for fields in db.iterate_query(original_table_name):
        fields = [msaccess.MsAccessDb.regulate_value_for_mongodb(field) for field in fields]
        table_data.append(dict(zip(field_names, fields)))
        
    db_data[original_table_name] = table_data

    print("translate...")
    db_data = translate_database(db_data, translation_dict)
    table_data = db_data[table_name]

    print("write out...")
    with open_output_stream(output) as of:
        for document in table_data:
            print(bson.json_util.dumps(document), file=of)
        
@begin.subcommand
def dump_mongodb_json(output=None, mdb=None, translation_words=None):
    """Dump database as json.
    """
    db = msaccess.MsAccessDb(mdb)
    db_data = dict()

    translation_dict = {}
    if translation_words:
        translation_dict = read_yaml(translation_words)

    for table_name in db.get_table_names():
        print("extract {0}...".format(translation_dict[table_name]))
        
        table_data = []

        field_names = [field_name for field_name in db.get_field_names(table_name)]
        for fields in db.iterate_query(table_name):
            fields = [msaccess.MsAccessDb.regulate_value_for_mongodb(field) for field in fields]
            table_data.append(dict(zip(field_names, fields)))
        db_data[table_name] = table_data

    print("translate...")

    db_data = translate_database(db_data, translation_dict)

    print("write out...")
    with open_output_stream(output) as of:
        of.write(bson.json_util.dumps(db_data))

@begin.subcommand
def export_mongodb(output, mdb, translation_words=None):
    """export to mongodb.
    """
    import pymongo

    db = msaccess.MsAccessDb(mdb)
    db_data = dict()

    with pymongo.Connection("localhost") as con:
        con.drop_database(output)
        mongodb = con[output]

        if translation_words:
            translation_dict = read_yaml(translation_words)
        else:
            translation_dict = defaultdict(lambda x: x)

        for table_name in db.get_table_names():
            translated_table_name = translation_dict[table_name]
            collection = mongodb[translated_table_name]
            field_names = [field_name for field_name in db.get_field_names(table_name)]
            print("exporting {0}...", translated_table_name)
            sys.stdout.flush()

            for fields in db.iterate_query(table_name):
                fields = [msaccess.MsAccessDb.regulate_value_for_mongodb(field) for field in fields]
                collection.insert(translate_mongo_document(table_name, dict(zip(field_names, fields)), translation_dict))
                break


@begin.subcommand
def export_schema(output=None, mdb=None, translation_words=None):
    """export schema by yaml
    """
    db = msaccess.MsAccessDb(mdb)
    db_data = dict()

    if translation_words:
        translation_dict = read_yaml(translation_words)
    else:
        translation_dict = defaultdict(lambda x: x)

    ret = {}

    for table_name in db.get_table_names():
        translated_table_name = translation_dict[table_name]

        ret[translated_table_name] = {}

        current_table = ret[translated_table_name]
        current_table["OriginalTableName"] = table_name
        current_table["Members"] = {}

        fields = db.get_field_attributes(table_name)
        for field in fields:
            translated_field_name = translate_database_entry(table_name, field["COLUMN_NAME"], translation_dict)
            field["DATA_TYPE_NAME"] = msaccess.typeNames[field["DATA_TYPE"]]
            field["ORIGINAL_NAME"] = field["COLUMN_NAME"]
            current_table["Members"][translated_field_name] = field
    dump_yaml(output, ret)


@begin.start
def run():
    sys.stdout = codecs.getwriter(OUTPUT_ENCODING)(sys.stdout.detach())

