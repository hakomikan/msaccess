#!python
# encoding: utf-8
import begin
import msaccess
import locale
import codecs
import sys

OUTPUT_ENCODING = "utf-8"


def open_output_stream(filename):
    """
    '-' だったら、標準出力を開く
    そうでなければ、ファイルを開く
    """
    if filename == "-":
        return sys.stdout
    else:
        return open(filename, "w")


def dump_yaml(filename, data):
    import yaml
    f = open_output_stream(filename)
    f.write(
        yaml.safe_dump(
            data,
            allow_unicode=True,
            default_flow_style=False,
            encoding=OUTPUT_ENCODING))


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
        print( "/".join([table_name,field_name]))

@begin.start
def run():
    sys.stdout = codecs.getwriter(OUTPUT_ENCODING)(sys.stdout.detach())

