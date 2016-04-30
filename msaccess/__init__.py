# -*- coding: utf-8 -*-
from collections import defaultdict
import datetime
import time
import win32com.client
import pywintypes
import sys
import codecs
from enum import Enum


def com_exception_print(func):
    def deco(*args):
        try:
            return func(*args)
        except pywintypes.com_error as e:
            print(str(e.args[1], "sjis"), file=sys.stderr)
            print(str(e.args[2][2], "sjis"), file=sys.stderr)
            raise e

    return deco


def EnsureUnicode(text):
    if type(text) == type(""):
        return text
    else:
        raise Exception("require unicode sting")


@com_exception_print
def ConvertFromAdoList(adoList):
    return [adoList.Item(x) for x in range(adoList.Count)]


typeNames = {
    0: 'adEmpty',
    2: 'adSmallInt',
    3: 'adInteger',
    4: 'adSingle',
    5: 'adDouble',
    6: 'adCurrency',
    7: 'adDate',
    8: 'adBSTR',
    9: 'adIDispatch',
    10: 'adError',
    11: 'adBoolean',
    12: 'adVariant',
    13: 'adIUnknown',
    14: 'adDecimal',
    16: 'adTinyInt',
    17: 'adUnsignedTinyInt',
    18: 'adUnsignedSmallInt',
    19: 'adUnsignedInt',
    20: 'adBigInt',
    21: 'adUnsignedBigInt',
    64: 'adFileTime',
    72: 'adGUID',
    128: 'adBinary',
    129: 'adChar',
    130: 'adWChar',
    131: 'adNumeric',
    132: 'adUserDefined',
    133: 'adDBDate',
    134: 'adDBTime',
    135: 'adDBTimeStamp',
    136: 'adChapter',
    138: 'adPropVariant',
    139: 'adVarNumeric',
    200: 'adVarChar',
    201: 'adLongVarChar',
    202: 'adVarWChar',
    203: 'adLongVarWChar',
    204: 'adVarBinary',
    205: 'adLongVarBinary'}

SchemaTypes = Enum("SchemaTypes", "TABLE")

class MsAccessDb:
    types = set()
    
    @com_exception_print
    def __init__(self, db_name):
        connection_string = (
            "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=%s;" % db_name)
        self.con = win32com.client.dynamic.Dispatch("ADODB.Connection")
        self.con.Open(connection_string)

        self.cat = win32com.client.Dispatch("ADOX.Catalog")
        self.cat.ActiveConnection = self.con

    @com_exception_print
    def close(self):
        self.con.Close()

    @classmethod
    def convert_pytime_to_datetime(cls, pytime):
        import datetime

        return datetime.datetime(pytime.year, pytime.month, pytime.day, pytime.hour, pytime.minute, pytime.second)

    @classmethod
    def regulate_value(cls, value):
        """PyTimeとかを普通のdatetimeとかに直しておく"""
        try:
            if "datetime" in str(type(value)):
                return cls.convert_pytime_to_datetime(value)
            elif "Decimal" in str(type(value)):
                return float(value)
            elif isinstance(value, str):
                return value
            else:
                return value
        except:
            raise

    @classmethod
    def regulate_value_for_mongodb(cls, value):
        type_name = str(type(value))
        if type_name not in cls.types:
            cls.types = cls.types | set([type_name])
            print("type: {0}".format(str(type(value))))
            import sys
            sys.stdout.flush()

        
        if isinstance(value, datetime.datetime) and False:
            return "ISODate({0})".format(value.isoformat())
        elif "Decimal" in str(type(value)):
            return float(value)
        else:
            return value

    @com_exception_print
    def execute_query(self, query):
        return self.con.Execute(query.encode("cp932"))[0]

    def iterate_query(self, query):
        qry = self.execute_query("Select * From %s;" % query)
        while not qry.EOF:
            try:
                yield [MsAccessDb.regulate_value(y.Value) for y in qry.Fields]
            except Exception as e:
                print("convert error '%s' in '%s'" % ([y.Value for y in qry.Fields], query), file=sys.stderr)
                print(str(e), file=sys.stderr)
                raise
            qry.MoveNext()

    @com_exception_print
    def open_schema(self):
        return self.con.OpenSchema(20)

    @com_exception_print
    def open_columns(self, tableName):
        return self.con.OpenSchema(4, [None, None, tableName])

    @com_exception_print
    def get_schema_names(self, schemaType):
        ret = []
        schemas = self.open_schema()
        while not schemas.EOF:
            fields = schemas.Fields
            # print fields.Item("TABLE_TYPE").Value, fields.Item("TABLE_NAME").Value
            if fields.Item("TABLE_TYPE").Value == str(schemaType):
                ret.append(fields.Item("TABLE_NAME").Value)
            schemas.MoveNext()
        return ret

    @com_exception_print
    def get_table_names(self):
        return self.get_schema_names("TABLE")

    @com_exception_print
    def get_field_names(self, queryName):
        field_list = [ (field.Item("ORDINAL_POSITION").Value, field.Item("COLUMN_NAME").Value) for field in self.GetSchemaColumns(queryName)]
        field_list.sort()
        return [ field_name for order, field_name in field_list ]

    @com_exception_print
    def get_field_attributes(self, queryName):
        field_list = [ dict([ (field(i).Name, field(i).Value) for i in range(field.Count) ]) for field in self.GetSchemaColumns(queryName)]
        return field_list

    @com_exception_print
    def get_table_and_field_names(self):
        for table_name in self.get_table_names():
            for field_name in self.get_field_names(table_name):
                yield (table_name, field_name)

    @com_exception_print
    def GetProcedureNames(self):
        return [self.cat.Procedures.Item(i).Name for i in range(self.cat.Procedures.Count) if
                "~" != self.cat.Procedures.Item(i).Name[0]]

    @com_exception_print
    def HasProcedure(self, name):
        return EnsureUnicode(name) in self.GetProcedureNames()

    @com_exception_print
    def GetViewNames(self):
        return [self.cat.Views.Item(i).Name for i in range(self.cat.Views.Count) if
                "~" != self.cat.Views.Item(i).Name[0]]

    @com_exception_print
    def HasView(self, name):
        return EnsureUnicode(name) in self.GetViewNames()

    @com_exception_print
    def GetQueryDefinitionNames(self):
        return sorted(self.GetProcedureNames() + self.GetViewNames())

    @com_exception_print
    def GetQueryDefinitionObject(self, name):
        name = EnsureUnicode(name)
        if self.HasProcedure(name):
            return self.cat.Procedures.Item(name)
        elif self.HasView(name):
            return self.cat.Views.Item(name)
        else:
            raise Exception("not found QueryDefinitionObject: %s" % name)


    @com_exception_print
    def PrintFields(self, fields):
        print("-" * 40)
        for i in range(fields.Count):
            print("-", fields[i].Name, fields[i].Value)

    @com_exception_print
    def GetSchemaColumns(self, tableName):
        """ 特定スキーマのカラムを返す
            ADO の OpenSchema とSchemaEnum を参照
            http://msdn.microsoft.com/ja-jp/library/cc389872
        """
        schemas = self.open_columns(tableName)
        while not schemas.EOF:
            fields = schemas.Fields
            yield fields
            schemas.MoveNext()

    @classmethod
    @com_exception_print
    def ConvertEasyField(cls, field):
        name = cls.regulate_value(field.Item("COLUMN_NAME").Value)
        attrs = {}
        attrs["Type"] = typeNames[field.Item("DATA_TYPE").Value]
        attrs["IsNullable"] = field.Item("IS_NULLABLE").Value
        attrs["OrdinalPosition"] = field.Item("ORDINAL_POSITION").Value
        return (name, attrs)

    def GetEasySchema(self, tableName):
        """ 他の形式への変換が簡単になるような形のスキーマにする
            あと、ORDINAL_POSITION でソートしておく

            { "columnName":
                { Type : "Char(3:4)",
                  IsNullable : true,
                  IsKey : false },
              ...
            }
        """
        ret = []
        for field in self.GetSchemaColumns(tableName):
            ret.append(self.ConvertEasyField(field))
        return dict(ret)

    @com_exception_print
    def PrintPrimaryKeys(self):
        keys = self.con.OpenSchema(28)
        while not keys.EOF:
            f = keys.Fields
            for i in range(f.Count):
                print(f.Item(i).Name, f.Item(i).Value)
                keys.MoveNext()

    def First(self, query):
        """最初のデータだけ取り出す．合計値を取り出すときとかに"""
        return next(self.iterate_query(query))

    @com_exception_print
    def CreateQueryDefinition(self, name, queryString):
        """ データベースへクエリ定義を登録する
        """
        # クエリのインスタンスを作成
        queryDefinition = win32com.client.Dispatch("ADODB.Command")
        # 定義を設定
        queryDefinition.CommandText = EnsureUnicode(queryString)
        # 追加
        self.cat.Procedures.Append(EnsureUnicode(name), queryDefinition)

    @com_exception_print
    def DeleteQueryDefinition(self, name):
        """ 登録されているクエリを削除する
        """
        name = EnsureUnicode(name)
        if self.HasProcedure(name):
            self.cat.Procedures.Delete(name)
        elif self.HasView(name):
            self.cat.Views.Delete(name)
        else:
            raise Exception("%s is not found" % name)


def MakeFromPhraseOfInnerJoin(field, names):
    head = names[0]
    names = names[1:]

    ret = head
    for name in names:
        ret = "(%(ret)s) inner join %(name)s on %(head)s.%(field)s = %(name)s.%(field)s" % locals()
    return ret
