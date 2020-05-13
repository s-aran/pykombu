import argparse
import enum
import csv
import json
import os
import os.path
import pprint
import sys
import re
import xml.etree.ElementTree as ET
from typing import Optional, Union


class ArgumentManager(object):

    def __init__(self):
        self.__parser = argparse.ArgumentParser()
        self.__parser.add_argument('input', action='store', nargs=None, const=None, default=None, type=str, choices=None,
            help='input file', metavar=None)
        self.__parser.add_argument('rule', action='store', nargs=None, const=None, default=None, type=str, choices=None, 
            help='convertion rule json file', metavar=None)
        self.__parser.add_argument('output', action='store', nargs=None, const=None, default=None, type=str, choices=None,
            help='output file', metavar=None)
        self.__parser.add_argument('--table', action='store', nargs=None, const=None, default=None, type=str, choices=None,
            help='table name (sql only)', metavar=None)

    def parse(self):
        return self.__parser.parse_args()

    def get_parser(self):
        return self.__parser

class Logger(object):
    class Level(enum.IntEnum):
        Debug = enum.auto()
        Trace = enum.auto()
        Info = enum.auto()
        Warn = enum.auto()
        Error = enum.auto()
        Fatal = enum.auto()

    __level_str_dict = {
        Level.Debug: "Debug",
        Level.Trace: "Trace",
        Level.Info: "Info",
        Level.Warn: "Warn",
        Level.Error: "Error",
        Level.Fatal: "Fatal",
    }

    __level = Level.Warn

    @classmethod
    def __write(cls, level: Level, message: str):
        if level >= cls.__level:
            print("{0}: {1}".format(cls.__level_str_dict[level], message))

    @classmethod
    def get_level(cls) -> Level:
        return cls.__level

    @classmethod
    def set_level(cls, level: Level):
        cls.__level = level

    @classmethod
    def print_container(cls, data: Union[dict, list]):
        pprint.pprint(data, indent=2)

    @classmethod
    def debug(cls, message: str):
        cls.__write(cls.Level.Debug, message)

    @classmethod
    def trace(cls, message: str):
        cls.__write(cls.Level.Trace, message)

    @classmethod
    def info(cls, message: str):
        cls.__write(cls.Level.Info, message)

    @classmethod
    def warn(cls, message: str):
        cls.__write(cls.Level.Warn, message)

    @classmethod
    def error(cls, message: str):
        cls.__write(cls.Level.Error, message)

    @classmethod
    def fatal(cls, message: str):
        cls.__write(cls.Level.Fatal, message)


class ReaderBase(object):
    class Constants(object):
        FileMode = "r"

    def load(self, file_path: str) -> dict:
        raise NotImplementedError()


class CsvReader(ReaderBase):
    pass


class JsonReader(ReaderBase):
    pass


class XmlReader(ReaderBase):
    def load(self, file_path: str) -> list:
        result = []
        tree = ET.parse(file_path)
        root = tree.getroot()

        def dig(parent):
            r = {}
            for child in parent:
                if "tag" in dir(child):
                    r[child.tag] = child.text
                    continue

                r[parent.tag] = dig(child)
            return r

        for child in root:
            result.append(dig(child))
        return result


class ExportedKeepFormatAndLayoutByMsAccessReader(ReaderBase):
    def load(self, file_path: str) -> list:
        result = []
        with open(file_path, ReaderBase.Constants.FileMode) as f:
            header_dict = {}
            for i, l in enumerate(f.readlines()):
                line = l.lstrip().rstrip()
                if i % 2 == 0:
                    continue

                tokens = []
                token = ""
                for p, c in enumerate(line):
                    if p == 0:
                        continue

                    if c != "|":
                        token += c
                    else:
                        tokens.append(token.lstrip().rstrip())
                        token = ""

                if i == 1:
                    header_dict = {k: p for p, k in enumerate(tokens)}
                    continue

                if tokens:
                    result.append(
                        {list(header_dict.keys())[p]: v for p, v in enumerate(tokens)}
                    )

        return result


class SqlReader(ReaderBase):
    pass


class ConverterBase(object):
    def load(self, file_path: str) -> list:
        with open(file_path, 'r') as f:
            return json.load(f)

    def convert(self, data: list, types: dict) -> list:
        raise NotImplementedError()


class CsvConverter(ConverterBase):
    def convert(self, data: list, types: dict) -> list:
        date_re_str = "([0-9]{4})[/-]([0-9]{2})[/-]([0-9]{2})"
        time_re_str = "([0-9]{2}):([0-9]{2}):([0-9]{2})"
        re_date = re.compile(date_re_str)
        re_datetime = re.compile("{0} {1}".format(date_re_str, time_re_str))

        result = []
        for i, d in enumerate(data):
            tv = {}
            for k, v in d.items():
                tv[k] = v
                if v is None:
                    tv[k] = ''

                if k not in types.keys():
                    continue

                t = types[k]
                if t == "str":
                    tv[k] = '"{0}"'.format(v)
                elif t == "int":
                    pass
                elif t == "date":
                    if v == '':
                        tv[k] = None
                        continue
                    d = re_date.match(v)
                    f = '{0}-{1}-{2}'
                    tv[k] = "{0}".format(f.format(d[1], d[2], d[3]))
                elif t == "datetime":
                    if v == '':
                        tv[k] = None
                        continue
                    tv[k] = "{0}".format(v)
                elif t == "bool":
                    lv = v.lower()
                    if lv == 'true' or lv == 'yes':
                        tv[k] = 'True'
                    else:
                        tv[k] = 'False'

            result.append(tv)
        return result


class SqlConverter(ConverterBase):
    def convert(self, data: list, types: dict) -> list:
        date_re_str = "[0-9]{4}[/-][0-9]{2}[/-][0-9]{2}"
        time_re_str = "[0-9]{2}:[0-9]{2}:[0-9]{2}"
        re_date = re.compile(date_re_str)
        re_datetime = re.compile("{0} {1}".format(date_re_str, time_re_str))

        result = []
        for i, d in enumerate(data):
            tv = {}
            for k, v in d.items():
                tv[k] = v
                if k not in types.keys():
                    continue

                t = types[k]
                if t == "str":
                    tv[k] = "'{0}'".format(v)
                elif t == "int":
                    pass
                elif t == "date":
                    tv[k] = "'{0}'".format(v)
                elif t == "datetime":
                    tv[k] = "'{0}'".format(v)
                elif t == "bool":
                    lv = v.lower()
                    if lv == 'true' or lv == 'yes':
                        tv[k] = True
                    else:
                        tv[k] = False

            result.append(tv)
        return result


class WriterBase(object):
    class Constants(object):
        FileMode = "w"

    def save(self, file_path: str, data: list):
        raise NotImplementedError()


class CsvWriter(WriterBase):
    def save(
        self,
        file_path: str,
        data: list
    ):
        with open(file_path, WriterBase.Constants.FileMode) as f:
            columns = []
            for i, d in enumerate(data):
                values = []

                if not columns:
                    for k in d.keys():
                        columns.append('"{0}"'.format(k))
                    line = '{0}\n'.format(','.join(columns))
                    f.write(line)

                for k, v in d.items():
                    values.append('{0}'.format(v if v is not None else ''))
                line = '{0}\n'.format(','.join(values))
                f.write(line)


class JsonWriter(WriterBase):
    pass


class XmlWriter(WriterBase):
    pass


class SqlWriter(WriterBase):
    def save(
        self,
        file_path: str,
        data: list,
        table_name: str = "table",
        truncate: bool = False,
        db_name: Optional[str] = None,
    ):
        with open(file_path, WriterBase.Constants.FileMode) as f:
            table = (
                table_name if db_name is None else "{0}.{1}".format(db_name, table_name)
            )
            if truncate:
                f.write("TRUNCATE TABLE {0};\n".format(table))

            base_sql = "INSERT INTO {0} ({1}) VALUES ({2});\n"

            for i, d in enumerate(data):
                columns = []
                values = []
                for k, v in d.items():
                    columns.append('{0}'.format(k))
                    values.append('{0}'.format(v))
                sql = base_sql.format(table, ", ".join(columns), ", ".join(values))
                f.write(sql)


class FileTypeDetecter(object):
    class FileType(enum.IntEnum):
        Csv = enum.auto()
        Json = enum.auto()
        Xml = enum.auto()
        ExportedKeepFormatAndLayoutTextByMsAccess = enum.auto()
        Sql = enum.auto()
        Others = 255

    @classmethod
    def __is_eq_ext(cls, file_path: str, extension: str) -> bool:
        _, ext = os.path.splitext(file_path)
        return ext.lower() == ".{0}".format(extension.lower())

    @classmethod
    def __is_csv(cls, file_path: str) -> bool:
        if not cls.__is_eq_ext(file_path, "csv"):
            return False

        with open(file_path, "r", newline="") as f:
            try:
                csv.reader(f)
            except:
                return False
        return True

    @classmethod
    def __is_json(cls, file_path: str) -> bool:
        if not cls.__is_eq_ext(file_path, "json"):
            return False

        with open(file_path, "r") as f:
            try:
                json.load(f)
            except:
                return False
        return True

    @classmethod
    def __is_xml(cls, file_path: str) -> bool:
        if not cls.__is_eq_ext(file_path, "xml"):
            return False

        with open(file_path, "r") as f:
            try:
                ET.parse(f)
            except:
                return False
        return True

    @classmethod
    def __is_exported_keep_format_and_layout_text_by_ms_access(
        cls, file_path: str
    ) -> bool:
        if not cls.__is_eq_ext(file_path, "txt"):
            return False

        re_row_separator = re.compile(r"^-+")
        re_data = re.compile("^|( +.*? +|)+$")

        with open(file_path, "r") as f:
            for i, l in enumerate(f.readlines()):
                line = l.lstrip().rstrip()
                if i == 0:
                    continue

                if i % 2 == 0:
                    result = re_row_separator.match(line) is not None
                    if not result:
                        return False
                else:
                    result = re_data.match(line) is not None
                    if not result:
                        return False
        return True

    @classmethod
    def __is_sql(cls, file_path: str) -> bool:
        # XXX: easy check
        if not cls.__is_eq_ext(file_path, "txt") and not cls.__is_eq_ext(
            file_path, "sql"
        ):
            return False

        query_string_list = [
            "USE",
            "SELECT",
            "CREATE",
            "UPDATE",
            "DELETE",
            "DROP",
        ]

        with open(file_path, "r") as f:
            is_comment = False
            for l in f.readlines():
                line = l.lstrip().rstrip()
                if len(line) >= 2 and (line[0] == "/" and line[1] == "*"):
                    Logger.trace("is_comment = True")
                    is_comment = True

                if "*/" not in line:
                    Logger.trace("is_comment = False")
                    is_comment = False

                if (
                    is_comment
                    or len(line) <= 0
                    or line[0] == "#"
                    or (len(line) >= 2 and line[0] == "-" and line[1] == "-")
                ):
                    Logger.trace("comment line, skip")
                    continue

                first = l.split()[0].upper()
                Logger.trace("first = {0}".format(first))
                if first in query_string_list:
                    return True
        return False

    @classmethod
    def __helper(cls, file_type: FileType, judgement: bool) -> FileType:
        return file_type if judgement else cls.FileType.Others

    @classmethod
    def detect(cls, file_path: str) -> FileType:
        if not os.path.exists(file_path):
            Logger.fatal("{0} is not found.".format(file_path))
            exit()

        results = {}
        results[cls.FileType.Csv] = cls.__is_csv(file_path)
        results[cls.FileType.Json] = cls.__is_json(file_path)
        results[cls.FileType.Xml] = cls.__is_xml(file_path)
        results[
            cls.FileType.ExportedKeepFormatAndLayoutTextByMsAccess
        ] = cls.__is_exported_keep_format_and_layout_text_by_ms_access(file_path)
        results[cls.FileType.Sql] = cls.__is_sql(file_path)
        results[cls.FileType.Others] = True
        Logger.print_container(results)

        return [k for k, v in results.items() if v][0]

    @classmethod
    def get_reader(cls, file_type: FileType) -> ReaderBase:
        readers = {
            cls.FileType.Csv: CsvReader(),
            cls.FileType.Json: JsonReader(),
            cls.FileType.Xml: XmlReader(),
            cls.FileType.ExportedKeepFormatAndLayoutTextByMsAccess: ExportedKeepFormatAndLayoutByMsAccessReader(),
            cls.FileType.Others: None,
        }

        return readers[file_type]

def convert(args, converter):
    rule_filepath = args.rule
    convert_rules = converter.load(rule_filepath)
    return converter.convert(data, convert_rules)
    # Logger.print_container(out_data)

if __name__ == "__main__":
    # Logger.set_level(Logger.Level.Debug)
    ap = ArgumentManager()
    args = ap.parse()

    # path = "test.xml"
    path = args.input
    file_type = FileTypeDetecter.detect(path)
    # print(file_type)
    reader = FileTypeDetecter.get_reader(file_type)
    data = reader.load(path)

    out_path = args.output
    _, out_ext = os.path.splitext(out_path)
    lower_ext = out_ext.lower()
    if lower_ext == '.sql':
        out_data = convert(args, SqlConverter())
        writer = SqlWriter()
        writer.save(out_path, out_data, args.table)
    elif lower_ext == '.csv':
        out_data = convert(args, CsvConverter())
        writer = CsvWriter()
        writer.save(out_path, out_data)


