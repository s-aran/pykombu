from datetime import datetime, date, time

from converter import SqlConverter, CsvConverter


class TestConverter:
    def test_sql_converter(self):
        table_name = "test_table"
        source = {
            "aaa": 0,
            "bbb": "foo",
            "ccc": None,
            "ddd": datetime(2022, 1, 23, 12, 34, 56),
            "eee": date(2022, 1, 23),
            "fff": time(12, 34, 56),
            "ggg": [123, "foobar", None],
            "hhh": {"a": 1, "b": "c", "d": None},
        }
        expected = 'INSERT INTO test_table (aaa, bbb, ccc, ddd, eee, fff, ggg, hhh) VALUES (0, "foo", null, "2022-1-23 12:34:56", "2022-1-23", "12:34:56", "[123, \\"foobar\\", null]", "{\\"a\\": 1, \\"b\\": \\"c\\", \\"d\\": null}");'
        converter = SqlConverter()
        converter.set_table_name(table_name)
        actual = converter.convert(source)
        assert expected == actual

    def test_csv_converter(self):
        source = {
            "aaa": 0,
            "bbb": "foo",
            "ccc": None,
            "ddd": datetime(2022, 1, 23, 12, 34, 56),
            "eee": date(2022, 1, 23),
            "fff": time(12, 34, 56),
            "ggg": [123, "foobar", None],
            "hhh": {"a": 1, "b": "c", "d": None},
        }
        expected_body = '"aaa","bbb","ccc","ddd","eee","fff","ggg","hhh"\n0,"foo",null,"2022-1-23 12:34:56","2022-1-23","12:34:56","[123, \\"foobar\\", null]","{\\"a\\": 1, \\"b\\": \\"c\\", \\"d\\": null}"'

        converter = CsvConverter()

        actual_body = converter.convert(source)
        assert expected_body == actual_body
