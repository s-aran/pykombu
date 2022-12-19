from pykombu2 import PyKombu


class TestPyKombu:
    def test_replace_table(self):
        source = [
            {"aaa": 0, "bbb": 1, "ccc": 2},
            {"aaa": 4, "bbb": 5, "ccc": 6},
            {"aaa": 7, "bbb": 8, "ccc": 9},
        ]

        replace = {"AAA": "aaa", "BBB": "bbb", "CCC": "ccc", "DDD": None}

        expected = [
            {"AAA": 0, "BBB": 1, "CCC": 2},
            {"AAA": 4, "BBB": 5, "CCC": 6},
            {"AAA": 7, "BBB": 8, "CCC": 9},
        ]
        actual = PyKombu._PyKombu__replace_column_name(source, replace)

        assert expected == actual

    def test_apply_initialization_table(self):
        source = [
            {"aaa": None, "bbb": 1, "ccc": 2},
            {"aaa": 4, "bbb": None, "ccc": 6},
            {
                "aaa": 7,
                "bbb": 8,
            },
        ]

        initialization = {"aaa": "foo", "bbb": "bar", "ccc": "baz", "ddd": "qux"}

        expected = [
            {"aaa": "foo", "bbb": 1, "ccc": 2, "ddd": "qux"},
            {"aaa": 4, "bbb": "bar", "ccc": 6, "ddd": "qux"},
            {"aaa": 7, "bbb": 8, "ccc": "baz", "ddd": "qux"},
        ]
        actual = PyKombu._PyKombu__apply_initialization_table(source, initialization)

        assert expected == actual

    def test_execute_handler_table(self):
        source = [
            {"aaa": None, "bbb": 1, "ccc": 2},
            {"aaa": 4, "bbb": None, "ccc": 6},
            {"aaa": 7, "bbb": 8, "ccc": None},
        ]

        handlers = {
            "aaa": lambda k, v, e: "{}{}{}_a".format(k, v, e),
            "bbb": lambda k, v, e: "{}{}{}_b".format(k, v, e),
            "ccc": lambda k, v, e: "{}{}{}_c".format(k, v, e),
        }

        expected = [
            {
                "aaa": "aaaNone{'aaa': None, 'bbb': 1, 'ccc': 2}_a",
                "bbb": "bbb1{'aaa': None, 'bbb': 1, 'ccc': 2}_b",
                "ccc": "ccc2{'aaa': None, 'bbb': 1, 'ccc': 2}_c",
            },
            {
                "aaa": "aaa4{'aaa': 4, 'bbb': None, 'ccc': 6}_a",
                "bbb": "bbbNone{'aaa': 4, 'bbb': None, 'ccc': 6}_b",
                "ccc": "ccc6{'aaa': 4, 'bbb': None, 'ccc': 6}_c",
            },
            {
                "aaa": "aaa7{'aaa': 7, 'bbb': 8, 'ccc': None}_a",
                "bbb": "bbb8{'aaa': 7, 'bbb': 8, 'ccc': None}_b",
                "ccc": "cccNone{'aaa': 7, 'bbb': 8, 'ccc': None}_c",
            },
        ]
        actual = PyKombu._PyKombu__execute_handler_table(source, handlers)

        assert expected == actual
