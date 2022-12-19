from typing import Any
from datetime import datetime, date, time


class Converter(object):
    """Converter base class"""

    @staticmethod
    def to_string(value: Any, escape_dq: bool = False) -> str:
        """Convert to str

        Args:
            value (Any): Source value
            escape_dq (bool, optional): True: escape double quote. Defaults to False.

        Raises:
            Exception: Unexpected type

        Returns:
            str: Converted value
        """
        if value is None:
            return "null"

        if isinstance(value, bool):
            return "{}".format(1 if value else 0)

        if isinstance(value, (int, float)):
            return "{}".format(value)

        if isinstance(value, str):
            return '\\"{}\\"'.format(value) if escape_dq else '"{}"'.format(value)

        if isinstance(value, datetime):
            return '"{}-{}-{} {}:{}:{}"'.format(
                value.year,
                value.month,
                value.day,
                value.hour,
                value.minute,
                value.second,
            )

        if isinstance(value, date):
            return '"{}-{}-{}"'.format(value.year, value.month, value.day)

        if isinstance(value, time):
            return '"{}:{}:{}"'.format(value.hour, value.minute, value.second)

        if isinstance(value, list):
            return '"[{}]"'.format(
                ", ".join(Converter._list_items_to_string(value, escape_dq=True))
            )

        if isinstance(value, dict):
            li = []
            for k, v in value.items():
                li.append(
                    "{}: {}".format(
                        Converter.to_string(k, escape_dq=True),
                        Converter.to_string(v, escape_dq=True),
                    )
                )

            return '"{{{}}}"'.format(", ".join(li))

        raise Exception("{} is invalid type".format(value))

    @staticmethod
    def _list_items_to_string(
        list_items: list[Any], escape_dq: bool = False
    ) -> list[str]:
        return [Converter.to_string(e, escape_dq=escape_dq) for e in list_items]

    def convert(self, row: dict[str, Any]) -> str:
        """Convert

        Args:
            row (dict[str, Any]): Source data

        Raises:
            NotImplementedError: When called directly this method.

        Returns:
            str: Converted data
        """
        raise NotImplementedError


class CsvConverter(Converter):
    """CSV converter class"""

    def __init__(self):
        super().__init__()
        self.set_header_outputed(False)

    def get_header_outputed(self) -> bool:
        """Get header outputed flag

        Returns:
            bool: True: Header was outputed, False: Header was not outputed.
        """
        return self.__header_outputed

    def set_header_outputed(self, value: bool):
        """Set header outputed flag

        Args:
            value (bool): True: Header was outputed, False: Header was not outputed.
        """
        self.__header_outputed = value

    def convert(self, row: dict[str, Any]) -> str:
        """Convert

        Args:
            row (dict[str, Any]): Source data

        Returns:
            str: Converted data
        """
        if not self.get_header_outputed():
            self.set_header_outputed(True)
            return "\n".join(
                [
                    ",".join(Converter._list_items_to_string(list(row.keys()))),
                    ",".join(Converter._list_items_to_string(list(row.values()))),
                ],
            )

        return ",".join(Converter._list_items_to_string(list(row.values())))


class SqlConverter(Converter):
    """SQL converter class"""

    def __init__(self):
        super().__init__()
        self.set_table_name("")

    def get_table_name(self) -> str:
        """Get table name

        Returns:
            str: Table name
        """
        return self.__table_name

    def set_table_name(self, name: str):
        """Set table name

        Args:
            name (str): Table name
        """
        self.__table_name = name

    def convert(self, row: dict[str, Any]) -> str:
        """Convert

        Args:
            row (dict[str, Any]): Source data

        Returns:
            str: Converted data
        """
        return "INSERT INTO {} ({}) VALUES ({});".format(
            self.get_table_name(),
            ", ".join(row.keys()),
            ", ".join(SqlConverter._list_items_to_string(list(row.values()))),
        )
