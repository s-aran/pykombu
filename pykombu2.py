import json
import os.path
from typing import Any
from converter import Converter


class PyKombu:
    """Converter control class"""

    __HandlersKey = "Handlers"

    @staticmethod
    def load(
        src_path: str,
        replace_path: str,
        init_path: str | None = None,
        handler_path: str | None = None,
    ) -> "PyKombu":
        """Load data and parameter

        Args:
            src_path (str): Source data filepath
            replace_path (str): Replace table json filepath
            init_path (str | None, optional): Initialization table json filepath. Defaults to None.
            handler_path (str | None, optional): Handlers python script filepath. Defaults to None.

        Raises:
            Exception: Unexpected error

        Returns:
            PyKombu: Instance
        """
        result = PyKombu()
        result.__initialize_instance()

        if not os.path.exists(src_path):
            raise Exception("{} not exists".format(src_path))

        with open(src_path, mode="r", encoding="utf8") as f:
            loaded_json = json.load(f)

            if len(loaded_json.keys()) <= 0:
                raise Exception("{} is maybe empty".format(src_path))

            first_key = list(loaded_json.keys())[0]
            data = loaded_json[first_key]

            result.__set_loaded_data(data)

        if replace_path is not None:
            if not os.path.exists(replace_path):
                raise Exception("{} not exists".format(replace_path))

            with open(replace_path, mode="r", encoding="utf8") as f:
                loaded_json = json.load(f)

                if len(loaded_json.keys()) <= 0:
                    raise Exception("{} is maybe empty".format(replace_path))

                result.__set_replace_table(loaded_json)

        if init_path is not None:
            if not os.path.exists(init_path):
                raise Exception("{} not exists", format(init_path))

            with open(init_path, mode="r", encoding="utf8") as f:
                loaded_json = json.load(f)

                if len(loaded_json.keys()) <= 0:
                    raise Exception("{} is maybe empty".format(init_path))

                result.__set_initialization_table(loaded_json)

        if handler_path is not None:
            if not os.path.exists(handler_path):
                raise Exception("{} not exists".format(handler_path))

            with open(handler_path, mode="r", encoding="utf8") as f:
                loaded_code = f.read()
                global_objects: dict[str, Any] = {}

                exec(loaded_code, global_objects)

                if PyKombu.__HandlersKey not in global_objects.keys():
                    raise Exception(
                        "{} not in {}".format(PyKombu.__HandlersKey, handler_path)
                    )

                result.__set_handlers(global_objects[PyKombu.__HandlersKey])

        return result

    def __initialize_instance(self):
        self.__set_replace_table({})
        self.__set_initialization_table({})
        self.__set_handlers({})

    def get_loaded_data(self) -> list[dict[str, Any]]:
        """Get loaded json data

        Returns:
            list[dict[str, Any]]: Loaded json data
        """
        return self.__loaded_data

    def __set_loaded_data(self, data: list[dict[str, Any]]):
        self.__loaded_data = data

    def get_replace_table(self) -> dict[str, str | None]:
        """Get replace table

        Returns:
            dict[str, str | None]: Replace table
        """
        return self.__replace_table

    def __set_replace_table(self, table: dict[str, str | None]):
        self.__replace_table = table

    def get_initialization_table(self) -> dict[str, Any]:
        """Get initialization table

        Returns:
            dict[str, Any]: Initialization table
        """
        return self.__initialization_table

    def __set_initialization_table(self, table: dict[str, Any]):
        self.__initialization_table = table

    def get_handlers(self) -> dict[str, Any]:
        """Get handlers

        Returns:
            dict[str, Any]: Handlers
        """
        return self.__handlers

    def __set_handlers(self, handlers: dict[str, Any]):
        self.__handlers = handlers

    @staticmethod
    def __replace_column_name(
        data: list[dict[str, Any]], replace_table: dict[str, str | None]
    ) -> list[dict[str, Any]]:
        result = []
        for e in data:
            row = {}
            for k, v in e.items():
                for rt_k, rt_v in replace_table.items():
                    if k == rt_v:
                        row[rt_k] = v
                        continue
                    # if rt_v is None:
                    #     row[rt_k] = None
                    #     continue

            result.append(row)

        return result

    @staticmethod
    def __apply_initialization_table(
        data: list[dict[str, Any]], init_table: dict[str, Any]
    ) -> list[dict[str, Any]]:
        result = []
        for e in data:
            row = {}
            for k, v in e.items():
                if v is None and k in init_table.keys():
                    row[k] = init_table[k]
                    continue

                for it_k, it_v in init_table.items():
                    if it_k not in e:
                        row[it_k] = it_v
                        continue

                row[k] = v

            result.append(row)

        return result

    @staticmethod
    def __execute_handler_table(
        data: list[dict[str, Any]], handlers: dict[str, Any]
    ) -> list[dict[str, Any]]:
        result = []
        for e in data:
            row = {}
            for k, v in e.items():
                if k in handlers.keys():
                    row[k] = handlers[k](k, v, e)
                    continue

                row[k] = v

            result.append(row)

        return result

    def __pre_process(self) -> list[dict[str, Any]]:
        data = self.__replace_column_name(
            self.get_loaded_data(), self.get_replace_table()
        )
        data = self.__apply_initialization_table(data, self.get_initialization_table())
        data = self.__execute_handler_table(data, self.get_handlers())
        return data

    def process(self, converter: Converter) -> list[str]:
        """Execute convert process

        Args:
            converter (Converter): Instance of inherited the Converter class

        Returns:
            list[str]: Convert result
        """
        data = self.__pre_process()
        if len(data) <= 0:
            return []

        result = []

        pre = converter.pre_data(data[0])
        if len(pre) > 0:
            result.append("{}\n".format(pre))

        for e in data[:-1]:
            result.append(
                "{}{}\n".format(converter.convert(e), converter.get_delimiter())
            )

        result.append(
            "{}{}\n".format(converter.convert(data[-1]), converter.get_last_delimiter())
        )

        post = converter.post_data(data[0])
        if len(post) > 0:
            result.append("{}\n".format(post))

        return result
