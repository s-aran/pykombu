# pykombu

DB data converter

## Usage

Write Python script.

- main.py

  ```py
  from pykombu2 import PyKombu
  from converter import SqlConverter

  kombu = PyKombu.load(source_path, replace_path, init_path, handlers_path)
  converter = SqlConverter()
  converter.set_table_name(table_name)
  r = kombu.process(converter)

  with open(
      os.path.join(DEST_DIR, "{}.sql".format(output_file_base)),
      mode="w",
      encoding="utf8",
  ) as f:
      f.writelines(r)
  ```

- source json file (required)

  Specify the table name for the key and the table data for the value. This format is the same as josn that DBeaver exports.

  ```json
  {
    "table_name": [
      {
        "ID": 1,
        "name": "aaa"
      },
      {
        "ID": 2,
        "name": "bbb"
      },
      {
        "ID": 3,
        "name": "ccc"
      }
    ]
  }
  ```

- replace json file (required)

  Specify the new column name for the key and the old name for the value. To add a new column, specify None for the vaule.

  ```json
  {
    "id": "ID",
    "value": "name",
    "new_column": null
  }
  ```

- init json file (optional)

  Specify the new column name for the key and an initial value for the value.

  ```json
  {
    "new_column": 0
  }
  ```

- handler script (optional)

  Python scripts can be specified for special processing on column values.

  The Python script must have a variable of type `dict` named `Handlers`. The key is the name of the new column name and the value is a function.

  The function is passed the column name (`str`), the value (`Any`), and the entire row (`dict[str, Any]`) as arguments.

  ```py
  from typing import Any


  def handle_value(key: str, value: Any, row: dict[str, Any]) -> str | None:
      if value is None:
          return None

      return "sample data {}".format(value)


  Handlers = {
      "value": handle_value,
  }
  ```

- output

  Given there files and an instance of `SqlConverter`, the following SQL is generated.

  ```sql
  INSERT INTO test_table_name (new_column, id, value) VALUES (0, 1, "sample data aaa");
  INSERT INTO test_table_name (new_column, id, value) VALUES (0, 2, "sample data bbb");
  INSERT INTO test_table_name (new_column, id, value) VALUES (0, 3, "sample data ccc");
  ```
