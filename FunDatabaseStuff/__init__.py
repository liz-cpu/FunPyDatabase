import sqlite3
from sqlite3.dbapi2 import Cursor
from FunDatabaseStuff.exceptions import (
    NonExistentColumnError,
    NonExistentTableError,
    ColumnError
)


class Table(object):

    def __init__(self, name: str, *cols: str) -> None:
        self.name = name
        self.columns = list(cols)

    def __str__(self) -> str:
        return self.name

    def select(self, db: Cursor, wildcard=False, *values):
        pass  # TODO

    def insert(self, db: Cursor, *values) -> None:
        if len(values) != len(self.columns):
            raise ColumnError(
                'Amount of values does not match amount of columns.')
        placeholders = '(' + ", ".join(tuple('?' for _ in values)) + ')'
        db.execute(f'INSERT INTO {self.name} VALUES {placeholders}', values)

    def update(self, db: Cursor, *vals) -> None:

        def create_query(x: list, sep: str) -> str:
            L = []
            for col, value in x:
                if isinstance(value, str):
                    value = f'\"{value}\"'
                L += [f'{col} = {value}']
            if len(L) > 1:
                return f" {sep} ".join(L)
            else:
                return str(L[0])

        name = self.name
        replace_list = []
        search_list = []
        for col_o_n in vals:
            if col_o_n[0] not in self.columns:
                raise NonExistentColumnError(
                    f'Column {col_o_n[0]} does not exist')

            if not isinstance(col_o_n, list) \
                    and not isinstance(col_o_n, tuple):
                raise TypeError('vals arguments should be list or tuple.')

            if not isinstance(col_o_n[0], str) or 1 <= len(col_o_n) > 3:
                raise TypeError("Function call is incorrect, should be `name, \
                current_value, new_value (optional)`.")

            if len(col_o_n) == 3:
                replace_list.append([col_o_n[0], col_o_n[2]])
                search_list.append(col_o_n[:2])
            else:
                search_list.append(col_o_n)

        if not replace_list:
            raise TypeError("Update should at least have 1 column to update.")
        else:
            updates = create_query(replace_list, ',')
            search_query = create_query(search_list, 'AND')
            db.execute(f'UPDATE {name} SET {updates} WHERE {search_query}')


class Database(object):

    def __init__(self, path: str, *tables: Table) -> None:
        self.__path = path
        self.tables = tables
        self.cursor, self.__conn = self.connect()
        for t in tables:
            self.test_table_existence(t)
        self.tables_names = [x.name for x in self.tables]

    def connect(self) -> tuple:
        db = sqlite3.connect(self.__path)
        return (db.cursor(), db)

    def test_table_existence(self, table: Table) -> bool:
        cursor = self.cursor
        table = str(table)
        try:
            cursor.execute(f"SELECT 1 FROM {table} LIMIT 1;")
            exists = True
        except sqlite3.OperationalError as e:
            message = e.args[0]
            if message.startswith("no such table"):
                raise NonExistentTableError('Table not in Database Object')
            else:
                raise sqlite3.OperationalError
        return exists

    def commit(self) -> None:
        self.__conn.commit()

    def check_table(self, table: Table) -> Table:
        if table not in self.tables:
            raise NonExistentTableError('Table not in Database Object')
        self.test_table_existence(table)
        return table

    def insert(self, table: Table, *values) -> None:
        table = self.check_table(table)
        table.insert(self.cursor, *values)
        self.commit()

    def update(self, table: Table, *col_values) -> None:
        table = self.check_table(table)
        table.update(self.cursor, *col_values)
        self.commit()

    def select(self, table: Table, wildcard=False, *values) -> list:
        table = self.check_table(table)
        return table.select(self.cursor, *values)

    def create(self):
        pass  # TODO ???
