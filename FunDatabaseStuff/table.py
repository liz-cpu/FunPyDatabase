from sqlite3.dbapi2 import Cursor
from FunDatabaseStuff.exceptions import (
    NonExistentColumnError,
    ColumnError,
    InvalidCreateError,
    ModelNonExistentError
)


class TableModel(object):
    """
    """

    def __init__(self, name: str, model: str) -> None:
        self.name = name
        self.model = self.check_model(model)

    def __repr__(self) -> str:
        return self.model

    def check_model(self, model: str) -> str:
        datatypes = ("INT", "CHAR", "TEXT", "BLOB", "REAL", "FLOAT",
                     "NUMERIC", "DECIMAL", "BOOLEAN", "DATE")
        model = model.strip()
        if model.startswith(f'CREATE TABLE IF NOT EXISTS {self.name} (') \
                or model.startswith(f'CREATE TABLE {self.name} ('):
            if model.endswith(';'):
                if "PRIMARY KEY" in model:
                    for type in datatypes:
                        if type in model:
                            return model
        raise InvalidCreateError('Create table is invalid')


class Table(object):
    """
    """

    def __init__(self, name: str, model: TableModel = None, *cols: str) -> None:
        self.name = name
        self.columns = list(cols)
        self.__model = model
        self.has_model = True if model else False

    def __str__(self) -> str:
        return self.name

    @staticmethod
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

    def create(self, db: Cursor) -> None:
        if self.__model:
            db.execute(str(self.__model))
        else:
            raise ModelNonExistentError('No model registered for this table')

    def select(self, db: Cursor, *cols: str, **where) -> list:

        name = self.name
        if not where and not cols:
            db.execute(f'SELECT * FROM {name}')

        elif cols and not where:
            db.execute(f'SELECT {", ".join(cols)} FROM {name}')

        elif where and not cols:
            where_list = [[column, value] for column, value in where.items()]
            where_str = self.create_query(where_list, 'AND')
            db.execute(f'SELECT * FROM {name} WHERE {where_str}')

        else:
            where_list = [[column, value] for column, value in where.items()]
            where_str = self.create_query(where_list, 'AND')
            db.execute(
                f'SELECT {", ".join(cols)} FROM {name} WHERE {where_str}')

        return [row for row in db]

    def insert(self, db: Cursor, *values) -> None:
        if len(values) != len(self.columns):
            raise ColumnError(
                'Amount of values does not match amount of columns.')
        placeholders = '(' + ", ".join(tuple('?' for _ in values)) + ')'
        db.execute(f'INSERT INTO {self.name} VALUES {placeholders}', values)

    def update(self, db: Cursor, *vals) -> None:
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
            updates = self.create_query(replace_list, ',')
            search_query = self.create_query(search_list, 'AND')
            db.execute(f'UPDATE {name} SET {updates} WHERE {search_query}')
