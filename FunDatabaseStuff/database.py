import sqlite3
from FunDatabaseStuff.exceptions import NonExistentTableError
from FunDatabaseStuff.table import Table


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

    def add(self, *tables: Table) -> None:
        for t in tables:
            self.test_table_existence(t)
        self.tables += tables

    def remove(self, *tables: Table) -> None:
        for t in tables:
            self.test_table_existence(t)
        self.tables -= tables

    def test_table_existence(self, table: Table) -> bool:
        cursor = self.cursor
        try:
            cursor.execute(f"SELECT 1 FROM {str(table)} LIMIT 1;")
        except sqlite3.OperationalError as e:
            message = e.args[0]
            if message.startswith("no such table"):
                if table.has_model:
                    table.create(cursor)
                else:
                    raise NonExistentTableError('Table not in Database Object')
            else:
                raise sqlite3.OperationalError
        return True

    def commit(self) -> None:
        self.__conn.commit()

    def check_table(self, table: Table) -> Table:
        if table not in self.tables:
            if not table.model:
                raise NonExistentTableError('Table not in Database Object')
            else:
                raise NonExistentTableError('Table not in Database Object',
                                            'but table has a model.',
                                            'Perhaps use table.create()?')
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

    def select(self, table: Table, *cols: str, **where) -> list:
        table = self.check_table(table)
        return table.select(self.cursor, *cols, **where)

    def create(self, *tables: Table) -> None:
        for table in tables:
            table.create(self.cursor)
        self.commit()
