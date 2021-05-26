import sqlite3
from FunDatabaseStuff.exceptions import NonExistentTableError
from FunDatabaseStuff.table import Table
from FunDatabaseStuff.types import Value, ColValueUpdateList


class Database(object):
    """Creates an object-representation of an SQLite3 Database

    :param path: Path to database
    :type path: str
    :param *tables: All tables in the Database
    :type tables: Table
    """

    def __init__(self, path: str, *tables: Table) -> None:
        """Constructor method
        """
        self.__path = path
        self.tables = tables
        self.cursor, self.__conn = self.connect()
        for t in tables:
            self.test_table_existence(t)
        self.tables_names = [x.name for x in self.tables]

    def connect(self) -> tuple:
        """Connects the database to an actual SQLite3 Database
        creates a cursor

        :return: The cursor and the connection
        :rtype: tuple
        """
        db = sqlite3.connect(self.__path)
        return (db.cursor(), db)

    def add(self, *tables: Table) -> None:
        """Adds a Table to the database
        """
        for t in tables:
            self.test_table_existence(t)
        self.tables += tables

    def commit(self) -> None:
        """Commits to the database
        """
        self.__conn.commit()

    def remove(self, *tables: Table) -> None:
        """Removes a table from the database
        """
        for t in tables:
            self.test_table_existence(t)
        self.tables -= tables

    def test_table_existence(self, table: Table) -> bool:
        """Tests if a table exists in the SQLite database

        :param table: Table to be checked of existence
        :type table: Table
        :raises NonExistentTableError: Table doesn't exist
        :raises sqlite3.OperationalError: sqlite3 general error
        :return: True if tablem exists
        :rtype: bool
        """
        cursor = self.cursor
        try:
            cursor.execute(f"SELECT 1 FROM {table.name} LIMIT 1;")
        except sqlite3.OperationalError as e:
            message = e.args[0]
            if message.startswith("no such table"):
                if table.has_create:
                    table.create(cursor)
                    return True
                else:
                    raise NonExistentTableError('Table does not exist')
            else:
                raise sqlite3.OperationalError
        return True

    def check_table(self, table: Table) -> Table:
        """Checks if table exists.

        :param table: Table to be checked of existence
        :type table: Table
        :raises NonExistentTableError: Table does not exist
        :return: table
        :rtype: Table
        """
        if table not in self.tables:
            if not self.test_table_existence(table):
                raise NonExistentTableError('Table does not exist')
        return table

    def insert(self, table: Table, *values: Value) -> None:
        """INSERT INTO table method
        """
        table = self.check_table(table)
        table.insert(self.cursor, *values)
        self.commit()

    def update(self, table: Table, *col_values: ColValueUpdateList) -> None:
        """UPDATE method
        """
        table = self.check_table(table)
        table.update(self.cursor, *col_values)
        self.commit()

    def select(self, table: Table, *cols: str, **where: Value) -> tuple:
        """SELECT method
        """
        table = self.check_table(table)
        return table.select(self.cursor, *cols, **where)

    def create(self, *tables: Table) -> None:
        """CREATE TABLE method
        """
        for table in tables:
            table.create(self.cursor)
        self.commit()
