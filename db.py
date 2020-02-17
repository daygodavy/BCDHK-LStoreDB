from table import Table
from config import *


class Database:

    def __init__(self):
        self.tables = {}
        pass

    def open(self):
        pass

    def close(self):
        pass


    def create_table(self, name, key, num_columns):
        """
        Creates a new table

        :param name: string         # Table name
        :param num_columns: int     # Number of Columns: all columns are integer
        :param key: int             # Index of table key in columns

        :return: table object       # the table object being created
        """
        table = Table(name, num_columns, key + NUMBER_OF_META_COLUMNS)
        return table

    def drop_table(self, name):
        """
        Deletes the specified table

        :param name: string         # The name of the table
        """
        pass
      