from table import Table
import logging


class Database:

    def __init__(self):
        self.tables = {}
        pass

    def open(self):
        pass

    def close(self):
        pass

    def create_table(self, name, num_columns, key):
        """
        # Creates a new table
        :param name: string         #Table name
        :param num_columns: int     #Number of Columns: all columns are integer
        :param key: int             #Index of table key in columns
        """
        table = Table(name, num_columns, key)
        return table

    def store_table(self, tobj):
        """
        # Stores the specified table with it's name as the key
        # :param tobj: table object?
        """
        self.tables[tobj.name] = tobj
        print('Successfully stored {} table in Database'.format(self.tables[tobj.name].name))

    def drop_table(self, name):
        """
        # Deletes the specified table
        # :param name: string       # the name of the table to be dropped
        """
        tobj = self.tables.pop(name, 0)
        if tobj == 0:
            print('{} table does not exist'.format(name))
        else:
            del tobj
            print('{} table dropped'.format(name))
