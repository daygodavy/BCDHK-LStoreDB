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

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def create_table(self, name, num_columns, key):
        table = Table(name, num_columns, key)
        return table

    """
    # Stores the specified table with it's name as the key
    """

    def store_table(self, tobj):
        self.tables[tobj.name] = tobj
        print('Successfully stored {} table in Database'.format(self.tables[tobj.name].name))

    """
    # Deletes the specified table
    """

    def drop_table(self, name):
        tobj = self.tables.pop(name, 0)
        if tobj == 0:
            print('{} table does not exist'.format(name))
        else:
            del tobj
            print('{} table dropped'.format(name))


db = Database()
tbl = db.create_table('Users', 2, 0)
db.store_table(tbl)
db.drop_table('Users')
