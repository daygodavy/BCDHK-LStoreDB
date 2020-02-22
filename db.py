import os

from table import Table
from config import *

class Database:

    def __init__(self):
        self.tables = {}

    def open(self, directory_name):
        # path = directory_name + '/tables'
        # print("new path: ", path)
        directory_list = os.listdir(directory_name)
        print("directory list: ", directory_list)

        # for _, name in enumerate(directory_list):
        #     newpath = path + '/' + name
        #     self.create_table(newpath)

    def close(self):
        pass

    def get_table(self, name):
        """
        # Returns table with the passed name
        """
        return self.tables(name)

    def create_table(self, name, num_columns, key):
        """
        Creates a new table

        :param name: string         # Table name
        :param num_columns: int     # Number of Columns: all columns are integer
        :param key: int             # Index of table key in columns

        :return: table object       # the table object being created
        """
        table = Table(name, num_columns, key + NUMBER_OF_META_COLUMNS)
        self.tables[table.name] = table

    def drop_table(self, name):
        """
        Deletes the specified table

        :param name: string         # The name of the table
        """
        tobj = self.tables.pop(name, 0)
        if tobj == 0:
            print('{} table does not exist'.format(name))
        else:
            del tobj
            print('{} table dropped'.format(name))


db = Database()
db.open('~/ECS165')
# tbl = db.create_table('Users', 2, 0)
# db.drop_table('Users')
# db.get_table('test.txt')