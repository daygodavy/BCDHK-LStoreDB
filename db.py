import os
import glob
import pickle
import shutil

from config import *
from table import Table
from bufferpool import *

class Database:

    def __init__(self):
        # a dictionary of the tables in the database keyed by name
        self.tables = {}

        # the directory of the database
        self.directory_name = "~/ECS165/"

        # # initialize buffer pool
        # self.bp = Bufferpool()

    def open(self, directory_name):
        """
        Start the database from the given directory

        :param directory_name: str      # a string representing the directory of the database
        """

        # check if directory exists, if not then create it
        self.directory_name = os.path.expanduser(directory_name) + '/'
        if not os.path.isdir(self.directory_name):
            os.mkdir(self.directory_name)

        # for each table in the database folder
        for _, name in enumerate(glob.glob(self.directory_name + "/*", recursive=True)):
            print("name : ", name)
            table_file = name + '/table'
            table = pickle.load(open(table_file, "rb"))
            self.tables[table.name] = table

        print("database opened")

    def close(self):
        """
        Close the db and save the table
        """
        for name, table in self.tables.items():
            table.save_table(self.directory_name)

    def create_table(self, name, num_columns, key):
        """
        Creates a new table

        :param name: string         # Table name
        :param num_columns: int     # Number of Columns: all columns are integer
        :param key: int             # Index of table key in columns

        :return: table object       # the table object being created
        """
        table = Table(name, num_columns, key)
        self.tables[name] = table
        # self.bp.table = table
        return table

    def drop_table(self, name):
        """
        Deletes the specified table

        :param name: string         # The name of the table
        """
        table_object = self.tables.pop(name, 0)
        if table_object == 0:
            print(name + ' does not exist')
        else:
            shutil.rmtree(self.directory_name + name)
            print(name + " has been dropped")
