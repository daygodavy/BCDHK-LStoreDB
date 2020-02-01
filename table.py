from page import Page
from time import time
from index import Index

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Column:

    def __init__(self, table):
        self.index = Index(table=table)
        self.base_pages = [ Page() ]
        self.tail_pages = [ Page() ]

class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def __init__(self, name, num_columns, key):
        # table name
        self.name = name

        # column number of primary key
        self.key = key

        # number of columns in the table
        # the number is augmented by four for the book keeping columns
        self.num_columns = num_columns + 4

        # A dictionary of the columns
        # key: is column number
        # value: first base page, index for column
        # columns 0-3 are bookkeeping
        # the rest of the columns are the user columns
        self.column_directory = self.make_columns()

        # page directory accepts RID and returns page# and offset of record
        self.page_directory = {}

        # number of records in the table
        self.num_records = 0
        pass

    """
    Method which instantiates an empty column for each column in the table
    """
    def make_columns(self):
        # the data structure which holds the columns
        column_directory = {}

        # create a base page and index for each column
        for i in range(0, self.num_columns-1):
            column_directory[i] = Column(table=self)
            column_directory[i].index.create_index(table=self, column_number=i)

        return column_directory


    """
    A method which manages the RIDs of the table
    """
    def get_RID_value(self):
        self.num_records += 1
        return self.num_records

    """
    Method which adds a record to the table
    
    :param key: int         #the index of the primary key column
    :param columns: []      #the column values 
    """
    def add_record(self, key, columnValues):
        # add the four bookkeeping columns to the beginning of columnValues
        columnValues.insert(0, None)
        columnValues.insert(1, self.get_RID_value())
        columnValues.insert(2, time())
        columnValues.insert(3, 0)

        # if this primary key already exists within the table then reject the addition of this record
        if self.column_directory[key].index.locate(columnValues[key]):
            return "ERROR: this primary key already exists within the table"

        # otherwise for each column in the table add the value
        else:
            for i, column in self.column_directory:
                column.add(columnValues[i])

    def delete_record(self):
        pass

    def update_record(self):
        pass

    def read_record(self):
        pass

    def __merge(self):
        pass

