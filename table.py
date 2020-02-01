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
        self.base_pages = [Page()]
        self.tail_pages = [Page()]


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

        # tail 2^64 - self.tail_record_tracker = number of tail records
        self.tail_record_tracker = (2 ** 64)
        pass

    """
    Method which instantiates an empty column for each column in the table
    """
    def make_columns(self):
        # the data structure which holds the columns
        column_directory = {}

        # create a base page and index for each column
        for i in range(0, self.num_columns - 1):
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
    A method which manages the LIDS of the table
    """
    def get_LID_value(self):
        self.tail_record_tracker -= 1
        return self.tail_record_tracker

    """
    Method which adds a record to the table
    
    :param key: int         #the index of the primary key column
    :param columns: []      #the column values 
    """
    def add_record(self, key, columnValues):
        # add the four bookkeeping columns to the beginning of columnValues
        columnValues.insert(INDIRECTION_COLUMN, None)
        columnValues.insert(RID_COLUMN, self.get_RID_value())
        columnValues.insert(TIMESTAMP_COLUMN, time())
        columnValues.insert(SCHEMA_ENCODING_COLUMN, 0)

        # if this primary key already exists within the table then reject the addition of this record
        if self.column_directory[key].index.locate(columnValues[key]):
            return "ERROR: this primary key already exists within the table"

        # otherwise for each column in the table add the value
        else:
            for i, column in self.column_directory:
                column.add(columnValues[i])

    def delete_record(self):
        pass

    """
    Add an update to a record to the tail pages
    :param: key: int        # the primary key value of the record we are adding an update for
    :param columns:         # the column values that are being updated
    """
    def update_record(self, key, columns):
        # get the base record
        record = self.read_record(key=key, query_columns=[0, 1, 2, 3])

        # get the schema encoding
        schema_encoding = ''
        for i in columns:
            # if value in column is 'None' add 0
            if columns[i]:
                schema_encoding = schema_encoding + '0'

            # else add 1
            else:
                schema_encoding = schema_encoding + '1'

        schema_encoding = int(schema_encoding, 2)

        # find value for indirection column
        if record.columns[INDIRECTION_COLUMN]:
            indirection_value = record.columns[INDIRECTION_COLUMN]
        else:
            indirection_value = None

        # get LID value
        LID = self.get_LID_value()

        # update base record
        self.update_schema_indirection(key=key, schema_encoding=schema_encoding, indirection_value=LID)

        # add the four bookkeeping columns to the beginning of columns
        columns.insert(INDIRECTION_COLUMN, indirection_value)
        columns.insert(RID_COLUMN, LID)
        columns.insert(TIMESTAMP_COLUMN, time())
        columns.insert(SCHEMA_ENCODING_COLUMN, schema_encoding)

        # add the tail record column by column
        for i, column in self.column_directory:
            column.update(columns[i])

    """
    A method which updates the schema and indirection columns of a base record when a tail record is added
    :param: key: int                                # the primary key value of the record we are adding an update for 
    :param: schema_encoding: int                    # a value representing which columns have had changes to them
    :param: indirection_value: int                  # the LID of the tail newest tail record for this base record
    """
    def update_schema_indirection(self, key, schema_encoding, indirection_value):
        # get RID from index
        RID = self.column_directory[self.key].index.locate(value=key)

        # get page number and offset of record within columns
        [page_number, offset] = self.page_directory[RID]

        # update the values in the base record
        self.column_directory[INDIRECTION_COLUMN].base_pages[page_number][offset: offset + 8] = indirection_value
        self.column_directory[SCHEMA_ENCODING_COLUMN].base_pages[page_number][offset: offset + 8] = schema_encoding
        pass

    def read_record(self, key, query_columns):
        return Record(rid=None, key=key, columns=query_columns)

    def __merge(self):
        pass
