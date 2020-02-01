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
    Method which adds a record to the table

    :param key: int         #the index of the primary key column
    :param columns: []      #the column values
    """

    def add_record(self, key, columnValues):
        # add the four bookkeeping columns to the beginning of columnValues
        columnValues.insert(INDIRECTION_COLUMN, None)
        columnValues.insert(RID_COLUMN, rid)
        columnValues.insert(TIMESTAMP_COLUMN, time())
        columnValues.insert(SCHEMA_ENCODING_COLUMN, 0)

        # if this primary key already exists within the table then reject the addition of this record
        if self.column_directory[key].index.locate(columnValues[key]):
            return "ERROR: this primary key already exists within the table"

        # otherwise for each column in the table add the value
        else:
            for i, column in self.column_directory:
                column.add(columnValues[i])

    def delete_record(self, key):
        rid = self.index.locate(key)
        page_num, offset = self.page_directory[rid]
        for i in range(0, self.num_columns-1):
            column_directory[i].base_pages[page_num].data[offset: offset + 8] = 0


        # byte_key = struct.pack(">q", key)
        # key_base_pages = self.column_directory[self.key].base_pages
        # i = 0
        # j = 0
        # match_found = False
        # match_idx = 0
        # for pages in key_base_pages:
        #     if j == len(key_base_pages):
        #         # key doesn't exist
        #
        #         break
        #     while !match_found:
        #         curr_byte = pages.data[i + 8]
        #         if byte_key == curr_byte:
        #             # match found
        #             # delete primary key from this page
        #             match_found = True
        #             match_idx = i
        #         else:
        #             i = i + 8
        #         if i == 4096:
        #             break
        #     j = j + 1
        #
        # pages.data[match_idx : match_idx + 8] = 0
        #
        # rid_base_pages = self.column_directory[RID_COLUMN].base_pages




    def update_record(self, key, columns):
        # TODO: find the record so I can set the indirection column value properly
        record = self.read_record(key=key, query_columns=[0, 1, 2, 3])
        # TODO: set schema encoding
        schema_encoding = None
        # TODO: get timestamp
        # TODO: LID?
        # TODO: set schema encoding of base record
        # TODO: set indirection in base record

        # add the four bookkeeping columns to the beginning of columns
        columns.insert(INDIRECTION_COLUMN, None)
        columns.insert(RID_COLUMN, self.get_RID_value())
        columns.insert(TIMESTAMP_COLUMN, time())
        columns.insert(SCHEMA_ENCODING_COLUMN, 0)

        for i, column in self.column_directory:
            column.update(columns[i])

    def read_record(self, key, query_columns):
        list = []
        rid = self.index.locate(key)
        page_num, offset = self.page_directory[rid]
        for i in range(0, self.num_columns-1):
            for j in query_columns:
                if column_directory[i].index == j:
                    list.append(column_directory[i].base_pages[page_num].data[offset: offset + 8])
        return list

    def __merge(self):
        pass
