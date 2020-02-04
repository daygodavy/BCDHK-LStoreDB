from page import Page
from time import time
from index import Index
from config import *
from BTrees.OOBTree import OOBTree
import struct

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

    def __init__(self):
        self.index = None
        self.base_pages = [Page()]
        self.tail_pages = [Page()]

    def add(self, col_val):
        if  not self.base_pages[-1].has_capacity():
            self.base_pages.append(Page())

        offset = self.base_pages[-1].write(col_val)

        #returning the page number and offset of new base record
        return len(self.base_pages) - 1, offset

    '''
    Adding a new data entry to the column.
    Using update means that there already existed a base record, so this
    new value will be added to a tail_page
    '''
    def update(self, value):
        #if the current page is full, create a new page
        if not self.tail_pages[-1].has_capacity():
            #appending new page to the tail_pages array of that column
            self.tail_pages.append(Page())

        offset = self.tail_pages[-1].write(value)
        return len(self.tail_pages) - 1, offset

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
        self.key = key + 4

        # number of columns in the table
        # the number is augmented by four for the book keeping columns
        self.num_columns = num_columns

        # An array of the columns where index is column number
        # Each element in the array represents a column in the table
        # columns 0-3 are bookkeeping
        # the rest of the columns are the user columns
        self.column_directory = self.make_columns()

        # page directory accepts RID and returns page# and offset of record
        self.page_directory = OOBTree()

        # number of records in the table
        self.num_records = 0

        # tail 2^64 - self.tail_record_tracker = number of tail records
        self.tail_record_tracker = (2 ** 64)
        pass

    # '''
    # Method which initialize the page directory as B+ tree
    # '''
    # def init_page_directory(self):
    #     return BPlusTree(order=10, key_size = 24)

    """
    Method which instantiates an empty column for each column in the table
    """
    def make_columns(self):
        # the data structure which holds the columns
        column_directory = []

        # create a base page and index for each column
        for i in range(0, self.num_columns+4):
            column_directory.append(Column())

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
    def add_record(self, *columnValues):
        # add the four bookkeeping columns to the beginning of columnValues
        rid = self.get_RID_value()

        #TODO: cannot just convert time to int
        col_vals = [0, rid, int(time()), 0]
        # columnValues.insert(INDIRECTION_COLUMN, None)
        # columnValues.insert(RID_COLUMN, rid)
        # columnValues.insert(TIMESTAMP_COLUMN, time())
        # columnValues.insert(SCHEMA_ENCODING_COLUMN, 0)

        for item in columnValues:
            col_vals.append(item)

        # if this primary key already exists within the table then reject the addition of this record
        index = self.column_directory[self.key].index
        if index and index.locate(col_vals[self.key]):
            return "ERROR: this primary key already exists within the table"

        # otherwise for each column in the table add the value
        else:
            page_num = offset = -1
            for i in range(0, len(self.column_directory)):
                page_num, offset = self.column_directory[i].add(col_vals[i])
                if self.column_directory[i].index:
                    column.index.add_index(col_vals[i])
            self.page_directory.update({rid : [page_num, offset]})

    """
    A method for deleting lazy deletion of a base record
    :param: key: int                # a primary key value used to find the record
    """
    def delete_record(self, key):
        # find the RID by the primary key value
        rid = self.column_directory[self.key].index.locate(key)
        if rid is None:
            return "ERROR: key does not exist"

        # find the base page number and offset in byte array for the relevant record
        page_num, offset = self.page_directory.get(rid)

        # for every column set value of record to 0
        for column in self.column_directory:
            column.base_pages[page_num].data[offset: offset + 8] = 0

        # decrement num of records
        self.num_records -= 1

    """
    Add an update to a record to the tail pages
    :param key: int        # the primary key value of the record we are adding an update for
    :param columns:         # the column values that are being updated
    """
    def update_record(self, key, columns):
        # get LID value
        LID = self.get_LID_value()

        # get the base record
        base_record, tail_record = self.read_record(key=key, query_columns=[1] * (len(columns) + 4))
        col_vals = [0]*(len(columns) + 4)
        col_vals[TIMESTAMP_COLUMN] = int(time())
        col_vals[RID_COLUMN] = LID
        col_vals[INDIRECTION_COLUMN] = base_record.rid

        schema_encoding = ''
        for i in range (0, len(columns)):
            col_vals[i+4] = columns[i]
            # if value in column is not 'None' add 1
            if columns[i]:
                schema_encoding = schema_encoding + '1'
            # else add 0
            else:
                schema_encoding = schema_encoding + '0'

        schema_encoding = int(schema_encoding, 2)
        col_vals[SCHEMA_ENCODING_COLUMN] = schema_encoding
        #update indirection column of base record
        self.update_schema_indirection(key, schema_encoding, LID)

        #find the latest tail record
        if tail_record: #latest is a tail record
            # for every column we want collect it in our column_values list
            for i in range(4, len(columns) + 4):
                if not col_vals[i]:
                    col_vals[i] = struct.unpack(ENCODING, tail_record.columns[i])[0]
        else: #latest record is base record
            for i in range(4, len(columns) + 4):
                if not col_vals[i]:
                    col_vals[i] = struct.unpack(ENCODING, base_record.columns[i])[0]

        page_num = offset = -1
        for i in range(0, len(col_vals)):
            page_num, offset = self.column_directory[i].update(col_vals[i])
        self.page_directory.update({LID : [page_num, offset]})
        #self.column_directory[INDIRECTION_COLUMN].update([page_num, offset])


    """
    A method which updates the schema and indirection columns of a base record when a tail record is added
    :param key: int                                # the primary key value of the record we are adding an update for
    :param schema_encoding: int                    # a value representing which columns have had changes to them
    :param indirection_value: int                  # the LID of the tail newest tail record for this base record
    """
    def update_schema_indirection(self, key, schema_encoding, indirection_value):
        # get RID from index
        # FIXME: how to account for which RID is used.. for now just subscript [0]
        RID = self.column_directory[self.key].index.locate(value=key)[0]
        # get page number and offset of record within columns
        [page_number, offset] = self.page_directory.get(RID)

        # update the values in the base record
        self.column_directory[INDIRECTION_COLUMN].base_pages[page_number].data[offset: offset + 8] = struct.pack(ENCODING, indirection_value)
        self.column_directory[SCHEMA_ENCODING_COLUMN].base_pages[page_number].data[offset: offset + 8] = struct.pack(ENCODING, schema_encoding)

    """
    A method which returns a the record with the given primary key
    :param key: int                    # the primary key for the wanted record
    :param query_columns: []           # a list of integers representing the columns wanted
    """
    def read_record(self, key, query_columns):
        # list to hold the wanted column values
        column_values = []
        base_record = None
        tail_record = None
        # if there is no index for this column yet, create it
        if not self.column_directory[self.key].index:
            self.column_directory[self.key].index = Index(table=self, column_number=self.key)
            self.column_directory[self.key].index.create_index()

        # find the RID by the primary key value
        rid = self.column_directory[self.key].index.locate(key)[0]

        # if key was located
        if rid != 0:
            # find the base page number and offset in the byte array for the relevant record
            base_page_num, base_offset = self.page_directory.get(rid)
            lid = struct.unpack(ENCODING, self.column_directory[INDIRECTION_COLUMN].base_pages[base_page_num].read(base_offset))[0]
            if lid != 0:
                tail_page_num, tail_offset = self.page_directory.get(lid)
                for i in range(len(self.column_directory)):
                    if query_columns[i]:
                        column_values.append(self.column_directory[i].tail_pages[tail_page_num].read(tail_offset))
                tail_record = Record(rid=column_values[RID_COLUMN], key=key, columns=column_values)
            # for every column we want collect it in our column_values list
            column_values = []
            for i in range(len(self.column_directory)):
                if query_columns[i]:
                    column_values.append(self.column_directory[i].base_pages[base_page_num].read(base_offset))
            base_record = Record(rid=rid, key=key, columns=column_values)

        return base_record, tail_record

    """
    A method which returns a the record with the given primary key
    :param start_range: int            # start of key range
    :param end_range: int              # end of key range (inclusive)
    :param aggr_column_index: int      # index of column to aggregate
    """
    def sum_records(self, start_range, end_range, aggr_column_index):
        sum_col = 0
        if end_range - start_range <= 0:
            return "ERROR: invalid range"

        # go through keys within range
        for key_in_range in range(start_range, end_range + 1):
            # read_record to get back column to aggregate
            aggr_record = self.read_record(key_in_range, [aggr_column_index])
            # if key exists, sum
            if aggr_record.rid is None:
                sum_col += aggr_record.columns[0]
        return sum_col

    def __merge(self):
        pass
