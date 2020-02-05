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


def get_schema_encoding(columns):
    """
    A method which creates the schema encoding for the given record
    :param columns: list            # the record in a list format
    """
    schema_encoding = ''
    for item in columns:
        # if value in column is not 'None' add 1
        if item:
            schema_encoding = schema_encoding + '1'
        # else add 0
        else:
            schema_encoding = schema_encoding + '0'
    return int(schema_encoding, 2)


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
        if not self.base_pages[-1].has_capacity():
            self.base_pages.append(Page())

        offset = self.base_pages[-1].write(col_val)

        # returning the page number and offset of new base record
        return len(self.base_pages) - 1, offset

    def update(self, value):
        """
        A method which updates an existing record in a column

        :param value: int           # the column value being added
        :return: int, int           # the page number and offset of the value added to the column
        """
        # if the current page is full, create a new page
        if not self.tail_pages[-1].has_capacity():
            # appending new page to the tail_pages array of that column
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

    def make_columns(self):
        """
        Method which instantiates an empty column for each column in the table

        :return: list           # a list of column objects that define the table's columns
        """
        # the data structure which holds the columns
        column_directory = []

        # create a base page and index for each column
        for i in range(0, self.num_columns + 4):
            column_directory.append(Column())

        return column_directory

    def get_RID_value(self):
        """
        A method which manages RIDs for the table

        :return: int            # an RID value
        """
        self.num_records += 1
        return self.num_records

    def get_LID_value(self):
        """
        A method which manages the LIDs for the table

        :return: int            # an LID value
        """
        self.tail_record_tracker -= 1
        return self.tail_record_tracker

    def add_record(self, *columnValues):
        """
        A method which adds a record to the table

        :param columnValues: tuple          # a tuple of the records user values
        :return: string                     # an error message
        """
        # create the meta column values for this record
        rid = self.get_RID_value()
        col_vals = [0, rid, int(time() * 1000000), 0]

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
                    self.column_directory[i].index.add_index(col_vals[i])
            self.page_directory.update({rid: [page_num, offset]})
        return "Success"

    def delete_record(self, key):
        """
        A method for the lazy deletion of a record

        :param key: int             # the primary key value of the record to be deleted
        :return: string             # error message
        """
        # find the RID by the key value
        rids = self.column_directory[self.key].index.locate(key)

        for rid in rids:
            if rid is None:
                return "ERROR: key does not exist"

            # find the base page number and offset in byte array for the relevant record
            page_num, offset = self.page_directory.get(rid)

            # for every column set value of record to 0
            for column in self.column_directory:
                column.base_pages[page_num].data[offset: offset + 8] = struct.pack(ENCODING, 0)

            # decrement num of records
            self.num_records -= 1

    def update_record(self, key, columns):
        """
        Add an update to a record to the tail pages

        :param key: int         # the primary key value of the record we are adding an update for
        :param columns:         # the column values that are being updated
        """
        # get LID value
        LID = self.get_LID_value()

        # get the base record
        # base_record, tail_record = self.read_record(key=key, query_columns=[1] * (len(columns) + 4))
        records = self.read_record(key=key, query_columns=[1] * (len(columns) + 4))

        for record in records:
            # create the full record as a list
            col_vals = [record.rid, LID, int(time() * 1000000), 0]

            for item in columns:
                col_vals.append(item)

            # get schema encoding for updated values
            schema_encoding = get_schema_encoding(columns)

            col_vals[SCHEMA_ENCODING_COLUMN] = schema_encoding

            # update indirection column of base record
            self.update_schema_indirection(key, schema_encoding, LID)

            # if the latest record is a tail record
            if record.rid > self.num_records:
                tail_page_num, tail_offset = self.page_directory.get(record.rid)

                # update the indirection value in the tail record
                self.column_directory[INDIRECTION_COLUMN].tail_pages[tail_page_num].data[
                tail_offset: tail_offset + 8] = struct.pack(ENCODING, LID)

                # # for every column, if we have a new value save it, otherwise use old value
                # for i in range(4, len(columns) + 4):
                #     if col_vals[i] is None:
                #         col_vals[i] = struct.unpack(ENCODING, tail_record.columns[i])[0]

            # otherwise the latest record is the base record

            # for every column, if we have a new value save it, otherwise use old value
            for i in range(4, len(columns) + 4):
                if col_vals[i] is None:
                    col_vals[i] = struct.unpack(ENCODING, record.columns[i])[0]

            # update page directory
            page_num = offset = -1
            for i in range(0, len(col_vals)):
                page_num, offset = self.column_directory[i].update(col_vals[i])
            self.page_directory.update({LID: [page_num, offset]})

    def update_schema_indirection(self, key, schema_encoding, indirection_value):
        """
        A method which updates the schema and indirection columns of a base record when a tail record is added

        :param key: int                                # the primary key value of the record we are adding an update for
        :param schema_encoding: int                    # a value representing which columns have had changes to them
        :param indirection_value: int                  # the LID of the tail newest tail record for this base record
        """
        # get a list of rids for all records being updated
        rids = self.column_directory[self.key].index.locate(value=key)

        # for each RID in rids
        for RID in rids:
            # get page number and offset of record within columns
            page_number, offset = self.page_directory.get(RID)

            # update the indirection value in the base record
            self.column_directory[INDIRECTION_COLUMN].base_pages[page_number].data[offset: offset + 8] = struct.pack(
                ENCODING, indirection_value)

            # get current schema encoding value
            current_schema_value = struct.unpack(ENCODING, self.column_directory[SCHEMA_ENCODING_COLUMN].base_pages[
                                                               page_number].data[offset: offset + 8])[0]

            # OR current schema encoding value and new schema encoding value
            schema_encoding |= current_schema_value

            # store new schema encoding value
            self.column_directory[SCHEMA_ENCODING_COLUMN].base_pages[page_number].data[
            offset: offset + 8] = struct.pack(ENCODING, schema_encoding)

    def read_record(self, key, query_columns):
        """
        A method which returns the record with the given primary key

        :param key: int                    # the primary key for the wanted record
        :param query_columns: []           # a list of integers representing the columns wanted
        """
        # list to hold the wanted column values
        column_values = []
        # base_record = []
        # tail_record = []
        records = []

        # if there is no index for this column yet, create it
        if not self.column_directory[self.key].index:
            self.column_directory[self.key].index = Index(table=self, column_number=self.key)
            self.column_directory[self.key].index.create_index()

        # find the RID by the primary key value
        rids = self.column_directory[self.key].index.locate(value=key)

        # if there wasn't a match for the primary key return empty-handed
        if rids is None:
            return records

        # for all RID values returned from the index
        for RID in rids:
            # find the base page number and offset in the byte array for the relevant record
            base_page_num, base_offset = self.page_directory.get(RID)

            # check for tail records
            lid = struct.unpack(ENCODING, self.column_directory[INDIRECTION_COLUMN].base_pages[base_page_num].read(base_offset))[0]
            if lid != 0:

                # if tail record/s find the page number and offset
                tail_page_num, tail_offset = self.page_directory.get(lid)

                # collect those values and create a record from it
                for i in range(len(self.column_directory)):
                    if query_columns[i]:
                        column_values.append(self.column_directory[i].tail_pages[tail_page_num].read(tail_offset))
                records.append(Record(rid=lid, key=key, columns=column_values))
            else:
                # for every column we want collect it in our column_values list
                column_values = []
                for i in range(len(self.column_directory)):
                    if query_columns[i]:
                        column_values.append(self.column_directory[i].base_pages[base_page_num].read(base_offset))
                records.append(Record(rid=RID, key=key, columns=column_values))

        return records

    def sum_records(self, start_range, end_range, aggr_column_index):
        """
        A method which returns a the record with the given primary key

        :param start_range: int            # start of key range
        :param end_range: int              # end of key range (inclusive)
        :param aggr_column_index: int      # index of column to aggregate
        """
        # instantiate sum
        sum_col = 0

        # check range for accuracy
        if end_range - start_range <= 0:
            return "ERROR: invalid range"

        # create a query_columns object
        query_columns = [0] * (self.num_columns + 4)
        query_columns[aggr_column_index + 4] = 1

        # go through keys within range
        for key_in_range in range(start_range, end_range + 1):
            # read_record to get back column to aggregate
            records = self.read_record(key_in_range, query_columns)
            for record in records:
                # if tail_record:
                #     aggr_record = tail_record
                # # if key exists, sum
                # if aggr_record:
                sum_col += struct.unpack(ENCODING, record.columns[0])[0]
        return sum_col

    def __merge(self):
        pass
