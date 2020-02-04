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
        #get the most recent page
        current_page = self.tail_pages[-1]
        #if the current page is full, create a new page
        if not current_page.has_capacity():
            current_page = Page()
            #appending new page to the tail_pages array of that column
            self.tail_pages.append(current_page)

        current_page.write(value)
        print("Record updated")

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

        # An array of the columns where index is column number
        # Each element in the array represents a column in the table
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

     '''
    Method which initialize the page directory as B+ tree
    '''
    def init_page_directory(self):
        return BPlusTree(order=10, key_size = 24)

    """
    Method which instantiates an empty column for each column in the table
    """
    def make_columns(self):
        # the data structure which holds the columns
        column_directory = []

        # create a base page and index for each column
        for i in range(0, self.num_columns - 1):
            column_directory[i] = Column()

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
        rid = self.get_RID_value()
        columnValues.insert(INDIRECTION_COLUMN, None)
        columnValues.insert(RID_COLUMN, rid)
        columnValues.insert(TIMESTAMP_COLUMN, time())
        columnValues.insert(SCHEMA_ENCODING_COLUMN, 0)

        # if this primary key already exists within the table then reject the addition of this record
        if self.column_directory[self.key].index.locate(columnValues[key]):
            return "ERROR: this primary key already exists within the table"

        # otherwise for each column in the table add the value
        else:
            for i in range(0, len(self.column_directory)):
                page_num, offset = self.column_directory[i].add(columnValues[i])
                if column.index:
                    column.index.add_index(columnValues[i])
            self.page_directory.insert(rid, [page_num, offset])

        # increment num of records
        self.num_records += 1

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
        page_num, offset = self.page_directory[rid]

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
        # get the base record
        record = self.read_record(key=key, query_columns=[1] * (len(columns) + 4))

        # get the schema encoding
        schema_encoding = ''
        for i in columns:
            # if value in column is not 'None' add 1
            if columns[i]:
                schema_encoding = schema_encoding + '1'
            # else add 0
            else:
                schema_encoding = schema_encoding + '0'

        schema_encoding = int(schema_encoding, 2)

        # Find the RID/LID of the latest record
        latest_rec_vals = []
        # for indirection column
        id = record.rid
        if record.columns[INDIRECTION_COLUMN]: #latest is a tail record
            id = record.columns[INDIRECTION_COLUMN]
            page_num, offset = self.page_directory[id]
            # for every column we want collect it in our column_values list
            for i, column in self.column_directory:
                latest_rec_vals.append(column.base_pages[page_num].data[offset: offset + 8])
        else: #latest record is base record
            latest_rec_vals = record.columns[4:]

        # get LID value
        LID = self.get_LID_value()

        # update base record
        self.update_schema_indirection(key=key, schema_encoding=schema_encoding, indirection_value=LID)

        # add the four bookkeeping columns to the beginning of columns
        columns.insert(INDIRECTION_COLUMN, id)
        columns.insert(RID_COLUMN, LID)
        columns.insert(TIMESTAMP_COLUMN, time())
        columns.insert(SCHEMA_ENCODING_COLUMN, schema_encoding)

        # add the tail record column by column
        for i in range(0, len(self.column_directory)):
            if columns[i]:
                self.column_directory[i].update(columns[i])
            else:
                self.column_directory[i].update(latest_rec_vals[i])

    """
    A method which updates the schema and indirection columns of a base record when a tail record is added
    :param key: int                                # the primary key value of the record we are adding an update for
    :param schema_encoding: int                    # a value representing which columns have had changes to them
    :param indirection_value: int                  # the LID of the tail newest tail record for this base record
    """
    def update_schema_indirection(self, key, schema_encoding, indirection_value):
        # get RID from index
        RID = self.column_directory[self.key].index.locate(value=key)

        # get page number and offset of record within columns
        [page_number, offset] = self.page_directory[RID]

        # update the values in the base record
        self.column_directory[INDIRECTION_COLUMN].base_pages[page_number][offset: offset + 8] = indirection_value
        self.column_directory[SCHEMA_ENCODING_COLUMN].base_pages[page_number][offset: offset + 8] = schema_encoding

    """
    A method which returns a the record with the given primary key
    :param key: int                    # the primary key for the wanted record
    :param query_columns: []           # a list of integers representing the columns wanted
    """
    def read_record(self, key, query_columns):
        # list to hold the wanted column values
        column_values = []

        # if there is no index for this column yet, create it
        if not self.column_directory[self.key].index:
            self.column_directory[self.key].index = Index(table=self, column_number=self.key)
            self.column_directory[self.key].index.create_index()

        # find the RID by the primary key value
        rid = self.column_directory[self.key].index.locate(key)

        # if key was located
        if rid is not None:
            # find the base page number and offset in the byte array for the relevant record
            page_num, offset = self.page_directory[rid]

            # for every column we want collect it in our column_values list
            for i, column in self.column_directory:
                if query_columns[i]:
                    column_values.append(column.base_pages[page_num].data[offset: offset + 8])

        # for every column we want collect it in our column_values list
        for i, column in self.column_directory:
            if query_columns[i]:
                column_values.append(column.base_pages[page_num].data[offset: offset + 8])

        return Record(rid=rid, key=key, columns=column_values)

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
