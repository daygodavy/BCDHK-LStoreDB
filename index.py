from BTrees.OOBTree import OOBTree
from config import *


class Index:
    """
    Indexes the specified column of the specified table to speed up select queries
    This data structure is usually a B-Tree
    """

    def __init__(self):
        # the column this index operates on
        self.column_number = 0

        # the actual tree that does the work associated with indexing
        self.index = OOBTree()

        # the table this index operates on
        self.table = None

    def locate(self, value):
        """
        returns the location of all records with the given value

        :param value: int                     # the value of the key for which we need the RID
        """
        return self.index.get(value, False)

    def create_index(self, table, column_number):
        """
        Create index on specific column

        :param table: table object              # the table the index will operate on
        :param column_number: int               # the column number in the table the index will operate on

        :return: index object                   # the created index object
        """
        self.table = table
        self.column_number = column_number

        page = 0
        offset = 0

        # for each page range in the table
        for page_range in self.table.ranges:

            # for each record in the page range
            for i in range(page_range.num_of_records):

                # get the key for this record relative to the chosen column
                key = page_range.columns[self.column_number].pages[page].read(offset)

                # get the RID for this record
                RID = page_range.columns[RID_COLUMN].pages[page].read(offset)

                # increment the offset
                offset += 8

                # if reached the end of the page increment page and reset offset
                if offset == PAGE_SIZE:
                    page += 1
                    offset = 0

                self.add_index_item(key, RID)
        return self

    def add_index_item(self, key, RID):
        """
        Add to the index the key value and RID pair

        :param key: int                 # the key to store this RID at
        :param RID: int                 # the RID of the key
        """
        # if there are RID/s at this key get them, else false
        rids = self.index.get(key, False)

        # if there are rids at this key already append the new RID
        if rids:
            self.index.update({key: rids.append(RID)})
            return

        # otherwise add key and RID
        self.index.update({key: [RID]})

    def drop_index(self, table, column_number):
        """
        Drop index of specific column
        """
        self.table.indexes[self.column_number] = None
        self.index = None
        self.table = None
        self.column_number = 0
