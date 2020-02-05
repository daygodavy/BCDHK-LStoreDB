from config import *
from BTrees.OOBTree import OOBTree
import struct


class Index:
    """
    # Indexes the specified column of the specified table to speed up select queries
    # This data structure is usually a B-Tree
    """

    def __init__(self, table, column_number):
        self.column_number = column_number
        self.table = table
        self.btree = OOBTree()
        self.rid = 0

    def locate(self, value):
        """
        # returns the location of all records with the given value
        # :param value: int         # the value to locate RIDs by
        """
        if self.btree.has_key(value) > 0:
            return self.btree.get(value)
        return None

    def create_index(self):
        """
        # Create index on specific column
        """
        # populate btree for this column
        for page in self.table.column_directory[self.column_number].base_pages:
            # FIXME: AFTER MERGING; this won't work anymore due to assumption of RID ~ idx
            for i in range(page.num_records):
                # get starting index of the relevant entry
                start_idx = i * 8

                # update the rid value for the next entry
                self.rid = self.rid + 1

                # get the entry value itself
                entry = struct.unpack(ENCODING, page.read(start_idx))[0]

                # check if key exists in btree
                if self.btree.has_key(entry) > 0:

                    # append new rid to the existing entry (key)
                    rids = self.btree.get(entry)
                    rids.append(self.rid)
                    self.btree.update({entry: rids})

                else:
                    # create new node { entry : rid }
                    self.btree.update({entry: [self.rid]})

    def drop_index(self):
        """
        A method which deletes the index
        """
        self.table.index = None

    def add_index(self, value):
        """
        A method which adds a value to the index
        :param value: int       # the value to be added to the table/index
        """
        # convert value to byte value comparison and addition to byte array
        # byte_val = struct.pack(ENCODING, value)
        self.rid = self.rid + 1

        # check if key exists in btree
        if self.btree.has_key(value):

            # append new rid to the existing entry (key)
            rids = self.btree.get(value)
            rids.append(self.rid)
            self.btree.update({value: rids})

        else:
            # create new node { entry : rid }
            self.btree.update({value: [self.rid]})
