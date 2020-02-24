import pickle
import os

import sys
from range import PageRange
from time import time
from BTrees.OOBTree import OOBTree
from config import *
from index import Index
from bufferpool import bp


class Table:

    def __init__(self, name, num_columns, key):
        """
        The actual table holding the records

        :param name: string         # Table name
        :param num_columns: int     # Number of user Columns: all columns are integer
        :param key: int             # Index of primary key column
        """

        # name of the table
        self.name = name

        # column number of the primary key column
        self.prim_key_col_num = key + NUMBER_OF_META_COLUMNS

        # the number user columns
        self.num_columns = num_columns

        # the total number of columns
        self.number_of_columns = self.num_columns + NUMBER_OF_META_COLUMNS

        # accepts a record's RID and returns page_range_index, page_number and offset
        self.page_directory = OOBTree()

        # a list containing the page ranges for the table
        self.ranges = [PageRange(self.number_of_columns, key, 0, os.path.expanduser("~/ECS165/" + self.name))]

        # the number of records in the tale
        self.num_records = 0

        # highest used rid number
        self.rid = 0

        # lowest used lid number
        self.lid = LID_MAX

        # a list of indexes for the table
        self.indexes = self.make_indexes(self.number_of_columns, self.prim_key_col_num, table=self)

        # name of directory that table is in
        self.directory_name = "~/ECS165/"

        # stack of deleted, available RIDs
        self.rid_stack = []

    def get_rid_value(self):
        """
        Manage the RIDs of the table

        :return: int                        # an integer value to represent the next RID
        """
        self.rid += 1
        return self.rid

    def get_lid_value(self):
        """
        Manage the LIDs of the table

        :return: int                        # an integer value to represent the next LID
        """
        self.lid -= 1
        return self.lid

    def add_record(self, columns):
        """
        Add a record to the table

        :param columns: []                  # the column values of the record
        """
        # TODO: Check to see if primary key already exists in table
        #   TA said this error check isn't necessary

        # check if can grab old, deleted rid from rid_stack
        if len(self.rid_stack) > 0:
            RID, page_range, page_num, offset = self.rid_stack.pop(0)
            columns = [0, RID, int(time() * 1000000), 0, 0] + columns
            page_range.update_base_record(page_num, offset, columns)
        else:
            # get new rid and add meta-data columns
            RID = self.get_rid_value()
            columns = [0, RID, int(time() * 1000000), 0, 0] + columns

            # get a hold of the last page range
            page_range = self.ranges[-1]

            # if it is full
            if not page_range.has_capacity:
                # create a new one and append it
                page_range = PageRange(num_of_columns=self.number_of_columns, primary_key_column=self.prim_key_col_num)
                self.ranges.append(page_range)

            # write record to page range and return page number and offset of record
            page_num, offset = page_range.add_base_record(columns)

        # increment the number of records
        self.num_records += 1

        # update page directory
        self.page_directory.update({RID: [page_range, page_num, offset]})

        # update primary key index
        self.indexes[self.prim_key_col_num].add_index_item(columns[self.prim_key_col_num], RID)

    def read_record(self, key, column_number, query_columns):
        """
        Read the record from the table

        :param key: int                     # the value to select records based on
        :param column_number: int           # the column number to match keys on
        :param query_columns: []            # a list of Nones and 1s defining which column values to return

        :return: []                         # a list of records matching the arguments
        """
        # a list to hold the matching records
        records = []

        # if no index exists for this column, create it
        index = self.indexes[column_number]
        if not index:
            self.indexes[column_number] = index = Index().create_index(table=self, column_number=column_number)

        # get the matching rids
        rids = index.locate(value=key)

        # if there are no matching rids return an empty list
        if not rids:
            return records

        # for each matching record
        for RID in rids:

            # get the location in the table
            page_range_num, page_num, offset = self.page_directory.get(RID)

            # TODO - double check that logic is correct
            # check to see if the record has been updated
            LID = self.ranges[page_range_num].read_column(page_num, offset, INDIRECTION_COLUMN)
            tps = self.ranges[page_num].tps

            # if it has been updated
            if LID > tps:
                # get the updated records location
                _, page_num, offset = self.page_directory.get(LID)

            # get the record
            record = self.ranges[page_range_num].read_record([[page_range_num, page_num, offset]], query_columns)

            # append the record to records
            records = records + record

        # return the records
        return records

    def update_record(self, key, columns):
        """
        Add a tail record for a specific record

        :param key: int                     # the primary key value for finding the record
        :param columns: []                  # a list of the new values to be added
        """
        # create an LID for the tail record
        LID = self.get_lid_value()

        # get the RID of the base record
        RID = self.indexes[self.prim_key_col_num].locate(value=key)

        # get the location of the base record
        page_range_num, page_num, offset = self.page_directory.get(RID[0])

        # get current schema encoding
        schema_encoding = self.ranges[page_range_num].read_column(page_range_num, page_num, offset,
                                                                  SCHEMA_ENCODING_COLUMN)

        # get the new schema encoding by ORing the new one with the existing one
        new_schema_encoding = schema_encoding | self.get_schema_encoding(columns)

        # if there is already a tail record get it's LID
        indirection_value = self.ranges[page_range_num].read_column(page_range_num, page_num, offset,
                                                                    INDIRECTION_COLUMN)

        # update the base record with the new indirection value and schema encoding
        self.ranges[page_range_num].update_schema_indirection(new_schema_encoding, LID, page_num, offset)

        # if there was originally an indirection value in the base record
        if indirection_value:
            # find the tail record associated with it
            _, page_num, offset = self.page_directory.get(indirection_value, [0, page_num, offset])

        # get the base or tail record
        # TODO: improve efficiency by only getting record values we need
        record = \
            self.ranges[page_range_num].read_record([[page_range_num, page_num, offset]], [1] * self.number_of_columns)[
                0]

        columns = [indirection_value, LID, int(time() * 1000000), new_schema_encoding, RID[0]] + list(columns)

        # for every column, if we have a new value save it, otherwise use old value
        for i in range(NUMBER_OF_META_COLUMNS, len(columns)):
            if columns[i] is None:
                columns[i] = record.columns[i]

        # add tail record
        page_num, offset = self.ranges[page_range_num].add_tail_record(columns)

        # update page directory
        self.page_directory.update({LID: [page_range_num, page_num, offset]})

        # check if we've reached update threshold
        if self.ranges[page_range_num].check_threshold():
            self.__merge(self.ranges[page_range_num])

    def sum_records(self, start_range, end_range, column_number):
        """
        Sum all the records of the given column from the starting rid to the ending rid

        :param start_range: int             # the key of the first record to accumulate
        :param end_range: int               # the rid of the last record to accumulate(inclusive)
        :param column_number: int           # the column to accumulate on

        :return: int                        # the outcome of summing all the records
        """
        sum = 0

        # TODO: TA said this check isn't necessary
        # if start_range >= end_range:
        #     return 0

        # for each possible key within the given range
        for key_in_range in range(start_range, end_range + 1):

            # get the list of RIDs that match
            rids = self.indexes[self.prim_key_col_num].locate(key_in_range)

            # if no matching RIDs break from this iteration of the loop
            if not rids:
                continue

            # otherwise for every matching RID
            for RID in rids:

                # get the location of the record and check to see if there has been an update
                page_range_num, page_num, offset = self.page_directory.get(RID)
                LID = self.ranges[page_range_num].read_column(page_range_num, page_num, offset, INDIRECTION_COLUMN)

                # if an update get the location of the latest update
                if LID:
                    page_range_num, page_num, offset = self.page_directory.get(LID)

                # do the actual summing
                sum += self.ranges[page_range_num].read_column(page_range_num, page_num, offset, column_number)

        return sum

    def delete_record(self, key):
        """
        delete all records with the given primary key

        :param key: int                     # primary key value of the record to be deleted
        """
        # get the RID of the record
        RID = self.indexes[self.prim_key_col_num].locate(key)

        # get the location of the record
        page_range_num, page_num, offset = self.page_directory.get(RID[0])

        # appending deleted RID to rid stack
        self.rid_stack.append([RID, page_range_num, page_num, offset])

        # lazy delete the record
        self.ranges[page_range_num].delete_record(page_num, offset)

        # TODO: why do we need to know how many valid records there are versus total records (valid+invalid) - terri and katya
        # modify the number of records in the table
        self.num_records -= 1

    def save_table(self, directory_name):
        """
         saves table data and page range data to files

         :param directory_name: string       # name of db directory
         """
        # write table data to file
        sys.setrecursionlimit(RECURSION_LIMIT)
        with open(os.path.expanduser(directory_name + self.name + '/table'), 'wb+') as output:
            pickle.dump(self, output, pickle.HIGHEST_PROTOCOL)

        bp.close()

    def __merge(self, original_page_range):

        # beginning merge on this page range
        original_page_range.merge = True

        # create a copy of the page range
        copy_page_range = PageRange(self.number_of_columns, self.prim_key_col_num)

        # num of base, tail pages of original to create copy
        num_of_base_pages = original_page_range.columns[0].last_base_page + 1
        num_of_tail_pages = original_page_range.columns[0].last_page - original_page_range.columns[0].last_base_page

        # copy base pages to the copy page range
        for i, col in enumerate(original_page_range.columns):
            for n in range(num_of_base_pages):
                copy_page_range.columns[i].pages.append(original_page_range.columns[i].pages[n])

        # dictionary of RIDs we've seen so far
        has_seen = []

        # iterate through all tail pages except last, check for last update to record
        iterate_offset = PAGE_SIZE
        for tail_num in range(original_page_range.tps + 1, -1, -2):
            for i in range(RECORDS_PER_PAGE):
                iterate_offset -= 8
                # return user data and rid column
                record = original_page_range.read_record([tail_num, iterate_offset], [1] * self.number_of_columns)
                base_rid = record.columns[BASE_RID]
                if not (base_rid in has_seen):
                    has_seen.append(base_rid)

                    _, page_num, offset = self.page_directory[base_rid]

                    # ignore meta data columns and copy just the user data over
                    for n, column in enumerate(copy_page_range.columns, start=NUMBER_OF_META_COLUMNS):
                        column.update_value((page_num, offset, record.columns[n]))

                    # TODO - double check that correct

                    # edit base record's meta data columns accordingly
                    copy_page_range.columns[INDIRECTION_COLUMN].update_value(page_num, offset, 0)
                    copy_page_range.columns[SCHEMA_ENCODING_COLUMN].update_value(page_num, offset, 0)

                # if have seen all rids in base pages, break since found all latest updates
                if len(has_seen) == self.rid:
                    break

        # append tail pages to copy of page range
        for i, col in enumerate(original_page_range.columns):
            for n in range(num_of_tail_pages):
                copy_page_range.columns[i].pages.append(original_page_range.columns[i].pages[n])

        # update tps of copy of page range
        copy_page_range.tps = copy_page_range.columns[0].last_page - 1

        # update range and get new index to update page directory
        self.ranges.append(copy_page_range)
        range_index = len(self.ranges) - 1

        # update page directory to point to new merged pages
        for rid in has_seen:
            _, page_num, offset = self.page_directory[rid]
            values = [range_index, page_num, offset]
            self.page_directory[rid] = values

        # finished merging this page range
        original_page_range.merge = False


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


def make_indexes(number_of_columns, prim_key_column, table):
    """
    Make a list of empty indices for the table on setup. The primary key column index must always exist so instantiate
    only this index to begin with.

    :param number_of_columns: int       # the number of columns in the table
    :param prim_key_column: int         # the index of the primary key column
    :param table: table object          # the table for which these indexes are being created

    :return: []                         # a list of None with an index in the primary key index index
    """
    indexes = [None] * number_of_columns
    index = Index()
    index.create_index(table=table, column_number=prim_key_column)
    indexes[prim_key_column] = index
    return indexes
