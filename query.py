from config import *
import struct


class Query:
    """
    Creates a Query object that can perform different queries on the specified table
    """

    def __init__(self, table):
        self.table = table
        pass

    def delete(self, key):
        """
        internal Method

        Read a record with specified RID
        :param key: int             # the primary key value of the record to be deleted
        """
        self.table.delete_record(key)

    def insert(self, *columns):
        """
        Insert a record with specified columns

        :param columns:            # the user column values to be saved in the database
        """
        self.table.add_record(*columns)

    def select(self, key, query_columns):
        """
        Read a record with specified key

        :param key: int             # the key value to select records based on
        :param query_columns: []    # what columns to return. array of 1 or 0 values
        """
        record_list = []
        query_columns = [0] * 4 + query_columns
        records = self.table.read_record(key, query_columns)
        for record in records:
            for i in range(0, len(record.columns)):
                record.columns[i] = struct.unpack(ENCODING, record.columns[i])[0]
            record_list.append(record)
        return record_list
        # if tail_record:
        #     record = tail_record
        # for i in range(0, len(record.columns)):
        #     record.columns[i] = struct.unpack(ENCODING, record.columns[i])[0]
        # record_list.append(record)
        # return record_list

    def update(self, key, *columns):
        """
        Update a record with specified key and columns

        :param key: int           # the primary key value to find the specific record for update
        :param columns: []        # the new values to update the record with, may contain values of None
        """
        self.table.update_record(key, columns)

    def sum(self, start_range, end_range, aggregate_column_index):
        """
        A method which sums the values in a column from start_range to end_range

        :param start_range: int                 # Start of the key range to aggregate
        :param end_range: int                   # End of the key range to aggregate
        :param aggregate_column_index: int      # Index of desired column to aggregate
        """
        return self.table.sum_records(start_range, end_range, aggregate_column_index)
