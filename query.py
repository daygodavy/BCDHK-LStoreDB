from table import Table, Record
from index import Index
from config import *
import time
import datetime
import struct


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    """
    def __init__(self, table):
        self.table = table
        pass

    """
    # internal Method
    # Read a record with specified RID
    """
    def delete(self, key):
        self.table.delete_record(key)

    """
    # Insert a record with specified columns
    """
    def insert(self, *columns):
        self.table.add_record(*columns)

    """
    # Read a record with specified key
    """
    def select(self, key, query_columns):
        record_list = []
        query_columns = [0]*4 + query_columns
        record, tail_record = self.table.read_record(key, query_columns)
        if tail_record:
            record = tail_record
        for i in range(0, len(record.columns)):
            record.columns[i] = struct.unpack(ENCODING, record.columns[i])[0]
        record_list.append(record)
        return record_list

    """
    # Update a record with specified key and columns
    """
    def update(self, key, *columns):
        self.table.update_record(key, columns)

    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        return self.table.sum_records(start_range, end_range, aggregate_column_index)
