from config import *


class Query:
    """
    Creates a Query object that can perform different queries on the specified table
    """

    def __init__(self, table):
        self.table = table
        pass

    def delete(self, key):
        """
        Read a record with specified RID

        :param key: int             # the primary key value of the record to be deleted
        """
        self.table.delete_record(key)

    def insert(self, *columns):
        """
        Insert a record with specified columns

        :param columns: tuple        # the user column values to be saved in the database
        """
        self.table.add_record(list(columns))

    def select(self, key, column, query_columns):
        """
        Read a record with specified key

        :param key: int             # the key value to select records based on
        :param column: int          # column number to perform query on
        :param query_columns: []    # what columns to return. array of 1 or 0 values
        """
        return self.table.read_record(key, column + NUMBER_OF_META_COLUMNS, [0] * NUMBER_OF_META_COLUMNS + query_columns)

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
        return self.table.sum_records(start_range, end_range, aggregate_column_index + NUMBER_OF_META_COLUMNS)

