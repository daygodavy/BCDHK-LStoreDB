from config import *
from column import Column
from record import Record


class PageRange:

    def __init__(self, num_of_columns, primary_key_column):
        """
        The definition of a page range object. A page range holds a subset of the table.
        A page range will maintain all columns of a table but only a subset of all the records.

        :param num_of_columns: int           # the number of columns in the table
        :param primary_key_column: int       # the number of the primary key column
        """
        # the number of columns in the table
        self.num_of_columns = num_of_columns

        # the column of the primary key
        self.primary_key_column = primary_key_column

        # the columns of the page range
        self.columns = make_columns(number_of_columns=self.num_of_columns)

        # the number of records in the page range
        self.num_of_records = 0

    def add_base_record(self, columns):
        """
        Add a record to the page range

        :param columns: []                  # a list of the values defining the record

        :return: int                        # the page number of the added record
        :return: int                        # the offset of the added record
        """
        page_number = 0
        offset = 0
        for i, column in enumerate(self.columns):
            page_number, offset = column.add_base_value(value=columns[i])
        self.num_of_records += 1
        return page_number, offset

    def read_record(self, locations, query_columns):
        """
        Read a record from the page range

        :param locations: []                # a list containing tuples of page number and offset of each record to be read
        :param query_columns: []            # a list of Nones and 1s defining which column values to return

        :return: []                         # the record values, in sequential order
        """
        # a list to hold the compiled records
        records = []

        # a list to hold the values of one record at a time
        values = []

        # for each location[page_number, offset]
        for location in locations:

            # for each column in the page range
            for i, _ in enumerate(self.columns):

                # append the record value for this column to values
                if query_columns[i]:
                    values.append(self.read_column(page_number=location[0], offset=location[1], column_number=i))

            # store the record in records
            records.append(Record(rid=values[RID_COLUMN], key=values[self.primary_key_column], columns=values))

            # reset values
            values = []

        # return all the collected records from this page range
        return records

    def read_column(self, page_number, offset, column_number):
        """
        read a single column in the page range

        :param page_number: int             # the page number of the record to be read
        :param offset: int                  # the offset of the record in the page to be read
        :param column_number: int           # the column number to read

        :return: int                        # the record value
        """
        return self.columns[column_number].read(page_number=page_number, offset=offset)

    def add_tail_record(self, columns):
        """
        Update a record by adding a tail record to the base record.
        The updated record in the tail page will contain all the values of the record.

        :param columns: []                  # the column values being updated

        :return: int                        # the page number of the added record
        :return: int                        # the offset of the added record
        """
        page_number = 0
        offset = 0
        for i, column in enumerate(self.columns):
            page_number, offset = column.add_tail_value(value=columns[i])
        return page_number, offset

    def delete_record(self, page_num, offset):
        """
        Lazily delete the base record. Complete deletion is done at merge

        :param page_num: int            # the page number where the record is located
        :param offset: int              # the offset in the page where the record is located
        """
        for column in self.columns:
            column.delete(page_num, offset)

        self.num_of_records -= 1

    def update_schema_indirection(self, encoding, indirection_value, page_num, offset):
        """
        update the base record with the new schema and indirection values

        :param encoding: int                        # an integer representing the bit value of the schema encoding
        :param indirection_value: int               # the LID value of the new tail record being added
        :param page_num: int                        # the page number of the above values
        :param offset: int                          # the offset of the above values
        """
        self.columns[INDIRECTION_COLUMN].update_value(page_num, offset, indirection_value)
        self.columns[SCHEMA_ENCODING_COLUMN].update_value(page_num, offset, encoding)

    def has_capacity(self):
        """
        check to see if this page range can support any more records

        :return: bool                       # relative to the number of records and PAGE_RANGE_SIZE
        """
        if self.num_of_records < PAGE_RANGE_SIZE:
            return True
        else:
            return False


def make_columns(number_of_columns):
    """
    Make the starting empty columns for the page range

    :param number_of_columns: int           # the number of columns in the table

    :return: []                             # a list of columns
    """
    columns = []
    for i in range(number_of_columns):
        columns.append(Column())
    return columns

