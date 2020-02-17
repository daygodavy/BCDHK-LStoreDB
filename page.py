from config import *


class Page:

    def __init__(self):
        # number of records in the page
        self.num_records = 0
        
        # the actual data for the page object
        self.data = bytearray(PAGE_SIZE)

    """
    A method which checks if the page has room for another record value
    """
    def has_capacity(self):
        """
        A method which checks if the page has room for another record value
        """
        if self.num_records < RECORDS_PER_PAGE:
            return True
        return False

    def write(self, value):
        """
        A method which writes a record value to the page

        :param value: int              # the value record value to be written
        """
        start = self.num_records * 8
        self.num_records += 1
        self.data[start: start + 8] = encode(value)
        return start

    def read(self, start_index):
        """
        A method which reads from the page beginning at the "start index" offset

        :param start_index: int        # start index to start the read from
        """
        return decode(self.data[start_index: start_index + 8])
        # TODO: TA said this error check wasn't necessary
        # if start_index < PAGE_SIZE:
        #     return decode(self.data[start_index: start_index + 8])
        # else:
        #     return print("Index Beyond Range")

    def overwrite(self, offset, value):
        """
        overwrite the value in the page at the given offset with the given value

        :param offset: int              # the offset in the page where the overwrite occurs
        :param value:  int              # the value being written to the offset
        """
        self.data[offset: offset + 8] = encode(value)
