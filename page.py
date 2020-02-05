from config import *
# from table import *
import struct
import logging


class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE)

    """
    A method which checks if the page has room for another record value
    """
    def has_capacity(self):
        if self.num_records < RECORDS_PER_PAGE:
            return True
        return False

    """
    A method which writes a record value to the page
    :param: value: int              # the value record value to be written
    """
    def write(self, value):
        if self.has_capacity():
            start = self.num_records * 8
            self.num_records += 1
            self.data[start: start + 8] = struct.pack(ENCODING, value)
            return start
        else:
            return print("page full")

    """
    A method which reads from the page beginning at the "start index" offset
    :param: start_index: int        # start index to start the read from
    """
    def read(self, start_index):
        if start_index < PAGE_SIZE:
            return self.data[start_index: start_index + 8]
        else:
            return print("Index Beyond Range")
