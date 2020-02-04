from config import *
from table import *
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
        logging.warning("this worked")
        if self.has_capacity():
            start = self.num_records * 8
            print("num records in write: " + str(temp.num_records))
            self.num_records += 1
            print("num records in write: " + str(temp.num_records))
            self.data[start: start + 8] = struct.pack(ENCODING, value)
            print("success")
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


print("Records per page: " + str(RECORDS_PER_PAGE))
temp = Page()
print("temp has capacity?")
print(temp.has_capacity())
print("num records: " + str(temp.num_records))
temp.write(123)
print("num records: " + str(temp.num_records))
temp.write(4)
print("num records: " + str(temp.num_records))
temp.write(6)
print("num records: " + str(temp.num_records))
temp.write(8)
print(temp.read(0))
print(temp.data)
