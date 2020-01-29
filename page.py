from config import *
import struct


class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE)

    def has_capacity(self):
        if self.num_records < RECORDS_PER_PAGE:
            return True
        return False

    def write(self, value):
        if self.has_capacity():
            self.num_records += 1
            start = self.num_records * 64
            self.data[start: start + 63] = struct.pack(">q", value)
        else:
            return "Page Full"

    def read(self, start_index):
        if start_index < PAGE_SIZE:
            return self.data[start_index: start_index + 63]
        else:
            return "Index Beyond Range"
