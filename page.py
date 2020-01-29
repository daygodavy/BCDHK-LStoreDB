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
            print("num records in write: " + str(temp.num_records))
            self.num_records += 1
            print("num records in write: " + str(temp.num_records))
            start = PAGE_SIZE - self.num_records * 8
            self.data[start: start + 8] = struct.pack(">q", value)
            print("success")
        else:
            return print("page full")

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
temp.write(2)
print("num records: " + str(temp.num_records))
temp.write(4)
print("num records: " + str(temp.num_records))
temp.write(6)
print("num records: " + str(temp.num_records))
temp.write(8)
print(temp.read(0))
print(temp.data)
