from config import *
from bufferpool import Bufferpool_Page, bp


class Page:

    def __init__(self, page_range, page_number):
        # number of records in the page
        self.num_records = 0

        # the page range this page belongs too
        self.page_range = page_range

        # the number of the page in the file
        self.page_number = page_number

        # store TPS, if None then it's a tail page
        self.tps = 0

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

        :param page_range:
        :param value: int              # the value record value to be written
        """
        start = self.num_records * 8
        self.num_records += 1
        bp.write(self.page_range, self.page_number, start, value)
        return start

    def read(self, page_range, page_number, start_index):
        """
        A method which reads from the page beginning at the "start index" offset

        :param start_index: int        # start index to start the read from
        """
        # TODO: TA said this error check wasn't necessary
        # if start_index < PAGE_SIZE:
        #     return decode(self.data[start_index: start_index + 8])
        # else:
        #     return print("Index Beyond Range")

        return bp.read(page_range, page_number, start_index)

    def overwrite(self, offset, value):
        """
        overwrite the value in the page at the given offset with the given value

        :param offset: int              # the offset in the page where the overwrite occurs
        :param value:  int              # the value being written to the offset
        """
        bp.write(self.page_range, self.page_number, offset, value)
