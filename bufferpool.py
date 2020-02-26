import os
from config import *


# we dont know either if it's page or page range
class Bufferpool_Page:
    def __init__(self, page_range_number, page_number):
        self.dirty = False
        self.pin = False
        self.page = None
        self.page_range_number = page_range_number
        self.page_number = page_number


class Bufferpool:
    def __init__(self):
        # an array of all bufferpool objects
        self.pool = []
        self.keys = {}
        self.max_num_objects = 16
        self.table = None
        self.latest_obj_key = None

    def __get_object(self, page_range_number, page_number):
        """
        Get the page to be written too

        :param page_range_number: int               # the number of the page range the page is in
        :param page_number: int                     # the number of the page in the page range

        :return: Bufferpool_Page                        # a bufferpool object containing the relevant page
        """
        # if the page is in the bufferpool
        buf_page = Bufferpool_Page(page_range_number, page_number)
        if (page_range_number, page_number) in self.keys:
            # find the index and the object
            for index, buf_object in enumerate(self.pool):
                if buf_object.page_range_number == page_range_number and buf_object.page_number == page_number:
                    break

            # shuffle the list accordingly
            self.pool[:] = self.pool[:index] + self.pool[index + 1:]

            # put the object at the front of the list
            self.pool.insert(0, buf_object)
            return buf_object

        # otherwise add a new item to the pool
        # if the pool is full evict the last item
        if len(self.pool) >= self.max_num_objects:
            self.__evict()

        # either way add the new buf_object to the pool
        self.pool.insert(0, buf_page)
        self.keys[(page_range_number, page_number)] = True

        # get page from disk
        target = "/pagerange" + str(page_range_number)
        file = open(os.path.expanduser(self.table.directory_name + self.table.name + target), 'rb+')

        # find the offset of the start of the page in the file
        file.seek(page_number * PAGE_SIZE, 0)

        # assign the buf_page's page the correct data from disk
        buf_page.pin = True
        buf_page.page = bytearray(file.read(PAGE_SIZE))
        buf_page.pin = False

        return buf_page

    def __evict(self):
        eviction_item = self.pool[-1]
        del self.keys[(eviction_item.page_range_number, eviction_item.page_number)]
        del self.pool[-1]

    def read(self, page_range_number, page_number, offset):
        """
        Read from a page

        :param page_range_number: int               # the page range to read from
        :param page_number: int                     # the page number witin the range to read from
        :param offset: int                          # the offset within the page to read from

        :return: int                                # the value to be read
        """
        # get the page to be read from
        buf_page = self.__get_object(page_range_number, page_number)

        # get the value we want to read
        buf_page.pin = True
        value = decode(buf_page.page[ offset: offset + 8 ])
        buf_page.pin = False

        return value

    def write(self, page_range, page_number, offset, value):
        """
        Write an object to a page

        :param page_range:
        :param page_number:
        :param offset:
        :param value:
        :return:
        """
        # get the page to be written too
        buf_page = self.__get_object(page_range.my_index, page_number)

        # update meta values and write value to page
        buf_page.pin = True
        buf_page.dirty = True
        buf_page.page[ offset: offset + 8 ] = encode(value)
        buf_page.pin = False
        return buf_page.page_number

    def overwrite(self, page_range_number, page_number, offset, val):
        pass


bp = Bufferpool()
