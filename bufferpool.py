import os
from config import *
from threading import Lock


# we dont know either if it's page or page range
class Bufferpool_Page:
    def __init__(self, page_range_number, page_number):
        self.dirty = False

        # if True then do not evict
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
        self.lock = Lock()

    def __get_object(self, page_range_number, page_number):
        """
        Get the page to be written too

        :param page_range_number: int               # the number of the page range the page is in
        :param page_number: int                     # the number of the page in the page range

        :return: Bufferpool_Page                    # a bufferpool object containing the relevant page
        """
        # if the page is in the bufferpool
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
            self.lock.acquire()
            self.__evict()
            self.lock.release()

        # either way add the new buf_object to the pool
        buf_object = Bufferpool_Page(page_range_number, page_number)
        self.pool.insert(0, buf_object)
        self.keys[(page_range_number, page_number)] = True

        # create path for pageRange file
        target = os.path.expanduser(
            self.table.directory_name + self.table.name + "/pageRange" + str(page_range_number) + "/" + str(
                buf_object.page_number))

        # if file doesn't exist, make it
        buf_object.pin = True
        if not os.path.isfile(target):
            buf_object.page = bytearray(4096)
        else:
            file = open(os.path.expanduser(target), 'rb+')
            buf_object.page = bytearray(file.read())
            file.close()

        buf_object.pin = False

        return buf_object

    def __evict(self):
        # get a handle on the item to evict
        for eviction_item in reversed(self.pool):
           #print("Key: " + str(eviction_item.page_range_number) + ", " + str(eviction_item.page_number))
            #print(eviction_item.pin)
            if not eviction_item.pin:
                break

        # open the file and write the contents to disk
        target = os.path.expanduser(self.table.directory_name + self.table.name + "/pageRange" + str(eviction_item.page_range_number) + "/" + str(eviction_item.page_number))
        file = open(target, 'wb+')
        file.write(eviction_item.page)
        file.close()

        # evict the item
        del self.keys[(eviction_item.page_range_number, eviction_item.page_number)]

        self.pool.pop()

    def read(self, page_range_number, page_number, offset):
        """
        Read from a page

        :param page_range_number: int               # the page range to read from
        :param page_number: int                     # the page number the range to read from
        :param offset: int                          # the offset within the page to read from

        :return: int                                # the value to be read
        """
        # get the page to be read from
        buf_page = self.__get_object(page_range_number, page_number)

        # get the value we want to read
        buf_page.pin = True
        value = decode(buf_page.page[offset: offset + 8])
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
        buf_page.page[offset: offset + 8] = encode(value)
        buf_page.pin = False
        return buf_page.page_number

    def close(self):
        for i in range(len(self.pool)):
            self.lock.acquire()
            self.__evict()
            self.lock.release()


bp = Bufferpool()
