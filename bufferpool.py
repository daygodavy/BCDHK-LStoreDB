from config import *
from collections import defaultdict
import os
from page import *


# we dont know either if it's page or page range
class Buff_object:
    def __init__(self):
        self.num_access = 0
        self.dirty = False
        self.pin = False
        # page
        self.object = None
        self.page_range = 0
        self.page_num = 0


class Bufferpool:
    def __init__(self):
        # an array of all bufferpool objects
        self.pool = defaultdict(int)
        self.num_of_objects = 0
        self.table = None
        self.latest_obj_key = None

    '''
    Creating new obj
    '''

    # should we pass boolean for read/write to mark it dirty for write?
    def __create_new_obj(self, page_range, page_num=""):
        obj = Buff_object()
        obj.pin = True  # FIXME: should we pin here?
        obj.object = Page()
        page_range.num_pages += 1
        #new_buf_obj.object.data = bytearray(file.read(PAGE_SIZE))  # FIXME: is this valid
        obj.page_num = page_range.num_pages - 1
        obj.page_range = page_range

        self.pool[(page_range.id, obj.page_num)] = obj
        self.latest_obj_key = (page_range.id, obj.page_num)
        self.num_of_objects += 1

        return obj

        #return self.__add_object(page_range)

    '''
    Get object from disk
    page_range: a PageRange obj
    '''

    def __get_object(self, page_range, page_num):
        # check if bufferpool is full, if so evict
        if self.num_of_objects > BUFFERPOOL_MAX_OBJECTS:
            print("============BUFFERPOOL FULL=============")
            print(self.num_of_objects)
            print(BUFFERPOOL_MAX_OBJECTS)
            self.__replace_object()
            # Need to evict 1 object out, i.e writing it back to the memory

        #if obj is on the disk
        # retrieve target obj from disk
        target = "/pagerange" + str(page_range.id)
        file = open(os.path.expanduser(self.table.directory_name + self.table.name + target), 'rb+')
        file.seek(page_num * PAGE_SIZE, 0)

        obj = Buff_object()
        obj.pin = True  # FIXME: should we pin here?
        obj.object = Page()
        obj.page_range = page_range
        obj.page_num = page_num
        self.pool[(page_range.id, page_num)] = obj
        self.num_of_objects += 1

        return obj

    '''
    Evict a bufferpool object from the bufferpool by its index
    #TODO:
    '''

    def __evict_object(self, key):
        try:
            self.pool[key] = -1
            self.num_of_objects -= 1
        except:
            print("Index out of range")

    '''
    Replacement policy for bufferpool via LRU
    '''

    def __replace_object(self):
        # 1)Find all unpinned objects in buffer pool
        # 2)Check number of accesses to determine evicted object (LRU)
        min_access = 999999999999
        min_obj = None
        key = -1

        for key in self.pool:
            obj = self.pool[key]
            print("Inside bufferpool", obj)
            if obj.pin is False and min_access > obj.num_access:
                min_access = obj.num_access
                min_obj = obj
                key = key
                if min_access == 1:
                    break

        # 3)If dirty, flush to disk
        if min_obj.dirty is True:
            # TODO: Flush to disk
            target = "/pagerange" + str(min_obj.page_range.id)
            file = open(os.path.expanduser(self.table.name + target), 'wb+')
            file.seek(min_obj.page_num * PAGE_SIZE, 0)
            file.write(min_obj.object.data)  # FIXME: is this valid or must loop?

        # 4)Evict from the buffer pool
        self.__evict_object(key)

    '''
    Read obj from bufferpool
    '''

    def read_object(self, page_range, page_num, offset):
        obj = self.pool[(page_range.id, page_num)]
        if obj == -1: #not in bufferpool but in disk
            print("NOT IN BUFFERPOOL")
            obj = self.__get_object(page_range, page_num)

        #print("read_obj:", obj)
        # for i in range(100):
        #     print(obj.object.data[i])
        obj.num_access += 1
        obj.pin = True
        val = obj.object.read(start_index=offset)
        obj.pin = False
        return val

    '''
    Write to obj in bufferpool
    '''

    def write_object(self, page_range, val):
        #getting the page for the new record
        obj = None
        if len(self.pool) == 0:
            obj = self.__create_new_obj(page_range)
        else:
            obj = self.pool[self.latest_obj_key]
            if obj.object.has_capacity() is False:
                obj = self.__create_new_obj(page_range)

        obj.num_access += 1
        obj.pin = True
        obj.dirty = True
        offset = obj.object.write(value=val)
        #obj.object.write(offset=obj.latest_offset, value=val)
        obj.pin = False
        #print("DONE WRITE")
        return obj.page_num, offset

bp = Bufferpool()
