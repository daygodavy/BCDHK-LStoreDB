from config import *
import os
from page import *



#we dont know either if it's page or page range
class Buff_object:
    def __init__(self):
        self.num_access = 0
        self.dirty = False
        self.pin = False
        # page.. page range
        self.object = None
        self.page_range = 0
        self.page_num = 0




class Bufferpool:
    def __init__(self):
        #an array of all bufferpool objects
        self.pool = []
        self.num_of_objects = 0
        self.table = None


    '''
    Check if object is in bufferpool for retrieval
    '''
    # should we pass boolean for read/write to mark it dirty for write?
    def __get_object(self, page_range, page_num):
        for i,obj in self.pool:
            if obj.page_range == page_range and obj.page_num == page_num:
                # obj found in bufferpool
                # increment obj num_access
                obj.num_access += 1
                # pin the obj
                obj.pin = True # FIXME: should we pin here?
                return obj.object
        # obj not found in bufferpool so grab from disk
        return self.__add_object(page_range, page_num)

    '''
    Adding an object to the bufferpool
    '''
    def __add_object(self, page_range, page_num):
        # check if bufferpool is full, if so evict
        if self.num_of_objects > BUFFERPOOL_MAX_OBJECTS:
            self.__replace_object(self, page_range, page_num)
            # Need to evict 1 onject out, i.e writing it back to the memory

        # retrieve target obj from disk
        target = "pagerange" + str(page_range)
        file = open(os.path.expanduser(self.table.directory_name + self.table.name + target), 'rb+')
        file.seek(page_num * PAGE_SIZE, 0)

        # add obj from disk to buffer pool
        new_buf_obj = Buff_object()
        new_buf_obj.pin = True  # FIXME: should we pin here?
        new_buf_obj.object = Page()
        new_buf_obj.object.data = file.read(PAGE_SIZE) # FIXME: is this valid
        new_buf_obj.page_num = page_num
        new_buf_obj.page_range = page_range
        new_buf_obj.num_access += 1

        self.pool.append(new_buf_obj)
        self.num_of_objects += 1
        return new_buf_obj

    '''
    Evict a bufferpool object from the bufferpool by its index
    #TODO:
    '''
    def __evict_object(self, ind):
        try:
            self.pool.pop(ind)
            self.num_of_objects -= 1
        except:
            print("Index out of range")

    '''
    Replacement policy for bufferpool via LRU
    '''
    def __replace_object(self):
        # 1)Find all unpinned objects in buffer pool
        unpinned_objs = []
        for obj in self.pool:
            if obj.pin is False:
                unpinned_objs.append(obj)

        # 2)Check number of accesses to determine evicted object (LRU)
        min_access = 999999999999
        min_obj = None
        idx = -1
        for i,obj in unpinned_objs:
            if min_access > obj.num_access:
                min_access = obj.num_access
                min_obj = obj
                idx = i
                if min_access == 1:
                    break

        # 3)If dirty, flush to disk
        if min_obj.dirty is True:
            #TODO: Flush to disk
            target = "pagerange" + str(min_obj.page_range)
            file = open(os.path.expanduser(self.table.directory_name + self.table.name + target), 'wb+')
            file.seek(min_obj.page_num * PAGE_SIZE, 0)
            file.write(min_obj.object.data) #FIXME: is this valid or must loop?

        # 4)Evict from the buffer pool
        self.__evict_object(idx)


    '''
    Read obj from bufferpool
    '''
    def __read_object(self, page_range, page_num, offset):
        obj = self.__get_object(page_range, page_num)
        obj.num_access += 1
        obj.pin = True
        val = obj.object.read(offset=offset)
        obj.pin = False
        return val

    '''
    Write to obj in bufferpool
    '''
    def __write_object(self, page_range, page_num, offset, val):
        obj = self.__get_object(page_range, page_num)
        obj.num_access += 1
        obj.pin = True
        obj.dirty = True
        obj.object.overwrite(value=val, offset=offset)
        obj.pin = False

