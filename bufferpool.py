from config import *
import os



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
    def __check_object(self, page_range, page_num, offset):
        for i,obj in self.pool:
            if obj.page_range == page_range and obj.page_num == page_num:
                # obj found in bufferpool
                # increment obj num_access
                obj.num_access += 1
                # pin the obj
                obj.pin = True # FIXME: should we pin here?
                return obj.object
        # obj not found in bufferpool
        self.__add_object(page_range, page_num, offset)



    '''
    Adding an object to the bufferpool
    '''
    def __add_object(self, page_range, page_num, offset):
        # check if bufferpool is full, if so evict
        if self.num_of_objects > BUFFERPOOL_MAX_OBJECTS:
            self.__replace_object(self)
            # Need to evict 1 onject out, i.e writing it back to the memory

        # retrieve target obj from disk
        target = "pagerange" + str(page_range)
        file = open(os.path.expanduser(self.table.directory_name + self.table.name + target), 'rb+')
        

        #append new object
        self.pool.append(Buff_object())
        self.num_of_objects += 1

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
                if min_access == 0:
                    break

        # 3)If dirty, flush to disk
        if min_obj.dirty is True:
            #TODO: Flush to disk
            #self.min_obj.object.filename
            #self.min_obj.object.page_offset
            pass

        # 4)Evict from the buffer pool
        self.__evict_object(idx)


