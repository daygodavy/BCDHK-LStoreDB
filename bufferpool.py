from config import *



#we dont know either if it's page or page range
class Buff_object:
    def __init__(self):
        self.num_access = 0
        self.dirty = False
        self.pin = False
        # page.. page range
        self.object = None




class Bufferpool:
    def __init__(self):
        #an array of all bufferpool objects
        self.pool = []
        self.num_of_objects = 0

    '''
    Adding an object to the bufferpool
    '''
    def __add_object(self):
        if self.num_of_objects > BUFFERPOOL_MAX_OBJECTS:
            self.__replace_object(self)
            #Need to evict 1 onject out, i.e writing it back to the memory

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

        # 2)Check number of accesses to determine evicted object
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


