from config import *

#we dont know either if it's page or page range
class Buff_object:
    def __init__(self):
        self.num_access = 0
        self.dirty = False

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
