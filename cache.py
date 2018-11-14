class CacheElement:
    def __init__(self, key, value, dirty=False):
        self.key = key
        self.value = value
        self.dirty = dirty

class Cache:
    def __init__(self, size):
        """ Initialize the Cache
        """
        self.limit = size # Total size of cache
        self.cache = [] #list of Cache elements (key, value, dirty)
        self.size = 0 # Current size of cache
        self.locks = {} # Dict to store key -> lock 

    def get(self, key):
        """ Get a value from a cache
            key: argument to get
            Returns Value if found, None otherwise
        """
        val = None
        position = self.__search(key)
        if position is not None:
            elem = self.__pop(key, position)
            self.__insert(key, elem.value, dirty=elem.dirty)
            val = elem.value
        return val

    def put(self, key, value):
        """ Put a value to the cache
            key, value: arguments to put
            Returns value if key existed in cache, else return None
        """
        retval = None
        position = self.__search(key)
        if position is not None:
            self.__pop(key, position)
            self.__insert(key, value, dirty=True)
            retval = value
        return retval
    
    def insert(self, key, value, dirty=True):
        """ Inserts an entry to cache
            Returns key, value if an eviction of dirty entry had to be made,
            else return (None, None)
        """
        ret_key, ret_val = None, None
        position = self.__search(key)
        if position is not None:
            #self.cache.put(key, value)
            return None, None, False
        else:
            if self.__is_full():
                elem = self.__pop()
                if elem.dirty:
                    ret_key, ret_val = elem.key, elem.value
            self.__insert(key, value, dirty=dirty)
        return ret_key, ret_val, True
                
    def evict(self):
        """ Evicts the last entry of the cache
            Returns key, value if the evicted entry was dirty, 
            else return (None, None)
        """
        ret_key, ret_val = None, None
        if self.__is_full():
            elem = self.__pop()
            if elem.dirty:
                ret_key, ret_val = elem.key, elem.value
        return ret_key, ret_val

    def delete(self, key=None):
        """ Deletes an entry from cache
        """
        position = self.__search(key)
        if position is not None:
            self.__pop(key, position)
            return True
        return False

    def __is_full(self):
        return (self.size >= self.limit)

    def __pop(self, key=None, position=None):
        """ Private method: Pops an entry from cache
            pops the last element by default (LRU)
        """
        elem = None
        if key:
            val = None
            if position is not None:
                elem = self.cache.pop(position)
        else:
            elem = self.cache.pop()
        self.size = self.size-1
        return elem

    def __insert(self, key, value, dirty=False, position=0):
        """ Private method: Inserts a value in the given position
            By default inserts at the beginning (LRU)
        """
        elem = CacheElement(key, value, dirty=dirty)
        self.cache.insert(position, elem)
        self.size = self.size+1

    def __search(self, key):
        """ Private method: Searches a key in cache
            Return position if found, None otherwise
        """
        if key is None:
            return None
        for i, item in enumerate(self.cache):
            if item.key == key:
                return i
        return None

    def show(self):
        for item in self.cache:
            print("Key: ", item.key, "Value: ", item.value, "Dirty: ", item.dirty)
