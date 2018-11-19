import time
from enum import Enum
import utils
from utils import binary_search
from lsm_node import LsmNode
from wal import WAL
from c1 import C1

class OP(Enum):
    WRITE = 1
    MERGE = 2

class LsmTree:
    def __init__(self, limit, db_name):
        self.memtable = {} # Memtable
        self.c1 = C1(db_name) # c1 class obj
        self.limit = limit # Memtable limit
        self.size = 0
        self.buffer = []
        self.wal = WAL(db_name+"_log.log") # write-ahead log

    def get(self, key):
        key = int(key)
        if key in self.memtable:
            if self.memtable[key].tombstone:
                return None
            return self.memtable[key].value
        return self.c1.get(key)
    
    def put(self, key, value):
        #Dedup keys here
        key = int(key)
        if key in self.memtable:
            node = LsmNode(key, value, tombstone=False)
            self.memtable[key] = node
        else:
            self.__add(key, value)
        return True

    def insert(self, key, value):
        self.put(key, value)
        return True

    def delete(self, key):
        self.__add(key, None, tombstone=True)
        return True
            
    def writeback(self, key, value):
        self.__add(key, value)
        return True

    def __is_full(self):
        return (self.size >= self.limit)

    def __add(self, key, value, tombstone=False):
        node = LsmNode(int(key), value, tombstone=tombstone) 
        self.memtable[int(key)] = node
        self.size = self.size + 1
        if self.__is_full():
            self.__flush()

    def __flush(self):
        self.buffer = [(int(k), v) for k, v in self.memtable.items()]
        self.memtable = {}
        self.size = 0
        self.buffer.sort(key=lambda x: int(x[0]))
        self._flush()

    def _flush(self):
        # Write to log
        c0_list = [[int(v.key), v.value, v.timestamp, v.tombstone] for k,v in self.buffer]
        self.wal.txn()
        self.wal.log(OP.MERGE, c0_list)
        # Flush to SS Table
        self.c1.merge(c0_list)
        self.wal.txn(end=True)

    def show(self):
        if self.size:
            print("### MEMTABLE ###")
            for k, elm in self.memtable.items():
                print(k, " ", elm.value, " ", elm.timestamp, " ", elm.tombstone)
        else:
            print("### BUFFER ###")
            for elm in self.buffer:
                print(elm[0], " ",  elm[1].value, " ", elm[1].timestamp, " ", elm[1].tombstone)



#Unit test here
if __name__ == "__main__":
    l = LsmTree(3, '0_t', 'test_log.log')
    l.put(2, 10)
    l.put(3, 11)
    l.put(2, 15)
    print(l.get(2))
    l.show()
    l.insert(1, 18) #Should flush here
    l.show()
    l.insert(4, 20)
    l.delete(4)
    print(l.get(4))
    l.show()
    l.put(2, 23)
    l.put(3, 12)
    l.put(0, 10)
    l.show()
