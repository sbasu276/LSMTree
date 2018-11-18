import time
from enum import Enum
import utils
from utils import binary_search
from lsm_node import LsmNode
from wal import WAL
from C1 import C1

class OP(Enum):
    WRITE = 1
    MERGE = 2
"""
class C1:
    def __init__(self, db_name):
        self.c1_file = db_name
        self.fd = None

    def get(self, key):
        self._gen_fd()
        c1 = self._load()
        self._close()
        keys = [int(x[0]) for x in c1]
        pos = binary_search(keys, int(key))
        if pos:
            elm = c1[pos]
            if bool(elm[3]):
                return None
            return elm[1]
        return None

    def merge(self, memtbl):
        self._gen_fd()
        c1 = self._load()


    def _get_fd(self, mode='r'):
        self.fd = open(self.c1_file, mode)

    def _close(self):
        if self.fd:
            self.fd.close()
    
    def _load(self):
        c1 = []
        for line in self.fd.readlines():
            c1.append(line.strip('\n').split(','))
        return c1
"""

class LsmTree:
    def __init__(self, limit, db_name, logfile):
        self.memtable = {} # Memtable
        self.c1 = C1(db_name) # c1 class obj
        self.limit = limit # Memtable limit
        self.size = 0
        self.buffer = []
        self.wal = WAL(logfile) # write-ahead log

    def get(self, key):
        if key in self.memtable:
            if self.memtable[key].tombstone:
                return None
            return self.memtable[key].value
        return self.c1.get(key)
    
    def put(self, key, value):
        #Dedup keys here
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
        node = LsmNode(key, value, tombstone=tombstone) 
        self.memtable[key] = node
        self.size = self.size + 1
        if self.__is_full():
            self.__flush()

    def __flush(self):
        self.buffer = [(k, v) for k, v in self.memtable.items()]
        self.memtable = {}
        self.size = 0
        self.buffer.sort(key=lambda x: int(x[0]))
        self._flush()

    def _flush(self):
        sst_name = "sstable_%s"%str(time.time())
        # Write to log
        log_args = (sst_name, self.buffer)
        self.wal.txn()
        self.wal.log(OP.MERGE, log_args)
        # Flush to SS Table
        c0_list = [v for k,v in self.buffer]
        self.c1.merge(c0_list)
        """
        with open(sst_name, 'w') as f:
            for key, elem in self.buffer:
                line = ",".join([str(key), str(elem.value), \
                                 str(elem.timestamp), str(elem.tombstone)])
                f.write(line+"\n")
        """
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
    l = LsmTree(3, 'c1_test', 'test_log.log')
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
