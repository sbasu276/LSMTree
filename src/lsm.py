import time
from enum import Enum

class OP(Enum):
    WRITE = 1
    MERGE = 2

class LsmNode:
    def __init__(self, key, value, tomestone=False):
        self.key = key
        self.value = value,
        self.timestamp = time.time()
        self.tombstone = tombstone

class C1:
    def get(self, key):
        pass

class WAL:
    def __init__(self, logger):
        pass

    def log(op, args):
        pass

class LsmTree:
    def __init__(self, limit):
        self.memtable = {}
        self.c1 = C1() #c1 class obj
        self.limit = limit
        self.size = 0
        self.buffer = None
        self.wal = WAL() #write-ahead log

    def get(self, key):
        if key in self.memtable:
            if self.memtable[key].tombstone:
                return None
            return self.memtable[key].value
        return self.c1.get(key)
    
    def put(self, key, value):
        if key in self.memtable:
            node = LsmNode(key, value, tombstone=False)
            self.memtable[key] = node
        else:
            self.__add(key, value)

    def insert(self, key, value):
        self.put(key, value)

    def delete(self, key):
        self.__add(key, value, tombstone=True)
            
    def writeback(self, key, value):
        self.__add(key, value)

    def __is_full(self):
        return (self.size >= self.limit)

    def __add(self, key, value, tombstone = False)
        node = LsmNode(key, value, tombstone=tombstone) 
        self.memtable[key] = node
        self.size = self.size + 1
        if self.__is_full():
            self.__flush_to_buf()

    def __merge(self):
        pass

    def __flush_to_buf(self):
        self.buffer = [(k, v) for k, v in self.memtable.items()]
        self.memtable = {}
        self.size = 0
        self.buffer.sort(key=lambda x: int(x[0]))
        self.__flush()

    def __flush(self):
        sst_name = "sstable_%s"%str(time.time())
        log_args = (sst_name, self.buffer)
        self.wal.log(OP.WRITE, log_args)
        with open(sst_name, 'w') as f:
            for key, elem in self.buffer:
                line = ",".join([key, elem[1].value, \
                                 elem[1].timestamp, elem[1].tombstone])
                f.write(line+"\n")

    def show(self):
        if self.size:
            print("### MEMTABLE ###")
            for k, elm in self.memtable.items():
                print(k, " ", elm.value, " ", elm.timestamp, " ", elm.tombstone)
        else:
            print("### BUFFER ###")
            for elm in self.buffer:
                print(elm[0], " ",  elm[1].value, " ", elm[1].timestamp, " ", elm[1].tombstone)


