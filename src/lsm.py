import time
import logging
from enum import Enum

class OP(Enum):
    WRITE = 1
    MERGE = 2

class LsmNode:
    def __init__(self, key, value, tombstone=False):
        self.key = key
        self.value = value
        self.timestamp = time.time()
        self.tombstone = tombstone

class C1:
    def __init__(self, db_name):
        self.c1_fd = open(db_name, 'a')
        self.name = db_name

    def get(self, key):
        pass

class WAL:
    def __init__(self, logfile):
        self.logger = logging.getLogger("lsm.LsmTree")
        self.fh = logging.FileHandler(logfile)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(self.fh)

    def txn(self, end=False):
        if end is False:
            self.logger.info("Txn_BEGIN")
        else:
            self.logger.info("Txn_END")

    def log(self, op, *args):
        msg = str(op)+str(args)
        self.logger.info(msg)

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

    def insert(self, key, value):
        self.put(key, value)

    def delete(self, key):
        self.__add(key, None, tombstone=True)
            
    def writeback(self, key, value):
        self.__add(key, value)

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
        self.wal.log(OP.WRITE, log_args)
        # Flush to SS Table
        #self.c1.merge(self.buffer)
        with open(sst_name, 'w') as f:
            for key, elem in self.buffer:
                line = ",".join([str(key), str(elem.value), \
                                 str(elem.timestamp), str(elem.tombstone)])
                f.write(line+"\n")
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
