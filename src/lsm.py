import time

class C0Element:
    def __init__(self, value, tomestone=False):
        self.value = value,
        self.timestamp = time.time()
        self.tombstone = tombstone

class LsmTree:
    def __init__(self, limit):
        self.memtable = {}
        self.limit = limit
        self.size = 0
        self.buffer = None

    def get(self, key):
        pass
    
    def put(self, key, value):
        self.__add(key, value)

    def insert(self, key, value):
        pass

    def delete(self, key):
        pass

    def writeback(self, key, value):
        pass

    def __is_full(self):
        return (self.size >= self.limit)

    def __add(self, key, value, tombstone = False)
        if self.__is_full():
            self.__flush_to_buf()
        elem = C0Element(value, tombstone) 
        self.memtable[key] = elem
        self.size = self.size + 1

    def __merge(self):
        pass

    def __flush_to_buf(self):
        self.buffer = [(k, v) for k, v in self.memtable.items()]
        self.buffer.sort(key=lambda x: int(x[0]))
        self.memtable = {}
        self.size = 0

    def flush(self):
        sst_name = "sstable_%s"%str(time.time())
        with open(sst_name, 'w') as f:
            for elem in self.buffer:
                line = ",".join([elem[0], elem[1].value, \
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


