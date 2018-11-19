from lsm_node import LsmNode
from heapq import merge

def dedup(merged):
    i = 0
    while i<len(merged)-1:
        if merged[i][0] == merged[i+1][0]:
            if merged[i][3] or merged[i+1][3]:
                merged.pop(i)
                merged.pop(i)
            else:
                merged.pop(i+1)
        else:
            i = i+1
    i = 0
    while i<len(merged):
        if merged[i][3]:
            merged.pop(i)
        else:
            i = i+1

class C1:
    def __init__(self, db_name):
        self.c1_file = db_name
        self.fd = None
        f = open(db_name, 'a')
        f.close()

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

    def merge(self, sst):
        self._gen_fd()
        c1 = self._load()
        self._close()
        merged = [e for e in merge(sst, c1, key=lambda x: x[0])]
        dedup(merged)
        self._write(merged)

    def _gen_fd(self, mode='r'):
        self.fd = open(self.c1_file, mode)

    def _close(self):
        if self.fd:
            self.fd.close()
    
    def _load(self):
        c1 = []
        for line in self.fd.readlines():
            key, value, timestamp, tombstone = line.strip('\n').split(',')
            tombstone = True if tombstone=="True" else False
            c1.append([int(key), value, timestamp, tombstone])
        return c1

    def _write(self, merged):
        self._gen_fd(mode='w')
        for e in merged:
            line = ",".join([str(x) for x in e])
            self.fd.write(line+'\n')
        self._close()

