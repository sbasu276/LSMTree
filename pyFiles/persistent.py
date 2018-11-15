import pickledb
import threading

class Persistent:
    def __init__(self, name, db=None):
        self.name = name
        self.db = db if db else pickledb.load(name, False)
        self.lock = threading.Lock()

    def get(self, key):
        return self.db.get(key)

    def put(self, key, value):
        #self.lock.acquire()
        if self.db.get(key):
            self.db.set(key, value)
            self.db.dump()
            #self.lock.release()
            return True
        else:
            #self.lock.release()
            return False

    def insert(self, key, value):
        self.lock.acquire()
        if self.db.get(key):
            self.lock.release()
            return False
        self.db.set(key, value)
        self.db.dump()
        self.lock.release()
        return True

    def delete(self, key):
        try:
            self.db.rem(key)
            self.db.dump()
            return True
        except:
            return False
       
    def writeback(self, key, value):
        self.db.set(key, value)
        self.db.dump()
