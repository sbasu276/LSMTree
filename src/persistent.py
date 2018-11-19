import pickledb

class Persistent:
    def __init__(self, name, db=None):
        self.name = name
        self.db = db if db else pickledb.load(name, False)

    def get(self, key):
        return self.db.get(key)

    def put(self, key, value):
        if self.db.get(key):
            self.db.set(key, value)
            self.db.dump()
            return True
        else:
            return False

    def insert(self, key, value):
        if self.db.get(key):
            return False
        self.put(key, value)
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

