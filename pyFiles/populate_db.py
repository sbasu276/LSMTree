import pickledb

db = pickledb.load('names.db', False)

with open('names.txt') as f:
    lines = f.readlines()
    for line in lines:
        kvs = line.split()
        db.set(kvs[0], kvs[1])
db.dump()
