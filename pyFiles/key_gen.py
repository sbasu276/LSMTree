import sys
import string
import random
import pickledb
import pickle
import csv


def rand_str(size):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=size))

def rand_val(size):
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=size))

def main():
    keys = []
    keys_append = keys.append
    num = int(sys.argv[1])
    size = int(sys.argv[2])
    db = pickledb.load(sys.argv[3], False)
    db_set = db.set
    for i in range(num):
        k = size - len(str(i))
        key = rand_str(k) + str(i)
        val = rand_val(size)
        db_set(key, val)
        keys_append(key)
    db.dump()
    with open('keys_%s.txt'%num, 'w') as f:
        for key in keys:
            f.write(key+'\n')

main()
