import sys
import string
import random
import csv
import argparse
import pickledb

def rand_str(size):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=size))

def rand_val(size):
    return ''.join(random.choices(string.ascii_lowercase, k=size))

def main(num, size, db, db_type):
    keys = []
    keys_append = keys.append
    for i in range(num):
        k = size - len(str(i))
        key = str(i) # '0'*k + str(i)
        value = rand_val(size)
        if db_type=="pickle":
            db.set(key, value)
        else:
            db.write(key+","+value+","+"0"+","+"False"+"\n")
        keys_append(key)
    if db_type=="pickle": 
        db.dump()
    with open('../data/keys_%s.txt'%size, 'w') as f:
        for key in keys:
            f.write(key+'\n')

def parse_arguments():
    """ Process command line arguments
    """
    parser = argparse.ArgumentParser(description = 'Scrap error codes')
    parser.add_argument('-A','--gen-all', dest='gen_all', action='store_true', required=False)
    parser.add_argument('-d','--db-name', dest='db_name', required=True)
    parser.add_argument('-t','--db-type', dest='db_type', required=True)
    parser.add_argument('-n','--num', dest='num', default=0, required=False)
    parser.add_argument('-s','--size', dest='size', default=0, required=False)
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    args = parse_arguments()
    if args.db_type=="pickle":
        db = pickledb.load(args.db_name, False)
    else:
        db = open("../data/"+args.db_name, "w")
    tot_size = 9000000
    if args.gen_all:
        for size in [100, 1000, 10000]:
            num = tot_size//3
            main(num//size, size//2, db, args.db_type)
    else:
        if args.size:
            num = int(args.num) if args.num else tot_size//int(args.size)
            main(num, int(args.size)//2, db, args.db_type)
        else:
            raise ValueError
