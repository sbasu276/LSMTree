import json
import sys
import multiprocessing as mp
import os
import time
import math
import random
from time import sleep
import string
import subprocess
import threading
from client import Client

def rand_val(size):
    return ''.join(random.choices(string.ascii_lowercase, k=size))

def gen_get(key):
    return 'GET '+str(key)+'\n'

def gen_put(key):
    val = rand_val(500)
    return 'PUT '+str(key)+' '+val+'\n'

def get_keys():
    size = 1000
    selected_keys = [v for v in range(size)]
    rem_keys = [v for v in range(size, 10000)]
    return selected_keys, rem_keys

def make_request_streams(rr, rw, size, keys, rem_keys):
    r_ratio = rw
    w_ratio = 1 - r_ratio
    tot_reqs = 10*rr
    num_clients = math.ceil(rr/2)
    stream = []
    key = None
    for c in range(tot_reqs):
        if random.random() <= 0.9:
            key = random.randint(0,999)
        else:
            key = random.randint(1000, 9999)
        if random.random()<=r_ratio:
            stream.append(gen_get(key))
        else:
            stream.append(gen_put(key))
    return stream

h = "127.0.0.1"
p = "65210"
size = [500]
rates = [600]
ratio = [0.9, 0.1]
for s in size:
    keys, rem_keys = get_keys()
    for rate in rates:
        for rw in ratio:
            out = '../data/tbs/%s'%(rw)
            streams = make_request_streams(rate, rw, s, keys, rem_keys)
            with open(out, "w") as f:
                for s in streams:
                    f.write(s)
            print("DONE %s"%out)
