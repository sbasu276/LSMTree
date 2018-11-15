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
    return ''.join(random.choices(string.ascii_lowercase+string.digits, k=size))

def gen_get(key):
    return 'GET '+str(key)

def gen_put(key):
    val = rand_val(len(key))
    return 'PUT '+str(key)+' '+val

def get_keys(fname):
    keys = []
    with open(fname, 'r') as f:
        for line in f.readlines():
            keys.append(line.strip('\n'))
    selected_keys = []
    size = math.ceil(len(keys)*0.1)
    samp = random.sample(range(len(keys)), size)
    for i in samp:
        selected_keys.append(keys[i])
    for v in selected_keys:
        keys.remove(v)
    return selected_keys, keys

def make_request_streams(rr, rw, size, keys, rem_keys):
    r_ratio = rw
    w_ratio = 1 - r_ratio
    tot_reqs = 10*rr
    num_clients = math.ceil(rr/2)
    streams = []
    for c in range(num_clients):
        stream = []
        for i in range(math.ceil(tot_reqs/num_clients)):
            if random.random() <= 0.9:
                arr = keys
            else:
                arr = rem_keys
            index = random.randint(0, len(keys)-1)
            if random.random()<=r_ratio:
                stream.append(gen_get(arr[index]))
            else:
                stream.append(gen_put(arr[index]))
        streams.append(stream)
    return streams, num_clients

size = [100, 1000, 10000]
rates = [60, 300, 600]
ratio = [0.9, 0.5, 0.1]
keysf = ['../data/k_50.txt', '../data/k_500.txt', '../data/k_5000.txt']
p = ['mt', 'ep']
for pr in p:
    for s in size:
        keys, rem_keys = get_keys(keysf[size.index(s)])
        for rate in rates:
            for rw in ratio:
                out = '../data/tbs/%s_%s_%s_%s'%(pr, rw, rate, s)
                out_f = '../data/res/%s_%s_%s_%s'%(pr, rw, rate, s)
                streams, n = make_request_streams(rate, rw, s, keys, rem_keys)
                p_args = [[[h, p, out_f], stream] for stream in streams] 
                pool()
                print("DONE %s"%out)
