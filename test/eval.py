import json
import sys
import multiprocessing.dummy as mp
import os
import time
import math
import random
from time import sleep
import string
import subprocess
import threading
from client import Client

#Note: This run_client() has been influenced by the code of Soumen's research project
def run_client(config):
    host = config[0][0]
    port = config[0][1]
    fname = config[0][2]
    #print(fname)
    stream = config[1]
    for msg in stream:
        req_type = msg.split()[0]
        c = Client(host, int(port))
        start = time.time()
        c.send_data(msg.encode('utf-8'))
        latency = time.time() - start
        c.sock.close()
        with open(fname, 'a') as f:
            f.write(req_type+' '+str(latency)+'\n')
        #print("In handler")
        sleep(0.5)

def get_args(tb):
    stream = []
    with open(tb, "r") as f:
        for line in f.readlines():
            stream.append(line.strip('\n'))
    s = 0
    streams = []
    for i in range(1,300):
        streams.append(stream[s:i*20])
        s = i*20
    return streams

h = "127.0.0.1"
p = "65211"
import sys
ratio = [float(sys.argv[1])]
size = sys.argv[2]
for rr in ratio:
    tb = "../data/tbs/%s"%rr
    res = "../data/res/%s_s"%(size,rr)
    streams = get_args(tb)
    pool_args = [[[h, p, res], stream] for stream in streams]
    with mp.Pool(300) as pool:
        pool.map(run_client, pool_args)
    print("DONE %s"%rr)
