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
import numpy as np

size = [100, 1000, 10000]
rates = [60, 300, 600]
ratio = [0.9, 0.5, 0.1]
keysf = ['../data/k_50.txt', '../data/k_500.txt', '../data/k_5000.txt']
p = ['mt', 'ep']
for pr in p:
    for s in size:
        for rate in rates:
            for rw in ratio:
                out = '../data/res/%s_%s_%s_%s'%(pr, rw, rate, s)
                x = []
                with open(out, 'r') as f:
                    for line in f.readlines():
                        r, t = line.split()
                        x.append([r,float(t)])
                x_g = [e[1] for e in x if e[0]=='GET']
                x_p = [e[1] for e in x if e[0]=='PUT']

                g_50 = np.percentile(np.array(x_g), 50)
                g_95 = np.percentile(np.array(x_g), 95)
                p_50 = np.percentile(np.array(x_p), 50)
                p_95 = np.percentile(np.array(x_p), 95)

                fname = '../data/res/lat_%s_%s_%s_%s'%(pr, rw, rate, s)
                with open(fname, 'w') as f:
                    f.write(str(g_50)+" "+str(g_95)+" "+str(p_50)+" "+str(p_95)+"\n")

