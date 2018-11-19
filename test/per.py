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

size = [200, 800]
ratio = [0.9, 0.1]
for s in size:
    for rw in ratio:
        out = '../data/res/%s_%s'%(s, rw)
        x = []
        with open(out, 'r') as f:
            for line in f.readlines():
                r, t = line.split()
                x.append([r,float(t)])
        x_g = [e[1] for e in x if e[0]=='GET']
        x_p = [e[1] for e in x if e[0]=='PUT']

        g_50 = np.percentile(np.array(x_g), 50)*1000
        g_95 = np.percentile(np.array(x_g), 95)*1000
        p_50 = np.percentile(np.array(x_p), 50)*1000
        p_95 = np.percentile(np.array(x_p), 95)*1000

        fname = '../data/res/lat_%s_%s'%(s, rw)
        with open(fname, 'w') as f:
            f.write(str(g_50)+" "+str(g_95)+" "+str(p_50)+" "+str(p_95)+"\n")
        print("%s_%s"%(s,rw))
        print(str(g_50)+" "+str(g_95)+" "+str(p_50)+" "+str(p_95))

