#!/usr/bin/env python3

import socket, time, sys

HOST = '127.0.0.1'  # The server's hostname or IP address
PORT = int(sys.argv[1])        # The port used by the server
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
  s.connect((HOST, PORT))
#  while 1:
  if 1:
    s.sendall(b'Hello, world\n')
    data = s.recv(1024)
    print('Received', repr(data))
    time.sleep(1)
