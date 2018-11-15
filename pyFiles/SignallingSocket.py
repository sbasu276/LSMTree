#!/usr/bin/env python3

import socket, asyncio, types

class SignallingSocket:

  def __init__(self, host, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((host, port))
    sock.listen()
    sock.setblocking(False)
    loop = asyncio.get_event_loop()
    

  def accept_connection(self, sock):
    conn, addr = sock.accept()  # Should be ready to read
    data = types.SimpleNamespace(addr=addr, req=b'', resp=b'')
    print('accepted connection from', addr)
    conn.setblocking(False)

  def service_connection(self, sock):
    recv_data = sock.recv(1024)  # Should be ready to read??
    if recv_data:
      data.req += recv_data
      if(data.req.decode().endswith('\n')):					#"\n" indicated end of request
#        print("Received a request from :", data.addr, data.req)
        new_request_list.append((sock, data.req))
    else:																						#empty message is close request
      print('closing connection to', data.addr)
      sock.close()
  return new_request_list


  def register_signal_handler(self, signum, callback):
		set_signal_handler(signum, callback)
