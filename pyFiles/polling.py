import socket, selectors, types

class PollingSocket:
    def __init__(self, host, port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((host, port))
        self.sock.listen()
        self.sock.setblocking(False)
        self.sel = selectors.DefaultSelector()
        events = selectors.EVENT_READ 
        self.sel.register(self.sock, events, data=None)
        self.sock_data_map = {}

    def accept_connection(self, sock):
        conn, addr = sock.accept()  # Should be ready to read
        #print('accepted connection from', addr)
        conn.setblocking(False)
        #TODO:change data to mutable key value pair
        data = types.SimpleNamespace(addr=addr, req=b'', resp=b'')
        self.sock_data_map[conn] = data
        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        self.sel.register(conn, events, data=data)

    def service_connection(self, info, mask):
        sock = info.fileobj
        data = self.sock_data_map[sock]
        new_request = None
        if mask & selectors.EVENT_READ:
            # Read event has occured in socket
            recv_data = sock.recv(4096)  # Should be ready to read
            if recv_data: #TODO: recvall??
                data.req += recv_data
                if '\n' in data.req.decode():
                    #print("Received a request from :", data.addr, data.req)
                    new_request = (sock, data.req)
                    data.req = b''
                self.sock_data_map[sock] = data

            else:
                #print('closing connection to', data.addr)
                self.sel.unregister(sock)
                sock.close()
                new_request = None
        else:
            # Write event has occured in socket
            if len(data.resp):
                sent = sock.send(data.resp.encode('utf-8'))
                data.resp = data.resp[sent:]
                self.sock_data_map[sock] = data

        return new_request

    def poll_connection(self):
        requests = []
        events = self.sel.select(timeout=0)
        for info, mask in events:
            if info.data is None:  
                #empty data object if connection is not established already
                self.accept_connection(info.fileobj)
            else:
                request = self.service_connection(info, mask)
                if request is not None:
                    requests.append(request)
        return requests


