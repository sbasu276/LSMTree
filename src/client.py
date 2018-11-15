from time import sleep
import sys
import socket

class Client:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def send_data(self, message):
        try:
            self.sock.connect(server_address)
            # Send data
            self.sock.send(message)
            data = self.sock.recv(1024)
            print(data.decode('utf-8'))
        finally:
            self.sock.close()

if __name__ == "__main__":
    # Connect the socket to the port where the server is listening
    host = sys.argv[1]
    port = int(sys.argv[2])
    msg = sys.argv[2]
    server_address = ('localhost', port)

    client = Client(server_address, port)
    client.send_data(msg.encode('utf-8'))
    #client.send_data("GET Rafael\n".encode('utf-8'))
    #client.send_data("DELETE Soumen\n".encode('utf-8'))
    #client.send_data("INSERT A 1\n".encode('utf-8'))
    #client.send_data("PUT A 3\n".encode('utf-8'))
    #client.send_data("PUT B 2\n".encode('utf-8'))

