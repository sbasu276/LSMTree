import signal, os, time, socket, asyncio

host = 'localhost'
port = 12345

def listen_handler(sock):
    print("Invoking listerner handler")
    csock = sock.accept()
    csockfd = csock[0]
    csockfd.setblocking(False)
    print("Connected to:", csock)
    loop.add_reader(csockfd, connect_handler, csockfd)

def connect_handler(sock):
    print("Invoking connect handler")
    data = sock.recv(4096)
    if(data==b''):
        print('Closing connection socket!')
        sock.close()
        loop.remove_reader(sock)
    else:
        print("Received data:",data)
        sock.send(b'Hai!')

async def main():
  while True:
    print ('Waiting...')
    time.sleep(3)

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.bind((host, port))
sock.listen()
sock.setblocking(False)

loop = asyncio.get_event_loop()
task = loop.add_reader(sock, listen_handler, sock)
print(task)
loop.run_until_complete(main())
#loop.run_forever()
