import asyncio, socket

async def handle_client(client):
    request = None
    while request != 'quit':
        request = (await loop.sock_recv(client, 255)).decode('utf8')
        response = str((request)) + '\n'
        await loop.sock_sendall(client, response.encode('utf8'))
    client.close()

async def run_server():
    while True:
        client, _ = await loop.sock_accept(server)
        loop.create_task(handle_client(client))

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(('localhost', 15555))
server.listen(8)
server.setblocking(False)

loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(run_server())
except KeyboardInterrupt:
    server.close()

