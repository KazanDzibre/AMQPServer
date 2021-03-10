import socket
import sys

HOSTNAME = 'localhost'
DEFAULT_PORT = 6000
MAX_BYTES = 4096

sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)

server_address = (HOSTNAME, DEFAULT_PORT)
sock.bind(server_address)

print("server opened socket connection")
sock.listen(1)

connection, client_address = sock.accept()

print("Server connected by", client_address)
data = connection.recv(MAX_BYTES, 0)
print(data)

