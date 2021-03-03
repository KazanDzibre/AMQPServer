import socket
import sys

sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM, 0)

server_address = ('localhost', 10000)
sock.bind(server_address)

sock.listen(1)

while True:
    # Wait for connection
    connection, client_address = sock.accept()

    try:

        # Receive the data in small chunks and retransmit it
        while True:
            data = connection.recv(64)
            print(data)
            if data:
                connection.sendall(data)
            else:
                break

    finally:
        # Clean up the connection
        connection.close()