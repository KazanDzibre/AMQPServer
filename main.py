import socket

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

sock.bind(('localhost',5671))

sock.listen()

c = sock.accept()
