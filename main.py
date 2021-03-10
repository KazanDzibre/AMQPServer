import socket
import struct
from pika.frame import ProtocolHeader
from pika.connection import Connection

def decode(data):
    # Look to see if it's a protocol header frame
    try:
        if data[0:4] == b'AMQP':
            major, minor, revision = struct.unpack_from('BBB', data, 5)
            return 8, ProtocolHeader(major, minor, revision)
    except (IndexError, struct.error):
        return 0, None
    # Get the Frame Type, channel Number and Frame Size
    try:
        (frame_type, channel_number, frame_size) = struct.unpack('>BHL',data[0:7])
    except struct.error:
        return 0, None


HOSTNAME = 'localhost'
DEFAULT_PORT = 6000
MAX_BYTES = 4096

sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)

server_address = (HOSTNAME, DEFAULT_PORT)
sock.bind(server_address)

print("server opened socket connection")
sock.listen(1)

client_sock, client_address = sock.accept()

print("Server connected by", client_address)
data = client_sock.recv(MAX_BYTES, 0)
print(data)

byte_rec, PH = decode(data)

Connection._send_connection_start()