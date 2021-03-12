import socket
import struct
from pika.frame import ProtocolHeader
from pika import data

HOSTNAME = 'localhost'
DEFAULT_PORT = 6000
MAX_BYTES = 4096
str_or_bytes = (str, bytes)
unicode_type = str

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

def encode_table(pieces, table):
    table = table or {}
    length_index = len(pieces)
    pieces.append(None)
    tablesize = 0
    for(key,value) in table.items():
        tablesize += data.encode_short_string(pieces, key)
        tablesize += data.encode_value(pieces, value)
    pieces[length_index] = struct.pack('>I', tablesize)
    return tablesize + 4

def start_method_encode(version_major, version_minor, server_properties, mechanism, locales):
    pieces = list()
    pieces.append(struct.pack('B', version_major))
    pieces.append(struct.pack('B', version_minor))
    data.encode_table(pieces, server_properties)
    assert isinstance(mechanism, str_or_bytes),\
            'A non-string value was supplied for self.mechanism'
    value = mechanism.encode('utf-8') if isinstance(mechanism, unicode_type) else mechanism
    pieces.append(struct.pack('>I'),len(value))
    pieces.append(value)
    assert isinstance(locales, str_or_bytes),\
            'A non-string value was supplied for self.locales'
    value = locales.encode('utf-8') if isinstance(locales, unicode_type) else locales
    pieces.append(struct.pack('>I', len(value)))
    pieces.append(value)
    return pieces

sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)

server_address = (HOSTNAME, DEFAULT_PORT)
sock.bind(server_address)

print("server opened socket connection")
sock.listen(1)

client_sock, client_address = sock.accept()

print("Server connected by", client_address)
data = client_sock.recv(MAX_BYTES, 0)
print(data)

# PH je objekat klase ProtocolHeader, byte_rec je koliko bajtova je primio
byte_rec, PH = decode(data)

pieces = start_method_encode(0, 9, None, 'PLAIN', 'en_US')