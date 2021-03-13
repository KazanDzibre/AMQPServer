# Sredi ovo kad bude radilo
import socket
import struct

from pika.compat import byte, as_bytes
from pika import exceptions
from pika.frame import ProtocolHeader
from pika import data
from pika.adapters.base_connection import BaseConnection
from pika import spec
from pika.frame import Method, Header


HOSTNAME = 'localhost'
DEFAULT_PORT = 6000
MAX_BYTES = 4096
CHANNEL_MAX = 2047
FRAME_MAX = 131072
HEARTBEAT = 60
str_or_bytes = (str, bytes)
unicode_type = str


def encode_short_string(pieces, value):
    encoded_value = as_bytes(value)
    length = len(encoded_value)

    if length > 255:
        raise exceptions.ShortStringTooLong(encoded_value)

    pieces.append(struct.pack('B', length))
    pieces.append(encoded_value)
    return 1 + length

def decode(data_in):
    # Look to see if it's a protocol header frame
    try:
        if data_in[0:4] == b'AMQP':
            major, minor, revision = struct.unpack_from('BBB', data_in, 5)
            return 8, ProtocolHeader(major, minor, revision)
    except (IndexError, struct.error):
        return 0, None
    # Get the Frame Type, channel Number and Frame Size
    try:
        (frame_type, channel_number, frame_size) = struct.unpack('>BHL', data_in[0:7])
    except struct.error:
        return 0, None

    #Get the frame data
    frame_end = spec.FRAME_HEADER_SIZE + frame_size + spec.FRAME_END_SIZE

    # We don't have all off the frame yet
    if frame_end > len(data_in):
        return 0, None

    # Get the raw frame data
    frame_data = data_in[spec.FRAME_HEADER_SIZE:frame_end - 1]
    if frame_type == spec.FRAME_METHOD:

        method_id = struct.unpack_from('>I', frame_data)[0]
        method = spec.methods[method_id]()
        method.decode(frame_data, 4)

        return frame_end, Method(channel_number, method)

    elif frame_type == spec.FRAME_HEADER:

        class_id, weight, body_size = struct.unpack_from('>HHQ', frame_data)

        properties = spec.props[class_id]()

        out = properties.decode(frame_data[12:])

        return frame_end, Header(channel_number, body_size, properties)


def encode_table(pieces, table):
    table = table or {}
    length_index = len(pieces)
    pieces.append(None)
    table_size = 0
    for(key, value) in table.items():
        table_size += data.encode_short_string(pieces, key)
        table_size += data.encode_value(pieces, value)
    pieces[length_index] = struct.pack('>I', table_size)
    return table_size + 4


def start_method_encode(version_major, version_minor, server_properties, mechanism, locales):
    pieces = list()
    pieces.append(struct.pack('B', version_major))
    pieces.append(struct.pack('B', version_minor))
    encode_table(pieces, server_properties)
    assert isinstance(mechanism, str_or_bytes),\
            'A non-string value was supplied for self.mechanism'
    value = mechanism.encode('utf-8') if isinstance(mechanism, unicode_type) else mechanism
    pieces.append(struct.pack('>I', len(value)))
    pieces.append(value)
    assert isinstance(locales, str_or_bytes),\
            'A non-string value was supplied for self.locales'
    value = locales.encode('utf-8') if isinstance(locales, unicode_type) else locales
    pieces.append(struct.pack('>I', len(value)))
    pieces.append(value)
    return pieces

def tune_method_encode(channel_max, frame_max, heartbeat):
    pieces = list()
    pieces.append(struct.pack('>H', channel_max))
    pieces.append(struct.pack('>I', frame_max))
    pieces.append(struct.pack('>H', heartbeat))
    return pieces


def openOk_method_encode(known_hosts):
    pieces = list()
    assert isinstance(known_hosts, str_or_bytes),\
        'A non-string value was supplied for known_hosts'
    length = encode_short_string(pieces, known_hosts)
    return pieces

def channel_openOk_method_encode(channel_id=''):
    pieces = list()
    assert isinstance(channel_id, str_or_bytes),\
        'A non-string value was supplied for channel_id'
    value = channel_id.encode('utf-8') if isinstance(channel_id, unicode_type) else channel_id
    pieces.append(struct.pack('>I', len(value)))
    pieces.append(value)
    return pieces


def marshal(pieces,INDEX):
    pieces.insert(0, struct.pack('>I', INDEX))
    payload = b''.join(pieces)
    return struct.pack('>BHI', 1, 0, len(payload)) + payload + byte(spec.FRAME_END) #frame_type = 1 channel_number = 0


sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)

server_address = (HOSTNAME, DEFAULT_PORT)
sock.bind(server_address)

print("server opened socket connection")
sock.listen(1)

client_sock, client_address = sock.accept()

print("Server connected by", client_address)
data = client_sock.recv(MAX_BYTES, 0)

# PH je objekat klase ProtocolHeader, byte_rec je koliko bajtova je primio
byte_rec, PH = decode(data)

pieces = start_method_encode(0, 9, None, 'PLAIN', 'en_US')

marshaled_frames = marshal(pieces, 0x000A000A)

#_output_marshaled_frames(marshaled_frames)

client_sock.send(marshaled_frames)

data = client_sock.recv(MAX_BYTES, 0)

fe, method = decode(data)

print(method.method.NAME)

pieces = tune_method_encode(CHANNEL_MAX, FRAME_MAX, HEARTBEAT)

marshaled_frames = marshal(pieces, 0x000A001E)

client_sock.send(marshaled_frames)

data = client_sock.recv(MAX_BYTES, 0)

tu, method = decode(data)

print(method.method.NAME)

data = client_sock.recv(MAX_BYTES, 0)

op, method = decode(data)

print(method.method.NAME)

pieces = openOk_method_encode('')

marshaled_frames = marshal(pieces, 0x000A0029)

client_sock.send(marshaled_frames)

data = client_sock.recv(MAX_BYTES, 0)

op_ok, method = decode(data)

print(method.method.NAME)

pieces = channel_openOk_method_encode('')

marshaled_frames = marshal(pieces, 0x0014000B)

client_sock.send(marshaled_frames)



