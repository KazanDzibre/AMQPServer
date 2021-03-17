# Sredi ovo kad bude radilo
import socket
import struct
import _thread

from pika.compat import byte, as_bytes
from pika import spec
from pika.spec import Channel, Connection
from pika.frame import decode_frame

from pika.connection import Parameters

HOSTNAME = 'localhost'
MAX_BYTES = 4096
CHANNEL_MAX = 2047
FRAME_MAX = 131072
HEARTBEAT = 60
str_or_bytes = (str, bytes)
unicode_type = str



def marshal(pieces,INDEX):
    pieces.insert(0, struct.pack('>I', INDEX))
    payload = b''.join(pieces)
    return struct.pack('>BHI', 1, 0, len(payload)) + payload + byte(spec.FRAME_END) #frame_type = 1 channel_number = 0


serverParameters = Parameters()

sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)

server_address = (HOSTNAME, serverParameters.DEFAULT_PORT)
sock.bind(server_address)

print("server opened socket connection")
sock.listen(1)

client_sock, client_address = sock.accept()

print("Server connected by", client_address)

#prvo primi verziju protokola
"Ovde prima verziju protokola"
data_in = client_sock.recv(MAX_BYTES, 0)
byte_rec, PH = decode_frame(data_in)
frame_type = PH.frame_type
major = PH.major
minor = PH.minor
revision = PH.revision

connection = Connection()
Start = connection.Start(major, minor, None, 'PLAIN', 'en_US')

"Slanje Start metode"
pieces = Start.encode()
marshaled_frames = marshal(pieces, Start.INDEX)
client_sock.send(marshaled_frames)
"Primi Start-Ok"
data_in = client_sock.recv(MAX_BYTES, 0)
"Ovde imam objekat StartOk"
fe, StartOk_method = decode_frame(data_in)

"Slanje Tune metode"
Tune = connection.Tune(CHANNEL_MAX, FRAME_MAX, HEARTBEAT)
pieces = Tune.encode()
marshaled_frames = marshal(pieces, Tune.INDEX)
client_sock.send(marshaled_frames)
"Primi TuneOK"
data_in = client_sock.recv(MAX_BYTES, 0)
tu, TuneOK_method = decode_frame(data_in)

"Primi OpenConnection"
data_in = client_sock.recv(MAX_BYTES, 0)
op, OpenConnection_method = decode_frame(data_in)

OpenOk = connection.OpenOk('')
pieces = OpenOk.encode()
marshaled_frames = marshal(pieces, OpenOk.INDEX)
client_sock.send(marshaled_frames)

"Ovde stigne channel_open, sad treba da se napravi taj channel"
data_in = client_sock.recv(MAX_BYTES, 0)
op_ok, method = decode_frame(data_in)

" ###  TO DO: Napravi zapravo neki kanal ovde! "
#Connection.channel(1)

channel = Channel()
OpenOk = channel.OpenOk()
pieces = OpenOk.encode()
marshaled_frames = marshal(pieces, OpenOk.INDEX)
client_sock.send(marshaled_frames)
data_in = client_sock.recv(MAX_BYTES, 0)
qd, heartbeat = decode_frame(data_in)
