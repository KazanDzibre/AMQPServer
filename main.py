import socket
import _thread

from pika import spec
from pika.spec import Channel
from pika.frame import decode_frame, Method

from pika.connection import Parameters

HOSTNAME = 'localhost'
MAX_BYTES = 4096
CHANNEL_MAX = 2047
FRAME_MAX = 131072
HEARTBEAT = 60
str_or_bytes = (str, bytes)
unicode_type = str


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

connectionSpec = spec.Connection()

Start = connectionSpec.Start(major, minor, None, 'PLAIN', 'en_US')
method = Method(0, Start)
"Slanje Start metode"
marshaled_frames = method.marshal()
client_sock.send(marshaled_frames)


"Primi Start-Ok"
data_in = client_sock.recv(MAX_BYTES, 0)
"Ovde imam objekat StartOk"
fe, StartOk_method = decode_frame(data_in)

"Slanje Tune metode"
Tune = connectionSpec.Tune(CHANNEL_MAX, FRAME_MAX, HEARTBEAT)
method = Method(0, Tune)
marshaled_frames = method.marshal()
client_sock.send(marshaled_frames)

"Primi TuneOK"
data_in = client_sock.recv(MAX_BYTES, 0)
tu, TuneOK_method = decode_frame(data_in)

"Primi OpenConnection"
data_in = client_sock.recv(MAX_BYTES, 0)
op, OpenConnection_method = decode_frame(data_in)

OpenOk = connectionSpec.OpenOk('')
method = Method(0, OpenOk)
marshaled_frames = method.marshal()
client_sock.send(marshaled_frames)

"Ovde stigne channel_open, sad treba da se napravi taj channel"
data_in = client_sock.recv(MAX_BYTES, 0)
op_ok, method = decode_frame(data_in)

" ###  TO DO: Napravi zapravo neki kanal ovde! "
#Connection.channel(1)

ChannelOpenOk = Channel.OpenOk()
method = Method(1, ChannelOpenOk)
marshaled_frames = method.marshal()
client_sock.send(marshaled_frames)


data_in = client_sock.recv(MAX_BYTES, 0)
qd, queue_declare = decode_frame(data_in)
