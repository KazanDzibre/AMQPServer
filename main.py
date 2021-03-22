import socket
import _thread

from Amqp_components import AmqpQueue, Globals
from pika import spec
from pika.spec import Channel, Queue, Basic, Connection
from pika.frame import decode_frame, Method, Body

from pika.connection import Parameters

HOSTNAME = 'localhost'
MAX_BYTES = 4096
CHANNEL_MAX = 2047
FRAME_MAX = 131072
HEARTBEAT = 60


def send_tune_method():
    "Slanje Tune metode"
    tune = Connection.Tune(CHANNEL_MAX, FRAME_MAX, HEARTBEAT)
    method = Method(0, tune)
    marshaled_frames = method.marshal()
    print('Poslao Tune')
    client_sock.send(marshaled_frames)


def send_open_ok_method():
    openOk = Connection.OpenOk('')
    method = Method(0, openOk)
    marshaled_frames = method.marshal()
    print('Poslao open_ok')
    client_sock.send(marshaled_frames)


def send_channel_open_ok_method():
    channelOpenOk = Channel.OpenOk()
    method = Method(1, channelOpenOk)
    marshaled_frames = method.marshal()
    print('Poslao channel open ok')
    client_sock.send(marshaled_frames)


def send_queue_declare_ok_method(method):
    queue = AmqpQueue(method.method.queue)
    queueDeclareOk = Queue.DeclareOk(method.method.queue, 0, 0)
    method = Method(1, queueDeclareOk)
    marshaled_frames = method.marshal()
    print('Poslao queue declare ok')
    client_sock.send(marshaled_frames)


def decode_message(data_in):
    message = decode_frame(data_in)
    Globals.queue_dict


def decode_basic_publish(method):
    print('uso sam ovde')
    Globals.routing_key = method.method.routing_key

def switch(method, message):
    if message != 'nothing':
        return decode_message(message)
    if method.method.NAME == Connection.StartOk.NAME:
        return send_tune_method()
    elif method.method.NAME == Connection.Open.NAME:
        return send_open_ok_method()
    elif method.method.NAME == Channel.Open.NAME:
        return send_channel_open_ok_method()
    elif method.method.NAME == Queue.Declare.NAME:
        queue = AmqpQueue(method.method.queue)
        Globals.queue_dict = {method.method.queue: queue}
        return send_queue_declare_ok_method(method)
    elif method.method.NAME == Basic.Publish.NAME:
        return decode_basic_publish(method)
    else:
        return 1


"Init socket"
serverParameters = Parameters()
sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
server_address = (HOSTNAME, serverParameters.DEFAULT_PORT)
sock.bind(server_address)
print("Server opened socket connection")
sock.listen(1)
client_sock, client_address = sock.accept()
print("Server connected by", client_address)

"Ovde prima verziju protokola"
data_in = client_sock.recv(MAX_BYTES, 0)
byte_rec, PH = decode_frame(data_in)
frame_type = PH.frame_type
major = PH.major
minor = PH.minor
revision = PH.revision

#connectionSpec = spec.Connection()
Start = Connection.Start(major, minor, None, 'PLAIN', 'en_US')
method = Method(0, Start)
"Slanje Start metode"
marshaled_frames = method.marshal()
client_sock.send(marshaled_frames)

while True:
    data_in = client_sock.recv(MAX_BYTES, 0)
    byte_rec, method, message = decode_frame(data_in)
    switch(method, message)

" ###  TO DO: Napravi zapravo neki kanal ovde! "
#Connection.channel(1)


data_in = client_sock.recv(MAX_BYTES, 0)
published_message, Basic_publish = decode_frame(data_in)

print(Basic_publish.method.NAME)

data_in = client_sock.recv(MAX_BYTES, 0)
frame_end1, Header, data_in = decode_frame(data_in)
frame_end2, message_body, data_in = decode_frame(data_in)

if message_body.NAME == 'Body':
    queue.append_to_queue(message_body.fragment)
    queue.append_to_queue('nesto drugo')
    queue.print_queue()

data_in = client_sock.recv(MAX_BYTES, 0)
frame_end3, Close = decode_frame(data_in)

if Close.method.NAME == Channel.Close.NAME:
    ChannelCloseOk = Channel.CloseOk()
    method = Method(1, ChannelCloseOk)
    marshaled_frames = method.marshal()
    client_sock.send(marshaled_frames)

client_sock.close()