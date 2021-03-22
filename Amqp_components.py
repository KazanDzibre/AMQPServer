import socket

from pika.spec import Channel, Queue, Basic, Connection
from pika.frame import decode_frame, Method

from pika.connection import Parameters

HOSTNAME = 'localhost'
MAX_BYTES = 4096
CHANNEL_MAX = 2047
FRAME_MAX = 131072
HEARTBEAT = 60
MAJOR = 0
MINOR = 9

serverParameters = Parameters()


class Globals:
    queue_num = 0
    exchange_num = 0
    queue_dict = {}
    exchange_dict = {}
    routing_key = ''

    def add_queue(self, queue):
        self.queue_dict = {queue.queue, queue}

class AmqpQueue:
    queue = []

    def __init__(self, name=''):
        self.name = name
        # self.auto_delete = auto_delete
        # self.durable = durable
        # self.exclusive = exclusive
        # self.synchronous = synchronous

    def append_to_queue(self, message):
        self.queue.append(message)

    def pop_from_queue(self):
        return self.queue.pop()

    def print_queue(self):
        print(self.queue)


class AmqpExchange:
    exchange = ''
    exchange_type = None

    def __init__(self, exchange, exchange_type):
        self.exchange = exchange
        self.exchange_type = exchange_type
        Globals.exchange_dict = {exchange: self}


class Utility:

    client_sock = None

    def __init__(self):
        sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        server_address = (HOSTNAME, serverParameters.DEFAULT_PORT)
        sock.bind(server_address)
        print("Server opened socket connection")
        sock.listen(1)
        self.client_sock, client_address = sock.accept()
        print("Server connected by", client_address)

    def receive_protocol_version(self):
        "Primi verziju protokola"
        data_in = self.client_sock.recv(MAX_BYTES, 0)
        byte_rec, PH = decode_frame(data_in)
        MAJOR = PH.major
        MINOR = PH.minor

    def send_start_ok_method(self):
        Start = Connection.Start(MAJOR, MINOR, None, 'PLAIN', 'en_US')
        method = Method(0, Start)
        "Slanje Start metode"
        marshaled_frames = method.marshal()
        self.client_sock.send(marshaled_frames)

    def send_tune_method(self):
        "Slanje Tune metode"
        tune = Connection.Tune(CHANNEL_MAX, FRAME_MAX, HEARTBEAT)
        method = Method(0, tune)
        marshaled_frames = method.marshal()
        self.client_sock.send(marshaled_frames)

    def send_open_ok_method(self):
        openOk = Connection.OpenOk('')
        method = Method(0, openOk)
        marshaled_frames = method.marshal()
        self.client_sock.send(marshaled_frames)

    def send_channel_open_ok_method(self):
        channelOpenOk = Channel.OpenOk()
        method = Method(1, channelOpenOk)
        marshaled_frames = method.marshal()
        self.client_sock.send(marshaled_frames)

    def send_queue_declare_ok_method(self, method):
        queue = AmqpQueue(method.method.queue)
        queueDeclareOk = Queue.DeclareOk(method.method.queue, 0, 0)
        method = Method(1, queueDeclareOk)
        marshaled_frames = method.marshal()
        self.client_sock.send(marshaled_frames)

    def send_channel_close_ok_method(self):
        ChannelCloseOk = Channel.CloseOk()
        method = Method(1, ChannelCloseOk)                       #nemoj da zaboravis da ovde posle menjas da zavisi od kanala na kom salje
        marshaled_frames = method.marshal()
        self.client_sock.send(marshaled_frames)

    @staticmethod
    def decode_message(data_in):
        message = decode_frame(data_in)
        Globals.queue_dict

    @staticmethod
    def decode_basic_publish(method):
        Globals.routing_key = method.method.routing_key

    def switch(self, method, message):
        if message != 'nothing':
            return Utility.decode_message(message)
        if method.method.NAME == Connection.Start.NAME:
            return self.send_start_ok_method()
        elif method.method.NAME == Connection.StartOk.NAME:
            return self.send_tune_method()
        elif method.method.NAME == Connection.Open.NAME:
            return self.send_open_ok_method()
        elif method.method.NAME == Channel.Open.NAME:
            return self.send_channel_open_ok_method()
        elif method.method.NAME == Queue.Declare.NAME:
            queue = AmqpQueue(method.method.queue)
            Globals.queue_dict = {method.method.queue: queue}
            return self.send_queue_declare_ok_method(method)
        elif method.method.NAME == Basic.Publish.NAME:
            return self.decode_basic_publish(method)
        elif method.method.NAME == Channel.Close.NAME:
            return self.send_channel_close_ok_method()
        else:
            return 1


