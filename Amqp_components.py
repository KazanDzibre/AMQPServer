import socket
import threading

from pika.spec import Channel, Queue, Basic, Connection
from pika.frame import decode_frame, Method
from pika.exchange_type import ExchangeType
from pika.connection import Parameters
from pika import exceptions

HOSTNAME = 'localhost'
MAX_BYTES = 4096
CHANNEL_MAX = 2047
FRAME_MAX = 131072
HEARTBEAT = 60
MAJOR = 0
MINOR = 9

serverParameters = Parameters()

_channels = []
_exchange_num = 0
_exchange_array = []
_queue_num = 0
_queue_array = []
_routing_key = ''


def get_next_channel_number():
    limit = CHANNEL_MAX
    if len(_channels) >= limit:
        raise exceptions.NoFreeChannels()

    for num in range(1, len(_channels)):
        if num not in _channels:
            return num
    return len(_channels) + 1


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


class Utility:

    client_sock = None
    consumer_tag = []

    def __init__(self):
        """declaring global variables on start"""
        global _queue_array
        global _queue_num
        global _routing_key
        global _exchange_num
        global _exchange_array
        sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        server_address = (HOSTNAME, serverParameters.DEFAULT_PORT)
        sock.bind(server_address)
        print("Server opened socket connection")
        sock.listen(1)
        default_exchange = AmqpExchange('', ExchangeType.fanout)
        _exchange_array.append(default_exchange)
        _exchange_num += 1
        print("Default exchange created")
        while True:
            self.client_sock, client_address = sock.accept()
            x = threading.Thread(target=self.init_protocol(), args=(1,))    #utility.init_protocol(utility)
            x.start()

    def init_protocol(self):
        self.receive_protocol_version()
        self.send_start_ok_method()
        self.handler()

    def receive_protocol_version(self):
        data_in = self.client_sock.recv(MAX_BYTES, 0)
        byte_rec, ph = decode_frame(data_in)

    def send_start_ok_method(self):
        start = Connection.Start(MAJOR, MINOR, None, 'PLAIN', 'en_US')
        method = Method(0, start)
        marshaled_frames = method.marshal()
        self.client_sock.send(marshaled_frames)

    def send_tune_method(self):
        tune = Connection.Tune(CHANNEL_MAX, FRAME_MAX, HEARTBEAT)
        method = Method(0, tune)
        marshaled_frames = method.marshal()
        self.client_sock.send(marshaled_frames)

    def send_open_ok_method(self):
        open_ok = Connection.OpenOk('')
        method = Method(0, open_ok)
        marshaled_frames = method.marshal()
        self.client_sock.send(marshaled_frames)

    def send_channel_open_ok_method(self):
        channel_open_ok = Channel.OpenOk()
        channel_number = get_next_channel_number()
        _channels.append(channel_number)
        method = Method(channel_number, channel_open_ok)
        marshaled_frames = method.marshal()
        self.client_sock.send(marshaled_frames)

    def send_queue_declare_ok_method(self, method):
        queue = AmqpQueue(method.method.queue)
        queue_declare_ok = Queue.DeclareOk(method.method.queue, 0, 0)
        method = Method(1, queue_declare_ok)
        marshaled_frames = method.marshal()
        self.client_sock.send(marshaled_frames)

    def send_channel_close_ok_method(self):
        channel_close_ok = Channel.CloseOk()
        method = Method(1, channel_close_ok)                       #nemoj da zaboravis da ovde posle menjas da zavisi od kanala na kom salje
        marshaled_frames = method.marshal()
        self.client_sock.send(marshaled_frames)

    def send_connection_close_ok(self):
        connection_close_ok = Connection.CloseOk()
        method = Method(0, connection_close_ok)
        marshalled_frames = method.marshal()
        self.client_sock.send(marshalled_frames)

    def send_basic_qos_ok_method(self):
        basic_qos = Basic.QosOk()
        method = Method(1, basic_qos)
        marshaled_frames = method.marshal()
        self.client_sock.send(marshaled_frames)

    def send_basic_consume_ok_method(self):
        basic_consume_ok = Basic.ConsumeOk(self.consumer_tag[0])
        method = Method(1, basic_consume_ok)
        marshaled_frames = method.marshal()
        self.client_sock.send(marshaled_frames)
        self.basic_deliver_method()

    def basic_deliver_method(self):
        basic_deliver = Basic.Deliver(self.consumer_tag[0], 178, False,
                                      'default_exchange', 'task_queue')
        method = Method(1, basic_deliver)
        marshaled_frames = method.marshal()
        self.client_sock.send(marshaled_frames)

    def handler(self):
        while True:
            data_in = self.client_sock.recv(MAX_BYTES, 0)
            if data_in == b'':
                break
            byte_received, method, message = decode_frame(data_in)
            self.switch(method, message)

    @staticmethod
    def decode_message(data_in):
        message = decode_frame(data_in)
        #Globals.queue_dict

    @staticmethod
    def decode_basic_publish(method):
        print("nista za sad")
        #Globals.routing_key = method.method.routing_key

    def switch(self, method, message):
        if message != 'nothing':
            return Utility.decode_message(message)
        if method.method.NAME == Connection.StartOk.NAME:
            return self.send_tune_method()
        elif method.method.NAME == Connection.Open.NAME:
            return self.send_open_ok_method()
        elif method.method.NAME == Channel.Open.NAME:
            return self.send_channel_open_ok_method()
        elif method.method.NAME == Queue.Declare.NAME:
            _queue_array.append(AmqpQueue(method.method.queue))
            return self.send_queue_declare_ok_method(method)
        elif method.method.NAME == Basic.Publish.NAME:
            return self.decode_basic_publish(method)
        elif method.method.NAME == Channel.Close.NAME:
            return self.send_channel_close_ok_method()
        elif method.method.NAME == Connection.Close.NAME:
            return self.send_connection_close_ok()
        elif method.method.NAME == Basic.Qos.NAME:
            return self.send_basic_qos_ok_method()
        elif method.method.NAME == Basic.Consume.NAME:
            self.consumer_tag.append(method.method.consumer_tag)
            return self.send_basic_consume_ok_method()
        else:
            return 1

