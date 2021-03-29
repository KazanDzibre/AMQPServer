import socket
import threading
import struct

from pika import spec
from pika.spec import BasicProperties
from pika.spec import Channel, Queue, Basic, Connection, Exchange
from pika.frame import decode_frame, Method, Header, Body, Heartbeat
from pika.exchange_type import ExchangeType
from pika.connection import Parameters
from pika import exceptions
from pika.compat import byte

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
#_routing_key = ''


def decode_message_from_header(data_in):
    if data_in is not b'':
        frame_type, channel_number, frame_size = struct.unpack('>BHL', data_in[0:7])

    # Get the frame data
    frame_end = spec.FRAME_HEADER_SIZE + frame_size + spec.FRAME_END_SIZE

    # We don't have all of the frame yet
    if frame_end > len(data_in):
        return None

    if data_in[frame_end - 1:frame_end] != byte(spec.FRAME_END):
        raise exceptions.InvalidFrameError("Invalid FRAME_END marker")

    data_in = data_in[frame_end:]

    if data_in is not b'':
        frame_type, channel_number, frame_size = struct.unpack('>BHL', data_in[0:7])

    frame_end = spec.FRAME_HEADER_SIZE + frame_size + spec.FRAME_END_SIZE

    if frame_end > len(data_in):
        return None

    if data_in[frame_end - 1:frame_end] != byte(spec.FRAME_END):
        raise exceptions.InvalidFrameError("Invalid FRAME_END marker")

    frame_data = data_in[spec.FRAME_HEADER_SIZE:frame_end - 1]

    return frame_data


def decode_message_from_body(data_in):
    if data_in is not b'':
        frame_type, channel_number, frame_size = struct.unpack('>BHL', data_in[0:7])

    frame_end = spec.FRAME_HEADER_SIZE + frame_size + spec.FRAME_END_SIZE

    if frame_end > len(data_in):
        return None

    if data_in[frame_end - 1:frame_end] != byte(spec.FRAME_END):
        raise exceptions.InvalidFrameError("Invalid FRAME_END marker")

    frame_data = data_in[spec.FRAME_HEADER_SIZE:frame_end - 1]

    return frame_data


def check_for_existing(array, name):
    for i in array:
        if name == i.name:
            return 0
    return 1


def find_exchange(exchange, array):
    for i in array:
        if i.name == exchange:
            return i
    return None


class AmqpQueue:
    queue = []

    def __init__(self, name=''):
        self.name = name
        # self.auto_delete = auto_delete
        # self.durable = durable
        # self.exclusive = exclusive
        # self.synchronous = synchronous

    def print_queue(self):
        print(self.queue)


class AmqpExchange:
    name = ''
    exchange_type = None
    message_to_publish = ''
    bound_queues = []

    def __init__(self, name, exchange_type):
        self.name = name
        self.exchange_type = exchange_type

    def push_message_to_all_bound_queues(self):                         #Ovo cemo samo da prosirimo za razlicite tipove exchange-a za sad je fanout tako da salje svima
        for i in self.bound_queues:
            i.queue.append(self.message_to_publish)

    def bind_queue(self, queue_to_bind):
        self.bound_queues.append(queue_to_bind)


class Utility:

    client_sock = None
    default_exchange = AmqpExchange('', ExchangeType.fanout)
    consumer_tag = []
    queue_to_consume = ''
    _routing_key = ''
    _exchange_to_publish = ''

    def __init__(self):
        """declaring global variables on start"""
        global _exchange_array
        global _exchange_num
        sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        server_address = (HOSTNAME, serverParameters.DEFAULT_PORT)
        sock.bind(server_address)
        print("Server opened socket connection")
        sock.listen(1)
        _exchange_array.append(self.default_exchange)
        _exchange_num += 1
        print("Default exchange created")
        while True:
            self.client_sock, client_address = sock.accept()
            print("Accepted client ", client_address)
            x = threading.Thread(target=self.init_protocol(), args=(1,))
            x.start()

    def init_protocol(self):
        print("Init protocol...")
        self.receive_protocol_version()
        self.send_start_ok_method()
        self.handler()

    def receive_protocol_version(self):
        print("Received protocol version")
        data_in = self.client_sock.recv(MAX_BYTES, 0)
        byte_rec, ph = decode_frame(data_in)

    def send_start_ok_method(self):
        print("Start method ok...")
        start = Connection.Start(MAJOR, MINOR, None, 'PLAIN', 'en_US')
        method = Method(0, start)
        marshaled_frames = method.marshal()
        self.client_sock.send(marshaled_frames)

    def send_tune_method(self):
        print("Send tune method")
        tune = Connection.Tune(CHANNEL_MAX, FRAME_MAX, HEARTBEAT)
        method = Method(0, tune)
        marshaled_frames = method.marshal()
        self.client_sock.send(marshaled_frames)

    def send_open_ok_method(self):
        print("Send open_ok method")
        open_ok = Connection.OpenOk('')
        method = Method(0, open_ok)
        marshaled_frames = method.marshal()
        self.client_sock.send(marshaled_frames)

    def send_channel_open_ok_method(self, method):
        print("Send channel open ok method")
        global _channels
        channel_open_ok = Channel.OpenOk()
        channel_number = method.channel_number
        _channels.append(channel_number)
        method = Method(channel_number, channel_open_ok)
        marshaled_frames = method.marshal()
        self.client_sock.send(marshaled_frames)

    def send_queue_declare_ok_method(self, method):
        print("Send queue declare ok method")
        queue = AmqpQueue(method.method.queue)
        self.default_exchange.bound_queues.append(queue)                #po defaultu su svi queue-ovi povezani na default_exchange
        queue_declare_ok = Queue.DeclareOk(method.method.queue, 0, 0)
        method = Method(method.channel_number, queue_declare_ok)
        marshaled_frames = method.marshal()
        self.client_sock.send(marshaled_frames)

    def send_exchange_declare_ok(self, method):
        print("Send exchange declare ok method")
        global _exchange_array
        global _exchange_num
        exchange_declare_ok = Exchange.DeclareOk()
        exchange = AmqpExchange(method.method.exchange, method.method.type)
        _exchange_array.append(exchange)
        _exchange_num += 1
        method = Method(method.channel_number, exchange_declare_ok)
        marshaled_frames = method.marshal()
        self.client_sock.send(marshaled_frames)

    def send_channel_close_ok_method(self, close_method):
        print("send channel close ok method")
        channel_close_ok = Channel.CloseOk()
        _channels.remove(close_method.channel_number)
        method = Method(close_method.channel_number, channel_close_ok)
        marshaled_frames = method.marshal()
        self.client_sock.send(marshaled_frames)

    def send_connection_close_ok(self):
        print("Send connection close ok")
        connection_close_ok = Connection.CloseOk()
        method = Method(0, connection_close_ok)
        marshalled_frames = method.marshal()
        self.client_sock.send(marshalled_frames)

    def send_basic_qos_ok_method(self):
        print("send basic qos ok method")
        basic_qos = Basic.QosOk()
        method = Method(1, basic_qos)
        marshaled_frames = method.marshal()
        self.client_sock.send(marshaled_frames)

    def send_basic_consume_ok_method(self):
        print("send basic consume ok method")
        basic_consume_ok = Basic.ConsumeOk(self.consumer_tag[0])
        method = Method(1, basic_consume_ok)
        marshaled_frames = method.marshal()
        self.client_sock.send(marshaled_frames)
        self.basic_deliver_method()

    def basic_deliver_method(self):
        basic_deliver = Basic.Deliver(self.consumer_tag[0], 1, False,
                                      '', self._routing_key)
        method = Method(1, basic_deliver)
        marshaled_frames = method.marshal()
        self.client_sock.send(marshaled_frames)
        self.send_content(self.default_exchange.message_to_publish)

    def send_content(self, message):
        body = Body(1, message)
        marshaled_frames_body = body.marshal()
        basic_properties = BasicProperties()
        header = Header(1, len(self.default_exchange.message_to_publish), basic_properties)             #ovde menjaj posle da bude dinamicno za sve vrste exchange-a ovo je hard kodovano sad
        marshaled_frames_header = header.marshal()
        self.client_sock.send(marshaled_frames_header)
        self.client_sock.send(marshaled_frames_body)

    def handler(self):
        while True:
            data_in = self.client_sock.recv(MAX_BYTES, 0)
            if data_in == b'':
                break
            byte_received, method = decode_frame(data_in)
            if method.NAME == Header.NAME:
                message = decode_message_from_header(data_in)
                exchange = find_exchange(self._exchange_to_publish, _exchange_array)
                if exchange is None:
                    print("There is no exchange with that name")
                else:
                    exchange.message_to_publish = message
            elif method.NAME == Body.NAME:
                message = decode_message_from_body(data_in)
                exchange = find_exchange(self._exchange_to_publish, _exchange_array)
                if exchange is None:
                    print("There is no exchange with that name")
                else:
                    exchange.message_to_publish = message
                    exchange.push_message_to_all_bound_queues()
            elif method.NAME == Heartbeat.NAME:
                print("Usao u heartbeat")                       #pogledaj sta treba da radim kad posalje heartbeat
                break
            else:
                self.switch(method)

    def decode_basic_publish(self, method):
        self._routing_key = method.method.routing_key
        self._exchange_to_publish = method.method.exchange

    def switch(self, method):
        if method.method.NAME == Connection.StartOk.NAME:
            print(_exchange_array[0].message_to_publish)
            return self.send_tune_method()
        elif method.method.NAME == Connection.Open.NAME:
            return self.send_open_ok_method()
        elif method.method.NAME == Channel.Open.NAME:
            return self.send_channel_open_ok_method(method)
        elif method.method.NAME == Queue.Declare.NAME:
            if check_for_existing(_queue_array, method.method.queue):
                _queue_array.append(AmqpQueue(method.method.queue))
            return self.send_queue_declare_ok_method(method)
        elif method.method.NAME == Exchange.Declare.NAME:
            return self.send_exchange_declare_ok(method)
        elif method.method.NAME == Basic.Publish.NAME:
            return self.decode_basic_publish(method)
        elif method.method.NAME == Channel.Close.NAME:
            return self.send_channel_close_ok_method(method)
        elif method.method.NAME == Connection.Close.NAME:
            return self.send_connection_close_ok()
        elif method.method.NAME == Basic.Qos.NAME:
            return self.send_basic_qos_ok_method()
        elif method.method.NAME == Basic.Consume.NAME:
            self._routing_key = method.method.queue
            self.consumer_tag.append(method.method.consumer_tag)
            return self.send_basic_consume_ok_method()
        elif method.method.NAME == Basic.Ack.NAME:
            print("Da prvo vidim kad udje ovde")
        else:
            return 1

