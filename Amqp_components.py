import socket
import threading
import time

from pika.spec import BasicProperties
from pika.spec import Channel, Queue, Basic, Connection, Exchange
from pika.frame import decode_frame, Method, Header, Body, Heartbeat
from pika.exchange_type import ExchangeType
from Amqp_exchange import AmqpExchange
from Amqp_queue import AmqpQueue
from Amqp_consumer import AmqpConsumer
from Amqp_helpers import *
from Amqp_bindings import AmqpBinding

threadLock = threading.Lock()

_exchange_num = 0
_exchange_array = []
_queue_num = 0
_queue_array = []
_consumers = 0
threadLock = threading.Lock()
event = threading.Event()
delivery_tag = 1
DEFAULT_EXCHANGE = ''
bindings = AmqpBinding()
_routing_key = ''


class Utility:
    client_sock = None
    default_exchange = AmqpExchange('', ExchangeType.fanout.value)
    _routing_key = ''
    _exchange_to_publish = ''

    def __init__(self, client_socket):
        """declaring global variables on start"""
        global _exchange_array
        global _exchange_num
        print("Server opened socket connection")
        self.client_sock = client_socket
        threadLock.acquire()
        _exchange_array.append(self.default_exchange)
        _exchange_num += 1
        threadLock.release()
        print("Default exchange created")
        t = threading.Thread(target=self.init_protocol)
        t.start()

    def init_protocol(self):
        print("Init protocol...")
        self.receive_protocol_version()
        self.send_start_method()
        self.handler()

    def receive_protocol_version(self):
        print("Received protocol version")
        data_in = self.client_sock.recv(MAX_BYTES, 0)
        byte_rec, ph = decode_frame(data_in)

    def send_start_method(self):
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
        channel_open_ok = Channel.OpenOk()
        channel_number = method.channel_number
        method = Method(channel_number, channel_open_ok)
        marshaled_frames = method.marshal()
        self.client_sock.send(marshaled_frames)

    def send_queue_declare_ok_method(self, method):
        print("Send queue declare ok method")
        if method.method.queue == '':
            queue_name = random_queue_name_gen()
            queue = AmqpQueue(queue_name)
        else:
            queue = AmqpQueue(method.method.queue)
        global _queue_array
        global _exchange_array
        exchange = find_item(DEFAULT_EXCHANGE, _exchange_array)
        if bindings.check_for_binding(exchange.name, queue.name, '') is False:
            bindings.bind(exchange.name, queue.name, '')  # svaki prijavljeni queue se odma vezuje na default exchange
            threadLock.acquire()
            _queue_array.append(queue)
            threadLock.release()
        queue_declare_ok = Queue.DeclareOk(queue.name, 0, 0)
        method = Method(method.channel_number, queue_declare_ok)
        marshaled_frames = method.marshal()
        self.client_sock.send(marshaled_frames)

    def send_queue_bind_ok_method(self, method):
        exchange_to_bind = method.method.exchange  # src
        queue_to_bind = method.method.queue  # dst
        routing_key = method.method.routing_key
        exchange = find_item(exchange_to_bind, _exchange_array)
        queue = find_item(queue_to_bind, _queue_array)
        if bindings.check_for_binding(exchange.name, queue.name, routing_key) is False:
            threadLock.acquire()
            bindings.bind(exchange.name, queue.name, routing_key)
            threadLock.release()
        queue_bind_ok = Queue.BindOk()
        method = Method(method.channel_number, queue_bind_ok)
        marshaled_frames = method.marshal()
        self.client_sock.send(marshaled_frames)

    def send_exchange_declare_ok(self, method):
        print("Send exchange declare ok method")
        global _exchange_array
        global _exchange_num
        exchange_declare_ok = Exchange.DeclareOk()
        exchange = AmqpExchange(method.method.exchange, method.method.type)
        if check_for_existing(_exchange_array, exchange.name):
            threadLock.acquire()
            _exchange_array.append(exchange)
            _exchange_num += 1
            threadLock.release()
        method = Method(method.channel_number, exchange_declare_ok)
        marshaled_frames = method.marshal()
        self.client_sock.send(marshaled_frames)

    def send_channel_close_ok_method(self, close_method):
        print("send channel close ok method")
        channel_close_ok = Channel.CloseOk()
        method = Method(close_method.channel_number, channel_close_ok)
        marshaled_frames = method.marshal()
        self.client_sock.send(marshaled_frames)

    def send_connection_close_ok(self):
        print("Send connection close ok")
        connection_close_ok = Connection.CloseOk()
        method = Method(0, connection_close_ok)
        marshalled_frames = method.marshal()
        self.client_sock.send(marshalled_frames)

    def send_basic_qos_ok_method(self, method):
        print("send basic qos ok method")
        basic_qos = Basic.QosOk()
        method = Method(method.channel_number, basic_qos)
        marshaled_frames = method.marshal()
        self.client_sock.send(marshaled_frames)

    def send_basic_consume_ok_method(self, method):
        print("send basic consume ok method")
        global _consumers
        global _queue_array
        consumer = AmqpConsumer(method.method.consumer_tag, method.method.queue)
        _consumers += 1
        consumer_thread = threading.Thread(target=self.handle_consumer, args=[consumer, method.channel_number, ])
        consumer_thread.start()
        event.set()
        basic_consume_ok = Basic.ConsumeOk(consumer.get_tag())
        method = Method(method.channel_number, basic_consume_ok)
        marshaled_frames = method.marshal()
        self.client_sock.send(marshaled_frames)

    def basic_deliver_method(self, channel_number, consumer_tag, message):
        global delivery_tag
        global _routing_key
        basic_deliver = Basic.Deliver(consumer_tag, delivery_tag, False,
                                      '', _routing_key)
        delivery_tag += 1
        method = Method(channel_number, basic_deliver)
        marshaled_frames = method.marshal()
        string_to_send = self.prepare_content(message, channel_number)
        string_to_send = marshaled_frames + string_to_send
        self.client_sock.send(string_to_send)

    @staticmethod
    def prepare_content(message, channel_number):
        body = Body(channel_number, message)
        marshaled_frames_body = body.marshal()
        basic_properties = BasicProperties()
        header = Header(channel_number, len(message),
                        basic_properties)  # ovde menjaj posle da bude dinamicno za sve vrste exchange-a ovo je hard kodovano sad
        marshaled_frames_header = header.marshal()
        return marshaled_frames_header + marshaled_frames_body

    def decode_basic_publish(self, method, data_in):
        print('decode_basic_publish')
        global _routing_key
        print(method.method.NAME)
        exchange = find_item(method.method.exchange, _exchange_array)
        routing_key = method.method.routing_key
        _routing_key = routing_key
        while True:
            publish_message(data_in, method, exchange, bindings, _queue_array, routing_key)
            

        while True:
            data_in = self.client_sock.recv(MAX_BYTES, 0)
            check_id, method = decode_data(data_in)
            if check_id == 2:

            elif check_id == 0:
                if method.method.NAME == Connection.Close.NAME:
                    return self.send_connection_close_ok()
            else:
                data_in = find_frame_end(data_in)
                print("couldn't read message...")

    def handle_consumer(self, consumer, channel_number):
        queue = find_item(consumer.queue, _queue_array)
        if queue is None:
            print("Specified queue doesn't exist")
        while True:
            event.wait()
            while len(queue.queue) > 0:
                message = queue.queue.pop()
                print("Sending message... ")
                self.basic_deliver_method(channel_number, consumer.get_tag(), message)
                print("Message sent... ")
            event.clear()

    def handler(self):
        while True:
            data_in = self.client_sock.recv(MAX_BYTES, 0)
            if data_in == b'':
                break
            byte_received, method = decode_frame(data_in)
            if method.NAME == Heartbeat.NAME:
                print("Waiting for messages...")  # pogledaj sta treba da radim kad posalje heartbeat
                break
            else:
                self.switch(method, data_in)

    def switch(self, method, data_in):
        if method.method.NAME == Connection.StartOk.NAME:
            return self.send_tune_method()
        elif method.method.NAME == Connection.Open.NAME:
            return self.send_open_ok_method()
        elif method.method.NAME == Channel.Open.NAME:
            return self.send_channel_open_ok_method(method)
        elif method.method.NAME == Queue.Declare.NAME:
            return self.send_queue_declare_ok_method(method)
        elif method.method.NAME == Queue.Bind.NAME:
            return self.send_queue_bind_ok_method(method)
        elif method.method.NAME == Exchange.Declare.NAME:
            return self.send_exchange_declare_ok(method)
        elif method.method.NAME == Basic.Publish.NAME:
            return self.decode_basic_publish(method, data_in)
        elif method.method.NAME == Channel.Close.NAME:
            return self.send_channel_close_ok_method(method)
        elif method.method.NAME == Connection.Close.NAME:
            return self.send_connection_close_ok()
        elif method.method.NAME == Basic.Qos.NAME:
            return self.send_basic_qos_ok_method(method)
        elif method.method.NAME == Basic.Consume.NAME:
            return self.send_basic_consume_ok_method(method)
        elif method.method.NAME == Basic.Ack.NAME:
            print("Message delivered")
        else:
            return 1
