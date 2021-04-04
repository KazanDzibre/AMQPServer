import socket
import threading

from pika.spec import BasicProperties
from pika.spec import Channel, Queue, Basic, Connection, Exchange
from pika.frame import decode_frame, Method, Header, Body, Heartbeat
from pika.exchange_type import ExchangeType
from Amqp_exchange import AmqpExchange
from Amqp_queue import AmqpQueue
from Amqp_consumer import AmqpConsumer
from Amqp_helpers import *

_exchange_num = 0
_exchange_array = []
_queue_num = 0
_queue_array = []
_consumers = 0
available_message = 0
threadLock = threading.Lock()
event = threading.Event()
delivery_tag = 1


class Utility:

    client_sock = None
    default_exchange = AmqpExchange('', ExchangeType.fanout)
    _routing_key = ''
    _exchange_to_publish = ''

    def __init__(self, client_socket):
        """declaring global variables on start"""
        global _exchange_array
        global _exchange_num
        print("Server opened socket connection")
        self.client_sock = client_socket
        _exchange_array.append(self.default_exchange)
        _exchange_num += 1
        print("Default exchange created")
        t = threading.Thread(target=self.init_protocol)
        t.start()

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
        channel_open_ok = Channel.OpenOk()
        channel_number = method.channel_number
        method = Method(channel_number, channel_open_ok)
        marshaled_frames = method.marshal()
        self.client_sock.send(marshaled_frames)

    def send_queue_declare_ok_method(self, method):
        print("Send queue declare ok method")
        queue = AmqpQueue(method.method.queue)
        global _queue_array
        if check_for_existing(self.default_exchange.bound_queues, queue.name):
            self.default_exchange.bound_queues.append(queue)                #po defaultu su svi queue-ovi povezani na default_exchange
            _queue_array.append(queue)
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
        if check_for_existing(_exchange_array, exchange.name):
            _exchange_array.append(exchange)
            _exchange_num += 1
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
        consumer_thread = threading.Thread(target=self.handle_consumer, args=[consumer])
        consumer_thread.start()
        queue = find_item(method.method.queue, _queue_array)
        queue.consumer_num += 1
        queue.consumers_array.append(consumer)
        basic_consume_ok = Basic.ConsumeOk(queue.consumers_array[0].get_tag())
        method = Method(1, basic_consume_ok)
        marshaled_frames = method.marshal()
        self.client_sock.send(marshaled_frames)

    def basic_deliver_method(self, channel_number, consumer_tag, message):
        global delivery_tag
        basic_deliver = Basic.Deliver(consumer_tag, delivery_tag, False,
                                      '', self._routing_key)
        delivery_tag += 1
        method = Method(channel_number, basic_deliver)
        marshaled_frames = method.marshal()
        string_to_send = self.prepare_content(message, channel_number)
        string_to_send = marshaled_frames + string_to_send
        self.client_sock.send(string_to_send)

    def prepare_content(self, message, channel_number):
        body = Body(channel_number, message)
        marshaled_frames_body = body.marshal()
        basic_properties = BasicProperties()
        header = Header(channel_number, len(self.default_exchange.message_to_publish), basic_properties)             #ovde menjaj posle da bude dinamicno za sve vrste exchange-a ovo je hard kodovano sad
        marshaled_frames_header = header.marshal()
        return marshaled_frames_header + marshaled_frames_body

    def decode_basic_publish(self, method):
        self._routing_key = method.method.routing_key
        self._exchange_to_publish = method.method.exchange

    def handle_consumer(self, consumer):
        queue = find_item(consumer.queue, _queue_array)
        if queue is None:
            print("Specified queue doesn't exist")
        while True:
            event.wait()
            while len(queue.queue) > 0:
                message = queue.queue.pop()
                print("Sending message... ")
                self.basic_deliver_method(1, consumer.get_tag(), message)
                print("Message sent... ")
            event.clear()

    def handler(self):
        global available_message
        while True:
            data_in = self.client_sock.recv(MAX_BYTES, 0)
            if data_in == b'':
                break
            byte_received, method = decode_frame(data_in)
            if method.NAME == Header.NAME:
                message = decode_message_from_header(data_in)
                exchange = find_item(self._exchange_to_publish, _exchange_array)
                if exchange is None:
                    print("There is no exchange with that name")
                else:
                    exchange.message_to_publish = message
                    print("Stigla poruka")
                    event.set()
                    exchange.push_message_to_all_bound_queues()
            elif method.NAME == Body.NAME:
                message = decode_message_from_body(data_in)
                exchange = find_item(self._exchange_to_publish, _exchange_array)
                if exchange is None:
                    print("There is no exchange with that name")
                else:
                    exchange.message_to_publish = message
                    event.set()
                    exchange.push_message_to_all_bound_queues()
            elif method.NAME == Heartbeat.NAME:
                print("Usao u heartbeat")                       #pogledaj sta treba da radim kad posalje heartbeat
                break
            else:
                self.switch(method)

    def switch(self, method):
        if method.method.NAME == Connection.StartOk.NAME:
            return self.send_tune_method()
        elif method.method.NAME == Connection.Open.NAME:
            return self.send_open_ok_method()
        elif method.method.NAME == Channel.Open.NAME:
            return self.send_channel_open_ok_method(method)
        elif method.method.NAME == Queue.Declare.NAME:
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
            return self.send_basic_qos_ok_method(method)
        elif method.method.NAME == Basic.Consume.NAME:
            return self.send_basic_consume_ok_method(method)
        elif method.method.NAME == Basic.Ack.NAME:
            print("Da prvo vidim kad udje ovde")
        else:
            return 1
