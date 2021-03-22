from Amqp_components import Utility
from pika.frame import decode_frame
from pika.exchange_type import ExchangeType
import Amqp_components

globals = Amqp_components.Globals()
utility = Utility()
default_exchange = Amqp_components.AmqpExchange('', ExchangeType.fanout)



utility.receive_protocol_version()
utility.send_start_ok_method()

while True:
    data_in = utility.client_sock.recv(Amqp_components.MAX_BYTES, 0)
    byte_rec, method, message = decode_frame(data_in)
    utility.switch(method, message)





