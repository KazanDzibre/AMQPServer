from Amqp_components import Utility
from pika.frame import decode_frame
from pika.exchange_type import ExchangeType
import Amqp_components

utility = Utility()

utility.init_protocol()

while True:
    data_in = utility.client_sock.recv(Amqp_components.MAX_BYTES, 0)
    if data_in == b'':
        break
    byte_received, method, message = decode_frame(data_in)
    utility.switch(method, message)


print("Closing Connection...")
utility.client_sock.close()


