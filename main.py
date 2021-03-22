from Amqp_components import Utility
from pika.frame import decode_frame
import Amqp_components

utility = Utility()

utility.receive_protocol_version()
utility.send_start_ok_method()

while True:
    data_in = utility.client_sock.recv(Amqp_components.MAX_BYTES, 0)
    byte_rec, method, message = decode_frame(data_in)
    utility.switch(method, message)





