from Amqp_components import Utility
from pika.frame import decode_frame
from pika.exchange_type import ExchangeType
import Amqp_components
import threading


if __name__ == "__main__":
    utility = Utility()
    while True:
        utility = Utility()
    print('Closing Connection...')
    utility.client_sock.close()




