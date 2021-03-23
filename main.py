from Amqp_components import Utility
from pika.frame import decode_frame
from pika.exchange_type import ExchangeType
import Amqp_components
import threading


if __name__ == "__main__":
    while True:
        utility = Utility()
        x = threading.Thread(target=utility.init_protocol(utility), args=(1,))    #utility.init_protocol(utility)
        x.start()

        x.join()

    print('Closing Connection...')
    utility.client_sock.close()




