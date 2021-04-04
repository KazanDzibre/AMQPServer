from Amqp_components import Utility
import socket
from Amqp_helpers import HOSTNAME, serverParameters
from pika.frame import decode_frame
from pika.exchange_type import ExchangeType
import Amqp_components
import threading


if __name__ == "__main__":
    sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    server_address = (HOSTNAME, serverParameters.DEFAULT_PORT)
    sock.bind(server_address)
    print("Server opened socket connection")
    sock.listen(5)
    while True:
        client_sock, client_address = sock.accept()
        print("Accepted client ", client_address)
        utility = Utility(client_sock)
    print('Closing Connection...')
    utility.client_sock.close()




