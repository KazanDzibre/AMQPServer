from pika.exchange_type import ExchangeType


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
