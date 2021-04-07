from pika.exchange_type import ExchangeType
from Amqp_helpers import find_item


class AmqpExchange:
    name = ''
    exchange_type = None

    def __init__(self, name, exchange_type):
        self.name = name
        self.exchange_type = exchange_type

    @staticmethod
    def push_message_to_all_bound_queues(exchange, bindings, queues, message, routing_key):  # Ovo cemo samo da prosirimo za razlicite tipove exchange-a za sad je fanout tako da salje svima
        if exchange.exchange_type == ExchangeType.fanout.value:
            for i in bindings:
                if i[0] == '':
                    queue = find_item(i[1], queues)
                    queue.queue.append(message)
        if exchange.exchange_type == ExchangeType.direct.value:
            for i in bindings:
                if i[0] == exchange.name and i[3] == routing_key:
                    queue = find_item(i[1], queues)
                    queue.queue.append(message)

    def bind_queue(self, queue_to_bind):
        self.bound_queues.append(queue_to_bind)

    def get_bound_queues(self):
        return self.bound_queues

