from pika.exchange_type import ExchangeType
from Amqp_helpers import find_item


class AmqpExchange:
    name = ''
    exchange_type = None
    message_to_publish = ''

    def __init__(self, name, exchange_type):
        self.name = name
        self.exchange_type = exchange_type

    def push_message_to_all_bound_queues(self, type_of_exchange, bindings, queues):  # Ovo cemo samo da prosirimo za razlicite tipove exchange-a za sad je fanout tako da salje svima
        if type_of_exchange == ExchangeType.fanout.value:
            for i in bindings:
                if i[0] == '':
                    queue = find_item(i[1], queues)
                    queue.queue.append(self.message_to_publish)
       # elif type == ExchangeType.direct.value:
       #     for i in self.bound_queues:
       #         if i.queue.routing_key == self.routing_key:
       #             i.queue.append(self.message_to_publish)
       # elif type == ExchangeType.headers.value:
       #     print("ovde cemo za headers")
       # elif type == ExchangeType.topic.value:
       #     print("ovde za topic")
       # else:
       #     print("Type of exchange not recognized")

    def bind_queue(self, queue_to_bind):
        self.bound_queues.append(queue_to_bind)

    def get_bound_queues(self):
        return self.bound_queues

    def set_routing_key(self, routing_key):
        self.routing_key = routing_key
