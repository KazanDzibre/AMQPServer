from pika.exchange_type import ExchangeType


class AmqpExchange:
    name = ''
    exchange_type = None
    message_to_publish = ''
    bound_queues = []
    routing_key = ''

    def __init__(self, name, exchange_type):
        self.name = name
        self.exchange_type = exchange_type

    def push_message_to_all_bound_queues(self, type):                         #Ovo cemo samo da prosirimo za razlicite tipove exchange-a za sad je fanout tako da salje svima
        if type == ExchangeType.fanout.value:
            for i in self.bound_queues:
                i.queue.append(self.message_to_publish)
        elif type == ExchangeType.direct.value:
            for i in self.bound_queues:
                if i.queue.name == self.routing_key:
                    i.queue.append(self.message_to_publish)
        elif type == ExchangeType.headers.value:
            print("ovde cemo za headers")
        elif type == ExchangeType.topic.value:
            print("ovde za topic")
        else:
            print("Type of exchange not recognized")

    def bind_queue(self, queue_to_bind):
        self.bound_queues.append(queue_to_bind)

    def get_bound_queues(self):
        return self.bound_queues

    def set_routing_key(self, routing_key):
        self.routing_key = routing_key