class AmqpExchange:
    name = ''
    exchange_type = None
    message_to_publish = ''
    bound_queues = []

    def __init__(self, name, exchange_type):
        self.name = name
        self.exchange_type = exchange_type

    def push_message_to_all_bound_queues(self):                         #Ovo cemo samo da prosirimo za razlicite tipove exchange-a za sad je fanout tako da salje svima
        print("USAO SAM U PUSH")
        global notify
        for i in self.bound_queues:
            i.queue.append(self.message_to_publish)
            notify = 1

    def bind_queue(self, queue_to_bind):
        self.bound_queues.append(queue_to_bind)

    def get_bound_queues(self):
        return self.bound_queues