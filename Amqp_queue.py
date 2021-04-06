class AmqpQueue:
    queue = []
    consumer_num = 0
    consumers_array = []
    routing_key = ''

    def __init__(self, name=''):
        self.name = name
        # self.auto_delete = auto_delete
        # self.durable = durable
        # self.exclusive = exclusive
        # self.synchronous = synchronous

    def print_queue(self):
        print(self.queue)
