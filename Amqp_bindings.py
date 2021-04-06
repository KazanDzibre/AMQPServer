class AmqpBinding:
    bindings_list = []

    def __init__(self):
        print('binding_list created')

    def bind(self, src, dst, routing_key):  # src = exchange, dst = queue
        self.bindings_list.append((src, dst, routing_key))

    def check_for_binding(self, src, dst, routing_key):
        for i in self.bindings_list:
            if (src, dst, routing_key) == i:
                return True
        return False
