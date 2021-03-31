class AmqpConsumer:
    _consumer_tag = ''
    queue = ''

    def __init__(self, _consumer_tag, queue):
        self._consumer_tag = _consumer_tag
        self.queue = queue

    def get_tag(self):
        return self._consumer_tag
