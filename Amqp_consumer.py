class AmqpConsumer:
    _consumer_tag = ''

    def __init__(self, _consumer_tag):
        self._consumer_tag = _consumer_tag

    def get_tag(self):
        return self._consumer_tag
