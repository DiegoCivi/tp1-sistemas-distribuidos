import pika

ALLOWED_TYPES = ('fanout', 'direct', 'topic', 'headers')

class Middleware:

    def __init__(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        self._connection = connection
        self._channel = connection.channel()
        self._queues = set()
        self._exchanges = set()


    def declare_exchange(self, name, type):
        if type not in ALLOWED_TYPES:
            raise Exception(f'Type {type} is not allowed')
        
        self._exchanges.add(name)
        self._channel.exchange_declare(exchange=name, exchange_type=type)

    def declare_queue(self, name):
        self._queues.add(name)
        self._channel.queue_declare(queue=name)

    def publish_message(self, exchange, routing_key, message):
        if exchange not in self._exchanges:
            raise Exception(f'Exchange {exchange} not declared before')
        
        self._channel.basic_publish(exchange=exchange, routing_key=routing_key, body=message)

    def send_message(self, queue, message):
        if queue not in self._queues:
            raise Exception(f'Queue {queue} not declared before')
        
        self._channel.basic_publish(exchange='', routing_key=queue, body=message)
        
    def receive_messages(self, queue, callback):
        if queue not in self._queues:
            raise Exception(f'Queue {queue} not declared before')
        
        self._consume(queue, callback)

    def subscribe(self, exchange, routing_key, callback):
        if exchange not in self._exchanges:
            raise Exception(f'Exchange {exchange} not declared before')
        
        queue = self._channel.queue_declare(queue='')
        self._channel.queue_bind(exchange=exchange, queue=queue.method.queue, routing_key=routing_key)
        self._consume(queue.method.queue, callback)

    def _consume(self, queue, callback):
        print("[_consume] Start")
        self._channel.basic_consume(queue, callback, auto_ack=True)
        self._channel.start_consuming()

    def close_connection(self):
        self._connection.close()

    def stop_consuming(self):
        self._channel.stop_consuming()

            
        