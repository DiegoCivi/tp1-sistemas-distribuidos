import pika

class Middleware:

    def __init__(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        self._connection = connection
        self._channel = connection.channel()

    def send_message(self, queue, message):
        print('send_message')
        #try: # Passive mode to check if queue exists
        #    print("Chequeo si la cola existe")
        #    self._channel.queue_declare(queue, passive=True)
        #except: # If queue does not exist, it raises an exception ---> then we declare the queue
        #    print("La cola no existe por ende la declaro")
        #    self._channel.queue_declare(queue)

        self._channel.queue_declare(queue)
        self._channel.basic_publish(exchange='', routing_key=queue, body=message)

    def publish_message(self, exchange, routing_key, message, exchange_type='fanout'):

        try:
            self._channel.exchange_declare(exchange, passive=True)
        except ValueError:
            self._channel.exchange_declare(exchange, exchange_type=exchange_type)

        self._channel.basic_publish(exchange=exchange, routing_key=routing_key, body=message)

    def receive_messages(self, queue, callback):
        print("[receive_message] Start")
        #try:
        #    self._channel.queue_declare(queue, passive=True)
        #except:
        #    print("[receive_message] No existia la cola, entonces la declaro")
        #    self._channel.queue_declare(queue)

        self._channel.queue_declare(queue)
        self._consume(queue, callback)

    def subscribe(self, exchange, queue_name, routing_key, callback, exchange_type='fanout'):
        
        try:
            self._channel.queue_declare(queue_name, passive=True)
            self._channel.exchange_declare(exchange, passive=True)
        except ValueError:
            self._channel.exchange_declare(exchange, exchange_type=exchange_type)
            self._channel.queue_declare(queue_name)

        self._channel.queue_bind(exchange=exchange, queue=queue_name, routing_key=routing_key)
        self._consume(queue_name, callback)

    def _consume(self, queue, callback):
        print("[_consume] Start")
        self._channel.basic_consume(queue, callback, auto_ack=True)
        self._channel.start_consuming()

    def close_connection(self):
        self._connection.close()

    def stop_consuming(self):
        self._channel.stop_consuming()

            
        