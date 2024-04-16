import pika
import socket

class Middleware:
    def __init__(self, port):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(5)
    
    def run(self):
        """
        Run the middleware server to listen for client connections
        and messages to be sent to RabbitMQ queues. Those queues will
        be consumed by the Amazon Books Recommendation System, according
        to the destination of the message.
        """
        c, addr = self._server_socket.accept()
        while True:

            data = c.recv(1024)
            destination, content = data.split(b'|')

            # Send message via RabbitMQ
            connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
            channel = connection.channel()
            channel.queue_declare(queue=str(destination))
            channel.basic_publish(exchange='', routing_key = str(destination), body = str(content))

    def receive_message(self, queue_name):
        """
        Receive a message from a RabbitMQ queue.
        """
        connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
        channel = connection.channel()
        channel.basic_consume(queue = queue_name, on_message_callback = self.deserialize_message, auto_ack = True)
        channel.start_consuming()

    def send_message(self, queue_name, message):
        

    
