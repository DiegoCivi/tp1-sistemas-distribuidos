from middleware import Middleware
import time
import signal
import os

import pika

class Main:

    def __init__(self):
        self.counter = 0
        #if os.getenv('HOST') == '2':
        #    time.sleep(25)
        #else:
        #    time.sleep(15)
        time.sleep(15)
        self.mid = Middleware()
        self.id = os.getenv('HOST')
        self.counter = 0

        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def handle_data(self, body, method):
        print(body)
        if body == b'EOF':
            self.mid.stop_consuming()
            
        self.mid.ack_message(method)
        self.counter += 1


    def exit_gracefully(self, *args):
        self.mid.stop_consuming()

    def main(self):
        print('Empeze soy consumer')
        
        print('Me conecto a rabbitmq')

        callback_with_params = lambda ch, method, properties, body: self.handle_data(body, method)

        self.mid.receive_messages("chan", callback_with_params)
        print(self.counter)
        #connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        #channel = connection.channel()
        #channel.queue_declare(queue='chan')
        #
        #channel.basic_consume('chan', foo, auto_ack=True)
        #channel.start_consuming()

m = Main()
m.main()