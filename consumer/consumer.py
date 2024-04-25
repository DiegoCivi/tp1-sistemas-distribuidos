from middleware import Middleware
import time

import pika

def foo(ch, method, properties, body):
    print(body)


def main():
    print('Empeze soy consumer')
    time.sleep(15)
    print('Me conecto a rabbitmq')
    mid = Middleware()
    mid.declare_queue("chan")
    mid.receive_messages("chan", foo)
    
    #connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    #channel = connection.channel()
    #channel.queue_declare(queue='chan')
    #
    #channel.basic_consume('chan', foo, auto_ack=True)
    #channel.start_consuming()

main()