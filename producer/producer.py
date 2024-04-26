import time
from middleware import Middleware
import pika

def main():
    print('Empeze soy producer')
    time.sleep(30)
    print('Me conecto a rabbitmq')

    mid = Middleware()
    mid.declare_queue("chan")

    dictionary = {'d': 10, 'b': 10, 'p': 2}
    mid.send_message('chan', dictionary)
    #for i in range(10):
    #    mid.send_message('chan', str(i))

    #connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    #channel = connection.channel()
    #
    #for i in range(10):
    #    msg = str(i)
    #    channel.basic_publish(exchange='', routing_key='chan', body=msg)
    #    time.sleep(1)

    
main()