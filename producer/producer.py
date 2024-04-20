import time
from middleware import Middleware
import pika

def main():
    print('Empeze soy producer')
    time.sleep(20)
    print('Me conecto a rabbitmq')

    mid = Middleware()
    for i in range(10):
        mid.send_message('chan', str(i))

    #connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    #channel = connection.channel()
    #channel.queue_declare(queue='chan')
    #for i in range(10):
    #    msg = str(i)
    #    channel.basic_publish(exchange='', routing_key='chan', body=msg)
    #    time.sleep(1)

    
main()