import time
from middleware import Middleware
import pika

def main():
    print('Empeze soy producer')
    time.sleep(15)
    print('Me conecto a rabbitmq')

    #connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
    #channel = connection.channel()
#
    ## Nombre del intercambiador del cual quieres verificar las uniones
    #nombre_exchange = 'mi_exchange'
#
    ## Obtener las uniones del intercambiador
    #bindings = channel.exchange_bindings(nombre_exchange)
#
    #if bindings:
    #    print(f"El intercambiador '{nombre_exchange}' está vinculado a las siguientes colas:")
    #    for binding in bindings:
    #        print(binding['queue'])
    #else:
    #    print(f"No hay uniones vinculadas al intercambiador '{nombre_exchange}'.")

    mid = Middleware()
    #mid.declare_queue("chan")

    for i in range(23):
        mid.send_message("chan", str(i))

    print("Termine")
#
    #dictionary = {'d': 10, 'b': 10, 'p': 2}
    #mid.send_message('chan', dictionary)
    
    
    
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