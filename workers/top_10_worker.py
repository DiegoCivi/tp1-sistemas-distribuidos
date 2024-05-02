from middleware import Middleware
from serialization import deserialize_titles_message, serialize_message, serialize_dict
from filters import get_top_n
import os
import time

def handle_data(method, body, middleware, top, top_n, last, eof_counter, workers_quantity):
    print(body)
    if body == b'EOF':
        eof_counter[0] += 1
        print('Mi cantidad de EOFS es: ', eof_counter)
        if last and eof_counter[0] == workers_quantity:
            middleware.stop_consuming()
        elif not last:
            middleware.stop_consuming()
        middleware.ack_message(method)
        return
    data = deserialize_titles_message(body)

    top[0] = get_top_n(data, top[0], top_n, last)
    middleware.ack_message(method)
    
    
    
def main():
    time.sleep(30)

    middleware = Middleware()

    top_n = int(os.getenv('TOP_N'))
    data_source_name = os.getenv('DATA_SOURCE_NAME')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')
    workers_quantity = int(os.getenv('WORKERS_QUANTITY'))
    last = True if os.getenv('LAST') == '1' else False
    top = [[]]
    eof_counter = [0]

    # Define a callback wrapper
    callback_with_params = lambda ch, method, properties, body: handle_data(method, body, middleware, top, top_n, last, eof_counter, workers_quantity)
    
    middleware.receive_messages(data_source_name, callback_with_params)
    middleware.consume()

    dict_to_send = {title:str(mean_rating) for title,mean_rating in top[0]}
    serialized_data = serialize_message([serialize_dict(dict_to_send)])
    if not last:
        if len(top[0]) != 0:
            print('Mi top es: ', top)
            middleware.send_message(data_output_name, serialized_data)

        middleware.send_message(data_output_name, 'EOF')
    else:
        # Send the results to the query_coordinator
        print('El topp serializazado es: ', serialized_data)
        middleware.send_message(data_output_name, serialized_data)
        middleware.send_message(data_output_name, 'EOF')
        print('El top en el acumulador es: ', top)


main()
