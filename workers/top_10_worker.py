from middleware import Middleware
from serialization import deserialize_titles_message, serialize_message, serialize_dict
from filters import get_top_n
import os
import time

def handle_data(body, middleware, top, top_n, last):
    if body == b'EOF':
        middleware.stop_consuming()
        return
    print(body)
    data = deserialize_titles_message(body)

    top[0] = get_top_n(data, top[0], top_n, last)
    
    
    
def main():
    time.sleep(30)

    middleware = Middleware()

    top_n = int(os.getenv('TOP_N'))
    #data_source_name = os.getenv('DATA_SOURCE_NAME')
    #data_output_name = os.getenv('DATA_OUTPUT_NAME')
    last = True if os.getenv('LAST') == '1' else False
    top = [[]]

    # Define a callback wrapper
    callback_with_params = lambda ch, method, properties, body: handle_data(body, middleware, top, top_n, last)
    
    # Declare the output queue
    #middleware.declare_queue(data_output_name)

    # Declare and subscribe to the titles exchange
    #middleware.declare_exchange(data_source_name, 'fanout')
    if last:
        middleware.receive_messages('Q4|top_10_last', callback_with_params)
        #middleware.sen()
    else:
        middleware.receive_messages('top_10', callback_with_params)
        print("En el 1 el top es: ", top)
        dict_to_send = {title:str(mean_rating) for title,mean_rating in top[0]}
        serialized_data = serialize_message([serialize_dict(dict_to_send)])
        middleware.send_message('Q4|top_10_last', serialized_data)
        middleware.send_message('Q4|top_10_last', 'EOF')
    middleware.consume()


    if last:
        print(top)


main()
