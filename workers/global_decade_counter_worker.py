from middleware import Middleware
from serialization import deserialize_titles_message, serialize_message
from filters import accumulate_authors_decades
import os
import time

def handle_data(method, body, middleware, counter_dict, eof_quantity, eof_counter):
    if body == b'EOF':
        eof_counter[0] += 1
        if eof_counter[0] == eof_quantity:
            middleware.stop_consuming()
        middleware.ack_message(method)
        return
    data = deserialize_titles_message(body)

    accumulate_authors_decades(data, counter_dict)

    middleware.ack_message(method)
    
def main():
    time.sleep(30)
    
    middleware = Middleware()

    data_source_name = os.getenv('DATA_SOURCE_NAME')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')
    eof_quantity = int(os.getenv('EOF_QUANTITY'))
    eof_counter = [0]
    counter_dict = {}

    # Define a callback wrapper
    callback_with_params = lambda ch, method, properties, body: handle_data(method, body, middleware, counter_dict, eof_quantity, eof_counter)
    
    # Declare the output queue
    middleware.receive_messages(data_source_name, callback_with_params)
    middleware.consume()

    # Collect the results
    results = []
    for key, value in counter_dict.items():
        if len(value) >= 10:
            results.append(key)
    # Send the results to the output queue
    serialized_message = serialize_message(results)
    
    middleware.send_message(data_output_name, serialized_message)
    middleware.send_message(data_output_name, 'EOF')

    middleware.close_connection()

main()

    