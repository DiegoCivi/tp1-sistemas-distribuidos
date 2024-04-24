from middleware import Middleware
from serialization import deserialize_message, serialize_message
from filters import different_decade_counter
import os
import time

def handle_data(body, data_output_name, middleware):
    data = deserialize_message(body)
    desired_data = different_decade_counter(data)
    
    # We have a dictionary with the desired data
    parsed_pairs = []
    for author, decades_set in desired_data.items():
        msg = author + ',' + '/'.join(decades_set)
        parsed_pairs.append(msg)

    serialized_data = serialize_message(parsed_pairs)
    middleware.send_message(data_output_name, serialized_data)
    
def main():
    time.sleep(30)

    middleware = Middleware()

    data_source_name = os.getenv('DATA_SOURCE_NAME')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')

    # Define a callback wrapper
    callback_with_params = lambda ch, method, properties, body: handle_data(body, data_output_name, middleware)
    
    # Declare the output queue
    middleware.declare_queue(data_output_name)

    # Declare and subscribe to the titles exchange
    middleware.declare_exchange(data_source_name, 'fanout')
    middleware.subscribe(data_source_name, callback_with_params)

main()

    