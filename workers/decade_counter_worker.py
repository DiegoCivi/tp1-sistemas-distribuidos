from middleware import Middleware
from serialization import deserialize_titles_message, serialize_message, serialize_dict
from filters import different_decade_counter
import os
import time

def handle_data(body, data_output_name, middleware):
    if body == b'EOF':
        middleware.stop_consuming()
        middleware.send_message(data_output_name, "EOF")
        print("m lelgo eof")
        return
    data = deserialize_titles_message(body)

    desired_data = different_decade_counter(data)

    if not desired_data:
        return

    serialized_data = serialize_message([serialize_dict(filtered_dictionary) for filtered_dictionary in desired_data])
    #print(serialized_data)
    middleware.send_message(data_output_name, serialized_data)
    
def main():
    time.sleep(30)

    middleware = Middleware()

    data_source_name = os.getenv('DATA_SOURCE_NAME')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')

    # Define a callback wrapper
    callback_with_params = lambda ch, method, properties, body: handle_data(body, data_output_name, middleware)
    
    # Declare the output queue
    #middleware.declare_queue(data_output_name)

    # Declare and subscribe to the titles exchange
    #middleware.declare_exchange(data_source_name, 'fanout')
    middleware.define_exchange('data', {'q2_titles': ['q2_titles']})
    middleware.subscribe('data', 'q2_titles', callback_with_params)
    middleware.consume()

main()

    