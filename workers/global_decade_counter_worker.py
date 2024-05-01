from middleware import Middleware
from serialization import deserialize_titles_message, serialize_message
from filters import accumulate_authors_decades
import os
import time

def handle_data(body, middleware, counter_dict):
    if body == b'EOF':
        middleware.stop_consuming()
        return
    data = deserialize_titles_message(body)

    accumulate_authors_decades(data, counter_dict)
    
def main():
    time.sleep(30)
    
    middleware = Middleware()

    data_source_name = os.getenv('DATA_SOURCE_NAME')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')
    counter_dict = {} 

    # Define a callback wrapper
    callback_with_params = lambda ch, method, properties, body: handle_data(body, middleware, counter_dict)
    
    # Declare the source queue
    #middleware.declare_queue(data_source_name)
    
    # Declare the output queue
    # middleware.declare_queue(data_output_name)
    middleware.receive_messages(data_source_name, callback_with_params)
    middleware.consume()

    # Collect the results
    #print(counter_dict['Jules Verne'], len(counter_dict['Jules Verne']))
    #print(counter_dict['Professor Jules Verne'], len(counter_dict['Professor Jules Verne']))
    results = []
    for key, value in counter_dict.items():
        if len(value) >= 10:
            results.append(key)
    print(len(results))
    print(results)
    # Send the results to the output queue
    serialized_message = serialize_message(results)
    middleware.send_message(data_output_name, serialized_message)


main()

    