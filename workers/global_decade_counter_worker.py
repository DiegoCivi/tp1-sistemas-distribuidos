from middleware import Middleware
from serialization import deserialize_message, serialize_message
import os

def handle_data(body, data_output_name, middleware, counter_dict):
    data = deserialize_message(body)
    if data == 'EOF':
        middleware.stop_consuming()
        return
    for pair in data:
        key = pair[0]
        values = pair[1].split('/')
        if key not in counter_dict:
            counter_dict[key] = 0
        for value in values:
            if value in counter_dict[key]:
                continue
            counter_dict[key] += 1
    
def main():
    middleware = Middleware()

    data_source_name = os.getenv('DATA_SOURCE_NAME')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')
    counter_dict = {} 

    # Define a callback wrapper
    callback_with_params = lambda ch, method, properties, body: handle_data(body, data_output_name, middleware, counter_dict)
    
    # Declare the source queue
    middleware.declare_queue(data_source_name)
    
    # Declare the output queue
    middleware.declare_queue(data_output_name)
    middleware.receive_messages(data_source_name, callback_with_params)

    # Collect the results
    results = []
    for key, value in counter_dict.items():
        if value > 10:
            results.append(key)
    
    # Send the results to the output queue
    serialized_message = serialize_message(results)
    middleware.send_message(data_output_name, serialized_message)




    