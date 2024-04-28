from middleware import Middleware
from serialization import deserialize_titles_message, serialize_message, serialize_dict
from filters import filter_by, category_condition
import os
import time

def handle_data(body, category, data_output_name, middleware, counter):
    if body == b'EOF':
        middleware.stop_consuming()
        middleware.send_message(data_output_name, "EOF")
        return
    data = deserialize_titles_message(body)
    
    desired_data = filter_by(data, category_condition, category)
    if not desired_data:
        return
    counter[0] = counter[0] + len(desired_data)
    serialized_data = serialize_message([serialize_dict(filtered_dictionary) for filtered_dictionary in desired_data])
    middleware.send_message(data_output_name, serialized_data)
    
def main():
    time.sleep(30)

    middleware = Middleware()

    category_to_filter = os.getenv('CATEGORY')
    data_source_name = os.getenv('DATA_SOURCE_NAME')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')
    counter = [0]

    # Define a callback wrapper
    callback_with_params = lambda ch, method, properties, body: handle_data(body, category_to_filter, data_output_name, middleware, counter)
    
    # Declare the output queue
    #middleware.declare_queue(data_output_name)

    # Declare and subscribe to the titles exchange
    #middleware.declare_exchange(data_source_name, 'fanout')
    middleware.define_exchange(data_source_name, {'q1_titles': ['q1_titles']})
    middleware.subscribe(data_source_name, 'q1_titles', callback_with_params)

    print(f"La cantidad de libros con la category COMPUTERS es: [{counter[0]}]")

main()
