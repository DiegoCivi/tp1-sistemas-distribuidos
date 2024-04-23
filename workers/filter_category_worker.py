from middleware import Middleware
from serialization import deserialize_message, serialize_message
from filters import filter_by, category_condition
import os
import time

def handle_data(body, category, data_output_name, middleware):
    data = deserialize_message(body)
    desired_data = filter_by(data, category_condition, category)
    serialized_data = serialize_message(desired_data)
    middleware.send_message(data_output_name, serialized_data)
    
def main():
    time.sleep(15)

    middleware = Middleware()

    category_to_filter = os.getenv('CATEGORY')
    data_source_name = os.getenv('DATA_SOURCE_NAME')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')

    # Define a callback wrapper
    callback_with_params = lambda ch, method, properties, body: handle_data(body, category_to_filter, data_output_name, middleware)
    
    # Declare the output queue
    middleware.declare_queue(data_output_name)

    # Declare and subscribe to the titles exchange
    middleware.declare_exchange(data_source_name, 'fanout')
    middleware.subscribe(data_source_name, callback_with_params)

main()