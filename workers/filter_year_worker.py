from middleware import Middleware
from serialization import deserialize_message, serialize_message
from filters import filter_by, year_range_condition
import os
import time

def handle_data(body, years, data_output_name, middleware):
    data = deserialize_message(body)
    desired_data = filter_by(data, year_range_condition, years)
    serialized_data = serialize_message(desired_data)
    middleware.send_message(data_output_name, serialized_data)
    
def main():
    time.sleep(15)

    middleware = Middleware()

    years = os.getenv('YEAR_RANGE_TO_FILTER').split(',')
    data_source_name = os.getenv('DATA_SOURCE_NAME')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')

    # Define a callback wrapper
    callback_with_params = lambda ch, method, properties, body: handle_data(body, years, data_output_name, middleware)
    
    # Declare the source queue
    middleware.declare_queue(data_source_name)
    
    # Declare the output queue
    middleware.declare_queue(data_output_name)
    middleware.receive_messages(data_source_name, callback_with_params)

main()  