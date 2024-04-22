from middleware import Middleware
from serialization import deserialize_message, serialize_message
from filters import filter_by, category_condition
import os

def handle_titles_data(body, counter_dict, data_output_name, middleware):
    """
    Acumulates the quantity of reviews for each book 
    """
    data = deserialize_message(body)
    if data == 'EOF':
        middleware.stop_consuming()
        return
    
    # For title batches
    for row in data:
        key = (row[0], row[2])
        counter_dict[key] = counter_dict.get(key, 0) + 1
        
    middleware.send_message(data_output_name, serialized_data)
    
def main():
    middleware = Middleware()

    data_source1_name, data_source2_name = os.getenv('DATA_SOURCE_NAME').split(',')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')
    counter_dict = {} 

    # Define a callback wrapper
    callback_with_params = lambda ch, method, properties, body: handle_titles_data(body, counter_dict, data_output_name, middleware)
    
    # Declare the output queue
    middleware.declare_queue(data_output_name)

    # Declare and subscribe to the titles exchange
    middleware.declare_exchange(data_source1_name, 'direct')
    middleware.subscribe(data_source1_name, callback_with_params)

    