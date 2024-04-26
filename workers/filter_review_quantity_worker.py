from middleware import Middleware
from serialization import deserialize_titles_message, serialize_message, serialize_dict
from filters import review_quantity_value
import os
import time
# titulo:cant_reviews,sumatoria_ratings,autores
def handle_data(body, data_output2_name, middleware, minimum_quantity, counter):
    if body == b'EOF':
        middleware.stop_consuming()
        middleware.send_message(data_output2_name, "EOF")
        return
    data = deserialize_titles_message(body)
    
    desired_data = review_quantity_value(data, minimum_quantity)
    counter = counter | desired_data[0]

    
def main():
    time.sleep(30)

    middleware = Middleware()

    minimum_quantity = os.getenv('MIN_QUANTITY')
    data_source_name = os.getenv('DATA_SOURCE_NAME')
    data_output1_name, data_output2_name = os.getenv('DATA_OUTPUT_NAME').split(',')
    filtered_titles = {}

    # Define a callback wrapper
    callback_with_params = lambda ch, method, properties, body: handle_data(body, data_output2_name, middleware, minimum_quantity, filtered_titles)
    
    # Declare the output queue
    middleware.declare_queue(data_output_name)

    # Declare and subscribe to the titles exchange
    middleware.declare_exchange(data_source_name, 'fanout')
    middleware.subscribe(data_source_name, callback_with_params)

    
    #serialized_data = serialize_message([serialize_dict(filtered_dictionary) for filtered_dictionary in desired_data])
    #middleware.send_message(data_output_name, serialized_data)



main()
