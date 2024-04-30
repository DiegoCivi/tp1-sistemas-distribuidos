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
    time.sleep(15)

    middleware = Middleware()

    category_to_filter = os.getenv('CATEGORY')
    data_source_name = os.getenv('DATA_SOURCE_NAME')
    source_queue = os.getenv('SOURCE_QUEUE')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')
    counter = [0]

    # Define a callback wrapper
    callback_with_params = lambda ch, method, properties, body: handle_data(body, category_to_filter, data_output_name, middleware, counter)

    # Declare and subscribe to the titles exchange
    middleware.define_exchange(data_source_name, {source_queue: [source_queue]})
    middleware.subscribe(data_source_name, source_queue, callback_with_params)
    middleware.consume()

    print(f"La cantidad de libros con la category {category_to_filter} es: [{counter[0]}]")

main()
