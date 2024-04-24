from middleware import Middleware
from serialization import deserialize_titles_message, serialize_message, serialize_dict
from filters import filter_by, year_range_condition
import os
import time

def handle_data(body, years, data_output_name, middleware, counter):
    if body == b'EOF':
        middleware.stop_consuming()
        middleware.send_message(data_output_name, "EOF")
        return
    data = deserialize_titles_message(body)

    desired_data = filter_by(data, year_range_condition, years)
    counter[0] = counter[0] + len(desired_data)
    serialized_data = serialize_message([serialize_dict(filtered_dictionary) for filtered_dictionary in desired_data])
    middleware.send_message(data_output_name, serialized_data)
    
def main():
    time.sleep(30)

    middleware = Middleware()

    years = [int(value) for value in os.getenv('YEAR_RANGE_TO_FILTER').split(',')]
    data_source_name = os.getenv('DATA_SOURCE_NAME')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')
    counter = [0]

    # Define a callback wrapper
    callback_with_params = lambda ch, method, properties, body: handle_data(body, years, data_output_name, middleware, counter)
    
    # Declare the source queue
    middleware.declare_queue(data_source_name)
    
    # Declare the output queue
    middleware.declare_queue(data_output_name)
    middleware.receive_messages(data_source_name, callback_with_params)
    print(f"La cantidad de libros con años entre {years[0]} y {years[1]} es: [{counter[0]}]")

main()  