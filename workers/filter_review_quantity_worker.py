from middleware import Middleware
from serialization import deserialize_titles_message, serialize_message, serialize_dict
from filters import review_quantity_value
import os
import time

# titulo:cant_reviews,sumatoria_ratings,autores
def handle_data(body, data_output2_name, middleware, minimum_quantity, filtered_titles, eof_counter, workers_quantity):
    if body == b'EOF':
        eof_counter[0] += 1
        if eof_counter[0] == workers_quantity:
            middleware.stop_consuming()
            middleware.send_message(data_output2_name, "EOF")
        return
    
    data = deserialize_titles_message(body)
    
    desired_data = review_quantity_value(data, minimum_quantity)
    #print(desired_data[0], len(desired_data[0]))
    for key, value in desired_data[0].items():
        filtered_titles[key] = value

    
def main():
    time.sleep(30)

    middleware = Middleware()

    minimum_quantity = os.getenv('MIN_QUANTITY')
    data_source_name = os.getenv('DATA_SOURCE_NAME')
    data_output1_name, data_output2_name = os.getenv('DATA_OUTPUT_NAME').split(',')
    workers_quantity = os.getenv('WORKERS_QUANTITY')
    filtered_titles = {}
    eof_counter = [0]

    # Define a callback wrapper
    callback_with_params = lambda ch, method, properties, body: handle_data(body, data_output2_name, middleware, int(minimum_quantity), filtered_titles, eof_counter, int(workers_quantity))
    
    # Declare the output queue to the server
    #middleware.declare_queue(data_output1_name)

    # Declare the output queue to the query 4
    #middleware.declare_queue(data_output2_name)

    # Declare and subscribe to the titles exchange
    #middleware.declare_queue(data_source_name)
    middleware.receive_messages(data_source_name, callback_with_params)

    print(f"La cant de titulos con {minimum_quantity} reviews es: {len(filtered_titles)} con el dict: {filtered_titles}")
    #serialized_data = serialize_message([serialize_dict(filtered_dictionary) for filtered_dictionary in desired_data])
    #middleware.send_message(data_output_name, serialized_data)



main()
