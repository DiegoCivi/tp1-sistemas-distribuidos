from middleware import Middleware
from serialization import deserialize_titles_message, serialize_titles_message
from filters import hash_title
import os
import time

####
#### TODO: This worker reads reviews from an exchange and titles from a queue. To do that, do we need another process?  
####

def handle_data(body, data_output_name, middleware, hash_modulus):
    if body == b'EOF':
        middleware.stop_consuming()
        middleware.send_message(data_output_name, "EOF")
        return
    data = deserialize_titles_message(body)

    desired_data = hash_title(data) # TODO: There has to be a way to identify if the batch is from reviews or from titles, so as to know the index
    for row_dictionary in desired_data:
        routing_key = row_dictionary['hashed_title'] % hash_modulus
        serialized_message = serialize_titles_message([row_dictionary])
        middleware.publish_message(data_output_name, routing_key, serialized_message) # TODO: I think the row is a list so we have to make it a string
    
def main():
    time.sleep(30)

    middleware = Middleware()

    data_source_name = os.getenv('DATA_SOURCE_NAME')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')
    hash_modulus = os.getenv('HASH_MODULUS')

    # Define a callback wrapper
    callback_with_params = lambda ch, method, properties, body: handle_data(body, data_output_name, middleware, hash_modulus)
    
    # Declare the output exchange
    middleware.declare_exchange(data_output_name, 'direct')

    # Declare and subscribe to the titles exchange
    middleware.declare_exchange(data_source_name, 'fanout')
    middleware.subscribe(data_source_name, callback_with_params)

main()   