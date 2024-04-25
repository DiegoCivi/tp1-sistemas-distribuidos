from middleware import Middleware
from serialization import deserialize_message, serialize_message
from filters import hash_title
import os
import time

####
#### TODO: This worker reads reviews from an exchange and titles from a queue. To do that, do we need another process?  
####

def handle_data(body, data_output_name, middleware, hash_modulus):
    data = deserialize_message(body)
    desired_data = hash_title(data) # TODO: There has to be a way to identify if the batch is from reviews or from titles, so as to know the index
    for row in desired_data:
        routing_key = row[-1] % hash_modulus
        middleware.publish_message(data_output_name, routing_key, row) # TODO: I think the row is a list so we have to make it a string
    
def main():
    time.sleep(15)

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