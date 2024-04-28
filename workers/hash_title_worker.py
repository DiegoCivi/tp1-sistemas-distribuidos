from middleware import Middleware
from serialization import deserialize_titles_message, serialize_message, serialize_dict
from filters import hash_title
import os
import time

####
#### TODO: This worker reads reviews from an exchange and titles from a queue. To do that, do we need another process?  
####

def handle_data(body, data_output_name, middleware, hash_modulus, temp):
    if body == b'EOF':
        middleware.stop_consuming()
        middleware.publish_message(data_output_name, "EOF", "EOF")
        return
    data = deserialize_titles_message(body)

    desired_data = hash_title(data)

    for row_dictionary in desired_data:
                
        routing_key = str(row_dictionary['hashed_title'] % hash_modulus)
        if data_output_name == 'Q3|reviews':
            if row_dictionary['Title'] == 'Pride and Prejudice':
                temp['contador'] = temp.get('contador', 0) + 1
        #title = row_dictionary['Title']
        #if title in temp and routing_key != temp[title]:
        #    raise Exception(f'La routing key para el titulo [{ title }] es [{routing_key}] pero antes daba [{ temp[title] }]')
        #temp[title] = routing_keyG

        row_dictionary['hashed_title'] = str(row_dictionary['hashed_title'])
        serialized_message = serialize_message([serialize_dict(row_dictionary)])
        middleware.publish_message(data_output_name, routing_key, serialized_message) # TODO: I think the row is a list so we have to make it a string
    
def main():
    time.sleep(15)

    middleware = Middleware()

    data_source1_name, data_source2_name = os.getenv('DATA_SOURCE_NAME').split(',')
    data_output1_name, data_output2_name = os.getenv('DATA_OUTPUT_NAME').split(',')
    hash_modulus = int(os.getenv('HASH_MODULUS'))

    # Define a callback wrapper
    temp = {}
    callback_with_params_titles = lambda ch, method, properties, body: handle_data(body, data_output1_name, middleware, hash_modulus, temp)
    callback_with_params_reviews = lambda ch, method, properties, body: handle_data(body, data_output2_name, middleware, hash_modulus, temp)
    
    # Declare the output exchange for titles
    middleware.declare_exchange(data_output1_name, 'direct')

    # Declare the output exchange for reviews
    middleware.declare_exchange(data_output2_name, 'direct')

    # Declare the source queue for the titles
    print("Voy a recibir los titulos")
    middleware.declare_queue(data_source1_name)
    middleware.receive_messages(data_source1_name, callback_with_params_titles)

    # Declare and subscribe to the reviews exchange
    print("Voy a recibir los reviews")
    middleware.declare_exchange(data_source2_name, 'fanout')
    middleware.subscribe(data_source2_name, 'q3' + 'reviews', callback_with_params_reviews)

    print(temp)

main()   