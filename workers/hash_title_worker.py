from middleware import Middleware
from serialization import deserialize_titles_message, serialize_message, serialize_dict
from filters import hash_title
import os
import time

####
#### TODO: This worker reads reviews from an exchange and titles from a queue. To do that, do we need another process?  
####

def handle_data(body, dataset_type, data_output_name, middleware, hash_modulus, temp):
    if body == b'EOF':
        middleware.stop_consuming()
        routing_key = 'EOF' + '_' + dataset_type
        middleware.publish_message(data_output_name, 'direct', routing_key, "EOF")
        return
    data = deserialize_titles_message(body)

    desired_data = hash_title(data)

    for row_dictionary in desired_data:
                
        worker_id = str(row_dictionary['hashed_title'] % hash_modulus)
        if dataset_type == 'reviews':
            if row_dictionary['Title'] == 'Pride and Prejudice':
                temp['contador'] = temp.get('contador', 0) + 1
        #title = row_dictionary['Title']
        #if title in temp and routing_key != temp[title]:
        #    raise Exception(f'La routing key para el titulo [{ title }] es [{routing_key}] pero antes daba [{ temp[title] }]')
        #temp[title] = routing_keyG

        row_dictionary['hashed_title'] = str(row_dictionary['hashed_title'])
        serialized_message = serialize_message([serialize_dict(row_dictionary)])
        routing_key = worker_id + '_' + dataset_type
        middleware.publish_message(data_output_name, 'direct', routing_key, serialized_message) 
    
def main():
    time.sleep(30)

    middleware = Middleware()

    data_source1_name, data_source2_name = os.getenv('DATA_SOURCE_NAME').split(',')
    data_output_name= os.getenv('DATA_OUTPUT_NAME')
    hash_modulus = int(os.getenv('HASH_MODULUS'))

    # Define a callback wrapper
    temp = {}
    callback_with_params_titles = lambda ch, method, properties, body: handle_data(body, 'titles', data_output_name, middleware, hash_modulus, temp)
    callback_with_params_reviews = lambda ch, method, properties, body: handle_data(body, 'reviews', data_output_name, middleware, hash_modulus, temp)
    
    # Declare the output exchange
    middleware.define_exchange(data_output_name,   {'0_titles': ['0_titles', 'EOF_titles'], '1_titles': ['1_titles', 'EOF_titles'], 
                                             '2_titles': ['2_titles', 'EOF_titles'], '3_titles': ['3_titles', 'EOF_titles'],
                                             '0_reviews': ['0_reviews', 'EOF_reviews'], '1_reviews': ['1_reviews', 'EOF_reviews'], 
                                             '2_reviews': ['2_reviews', 'EOF_reviews'], '3_reviews': ['3_reviews', 'EOF_reviews']
                                            })
    #middleware.declare_exchange(data_output1_name, 'direct')

    # Declare the output exchange for reviews
    #middleware.declare_exchange(data_output2_name, 'direct')

    # Declare the source queue for the titles
    print("Voy a recibir los titulos")
    middleware.receive_messages(data_source1_name, callback_with_params_titles)

    # Declare and subscribe to the reviews exchange
    print("Voy a recibir los reviews")
    middleware.define_exchange('data', {'q3_reviews': ['q3_reviews']})
    middleware.subscribe('data', 'q3_reviews', callback_with_params_reviews)

    print(temp)

main()   