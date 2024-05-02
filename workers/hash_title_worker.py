from middleware import Middleware
from serialization import deserialize_titles_message, serialize_message, serialize_dict
from filters import hash_title
import os
import time

def handle_data(method, body, dataset_and_query, data_output_name, middleware, hash_modulus, eof_counter):
    if body == b'EOF':
        eof_counter[0] += 1
        routing_key = 'EOF' + '_' + dataset_and_query
        print('Me llego un EOF para la routing_key ', routing_key)
        middleware.publish_message(data_output_name, 'direct', routing_key, "EOF")
        middleware.stop_consuming(method)
        middleware.ack_message(method)
        #if eof_counter[0] == 2:
        #    middleware.stop_consuming()
        return
    data = deserialize_titles_message(body)

    desired_data = hash_title(data)

    for row_dictionary in data:
                
        worker_id = str(row_dictionary['hashed_title'] % hash_modulus)
        ###################################
        if "November of the Heart" in row_dictionary['Title']:
            if 'reviews_Q5' in dataset_and_query:
                title = row_dictionary['Title']
                sentiment = row_dictionary['text_sentiment']
                print(title, ' con sentiment: ', sentiment,' con routing_key: ', worker_id, dataset_and_query, ' @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
        ###################################
        #if dataset_and_query == 'reviews':
        #    if row_dictionary['Title'] == 'Pride and Prejudice':
        #        temp['contador'] = temp.get('contador', 0) + 1
        #title = row_dictionary['Title']
        #if title in temp and routing_key != temp[title]:
        #    raise Exception(f'La routing key para el titulo [{ title }] es [{routing_key}] pero antes daba [{ temp[title] }]')
        #temp[title] = routing_keyG

        row_dictionary.pop('hashed_title')
        serialized_message = serialize_message([serialize_dict(row_dictionary)])
        routing_key = worker_id + '_' + dataset_and_query
        middleware.publish_message(data_output_name, 'direct', routing_key, serialized_message)
    
    middleware.ack_message(method)
    
def main():
    time.sleep(30)

    middleware = Middleware()

    data_source1_name, data_source2_name, data_source3_name, data_source4_name  = os.getenv('DATA_SOURCE_NAME').split(',')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')
    hash_modulus = int(os.getenv('HASH_MODULUS'))

    # Define a callback wrapper
    eof_counter = [0]
    callback_with_params_titles_q3 = lambda ch, method, properties, body: handle_data(method, body, 'titles_Q3', data_output_name, middleware, hash_modulus, eof_counter)
    callback_with_params_reviews_q3 = lambda ch, method, properties, body: handle_data(method, body, 'reviews_Q3', data_output_name, middleware, hash_modulus, eof_counter)

    callback_with_params_titles_q5 = lambda ch, method, properties, body: handle_data(method, body, 'titles_Q5', data_output_name, middleware, hash_modulus, eof_counter)
    callback_with_params_reviews_q5 = lambda ch, method, properties, body: handle_data(method, body, 'reviews_Q5', data_output_name, middleware, hash_modulus, eof_counter)

    
    # Declare the output exchange for query 3 and query 5
    middleware.define_exchange(data_output_name,   {'0_titles_Q3': ['0_titles_Q3', 'EOF_titles_Q3'], '1_titles_Q3': ['1_titles_Q3', 'EOF_titles_Q3'], 
                                             '2_titles_Q3': ['2_titles_Q3', 'EOF_titles_Q3'], '3_titles_Q3': ['3_titles_Q3', 'EOF_titles_Q3'],
                                             '0_reviews_Q3': ['0_reviews_Q3', 'EOF_reviews_Q3'], '1_reviews_Q3': ['1_reviews_Q3', 'EOF_reviews_Q3'], 
                                             '2_reviews_Q3': ['2_reviews_Q3', 'EOF_reviews_Q3'], '3_reviews_Q3': ['3_reviews_Q3', 'EOF_reviews_Q3'],
                                             '0_titles_Q5': ['0_titles_Q5', 'EOF_titles_Q5'], '1_titles_Q5': ['1_titles_Q5', 'EOF_titles_Q5'], 
                                             '2_titles_Q5': ['2_titles_Q5', 'EOF_titles_Q5'], '3_titles_Q5': ['3_titles_Q5', 'EOF_titles_Q5'],
                                             '0_reviews_Q5': ['0_reviews_Q5', 'EOF_reviews_Q5'], '1_reviews_Q5': ['1_reviews_Q5', 'EOF_reviews_Q5'], 
                                             '2_reviews_Q5': ['2_reviews_Q5', 'EOF_reviews_Q5'], '3_reviews_Q5': ['3_reviews_Q5', 'EOF_reviews_Q5']
                                            })

    # Declare the source queue for the titles
    print("Voy a recibir los titulos")
    # For Q3
    middleware.receive_messages(data_source1_name, callback_with_params_titles_q3)
    # For Q5
    middleware.receive_messages(data_source2_name, callback_with_params_titles_q5)
    middleware.consume()

    # Declare and subscribe to the reviews exchange
    print("Voy a recibir los reviews")
    # For Q3
    middleware.define_exchange('data', {'q3_reviews': ['q3_reviews']})
    middleware.subscribe('data', 'q3_reviews', callback_with_params_reviews_q3)
    # For Q5
    middleware.receive_messages(data_source4_name, callback_with_params_reviews_q5)
    middleware.consume()

main()   