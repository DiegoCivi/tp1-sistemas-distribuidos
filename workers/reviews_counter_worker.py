from middleware import Middleware
from serialization import deserialize_titles_message, serialize_message, serialize_dict
import os
import time

def handle_titles_data(method, body, counter_dict, middleware, eof_quantity, eof_counter):
    """
    Acumulates the quantity of reviews for each book 
    """
    if body == b'EOF':
        eof_counter[0] += 1
        if eof_counter[0] == eof_quantity:
            middleware.stop_consuming()
        middleware.ack_message(method)
        return
    data = deserialize_titles_message(body)
    
    for row_dictionary in data:
        title = row_dictionary['Title']
        counter_dict[title] = [0, 0, row_dictionary['authors']] # [reviews_quantity, ratings_summation, authors]
    
    middleware.ack_message(method)

def handle_reviews_data(method, body, counter_dict, middleware, eof_quantity, eof_counter):
    if body == b'EOF':
        eof_counter[0] += 1
        if eof_counter[0] == eof_quantity:
            middleware.stop_consuming()
            
        middleware.ack_message(method)
        return
    data = deserialize_titles_message(body)

    for row_dictionary in data:
        title = row_dictionary['Title']
        if title not in counter_dict:
            continue

        try:
            title_rating = float(row_dictionary['review/score'])
        except Exception as e:
            print(f"Error: [{e}] when parsing 'review/score' to float.")
            continue

        counter = counter_dict[title]
        counter[0] += 1
        counter[1] += title_rating

        counter_dict[title] = counter
    
    middleware.ack_message(method)

            
def main():
    time.sleep(30)

    middleware = Middleware()

    data_source_name= os.getenv('DATA_SOURCE_NAME')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')
    worker_id = os.getenv('WORKER_ID')
    eof_quantity = int(os.getenv('EOF_QUANTITY'))
    counter_dict = {}
    eof_counter = [0]

    # Define a callback wrappers
    callback_with_params_titles = lambda ch, method, properties, body: handle_titles_data(method, body, counter_dict, middleware, eof_quantity, eof_counter)
    callback_with_params_reviews = lambda ch, method, properties, body: handle_reviews_data(method, body, counter_dict, middleware, eof_quantity, eof_counter)

    # The name of the queues the worker will read data
    titles_queue = worker_id + '_titles_Q3'
    reviews_queue = worker_id + '_reviews_Q3'

    # Declare and subscribe to the titles queue in the exchange
    middleware.define_exchange(data_source_name, {titles_queue: [titles_queue], reviews_queue: [reviews_queue]})

    # Read from the titles queue
    print("Voy a leer titles")
    middleware.subscribe(data_source_name, titles_queue, callback_with_params_titles)
    middleware.consume()

    eof_counter[0] = 0 

    # Read from the reviews queue
    print("Voy a leer reviews")
    middleware.subscribe(data_source_name, reviews_queue,  callback_with_params_reviews)
    middleware.consume()

    # Once all the reviews were received, the counter_dict needs to be sent to the next stage
    batch_size = 0
    batch = {}
    for title, counter in counter_dict.items():
        batch[title] = counter
        batch_size += 1
        if batch_size == 100: # TODO: Maybe the 100 could be an env var
            serialized_message = serialize_message([serialize_dict(batch)])
            middleware.send_message(data_output_name, serialized_message)
            batch = {}
            batch_size = 0

    if len(batch) != 0:
        serialized_message = serialize_message([serialize_dict(batch)])
        middleware.send_message(data_output_name, serialized_message)
    

    middleware.send_message(data_output_name, "EOF")

    middleware.close_connection()
    
main()