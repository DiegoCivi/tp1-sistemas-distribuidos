from middleware import Middleware
from serialization import deserialize_titles_message, serialize_message, serialize_dict
import os
import time

def handle_titles_data(body, counter_dict, middleware):
    """
    Acumulates the quantity of reviews for each book 
    """
    if body == b'EOF':
        middleware.stop_consuming()
        return
    data = deserialize_titles_message(body)
    
    for row_dictionary in data:
        title = row_dictionary['Title']
        if title in counter_dict:
            print("######### El titulo: ", title, " Ya estaba en el dict con los valores: ", counter_dict[title], " #########")
        counter_dict[title] = [0, 0, row_dictionary['authors']] # [reviews_quantity, ratings_summation, authors]

def handle_reviews_data(body, counter_dict, middleware):
    if body == b'EOF':
        middleware.stop_consuming()
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
        # print("Title: ", title, "Rating: ", title_rating, "Counter: ", counter)
        counter[0] += 1
        counter[1] += title_rating

        counter_dict[title] = counter

        # print("Title: ", title, "new counter is: ", counter_dict[title])

#def handle_reviews_data(body, counter_dict, data_output_name, middleware):
#    data = deserialize_message(body)
#    if data == 'EOF':
#        middleware.stop_consuming()
#        return
#
#    for row in data:
#        key = row[1]
#        if key in counter_dict:
#            # Update counters_dict with reviews data
#            reviews_quantity = counter_dict[key][0] + 1
#            ratings_summation = counter_dict[key][1] + row[6]
#            authors = counter_dict[key][2]
#            counter_dict[key] = (reviews_quantity, ratings_summation, authors)
      
            
def main():
    time.sleep(15)

    middleware = Middleware()

    data_source_name= os.getenv('DATA_SOURCE_NAME')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')
    worker_id = os.getenv('WORKER_ID')
    counter_dict = {} 

    # Define a callback wrappers
    callback_with_params_titles = lambda ch, method, properties, body: handle_titles_data(body, counter_dict, middleware)
    callback_with_params_reviews = lambda ch, method, properties, body: handle_reviews_data(body, counter_dict, middleware)
    
    # Declare the output queue
    #middleware.declare_queue(data_output_name)

    # The name of the queues the worker will read data
    titles_queue = worker_id + '_titles_Q3'
    reviews_queue = worker_id + '_reviews_Q3'

    # Declare and subscribe to the titles queue in the exchange
    middleware.define_exchange(data_source_name, {titles_queue: [titles_queue], reviews_queue: [reviews_queue]})

    # Read from the titles queue
    print("Voy a leer titles")
    middleware.subscribe(data_source_name, titles_queue, callback_with_params_titles)
    middleware.consume()

    # Read from the reviews queue
    print("Voy a leer reviews")
    middleware.subscribe(data_source_name, reviews_queue,  callback_with_params_reviews)
    middleware.consume()

    #print(counter_dict)

    # Once all the reviews were received, the counter_dict needs to be sent to the next stage
    batch_size = 0
    batch = {}
    for title, counter in counter_dict.items():
        if counter[0] >= 500 or title == 'Pride and Prejudice': # TODO: Erase
            print(title, counter)

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
    
main()