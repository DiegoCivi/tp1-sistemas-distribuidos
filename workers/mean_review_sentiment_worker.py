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
        if title == '101 favorite stories from the Bible':
            print("EN titles me llego000")
        counter_dict[title] = [0,0] # [reviews_quantity, review_sentiment_summation]

def handle_reviews_data(body, counter_dict, middleware):
    if body == b'EOF':
        middleware.stop_consuming()
        return
    data = deserialize_titles_message(body)

    for row_dictionary in data:
        title = row_dictionary['Title']
        if title not in counter_dict:
            continue
        
        if title == '101 favorite stories from the Bible':
            print("En REVIEWS me llega")

        text_sentiment = row_dictionary['text_sentiment']
        try:
            text_sentiment = float(text_sentiment)
        except Exception as e:
            print(f"Error: [{e}] when parsing 'text_sentiment' to float.")
            continue
        
        counter = counter_dict[title]
        counter[0] += 1
        counter[1] += text_sentiment 
        counter_dict[title] = counter

        #for title, review_sentiment in row_dictionary.items():
        #    if title not in counter_dict:
        #        continue
#
        #    try:
        #        review_sentiment = float(review_sentiment)
        #    except Exception as e:
        #        print(f"Error: [{e}] when parsing 'review/score' to float.")
        #        continue
#
        #    counter = counter_dict[title]
        #    counter[0] += 1
        #    counter[1] += review_sentiment 
        #    counter_dict[title] = counter
      
            
def main():
    time.sleep(30)

    middleware = Middleware()

    data_source_name = os.getenv('DATA_SOURCE_NAME')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')
    worker_id = os.getenv('WORKER_ID')
    counter_dict = {} 

    # Define a callback wrappers
    callback_with_params_titles = lambda ch, method, properties, body: handle_titles_data(body, counter_dict, middleware)
    callback_with_params_reviews = lambda ch, method, properties, body: handle_reviews_data(body, counter_dict, middleware)

    # The name of the queues the worker will read data
    titles_queue = worker_id + '_titles_Q5'
    reviews_queue = worker_id + '_reviews_Q5'

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


    # Once all the reviews were received, the counter_dict needs to be sent to the next stage
    batch_size = 0
    batch = {}
    for title, counter in counter_dict.items():
        # Ignore titles with no reviews
        if counter[0] == 0:
            continue
        
        batch[title] = str(counter[1] / counter[0])
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