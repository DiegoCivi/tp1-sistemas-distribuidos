from middleware import Middleware
from serialization import deserialize_message, serialize_message
import os
import time

def handle_titles_data(body, counter_dict, middleware):
    """
    Acumulates the quantity of reviews for each book 
    """
    if body == b'EOF':
        middleware.stop_consuming()
        return
    data = deserialize_message(body)
    
    # For title batches
    for row_dictionary in data:
        title = row_dictionary['Title']
        counter_dict[title] = (0, 0, row_dictionary['authors']) # (reviews_quantity, ratings_summation, authors)

def handle_reviews_data(body, counter_dict, middleware):
    if body == b'EOF':
        middleware.stop_consuming()
        return
    data = deserialize_message(body)

    for row_dictionary in data:
        if row_dictionary['Title'] not in counter_dict:
            continue

        try:
            title_rating = float(row_dictionary['review/score'])
        except Exception as e:
            print(f"Error: [{e}] when parsing 'review/score' to float.")
            continue

        title = row_dictionary['Title']
        counter = counter_dict[title]
        counter[0] += 1
        counter[1] += title_rating

        counter_dict[title] = counter

def handle_reviews_data(body, counter_dict, data_output_name, middleware):
    data = deserialize_message(body)
    if data == 'EOF':
        middleware.stop_consuming()
        return

    for row in data:
        key = row[1]
        if key in counter_dict:
            # Update counters_dict with reviews data
            reviews_quantity = counter_dict[key][0] + 1
            ratings_summation = counter_dict[key][1] + row[6]
            authors = counter_dict[key][2]
            counter_dict[key] = (reviews_quantity, ratings_summation, authors)
      
            
def main():
    time.sleep(30)

    middleware = Middleware()

    data_source1_name, data_source2_name = os.getenv('DATA_SOURCE_NAME').split(',')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')
    worker_id = os.getenv('WORKER_ID')
    counter_dict = {} 

    # Define a callback wrappers
    callback_with_params_titles = lambda ch, method, properties, body: handle_titles_data(body, counter_dict, middleware)
    callback_with_params_reviews = lambda ch, method, properties, body: handle_reviews_data(body, counter_dict, data_output_name, middleware)
    
    # Declare the output queue
    middleware.declare_queue(data_output_name)

    # Declare and subscribe to the titles exchange
    middleware.declare_exchange(data_source1_name, 'direct')
    middleware.subscribe(data_source1_name, callback_with_params_titles, worker_id)

    # Declare and subscribe to the reviews exchange
    middleware.declare_exchange(data_source2_name, 'direct')
    middleware.subscribe(data_source2_name, callback_with_params_reviews, worker_id)

    # Once all the reviews where received, the counter_dict needs to be sent to the next stage
    parsed_pairs = []
    for title, info_tuple in counter_dict.items():
        msg = title + ',' + '/'.join([str(value) for value in info_tuple])
        parsed_pairs.append(msg)
    
main()