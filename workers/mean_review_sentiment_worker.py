from middleware import Middleware
from serialization import deserialize_titles_message, serialize_message, serialize_dict
import os
from workers import JoinWorker

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
        counter_dict[title] = [0,0] # [reviews_quantity, review_sentiment_summation]
    
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
    
    middleware.ack_message(method)
      
            
def main():
    data_source_name = os.getenv('DATA_SOURCE_NAME')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')
    worker_id = os.getenv('WORKER_ID')
    eof_quantity = int(os.getenv('EOF_QUANTITY'))

    worker = JoinWorker(worker_id, data_source_name, data_output_name, eof_quantity, 5)
    worker.run() 

main()