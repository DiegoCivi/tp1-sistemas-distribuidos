from middleware import Middleware
from serialization import deserialize_titles_message, deserialize_into_titles_dict
from query_coordinator import QueryCoordinator
import time
import os


def handle_data(method, body, query_coordinator):
    if body == b'EOF':
        print('Ya mande todo el archivo ', query_coordinator.parse_mode)
        # query_coordinator.middleware.ack_message(method)
        query_coordinator.send_EOF()
        query_coordinator.change_parse_mode('reviews')

        ###################
        #query_coordinator.middleware._channel.basic_ack(delivery_tag=method.delivery_tag)
        query_coordinator.middleware.ack_message(method)
        ###################
        return
    
    batch = deserialize_titles_message(body)

    query_coordinator.send_to_pipelines(batch)

    ###################
    #query_coordinator.middleware._channel.basic_ack(delivery_tag=method.delivery_tag)
    query_coordinator.middleware.ack_message(method)
    ###################

def main():
    time.sleep(15)
    middleware = Middleware()

    eof_titles_max_subscribers = int(os.getenv('EOF_TITLES_MAX_SUBS'))
    eof_reviews_max_subscribers = int(os.getenv('EOF_REVIEWS_MAX_SUBS'))

    query_coordinator = QueryCoordinator(middleware, eof_titles_max_subscribers, eof_reviews_max_subscribers)
    
    
    queues_dict = {'q1_titles': ['q1_titles', 'EOF_titles'], 'q2_titles': ['q2_titles', 'EOF_titles'], 
                   'q3_titles': ['q3_titles', 'EOF_titles'], 'q3_reviews': ['q3_reviews', 'EOF_reviews'],
                   'q5_titles': ['q5_titles', 'EOF_titles'], 'q5_reviews': ['q5_reviews', 'EOF_reviews'],
                   }
    middleware.define_exchange('data', queues_dict)

    callback_with_params = lambda ch, method, properties, body: handle_data(method, body, query_coordinator)

    # Read the data from the server, parse it and fordward it
    middleware.receive_messages('query_coordinator', callback_with_params)
    middleware.consume()
    

    # Read the queries results
    #middleware.receive_messages('', callback)

main()