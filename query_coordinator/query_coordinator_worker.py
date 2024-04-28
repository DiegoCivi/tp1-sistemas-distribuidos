from middleware import Middleware
from serialization import deserialize_titles_message
from query_coordinator import QueryCoordinator
import time


    

def handle_data(body, middleware, query_parser, queues_dict):
    if body == b'EOF':
        middleware.stop_consuming()
        query_parser.send_EOF()
        return
    
    batch = deserialize_titles_message(body)

    #query_parser.parse_and_send_q1(batch)
    query_parser.parse_and_send_q2(batch)
    #query_parser.parse_and_send_q3(batch)
    #query_parser.parse_q4(batch)
    #query_parser.parse_q5(batch)


def main():
    time.sleep(30)
    middleware = Middleware()
    query_parser = QueryCoordinator(middleware)
    
    
    queues_dict = {'q1_titles': ['q1_titles', 'EOF'], 'q2_titles': ['q2_titles', 'EOF'], 'q3_titles': ['q3_titles', 'EOF'], 'q3_reviews': ['q3_reviews', 'EOF']}
    middleware.define_exchange('data', queues_dict)


    callback_with_params = lambda ch, method, properties, body: handle_data(body, middleware, query_parser, queues_dict)
    # Read the data from the server, parse it and fordward it
    middleware.receive_messages('query_coordinator', callback_with_params)

    #query_parser.change_parse_mode('reviews')

    #middleware.receive_messages('query_coordinator', callback_with_params)

    # Read the queries results
    #middleware.receive_messages('query_coordinator', callback)

main()