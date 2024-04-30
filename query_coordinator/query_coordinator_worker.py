from middleware import Middleware
from serialization import deserialize_titles_message
from query_coordinator import QueryCoordinator
import time


    

def handle_data(body, middleware, query_coordinator, queues_dict):
    if body == b'EOF':
        print('Ya mande todo el archivo ', query_coordinator.parse_mode)
        middleware.stop_consuming()
        query_coordinator.send_EOF()
        return
    
    batch = deserialize_titles_message(body)

    #if query_coordinator.parse_mode == 'reviews':
    #    for d in batch:
    #        if d['Title'] == 'Pride and Prejudice':
    #            print(1)

    query_coordinator.send_to_pipelines(batch)


def main():
    time.sleep(30)
    middleware = Middleware()
    query_coordinator = QueryCoordinator(middleware)
    
    
    queues_dict = {'q1_titles': ['q1_titles', 'EOF_titles'], 'q2_titles': ['q2_titles', 'EOF_titles'], 
                   'q3_titles': ['q3_titles', 'EOF_titles'], 'q3_reviews': ['q3_reviews', 'EOF_reviews'],
                   'q5_titles': ['q5_titles', 'EOF_titles'], 'q5_reviews': ['q5_reviews', 'EOF_reviews'],
                   }
    middleware.define_exchange('data', queues_dict)


    callback_with_params = lambda ch, method, properties, body: handle_data(body, middleware, query_coordinator, queues_dict)
    # Read the data from the server, parse it and fordward it
    middleware.receive_messages('query_coordinator', callback_with_params)

    middleware.consume()

    query_coordinator.change_parse_mode('reviews')

    middleware.receive_messages('query_coordinator', callback_with_params)
    
    middleware.consume()

    print(query_coordinator.temp)

    # Read the queries results
    #middleware.receive_messages('query_coordinator', callback)

main()