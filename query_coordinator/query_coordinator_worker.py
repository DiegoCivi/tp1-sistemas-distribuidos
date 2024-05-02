from middleware import Middleware
from serialization import deserialize_titles_message, deserialize_into_titles_dict, ROW_SEPARATOR
from query_coordinator import QueryCoordinator
import time
import os

def handle_data(method, body, query_coordinator):
    if body == b'EOF':
        print('Ya mande todo el archivo ', query_coordinator.parse_mode)
        query_coordinator.send_EOF()
        query_coordinator.change_parse_mode('reviews')

        query_coordinator.middleware.ack_message(method)
        return
    
    batch = deserialize_titles_message(body)
    query_coordinator.send_to_pipelines(batch)

    query_coordinator.middleware.ack_message(method)

def handle_results(method, body, query_coordinator, results_string, fields_to_print, query):
    #print("ME LLEGO, ", body)
    if body == b'EOF':
        print(f"Me llego un eof para {query}")
        query_coordinator.middleware.ack_message(method)
        query_coordinator.middleware.stop_consuming(method)
        return

    #query_coordinator.middleware.ack_message(method)
    data = query_coordinator.deserialize_result(body, query)
    results_string[0] += '\n' + query_coordinator.build_result_line(data, fields_to_print, query)

    query_coordinator.middleware.ack_message(method)



def main():
    time.sleep(30)
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
    
    results_string_q1 = ['[QUERY 1] Results']
    results_string_q2 = ['[QUERY 2] Results']
    results_string_q3 = ['[QUERY 3] Results']
    results_string_q4 = ['[QUERY 4] Results']
    # results_string_q5 = ['[QUERY 5] Results']
    # Use queues to receive the queries results
    q1_results_with_params = lambda ch, method, properties, body: handle_results(method, body, query_coordinator, results_string_q1, ['Title', 'authors', 'publisher'], 'Q1')
    q2_results_with_params = lambda ch, method, properties, body: handle_results(method, body, query_coordinator, results_string_q2, ['authors'], 'Q2')
    q3_results_with_params = lambda ch, method, properties, body: handle_results(method, body, query_coordinator, results_string_q3, ['Title', 'authors'], 'Q3')
    q4_results_with_params = lambda ch, method, properties, body: handle_results(method, body, query_coordinator, results_string_q4, ['Title'], 'Q4')
    # q5_results_with_params = lambda ch, method, properties, body: handle_results(method, body, query_coordinator, results_string_q5, ['Title'], 'Q5')
    middleware.receive_messages('q1_results', q1_results_with_params)
    middleware.receive_messages('q2_results', q2_results_with_params)
    middleware.receive_messages('q3_results', q3_results_with_params)
    middleware.receive_messages('q4_results', q4_results_with_params)
    # middleware.receive_messages('q5_results', q5_results_with_params)
    middleware.consume()

    # Assemble the results 
    final_results = '\n'.join(results_string_q1 + results_string_q2 + results_string_q3 + results_string_q4)
    print(final_results)
    # Send the results to the server 

    


main()