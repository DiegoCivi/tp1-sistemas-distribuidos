from middleware import Middleware
from serialization import deserialize_titles_message, serialize_message, serialize_dict
from filters import calculate_review_sentiment
import os
import time

def handle_data(method, body, data_output_name, middleware):
    if body == b'EOF':
        middleware.stop_consuming()
        #middleware.send_message(data_output_name, "EOF")
        middleware.ack_message(method)
        return
    data = deserialize_titles_message(body)


    desired_data = calculate_review_sentiment(data)
    serialized_message = serialize_message([serialize_dict(filtered_dict) for filtered_dict in desired_data])
    middleware.send_message(data_output_name, serialized_message)
    ###################################
    for d in desired_data:
        if 'November of the Heart' in d['Title']:
            title = d['Title']
            sentiment = d['text_sentiment']
            print(f'Titulo: {title} con sentiment: {sentiment}', ' @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
    ###################################
    
    middleware.ack_message(method)
    
def handle_eof(method, body, eof_counter, worker_quantity, data_output_name, next_worker_quantity, middleware):
    if body != b'EOF':
        print("[ERROR] Not an EOF on handle_eof(), system BLOCKED!. Received: ", body)
    
    eof_counter[0] += 1
    print('Me llego un eof, llevo ', eof_counter[0])
    if eof_counter[0] == worker_quantity:
        for _ in range(next_worker_quantity):
            print("MANDO UN EOF")
            middleware.send_message(data_output_name, 'EOF')
        middleware.stop_consuming()

    middleware.ack_message(method)

def main():
    time.sleep(15)

    middleware = Middleware()

    data_source_name = os.getenv('DATA_SOURCE_NAME')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')
    #data_output_name = 'test' #################################### BORRARRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR
    source_queue = os.getenv('SOURCE_QUEUE')
    worker_id = os.getenv('WORKER_ID')
    workers_quantity = int(os.getenv('WORKERS_QUANTITY'))
    next_workers_quantity = int(os.getenv('NEXT_WORKERS_QUANTITY'))
    eof_queue = os.getenv('EOF_QUEUE')

    # Define a callback wrapper
    callback_with_params = lambda ch, method, properties, body: handle_data(method, body, data_output_name, middleware)

    # Declare and subscribe to the titles exchange
    middleware.define_exchange(data_source_name, {source_queue: [source_queue]})
    middleware.subscribe(data_source_name, source_queue, callback_with_params)
    middleware.consume()

    if worker_id == '0':
        if workers_quantity == 1:
            for _ in range(next_workers_quantity):
                print("MANDO UN EOF")
                middleware.send_message(data_output_name, 'EOF')
            return
        eof_counter = [0]
        eof_callback = lambda ch, method, properties, body: handle_eof(method, body, eof_counter, workers_quantity - 1, data_output_name, next_workers_quantity, middleware)
        middleware.receive_messages(eof_queue, eof_callback)
        middleware.consume()
    else:
        middleware.send_message(eof_queue, 'EOF')

main()

    