from middleware import Middleware
from serialization import deserialize_titles_message, serialize_message, serialize_dict
from filters import calculate_review_sentiment
import os
import time

def handle_data(body, data_output_name, middleware):
    if body == b'EOF':
        middleware.stop_consuming()
        middleware.send_message(data_output_name, "EOF")
        return
    data = deserialize_titles_message(body)

    desired_data = calculate_review_sentiment(data)
    serialized_message = serialize_message([serialize_dict(filtered_dict) for filtered_dict in desired_data])
    middleware.send_message(data_output_name, serialized_message)
    
    
def main():
    time.sleep(15)

    middleware = Middleware()

    data_source_name = os.getenv('DATA_SOURCE_NAME')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')
    source_queue = os.getenv('SOURCE_QUEUE')

    # Define a callback wrapper
    callback_with_params = lambda ch, method, properties, body: handle_data(body, data_output_name, middleware)

    # Declare and subscribe to the titles exchange
    middleware.define_exchange(data_source_name, {source_queue: [source_queue]})
    middleware.subscribe(data_source_name, source_queue, callback_with_params)
    middleware.consume()

main()

    