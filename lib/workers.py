from middleware import Middleware
from filters import filter_by, eof_manage_process, hash_title
from serialization import serialize_message, serialize_dict, deserialize_titles_message
import signal

YEAR_CONDITION = 'YEAR'
TITLE_CONDITION = 'TITLE'
CATEGORY_CONDITION = 'CATEGORY'

class FilterWorker:

    def __init__(self, id, input_name, output_name, eof_queue, workers_quantity, next_workers_quantity, exchange_queue):
        signal.signal(signal.SIGTERM, self.handle_signal)

        self.id = id
        self.eof_queue = eof_queue
        self.input_name = input_name
        self.output_name = output_name
        self.exchange_queue = exchange_queue
        self.workers_quantity = workers_quantity
        self.next_workers_quantity = next_workers_quantity
        self.middleware = Middleware()
        
        self.stop_worker = False
        self.filtered_results_quantity = 0

    def handle_signal(self):
        self.middleware.close_connection()
    
    def set_filter_type(self, type, filtering_function, value):
        self.filtering_function = filtering_function
        self.filter_condition = type
        self.filter_value = value

    def handle_data(self, method, body):
        if body == b'EOF':
            self.middleware.stop_consuming()
            self.middleware.ack_message(method)
            return
        data = deserialize_titles_message(body)

        desired_data = filter_by(data, self.filtering_function, self.filter_value)
        if not desired_data:
            self.middleware.ack_message(method)
            return
        
        self.filtered_results_quantity += len(desired_data)
        serialized_data = serialize_message([serialize_dict(filtered_dictionary) for filtered_dictionary in desired_data])
        self.middleware.send_message(self.output_name, serialized_data)

        self.middleware.ack_message(method)

    def run(self):
        callback_with_params = lambda ch, method, properties, body: self.handle_data(method, body)

        # Declare the source
        if not self.input_name.startswith("QUEUE_"):
            self.middleware.define_exchange(self.input_name, {self.exchange_queue: [self.exchange_queue]})

        # Declare the output
        print("Voy a leer titulos")
        if not self.output_name.startswith("QUEUE_"):
            self.middleware.define_exchange(self.output_name, {self.exchange_queue: [self.exchange_queue]})

        # Read the data
        if self.input_name.startswith("QUEUE_"):
            self.middleware.receive_messages(self.input_name, callback_with_params)
        else:
            self.middleware.subscribe(self.input_name, self.exchange_queue, callback_with_params)

        self.middleware.consume()

        print(f'El worker se quedo con {self.filtered_results_quantity} cantidad de titulos')
        # Once received the EOF, if I am the leader (WORKER_ID == 0), propagate the EOF to the next filter
        # after receiving WORKER_QUANTITY EOF messages.
        eof_manage_process(self.id, self.workers_quantity, self.next_workers_quantity, self.output_name, self.middleware, self.eof_queue)

        self.middleware.close_connection()


class HashWorker:

    def __init__(self, input_titles_q3, input_titles_q5, input_reviews_q3, input_reviews_q5, output, hash_modulus):
        self.middleware = Middleware()
        self.id = id
        self.input_titles_q3 = input_titles_q3
        self.input_titles_q5 = input_titles_q5
        self.input_reviews_q3 = input_reviews_q3
        self.input_reviews_q5 = input_reviews_q5
        self.output = output
        self.hash_modulus = hash_modulus

    def handle_data(self, method, body, dataset_and_query):
        if body == b'EOF':
            routing_key = 'EOF' + '_' + dataset_and_query
            print('Me llego un EOF para la routing_key ', routing_key)
            self.middleware.publish_message(self.output, 'direct', routing_key, "EOF")
            self.middleware.stop_consuming(method)
            self.middleware.ack_message(method)

            return
        data = deserialize_titles_message(body)

        hash_title(data)

        for row_dictionary in data:
                    
            worker_id = str(row_dictionary['hashed_title'] % self.hash_modulus)

            row_dictionary.pop('hashed_title')
            serialized_message = serialize_message([serialize_dict(row_dictionary)])
            routing_key = worker_id + '_' + dataset_and_query
            self.middleware.publish_message(self.output, 'direct', routing_key, serialized_message)
        
        self.middleware.ack_message(method)

    def run(self):
        # Define a callback wrapper
        callback_with_params_titles_q3 = lambda ch, method, properties, body: self.handle_data(method, body, 'titles_Q3')
        callback_with_params_reviews_q3 = lambda ch, method, properties, body: self.handle_data(method, body, 'reviews_Q3')

        callback_with_params_titles_q5 = lambda ch, method, properties, body: self.handle_data(method, body, 'titles_Q5')
        callback_with_params_reviews_q5 = lambda ch, method, properties, body: self.handle_data(method, body, 'reviews_Q5')

        # Declare the output exchange for query 3 and query 5
        self.middleware.define_exchange(self.output,   {'0_titles_Q3': ['0_titles_Q3', 'EOF_titles_Q3'], '1_titles_Q3': ['1_titles_Q3', 'EOF_titles_Q3'], 
                                                '2_titles_Q3': ['2_titles_Q3', 'EOF_titles_Q3'], '3_titles_Q3': ['3_titles_Q3', 'EOF_titles_Q3'],
                                                '4_titles_Q3': ['4_titles_Q3', 'EOF_titles_Q3'], '5_titles_Q3': ['5_titles_Q3', 'EOF_titles_Q3'],
                                                '0_reviews_Q3': ['0_reviews_Q3', 'EOF_reviews_Q3'], '1_reviews_Q3': ['1_reviews_Q3', 'EOF_reviews_Q3'], 
                                                '2_reviews_Q3': ['2_reviews_Q3', 'EOF_reviews_Q3'], '3_reviews_Q3': ['3_reviews_Q3', 'EOF_reviews_Q3'],
                                                '4_reviews_Q3': ['4_reviews_Q3', 'EOF_reviews_Q3'], '5_reviews_Q3': ['5_reviews_Q3', 'EOF_reviews_Q3'],

                                                '0_titles_Q5': ['0_titles_Q5', 'EOF_titles_Q5'], '1_titles_Q5': ['1_titles_Q5', 'EOF_titles_Q5'], 
                                                '2_titles_Q5': ['2_titles_Q5', 'EOF_titles_Q5'], '3_titles_Q5': ['3_titles_Q5', 'EOF_titles_Q5'],
                                                '4_titles_Q5': ['4_titles_Q5', 'EOF_titles_Q5'], '5_titles_Q5': ['5_titles_Q5', 'EOF_titles_Q5'],

                                                '0_reviews_Q5': ['0_reviews_Q5', 'EOF_reviews_Q5'], '1_reviews_Q5': ['1_reviews_Q5', 'EOF_reviews_Q5'], 
                                                '2_reviews_Q5': ['2_reviews_Q5', 'EOF_reviews_Q5'], '3_reviews_Q5': ['3_reviews_Q5', 'EOF_reviews_Q5'],
                                                '4_reviews_Q5': ['4_reviews_Q5', 'EOF_reviews_Q5'], '5_reviews_Q5': ['5_reviews_Q5', 'EOF_reviews_Q5']
                                                })

        # Declare the source queue for the titles
        print("Voy a recibir los titulos")
        # For Q3
        self.middleware.receive_messages(self.input_titles_q3, callback_with_params_titles_q3)
        # For Q5
        self.middleware.receive_messages(self.input_titles_q5, callback_with_params_titles_q5)
        self.middleware.consume()

        # Declare and subscribe to the reviews exchange
        print("Voy a recibir los reviews")
        # For Q3
        self.middleware.define_exchange(self.input_reviews_q3, {'q3_reviews': ['q3_reviews']})
        self.middleware.subscribe(self.input_reviews_q3, 'q3_reviews', callback_with_params_reviews_q3)
        # For Q5
        self.middleware.receive_messages(self.input_reviews_q5, callback_with_params_reviews_q5)
        self.middleware.consume()

        self.middleware.close_connection()