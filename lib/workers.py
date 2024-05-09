from middleware import Middleware
from filters import filter_by, eof_manage_process, hash_title, accumulate_authors_decades, different_decade_counter, titles_in_the_n_percentile, get_top_n, calculate_review_sentiment, review_quantity_value
from serialization import serialize_message, serialize_dict, deserialize_titles_message
import signal
import queue

YEAR_CONDITION = 'YEAR'
TITLE_CONDITION = 'TITLE'
CATEGORY_CONDITION = 'CATEGORY'
QUERY_5 = 5
QUERY_3 = 3
BATCH_SIZE = 100

class FilterWorker:

    def __init__(self, id, input_name, output_name, eof_queue, workers_quantity, next_workers_quantity):
        signal.signal(signal.SIGTERM, self.handle_signal)

        self.id = id
        self.eof_queue = eof_queue
        self.input_name = input_name
        self.output_name = output_name
        self.workers_quantity = workers_quantity
        self.next_workers_quantity = next_workers_quantity
        self.middleware = None
        self.queue = queue.Queue()
        try:
            middleware = Middleware(self.queue)
        except Exception as e:
            raise e
        self.middleware = middleware
        
        self.stop_worker = False
        self.filtered_results_quantity = 0

    def handle_signal(self, *args):
        print("Gracefully exit")
        self.queue.put('SIGTERM')
        self.stop_worker = True
        if self.middleware != None:
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
        try:
            # Declare the output
            print("Voy a leer titulos")
            if not self.output_name.startswith("QUEUE_"):
                self.middleware.define_exchange(self.output_name, {self.exchange_queue: [self.exchange_queue]})

            # Read the data
            self.middleware.receive_messages(self.input_name, callback_with_params)

            self.middleware.consume()

            print(f'El worker se quedo con {self.filtered_results_quantity} cantidad de titulos')
            # Once received the EOF, if I am the leader (WORKER_ID == 0), propagate the EOF to the next filter
            # after receiving WORKER_QUANTITY EOF messages.
            eof_manage_process(self.id, self.workers_quantity, self.next_workers_quantity, self.output_name, self.middleware, self.eof_queue)

            self.middleware.close_connection()

        except Exception as e:
            if self.stop_worker:
                print("Gracefully exited")
            else:
                print("An errror ocurred: ", e)
            return



class HashWorker:

    def __init__(self, input_titles_q3, input_titles_q5, input_reviews_q3, input_reviews_q5, output, hash_modulus):
        signal.signal(signal.SIGTERM, self.handle_signal)
        self.stop_worker = False
        self.input_titles_q3 = input_titles_q3
        self.input_titles_q5 = input_titles_q5
        self.input_reviews_q3 = input_reviews_q3
        self.input_reviews_q5 = input_reviews_q5
        self.output = output
        self.hash_modulus = hash_modulus
        self.middleware = None
        self.queue = queue.Queue()
        try:
            middleware = Middleware(self.queue)
        except Exception as e:
            raise e
        self.middleware = middleware

    def handle_signal(self, *args):
        print("Gracefully exit")
        self.queue.put('SIGTERM')
        self.stop_worker = True
        if self.middleware != None:
            self.middleware.close_connection()

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
        try:

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
            self.middleware.receive_messages(self.input_reviews_q3, callback_with_params_reviews_q3)
            # For Q5
            self.middleware.receive_messages(self.input_reviews_q5, callback_with_params_reviews_q5)
            self.middleware.consume()

            self.middleware.close_connection()
        except Exception as e:
            if self.stop_worker:
                print("Gracefully exited")
            else:
                print("An errror ocurred: ", e)
            return

class JoinWorker:

    def __init__(self, id, input_name, output_name, eof_quantity, query):
        signal.signal(signal.SIGTERM, self.handle_signal)
        self.stop_worker = False

        if query != QUERY_5 and query != QUERY_3:
            raise Exception('Query not supported')

        self.id = id
        self.input_name = input_name
        self.output_name = output_name
        self.eof_counter = 0
        self.eof_quantity = eof_quantity
        self.counter_dict = {}
        self.query = query
        self.middleware = None
        self.queue = queue.Queue()
        try:
            middleware = Middleware(self.queue)
        except Exception as e:
            raise e
        self.middleware = middleware
    
    def handle_signal(self, *args):
        print("Gracefully exit")
        self.queue.put('SIGTERM')
        self.stop_worker = True
        if self.middleware != None:
            self.middleware.close_connection()

    def handle_titles_data(self, method, body):
        if body == b'EOF':
            self.eof_counter += 1
            if self.eof_counter == self.eof_quantity:
                self.middleware.stop_consuming()
            self.middleware.ack_message(method)
            return
        data = deserialize_titles_message(body)

        for row_dictionary in data:
            title = row_dictionary['Title']
            if self.query == QUERY_5:
                self.counter_dict[title] = [0,0] # [reviews_quantity, review_sentiment_summation]
            else:
                self.counter_dict[title] = [0, 0, row_dictionary['authors']] # [reviews_quantity, ratings_summation, authors]
        
        self.middleware.ack_message(method)

    def handle_reviews_data(self, method, body):
        if body == b'EOF':
            self.eof_counter += 1
            if self.eof_counter == self.eof_quantity:
                self.middleware.stop_consuming()
            self.middleware.ack_message(method)
            return
        data = deserialize_titles_message(body)

        for row_dictionary in data:
            title = row_dictionary['Title']
            if title not in self.counter_dict:
                continue
            
            try:
                if self.query == QUERY_5:
                    parsed_value = self.parse_text_sentiment(row_dictionary['text_sentiment'])
                else:
                    parsed_value = self.parse_review_rating(row_dictionary['review/score'])
            except:
                continue

            counter = self.counter_dict[title]
            counter[0] += 1
            counter[1] += parsed_value 
            self.counter_dict[title] = counter
        
        self.middleware.ack_message(method)

    def parse_text_sentiment(self, text_sentiment):
        try:
            text_sentiment = float(text_sentiment)
        except Exception as e:
            print(f"Error: [{e}] when parsing 'text_sentiment' to float.")
            raise e
        return text_sentiment
        
    def parse_review_rating(self, rating):
        try:
            title_rating = float(rating)
        except Exception as e:
            print(f"Error: [{e}] when parsing 'review/score' to float.")
            raise e
        return title_rating
            
    def run(self):

        # Define a callback wrappers
        callback_with_params_titles = lambda ch, method, properties, body: self.handle_titles_data(method, body)
        callback_with_params_reviews = lambda ch, method, properties, body: self.handle_reviews_data(method, body)

        # The name of the queues the worker will read data
        titles_queue = self.id + '_titles_Q' + str(self.query)
        reviews_queue = self.id + '_reviews_Q' + str(self.query)

        try:
            # Declare and subscribe to the titles queue in the exchange
            self.middleware.define_exchange(self.input_name, {titles_queue: [titles_queue], reviews_queue: [reviews_queue]})

            # Read from the titles queue
            print("Voy a leer titles")
            self.middleware.subscribe(self.input_name, titles_queue, callback_with_params_titles)
            self.middleware.consume()

            self.eof_counter = 0 

            # Read from the reviews queue
            print("Voy a leer reviews")
            self.middleware.subscribe(self.input_name, reviews_queue,  callback_with_params_reviews)
            self.middleware.consume()


            # Once all the reviews were received, the counter_dict needs to be sent to the next stage
            batch_size = 0
            batch = {}
            for title, counter in self.counter_dict.items():
                # Ignore titles with no reviews
                if counter[0] == 0:
                    continue

                if self.query == QUERY_5:
                    batch[title] = str(counter[1] / counter[0])
                else:
                    batch[title] = counter

                batch_size += 1
                if batch_size == BATCH_SIZE: # TODO: Maybe the 100 could be an env var
                    serialized_message = serialize_message([serialize_dict(batch)])
                    self.middleware.send_message(self.output_name, serialized_message)
                    batch = {}
                    batch_size = 0

            if len(batch) != 0:
                serialized_message = serialize_message([serialize_dict(batch)])
                self.middleware.send_message(self.output_name, serialized_message)
            
            self.middleware.send_message(self.output_name, "EOF")
            
            self.middleware.close_connection()
        except Exception as e:
            if self.stop_worker:
                print("Gracefully exited")
            else:
                print("An errror ocurred: ", e)
            return

class DecadeWorker:

    def __init__(self, input_name, output_name):
        signal.signal(signal.SIGTERM, self.handle_signal)
        self.stop_worker = False
        self.input_name = input_name
        self.output_name = output_name
        self.middleware = None
        self.queue = queue.Queue()
        try:
            middleware = Middleware(self.queue)
        except Exception as e:
            raise e
        self.middleware = middleware

    def handle_signal(self, *args):
        print("Gracefully exit")
        self.queue.put('SIGTERM')
        self.stop_worker = True
        if self.middleware != None:
            self.middleware.close_connection()

    def handle_data(self, method, body):
        if body == b'EOF':
            self.middleware.stop_consuming()
            self.middleware.send_message(self.output_name, "EOF")
            self.middleware.ack_message(method)
            return
        data = deserialize_titles_message(body)

        desired_data = different_decade_counter(data)
        if not desired_data:
            self.middleware.ack_message(method)
            return

        serialized_data = serialize_message([serialize_dict(filtered_dictionary) for filtered_dictionary in desired_data])
        self.middleware.send_message(self.output_name, serialized_data)

        self.middleware.ack_message(method)

    def run(self):
        # Define a callback wrapper
        callback_with_params = lambda ch, method, properties, body: self.handle_data(method, body)

        try:
            # Declare and subscribe to the titles exchange
            self.middleware.receive_messages(self.input_name, callback_with_params)
            self.middleware.consume()

            self.middleware.close_connection()
        except Exception as e:
            if self.stop_worker:
                print("Gracefully exited")
            else:
                print("An errror ocurred: ", e)
            return


class GlobalDecadeWorker:

    def __init__(self, input_name, output_name, eof_quantity):
        signal.signal(signal.SIGTERM, self.handle_signal)
        self.stop_worker = False
        self.input_name = input_name
        self.output_name = output_name
        self.eof_quantity = eof_quantity
        self.eof_counter = 0
        self.counter_dict = {}
        self.middleware = None
        self.queue = queue.Queue()
        try:
            middleware = Middleware(self.queue)
        except Exception as e:
            raise e
        self.middleware = middleware

    def handle_signal(self, *args):
        print("Gracefully exit")
        self.queue.put('SIGTERM')
        self.stop_worker = True
        if self.middleware != None:
            self.middleware.close_connection()

    def handle_data(self, method, body):
        if body == b'EOF':
            self.eof_counter += 1
            if self.eof_counter == self.eof_quantity:
                self.middleware.stop_consuming()
            self.middleware.ack_message(method)
            return
        data = deserialize_titles_message(body)

        accumulate_authors_decades(data, self.counter_dict)

        self.middleware.ack_message(method)

    def run(self):
        # Define a callback wrapper
        callback_with_params = lambda ch, method, properties, body: self.handle_data(method, body)
        
        try:
            # Declare the output queue
            self.middleware.receive_messages(self.input_name, callback_with_params)
            self.middleware.consume()

            # Collect the results
            results = []
            for key, value in self.counter_dict.items():
                if len(value) >= 10:
                    results.append(key)
            # Send the results to the output queue
            serialized_message = serialize_message(results)
            
            self.middleware.send_message(self.output_name, serialized_message)
            self.middleware.send_message(self.output_name, 'EOF')

            self.middleware.close_connection()
        except Exception as e:
            if self.stop_worker:
                print("Gracefully exited")
            else:
                print("An errror ocurred: ", e)
            return
        
class PercentileWorker:

    def __init__(self, input_name, output_name, percentile, eof_quantity):
        signal.signal(signal.SIGTERM, self.handle_signal)
        self.stop_worker = False
        
        self.input_name = input_name
        self.output_name = output_name
        self.percentile = percentile
        self.eof_quantity = eof_quantity
        self.eof_counter = 0
        self.titles_with_sentiment = {}
        self.middleware = None
        self.queue = queue.Queue()
        try:
            middleware = Middleware(self.queue)
        except Exception as e:
            raise e
        self.middleware = middleware

    def handle_signal(self, *args):
        print("Gracefully exit")
        self.queue.put('SIGTERM')
        self.stop_worker = True
        if self.middleware != None:
            self.middleware.close_connection()

    def handle_data(self, method, body):
        if body == b'EOF':
            self.eof_counter += 1
            if self.eof_counter == self.eof_quantity:
                self.middleware.stop_consuming()
            self.middleware.ack_message(method)
            return
        
        data = deserialize_titles_message(body)
        
        for key, value in data[0].items():
            self.titles_with_sentiment[key] = float(value)

        self.middleware.ack_message(method)

    def run(self):
        # Define a callback wrapper
        callback_with_params = lambda ch, method, properties, body: self.handle_data(method, body)
        
        try:
            # Read the titles with their sentiment
            self.middleware.receive_messages(self.input_name, callback_with_params)
            self.middleware.consume()


            titles = titles_in_the_n_percentile(self.titles_with_sentiment, self.percentile)

            serialized_data = serialize_message(titles)
            self.middleware.send_message(self.output_name, serialized_data)
            self.middleware.send_message(self.output_name, "EOF")

            self.middleware.close_connection()
        except Exception as e:
            if self.stop_worker:
                print("Gracefully exited")
            else:
                print("An errror ocurred: ", e)
            return

class TopNWorker:

    def __init__(self, input_name, output_name, eof_quantity, n, last):
        signal.signal(signal.SIGTERM, self.handle_signal)
        self.stop_worker = False
        
        self.input_name = input_name
        self.output_name = output_name
        self.top_n = n
        self.last = last
        self.eof_quantity = eof_quantity
        self.eof_counter = 0
        self.top = []
        self.middleware = None
        self.queue = queue.Queue()
        try:
            middleware = Middleware(self.queue)
        except Exception as e:
            raise e
        self.middleware = middleware

    def handle_signal(self, *args):
        print("Gracefully exit")
        self.queue.put('SIGTERM')
        self.stop_worker = True
        if self.middleware != None:
            self.middleware.close_connection()

    def handle_data(self, method, body):
        if body == b'EOF':
            self.eof_counter += 1
            if self.last and self.eof_counter == self.eof_quantity:
                self.middleware.stop_consuming()
            elif not self.last:
                self.middleware.stop_consuming()
            self.middleware.ack_message(method)
            return
        data = deserialize_titles_message(body)

        self.top = get_top_n(data, self.top, self.top_n, self.last)
        self.middleware.ack_message(method)        

    def run(self):
        # Define a callback wrapper
        callback_with_params = lambda ch, method, properties, body: self.handle_data(method, body)
        
        try:
            self.middleware.receive_messages(self.input_name, callback_with_params)
            self.middleware.consume()

            dict_to_send = {title:str(mean_rating) for title,mean_rating in self.top}
            serialized_data = serialize_message([serialize_dict(dict_to_send)])
            if not self.last:
                if len(self.top) != 0:
                    self.middleware.send_message(self.output_name, serialized_data)

                self.middleware.send_message(self.output_name, 'EOF')
            else:
                # Send the results to the query_coordinator
                self.middleware.send_message(self.output_name, serialized_data)
                self.middleware.send_message(self.output_name, 'EOF')

            self.middleware.close_connection()
        except Exception as e:
            if self.stop_worker:
                print("Gracefully exited")
            else:
                print("An errror ocurred: ", e)
            return

class ReviewSentimentWorker:
    
    def __init__(self, input_name, output_name, worker_id, workers_quantity, next_workers_quantity, eof_queue):
        signal.signal(signal.SIGTERM, self.handle_signal)
        self.stop_worker = False
        
        self.input_name = input_name
        self.output_name = output_name
        self.worker_id = worker_id
        self.workers_quantity = workers_quantity
        self.next_workers_quantity = next_workers_quantity
        self.eof_queue = eof_queue
        self.middleware = None
        self.queue = queue.Queue()
        try:
            middleware = Middleware(self.queue)
        except Exception as e:
            raise e
        self.middleware = middleware

    def handle_signal(self, *args):
        print("Gracefully exit")
        self.queue.put('SIGTERM')
        self.stop_worker = True
        if self.middleware != None:
            self.middleware.close_connection()

    def handle_data(self, method, body):
        if body == b'EOF':
            self.middleware.stop_consuming()
            self.middleware.ack_message(method)
            return
        data = deserialize_titles_message(body)

        desired_data = calculate_review_sentiment(data)
        serialized_data = serialize_message([serialize_dict(filtered_dict) for filtered_dict in desired_data])
        self.middleware.send_message(self.output_name, serialized_data)
        
        self.middleware.ack_message(method)

    def run(self):
        # Define a callback wrapper
        callback_with_params = lambda ch, method, properties, body: self.handle_data(method, body)

        try:
            # Declare and subscribe to the titles exchange
            self.middleware.receive_messages(self.input_name, callback_with_params)
            self.middleware.consume()

            eof_manage_process(self.worker_id, self.workers_quantity, self.next_workers_quantity, self.output_name, self.middleware, self.eof_queue)

            self.middleware.close_connection()
        except Exception as e:
            if self.stop_worker:
                print("Gracefully exited")
            else:
                print("An errror ocurred: ", e)
            return
        
class FilterReviewsWorker:
    
    def __init__(self, input_name, output_name1, output_name2, minimum_quantity, eof_quantity, next_workers_quantity):
        signal.signal(signal.SIGTERM, self.handle_signal)

        self.input_name = input_name
        self.output_name1 = output_name1
        self.output_name2 = output_name2
        self.eof_quantity = eof_quantity
        self.minimum_quantity = minimum_quantity
        self.next_workers_quantity = next_workers_quantity
        self.eof_counter = 0
        self.filtered_titles = {}
        self.middleware = None
        self.queue = queue.Queue()
        try:
            middleware = Middleware(self.queue)
        except Exception as e:
            raise e
        self.middleware = middleware
        
        self.stop_worker = False
        self.filtered_results_quantity = 0

    def handle_signal(self, *args):
        print("Gracefully exit")
        self.queue.put('SIGTERM')
        self.stop_worker = True
        if self.middleware != None:
            self.middleware.close_connection()


    def handle_data(self, method, body):
        if body == b'EOF':
            self.eof_counter += 1
            if self.eof_counter == self.eof_quantity:
                self.middleware.stop_consuming()
            self.middleware.ack_message(method)
            return
        
        data = deserialize_titles_message(body)
        
        desired_data = review_quantity_value(data, self.minimum_quantity)
        for key, value in desired_data[0].items():
            self.filtered_titles[key] = value
    
        self.middleware.ack_message(method)

    def run(self):
        # Define a callback wrapper
        callback_with_params = lambda ch, method, properties, body: self.handle_data(method, body)
        try:

            # Declare and subscribe to the titles exchange
            self.middleware.receive_messages(self.input_name, callback_with_params)
            self.middleware.consume()

            serialized_data = serialize_message([serialize_dict(self.filtered_titles)])
            if serialized_data != '':
                self.middleware.send_message(self.output_name1, serialized_data)
                self.middleware.send_message(self.output_name2, serialized_data)

            for _ in range(self.next_workers_quantity):
                self.middleware.send_message(self.output_name2, 'EOF')

            
            self.middleware.send_message(self.output_name1, 'EOF')

            self.middleware.close_connection()
        except Exception as e:
            if self.stop_worker:
                print("Gracefully exited")
            else:
                print("An errror ocurred: ", e)
            return