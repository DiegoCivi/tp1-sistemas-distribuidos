from middleware import Middleware
from filters import filter_by, accumulate_authors_decades, different_decade_counter, titles_in_the_n_percentile, get_top_n, calculate_review_sentiment, review_quantity_value, COUNTER_FIELD
from serialization import *
import signal
import queue

YEAR_CONDITION = 'YEAR'
TITLE_CONDITION = 'TITLE'
CATEGORY_CONDITION = 'CATEGORY'
QUERY_5 = 5
QUERY_3 = 3
BATCH_SIZE = 100
QUERY_COORDINATOR_QUANTITY = 1

class Worker:

    def _create_batches(self, batch, next_workers_quantity):
        raise Exception('Function needs to be implemented')

    def _send_batches(self, workers_batches, output_queue, client_id):
        raise Exception('Function needs to be implemented')

    def create_and_send_batches(self, batch, client_id, output_queue, next_workers_quantity):
        workers_batches = self._create_batches(batch, next_workers_quantity)

        self._send_batches(workers_batches, output_queue, client_id)

    def send_EOFs(self, client_id, output_queue, next_workers_quantity):
        eof_msg = create_EOF(client_id, self.worker_id)
        for worker_id in range(next_workers_quantity):
            worker_queue = create_queue_name(output_queue, str(worker_id))
            self.middleware.send_message(worker_queue, eof_msg)

    def manage_EOF(self, body, method):
        raise Exception('Function needs to be implemented')
    
    def manage_message(self, client_id, data, method):
        raise Exception('Function needs to be implemented')
    
    def add_EOF_worker_id(self, client_id, worker_id):
        client_eof_workers_ids = self.eof_workers_ids.get(client_id, set())
        client_eof_workers_ids.add(worker_id)
        self.eof_workers_ids[client_id] = client_eof_workers_ids

    def handle_data(self, method, body):            
        if is_EOF(body):
            worker_id = get_EOF_worker_id(body)                                 # The id of the worker that sent the EOF
            client_id = get_EOF_client_id(body)                                 # The id of the active client
            client_eof_workers_ids = self.eof_workers_ids.get(client_id, set()) # A set with the ids of the workers that already sent their EOF for this client
            
            # Check if the EOF was already received from that worker (This is done to handle duplicated EOFs). 
            # If already received, the EOF is inmediately acked.
            # If not, the workers id is saved and the handling of the EOF is done.
            if worker_id not in client_eof_workers_ids:
                self.add_EOF_worker_id(client_id, worker_id)
                self.manage_EOF(body, method, client_id)

            self.middleware.ack_message(method)
            return
        
        client_id, data = deserialize_titles_message(body)

        self.manage_message(client_id, data, method)

        self.middleware.ack_message(method)
            
    def run(self):
        callback_with_params = lambda ch, method, properties, body: self.handle_data(method, body)
        try:
            # Read the data
            self.middleware.receive_messages(self.input_name, callback_with_params)
            self.middleware.consume()
        except Exception as e:
            if self.stop_worker:
                print("Gracefully exited")
            else:
                raise e


class FilterWorker(Worker):

    def __init__(self, id, input_name, output_name, eof_queue, workers_quantity, next_workers_quantity, iteration_queue, eof_quantity, last):
        signal.signal(signal.SIGTERM, self.handle_signal)

        self.worker_id = id
        self.last = last
        self.eof_queue = eof_queue
        self.input_name = create_queue_name(input_name, id) 
        self.iteration_queue = iteration_queue
        self.eof_counter = {}
        self.eof_workers_ids = {} # This dict stores for each active client, the workers ids of the eofs received.
        self.eof_quantity = eof_quantity
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

    def _send_batches(self, workers_batches, output_queue, client_id):
        for worker_id, batch in workers_batches.items():
            serialized_batch = serialize_batch(batch)
            serialized_message = serialize_message(serialized_batch, client_id)
            worker_queue = create_queue_name(output_queue, worker_id)
            self.middleware.send_message(worker_queue, serialized_message)

    def _create_batches(self, batch, next_workers_quantity):
        workers_batches = {}
        for row in batch:
            hashed_title = hash_title(row['Title'])
            choosen_worker = str(hashed_title % next_workers_quantity)
            if choosen_worker not in workers_batches:
                workers_batches[choosen_worker] = []
            workers_batches[choosen_worker].append(row)

        return workers_batches

    def manage_EOF(self, body, method, client_id):
        #client_id = get_EOF_client_id(body)
        self.eof_counter[client_id] = self.eof_counter.get(client_id, 0) + 1
        if self.eof_quantity == self.eof_counter[client_id]:
            self.send_EOFs(client_id, self.output_name, self.next_workers_quantity)
            del self.eof_counter[client_id]

    def manage_message(self, client_id, data, method):
        desired_data = filter_by(data, self.filtering_function, self.filter_value)
        if not desired_data:
            return

        self.filtered_results_quantity += len(desired_data)

        # Create batches for each worker in the next stage and send those batches
        self.create_and_send_batches(desired_data, client_id, self.output_name, self.next_workers_quantity)


class JoinWorker:

    def __init__(self, id, input_titles_name, input_reviews_name, output_name, eof_quantity_titles, eof_quantity_reviews, query, iteration_queue):
        signal.signal(signal.SIGTERM, self.handle_signal)
        self.stop_worker = False

        if query != QUERY_5 and query != QUERY_3:
            raise Exception('Query not supported')

        self.worker_id = id
        self.input_titles_name = create_queue_name(input_titles_name, id)
        self.input_reviews_name = create_queue_name(input_reviews_name, id) 
        self.output_name = output_name
        self.iteration_queue = iteration_queue
        self.eof_counter_titles = {}
        self.eof_counter_reviews = {}
        self.eof_workers_ids_titles = {} # This dict stores for each active client, the workers ids of the eofs received in the titles queue
        self.eof_workers_ids_reviews = {} # This dict stores for each active client, the workers ids of the eofs received in the reviews queue
        self.eof_quantity_titles = eof_quantity_titles
        self.eof_quantity_reviews = eof_quantity_reviews
        self.counter_dicts = {}
        self.leftover_reviews = {}
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
        if is_EOF(body):
            print("Me llego un EOF en titles")
            worker_id = get_EOF_worker_id(body)
            client_id = get_EOF_client_id(body)
            client_eof_workers_ids = self.eof_workers_ids_titles.get(client_id, set())
            if worker_id not in client_eof_workers_ids:
                client_eof_workers_ids.add(worker_id)
                self.eof_workers_ids_titles[client_id] = client_eof_workers_ids

                self.eof_counter_titles[client_id] = self.eof_counter_titles.get(client_id, 0) + 1
                self.eof_counter_reviews[client_id] = self.eof_counter_reviews.get(client_id, 0)
                if self.eof_counter_titles[client_id] == self.eof_quantity_titles and self.eof_counter_reviews[client_id] == self.eof_quantity_reviews:
                    self.send_results(client_id)

            self.middleware.ack_message(method)
            return
        client_id, data = deserialize_titles_message(body)

        if client_id not in self.counter_dicts:
            self.counter_dicts[client_id] = {}

        for row_dictionary in data:
            title = row_dictionary['Title']
            if self.query == QUERY_5:
                self.counter_dicts[client_id][title] = [0,0] # [reviews_quantity, review_sentiment_summation]
            else:
                self.counter_dicts[client_id][title] = [0, 0, row_dictionary['authors']] # [reviews_quantity, ratings_summation, authors]

        self.middleware.ack_message(method)

    def handle_reviews_data(self, method, body):
        if is_EOF(body):
            worker_id = get_EOF_worker_id(body)
            client_id = get_EOF_client_id(body)
            client_eof_workers_ids = self.eof_workers_ids_reviews.get(client_id, set())
            if worker_id not in client_eof_workers_ids:
                client_eof_workers_ids.add(worker_id)
                self.eof_workers_ids_reviews[client_id] = client_eof_workers_ids

                self.eof_counter_reviews[client_id] = self.eof_counter_reviews.get(client_id, 0) + 1
                self.eof_counter_titles[client_id] = self.eof_counter_titles.get(client_id, 0)
                if self.eof_counter_titles[client_id] == self.eof_quantity_titles and self.eof_counter_reviews[client_id] == self.eof_quantity_reviews:
                    self.send_results(client_id)

            self.middleware.ack_message(method)
            return
        client_id, data = deserialize_titles_message(body)

        if client_id not in self.counter_dicts:
            self.counter_dicts[client_id] = {}

        self.add_review(client_id, data)

        self.middleware.ack_message(method)

    def add_review(self, client_id, batch):
        for row_dictionary in batch:
            title = row_dictionary['Title']

            if client_id in self.eof_counter_titles  and self.eof_counter_titles[client_id] == self.eof_quantity_titles and title not in self.counter_dicts[client_id]:
                # If all the titles already arrived and the title of this review has been already filtered,
                # then this review has to be ignored.
                continue
            elif title not in self.counter_dicts[client_id]:
                # If the titles didnt finished to arrive and the title of this review is not in the counter_dict,
                # we have to save the review to check later if the title has been filtered or not
                if client_id not in self.leftover_reviews:
                    self.leftover_reviews[client_id] = []
                self.leftover_reviews[client_id].append(row_dictionary)
                continue

            try:
                if self.query == QUERY_5:
                    parsed_value = self.parse_text_sentiment(row_dictionary['text_sentiment'])
                else:
                    parsed_value = self.parse_review_rating(row_dictionary['review/score'])
            except:
                continue

            counter = self.counter_dicts[client_id][title]
            counter[0] += 1
            counter[1] += parsed_value
            self.counter_dicts[client_id][title] = counter

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

    def check_leftover_reviews(self, client_id):
        if client_id in self.leftover_reviews:
            self.add_review(client_id, self.leftover_reviews[client_id])

    def send_results(self, client_id):
        # Check if there are leftover reviews that need to be added to the counter_dict
        self.check_leftover_reviews(client_id)

        # Send batch
        batch_size = 0
        batch = {}
        for title, counter in self.counter_dicts[client_id].items():
            # Ignore titles with no reviews
            if counter[0] == 0:
                continue

            # Serialization of the message depends on which query is using the JoinWorker
            if self.query == QUERY_5:
                batch[title] = str(counter[1] / counter[0])
            else:
                batch[title] = counter

            # Once the batch reached the BATCH_SIZE. It can be sent.
            batch_size += 1
            if batch_size == BATCH_SIZE:
                serialized_message = serialize_message([serialize_dict(batch)], client_id)
                self.middleware.send_message(self.output_name, serialized_message)
                batch = {}
                batch_size = 0

        # If the for loop ended with a batch that was never sent, send it
        if len(batch) != 0:
            serialized_message = serialize_message([serialize_dict(batch)], client_id)
            self.middleware.send_message(self.output_name, serialized_message)

        # Finally, send the EOF
        eof_msg = create_EOF(client_id, self.worker_id)
        self.middleware.send_message(self.output_name, eof_msg)

    def run(self):

        # Define a callback wrappers
        callback_with_params_titles = lambda ch, method, properties, body: self.handle_titles_data(method, body)
        callback_with_params_reviews = lambda ch, method, properties, body: self.handle_reviews_data(method, body)

        try:
            # Titles messages
            self.middleware.receive_messages(self.input_titles_name, callback_with_params_titles)
            # Reviews messages
            self.middleware.receive_messages(self.input_reviews_name, callback_with_params_reviews)

            self.middleware.consume()

        except Exception as e:
            if self.stop_worker:
                print("Gracefully exited")
            else:
                raise e


class DecadeWorker(Worker):

    def __init__(self, input_name, output_name, iteration_queue, worker_id, next_workers_quantity):
        signal.signal(signal.SIGTERM, self.handle_signal)
        
        self.worker_id = worker_id
        self.next_workers_quantity = next_workers_quantity
        self.stop_worker = False
        self.input_name = create_queue_name(input_name, worker_id)
        self.output_name = output_name
        self.iteration_queue = iteration_queue
        self.middleware = None
        self.queue = queue.Queue()
        try:
            middleware = Middleware(self.queue)
        except Exception as e:
            raise e
        self.middleware = middleware

        self.eof_workers_ids = {} # This dict stores for each active client, the workers ids of the eofs received.

    def handle_signal(self, *args):
        print("Gracefully exit")
        self.queue.put('SIGTERM')
        self.stop_worker = True
        if self.middleware != None:
            self.middleware.close_connection()

    def _create_batches(self, batch, next_workers_quantity):
        workers_batches = {}
        for worker_id in range(next_workers_quantity):
            workers_batches[str(worker_id)] = batch

        return workers_batches
    
    def _send_batches(self, workers_batches, output_queue, client_id):
        for worker_id, batch in workers_batches.items():
            serialized_batch = serialize_batch(batch)
            serialized_message = serialize_message(serialized_batch, client_id)
            worker_queue = create_queue_name(output_queue, worker_id) # output_queue + '_' + worker_id
            self.middleware.send_message(worker_queue, serialized_message)
    
    def manage_EOF(self, body, method, client_id):
        #client_id = get_EOF_client_id(body)
        self.send_EOFs(client_id, self.output_name, self.next_workers_quantity)

    def manage_message(self, client_id, data, method):
        desired_data = different_decade_counter(data)
        if not desired_data:
            return

        self.create_and_send_batches(desired_data, client_id, self.output_name, self.next_workers_quantity)

    def handle_ok(self, method, body):
        """
        If an 'OK' was received, it means we can continue to the next iteration
        """
        self.middleware.ack_message(method)
        self.middleware.stop_consuming()


class GlobalDecadeWorker(Worker):

    def __init__(self, worker_id, input_name, output_name, eof_quantity, iteration_queue, next_workers_quantity):
        signal.signal(signal.SIGTERM, self.handle_signal)

        self.worker_id = worker_id
        self.stop_worker = False
        self.input_name = create_queue_name(input_name, worker_id)
        self.output_name = output_name
        self.eof_quantity = eof_quantity
        self.iteration_queue = iteration_queue
        self.next_workers_quantity = next_workers_quantity
        self.eof_counter = {}
        self.eof_workers_ids = {} # This dict stores for each active client, the workers ids of the eofs received.
        self.counter_dicts = {}
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

    def _send_batches(self, workers_batches, output_queue, client_id):
        for worker_id, batch in workers_batches.items():
            worker_queue = create_queue_name(output_queue, worker_id) #output_queue + '_' + worker_id
            print("Voy a mandar el batch a la cola: ", worker_queue)
            self.middleware.send_message(worker_queue, batch)

    def _create_batches(self, batch, next_workers_quantity):
        workers_batches = {}
        for worker_id in range(next_workers_quantity):
            workers_batches[str(worker_id)] = batch

        return workers_batches

    def send_client_results(self, client_id):
        # Collect the results
        results = {'results': []}
        for key, value in self.counter_dicts[client_id].items():
            if len(value) >= 10:
                results['results'].append(key)
        # Send the results to the output queue
        serialized_dict = serialize_batch([results])
        serialized_message = serialize_message(serialized_dict, client_id)

        self.create_and_send_batches(serialized_message, client_id, self.output_name, self.next_workers_quantity)
        self.send_EOFs(client_id, self.output_name, self.next_workers_quantity)
    
    def manage_EOF(self, body, method, client_id):
        #client_id = get_EOF_client_id(body)
        self.eof_counter[client_id] = self.eof_counter.get(client_id, 0) + 1
        if self.eof_counter[client_id] == self.eof_quantity:
            self.send_client_results(client_id)

    def manage_message(self, client_id, data, method):
        if client_id not in self.counter_dicts:
            self.counter_dicts[client_id] = {}

        accumulate_authors_decades(data, self.counter_dicts[client_id])


class PercentileWorker(Worker):

    def __init__(self, worker_id, input_name, output_name, percentile, eof_quantity, iteration_queue, next_workers_quantity):
        signal.signal(signal.SIGTERM, self.handle_signal)
        
        self.worker_id = worker_id
        self.stop_worker = False
        self.input_name = input_name
        self.next_workers_quantity = next_workers_quantity
        self.output_name = output_name
        self.iteration_queue = iteration_queue
        self.percentile = percentile
        self.eof_quantity = eof_quantity
        self.eof_counter = {}
        self.eof_workers_ids = {} # This dict stores for each active client, the workers ids of the eofs received.
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

    def manage_EOF(self, body, method, client_id):
        # client_id = get_EOF_client_id(body)
        self.eof_counter[client_id] = self.eof_counter.get(client_id, 0) + 1
        if self.eof_counter[client_id] == self.eof_quantity:
            self.send_results(client_id)

    def manage_message(self, client_id, data, method):
        if client_id not in self.titles_with_sentiment:
            self.titles_with_sentiment[client_id] = {}

        for title, sentiment_value in data[0].items():
            self.titles_with_sentiment[client_id][title] = float(sentiment_value)

    def send_results(self, client_id):
        titles = titles_in_the_n_percentile(self.titles_with_sentiment[client_id], self.percentile)
        titles = [{'results': titles}] # This needs to be done so it can be serialized correctly
        self.create_and_send_batches(titles, client_id, self.output_name, self.next_workers_quantity)

        self.send_EOFs(client_id, self.output_name, self.next_workers_quantity)

        print(len(titles[0]['results']))

    def _send_batches(self, workers_batches, output_queue, client_id):
        for worker_id, batch in workers_batches.items():
            serialized_batch = serialize_batch(batch)
            serialized_message = serialize_message(serialized_batch, client_id)
            worker_queue = create_queue_name(output_queue, worker_id) # output_queue + '_' + worker_id
            self.middleware.send_message(worker_queue, serialized_message)

    def _create_batches(self, batch, next_workers_quantity):
        workers_batches = {}
        for worker_id in range(next_workers_quantity):
            workers_batches[str(worker_id)] = batch

        return workers_batches


class TopNWorker(Worker):

    def __init__(self, id, input_name, output_name, eof_quantity, n, last, iteration_queue, next_workers_quantity):
        signal.signal(signal.SIGTERM, self.handle_signal)
        
        self.worker_id = id
        self.stop_worker = False
        self.input_name = create_queue_name(input_name, id)
        self.output_name = output_name
        self.next_workers_quantity = next_workers_quantity
        self.top_n = n
        self.last = last
        self.eof_quantity = eof_quantity
        self.iteration_queue = iteration_queue
        self.eof_counter = {}
        self.eof_workers_ids = {} # This dict stores for each active client, the workers ids of the eofs received.
        self.tops = {}
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
        print(self.eof_counter)

    def send_results(self, client_id):
        if not self.last:
            if client_id in self.tops:
                self.parse_top(client_id)
                # If the top isnt empty, then we send it
                self.create_and_send_batches(self.tops[client_id], client_id, self.output_name, self.next_workers_quantity)

            self.send_EOFs(client_id, self.output_name, self.next_workers_quantity)

        else:
            if client_id in self.tops:
                # Send the results to the query_coordinator
                self.create_and_send_batches(self.tops[client_id], client_id, self.output_name, self.next_workers_quantity)

            self.send_EOFs(client_id, self.output_name, self.next_workers_quantity)

    def manage_EOF(self, body, method, client_id):
        # client_id = get_EOF_client_id(body)
        self.eof_counter[client_id] = self.eof_counter.get(client_id, 0) + 1
        if self.eof_counter[client_id] == self.eof_quantity:
            self.send_results(client_id)

    def manage_message(self, client_id, data, method):
        if client_id not in self.tops:
            self.tops[client_id] = []

        self.tops[client_id] = get_top_n(data, self.tops[client_id], self.top_n, self.last)

    def _send_batches(self, workers_batches, output_queue, client_id):
        for worker_id, batch in workers_batches.items():
            serialized_batch = serialize_batch(batch)
            serialized_message = serialize_message(serialized_batch, client_id)
            worker_queue = create_queue_name(output_queue, worker_id) #output_queue + '_' + worker_id
            self.middleware.send_message(worker_queue, serialized_message)

    def _create_batches(self, batch, next_workers_quantity):
        workers_batches = {}
        for row in batch:
            hashed_title = hash_title(row['Title'])
            choosen_worker = str(hashed_title % next_workers_quantity)
            if choosen_worker not in workers_batches:
                workers_batches[choosen_worker] = []
            workers_batches[choosen_worker].append(row)

        return workers_batches

    def parse_top(self, client_id):
        for title_dict in self.tops[client_id]:
            title_dict[COUNTER_FIELD] = str(title_dict[COUNTER_FIELD])


class ReviewSentimentWorker(Worker):

    def __init__(self, input_name, output_name, worker_id, workers_quantity, next_workers_quantity, eof_queue, iteration_queue):
        signal.signal(signal.SIGTERM, self.handle_signal)
        self.stop_worker = False

        self.input_name = create_queue_name(input_name, worker_id)
        self.output_name = output_name
        self.worker_id = worker_id
        self.workers_quantity = workers_quantity
        self.next_workers_quantity = next_workers_quantity
        self.eof_queue = eof_queue
        self.iteration_queue = iteration_queue
        self.eof_counter = 0
        self.middleware = None
        self.queue = queue.Queue()
        try:
            middleware = Middleware(self.queue)
        except Exception as e:
            raise e
        self.middleware = middleware
        self.eof_workers_ids = {} # This dict stores for each active client, the workers ids of the eofs received.

    def handle_signal(self, *args):
        print("Gracefully exit")
        self.queue.put('SIGTERM')
        self.stop_worker = True
        if self.middleware != None:
            self.middleware.close_connection()

    def create_and_send_batches(self, batch, client_id, output_queue=None):
        workers_batches = {}
        for row in batch:
            hashed_title = hash_title(row['Title'])
            choosen_worker = str(hashed_title % self.next_workers_quantity)
            if choosen_worker not in workers_batches:
                workers_batches[choosen_worker] = []
            workers_batches[choosen_worker].append(row)

        if output_queue == None:
            output_queue = self.output_name
        for worker_id, batch in workers_batches.items():
            serialized_batch = serialize_batch(batch)
            serialized_message = serialize_message(serialized_batch, client_id)
            worker_queue = create_queue_name(output_queue, worker_id)
            self.middleware.send_message(worker_queue, serialized_message)

    def manage_EOF(self, body, method, client_id):
        # client_id = get_EOF_client_id(body)
        self.send_EOFs(client_id, self.output_name, self.next_workers_quantity)
    
    def manage_message(self, client_id, data, method):
        desired_data = calculate_review_sentiment(data)
        self.create_and_send_batches(desired_data, client_id)


class FilterReviewsWorker(Worker):

    def __init__(self, worker_id, input_name, output_name1, output_name2, minimum_quantity, eof_quantity, next_workers_quantity, iteration_queue):
        signal.signal(signal.SIGTERM, self.handle_signal)

        self.worker_id = worker_id
        self.input_name = input_name
        self.output_name1 = output_name1
        self.iteration_queue = iteration_queue
        self.output_name2 = output_name2
        self.eof_quantity = eof_quantity
        self.minimum_quantity = minimum_quantity
        self.next_workers_quantity = next_workers_quantity
        self.eof_counter = {}
        self.eof_workers_ids = {} # This dict stores for each active client, the workers ids of the eofs received.
        self.filtered_client_titles = {}
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

    def send_results(self, client_id):
        # Send the results to the query 4 and the QueryCoordinator
        if len(self.filtered_client_titles[client_id]) != 0:
            # First to the Query Coordinator
            print("MANDO LOS RESULTADOS AL QC")
            self.create_and_send_batches(self.filtered_client_titles[client_id], client_id, self.output_name1, QUERY_COORDINATOR_QUANTITY) # next_workers_quantity parameter is set to 1 because there is only 1 QueryCoordinator
            # Then to the query 4
            self.create_and_send_batches(self.filtered_client_titles[client_id], client_id, self.output_name2, self.next_workers_quantity)

        # Send the EOF to the QueryCoordinator
        self.send_EOFs(client_id, self.output_name1, QUERY_COORDINATOR_QUANTITY) # next_workers_quantity parameter is set to 1 because there is only 1 QueryCoordinator

        # Send the EOFs to the workers on the query 4
        self.send_EOFs(client_id, self.output_name2, self.next_workers_quantity)

    def manage_EOF(self, body, method, client_id):
        # client_id = get_EOF_client_id(body)
        self.eof_counter[client_id] = self.eof_counter.get(client_id, 0) + 1
        if self.eof_counter[client_id] == self.eof_quantity:
            self.send_results(client_id)

    def manage_message(self, client_id, data, method):
        if client_id not in self.filtered_client_titles:
            self.filtered_client_titles[client_id] = []

        desired_data = review_quantity_value(data, self.minimum_quantity)
        for title, counter in desired_data[0].items():
            self.filtered_client_titles[client_id].append({'Title': title, 'counter': counter})

    def _create_batches(self, batch, next_workers_quantity):
        workers_batches = {}
        print(batch)
        for row in batch:
            hashed_title = hash_title(row['Title'])
            choosen_worker = str(hashed_title % next_workers_quantity)
            if choosen_worker not in workers_batches:
                workers_batches[choosen_worker] = []
            workers_batches[choosen_worker].append(row)

        return workers_batches

    def _send_batches(self, workers_batches, output_queue, client_id):
        for worker_id, batch in workers_batches.items():
            serialized_batch = serialize_batch(batch)
            serialized_message = serialize_message(serialized_batch, client_id)
            worker_queue = create_queue_name(output_queue, worker_id)#output_queue + '_' + worker_id
            self.middleware.send_message(worker_queue, serialized_message)
