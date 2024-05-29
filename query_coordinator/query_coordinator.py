from middleware import Middleware
from serialization import serialize_message, deserialize_titles_message, ROW_SEPARATOR, is_EOF, get_EOF_id, create_EOF, serialize_batch, add_id, hash_title
import signal
import queue
from multiprocessing import Process
import logging

TITLES_MODE = 'titles'
REVIEWS_MODE = 'reviews'
BATCH_SIZE = 100
SEND_SERVER_QUEUE = 'server'
RECEIVE_SERVER_QUEUE = 'query_coordinator'
Q1 = '[QUERY 1]'
Q2 = '[QUERY 2]'
Q3 = '[QUERY 3]'
Q4 = '[QUERY 4]'
Q5 = '[QUERY 5]'
QUANTITY_INDEX = 0
QUEUE_INDEX = 1
Q1_KEY = '1'
Q2_KEY = '2'
Q3_TITLES_KEY = '3_titles'
Q3_REVIEWS_KEY = '3_reviews'
Q5_TITLES_KEY = '5_titles'
Q5_REVIEWS_KEY = '5_reviews'
ID = '0' # WARNING: If this is changed, results will never get back to the QueryCoordinator 

class QueryCoordinator:

    def __init__(self, workers_q1, workers_q2, workers_q3_titles, workers_q3_reviews, workers_q5_titles, workers_q5_reviews, eof_quantity):
        """
        Initializes the query coordinator with the title parse mode
        """
        signal.signal(signal.SIGTERM, self.handle_signal)

        self.id = ID
        self.workers = {Q1_KEY: workers_q1, Q2_KEY: workers_q2, Q3_TITLES_KEY: workers_q3_titles, Q3_REVIEWS_KEY: workers_q3_reviews,
                             Q5_TITLES_KEY: workers_q5_titles, Q5_REVIEWS_KEY: workers_q5_reviews}
        self.eof_quantity = eof_quantity

        self.stop_coordinator = False
        self.middleware = None
        self.queue = queue.Queue()
        try:
            middleware = Middleware(self.queue)
        except Exception as e:
            raise e
        self.middleware = middleware        

    def handle_signal(self, *args):
        self.stop_coordinator = True
        self.queue.put('SIGTERM')
        if self.middleware != None:
            self.middleware.close_connection()

    def run(self):
        data_coordinator_p = Process(target=self.initiate_data_coordinator, args=())
        data_coordinator_p.start()
        result_coordinator_p = Process(target=self.initiate_result_coordinator, args=())
        result_coordinator_p.start()

        data_coordinator_p.join()
        logging.info("Termino el data")
        result_coordinator_p.join()
        logging.info("Termino el results")

    def initiate_data_coordinator(self):
        data_coordinator = DataCoordinator(self.id, self.workers)
        data_coordinator.run()

    def initiate_result_coordinator(self):
        result_coordinator = ResultsCoordinator(self.id, self.eof_quantity)
        result_coordinator.run()

class DataCoordinator:

    def __init__(self, id, workers):
        self.id = id
        self.workers = workers
        self.clients_parse_mode = {}
        self.stop = False
        self.middleware = None
        self.queue = queue.Queue()
        try:
            middleware = Middleware(self.queue)
        except Exception as e:
            raise e
        self.middleware = middleware
    
    def change_parse_mode(self, mode, client_id):
        """
        Changes the parse mode to the one specified
        """
        if mode != TITLES_MODE and mode != REVIEWS_MODE:
            raise Exception("Mode not supported.")
        self.clients_parse_mode[client_id] = mode

    def run(self):
        self.receive_and_fordward_data()

    def handle_data(self, method, body):
        if is_EOF(body):
            client_id = get_EOF_id(body)
            self.manage_EOF(client_id)
            self.change_parse_mode(REVIEWS_MODE, client_id)
            
            self.middleware.ack_message(method)
            return
        
        #print(body.decode('utf-8')[:10])
        client_id, batch = deserialize_titles_message(body)
        if client_id not in self.clients_parse_mode:
            # Since titles are always first, every new client needs to be initialized with the TITLES_MODE
            self.clients_parse_mode[client_id] = TITLES_MODE

        self.send_to_pipelines(batch, client_id)

        self.middleware.ack_message(method)

    def receive_and_fordward_data(self):
        callback_with_params = lambda ch, method, properties, body: self.handle_data(method, body)

        # Read the data from the server, parse it and fordward it
        self.middleware.receive_messages(RECEIVE_SERVER_QUEUE, callback_with_params)
        self.middleware.consume()

    def send_to_pipelines(self, batch, client_id):
        batch = self.drop_rows_with_missing_values(batch, ['Title', 'authors', 'categories', 'publishedDate'], client_id)
        if len(batch) == 0:
            return
        
        # There isn't a parse_and_send_q4 because query 4 pipeline 
        # receives the data from the query 3 pipeline results
        self.parse_and_send_q1(batch, client_id)
        self.parse_and_send_q2(batch, client_id)
        self.parse_and_send_q3(batch, client_id)
        # self.parse_and_send_q5(batch, client_id)

    def parse_and_send(self, batch, desired_keys, queue, query, client_id):
        # First, we get only the columns the query needs
        new_batch = []
        for row in batch:
            row = {k: v for k, v in row.items() if k in desired_keys}
            new_batch.append(row)
        
        # Second, we create the batches for each worker
        workers_batches = {}
        workers_quantity = int(self.workers[query][QUANTITY_INDEX])
        for row in new_batch:
            hashed_title = hash_title(row['Title'])
            choosen_worker = str(hashed_title % workers_quantity)
            
            if choosen_worker not in workers_batches:
                workers_batches[choosen_worker] = []
            workers_batches[choosen_worker].append(row)

        # Third, we send the batches 
        for worker_id, batch in workers_batches.items():
            serialized_batch = serialize_batch(batch)
            serialized_message = serialize_message(serialized_batch, client_id)
            worker_queue = queue + '_' + worker_id
            self.middleware.send_message(worker_queue, serialized_message)



    def parse_and_send_q1(self, batch, client_id):
        """
        Parses the rows of the batch to return only
        required columns in the query 1
        """
        if self.clients_parse_mode[client_id] == REVIEWS_MODE:
            return
        desired_keys = ['Title', 'publishedDate', 'categories', 'authors', 'publisher']
        self.parse_and_send(batch, desired_keys, self.workers[Q1_KEY][QUEUE_INDEX], Q1_KEY, client_id)

    def parse_and_send_q2(self, batch, client_id):
        if self.clients_parse_mode[client_id] == REVIEWS_MODE:
            return
        desired_keys = ['Title','authors', 'publishedDate']
        batch = self.drop_rows_with_missing_values(batch, ['Title', 'authors', 'categories', 'publishedDate'], client_id)
        self.parse_and_send(batch, desired_keys, self.workers[Q2_KEY][QUEUE_INDEX], Q2_KEY, client_id)
    
    def parse_and_send_q3(self, batch, client_id):
        if self.clients_parse_mode[client_id] == TITLES_MODE:
            desired_keys = ['Title', 'authors', 'publishedDate']
            self.parse_and_send(batch, desired_keys, self.workers[Q3_TITLES_KEY][QUEUE_INDEX], Q3_TITLES_KEY, client_id)
        else:
            desired_keys = ['Title', 'review/score']
            self.parse_and_send(batch, desired_keys, self.workers[Q3_REVIEWS_KEY][QUEUE_INDEX], Q3_REVIEWS_KEY, client_id)
    
    def parse_and_send_q5(self, batch, client_id):
        if self.clients_parse_mode[client_id] == TITLES_MODE:
            desired_keys = ['Title', 'categories']
            self.parse_and_send(batch, desired_keys, self.workers[Q5_TITLES_KEY][QUEUE_INDEX], Q5_TITLES_KEY, client_id)
        else:
            desired_keys = ['Title', 'review/text']
            self.parse_and_send(batch, desired_keys, self.workers[Q5_REVIEWS_KEY][QUEUE_INDEX], Q5_REVIEWS_KEY, client_id)

    def drop_rows_with_missing_values(self, batch, columns, client_id):
        """
        Drops the rows with missing values in the specified columns
        """
        new_batch = []
        for row in batch:
            if self.clients_parse_mode[client_id] == TITLES_MODE and not all([row.get(column) for column in columns]): # We drop the Nan values only for the titles dataset
                continue    
            new_batch.append(row)
            
        return new_batch

    def manage_EOF(self, client_id):
        """
        Sends the EOF message to the middleware
        """
        if self.clients_parse_mode[client_id] == TITLES_MODE:
            self.send_EOF(Q1_KEY, self.workers[Q1_KEY][QUEUE_INDEX], client_id)
            self.send_EOF(Q2_KEY, self.workers[Q2_KEY][QUEUE_INDEX], client_id)
            self.send_EOF(Q3_TITLES_KEY, self.workers[Q3_TITLES_KEY][QUEUE_INDEX], client_id)
            self.send_EOF(Q5_TITLES_KEY, self.workers[Q5_TITLES_KEY][QUEUE_INDEX], client_id)
        else: 
            self.send_EOF(Q3_REVIEWS_KEY, self.workers[Q3_REVIEWS_KEY][QUEUE_INDEX], client_id)
            self.send_EOF(Q5_REVIEWS_KEY, self.workers[Q5_REVIEWS_KEY][QUEUE_INDEX], client_id)

    def send_EOF(self, workers_dict_key, queue, client_id):

        eof_msg = create_EOF(client_id)
        workers_quantity = int(self.workers[workers_dict_key][QUANTITY_INDEX])
        for worker_id in range(workers_quantity):
            worker_queue = queue + '_' + str(worker_id)
            self.middleware.send_message(worker_queue, eof_msg)

class ResultsCoordinator:

    def __init__(self, id, eof_quantity):
        self.id = id
        self.eof_quantity = eof_quantity
        self.clients_results = {}
        self.clients_results_counter = {}
        self.stop = False
        self.middleware = None
        self.queue = queue.Queue()
        try:
            middleware = Middleware(self.queue)
        except Exception as e:
            raise e
        self.middleware = middleware

    def run(self):
        logging.info('ResultsCoordinatoor running')
        self.manage_results()
        
    def build_result_line(self, data, fields_to_print, query):
        """
        Builds the result line for the query
        """
        if query == Q1:
            return ' - '.join(f'{field.upper()}: {row[field]}' for row in data for field in fields_to_print)
        elif query == Q3:
            line = ''
            for result_dict in data:
                title = result_dict['Title']
                authors = result_dict['counter'].split(',', 2)[2] # The split is 2 until the second comma because the auuthors field can have comas
                line += 'TITLE: ' + title + '    ' + 'AUTHORS: ' + authors + '\n' 
            return line
        elif query  == Q4:
            line = ''
            top_position = 1
            for result_dict in data:
                title = result_dict['Title']
                mean_rating = result_dict['counter']
                line += str(top_position) +'.   TITLE: ' + title + '    ' + 'MEAN-RATING: ' +  mean_rating + '\n'
                top_position += 1
            return line
        else:
            return data[0]['results']

    def handle_results(self, method, body, fields_to_print, query):
        #print(f'De la query {query} me llego {body}')
        if is_EOF(body):
            print("Me llefo un eof: ", body)
            client_id = get_EOF_id(body)
            self.clients_results_counter[client_id] = self.clients_results_counter.get(client_id, 0) + 1
            if self.clients_results_counter[client_id] == 6: # VALOR HARDCODEADO DEPENDE DE CUANTAS QUERIES ESTEN CORRIENDO
                self.send_results(client_id)
                # self.middleware.stop_consuming()

            self.middleware.ack_message(method)
            return

        client_id, result_dict = deserialize_titles_message(body) # If it fails in this line. It may be because the results are sent in a way that "deserialize_titles_message()" cannot bee used. Then "split_message_info()" should be used
        #print(result_dict)
        if client_id not in self.clients_results:
            self.clients_results[client_id] = {}
        
        #print(data)
        new_result_line = '\n' + self.build_result_line(result_dict, fields_to_print, query)
        self.clients_results[client_id][query] = self.clients_results[client_id].get(query, '') + new_result_line 

        self.middleware.ack_message(method)

    def manage_results(self):
    
        # # Use queues to receive the queries results
        q1_results_with_params = lambda ch, method, properties, body: self.handle_results(method, body, ['Title', 'authors', 'publisher'], Q1)
        q2_results_with_params = lambda ch, method, properties, body: self.handle_results(method, body, ['authors'], Q2)
        q3_results_with_params = lambda ch, method, properties, body: self.handle_results(method, body, ['Title', 'authors'], Q3)
        q4_results_with_params = lambda ch, method, properties, body: self.handle_results(method, body, ['Title'], Q4)
        # q5_results_with_params = lambda ch, method, properties, body: self.handle_results(method, body, ['Title'], Q5)
        self.middleware.receive_messages('QUEUE_q1_results' + '_' +  self.id, q1_results_with_params)
        self.middleware.receive_messages('QUEUE_q2_results' + '_' +  self.id, q2_results_with_params)
        self.middleware.receive_messages('QUEUE_q3_results' + '_' +  self.id, q3_results_with_params)
        self.middleware.receive_messages('QUEUE_q4_results' + '_' +  self.id, q4_results_with_params)
        # self.middleware.receive_messages('QUEUE_q5_results' + '_' +  self.id, q5_results_with_params)
        self.middleware.consume()

    def assemble_results(self, client_id):
        # print(self.clients_results)
        client_results_dict = self.clients_results[client_id]
        results = []
        
        results1 = Q1 + client_results_dict[Q1]
        results.append(results1)
        results2 = Q2 + client_results_dict[Q2]
        results.append(results2)
        results3 = Q3 + client_results_dict[Q3]
        results.append(results3)
        results4 = Q4 + client_results_dict[Q4]
        results.append(results4)
        # results5 = Q5 + client_results_dict[Q5]
        # results.append(results5)
        
        results = '\n'.join(results)
        return results

    def send_results(self, client_id):
        # Create the result
        result_msg = self.assemble_results(client_id)
        #print("El resultado es: ", result_msg)
        # Send the results to the server
        chars_sent = 0
        chars_to_send = len(result_msg)
        while chars_sent < chars_to_send:
            start_index = chars_sent
            end_idex = chars_sent + BATCH_SIZE
            if end_idex >= len(result_msg):
                end_idex = len(result_msg) - 1

            result_slice = result_msg[start_index: end_idex]
            result_slice = add_id(result_slice, client_id)
            #print("El result_slice es: ", result_slice)
            self.middleware.send_message(SEND_SERVER_QUEUE, result_slice)
            chars_sent += BATCH_SIZE

        eof_msg = create_EOF(client_id)
        self.middleware.send_message(SEND_SERVER_QUEUE, eof_msg)