from middleware import Middleware
from serialization import serialize_dict, serialize_message

TITLES_MODE = 'titles'
REVIEWS_MODE = 'reviews'

class QueryCoordinator:

    def __init__(self, middleware):
        """
        Initializes the query coordinator with the title parse mode
        """
        self.parse_mode = TITLES_MODE
        self.middleware = middleware
    
    def change_parse_mode(self, mode):
        """
        Changes the parse mode to the one specified
        """
        if mode != TITLES_MODE and mode != REVIEWS_MODE:
            raise Exception("Mode not supported.")
        self.parse_mode = mode
    
    def send_EOF(self):
        """
        Sends the EOF message to the middleware
        """
        routing_key = 'EOF_' + self.parse_mode
        self.middleware.publish_message('data', 'direct', routing_key, 'EOF')
        self.middleware.publish_message('data', 'direct', routing_key, 'EOF')
        self.middleware.publish_message('data', 'direct', routing_key, 'EOF')
        if self.parse_mode == REVIEWS_MODE:
            self.middleware.stop_consuming()

    def drop_rows_with_missing_values(self, batch, columns):
        """
        Drops the rows with missing values in the specified columns
        """
        new_batch = []
        for row in batch:
            if self.parse_mode == TITLES_MODE and not all([row.get(column) for column in columns]): # We drop the Nan values only for the titles dataset
                continue    
            new_batch.append(row)
            
        return new_batch
    
    def send_to_pipelines(self, batch):
        batch = self.drop_rows_with_missing_values(batch, ['Title', 'authors', 'categories', 'publishedDate'])
        if len(batch) == 0:
            return
        
        # There isn't a parse_and_send_q4 because query 4 pipeline 
        # receives the data from the query 3 pipeline results
         
        self.parse_and_send_q1(batch)
        #self.parse_and_send_q2(batch)
        #self.parse_and_send_q3(batch)
        #self.parse_and_send_q5(batch)

    def parse_and_send(self, batch, desired_keys, routing_key):
        new_batch = []
        for row in batch:
            row = {k: v for k, v in row.items() if k in desired_keys}
            new_batch.append(row)
            
        serialized_message = serialize_message([serialize_dict(filtered_dictionary) for filtered_dictionary in new_batch])
        self.middleware.publish_message('data', 'direct', routing_key, serialized_message)

    def parse_and_send_q1(self, batch):
        """
        Parses the rows of the batch to return only
        required columns in the query 1
        """
        if self.parse_mode == REVIEWS_MODE:
            return
        desired_keys = ['Title', 'publishedDate', 'categories', 'authors', 'publisher']
        self.parse_and_send(batch, desired_keys, 'q1_titles')
    
    def parse_and_send_q2(self, batch):
        desired_keys = ['authors', 'publishedDate']
        batch = self.drop_rows_with_missing_values(batch, ['Title', 'authors', 'categories', 'publishedDate'])
        self.parse_and_send(batch, desired_keys, 'q2_titles')
    
    def parse_and_send_q3(self, batch):
        if self.parse_mode == TITLES_MODE:
            desired_keys = ['Title', 'authors', 'publishedDate']
            self.parse_and_send(batch, desired_keys, 'q3_titles')
        else:
            desired_keys = ['Title', 'review/score']
            self.parse_and_send(batch, desired_keys, 'q3_reviews')
    
    def parse_and_send_q5(self, batch):
        if self.parse_mode == 'titles':
            desired_keys = ['Title', 'categories']
            self.parse_and_send(batch, desired_keys, 'q5_titles')
        else:
            desired_keys = ['Title', 'review/text']
            self.parse_and_send(batch, desired_keys, 'q5_reviews')
    