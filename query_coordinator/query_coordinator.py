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
        self.middleware.publish_message('data', 'direct', 'EOF', 'EOF')

    def drop_rows_with_missing_values(self, batch, columns):
        """
        Drops the rows with missing values in the specified columns
        """
        new_batch = []
        for row in batch:
            if all([row.get(column) for column in columns]):
                new_batch.append(row)
        return new_batch

    def parse_and_send_q1(self, batch):
        """
        Parses the rows of the batch to return only
        required columns in the query 1
        """
        if self.parse_mode == TITLES_MODE:
            desired_keys = ['Title', 'publishedDate', 'categories', 'authors', 'publisher']
            new_batch = []
            for row in batch:
                row = {k: v for k, v in row.items() if k in desired_keys}
                new_batch.append(row)
            
            serialized_message = serialize_message([serialize_dict(filtered_dictionary) for filtered_dictionary in new_batch])
            self.middleware.publish_message('data', 'direct', 'q1_titles', serialized_message)
        #else:
        #    return None
    
    def parse_and_send_q2(self, batch):
        batch = self.drop_rows_with_missing_values(batch, ['Title', 'authors', 'categories', 'publishedDate'])
        if len(batch) == 0:
            return
        if self.parse_mode == 'titles':
            desired_keys = ['authors', 'publishedDate']
            new_batch = []
            for row in batch:
                row = {k: v for k, v in row.items() if k in desired_keys}
                new_batch.append(row)

            serialized_message = serialize_message([serialize_dict(filtered_dictionary) for filtered_dictionary in new_batch])
            self.middleware.publish_message('data', 'direct', 'q2_titles', serialized_message)
        #else:
        #    return batch['reviews']
    
    def parse_and_send_q3(self, batch):
        if self.parse_mode == 'titles':
            desired_keys = ['Title', 'authors', 'publishedDate']
            for row in batch:
                row = {k: v for k, v in row.items() if k in desired_keys}
            self.middleware.publish_message('data', 'direct', 'q3_titles', batch)
            
        else:
            desired_keys = ['Title', 'review/score']
            for row in batch:
                row = {k: v for k, v in row.items() if k in desired_keys}
            self.middleware.publish_message('data', 'direct', 'q3_reviews', batch)
    
    # def parse_and_send_q4(self, batch):
    #     if self.parse_mode == 'titles':
    #         return batch['titles']
    #     else:
    #         return batch['reviews']
    
    # def parse_and_send_q5(self, batch):
    #     if self.parse_mode == 'titles':
    #         return batch['titles']
    #     else:
    #         return batch['reviews']
    