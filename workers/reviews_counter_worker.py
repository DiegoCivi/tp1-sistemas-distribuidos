from middleware import Middleware
from serialization import deserialize_titles_message, serialize_message, serialize_dict
import os
from workers import JoinWorker
            
def main():

    data_source_name= os.getenv('DATA_SOURCE_NAME')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')
    worker_id = os.getenv('WORKER_ID')
    eof_quantity = int(os.getenv('EOF_QUANTITY'))

    worker = JoinWorker(worker_id, data_source_name, data_output_name, eof_quantity, 3)
    worker.run()
    
main()