import os
from workers import ReviewSentimentWorker

def main():
    data_source_name = os.getenv('DATA_SOURCE_NAME')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')
    worker_id = os.getenv('WORKER_ID')
    workers_quantity = int(os.getenv('WORKERS_QUANTITY'))
    next_workers_quantity = int(os.getenv('NEXT_WORKERS_QUANTITY'))
    eof_queue = os.getenv('EOF_QUEUE')

    worker = ReviewSentimentWorker(data_source_name, data_output_name, worker_id, workers_quantity, next_workers_quantity, eof_queue)
    worker.run()

    
main()

    