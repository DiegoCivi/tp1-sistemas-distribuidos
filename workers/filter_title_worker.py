from filters import title_condition
from workers import FilterWorker
import os

def main():

    title_to_filter = os.getenv('TITLE_TO_FILTER')
    source_name = os.getenv('DATA_SOURCE_NAME')
    output_name = os.getenv('DATA_OUTPUT_NAME')
    worker_id = os.getenv('WORKER_ID')
    eof_queue = os.getenv('EOF_QUEUE')
    worker_quantity = int(os.getenv('WORKER_QUANTITY'))
    next_worker_quantity = int(os.getenv('NEXT_WORKER_QUANTITY'))

    worker = FilterWorker(worker_id, source_name, output_name, eof_queue, worker_quantity, next_worker_quantity)
    worker.set_filter_type('TITLE', title_condition, title_to_filter)
    worker.run()

main()