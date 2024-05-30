from workers import DecadeWorker
import os
    
def main():

    data_source_name = os.getenv('DATA_SOURCE_NAME')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')
    iteration_queue = os.getenv('ITERATION_QUEUE')
    worker_id = os.getenv('WORKER_ID')
    next_workers_quantity = int(os.getenv('NEXT_WORKERS_QUANTITY'))

    worker = DecadeWorker(data_source_name, data_output_name, iteration_queue, worker_id, next_workers_quantity)
    worker.run()
    
main()

    