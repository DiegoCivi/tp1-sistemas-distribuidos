from workers import FilterReviewsWorker
import os
import time
    
def main():

    minimum_quantity = int(os.getenv('MIN_QUANTITY'))
    data_source_name = os.getenv('DATA_SOURCE_NAME')
    data_output1_name, data_output2_name = os.getenv('DATA_OUTPUT_NAME').split(',')
    workers_quantity = int(os.getenv('EOF_QUANTITY'))
    next_workers_quantity = int(os.getenv('NEXT_WORKER_QUANTITY'))

    worker = FilterReviewsWorker(data_source_name, data_output1_name, data_output2_name, minimum_quantity, workers_quantity, next_workers_quantity)
    worker.run()

main()
