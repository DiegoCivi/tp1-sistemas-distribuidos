from workers import PercentileWorker
import os
    
def main():

    data_source_name = os.getenv('DATA_SOURCE_NAME')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')
    percentile = os.getenv('PERCENTILE')
    workers_quantity = int(os.getenv('WORKER_QUANTITY'))
    

    worker = PercentileWorker(data_source_name, data_output_name, percentile, workers_quantity)
    worker.run()


main()
