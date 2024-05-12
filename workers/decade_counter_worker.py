from workers import DecadeWorker
import os
    
def main():

    data_source_name = os.getenv('DATA_SOURCE_NAME')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')
    iteration_queue = os.getenv('ITERATION_QUEUE')


    worker = DecadeWorker(data_source_name, data_output_name, iteration_queue)
    worker.run()
main()

    