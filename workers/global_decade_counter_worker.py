from workers import GlobalDecadeWorker
import os


    
def main():

    data_source_name = os.getenv('DATA_SOURCE_NAME')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')
    eof_quantity = int(os.getenv('EOF_QUANTITY'))
    iteration_queue = os.getenv('ITERATION_QUEUE')
    
    worker = GlobalDecadeWorker(data_source_name, data_output_name, eof_quantity, iteration_queue)
    worker.run()


main()

    