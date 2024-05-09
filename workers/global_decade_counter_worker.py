from workers import GlobalDecadeWorker
import os


    
def main():

    data_source_name = os.getenv('DATA_SOURCE_NAME')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')
    eof_quantity = int(os.getenv('EOF_QUANTITY'))

    
    worker = GlobalDecadeWorker(data_source_name, data_output_name, eof_quantity)
    worker.run()


main()

    