from workers import TopNWorker
import os 
    
    
def main():

    top_n = int(os.getenv('TOP_N'))
    data_source_name = os.getenv('DATA_SOURCE_NAME')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')
    workers_quantity = int(os.getenv('EOF_QUANTITY'))
    last = True if os.getenv('LAST') == '1' else False
    
    worker = TopNWorker(data_source_name, data_output_name, workers_quantity, top_n, last)
    worker.run()


main()
