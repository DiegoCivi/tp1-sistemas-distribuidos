from workers import DecadeWorker
import os
    
def main():

    data_source_name = os.getenv('DATA_SOURCE_NAME')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')
    source_queue = os.getenv('SOURCE_QUEUE')

    worker = DecadeWorker(data_source_name, data_output_name, source_queue)
    worker.run()

main()

    