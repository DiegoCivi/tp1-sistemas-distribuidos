from workers import DecadeWorker
import os
    
def main():

    data_source_name = os.getenv('DATA_SOURCE_NAME')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')

    worker = DecadeWorker(data_source_name, data_output_name)
    worker.run()
main()

    