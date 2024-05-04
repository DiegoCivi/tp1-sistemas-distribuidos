from workers import HashWorker
import os
    
def main():

    data_source1_name, data_source2_name, data_source3_name, data_source4_name  = os.getenv('DATA_SOURCE_NAME').split(',')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')
    hash_modulus = int(os.getenv('HASH_MODULUS'))

    worker = HashWorker(data_source1_name, data_source2_name, data_source3_name, data_source4_name, data_output_name, hash_modulus)
    worker.run()

main()   