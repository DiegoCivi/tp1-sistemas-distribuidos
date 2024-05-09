from workers import HashWorker
import os
    
def main():

    data_source1_name, data_source2_name, data_source3_name, data_source4_name  = os.getenv('DATA_SOURCE_NAME').split(',')
    data_output_name = os.getenv('DATA_OUTPUT_NAME')
    hash_modulus = int(os.getenv('HASH_MODULUS'))
    q3_quantity = int(os.getenv('Q3_QUANTITY'))
    q5_quantity = int(os.getenv('Q5_QUANTITY'))


    worker = HashWorker(data_source1_name, data_source2_name, data_source3_name, data_source4_name, data_output_name, hash_modulus, q3_quantity, q5_quantity)
    worker.run()


main()   