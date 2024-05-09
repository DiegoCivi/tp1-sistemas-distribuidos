import pandas as pd
import sys

PARAMETERS_LEN = 3

def main():
    if len(sys.argv) > PARAMETERS_LEN:
        raise Exception("Only 2 parameters need to be received.")
    
    sample_frac = float(sys.argv[1])
    path = sys.argv[2]
    df = pd.read_csv(path)
    dataset_sample = df.sample(frac=sample_frac)


    path_to_download = './datasets/books_rating_sample-' + str(sample_frac) + '.csv' 
    dataset_sample.to_csv(path_to_download, index=False)

main()