# File handling functions

def create_file_reader(file_path):
    """
    Create a file reader object
    """
    try:
        file_reader = open(file_path, 'r')
    except:
        return None
    return file_reader

def read_csv_batch(file_reader, threshold=100):
    """
    Read a batch of rows from a CSV file
    """
    batch = []
    # EOF reached
    if file_reader is None:
        return batch
    for i, row in enumerate(file_reader):
        if i >= threshold:
            break
        batch.append(row.rstrip('\n'))
    return batch
