from csv import DictReader
from serialization import serialize_dict, serialize_message
from communications import write_socket, sendEOF, read_socket

TITLES_IDENTIFIER = 't'
REVIEWS_IDENTIFIER = 'r'

########### FILE MANAGMENT FUNCTIONS ###########

def create_file_reader(file_path):
    """
    Create a file reader object
    """
    try:
        file = open(file_path, 'r')
    except Exception as e:
        raise e
    
    reader = DictReader(file)
    return file, reader

def read_csv_batch(file_reader, threshold=200):
    """
    Read a batch of rows from a CSV file
    """
    batch = []
    # EOF reached
    if file_reader is None:
        raise Exception("FileReader is None")
    
    for i, dictionary in enumerate(file_reader):
        serialized_dict = serialize_dict(dictionary)
        batch.append(serialized_dict)

        if i >= threshold:
            break
        
    return batch

################################################

########### SEND MANAGMENT FUNCTIONS ###########

def send_file(file_path, system_socket, file_identifier):
    msg_id = 0
    file, file_reader = create_file_reader(file_path)
    file_batch = read_csv_batch(file_reader)
    while file_batch:
        serialized_message = serialize_message(file_batch, '', str(msg_id))
        write_socket(system_socket, serialized_message)
        file_batch = read_csv_batch(file_reader)
        msg_id += 1

    # Send the corresponding EOF
    eof_msg = 'EOF_' + file_identifier
    write_socket(system_socket, eof_msg)
    file.close()

def send_data(titles_filepath, reviews_filepath, system_socket):
    # Send the titles dataset
    send_file(titles_filepath, system_socket, TITLES_IDENTIFIER)

    # Send the reviews dataset
    send_file(reviews_filepath, system_socket, REVIEWS_IDENTIFIER)