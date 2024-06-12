from csv import DictReader
from serialization import serialize_dict, serialize_message
from communications import write_socket, sendEOF, read_socket

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

def send_file(file_path, system_socket):
    # print(1)
    msg_id = 0
    file, file_reader = create_file_reader(file_path)
    # print(2)
    file_batch = read_csv_batch(file_reader)
    # print("el batch apenas lo leo: "file_batch[:1])
    while file_batch:
        # print(file_batch[:1])
        serialized_message = serialize_message(file_batch, '', str(msg_id))
        # print(serialized_message)
        write_socket(system_socket, serialized_message)
        file_batch = read_csv_batch(file_reader)
        msg_id += 1
    # print(3)
    eof_e = sendEOF(system_socket)
    if eof_e != None:
        raise eof_e
    # print(4)
    file.close()