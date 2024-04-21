from communications import write_socket, sendEOF
from files import create_file_reader, read_csv_batch
import socket
from serialization import serialize_message


def scrape_and_send_file(file_reader, socket):
    """
    Gets batches of info from reading a file and sends it through the socket
    """
    while True:
        file_batch = read_csv_batch(file_reader)
        serialized_message = serialize_message(file_batch)
        if not file_batch:
            break
        write_socket(socket, serialized_message)

def main():
    socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    socket.connect((HOST, PORT)) # TODO: HOST anf PORT is an env var
    titles_file_reader = create_file_reader(TITLES_FILEPATH) # TODO: TITLES_FILEPATH is an env var
    reviews_file_reader = create_file_reader(REVIEWS_FILEPATH) # TODO: REVIEWS_FILEPATH is an env var
    
    scrape_and_send_file(titles_file_reader, socket)
    sendEOF(socket)
    scrape_and_send_file(reviews_file_reader, socket)
    sendEOF(socket)

    # TODO: Listen the server response and print it (Ponele colorcitos flaco)
    
    
    
    
    
        
        
        
    