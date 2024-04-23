from communications import write_socket, sendEOF
from files import create_file_reader, read_csv_batch
import socket
from csv import DictReader
from serialization import serialize_message
import os
import time


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
    time.sleep(15)

    host = os.getenv('HOST')
    port = os.getenv('PORT')
    titles_filepath = os.getenv('TITLES_FILEPATH')
    reviews_filepath = os.getenv('REVIEWS_FILEPATH')

    conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    conn.connect((host, int(port))) 
    titles_file_reader = create_file_reader(titles_filepath) 
    reviews_file_reader = create_file_reader(reviews_filepath) 
    
    scrape_and_send_file(titles_file_reader, conn)
    sendEOF(conn)
    scrape_and_send_file(reviews_file_reader, conn)
    sendEOF(conn)

    # TODO: Listen the server response and print it (Ponele colorcitos flaco)
    
    
    
main()
        
        
    