from communications import write_socket, sendEOF, read_socket
from files import create_file_reader, read_csv_batch
import socket
from csv import DictReader
from serialization import serialize_message
import os
import time

CONNECT_TRIES = 5
CONN_LOOP_LAPSE = 1

def scrape_and_send_file(file_reader, socket):
    """
    Gets batches of info from reading a file and sends it through the socket
    """
    while True:
        file_batch = read_csv_batch(file_reader)
        serialized_message = serialize_message(file_batch)
        if not file_batch:
            print("Termine de leer el archivo")
            break
        write_socket(socket, serialized_message)

def main():
    time.sleep(30)
    host = os.getenv('HOST')
    port = os.getenv('PORT')
    titles_filepath = os.getenv('TITLES_FILEPATH')
    reviews_filepath = os.getenv('REVIEWS_FILEPATH')

    conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    for i in range(CONNECT_TRIES):
        try:
            print("Connecting to server. Attempt: ", i)
            conn.connect((host, int(port)))
            time.sleep(CONN_LOOP_LAPSE)
            break
        except:
            if i == CONNECT_TRIES - 1:
                print("Could not connect to server")
                return

    titles_file_reader = create_file_reader(titles_filepath) 
    reviews_file_reader = create_file_reader(reviews_filepath) 
    
    scrape_and_send_file(titles_file_reader, conn)
    sendEOF(conn)
    print("Ya mande todo el archivo titles")
    scrape_and_send_file(reviews_file_reader, conn)
    sendEOF(conn)

    # Listen the server response and print it
    msg = None
    while msg != "EOF":
        msg, e = read_socket(conn)
        if e != None:
            # TODO: Maybe raise the exception or just print it and return
            print(f"Hubo un error en la lectura del socket del server. El error fue: {e}")
            return
        
        print(msg)
    
    conn.close()
    
    
    
main()
        
        
    