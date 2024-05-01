from communications import write_socket, sendEOF
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
        #print("Voy a leer un batch")
        file_batch = read_csv_batch(file_reader)
        #print("Lei un batch del archivo")
        serialized_message = serialize_message(file_batch)
        if not file_batch:
            print("[scrape_and_send_file] Termine de leer el archivo")
            break
        #print(f"Voy a mandar el batch con un largo de: {len(serialized_message)} y en bytes tiene un largo de: {len(bytes(serialized_message, 'utf-8'))}")
        write_socket(socket, serialized_message)

def main():
    #print("Empezando")
    time.sleep(15)
    #print("Termine de dormir")
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
    print("[CLIENT] Ya mande todo el archivo titles")
    scrape_and_send_file(reviews_file_reader, conn)
    sendEOF(conn)

    # TODO: Listen the server response and print it (Ponele colorcitos flaco)
    
    
    
main()
        
        
    