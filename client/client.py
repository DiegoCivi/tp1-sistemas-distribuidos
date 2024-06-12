from communications import write_socket, sendEOF, read_socket
from files import create_file_reader, read_csv_batch
import socket
from csv import DictReader
from serialization import serialize_message
import os
import time
import signal
from client_lib import send_file

CONNECT_TRIES = 10
CONN_LOOP_LAPSE_START = 1

class Client:

    def __init__(self, host, port, titles_filepath, reviews_filepath):
        self.server_host = host
        self.server_port = int(port)
        self._server_socket = None
        self.titles_filepath = titles_filepath
        self.reviews_filepath = reviews_filepath
        self.file_titles = None
        self.file_reviews = None
        self.stop_client = False

        signal.signal(signal.SIGTERM, self.handle_signal)

    def _connect_server(self):
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        loop_lapse = CONN_LOOP_LAPSE_START
        for i in range(CONNECT_TRIES):
            if self.stop_client:
                raise OSError
            try:
                print("Connecting to server. Attempt: ", i)
                conn.connect((self.server_host, self.server_port))
                break
            except Exception as e:
                time.sleep(loop_lapse)
                loop_lapse = loop_lapse * 2
                if i == CONNECT_TRIES - 1:
                    print("Could not connect to server")
                    raise e
        self._server_socket = conn

    def scrape_and_send_file(self, file_reader):
        """
        Gets batches of info from reading a file and sends it through the socket
        """
        while not self.stop_client:
            file_batch = read_csv_batch(file_reader)
            serialized_message = serialize_message(file_batch)
            if not file_batch:
                break
            write_socket(self._server_socket, serialized_message)

    
    def receive_results(self):
        # Listen the server response and print it
        msg = None
        while msg != "EOF":
            msg, e = read_socket(self._server_socket)
            if e != None:
                # TODO: Maybe raise the exception or just print it and return
                print(f"Hubo un error en la lectura del socket del server. El error fue: {e}")
                return

            print(msg)

    def run(self):

        try:
            self._connect_server()
            # self.file_titles, titles_file_reader = create_file_reader(self.titles_filepath) 
            # self.file_reviews, reviews_file_reader = create_file_reader(self.reviews_filepath) 
            # # Send the titles dataset with its EOF
            send_file(self.titles_filepath, self._server_socket)
            # self.scrape_and_send_file(titles_file_reader)
            # eof_e = sendEOF(self._server_socket)
            # if eof_e != None:
            #     raise eof_e
            # self.file_titles.close()
            print("Ya mande todo el archivo titles")

            # Send the reviews dataset with its EOF
            send_file(self.reviews_filepath, self._server_socket)
            # self.scrape_and_send_file(reviews_file_reader)
            # eof_e = sendEOF(self._server_socket)
            # if eof_e != None:
            #     raise eof_e
            # self.file_reviews.close()

            # Receice the queries results
            self.receive_results()
        except (OSError, ValueError):
            print("Server socket was closed after receiving SIGTERM")
            return

        self._server_socket.close()

    def handle_signal(self, *args):
        self.stop_client = True
        if self._server_socket != None:
            self._server_socket.shutdown(socket.SHUT_RDWR)
            self._server_socket.close()

        if self.file_titles != None:
            self.file_titles.close()
        
        if self.file_reviews != None:
            self.file_reviews.close()

def main():
    host = os.getenv('HOST')
    port = os.getenv('PORT')
    titles_filepath = os.getenv('TITLES_FILEPATH')
    reviews_filepath = os.getenv('REVIEWS_FILEPATH')

    client = Client(host, port, titles_filepath, reviews_filepath)
    client.run()
    
    
    
main()
        
        
    