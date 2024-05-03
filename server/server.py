import socket
from communications import read_socket, write_socket
from middleware import Middleware
import os
import time
import signal


SEND_COORDINATOR_QUEUE = 'query_coordinator'
RECEIVE_COORDINATOR_QUEUE = 'server' 


class Server:

    def __init__(self, host, port, listen_backlog):
        signal.signal(signal.SIGTERM, self.handle_signal)
        
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.host = host
        self.port = port
        self.listen_backlog = listen_backlog
        self.middleware = Middleware()
        print("Middleware established the connection")
        self._client_socket = None
        self._stop_server= False

        self.results = ['']

        self._server_socket.bind((self.host, self.port))
        self._server_socket.listen(self.listen_backlog) 


    def run(self):

        while not self._stop_server:
            try:
                conn, addr = self._server_socket.accept()
                self._client_socket = conn
                print(f'New connection from address {addr}')
                self.handle_client()
            except OSError:
                print("Server shutdown")
                self._server_socket.close()

    def handle_client(self):
        """
        Reads the client data and fordwards it to the corresponding parts of the system
        """
        # First read the titles dataset
        self._receive_and_forward_data()
        print("Ya mande todo el archvio titles")
        # Then read the reviews dataset
        self._receive_and_forward_data() 
        print("Ya mande todo el archivo reviews")

        # Finally read the results and send it to the client
        self._receive_and_forward_results()  
        
        self._client_socket.close()
        
    def _receive_and_forward_data(self):
        msg = None
        while msg != "EOF":
            msg, e = read_socket(self._client_socket)
            if e != None:
                print(f"Hubo un error en la lectura del socker del cliente. El error fue: {e}")
                raise e

            self.middleware.send_message(SEND_COORDINATOR_QUEUE, msg)

    def read_results(self, method, body):
        if body == b'EOF':
            self.middleware.stop_consuming()
            self.middleware.ack_message(method)
            return
        
        result_slice = body.decode('utf-8')
        self.results[0] += result_slice
        self.middleware.ack_message(method)
    
    def _receive_and_forward_results(self):
        callback_with_params = lambda ch, method, properties, body: self.read_results(method, body)
        self.middleware.receive_messages(RECEIVE_COORDINATOR_QUEUE, callback_with_params)
        self.middleware.consume()
        print("Enviando resultados de queries 1 a 5 al cliente...")
        write_socket(self._client_socket, self.results[0])
        write_socket(self._client_socket, 'EOF')

    def handle_signal(self, *args):
        print("Gracefully exit")
        self._stop_server = True
        self.middleware.close_connection()
        if self._client_socket != None:
            self._client_socket.close()
        if self._server_socket != None:
            self._server_socket.shutdown(socket.SHUT_RDWR)



def main():    
    HOST, PORT, LISTEN_BACKLOG = os.getenv('HOST'), os.getenv('PORT'), os.getenv('LISTEN_BACKLOG') 
    server = Server(HOST, int(PORT), int(LISTEN_BACKLOG))
    server.run()

main()
    