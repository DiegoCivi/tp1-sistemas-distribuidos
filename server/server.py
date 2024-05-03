import socket
from communications import read_socket, write_socket
from middleware import Middleware
import os
import time


SEND_COORDINATOR_QUEUE = 'query_coordinator'
RECEIVE_COORDINATOR_QUEUE = 'server' 


class Server:
    def __init__(self, host, port, listen_backlog):
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.host = host
        self.port = port
        self.listen_backlog = listen_backlog
        self.middleware = Middleware()

        self.results = ['']

        self._server_socket.bind((self.host, self.port))
        self._server_socket.listen(self.listen_backlog)  

    def run(self):

        while True:
            conn, addr = self._server_socket.accept()
            print(f'New connection from address {addr}')
            self.handle_client(conn) 

    def handle_client(self, client_socket):
        """
        Reads the client data and fordwards it to the corresponding parts of the system
        """
        # First read the titles dataset
        self._receive_and_forward_data(client_socket)
        print("Ya mande todo el archvio titles")
        # Then read the reviews dataset
        self._receive_and_forward_data(client_socket) 
        print("Ya mande todo el archivo reviews")

        # Finally read the results and send it to the client
        self._receive_and_forward_results(client_socket)  
        client_socket.close()
        self.middleware.close_connection()
        
    def _receive_and_forward_data(self, client_socket):
        msg = None
        while msg != "EOF":
            msg, e = read_socket(client_socket)
            if e != None:
                print(f"Hubo un error en la lectura del socker del cliente. El error fue: {e}")
                return

            self.middleware.send_message(SEND_COORDINATOR_QUEUE, msg)

    def read_results(self, method, body):
        if body == b'EOF':
            self.middleware.stop_consuming()
            self.middleware.ack_message(method)
            return
        
        result_slice = body.decode('utf-8')
        self.results[0] += result_slice
        self.middleware.ack_message(method)
    
    def _receive_and_forward_results(self, client_socket):
        callback_with_params = lambda ch, method, properties, body: self.read_results(method, body)
        self.middleware.receive_messages(RECEIVE_COORDINATOR_QUEUE, callback_with_params)
        self.middleware.consume()
        print("Enviando resultados de queries 1 a 5 al cliente...")
        write_socket(client_socket, self.results[0])
        write_socket(client_socket, 'EOF')


def main():
    time.sleep(30)
    
    HOST, PORT, LISTEN_BACKLOG = os.getenv('HOST'), os.getenv('PORT'), os.getenv('LISTEN_BACKLOG') 
    server = Server(HOST, int(PORT), int(LISTEN_BACKLOG))
    server.run()

main()
    