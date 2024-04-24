import socket
from communications import read_socket
from middleware import Middleware
import os
import time

FIELD_SEPARATOR = "@|@"
ROW_SEPARATOR = "-|-"

class Server:
    def __init__(self, host, port, listen_backlog):
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.host = host
        self.port = port
        self.listen_backlog = listen_backlog
        self.middleware = Middleware()

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
        self._receive_and_forward_data(client_socket, 'titles', 'fanout')
        print("Ya mande todo el archvio titles")
        ## Then read the reviews dataset
        #self._receive_and_forward_data(client_socket, 'reviews', 'fanout')        
        
    def _receive_and_forward_data(self, client_socket, exchange, type):
        msg = None
        self.middleware.declare_exchange(exchange, type)
        while msg != "EOF":
            msg, e = read_socket(client_socket)
            #print('Lei un batch del cliente')
            if e != None:
                # TODO: Maybe raise the exception or just print it and return
                print(f"Hubo un error en la lectura del socker del cliente. El error fue: {e}")
                return
            
            self.middleware.publish_message(exchange, '', msg)


def main():
    time.sleep(10)
    
    HOST, PORT, LISTEN_BACKLOG = os.getenv('HOST'), os.getenv('PORT'), os.getenv('LISTEN_BACKLOG') 
    server = Server(HOST, int(PORT), int(LISTEN_BACKLOG))
    server.run()

main()
    