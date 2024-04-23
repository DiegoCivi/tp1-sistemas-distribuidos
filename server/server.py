import socket
from communications import read_socket
from middleware import Middleware
import os
import time

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

    def handle_client(self):
        """
        Reads the client data and fordwards it to the corresponding parts of the system
        """
        # First read the titles dataset
        self._receive_and_forward_data('titles', 'fanout')
        
        # Then read the reviews dataset
        self._receive_and_forward_data('reviews', 'fanout')        
        
    def _receive_and_forward_data(self, exchange, type):
        msg = None
        while msg != "EOF":
            msg, e = read_socket(socket)
            if e != None:
                # TODO: Maybe raise the exception or just print it and return
                return
            self.middleware.declare_exchange(exchange, type)
            self.middleware.publish_message(exchange, '', msg)

def main():
    time.sleep(15)
    
    HOST, PORT, LISTEN_BACKLOG = os.getenv('HOST'), os.getenv('PORT'), os.getenv('LISTEN_BACKLOG') 
    server = Server(HOST, int(PORT), int(LISTEN_BACKLOG))
    server.run()

main()
    