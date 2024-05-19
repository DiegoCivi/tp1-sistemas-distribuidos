import socket
from communications import read_socket, write_socket, Message
from middleware import Middleware
import os
import signal
import queue
from multiprocessing import Process


SEND_COORDINATOR_QUEUE = 'query_coordinator'
RECEIVE_COORDINATOR_QUEUE = 'server' 


class Server: # TODO: Implement SIGTERM handling

    def __init__(self, host, port, listen_backlog):
        self.clients = {}
        self._stop_server= False
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind((host, port))
        self._server_socket.listen(listen_backlog) 

    def run(self):
        # Create a process that will be to send the results back to the client
        

        # Receive new clients and create a process that will handle them
        while not self._stop_server:
            conn, addr = self._server_socket.accept()
            
            p = Process(target=self.initiate_abstract_client, args=(conn,))
            p.start()

    
    def initiate_abstract_client(self, socket):
        abstract_client = AbstractClient(socket)
        abstract_client.handle_client()


class AbstractClient:

    def __init__(self, socket):
        signal.signal(signal.SIGTERM, self.handle_signal)
        
        self._client_socket = socket
        self._stop_server= False
        # self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # self._server_socket.bind((host, port))
        # self._server_socket.listen(listen_backlog) 
        
        self.middleware = None
        self.queue = queue.Queue()
        try:
            middleware = Middleware(self.queue)
        except Exception as e:
            raise e
        self.middleware = middleware
        print("Middleware established the connection")

        self.results = Message("")


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
                
    def _receive_and_forward_data(self):
        msg = Message("")
        while msg.decode() != "EOF":
            socket_content, e = read_socket(self._client_socket)
            if e != None:
                raise e
            
            msg = Message(socket_content)

            self.middleware.send_message(SEND_COORDINATOR_QUEUE, msg.encode())

    def read_results(self, method, body):
        result_slice = Message(body).decode()

        if result_slice == "EOF":
            self.results.set_end_flag()
            self.middleware.stop_consuming()
            self.middleware.ack_message(method)
            return
        
        self.results.push(result_slice)
        self.middleware.ack_message(method)
    
    def _receive_and_forward_results(self):
        callback_with_params = lambda ch, method, properties, body: self.read_results(method, body)
        self.middleware.receive_messages(RECEIVE_COORDINATOR_QUEUE, callback_with_params)
        self.middleware.consume()
        print("Enviando resultados de queries 1 a 5 al cliente...")
        if self.results.is_ended():
            write_socket(self._client_socket, self.results.get_message())
            write_socket(self._client_socket, 'EOF')

    def handle_signal(self, *args):
        print("Gracefully exit")
        self.queue.put('SIGTERM')
        self._stop_server = True
        if self.middleware != None:
            self.middleware.close_connection()
        if self._client_socket != None:
            self._client_socket.close()
        if self._server_socket != None:
            self._server_socket.shutdown(socket.SHUT_RDWR)



def main():    
    HOST, PORT, LISTEN_BACKLOG = os.getenv('HOST'), os.getenv('PORT'), os.getenv('LISTEN_BACKLOG')
    try:
        server = Server(HOST, int(PORT), int(LISTEN_BACKLOG))
        server.run()
    except:
        print('SIGTERM received')

main()
    