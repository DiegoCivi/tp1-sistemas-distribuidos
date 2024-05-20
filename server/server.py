import socket
from communications import read_socket, write_socket
from serialization import Message
from middleware import Middleware
import os
import signal
import queue
from multiprocessing import Process


SEND_COORDINATOR_QUEUE = 'query_coordinator'
RECEIVE_COORDINATOR_QUEUE = 'server' 
EOF_MSG = "EOF"


class Server: # TODO: Implement SIGTERM handling

    def __init__(self, host, port, listen_backlog):
        self.clients = {}
        self._stop_server= False
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind((host, port))
        self._server_socket.listen(listen_backlog)
        self.clients_accepted = 0

    def run(self):
        # Create a process that will be to send the results back to the client
        results_p = Process(target=initiate_result_fordwarder, args=)

        # Receive new clients and create a process that will handle them
        while not self._stop_server:
            conn, addr = self._server_socket.accept()

            print("A new client has connected: ", addr)
            
            client_id = self.clients_accepted
            p = Process(target=initiate_data_fordwarder, args=(conn, client_id,))
            p.start()
            self.clients[client_id] = p
            self.clients_accepted += 1

    
def initiate_data_fordwarder(socket, client_id):
    data_fordwarder = DataFordwarder(socket, client_id)
    data_fordwarder.handle_client()

def initiate_result_fordwarder():
    result_fordwarder = ResultFordwarder()
    result_fordwarder.run()


class ResultFordwarder:

    def __init__(self):
        self.middleware = None
        self.queue = queue.Queue()
        try:
            middleware = Middleware(self.queue)
        except Exception as e:
            raise e
        self.middleware = middleware
        print("Middleware established the connection")

        self.results = Message("")

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


class DataFordwarder:

    def __init__(self, socket, id):
        signal.signal(signal.SIGTERM, self.handle_signal)
        
        self.id = id
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
        while msg.decode() != EOF_MSG:
            socket_content, e = read_socket(self._client_socket)
            if e != None:
                raise e
            if socket_content != EOF_MSG:
                msg = Message(socket_content)
                self.middleware.send_message(SEND_COORDINATOR_QUEUE, msg.encode())
            else:
                msg = EOF_MSG
                self.middleware.send_message(SEND_COORDINATOR_QUEUE, msg)




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
    