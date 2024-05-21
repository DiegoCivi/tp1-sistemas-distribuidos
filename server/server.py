import socket
from communications import read_socket, write_socket
from serialization import Message, deserialize_into_titles_dict, ID_FIELD, RESULT_SLICE_FIELD, LAST_EOF_INDEX, EOF_ID_INDEX
from middleware import Middleware
import os
import signal
import queue
from multiprocessing import Process, Queue


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
        self.sockets_queue = Queue()

    def run(self):
        # Create a process that will be to send the results back to the client
        results_p = Process(target=initiate_result_fordwarder, args=(self.sockets_queue))
        results_p.start()

        # Receive new clients and create a process that will handle them
        while not self._stop_server:
            conn, addr = self._server_socket.accept()

            print("A new client has connected: ", addr)
            
            # Send the new socket with its id through the Queue
            client_id = self.clients_accepted
            self.sockets_queue.put((conn, client_id))
            # Start the process responsible for receiving the data from the client
            p = Process(target=initiate_data_fordwarder, args=(conn, client_id,))
            p.start()
            self.clients[client_id] = p
            self.clients_accepted += 1

        # TODO: Put this inf a separated function
        for process in self.clients.values():
            process.join()

        results_p.join()

    
def initiate_data_fordwarder(socket, client_id):
    data_fordwarder = DataFordwarder(socket, client_id)
    data_fordwarder.handle_client()

def initiate_result_fordwarder(sockets_queue):
    result_fordwarder = ResultFordwarder(sockets_queue)
    result_fordwarder.run()


class ResultFordwarder:

    def __init__(self, sockets_queue):
        self.clients = {}
        self.sockets_queue = sockets_queue
        self.middleware = None
        self.queue = queue.Queue()
        self.results_dict = {}
        
        try:
            middleware = Middleware(self.queue)
        except Exception as e:
            raise e
        self.middleware = middleware
        print("Middleware established the connection")

        self.results = Message("")

    def run(self):
        self._receive_results()
        print("Enviando resultados de queries 1 a 5 al cliente...")
        self.send_results()

    def read_results(self, method, body):

        if body[:LAST_EOF_INDEX] == "EOF_":
            client_id = body[EOF_ID_INDEX].decode('utf-8')
            self._send_result(client_id)
            self.middleware.ack_message(method)
            return
        
        # Take the id out of the message so it can be added to its corresponding client id
        # From the QueryCoordinator, messages with format ID:[client_id]@|@result_slice:[result_slice]
        message_dict = deserialize_into_titles_dict(body)
        client_id = message_dict[ID_FIELD]
        result_slice = message_dict[RESULT_SLICE_FIELD]

        if client_id not in self.results_dict:
            self.results_dict[client_id] = Message()
        else:
            self.results_dict[client_id].push(result_slice)
                
        self.middleware.ack_message(method)
    
    def _receive_results(self):
        callback_with_params = lambda ch, method, properties, body: self.read_results(method, body)
        self.middleware.receive_messages(RECEIVE_COORDINATOR_QUEUE, callback_with_params)
        self.middleware.consume()
        
        # Para que esta este if? Que pasa si no see cumple la condicion? En que casos no se cumpliria la condicion????? 
        # if self.results.is_ended():
        #     write_socket(self._client_socket, self.results.get_message())
        #     write_socket(self._client_socket, 'EOF')

    def _send_result(self, client_id):
        # If the id is not in our clients dictionary, it must be on the sockets_queue
        while not self.sockets_queue.empty():
            new_client_socket, new_client_id = self.sockets_queue.get()
            self.clients[new_client_id] = new_client_socket

        client_socket = self.clients[client_id]
        client_results = self.results_dict[client_id]
        write_socket(client_socket, client_results)
        write_socket(client_socket, 'EOF')
        client_socket.close()
        del self.results_dict[client_id]



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
    