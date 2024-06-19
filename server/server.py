import socket
from communications import read_socket, write_socket
from serialization import Message, is_EOF, get_EOF_client_id, split_message_info
from middleware import Middleware
import os
import signal
import queue
from multiprocessing import Process, Queue
from logger import Logger


SEND_COORDINATOR_QUEUE = 'query_coordinator'
RECEIVE_COORDINATOR_QUEUE = 'server' 
EOF_MSG = "EOF"
TITLES_FILE_IDENTIFIER = 't'
REVIEWS_FILE_IDENTIFIER = 'r'
CLIENTS = 'clients'
CLIENTS_QUANTITY = 'clients_quantity'


class Server: # TODO: Implement SIGTERM handling

    def __init__(self, host, port, listen_backlog, log):
        self.clients = {}
        self._stop_server= False
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind((host, port))
        self._server_socket.listen(listen_backlog)
        self.clients_accepted = 1
        self.active_clients = {}                            # Contains the id of the client and their address
        self.sockets_queue = Queue()
        self.log = Logger(log, '0')

    def persist_state(self):
        curr_state = {}
        curr_state[CLIENTS] = self.active_clients
        curr_state[CLIENTS_QUANTITY] = self.clients_accepted
        self.log.persist(curr_state)

    def initialize_state(self):
        prev_state = self.log.read_persisted_data()
        if prev_state != None:
            self.active_clients = prev_state[CLIENTS]
            self.clients_accepted = prev_state[CLIENTS_QUANTITY]

    def run(self):
        # Get the previous state if there was one
        self.initialize_state()

        # Create a process that will be to send the results back to the client
        results_p = Process(target=initiate_result_fordwarder, args=(self.sockets_queue,))
        results_p.start()

        # Receive new clients and create a process that will handle them
        while not self._stop_server:
            conn, addr = self._server_socket.accept()

            print("A new client has connected: ", addr)

            if addr in self.active_clients.values():
                
            
            # Send the new socket with its id through the Queue
            client_id = self.clients_accepted
            self.sockets_queue.put((conn, str(client_id)))
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

    def read_results(self, method, body):
        if is_EOF(body):
            client_id = get_EOF_client_id(body)

            # If the client_id is not in the results_dict. It means
            # his results have already been sent and that the received EOF
            # is a duplicated one, so we hacee to ignore it.
            if client_id in self.results_dict:
                client_id = get_EOF_client_id(body)
                self._send_result(client_id)

            self.middleware.ack_message(method)
            return
        
        # Take the id out of the message so it can be added to its corresponding client id 
        client_id, result_slice = split_message_info(body)
        

        if client_id not in self.results_dict:
            self.results_dict[client_id] = Message(result_slice)

        else:
            self.results_dict[client_id].push(result_slice)
                
        self.middleware.ack_message(method)
    
    def _receive_results(self):
        callback_with_params = lambda ch, method, properties, body: self.read_results(method, body)
        self.middleware.receive_messages(RECEIVE_COORDINATOR_QUEUE, callback_with_params)
        self.middleware.consume()

    def _send_result(self, client_id):
        # If the id is not in our clients dictionary, it MUST be on the sockets_queue
        while not self.sockets_queue.empty():
            new_client_socket, new_client_id = self.sockets_queue.get()
            self.clients[new_client_id] = new_client_socket

        client_socket = self.clients[client_id]
        client_results = self.results_dict[client_id]

        e = write_socket(client_socket, client_results.msg)
        if e != None:
            raise e
        
        write_socket(client_socket, EOF_MSG)
        client_socket.close()
        del self.results_dict[client_id]
        del self.clients[client_id]

class DataFordwarder:

    def __init__(self, socket, id):
        signal.signal(signal.SIGTERM, self.handle_signal)
        
        self.id = str(id)
        self._client_socket = socket
        self._stop_server= False
        
        self.middleware = None
        self.queue = queue.Queue()
        try:
            middleware = Middleware(self.queue)
        except Exception as e:
            raise e
        self.middleware = middleware
        print("Middleware established the connection")
        self.message_parser = Message()


    def handle_client(self):
        """
        Reads the client data and fordwards it to the corresponding parts of the system
        """
        # First read the titles dataset
        self._receive_and_forward_data(TITLES_FILE_IDENTIFIER)
        print("Ya mande todo el archvio titles")
        # Then read the reviews dataset
        self._receive_and_forward_data(REVIEWS_FILE_IDENTIFIER) 
        print("Ya mande todo el archivo reviews")
        #self._client_socket.close()
        self.middleware.close_connection()
                
    def _receive_and_forward_data(self, file_identifier):
        while self.message_parser.decode() != EOF_MSG:
            socket_content, e = read_socket(self._client_socket)
            if e != None:
                raise e
            
            if socket_content != EOF_MSG:
                # Add the id to the message
                msg_id, _, message = split_message_info(socket_content)
                self.message_parser.push(message)
                self.message_parser.add_ids(self.id, msg_id)
                self.middleware.send_message(SEND_COORDINATOR_QUEUE, self.message_parser.encode())
                self.message_parser.clean()
            else:
                # Add the file identifier to the EOF and send it
                self.message_parser.push(socket_content)                        # TODO: Add a func to message_parser so it can create the eof_msg by its own
                msg = EOF_MSG + '_' + self.id + '_' + file_identifier
                self.middleware.send_message(SEND_COORDINATOR_QUEUE, msg)
        
        self.message_parser.clean()
        print(f'CLIENT_{self.id} HAS FINISHED')

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
    HOST, PORT, LISTEN_BACKLOG, LOG = os.getenv('HOST'), os.getenv('PORT'), os.getenv('LISTEN_BACKLOG'), os.getenv('LOG')
    server = Server(HOST, int(PORT), int(LISTEN_BACKLOG), LOG)
    server.run()

main()
    