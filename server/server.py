import socket
from communications import read_socket, write_socket
from serialization import Message, is_EOF, get_EOF_client_id, split_message_info
from middleware import Middleware
import os
import signal
import queue
from multiprocessing import Process, Queue, Lock
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
        self.log_lock = Lock()

    def persist_state(self):
        curr_state = {}
        curr_state[CLIENTS] = self.active_clients
        curr_state[CLIENTS_QUANTITY] = self.clients_accepted
        
        self.log_lock.acquire()
        self.log.persist(curr_state)
        self.log_lock.release()

    def initialize_state(self):
        prev_state = self.log.read_persisted_data()
        if prev_state != None:
            self.active_clients = prev_state[CLIENTS]
            self.clients_accepted = prev_state[CLIENTS_QUANTITY]

    def run(self):
        # Get the previous state if there was one
        self.initialize_state()

        # Create a process that will be to send the results back to the client
        results_p = Process(target=initiate_result_fordwarder, args=(self.sockets_queue, self.log, self.log_lock,))
        results_p.start()

        # Receive new clients and create a process that will handle them
        while not self._stop_server:
            conn, addr = self._server_socket.accept()

            print("A new client has connected: ", addr)

            if addr in self.active_clients:
                # The address was already in our state, meaning the client had been already received before
                client_id = self.active_clients[addr]
            else:
                # Send the new socket with its id through the Queue
                client_id = self.clients_accepted
                self.active_clients[addr] = client_id
                self.clients_accepted += 1
                self.persist_state()
            
            self.sockets_queue.put((conn, str(client_id)))
            # Start the process responsible for receiving the data from the client
            p = Process(target=initiate_data_fordwarder, args=(conn, client_id,))
            p.start()
            self.clients[client_id] = p

        # TODO: Put this in a separated function
        for process in self.clients.values():
            process.join()

        results_p.join()

def initiate_data_fordwarder(socket, client_id):
    data_fordwarder = DataFordwarder(socket, client_id)
    data_fordwarder.handle_client()

def initiate_result_fordwarder(sockets_queue, log, log_lock):
    result_fordwarder = ResultFordwarder(sockets_queue, log, log_lock)
    result_fordwarder.run()


class ResultFordwarder:

    def __init__(self, sockets_queue, log, log_lock):
        self.clients = {}
        self.sockets_queue = sockets_queue
        self.log = log
        self.log_lock = log_lock
        self.middleware = None
        self.queue = queue.Queue()
        
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
        """
        Each message contains the results of every query for
        a specific client.
        """
        client_id, result_msg = split_message_info(body)
        self._send_result(client_id, result_msg)

        self.middleware.ack_message(method)
   
    def _receive_results(self):
        callback_with_params = lambda ch, method, properties, body: self.read_results(method, body)
        self.middleware.receive_messages(RECEIVE_COORDINATOR_QUEUE, callback_with_params)
        self.middleware.consume()

    def _send_result(self, client_id, result_msg):
        # If the id is not in our clients dictionary, it MUST be on the sockets_queue
        while not self.sockets_queue.empty():
            new_client_socket, new_client_id = self.sockets_queue.get()
            self.clients[new_client_id] = new_client_socket

        client_socket = self.clients[client_id]

        e = write_socket(client_socket, result_msg)
        if e != None:
            # If the writing fails, its because the client crashed.
            # In this case we will do nothing and forget about 
            # the client. 
            return
        
        write_socket(client_socket, EOF_MSG)
        client_socket.close()
        del self.clients[client_id]

        self.update_state(client_id)
    
    def update_state(self, client_id):
        self.log_lock.acquire()
        prev_state = self.log.read_persisted_data()
        addr_to_delete = None
        for addr, id in prev_state[CLIENTS].items():
            if id == client_id:
                addr_to_delete = addr
                break
        if addr_to_delete != None:
            del prev_state[CLIENTS][addr_to_delete]
            self.log.persist(prev_state)
        self.log_lock.release()

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
        self._receive_and_forward_data()
        self.middleware.close_connection()
                
    def _receive_and_forward_data(self):
        while True:
            socket_content, e = read_socket(self._client_socket)
            if e != None:
                raise e
            
            if not self.message_parser.is_EOF(socket_content):
                # Add the id to the message and send it
                msg_id, _, message = split_message_info(socket_content)
                self.message_parser.push(message)
                self.message_parser.add_ids(self.id, msg_id)
                self.middleware.send_message(SEND_COORDINATOR_QUEUE, self.message_parser.encode())
                # Send the ack
                write_socket(self._client_socket, 'ACK')
                # Clean it so the content of the next message doesnt mix with the current one
                self.message_parser.clean()
            else:
                # Add the file identifier to the EOF and send it
                self.message_parser.create_EOF(self.id, socket_content)
                self.middleware.send_message(SEND_COORDINATOR_QUEUE, self.message_parser.encode())
                if self.message_parser.get_file_identifier(socket_content) ==  REVIEWS_FILE_IDENTIFIER:
                    break
        
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
    