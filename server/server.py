import socket
from communications import read_socket, write_socket
from serialization import Message, is_EOF, get_EOF_id, split_message_info
from middleware import Middleware
import os
import signal
import queue
from multiprocessing import Process, Queue
from healthchecking import HealthCheckHandler


SEND_COORDINATOR_QUEUE = 'query_coordinator'
RECEIVE_COORDINATOR_QUEUE = 'server' 
EOF_MSG = "EOF"


class Server: # TODO: Implement SIGTERM handling

    def __init__(self, host, port, health_check_port, listen_backlog):
        self.clients = {}
        self._stop_server= False
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind((host, port))
        self._server_socket.listen(listen_backlog)
        self.clients_accepted = 0
        self.sockets_queue = Queue()
        self.health_check_handler = HealthCheckHandler(host, health_check_port)
        self.health_check_handler_p = Process(target=self.health_check_handler.handle_health_check)
        self.health_check_handler_p.start()

    def run(self):
        # Create a process that will be to send the results back to the client
        results_p = Process(target=initiate_result_fordwarder, args=(self.sockets_queue,))
        results_p.start()

        # Receive new clients and create a process that will handle them
        while not self._stop_server:
            conn, addr = self._server_socket.accept()

            print("A new client has connected: ", addr)
            
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
        self.health_check_handler_p.join()

    
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
        print("ResultFordwarder is running")
        self._receive_results()
        #print("Enviando resultados de queries 1 a 5 al cliente...")
        #self.send_results()

    def read_results(self, method, body):
        #print(body)
        if is_EOF(body):
            print('LLego el EOF')
            client_id = get_EOF_id(body)
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
        #print(client_results)
        e = write_socket(client_socket, client_results.msg)
        if e != None:
            raise e
        write_socket(client_socket, EOF_MSG)
        client_socket.close()
        del self.results_dict[client_id]
        del self.clients[client_id]
        print("YA LE MANDE AL CLIENT")



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
        self._receive_and_forward_data()
        print("Ya mande todo el archvio titles")
        # Then read the reviews dataset
        self._receive_and_forward_data() 
        print("Ya mande todo el archivo reviews")
                
    def _receive_and_forward_data(self):
        while self.message_parser.decode() != EOF_MSG:
            socket_content, e = read_socket(self._client_socket)
            if e != None:
                raise e
            
            if socket_content != EOF_MSG:
                # Add the id to the message
                _, message = split_message_info(socket_content)
                self.message_parser.push(message)
                self.message_parser.add_id(self.id)
                self.middleware.send_message(SEND_COORDINATOR_QUEUE, self.message_parser.encode())
                self.message_parser.clean()
            else:
                # Add the id to the EOF and send it
                self.message_parser.push(socket_content)
                msg = EOF_MSG + '_' + self.id
                self.middleware.send_message(SEND_COORDINATOR_QUEUE, msg)
        
        print(f'CLIENT_{self.id} HAS FINISHED')
        self.message_parser.clean()

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
    HOST, PORT, HEALTH_CHECK_PORT, LISTEN_BACKLOG = os.getenv('HOST'), os.getenv('PORT'), os.getenv("HC_PORT"), os.getenv('LISTEN_BACKLOG')
    server = Server(HOST, int(PORT), int(HEALTH_CHECK_PORT), int(LISTEN_BACKLOG))
    server.run()

main()
    