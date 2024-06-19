import socket
from multiprocessing import Process, Manager, Queue
from communications import read_socket, write_socket
import random
import os
import docker
import time
import signal
from healthchecking import HealthChecker, HealthCheckHandler

CONNECTION_TRIES = 20
LOOP_CONNECTION_PERIOD = 2
HOST_INDEX = 0
PORT_INDEX = 1
RECONNECTION_SLEEP = 5
QUEUE_SIZE = 10
END_MSG = 'END'
CONTAINERS_LIST = "containers_list.config"
WORKERS_PORT = 4321
HC_PORT = 2345

class ProcessCreator:
    """
    Classes that create other process need some simple and common functionality.
    They will inherit this from ProcessCreator which will have the fundamentals
    for the handling of new process.
    For example, joining the processes, handling their messages, or adding their
    connection info.
    """

    def __init__(self):
        self.processes = []

    def add_connection(self, connections, conn_identifier, conn):
        """
        Adds a connection to a process shared dict. This dict will have the 
        necessary info for processes to communicate with any socket they need.
        """
        connections[conn_identifier] = conn
    
    def initiate_connection(self, id, port, sock, hc_sock, connections, leader = False, current_connection=None):
        """
        Handler of messages of a connection. This connection can be another ContainerCoordinator
        or a worker from the system.
        """
        try:
            if leader:
                # I'm the leader, I have to send healthchecks to the workers and coordinators, if
                # they don't respond, I have to restart them, and if I die, coordinators will
                # notice and start a new election 
                health_checker = HealthChecker()
                health_checker.check_connection(id, HC_PORT, hc_sock, True if "container_coordinator" in id else False)
                hc_sock.close()
                return
            
            print(f'socket is alive: {sock.fileno()}')
            while True:
                # Use the sock here (messages between coords)
                if current_connection:
                    sock = connections[current_connection]
                msg, err = read_socket(sock)
                if err != None:
                    # print(f"Error reading from socket: {err}")
                    break
                print(f"Message received from {id}: {msg}")
                if msg.startswith('ELECTION'):
                    # Get the id
                    # Get the socket from connections with the id
                    # Send the new ELECTION message to the next through that socket
                    pass

            # while True: # Use the sock here (messages between coords)
                # self._socket.settimeout(1)
            #     msg = read_socket(socket)
            #     if msg.startswith('ELECTION'):
            #         # Get the id
            #         # Get the socket from connections with the id
            #         # Send the new ELECTION message to the next through that socket
            #         pass
        except Exception as e:
            print(f"Exception in initiate_connection for id {id}: {e}")
    

    def join_processes(self):
        """
        Every class that manages processes needs to join them and close them.
        """
        
        for p in self.processes:
            try:
                p.join()
                p.close()
            except Exception as e:
                print(f"Error joining process: {e}")
                continue


class Connector(ProcessCreator):
    """
    Responsible for connecting either to other ContainerCoordinators or workers.
    Every ContainerCoodinator needs to be connected to every other ContainerCoodinator.
    This means that some of them will wait for connections, and others will be responsible for connecting.
    
    Reconnection to the network is necessary if a ContainerCoordinator or worker wants to get back to the network.
    If this is the case, he will need to connect back to every other ContainerCoordinator. 
    In case of being a worker the healthchecker will be responsible for restarting the container.
    """

    def __init__(self, id, connections, coordinators_list = [], containers_list = []):
        self.id = id
        self.connections = connections
        self.coordinators_list = coordinators_list
        self.containers_list = containers_list
        # List to have all the created processes to join them later
        self.processes = []

    def connect_to_coordinators(self, reconnection, port):
        """
        Connect to the other ContainerCoordinators.
        
        If reconnection = False, he will need to connect to every ContainerCoordinator
        that has a bigger id. (This is how the connection is configured)

        If reconnection = True, he will need to connect back to all the ContainerCoordinators.
        """
        if reconnection: # Check if all the connectionss are already done to avoid reconnection
            time.sleep(RECONNECTION_SLEEP)
            print(f"INICIO RECONECCION, connections: {len(self.connections)}  list: {len(self.coordinators_list) - 1}")
            if len(self.connections) == len(self.coordinators_list) - 1:
                print('NO SE HACE LA RECONEXION')
                return
            coordinators_list = self.coordinators_list
        else:
            coordinators_list = self.coordinators_list[self.id + 1:]

        for host, id in coordinators_list:
            if str(id) not in self.connections and self.id != id:
                for _ in range(CONNECTION_TRIES):            
                    try:
                        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        print(f'Soy {self.id} y me voy a conectar a: {id} con host: {host} y port: {port}')
                        print(f'Types de host y port: {type(host)} {type(port)}')
                        s.connect((host, port))
                        print(f'Soy {self.id} y me CONECTE a: ', id)
                        # We send our id and create a process to handle the connection
                        err = write_socket(s, str(self.id))
                        if err != None:
                            print('Error')
                            raise err
                        print("Soy un coordinador mas, paso el id: ", host)
                        p = Process(target=self.initiate_connection, args=(host, port, s , None, self.connections, False))
                        self.add_connection(self.connections, str(id), s)

                        p.start()
                        self.processes.append(p)
                        break
                    except Exception as e:
                        print(f"No se pudo conectar al coordinator {id}. Error: ", e)
                        time.sleep(LOOP_CONNECTION_PERIOD)
                        continue
        self.join_processes()

    def connect_to_workers(self, port):
        """
        Connect to the workers. This is only done by the last ContainerCoordinator.
        """
        print("Voy a conectarme a los workers")
        for container in self.containers_list:
            if "container_coordinator" in container:
                continue
            while True: # cambiar esto
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.connect((container, port))
                    print(f'Soy {self.id} y me conecte a: ', container)

                    p = Process(target=self.initiate_connection, args=(container, port, None, s, self.connections, True,))
                    
                    self.add_connection(self.connections, self.id, container, s)

                    p.start()
                    self.processes.append(p)
                    break
                except Exception as e:
                    print(f"No se pudo conectar al worker {container}. Error: ", e)
                    time.sleep(LOOP_CONNECTION_PERIOD)
                    continue
        self.join_processes()
    

class ContainerCoordinator(ProcessCreator):
    """
    Responsible for handling connections from other ContainerCoordinators or
    workers from the system. It has a leader in his network. If he is the leader,
    and one of his connection shutdowns, it needs to get that container back and running.
    """

    def __init__(self, id, address, listen_backlog, coordinators_list, containers_list, coords_port, workers_port):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.bind(address)
        self._socket.listen(listen_backlog)
        self.hc_socket = None
        self.container_coordinators = {}
        self.containers = {}
        self.stop = False
        self.manager = Manager()
        self.connections = self.manager.dict() # { identifier: TCPsocket, ...}
        self.id = id
        # List of containers that the coordinator is responsible for
        self.containers_list = containers_list
        self.workers_port = workers_port
        self.coords_port = coords_port
        # This list of tuples has the address of the other coordinators with their id [(host1, port1, id1), (host2, port2, id2), ...]
        self.coordinators_list = coordinators_list
        # List to have all the created processes to join them later
        self.processes = []
        self.health_check_sockets = {}
        self.hc_socket = None
        self.hc_conn = None
        if not self.im_last_coord():
            health_checking_process = Process(target=self.create_health_check)
            health_checking_process.start()
            self.processes.append(health_checking_process)

    def im_last_coord(self):
        """
        Returns True if the coord is the one with the biggest id
        """
        return self.id == len(self.coordinators_list) - 1
    
    def create_connector(self, coord_id, connections, coordinators_list, containers_list, reconnection, coord = True):
        connector = Connector(coord_id, connections, coordinators_list, containers_list)
        connector.connect_to_coordinators(reconnection, self.coords_port) if coord == True else connector.connect_to_workers(self.workers_port)
    
    def initiate_reconnection(self):
        """
        For a reconnection, a new process needs to run Connector instance with
        reconnection parameter set to True.
        """
        self.initiate_connector(True)

    def initiate_connector(self, reconnection):
        """
        Creates, starts and saves a process that runs a Connector instance.
        """
        p = Process(target=self.create_connector, args=(self.id, self.connections, self.coordinators_list, [], reconnection, True))
        p.start()
        self.processes.append(p)
    
    def close_resources(self):
        """
        Close the socket and the processes
        """
        self.join_processes()
        for conn in self.health_check_sockets.values():
            conn.close()
        self._socket.close()

    def create_health_check(self):
        self.hc_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.hc_socket.bind((f'container_coordinator_{self.id}', HC_PORT)) 
        self.hc_socket.listen(1)
        self.hc_conn, _ = self.hc_socket.accept()
        self.health_check_handler = HealthCheckHandler(socket=None, conn=self.hc_conn)
        hc_process = Process(target=self.health_check_handler.handle_health_check_with_timeout, args=(5,))
        hc_process.start()
        hc_process.join()

    def run(self):
        """
        Here everything starts. 
        -   A reconnection process is crated for later and will be used if the container was restarted
            and needs to connect back to everybody.
        -   Connections are also accepted. If a new ContainerCordinator or worker wants to connect, he will
            be accepted here.
        -   When everything finishes, all process are joined and closed 
        """
        # Process that will reconnect to the network if it crashed before
        self.initiate_reconnection()

        if not self.im_last_coord():
            # Create the process that will send the id to the other
            # This will be done by all coordinators, except for the last one
            self.initiate_connector(False)

        # Receive new connections and create a process that will handle them
        print("Voy a entrar al while")
        while True:
            try:
                self._socket.settimeout(1)
                print("Entre")
                conn, addr = self._socket.accept()
                print("Nueva conexion")
                # Once the connection is done. We need to hear for the id or name of the connected one.
                identifier, err = read_socket(conn)
                if err:
                    raise err
                # if identifier in self.connections:
                #     print(f"Ya esta conectado el container_coordinator_{identifier}")
                #     continue
                print(f"Soy {self.id} y se me conecto: container_coordinator_{identifier}")
                # Start the process responsible for receiving the data from the new connection
                hc_socket = None #TODO: See where to close the socket 
                if self.im_last_coord():
                    hc_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    hc_socket.connect((f'container_coordinator_{identifier}', HC_PORT))
                    self.health_check_sockets[identifier] = hc_socket
                    p = Process(target=self.initiate_connection, args=(f'container_coordinator_{identifier}', self.coords_port, conn, hc_socket, self.connections, True,))
                else:
                    p = Process(target=self.initiate_connection, args=(self.id, self.coords_port, conn, None, self.connections, False, identifier))

                # Put in the dict the identifier with the TCP socket, if it's already added, it will be replaced
                self.add_connection(self.connections, identifier, conn)

                p.start()
                self.processes.append(p)
            except Exception as e:
                print("Error: ", e)
                pass
        
        # Now we have all the connections to the other coordinators, we need to connect to the workers if 
        # I am the last coordinator
        if self.im_last_coord():
            p = Process(target=self.create_connector, args=(self.id, self.connections, [], self.containers_list, False, False))
            p.start()
            self.processes.append(p)


        # We wait for the end of the children processes
        self.close_resources()
        hc_socket.close()
        self.hc_conn.close()

# HOW TO START A CONTAINER AGAIN:
# To start again a container, the command "docker start <container_name>" can be used.

# Other way could be:
# os.system('docker build -f ./workers_dockerfiles/filter_review_quantity_worker.dockerfile -t "test_container:latest" .')
# os.system('docker run --rm --name test_container "test_container:latest"')

def parse_string_to_list(input_string):
    elements = input_string.split(',')
    
    result = [(element, int(element[-1])) for element in elements]
    
    return result

def read_workers_file(file_path):
    with open(file_path, 'r') as file:
        return [line.strip() for line in file.readlines()]

def main():
    coordinators_list = os.getenv('COORDS_LIST')                      # host1, port1, id1, host2, port2, id2, ...
    coordinators_list = parse_string_to_list(coordinators_list)             # [(host1, port1, id1), (host2, port2, id2), ...]
    coord_id = int(os.getenv('ID'))
    coords_port = int(os.getenv('COORDS_PORT'))
    workers_port = int(os.getenv('WORKERS_PORT'))
    listen_backlog = int(os.getenv('LISTEN_BACKLOG'))
    containers_list = read_workers_file(CONTAINERS_LIST)

    coord_info = coordinators_list[coord_id]                                # (host, port, id)
    coord_addr = (coord_info[HOST_INDEX], coords_port)
    print("Mi addr es: ", coord_addr)
    container_coord = ContainerCoordinator(coord_id, coord_addr, listen_backlog, coordinators_list, containers_list, coords_port, workers_port)
    container_coord.run()


main()

        