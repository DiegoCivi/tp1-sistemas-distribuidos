import socket
from multiprocessing import Process, Manager, Queue
from communications import read_socket, write_socket
import os
import time
import signal

CONNECTION_TRIES = 3
LOOP_CONNECTION_PERIOD = 2
HOST_INDEX = 0
PORT_INDEX = 1
RECONNECTION_SLEEP = 5
QUEUE_SIZE = 10
END_MSG = 'END'
CONTAINERS_LIST = "containers_list.config"
WORKERS_PORT = 4321

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
    
    def initiate_connection(self, id, socket, connections, leader = False):
        """
        Handler of messages of a connection. This connection can be another ContainerCoordinator
        or a worker from the system.
        """
        if leader:
            health_checker = HealthChecker()
            healthcheck_process = Process(target=health_checker.check_connection, args=(id, socket, ))
            healthcheck_process.start()
            self.processes.append(healthcheck_process)
            
        # Do whatever it follows...

        # while True: # TODO: Check this condition
            # self._socket.settimeout(1)
        #     msg = read_socket(socket)
        #     if msg.startswith('ELECTION'):
        #         # Get the id
        #         # Get the socket from connections with the id
        #         # Send the new ELECTION message to the next through that socket
        #         pass
    
    def join_processes(self):
        """
        Every class that manages processes needs to join them and close them.
        """
        for p in self.processes:
            p.join()
            p.close()

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

    def connect_to_coordinators(self, reconnection):
        """
        Connect to the other ContainerCoordinators.
        
        If reconnection = False, he will need to connect to every ContainerCoordinator
        that has a bigger id. (This is how the connection is configured)

        If reconnection = True, he will need to connect back to all the ContainerCoordinators.
        """
        if reconnection:
            time.sleep(RECONNECTION_SLEEP)
            print(f"INICIO RECONECCION, connections: {len(self.connections)}  list: {len(self.coordinators_list) - 1}")
            if len(self.connections) == len(self.coordinators_list) - 1:
                print('NO SE HACE LA RECONEXION')
                return
            coordinators_list = self.coordinators_list
        else:
            coordinators_list = self.coordinators_list[self.id + 1:]

        for host, port, id in coordinators_list:
            if id not in self.connections and str(self.id) != id:
                for _ in range(CONNECTION_TRIES):            
                    try:
                        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        print(f'Soy {self.id} y me voy a conectar a: ', id)
                        s.connect((host, port))
                        print(f'Soy {self.id} y me conecte a: ', id)
                        # We send our id and create a process to handle the connection
                        err = write_socket(s, str(self.id))
                        if err != None:
                            print('Error')
                            raise err

                        p = Process(target=self.initiate_connection, args=(self.id, s, self.connections, False,))
                        
                        self.add_connection(self.connections, id, s)

                        p.start()
                        self.processes.append(p)
                        break
                    except Exception as e:
                        print(f"No se pudo conectar al coordinator {id}. Error: ", e)
                        time.sleep(LOOP_CONNECTION_PERIOD)
                        continue
        self.join_processes()

    def connect_to_workers(self, workers_port):
        """
        Connect to the workers. This is only done by the last ContainerCoordinator.
        """
        print("Voy a conectarme a los workers")
        for container in self.containers_list:
            for _ in range(CONNECTION_TRIES):
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.connect((container, workers_port))
                    print(f'Soy {self.id} y me conecte a: ', container)

                    p = Process(target=self.initiate_connection, args=(container, s, self.connections, True))
                    
                    self.add_connection(self.connections, container, s)

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

    def __init__(self, id, address, listen_backlog, coordinators_list, containers_list, workers_port):

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.bind(address)
        self._socket.listen(listen_backlog)
        self.container_coordinators = {}
        self.containers = {}
        self.stop = False
        self.manager = Manager()
        self.connections = self.manager.dict() # { identifier: TCPsocket }
        self.id = id
        # List of containers that the coordinator is responsible for
        self.containers_list = containers_list
        self.workers_port = workers_port
        # This list of tuples has the address of the other coordinators with their id [(host1, port1, id1), (host2, port2, id2), ...]
        self.coordinators_list = coordinators_list
        # List to have all the created processes to join them later
        self.processes = []

    def im_last_coord(self):
        """
        Returns True if the coord is the one with the biggest id
        """
        return self.id == len(self.coordinators_list) - 1
    
    def create_connector(self, coord_id, connections, coordinators_list, containers_list, reconnection, coord = True):
        connector = Connector(coord_id, connections, coordinators_list, containers_list)
        connector.connect_to_coordinators(reconnection) if coord == True else connector.connect_to_workers(self.workers_port)
    
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
        self._socket.close()

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
        while len(self.connections) < len(self.coordinators_list) - 1:
            try:
                self._socket.settimeout(1)
                print("Entre")
                conn, addr = self._socket.accept()
                print("Nueva conexion")
                # Once the connection is done. We need to hear for the id or name of the connected one.
                identifier, err = read_socket(conn)
                if err:
                    raise err
                print(f"Soy {self.id} y se me conecto: ", identifier)
                # Start the process responsible for receiving the data from the new connection
                p = Process(target=self.initiate_connection, args=(self.id, conn, self.connections, False,))

                # Put in the dict the identifier with the TCP socket
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

class HealthChecker():
    """
    Responsible for checking the health of a certain connection. If the connection
    is not healthy, it will restart the container. To determine if the connection is
    healthy, a simple message will be sent to the other side with a timeout of fixed
    time. If an ACK is not received, the container will be restarted after waiting 
    for a little bit.

    """
    
    def check_connection(self, id, conn):
        """
        Check the health of the connection with the id.
        """
        while True:
            try:
                # print(f"Performing health check with {id}")
                err = write_socket(conn, "HEALTH_CHECK")
                if err:
                    print(f"Error in container {id}, err was: {err}")
                    raise err
                conn.settimeout(5) # TODO: Use an environment variable for this or a constant
                msg, err = read_socket(conn) # TODO: CHECK THE SOCKET PROTOCOL (udp or tcp) *IMPORTANT*
                if id == "filter_title_worker1":
                    # print(msg, err)
                    pass
                if err:
                    print(f"Error in container {id}, err was: {err}")
                    raise err
                elif msg == "ACK":
                    # print(f"Health check with {id} was successful")
                    continue
                else:
                    raise Exception(f"Unexpected message from container: {msg}")
                
            except: # (socket.timeout, socket.error, ConnectionResetError, Exception)
                print(f"REINICIO DE CONTAINER {id} POR TIMEOUT O ERROR", flush=True, end="\n")
                self.restart_container(id)
                # raise Exception(f"Error in container {id}, err was: {err} ESTOY POR REINICIAR, LLEGUE ACA {a}")
                time.sleep(15)
                conn = self.reconnect(id)

    
    def restart_container(self, id):
        """
        Restart the container with the id.
        """
        try:
            print(f"Restarting container {id}")
            result = os.system(f"docker restart {id}")
            if result != 0:
                raise Exception(f"Failed to restart container {id}")
            print(f"Container {id} has been restarted")
        except Exception as e:
            print(f"Exception occurred while restarting container {id}: {e}")

    def reconnect(self, id):
        while True:
            try:
                conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                print(f"Reconnecting to {(id, WORKERS_PORT)}")
                conn.connect((id, WORKERS_PORT))
                # print(f"Reconnected to {self.host}:{self.port}")
                return conn
            except Exception as e:
                print(f"Reconnection failed: {e}")
                raise e
                time.sleep(5)


# HOW TO START A CONTAINER AGAIN:
# To start again a container, the command "docker start <container_name>" can be used.

# Other way could be:
# os.system('docker build -f ./workers_dockerfiles/filter_review_quantity_worker.dockerfile -t "test_container:latest" .')
# os.system('docker run --rm --name test_container "test_container:latest"')

def parse_string_to_list(input_string):
    elements = input_string.split(',')
    
    result = [(elements[i], int(elements[i + 1]), elements[i + 2]) for i in range(0, len(elements), 3)]
    
    return result

def read_workers_file(file_path):
    with open(file_path, 'r') as file:
        return [line.strip() for line in file.readlines()]

def main():
    coordinators_list = os.getenv('COORDINATORS_LIST')                      # host1, port1, id1, host2, port2, id2, ...
    coordinators_list = parse_string_to_list(coordinators_list)             # [(host1, port1, id1), (host2, port2, id2), ...]
    coord_id = int(os.getenv('ID'))
    listen_backlog = int(os.getenv('LISTEN_BACKLOG'))
    workers_port = int(os.getenv('WORKERS_PORT'))
    containers_list = read_workers_file(CONTAINERS_LIST)

    coord_info = coordinators_list[coord_id]                                # (host, port, id)
    coord_addr = (coord_info[HOST_INDEX], coord_info[PORT_INDEX])
    print("Mi addr es: ", coord_addr)
    container_coord = ContainerCoordinator(coord_id, coord_addr, listen_backlog, coordinators_list, containers_list, workers_port)
    container_coord.run()


main()

        