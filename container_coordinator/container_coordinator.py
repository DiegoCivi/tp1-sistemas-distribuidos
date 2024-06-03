import socket
from multiprocessing import Process, Manager, Lock
from communications import read_socket, write_socket
import os
import time
import signal

CONNECTION_TRIES = 3
LOOP_CONNECTION_PERIOD = 2
HOST_INDEX = 0
PORT_INDEX = 1

class ContainerCoordinator:

    def __init__(self, id, address, listen_backlog, coordinators_list):

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.bind(address)
        self._socket.listen(listen_backlog)
        self.container_coordinators = {}
        self.stop = False
        self.manager = Manager()
        self.connections = self.manager.dict() # { identifier: TCPsocket }
        self.id = id
        # This list of tuples has the address of the other coordinators with their id [(host1, port1, id1), (host2, port2, id2), ...]
        self.coordinators_list = coordinators_list


    def im_last_coord(self):
        """
        Returns True if the coord is the one with the biggest id
        """
        return self.id == len(self.coordinators_list) - 1

    def run(self):
        if not self.im_last_coord():
            # Create the process that will send the id to the other
            # This will be done by all coordinators, except for the last one  
            id_sender_p = Process(target=self.initiate_id_sender, args=(self.connections,))
            id_sender_p.start()
        processes = []

        # Receive new connections and create a process that will handle them
        while not self.stop:
            conn, addr = self._socket.accept()
            
            # Once the connection is done. We need to hear for the id or name of the connected one.
            identifier, err = read_socket(conn)
            print(f"Soy {self.id} y se me conecto: ", identifier)

            # Start the process responsible for receiving the data from the new connection
            print("Lanzo un proceso")
            p = Process(target=self.initiate_connection, args=(self.id, conn, self.connections,))

            # Put in the dict the identifier with the TCP socket
            self.add_connection(self.connections, identifier, conn)

            p.start()

            processes.append(p)

        if not self.im_last_coord():
            id_sender_p.join()

        for conn_socket in self.connections.items():
            conn_socket.close()

        for p in processes:
            p.join() 

    def initiate_connection(self, coordinator_id, socket, connections):
        while True: # TODO: Check this condition
            msg = read_socket(socket)
            if msg.startswith('ELECTION'):
                # Get the id
                # Get the socket from connections with the id
                # Send the new ELECTION message to the next through that socket
                pass

    def initiate_id_sender(self, connections):
        """
        Tries to connect to the other ContainerCoordinators.
        If the connect() fails, it may be because:
            - The other ContainerCoordinator isn't up yet.
            - The other ContainerCoordinator connected first to us. If this is the case, the connect() will never succed.
        If the connect() succeds, we send our id.
        """
        processes = []
        for host, port, id in self.coordinators_list[self.id + 1:]:
            for _ in range(CONNECTION_TRIES):            
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.connect((host, port))
                    print(f'Soy {self.id} y me conecte a: ', id)
                    # We send our id and create a process to handle the connection
                    err = write_socket(s, str(self.id))
                    if err != None:
                        print('Error')
                        raise err

                    print("Lanzo un proceso")
                    p = Process(target=self.initiate_connection, args=(self.id, s, connections,))
                    
                    self.add_connection(connections, id, s)

                    p.start()
                    processes.append(p)
                    break
                except Exception as e:
                    print(f"No se pudo conectar al coordinator {id}. Error: ", e)
                    time.sleep(LOOP_CONNECTION_PERIOD)
                    continue
        
        for p in processes:
            p.join()


    def add_connection(self, connections, conn_identifier, conn):
        connections[conn_identifier] = conn
        #print(connections)


# HOW TO START A CONTAINER AGAIN:
# To start again a container, the command "docker start <container_name>" can be used.

# Other way could be:
# os.system('docker build -f ./workers_dockerfiles/filter_review_quantity_worker.dockerfile -t "test_container:latest" .')
# os.system('docker run --rm --name test_container "test_container:latest"')

def parse_string_to_list(input_string):
    elements = input_string.split(',')
    
    result = [(elements[i], int(elements[i + 1]), elements[i + 2]) for i in range(0, len(elements), 3)]
    
    return result

def main():
    coordinators_list = os.getenv('COORDINATORS_LIST')                      # host1, port1, id1, host2, port2, id2, ...
    coordinators_list = parse_string_to_list(coordinators_list)             # [(host1, port1, id1), (host2, port2, id2), ...]
    coord_id = int(os.getenv('ID'))
    listen_backlog = int(os.getenv('LISTEN_BACKLOG'))

    coord_info = coordinators_list[coord_id]                                # (host, port, id)
    coord_addr = (coord_info[HOST_INDEX], coord_info[PORT_INDEX])
    print("Mi addr es: ", coord_addr)
    container_coord = ContainerCoordinator(coord_id, coord_addr, listen_backlog, coordinators_list)
    container_coord.run()


main()

        