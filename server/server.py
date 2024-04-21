import socket
from communications import read_socket

def receive_and_forward_data(socket):
    msg = None
    while msg != "EOF":
        msg, e = read_socket(socket)
        if e != None:
            # TODO: Maybe raise the exception or just print it and return
        

def handle_client(socket):
    """
    Reads the client data and fordwards it to the corresponding parts of the system
    """
    # First read the titles dataset
    receive_and_forward_data(socket)
    

    # Then read the reviews dataset
    receive_data(socket)        
        

def main(): 
    socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    socket.bind((HOST, PORT))
    socket.listen(1)

    while True:
        conn, addr = socket.accept()
        print(f'New connection from address {addr}')
        handle_client(conn)
    