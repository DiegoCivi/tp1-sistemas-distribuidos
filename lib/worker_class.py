from serialization import *


class Worker:

    def _create_batches(self, batch, next_workers_quantity):
        raise Exception('Function needs to be implemented')

    def _send_batches(self, workers_batches, output_queue, client_id, msg_id):
        raise Exception('Function needs to be implemented')

    def create_and_send_batches(self, batch, client_id, output_queue, next_workers_quantity, msg_id=NO_ID):
        workers_batches = self._create_batches(batch, next_workers_quantity)

        self._send_batches(workers_batches, output_queue, client_id, msg_id)

    def send_EOFs(self, client_id, output_queue, next_workers_quantity):
        eof_msg = create_EOF(client_id, self.worker_id)
        for worker_id in range(next_workers_quantity):
            worker_queue = create_queue_name(output_queue, str(worker_id))
            self.middleware.send_message(worker_queue, eof_msg)
        
    def manage_message(self, client_id, data, method, msg_id=NO_ID):
        raise Exception('Function needs to be implemented')
    
    def received_all_clients_EOFs(self, client_id):
        self.eof_counter[client_id] = self.eof_counter.get(client_id, 0) + 1
        if self.eof_quantity == self.eof_counter[client_id]:
            return True
        return False
    
    def add_EOF_worker_id(self, client_id, worker_id):
        client_eof_workers_ids = self.eof_workers_ids.get(client_id, set())
        client_eof_workers_ids.add(worker_id)
        self.eof_workers_ids[client_id] = client_eof_workers_ids

    def delete_client_EOF_counter(self, client_id):
        """
        All the EOFs for the client with id = client_id have reached, so we
        delete the counter.
        """
        del self.eof_counter[client_id]

    def is_EOF_repeated(self, worker_id, client_id, client_eof_workers_ids):
        """
        Check if the EOF was already received from that worker (This is done to handle duplicated EOFs). 
        If already received, we reeturn True and the EOF can be inmediately acked.
        If not, the worker id is saved, we return False and the EOF can be managed.
        """
        if worker_id not in client_eof_workers_ids:
            self.add_EOF_worker_id(client_id, worker_id)
            return False
        return True
    
    def client_is_active(self, client_id):
        raise Exception('Function needs to be implemented')
    
    def manage_EOF(self, body, method, client_id):
        raise Exception('Function needs to be implemented')


    def ack_EOFs(self, client_id):
        # try:
        #     if self.last_msg:
        #         self.middleware.ack_all(self.last_msg)
        #         self.last_msg = None
        #     else:
        #         raise Exception('si')
        #     return
        # except:

        for delivery_tag in self.clients_unacked_eofs[client_id]:
            self.middleware.ack_message(delivery_tag)

        del self.clients_unacked_eofs[client_id]
        
    def add_unacked_EOF(self, client_id, eof_method):
        unacked_eofs = self.clients_unacked_eofs.get(client_id, set())         
        unacked_eofs.add(eof_method.delivery_tag)

        self.clients_unacked_eofs[client_id] = unacked_eofs

    def is_message_repeated(self, client_id, msg_id):
        raise Exception('Function needs to be implemented')
    
    def handle_message(self, method, client_id, msg_id, data):
        raise Exception('Function needs to be implemented')

    def handle_data(self, method, body):       
        if is_EOF(body):
            #############################################################
            try:
                if self.last_msg == 'NOSE':
                    self.middleware.send_message('DEBUG', f'LLEGO UN EOF [{body}]')
                else:
                    self.middleware.send_message('DEBUG', f'LLEGO UN EOF [{body}]')
            except:
                pass
            #############################################################
            worker_id = get_EOF_worker_id(body)                                 # The id of the worker that sent the EOF
            client_id = get_EOF_client_id(body)                                 # The id of the active client
            client_eof_workers_ids = self.eof_workers_ids.get(client_id, set()) # A set with the ids of the workers that already sent their EOF for this client

            #############################################################
            try:
                if len(self.clients_unacked_msgs) > 0:
                    self.middleware.send_message('DEBUG', 'LLEGO UN EOF, PERO QUEDABAN MENSAJES PARA ACK')
                    self.ack_messages(client_id)
            except:
                pass
            #############################################################
            
            if not self.is_EOF_repeated(worker_id, client_id, client_eof_workers_ids):
                if self.received_all_clients_EOFs(client_id):
                    self.add_unacked_EOF(client_id, method)
                    self.manage_EOF(body, method, client_id)
                    self.delete_client_EOF_counter(client_id)
                    return
                else:
                    if self.client_is_active(client_id):
                        # Add the EOF delivery tag to the list of unacked EOFs
                        self.add_unacked_EOF(client_id, method)
                        return

            self.middleware.ack_message(method)
            return
        
        msg_id, client_id, data = deserialize_titles_message(body)

        self.handle_message(method, client_id, msg_id, data)
        # if not self.is_message_repeated(client_id, msg_id):
        #     self.manage_message(client_id, data, method, msg_id)
        
        # self.middleware.ack_message(method)
            
    def run(self):
        callback_with_params = lambda ch, method, properties, body: self.handle_data(method, body)
        try:
            # Read the data
            self.middleware.receive_messages(self.input_name, callback_with_params)
            self.middleware.consume()
        except Exception as e:
            if self.stop_worker:
                print("Gracefully exited")
            else:
                raise e
            

class StateWorker(Worker):
    """
    This type of workers acummulate various messages for each client, creating only one big message
    """
    def ack_EOFs(self, client_id): # TODO: Borrar estA FUNCION, YA ESTA EN Worker
        # try:
        #     if self.last_msg:
        #         self.middleware.ack_all(self.last_msg)
        #         self.last_msg = None
        #     else:
        #         raise Exception('si')
        #     return
        # except:

        for delivery_tag in self.clients_unacked_eofs[client_id]:
            self.middleware.send_message('DEBUG', f'Hago ack del tag: {delivery_tag}')
            self.middleware.ack_message(delivery_tag)

        del self.clients_unacked_eofs[client_id]

    def handle_message(self, method, client_id, msg_id, data):
        if not self.is_message_repeated(client_id, msg_id):
            self.manage_message(client_id, data, method, msg_id)
        else:
            self.middleware.ack_message(method)

    def is_message_repeated(self, client_id, msg_id):
        if client_id in self.clients_acummulated_msgs:
            # temp = msg_id in self.clients_acummulated_msgs[client_id]
            # self.middleware.send_message("DEBUG", f'Entro al if del is repeated y dio: {temp}')
            return msg_id in self.clients_acummulated_msgs[client_id]
        return False

    def remove_active_client(self, client_id):
        raise Exception('Function needs to be implemented')

    def manage_EOF(self, body, method, client_id):
        self.send_results(client_id)
        self.ack_EOFs(client_id)
        self.remove_active_client(client_id)

    def send_results(self, client_id):
        raise Exception('Function needs to be implemented')
    
    def client_is_active(self, client_id):
        return client_id in self.clients_acum
    
    def remove_active_client(self, client_id):
        if client_id not in self.clients_acum:
            # This is a special case. Workers may not receive any message
            # of a client, only its EOF. 
            return
        del self.clients_acum[client_id]

        # self.log.persist(self.clients_acum) # TODO: Persist also the msg_ids of the messages acumulated

    def acummulate_message(self, client_id, data):
        raise Exception('Function needs to be implemented')

    def need_to_persist(self):
        return self.msg_counter == 200 # TODO: Make this a parameter for the worker!
    
    def persist_acum(self):
        # TODO: Persist also the ids of the messages accumulated
        # self.log.persist(self.acum)
        self.msg_counter = 0

    def ack_messages(self, client_id):
        # self.middleware.ack_all(self.last_msg)
        for msg_delivery_tag in self.clients_unacked_msgs[client_id]:
            self.middleware.ack_message(msg_delivery_tag)
        self.middleware.send_message('DEBUG', f'Ya se persistio y tenemos un counter en {self.msg_counter}')
        self.clients_unacked_msgs[client_id] = set()

    def manage_message(self, client_id, data, method, msg_id):
        self.acummulate_message(client_id, data)

        self.add_acummulated_msg_id(client_id, method, msg_id)
        self.last_msg = method.delivery_tag
        if self.need_to_persist():
            self.middleware.send_message('DEBUG', f'Hay que persistir con un counter en {self.msg_counter}')
            self.persist_acum()
            self.ack_messages(client_id)

    def add_acummulated_msg_id(self, client_id, msg_method, msg_id):
        """
        Stores the msg_id of the recently acummulated message and its delivery_tag.
        Storing the msg_id helps to detect possible repeated messages.
        Storing the delivery_tag lets us ack the message onece we know is persited on disk. 
        """
        if client_id not in self.clients_acummulated_msgs:
            self.clients_acummulated_msgs[client_id] = set()
            
        if client_id not in self.clients_unacked_msgs:
            self.clients_unacked_msgs[client_id] = set()

        self.clients_acummulated_msgs[client_id].add(msg_id)
        self.clients_unacked_msgs[client_id].add(msg_method.delivery_tag)

        self.msg_counter += 1

class NoStateWorker(Worker):
    """
    This type of workers filter each message and create one message per message receive.
    """

    def handle_message(self, method, client_id, msg_id, data):
        if not self.is_message_repeated(client_id, msg_id):
            self.manage_message(client_id, data, method, msg_id)
        
        self.middleware.ack_message(method)

    def is_message_repeated(self, client_id, msg_id):
        """
        If the message received has the same id that the last message received,
        it is a repeated message.
        """
        last_client_message = self.last_clients_msg.get(client_id, None)
        
        self.last_clients_msg[client_id] = msg_id

        return last_client_message == msg_id

    def remove_active_client(self, client_id):
        self.active_clients.remove(client_id)

        self.log.persist(self.active_clients)

    def client_is_active(self, client_id):
        return client_id in self.active_clients

    def add_new_active_client(self, client_id):
        self.active_clients.add(client_id)
        
        self.log.persist(self.active_clients)

    def manage_EOF(self, body, method, client_id):
        self.send_EOFs(client_id, self.output_name, self.next_workers_quantity)
        self.ack_EOFs(client_id)
        self.remove_active_client(client_id)

    def _send_batches(self, workers_batches, output_queue, client_id, msg_id):
        msg_id = int(msg_id)
        for worker_id, batch in workers_batches.items():
            serialized_batch = serialize_batch(batch)
            batch_msg_id = msg_id + worker_id
            serialized_message = serialize_message(serialized_batch, client_id, str(batch_msg_id))
            worker_queue = create_queue_name(output_queue, str(worker_id))
            self.middleware.send_message(worker_queue, serialized_message)

    def _create_batches(self, batch, next_workers_quantity):
        workers_batches = {}
        for row in batch:
            hashed_title = hash_title(row['Title'])
            choosen_worker = hashed_title % next_workers_quantity
            if choosen_worker not in workers_batches:
                workers_batches[choosen_worker] = []
            workers_batches[choosen_worker].append(row)
        
        return workers_batches
    
    def manage_message(self, client_id, data, method, msg_id=NO_ID):
        if not self.client_is_active(client_id):
            self.add_new_active_client(client_id)
        
        desired_data = self.apply_filter(data)
        if not desired_data:
            return
        
        self.create_and_send_batches(desired_data, client_id, self.output_name, self.next_workers_quantity, msg_id)

    def apply_filter(self, data):
        raise Exception('Function needs to be implemented')
