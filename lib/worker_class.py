from serialization import *


class Worker:

    def _create_batches(self, batch, next_workers_quantity):
        raise Exception('Function needs to be implemented')

    def _send_batches(self, workers_batches, output_queue, client_id):
        raise Exception('Function needs to be implemented')

    def create_and_send_batches(self, batch, client_id, output_queue, next_workers_quantity):
        workers_batches = self._create_batches(batch, next_workers_quantity)

        self._send_batches(workers_batches, output_queue, client_id)

    def send_EOFs(self, client_id, output_queue, next_workers_quantity):
        eof_msg = create_EOF(client_id, self.worker_id)
        for worker_id in range(next_workers_quantity):
            worker_queue = create_queue_name(output_queue, str(worker_id))
            self.middleware.send_message(worker_queue, eof_msg)

    def remove_active_client(self, client_id):
        self.active_clients.remove(client_id)

        # TODO: Write on disk the new active clients!!!!!!!!
        self.log.persist(self.active_clients)
        
    
    def manage_message(self, client_id, data, method):
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
    
    def send_results(self, client_id):
        raise Exception('Function needs to be implemented')
    
    def manage_EOF(self, body, method, client_id):
        if self.acum:
            self.send_results(client_id)
        else:
            self.send_EOFs(client_id, self.output_name, self.next_workers_quantity)
        self.ack_EOFs(client_id)
        self.remove_active_client(client_id)

    def ack_EOFs(self, client_id):
        for delivery_tag in self.clients_unacked_eofs[client_id]:
            self.middleware.ack_message(delivery_tag)

        del self.clients_unacked_eofs[client_id]
    
    def add_unacked_EOF(self, client_id, eof_method):
        unacked_eofs = self.clients_unacked_eofs.get(client_id, set())         
        unacked_eofs.add(eof_method.delivery_tag)

        self.clients_unacked_eofs[client_id] = unacked_eofs

    def handle_data(self, method, body):            
        if is_EOF(body):
            worker_id = get_EOF_worker_id(body)                                 # The id of the worker that sent the EOF
            client_id = get_EOF_client_id(body)                                 # The id of the active client
            client_eof_workers_ids = self.eof_workers_ids.get(client_id, set()) # A set with the ids of the workers that already sent their EOF for this client
            
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

        self.manage_message(client_id, data, method)

        self.middleware.ack_message(method)
            
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