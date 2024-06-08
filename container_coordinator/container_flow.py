import os
from random import choice
from time import time

class ContainerFlow:
    def __init__(self, container_list = []):
        self.container_list = container_list
        self.time_interval = 10 #TODO: Change this to be random after testing
        self.start_interval_time = time()
        self.simulate_random_failure()

    def simulate_random_failure(self):
        print("Simulating random failure")
        if len(self.container_list) > 0 and time() - self.start_interval_time > self.time_interval:
            container_id = choice(self.container_list)
            self.stop_container(container_id)
            self.start_interval_time = time()

    def add_container(self, container_id):
        self.container_list.append(container_id)

    def remove_container(self, container_id):
        self.container_list.remove(container_id)

    def restart_container(self, container_id):
        os.system(f"docker restart {container_id}")

    def stop_container(self, container_id):
        os.system(f"docker stop {container_id}")
    
    def start_container(self, container_id):
        os.system(f"docker start {container_id}")
