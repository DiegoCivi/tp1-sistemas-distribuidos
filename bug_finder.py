import docker
import time
import subprocess
from threading import Thread

class BugFinder:

    def __init__(self):
        self.q1_res = []
        self.q2_res = 0
        self.q3_res = []
        self.q4_res = []
        self.q5_res = 0

        self.containers = []
        self.curr_thread = []


    def run(self):
        self.get_state()
        for i in range(5):
            self.run_iteration(i)
    
    def run_compose(self):
        subprocess.run(["docker", "compose", "-f", "docker-compose-test.yaml", "up", "--build", "--remove-orphans"], check=True)

    def start_system(self):
        # self.curr_thread = Thread(target=self.run_compose)
        # self.curr_thread.start()
        subprocess.run(["bash", "docker", "compose", "-f", "docker-compose-test.yaml", "up", "--build", "--remove-orphans", "&"], check=True)
        
        while not self.is_container_running('rabbit'):
            time.sleep(2)

    def stop_system(self):
        subprocess.run(['bash', 'stop.sh'], check=True)
        time.sleep(30)

    def run_iteration(self, id):
        
        self.start_system()

        while self.is_container_running('client_1') or self.is_container_running('client_2'):
            for container in self.containers:
                if not container.startswith('client') and not self.is_container_running(container):
                    subprocess.run(['docker', 'restart', container])
            time.sleep(5)

        cl_q1, cl_q2, cl_q3, cl_q4, cl_q5 = self.get_client_results(1)
        ok = self.check_client_results(cl_q1, cl_q2, cl_q3, cl_q4, cl_q5)

        cl_q1, cl_q2, cl_q3, cl_q4, cl_q5 = self.get_client_results(2)
        ok = self.check_client_results(cl_q1, cl_q2, cl_q3, cl_q4, cl_q5)

        self.curr_thread.join()
        print(f"Iteration [{id}] OK")
        self.stop_system()
    
    def is_container_running(self, container_name):
        """Verify the status of a container by it's name

        :param container_name: the name of the container
        :return: boolean or None
        """
        RUNNING = "running"
        # Connect to Docker using the default socket or the configuration
        # in your environment
        docker_client = docker.from_env()

        try:
            container = docker_client.containers.get(container_name)
        except docker.errors.NotFound as exc:
            print(f"Check container name!\n{exc.explanation}")
            return False
        else:
            container_state = container.attrs["State"]
            return container_state["Status"] == RUNNING

    def get_state(self):
        with open(f'./debug/results.txt', 'r') as f:
            lines = f.readlines()
            curr_q = 1
            for line in lines:
                if line == '[QUERY 1]\n':
                    curr_q = 1
                elif line == '[QUERY 2]\n':
                    curr_q = 2
                elif line == '[QUERY 3]\n':
                    curr_q = 3
                elif line == '[QUERY 4]\n':
                    curr_q = 4
                elif line == '[QUERY 5]\n':
                    curr_q = 5
                elif curr_q == 1:
                    self.q1_res.append(line)
                elif curr_q == 2:
                    authors_quantity = len(line.split(','))
                    self.q2_res += authors_quantity
                elif curr_q == 3:
                    self.q3_res.append(line)
                elif curr_q == 4:
                    self.q4_res.append(line)
                elif curr_q == 5:
                    titles_quantity =  len(line.split(','))
                    self.q5_res += titles_quantity

        with open('./containers_list.txt') as f:
            lines = f.readlines()

            for line in lines:
                self.containers.append(line.rstrip('\n'))

    def get_client_results(self, client_id):
        with open(f'./debug/results_{client_id}.txt', 'r') as f:
            lines = f.readlines()
            cl_q1 = []
            cl_q2 = 0
            cl_q3 = []
            cl_q4 = []
            cl_q5 = 0

            curr_q = 1
            for line in lines:
                if line == '[QUERY 1]\n':
                    curr_q = 1
                elif line == '[QUERY 2]\n':
                    curr_q = 2
                elif line == '[QUERY 3]\n':
                    curr_q = 3
                elif line == '[QUERY 4]\n':
                    curr_q = 4
                elif line == '[QUERY 5]\n':
                    curr_q = 5
                elif curr_q == 1:
                    cl_q1.append(line)
                elif curr_q == 2:
                    authors_quantity = len(line.split(','))
                    cl_q2 += authors_quantity
                elif curr_q == 3:
                    cl_q3.append(line)
                elif curr_q == 4:
                    cl_q4.append(line)
                elif curr_q == 5:
                    titles_quantity =  len(line.split(','))
                    cl_q5 += titles_quantity

        return cl_q1, cl_q2, cl_q3, cl_q4, cl_q5
    
    def check_client_results(self, cl_q1, cl_q2, cl_q3, cl_q4, cl_q5):
        # Check results from Q1
        if len(cl_q1) != len(self.q1_res):
            print("ERROR ON Q1")
            raise Exception('Error')

        for line in cl_q1:
            if line not in self.q1_res:
                print("ERROR ON Q1")
                raise Exception('Error')
            
        # Check results from Q2
        if self.q2_res != cl_q2:
            print("ERROR ON Q2")
            raise Exception('Error')
        
        # Check results from Q3
        if len(cl_q3) != len(self.q3_res):
            print("ERROR ON Q3")
            raise Exception('Error')

        for line in cl_q3:
            if line not in self.q3_res:
                print("ERROR ON Q3")
                raise Exception('Error')
            
        # Check results from Q4
        if len(cl_q4) != len(self.q4_res):
            print("ERROR ON Q4")
            raise Exception('Error')

        for line in cl_q4:
            if line not in self.q4_res:
                print("ERROR ON Q4")
                raise Exception('Error')
            
        # Check results from Q5
        if self.q5_res != cl_q5:
            print("ERROR ON Q5")
            raise Exception('Error')
                
bg = BugFinder()
bg.run()