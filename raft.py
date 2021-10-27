from mpi4py import MPI
import sys
import random
import time
import os

nb_clients = int(sys.argv[1])
nb_servers = int(sys.argv[2])
debug_output = True if sys.argv[3] == "y" else False
comm = MPI.COMM_WORLD
majority = nb_servers // 2 + 1

VOTE_REQ, VOTE_POS, VOTE_NEG, HEARTBEAT, CLIENT_COMMAND = 0, 1, 2, 3, 4

request = {0: "VOTE_REQ", 1: "VOTE_POS", 2: "VOTE_NEG", 3: "HEARTBEAT", 4: "CLIENT_COMMAND"}

START, CRASH, SPEED, RECOVERY = 0,1,2,3

speed_value = {"LOW" : 1, "MEDIUM": 2, "HIGH": 3}

def debug_out(msg):
    if debug_output:
        print(msg)


class Server:

    """docstring for Server."""

    def __init__(self, rank):
        super(Server, self).__init__()
        self.rank = rank
        self.role = "FOLLOWER"
        self.term = 0
        self.timeout = 0
        self.leader_heartbeat = 0
        self.request_vote = 0
        self.log = []
        self.replicated = []
        self.vote = [-1] * (nb_servers + 1)
        self.waiting_clients = []
        self.terminate = False
        self.start = False
        self.crash = False
        self.speed = 0

    def save_term(self):
        filename = "disk/" + str(self.rank) + ".term"
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "w") as file:
            file.write(str(self.term) + '\n')

    def update_term(self, new_term):
        """ change value of the term and saves it automatically"""
        self.term = new_term
        self.save_term()

    def notify_client(self):
        print("notif")
        debug_out("notifying client " + str(self.waiting_clients[0]))
        comm.isend(self.waiting_clients[0][0], dest=self.waiting_clients[0][1])
        msg, src = self.waiting_clients.pop(0)
        filename = "disk/" + str(self.rank) + ".command"
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "w") as file:
            file.write("Message : " + msg + " Client : " + src)


    def handle_log(self, log):
        self.log = log.copy()

    def process_vote_request(self, src, msg):
        #print("server number " + str(self.rank) + " received vote request term : " + str(self.term))
        term, log = msg
        if term > self.term and len(log) >= len(self.log):
            self.update_term(term)
            self.vote = [-1] * (nb_servers + 1)
            self.vote[self.rank] = src
            self.role = "FOLLOWER"
            self.timeout += random.randint(2, 5)
            req = comm.isend(self.term, dest=src, tag=VOTE_POS)
            req.wait()
        else:
            req = comm.isend(self.term, dest=src, tag=VOTE_NEG)
            req.wait()

    def process_positive_vote(self, src):
        if self.role != "CANDIDATE":
            return
        self.vote[src] = self.rank
        nb_vote = len([vote for vote in self.vote if vote == self.rank])
        if nb_vote >= majority:
            if self.role != "LEADER":
                print("server number " + str(self.rank) + " is now leader")
            self.role = "LEADER"
            self.leader_heartbeat = time.time()

    def process_heartbeat(self, src, msg):
        debug_out("server number " + str(self.rank) + " receiving HeartBeat")
        self.role = "FOLLOWER"
        self.timeout += random.randint(2, 5)
        term, log = msg
        self.update_term(term)
        self.handle_log(log)
        req = comm.isend(self.log, dest=src, tag=HEARTBEAT)
        req.wait()

    def process_heartbeat_response(self, src, msg):
        for i in range(len(msg)):
            self.replicated[i] += 1
        return

    def process_client_command(self, src, msg):
        if self.role == "LEADER":
            print("Receiving " + msg + " from " + str(src))
            self.log.append((msg, src))
            self.replicated.append(1)
            self.waiting_clients.append((msg, src))

    def handle_message(self):
        status = MPI.Status()
        is_message = comm.Iprobe(status=status)

        if not is_message:
            return

        print("Message")

        src = status.source  # status.Get_source()
        tag = status.tag
        msg = comm.irecv().wait()
        debug_out("server number " + str(self.rank)
              + " source : " + str(src)
              + " tag : " + request[tag]
              + " message : " + str(msg) + " " + str(time.time()))

        if tag == VOTE_REQ:
            self.process_vote_request(src, msg)

        elif tag == VOTE_POS:
            self.process_positive_vote(src)

        elif tag == VOTE_NEG:
            self.vote[src] = -2

        elif tag == HEARTBEAT:
            if self.role != "LEADER":
                self.process_heartbeat(src, msg)
            else:  # on utilise le tag heartbeat pour repondre au heartbeat
                self.process_heartbeat_response(src, msg)

        elif tag == CLIENT_COMMAND:
            print("server number " + str(self.rank) + " received message")
            self.process_client_command(src, msg)

    def handle_send(self):
        tmp = time.time()
        if self.role == "CANDIDATE":
            if tmp > self.request_vote + 1:
                self.request_vote = time.time()
                for i in range(1, nb_servers + 1):
                    if self.vote[i] == -1:
                        req = comm.isend((self.term, self.log), dest=i, tag=VOTE_REQ)
                        req.wait()
                debug_out("server number " + str(self.rank) + " is sending VOTE_REQ to everyone " + str(time.time()))

        if self.role == "LEADER":
            if len(self.waiting_clients) > 0:
                # on notifie le client si le log a ete replique chez une majorite
                if self.replicated[-len(self.waiting_clients)] >= majority:
                    self.notify_client()
            if tmp > self.leader_heartbeat + 1:
                debug_out("Server number " + str(self.rank) + " Sending HeartBeat time : " + str(time.time()))
                self.replicated = [1] * len(self.log)
                self.leader_heartbeat = time.time()
                for i in range(1, nb_servers + 1):
                    if i != self.rank:
                        req = comm.isend((self.term, self.log), dest=i, tag=HEARTBEAT)
                        req.wait()

    def handle_repl(self):
        status = MPI.Status()
        is_message = comm.Iprobe(status=status)

        if not is_message:
            return

        tag = status.tag
        msg = comm.irecv().wait()
       
        if tag == CRASH:
            self.crash = True
        elif tag == SPEED:
            self.speed = msg
        elif tag == RECOVERY:
            self.crash = False

        return

    def consensus(self):
        # follower
        # temps aléatoire -> candidat
        """
        vote pour lui
        votez pour moi
        infini et si leader il reset son timeout jusqu'à avoir plus de la moitié
        """
        self.update_term(self.term + 1)

        current_time = time.time()
        self.timeout = current_time + random.uniform(3.0, 8.0)

        while current_time <= self.timeout:
            if self.role != "LEADER":
                current_time = time.time()
            if not self.crash:
                self.handle_message()
                self.handle_send()
            self.handle_repl()

        # Too long
        debug_out("server number " + str(self.rank) + " has timed out")
        debug_out("server number " + str(self.rank) + " is now candidate")
        self.role = "CANDIDATE"
        self.vote = [-1] * (nb_servers + 1)

        # Vote for himself
        self.vote[self.rank] = self.rank

    def run(self):
        print(str(self.rank) + " is a server")
        while not self.terminate:
            self.consensus()
        debug_out("server number " + str(self.rank) + " log : " + str(self.log))


class Client:
    """docstring for Client."""

    def __init__(self, rank):
        super(Client, self).__init__()
        self.rank = rank
        self.nb_command = 10
        self.commands = []

    def wait_start(self):
        req = comm.irecv(source=0)
        req.wait()

    def read_commands(self):
        filename = "client/" + str(self.rank) + ".command"
        file = open(filename)
        self.commands = file.read().splitlines()
        file.close()

    def run(self):
        self.wait_start()
        self.read_commands()
        debug_out(str(self.rank) + " : I'm a client")
        nb_req = 3
        while nb_req > 0:
            nb_req -= 1
            time.sleep(random.uniform(5, 8))
            command = random.randint(0, self.nb_command - 1)
            print("Sending message " + str(self.commands[command]))
            #for i in range(1, nb_servers + 1):
            req = comm.isend(self.commands[command], dest=1, tag=CLIENT_COMMAND)

def REPL():
    while True:
        try:
            command = input("REPL : ")
        except EOFError as e:
            break
        command = command.split()
        print(command)
        if command[0] == "START":
            for i in range(nb_servers + 1, nb_servers + nb_clients + 1):
                req = comm.isend("START", dest=i, tag=START)
                req.wait()
        elif command[0] == "CRASH":
            comm.isend("CRASH", dest=int(command[1]), tag=CRASH)
        elif command[0] == "SPEED":
            comm.isend(speed_value[command[2]], dest=int(command[1]), tag=SPEED)
        elif command[0] == "RECOVERY":
            comm.isend("RECOVERY", dest=int(command[1]), tag=RECOVERY)
        else:
            print("Invalid command")
        time.sleep(0.1)

def main():
    rank = comm.Get_rank()

    if rank == 0:
        REPL()

    elif rank <= nb_servers:
        my_server = Server(rank)
        my_server.run()

    elif nb_servers < rank < (nb_clients + nb_servers + 1):
        my_client = Client(rank)
        my_client.run()



if __name__ == "__main__":
    main()
