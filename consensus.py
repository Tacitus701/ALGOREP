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
        self.vote = [-1] * nb_servers
        self.waiting_clients = []
        self.terminate = False

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
        debug_out("notifying client " + str(self.waiting_clients[0]))
        comm.isend(self.waiting_clients[0], dest=self.waiting_clients[0])
        self.waiting_clients.pop(0)

    def handle_log(self, log):
        for i in range(len(self.log), len(log)):
            self.log.append(log[i])

    def process_vote_request(self, src, term):
        if term > self.term:
            self.update_term(term)
            self.vote = [-1] * nb_servers
            self.vote[self.rank] = src
            self.role = "FOLLOWER"
            self.timeout += 2
            comm.isend(self.term, dest=src, tag=VOTE_POS)
        else:
            comm.isend(self.term, dest=src, tag=VOTE_NEG)

    def process_positive_vote(self, src):
        if self.role != "CANDIDATE":
            return
        self.vote[src] = self.rank
        nb_vote = len([vote for vote in self.vote if vote == self.rank])
        if nb_vote >= majority:
            if self.role != "LEADER":
                debug_out("server number " + str(self.rank) + " is now leader")
            self.role = "LEADER"
            self.leader_heartbeat = time.time()

    def process_heartbeat(self, src, msg):
        self.role = "FOLLOWER"
        self.timeout += 2
        term, log = msg
        self.update_term(term)
        self.handle_log(log)
        comm.isend(self.log, dest=src, tag=HEARTBEAT)

    def process_heartbeat_response(self, src, msg):
        for i in range(len(msg)):
            self.replicated[i] += 1
        return

    def process_client_command(self, src, msg):
        if self.role == "LEADER":
            self.log.append(msg)
            self.replicated.append(0)
            self.waiting_clients.append(src)

    def handle_message(self):
        status = MPI.Status()
        is_message = comm.Iprobe(status=status)

        if not is_message:
            return

        src = status.source  # status.Get_source()
        tag = status.tag
        msg = comm.irecv().wait()
        debug_out("server number " + str(self.rank)
              + " source : " + str(src)
              + " tag : " + request[tag]
              + " term : " + str(msg))

        if tag == VOTE_REQ:
            self.process_vote_request(src, msg)

        if tag == VOTE_POS:
            self.process_positive_vote(src)

        if tag == VOTE_NEG:
            self.vote[src] = -2

        if tag == HEARTBEAT:
            if self.role != "LEADER":
                self.process_heartbeat(src, msg)
            else:  # on utilise le tag heartbeat pour repondre au heartbeat
                self.process_heartbeat_response(src, msg)

        if tag == CLIENT_COMMAND:
            self.process_client_command(src, msg)

    def handle_send(self):
        tmp = time.time()
        if self.role == "CANDIDATE":
            if tmp > self.request_vote + 1:
                debug_out("server number " + str(self.rank) + " is sending VOTE_REQ to everyone")
                self.request_vote = time.time()
                for i in range(nb_servers):
                    if self.vote[i] == -1:
                        req = comm.isend(self.term, dest=i, tag=VOTE_REQ)

        if self.role == "LEADER":
            if len(self.waiting_clients) > 0:
                # on notifie le client si le log a ete replique chez une majorite
                if self.replicated[-len(self.waiting_clients)] >= majority:
                    self.notify_client()
            if tmp > self.leader_heartbeat + 1:
                self.replicated = [1] * len(self.log)
                self.leader_heartbeat = time.time()
                for i in range(nb_servers):
                    if i != self.rank:
                        comm.isend((self.term, self.log), dest=i, tag=HEARTBEAT)

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
        self.timeout = current_time + random.uniform(3.0, 5.0)

        while current_time <= self.timeout:
            if self.role != "LEADER":
                current_time = time.time()
            self.handle_message()
            self.handle_send()

        # Too long
        debug_out("server number " + str(self.rank) + " is now candidate")
        self.role = "CANDIDATE"

        # Vote for himself
        self.vote[self.rank] = self.rank

    def run(self):
        while not self.terminate:
            self.consensus()
        debug_out("server number " + str(self.rank) + " log : " + str(self.log))


class Client:
    """docstring for Client."""

    def __init__(self, rank):
        super(Client, self).__init__()
        self.rank = rank

    def run(self):
        debug_out(str(self.rank) + " : I'm a client")
        nb_req = 1
        while nb_req > 0:
            nb_req -= 1
            time.sleep(random.uniform(5, 8))
            for i in range(nb_servers):
                req = comm.isend(self.rank, dest=i, tag=CLIENT_COMMAND)


def main():
    rank = comm.Get_rank()

    if rank < nb_servers:
        my_server = Server(rank)
        my_server.run()

    if nb_servers <= rank < (nb_clients + nb_servers):
        my_client = Client(rank)
        my_client.run()


if __name__ == "__main__":
    main()
