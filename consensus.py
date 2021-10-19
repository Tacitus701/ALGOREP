from mpi4py import MPI
from enum import Enum
import sys
import random
import time

nb_clients = int(sys.argv[1])
nb_servers = int(sys.argv[2])
comm = MPI.COMM_WORLD
majority = nb_servers // 2 + 1

VOTE_REQ, VOTE_POS, VOTE_NEG, HEARTBEAT = 0,1,2,3

request = {0: "VOTE_REQ", 1: "VOTE_POS", 2: "VOTE_NEG", 3: "HEARTBEAT"}

class Server():

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
        self.vote = [-1] * nb_servers
        self.data = [None] * nb_clients
        for i in range(nb_clients):
            req = comm.irecv(source=nb_servers + i)
            self.data[i] = req.wait()


    def respond(self):
        comm.isend(self.data[-1], dest=self.data[-1])
        return self.data.pop()


    def handleLog(self, log):
        for i in range(len(self.log), len(log)):
            self.log.append(log[i])
            self.data.remove(log[i])


    def handleRecv(self):

        status = MPI.Status()
        is_message = comm.Iprobe(status=status)

        if (not is_message):
            return

        src = status.source #status.Get_source()
        tag = status.tag
        term = comm.irecv().wait()
        print("server number " + str(self.rank) + " source : " + str(src) + " tag : " + request[tag] + " term : " + str(term))

        if (tag == VOTE_REQ):
            if term > self.term:
                self.term = term
                self.vote = [-1] * nb_servers
                self.vote[self.rank] = src
                self.role = "FOLLOWER"
                self.timeout += 2
                comm.isend(self.term, dest=src, tag=VOTE_POS)
            else:
                comm.isend(self.term, dest=src, tag=VOTE_NEG)

        if (tag == VOTE_POS):
            self.vote[src] = self.rank
            nb_vote = len([vote for vote in self.vote if vote == self.rank])
            if nb_vote >= majority:
                if (self.role != "LEADER"):
                    print("server number " + str(self.rank) + " is now leader")
                self.role = "LEADER"
                self.leader_heartbeat = time.time()

        if (tag == VOTE_NEG):
            self.vote[src] = -2

        if (tag == HEARTBEAT):
            self.timeout += 2
            term, log = term
            self.term = term
            self.handleLog(log)


    def handleSend(self):

        tmp = time.time()
        if self.role == "CANDIDATE":
            if (tmp > self.request_vote + 1):
                print("server number " + str(self.rank) + " is sending VOTE_REQ to everyone")
                self.request_vote = time.time()
                for i in range(nb_servers):
                    if self.vote[i] == -1:
                        req = comm.isend(self.term, dest=i, tag=VOTE_REQ)

        if self.role == "LEADER":
            if len(self.data) > 0:
                value = self.respond()
                self.log.append(value)
            if (tmp > self.leader_heartbeat + 1):
                self.leader_heartbeat = time.time()
                for i in range(nb_servers):
                    if i != self.rank:
                        comm.isend((self.term, self.log), dest=i, tag=HEARTBEAT)


    def consensus(self):
        #follower
        #temps aléatoire -> candidat
        """
        vote pour lui
        votez pour moi
        infini et si leader il reset son timeout jusqu'à avoir plus de la moitié
        """
        terminate = False
        self.term += 1

        current_time = time.time()
        self.timeout = current_time + random.randint(3,5)

        while (current_time <= self.timeout) and not terminate:
            if self.role != "LEADER":
                current_time = time.time()
            self.handleRecv()
            self.handleSend()
            if len(self.data) == 0:
                time.sleep(1)
                self.handleSend()
                terminate = True

        # Too long
        if not terminate:
            print("server number " + str(self.rank) + " is now candidate")
        self.role = "CANDIDATE"

        # Vote for himself
        self.vote[self.rank] = self.rank

        return terminate

    def run(self):
        terminate = False
        while not terminate:
            terminate = self.consensus()
        print("server number " + str(self.rank) + " log : " + str(self.log))


class Client():
    """docstring for Client."""

    def __init__(self, rank):
        super(Client, self).__init__()
        self.rank = rank

    def run(self):
        print(str(self.rank) + " : I'm a client")
        for i in range(nb_servers):
            req = comm.isend(self.rank, dest=i)

def main():
    rank = comm.Get_rank()

    if (rank < nb_servers):
        myServer = Server(rank)
        myServer.run()

    if (rank >= nb_servers and rank < (nb_clients + nb_servers)):
        myClient = Client(rank)
        myClient.run()




if __name__ == "__main__":
    main()
