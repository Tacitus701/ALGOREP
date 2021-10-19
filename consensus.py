from mpi4py import MPI
from enum import Enum
import sys
import random
import time

nb_clients = int(sys.argv[1])
nb_servers = int(sys.argv[2])
comm = MPI.COMM_WORLD


class Server():

    """docstring for Server."""

    def __init__(self, rank):
        super(Server, self).__init__()
        self.rank = rank
        self.role = "FOLLOWER"
        self.term = 0
        self.data = [None] * nb_clients
        for i in range(nb_clients):
            req = comm.irecv(source=nb_servers + i)
            self.data[i] = req.wait()

    def argmax(iterable):
        return max(enumerate(iterable), key=lambda x: x[1])[0]

    def handleMessage(self):

        status = MPI.Status()
        is_message = comm.Iprobe(status=status)
        if (is_message):
            src = status.source #status.Get_source()
            msg = comm.irecv()

    def consensus(self):
        #follower
        #temps aléatoire -> candidat
        """
        vote pour lui
        votez pour moi
        infini et si leader il reset son timeout jusqu'à avoir plus de la moitié
        """
        self.term += 1

        vote = [0] * nb_servers

        if self.role == "CANDIDATE":
            for i in range(nb_servers):
                if i == self.rank:
                    continue
                req = comm.isend(self.term, dest=i)
                req.wait()


        current_time = time.time()
        timeout = current_time + time.struct_time(tm_sec=5)

        while (current_time <= timeout):
            if self.role != "LEADER":
                current_time = time.time()
            self.handleMessage()

        # Too long
        self.role = "CANDIDATE"

        # Vote for himself
        self.vote[self.rank] += 1

        return self.consensus()

    def run(self):
        data = init_servers()
        term = 0
        while len(data) > 0:
            term, leader = consensus(term)
            print("server number " + str(self.rank) + ", leader is " + str(leader) + ", responding to " + str(data[0]))
            elt = data[0]
            data.remove(elt)
            print(data)
            time.sleep(1)

class Client():
    """docstring for Client."""

    def __init__(self, rank):
        super(Client, self).__init__()
        self.rank = rank

    def run(self):
        print(str(self.rank) + " : I'm a client")
        for i in range(nb_servers):
            req = comm.isend(self.rank, dest=i)
            req.wait()

def main():
    rank = comm.Get_rank()

    if (rank < nb_servers):
        myServer = Server(rank)

    if (rank >= nb_servers and rank < (nb_clients + nb_servers)):
        myClient = Client(rank)
        myClient.run()




if __name__ == "__main__":
    main()
