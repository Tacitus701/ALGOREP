from mpi4py import MPI
from enum import Enum
import sys
import random
import time

nb_clients = int(sys.argv[1])
nb_servers = int(sys.argv[2])
comm = MPI.COMM_WORLD


class Server(object):

    """docstring for Server."""

    def __init__(self, rank):
        super(Server, self).__init__()
        self.rank = rank
        self.role = "FOLLOWER"

    def recvFromClients(self):
        data = [None] * nb_clients
        for i in range(nb_clients):
            req = comm.irecv(source=nb_servers + i)
            data[i] = req.wait()
        return data

    def argmax(iterable):
        return max(enumerate(iterable), key=lambda x: x[1])[0]

    def consensus(self, term):
        #follower
        #temps aléatoire -> candidat
        """
        vote pour lui
        votez pour moi
        infini et si leader il reset son timeout jusqu'à avoir plus de la moitié
        """

        vote = [0] * nb_servers
        voted = False

        # Sleep random amount of time before declaring himself candidate
        sleep(randint(1, 5))
        self.role = "CANDIDATE"

        # Vote for himself
        self.vote[self.rank] += 1
        voted = True

        
        for i in range(nb_servers):
            req = comm.isend(term, dest=i, tag=0)
            req.wait()

        for i in range(nb_servers):
            if i == self.rank:
                continue
            req = comm.irecv(source=i)
            print("server " + str(self.rank) + " waiting for response")
            if req.get_status():
                vote[self.rank] += req.wait()
            else:
                req.cancel()


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
"""
    def consensus(self, term):
        term += 1
        print(f"Server {self.rank} start consensus")
        vote = [0] * nb_servers
        voted = False

        for i in range(nb_servers):
            if i == self.rank:
                continue
            req = comm.irecv(source=i)
            if req.get_status():
                print(str(self.rank) + " received vote from " + str(i))
                recv_term = req.wait()
                if recv_term < term:
                    continue
                term = recv_term
                vote[i] += 1 + vote[self.rank]
                vote[self.rank] = 0
                voted = True
                req.cancel()
                req = comm.isend(vote[i], dest=i, tag=1)
                req.wait()

        if voted:
            leader = argmax(vote)
            req = comm.irecv(source=leader, tag=2)
            print("server number " + str(self.rank) + " waiting for leader " + str(leader))
            term = req.wait()
            return term, leader

        vote[self.rank] += 1
        voted = True

        for i in range(nb_servers):
            req = comm.isend(term, dest=i, tag=0)
            req.wait()

        for i in range(nb_servers):
            if i == self.rank:
                continue
            req = comm.irecv(source=i)
            print("server " + str(self.rank) + " waiting for response")
            if req.get_status():
                vote[self.rank] += req.wait()
            else:
                req.cancel()

        print("server number " + str(self.rank))
        print(vote)

        leader = argmax(vote)
        if vote[leader] < nb_servers // 2 + 1:
            print("server number " + str(self.rank) + " does not have enough vote")
            return consensus(term)

        for i in range(nb_servers):
            if i == self.rank:
                continue
            print("sent to " + str(i))
            req = comm.isend(term, dest=i, tag=2)
            req.wait()
        print("server number " + str(self.rank) + " is sleeping")

        return (term, leader)
"""
class Client(object):
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
