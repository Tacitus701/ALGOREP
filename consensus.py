from mpi4py import MPI
import sys
import random
import time

nb_clients = int(sys.argv[1])
nb_servers = int(sys.argv[2])

def argmax(iterable):
    return max(enumerate(iterable), key=lambda x: x[1])[0]

def clients():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    print(str(rank) + " : I'm a client")
    for i in range(nb_servers):
        req = comm.isend(rank, dest=i)
        req.wait()

def init_servers():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    data = [None] * nb_clients
    for i in range(nb_clients):
        req = comm.irecv(source=nb_servers + i)
        data[i] = req.wait()
    return data

def consensus(term):
    term += 1
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    print(str(rank) + " time for consensus")
    vote = [0] * nb_servers
    voted = False

    for i in range(nb_servers):
        if i == rank:
            continue
        req = comm.irecv(source=i, tag=0)
        if req.get_status():
            print(str(rank) + " received vote from " + str(i))
            recv_term = req.wait()
            if recv_term < term:
                continue
            term = recv_term
            vote[i] += 1 + vote[rank]
            vote[rank] = 0
            voted = True
            req.cancel()
            req = comm.isend(vote[i], dest=i, tag=1)
            req.wait()

    if voted:
        leader = argmax(vote)
        req = comm.irecv(source=leader, tag=2)
        print("server number " + str(rank) + " waiting for leader " + str(leader))
        term = req.wait()
        return term, leader

    vote[rank] += 1
    voted = True

    for i in range(nb_servers):
        req = comm.isend(term, dest=i, tag=0)
        req.wait()

    for i in range(nb_servers):
        if i == rank:
            continue
        req = comm.irecv(source=i)
        print("server " + str(rank) + " waiting for response")
        if req.get_status():
            vote[rank] += req.wait()
        else:
            req.cancel()

    print("server number " + str(rank))
    print(vote)

    leader = argmax(vote)
    if vote[leader] < nb_servers // 2 + 1:
        print("server number " + str(rank) + " does not have enough vote")
        return consensus(term)

    for i in range(nb_servers):
        if i == rank:
            continue
        print("sent to " + str(i))
        req = comm.isend(term, dest=i, tag=2)
        req.wait()
    print("server number " + str(rank) + " is sleeping")

    return (term, leader)

def servers():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    data = init_servers()
    term = 0
    while len(data) > 0:
        term, leader = consensus(term)
        print("server number " + str(rank) + ", leader is " + str(leader) + ", responding to " + str(data[0]))
        elt = data[0]
        data.remove(elt)
        print(data)
        time.sleep(1)

def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    
    if (rank < nb_servers):
        servers()
    if (rank >= nb_servers and rank < (nb_clients + nb_servers)):
        clients()

if __name__ == "__main__":
    main()
