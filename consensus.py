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
    t = random.randint(0,1)
    time.sleep(t)
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
            vote[i] += 1 + vote[rank]
            vote[rank] = 0
            voted = True
            req = comm.isend(vote[i], dest=i, tag=1)
            req.wait()

    if voted:
        leader = argmax(vote)
        print("server number " + str(rank) + " waiting for leader " + str(leader))
        req = comm.irecv(source=leader, tag=2)
        time.sleep(2)
        if req.get_status():
            term = req.wait()
            print("term " + str(term))
            return term, leader
        return consensus(term)


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
        return consensus(term)

    for i in range(nb_servers):
        req = comm.isend(term, dest=i, tag=2)
        req.wait()

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

def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    
    if (rank < nb_servers):
        servers()
    if (rank >= nb_servers and rank < (nb_clients + nb_servers)):
        clients()

if __name__ == "__main__":
    main()
