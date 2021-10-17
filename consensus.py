from mpi4py import MPI
import sys

nb_clients = int(sys.argv[1])
nb_servers = int(sys.argv[2])
term = 0

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
    print("server nb " + str(rank) + " ")
    print(data)
    return data

def consensus():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    print(str(rank) + " time for consensus")
    current_term = term
    return rank

def servers():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    data = init_servers()
    leader = consensus(data)
    print("leader is " + leader + ", responding to " + data[0])
    elt = data[0]
    data.remove(elt)

def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    
    if (rank < nb_servers):
        servers()
    if (rank >= nb_servers and rank < (nb_clients + nb_servers)):
        clients()

if __name__ == "__main__":
    main()
