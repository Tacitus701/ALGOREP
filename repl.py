from mpi4py import MPI
import time

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
