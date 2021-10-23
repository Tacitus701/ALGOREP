import os

def main():
    nb_clients = input("Number of clients : ")
    nb_servers = input("Number of servers : ")
    answer = False
    debug_output = input("Would you like debug prints (y/n)?")
    print("There is " + nb_clients + " clients and " + nb_servers + " servers")
    nb_process = int(nb_servers) + int(nb_clients)
    s = "mpiexec -n " + str(nb_process) + " --mca opal_warn_on_missing_libcuda 0 --oversubscribe python consensus.py "\
        + nb_clients + " " + nb_servers + " " + debug_output
    print(s)
    os.system(s)

if __name__ == "__main__":
    main()
