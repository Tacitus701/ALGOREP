import os
import random


def create_command_list(start, nb_clients):
    nb_command = 10
    for i in range(start, start + nb_clients):
        filename = "client/" + str(i) + ".command"
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "w") as file:
            for j in range(nb_command):
                command = random.randint(i * 100, (i + 1) * 100)
                file.write(str(command) + '\n')

def main():
    nb_clients = input("Number of clients : ")
    nb_servers = input("Number of servers : ")
    answer = False
    debug_output = input("Would you like debug prints (y/n)?")
    print("There is " + nb_clients + " clients and " + nb_servers + " servers")
    nb_process = int(nb_servers) + int(nb_clients) + 1
    s = "mpiexec -n " + str(nb_process) + " --mca opal_warn_on_missing_libcuda 0 --oversubscribe python consensus.py "\
        + nb_clients + " " + nb_servers + " " + debug_output
    print(s)
    create_command_list(int(nb_servers) + 1, int(nb_clients))
    os.system(s)

if __name__ == "__main__":
    main()
