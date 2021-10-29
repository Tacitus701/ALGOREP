from mpi4py import MPI
import sys
import random
import time
import os

#Number of clients
nb_clients = int(sys.argv[1])

#Number of servers
nb_servers = int(sys.argv[2])

#Debug option
debug_output = True if sys.argv[3] == "y" else False

#Communicator used to get rank and to send and receive messages
comm = MPI.COMM_WORLD

#Number of vote required to get majority
majority = nb_servers // 2 + 1

#Every tags used in message
VOTE_REQ, VOTE_POS, VOTE_NEG, HEARTBEAT, CLIENT_COMMAND, START, CRASH, SPEED, RECOVERY = 0, 1, 2, 3, 4, 5, 6, 7, 8

#Dictionary used for debug print
request = {0: "VOTE_REQ", 1: "VOTE_POS", 2: "VOTE_NEG", 3: "HEARTBEAT", 4: "CLIENT_COMMAND",
           5: "START", 6: "CRASH", 7: "SPEED", 8: "RECOVERY"}

#Values for SPEED command
LOW, MEDIUM, HIGH = 3, 2, 1


def debug_out(msg):
    '''
    Print debug message only if debug option is activated
    '''

    if debug_output:
        print(msg)


class Server:

    """
    RAFT implementation
    """

    def __init__(self, rank):

        self.rank = rank                    # Id of the thread
        self.role = "FOLLOWER"              # Role in RAFT algorithm
        self.term = 0                       # Actual term
        self.timeout = 0                    # Time limit for timeout
        self.leader_heartbeat = 0           # Time of previous heartbeat
        self.request_vote = 0               # Time of previous vote request send to followers
        self.log = []                       # List of responded messages
        self.replicated = []                # List of number of servers that have replicated the log
        self.vote = [-1] * (nb_servers + 1) # List of vote (vote[i] number of vote for server i)
        self.waiting_clients = []           # List of messages waiting for other servers to replicate the message
        self.crash = False                  # Bool to indicate if the server has crashed or not
        self.speed = MEDIUM                 # Frequency of the heartbeat

        os.makedirs(os.path.dirname("disk/"), exist_ok=True) # Create disk directory

    def save_term(self):
        '''
        Write the term in a file every time it is updated
        '''

        filename = "disk/" + str(self.rank) + ".term"
        with open(filename, "w+") as file:
            file.write(str(self.term) + '\n')

    def update_term(self, new_term):
        '''
        change value of the term and saves it automatically

            Parameters:
                    new_term (int): the new term to save in the term file
        '''

        self.term = new_term
        self.save_term()

    def notify_client(self):
        '''
        If the majority of servers has replicated the message, the leader respond to the client
        '''

        debug_out("notifying client " + str(self.waiting_clients[0]))

        #Send response to client
        comm.isend(self.waiting_clients[0][0], dest=self.waiting_clients[0][1])

        #Remove the message from the list waiting clients
        msg, src = self.waiting_clients.pop(0)

        #Write the message in the log file
        filename = "disk/" + str(self.rank) + ".command"
        with open(filename, "a+") as file:
            file.write("Message : " + msg + "\n")


    def handle_log(self, log):
        '''
        Replicate leader's log into current server log and write it into log file

            Parameters:
                    log (int[]): The list of all logs
        '''

        #Copy leader's log into the server's log
        self.log = log.copy()

        #Write log into the log file
        filename = "disk/" + str(self.rank) + ".command"
        with open(filename, "w+") as file:
            for elt in self.log:
                file.write("Message : " + elt + "\n")

    def process_vote_request(self, src, msg):
        '''
        Returns the sum of two decimal numbers in binary digits.

            Parameters:
                    a (int): A decimal integer
                    b (int): Another decimal integer
        '''

        term, log = msg
        if term > self.term and len(log) >= len(self.log):
            self.update_term(term)
            self.vote = [-1] * (nb_servers + 1)
            self.vote[self.rank] = src
            self.role = "FOLLOWER"
            self.timeout += random.randint(3, 5)
            req = comm.isend(self.term, dest=src, tag=VOTE_POS)
            req.wait()
        else:
            req = comm.isend(self.term, dest=src, tag=VOTE_NEG)
            req.wait()

    def process_positive_vote(self, src):
        if self.role != "CANDIDATE":
            return
        self.vote[src] = self.rank
        nb_vote = len([vote for vote in self.vote if vote == self.rank])
        if nb_vote >= majority:
            if self.role != "LEADER":
                print("server number " + str(self.rank) + " is now leader")
            self.role = "LEADER"
            self.leader_heartbeat = time.time()

    def process_heartbeat(self, src, msg):
        debug_out("server number " + str(self.rank) + " receiving HeartBeat")
        self.role = "FOLLOWER"
        self.timeout += random.randint(3, 5)
        term, log = msg
        self.update_term(term)
        self.handle_log(log)
        req = comm.isend(self.log, dest=src, tag=HEARTBEAT)
        req.wait()

    def process_heartbeat_response(self, src, msg):
        for i in range(len(msg)):
            self.replicated[i] += 1
        return

    def process_client_command(self, src, msg):
        if self.role == "LEADER":
            self.log.append(msg)
            self.replicated.append(1)
            self.waiting_clients.append((msg, src))
            
    def load_data(self):
        term_filename = "disk/" + str(self.rank) + ".term"
        with open(term_filename, "r+") as file:
            self.term = int(file.readline())

        log_filename = "disk/" + str(self.rank) + ".command"
        self.log = []
        with open(log_filename, "r+") as file:
            for line in file:
                log = line.split()[2]
                self.log.append(log)

    def handle_message(self):
        status = MPI.Status()
        is_message = comm.Iprobe(status=status)

        if not is_message:
            return

        src = status.source  # status.Get_source()
        tag = status.tag
        msg = comm.irecv().wait()

        if tag == CRASH:
            self.crash = True
            self.role = "FOLLOWER"
        elif tag == SPEED:
            if msg == "LOW":
                self.speed = LOW
            elif msg == "MEDIUM":
                self.speed = MEDIUM
            elif msg == "HIGH":
                self.speed = HIGH
        elif tag == RECOVERY:
            self.crash = False
            self.load_data()

        if self.crash:
            return

        debug_out("server number " + str(self.rank)
              + " source : " + str(src)
              + " tag : " + request[tag]
              + " message : " + str(msg))

        if tag == VOTE_REQ:
            self.process_vote_request(src, msg)

        elif tag == VOTE_POS:
            self.process_positive_vote(src)

        elif tag == VOTE_NEG:
            self.vote[src] = -2

        elif tag == HEARTBEAT:
            if self.role != "LEADER":
                self.process_heartbeat(src, msg)
            else:  # on utilise le tag heartbeat pour repondre au heartbeat
                self.process_heartbeat_response(src, msg)

        elif tag == CLIENT_COMMAND:
            self.process_client_command(src, msg)


    def handle_send(self):
        tmp = time.time()
        if self.role == "CANDIDATE":
            if nb_servers == 1:
                debug_out("server number " + str(self.rank) + " is now leader")
                self.role = "LEADER"
                self.leader_heartbeat = time.time()
                return
            if tmp > self.request_vote + 1:
                self.request_vote = time.time()
                for i in range(1, nb_servers + 1):
                    if self.vote[i] == -1:
                        req = comm.isend((self.term, self.log), dest=i, tag=VOTE_REQ)
                        req.wait()
                debug_out("server number " + str(self.rank) + " is sending VOTE_REQ to everyone")

        if self.role == "LEADER":
            if len(self.waiting_clients) > 0:
                # on notifie le client si le log a ete replique chez une majorite
                if self.replicated[-len(self.waiting_clients)] >= majority:
                    self.notify_client()
            if tmp > self.leader_heartbeat + self.speed:
                debug_out("Server number " + str(self.rank) + " Sending HeartBeat")
                self.replicated = [1] * len(self.log)
                self.leader_heartbeat = time.time()
                for i in range(1, nb_servers + 1):
                    if i != self.rank:
                        req = comm.isend((self.term, self.log), dest=i, tag=HEARTBEAT)
                        req.wait()


    def consensus(self):
        '''
        Launch the consensus process bewteen all servers
        '''

        #Increment term by 1
        self.update_term(self.term + 1)

        #Get the current time
        current_time = time.time()

        #Create a timeout, if the server timeout it will launch a new consensus
        self.timeout = current_time + random.uniform(3.0, 8.0)

        #Check if the server has timed out
        while current_time <= self.timeout:
            #Update current time only if the server is not leader and has not crashed
            if self.role != "LEADER" and not self.crash:
                current_time = time.time()

            #Check if the server received message from REPL or from other servers
            self.handle_message()

            #If needed send message to other servers
            if not self.crash:
                self.handle_send()

        # Too long
        debug_out("server number " + str(self.rank) + " has timed out")
        debug_out("server number " + str(self.rank) + " is now candidate")
        self.role = "CANDIDATE"
        self.vote = [-1] * (nb_servers + 1)

        # Vote for himself
        self.vote[self.rank] = self.rank

    def run(self):
        '''
        Run consensus while the process is not killed
        '''

        debug_out(str(self.rank) + " is a server")

        while True:
            self.consensus()


def REPL():
    '''
    REPL, read stdin and send commands to servers
    '''

    while True:
        #Catch EOF error in order to avoid error massage when sending SIGINT to process
        try:
            #Read command
            command = input("REPL : ")
        except EOFError as e:
            break

        #Split command if it contains more than 1 word
        command = command.split()
        print(command)

        #If START send message to all clients
        if command[0] == "START":
            for i in range(nb_servers + 1, nb_servers + nb_clients + 1):
                req = comm.isend("START", dest=i, tag=START)
                req.wait()

        #If CRASH send message to the server specified in the command
        elif command[0] == "CRASH":
            comm.isend("CRASH", dest=int(command[1]), tag=CRASH)

        #If SPEED send message (LOW/MEDIUM/HIGH) to the server specified in the command
        elif command[0] == "SPEED":
            comm.isend(speed_value[command[2]], dest=int(command[1]), tag=SPEED)

        #If RECOVERY send message to the server specified in the command
        elif command[0] == "RECOVERY":
            comm.isend("RECOVERY", dest=int(command[1]), tag=RECOVERY)
        else:
            print("Invalid command")

        #Wait for next command
        time.sleep(0.1)


class Client:
    """
    Methods that simulate a client process
    """

    def __init__(self, rank):
        self.rank = rank
        self.nb_command = 10
        self.commands = []

    def wait_start(self):
        '''
        Wait for START command from REPL
        '''
        req = comm.irecv(source=0)
        req.wait()

    def read_commands(self):
        '''
        Load the commands list in client attribute
        '''

        #Name of the file containing the commands list
        filename = "client/" + str(self.rank) + ".command"

        #Open and read commands from file
        file = open(filename, "r+")
        self.commands = file.read().splitlines()
        file.close()

    def run(self):
        '''
        Send nb_req commands to leader
        '''

        #Wait for START command from REPL
        self.wait_start()

        #Load commands list from file
        self.read_commands()

        debug_out(str(self.rank) + " : I'm a client")

        #Number of request to send to leader
        nb_req = 3
        
        #Send request to leader and wait before sending the next request
        while nb_req > 0:
            nb_req -= 1

            #Wait time between 2 requests
            time.sleep(random.uniform(5, 8))

            #Select a command in the command list
            command = random.randint(0, self.nb_command - 1)

            debug_out("Sending message " + str(self.commands[command]))

            #Send request to every server because client don't know which one is leader
            for i in range(1, nb_servers + 1):
                req = comm.isend(self.commands[command], dest=i, tag=CLIENT_COMMAND)
                req.wait()


def main():
    '''
    Launch the correct function for every process in function of their rank
    '''

    rank = comm.Get_rank()

    #The rank 0 is the main thread, so it is the REPL because it needs to read stdin
    if rank == 0:
        REPL()

    #Ranks between 1 and nb_servers are for servers
    elif rank <= nb_servers:
        my_server = Server(rank)
        my_server.run()

    #Other ranks are for clients
    elif nb_servers < rank < (nb_clients + nb_servers + 1):
        my_client = Client(rank)
        my_client.run()



if __name__ == "__main__":
    main()
