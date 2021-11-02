import mpi4py.MPI
from mpi4py import MPI
import sys
import random
import time
import os

# Number of clients
nb_clients = int(sys.argv[1])

# Number of servers
nb_servers = int(sys.argv[2])

# Debug option
debug_output = True if sys.argv[3] == "y" else False

# Communicator used to get rank and to send and receive messages
comm = MPI.COMM_WORLD

# Number of vote required to get majority
majority = nb_servers // 2 + 1

# Every tags used in message
VOTE_REQ, VOTE_POS, VOTE_NEG, HEARTBEAT, CLIENT_COMMAND, START, CRASH, SPEED, RECOVERY, TIMEOUT = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9

# Dictionary used for debug print
request = {0: "VOTE_REQ", 1: "VOTE_POS", 2: "VOTE_NEG", 3: "HEARTBEAT", 4: "CLIENT_COMMAND",
           5: "START", 6: "CRASH", 7: "SPEED", 8: "RECOVERY", 9: "TIMEOUT"}

# Values for SPEED command
speed_value = {"LOW": 3, "MEDIUM": 2, "HIGH": 1}
LOW, MEDIUM, HIGH = 3, 2, 1


def debug_out(msg):
    """
    Print debug message only if debug option is activated
    """

    if debug_output:
        print(msg)


class Server:
    """
    RAFT implementation
    """

    def __init__(self, rank):

        self.rank = rank  # Id of the thread
        self.role = "FOLLOWER"  # Role in RAFT algorithm
        self.update_term(0)  # Actual term
        self.timeout = 0  # Time limit for timeout
        self.leader_heartbeat = 0  # Time of previous heartbeat
        self.request_vote = 0  # Time of previous vote request send to followers
        self.log = []  # List of responded messages
        self.save_log()
        self.replicated = []  # List of number of servers that have replicated the log
        self.safe = 0
        self.vote = [-1] * (nb_servers + 1)  # List of vote (vote[i] is the server for which i has voted)
        self.waiting_clients = []  # List of messages waiting for other servers to replicate the message
        self.crash = False  # Bool to indicate if the server has crashed or not
        self.speed = MEDIUM  # Frequency of the heartbeat

        os.makedirs(os.path.dirname("disk/"), exist_ok=True)  # Create disk directory

    def save_term(self):
        """
        Write the term in a file every time it is updated
        """

        filename = "disk/" + str(self.rank) + ".term"
        with open(filename, "w+") as file:
            file.write(str(self.term) + '\n')

    def update_term(self, new_term):
        """
        change value of the term and saves it automatically

            Parameters:
                    new_term (int): the new term to save in the term file
        """

        self.term = new_term
        self.save_term()

    def notify_client(self):
        """
        If the majority of servers has replicated the message, the leader respond to the client
        """

        print("Responded to message", str(self.waiting_clients[0][0]))

        # Send response to client
        req = comm.isend(self.waiting_clients[0][0], dest=self.waiting_clients[0][1])
        req.wait()

        # Remove the message from the list waiting clients
        msg, src = self.waiting_clients.pop(0)

        # Write the message in the log file
        filename = "disk/" + str(self.rank) + ".command"
        with open(filename, "a+") as file:
            file.write("Message : " + msg + "\n")

    def save_log(self):
        filename = "disk/" + str(self.rank) + ".command"
        with open(filename, "w+") as file:
            for elt in self.log:
                file.write("Message : " + elt + "\n")

    def handle_log(self, log, replicated):
        """
        Replicate leader's log into current server log and write it into log file

            Parameters:
                    log (int[]): The list of all logs
                    replicated (int[]): The number of replication of log entry
        """
        self.replicated = replicated
        insert = -1
        # Copy leader's log into the server's log
        if len(log) > len(self.log):
            insert = len(self.log)
        else:
            i = 0
            while insert == -1 and i < len(self.log):
                if log[i] != self.log[i]:
                    insert = i
                i += 1
        if insert != -1:
            if insert == len(self.log):
                self.log.append(log[insert])
            else:
                self.log[insert] = log[insert]
            # write to file
            self.save_log()
        return insert

    def process_vote_request(self, src, msg):
        """
        Respond Yes or No to the sender of the vote request

            Parameters:
                    src (int): Id of the sender of the message
                    msg (int,int[]): Term and list of logs
        """

        term, log = msg
        """if term > self.term:
            self.update_term(term)"""

        # Check if the server can respond Yes
        if term > self.term and len(log) >= len(self.log):
            # Update the term
            self.update_term(term)

            # Reset current vote if the server was CANDIDATE
            self.vote = [-1] * (nb_servers + 1)

            # Change the vote of the current server
            self.vote[self.rank] = src

            # Change the role of the current server
            self.role = "FOLLOWER"

            # Add time to timeout
            self.timeout = time.time() + random.randint(3, 5)
            # Send positive response
            req = comm.isend(self.term, dest=src, tag=VOTE_POS)
            req.wait()
        else:
            if term > self.term:
                self.role = "FOLLOWER"
                self.update_term(term)
                self.timeout = time.time() + random.randint(3, 5)
            # Send negative response
            req = comm.isend(self.term, dest=src, tag=VOTE_NEG)
            req.wait()

    def process_positive_vote(self, src):
        """
        Process a positive vote response

            Parameters:
                    src (int): Id of the sender of the response
        """

        # Return if the server is not a CANDIDATE
        if self.role != "CANDIDATE":
            return

        # Set the vote of the sender of the response
        self.vote[src] = self.rank

        # Count if the current CANDIDATE has enough vote
        nb_vote = len([vote for vote in self.vote if vote == self.rank])
        if nb_vote >= majority:
            # If the CANDIDATE has enough vote he become LEADER
            if self.role != "LEADER":
                print("server number " + str(self.rank) + " is now leader")
            self.role = "LEADER"

            # The server is now LEADER and start sending heartbeat to other servers
            self.leader_heartbeat = time.time()

    def process_heartbeat(self, src, msg):
        """
        Process a heartbeat message

            Parameters:
                    src (int): Id of the sender of the heartbeat (LEADER)
                    msg (int, int[]): Term and list of logs
        """

        term, log, replicated = msg

        debug_out("server number " + str(self.rank) + " receiving HeartBeat")

        # If the server is not already FOLLOWER change the role
        self.role = "FOLLOWER"

        # Add time to timeout
        self.timeout += random.randint(3, 5)

        # Update term and log
        self.update_term(term)
        # On envoie le rang dans le log qui a ete replique -1 si on a rien replique
        tosend = self.handle_log(log, replicated)
        req = comm.isend(tosend, dest=src, tag=HEARTBEAT)
        req.wait()

    def process_heartbeat_response(self, src, msg):
        """
        Process heartbeat message if current server is LEADER

            Parameters:
                    src (int): Id of the sender of the response
                    msg (int): index of replicated log entry
        """
        if msg != -1:
            self.replicated[msg] += 1
        return

    def process_client_command(self, src, msg):
        """
        Process heartbeat message if current server is LEADER

            Parameters:
                    src (int): Id of the client
                    msg (int): Command of the client

        """

        # Only the LEADER handle a command from a client
        if self.role == "LEADER":
            self.log.append(msg)
            self.replicated.append(1)
            self.waiting_clients.append((msg, src))
            
    def load_data(self):
        """
        Read from files the term and the logs
        """

        # Load term from file
        term_filename = "disk/" + str(self.rank) + ".term"
        with open(term_filename, "r+") as file:
            self.term = int(file.readline())

        # Load logs from file
        log_filename = "disk/" + str(self.rank) + ".command"
        self.log = []
        with open(log_filename, "r+") as file:
            for line in file:
                log = line.split()[2]
                self.log.append(log)

    def handle_message(self):
        """
        Check if a message has been received and call the right function to handle the request
        """

        status = MPI.Status()
        # Check if a message has been received
        is_message = comm.Iprobe(status=status)

        if not is_message:
            return

        # Get source
        src = status.source
        # Get tag
        tag = status.tag
        # Wait to receive message
        msg = comm.irecv().wait()

        # If source is REPL
        # Change crash boolean
        if tag == CRASH:
            self.crash = True
            self.role = "FOLLOWER"
        # Update speed of the server
        elif tag == SPEED:
            if msg == "LOW":
                self.speed = LOW
            elif msg == "MEDIUM":
                self.speed = MEDIUM
            elif msg == "HIGH":
                self.speed = HIGH
        # Change crash boolean and load data
        elif tag == RECOVERY:
            self.crash = False
            self.timeout = time.time() + random.randint(3, 5)
            self.load_data()
        elif tag == TIMEOUT:
            self.timeout = 0;
        # If we crashed we don't communicate with other servers
        if self.crash:
            return

        # If message come from clients or servers
        debug_out("server number " + str(self.rank)
                  + " source : " + str(src)
                  + " tag : " + request[tag]
                  + " message : " + str(msg))

        # If tag is a vote request
        if tag == VOTE_REQ:
            self.process_vote_request(src, msg)

        # If tag is a positive response
        elif tag == VOTE_POS:
            self.process_positive_vote(src)

        # If tag is a negative response
        elif tag == VOTE_NEG:
            self.vote[src] = -2

        # If tag is a heartbeat
        elif tag == HEARTBEAT:
            if self.role != "LEADER":
                self.process_heartbeat(src, msg)
            else:  # We use heartbeat tag to respond to heartbeat
                self.process_heartbeat_response(src, msg)

        # If tag is a client command
        elif tag == CLIENT_COMMAND:
            self.process_client_command(src, msg)

    def handle_send(self):
        """
        Check if the server need to send message
        """

        # Get the current time
        tmp = time.time()

        # If the server is a CANDIDATE
        if self.role == "CANDIDATE":

            # If the system contains only one server the CANDIDATE become LEADER
            if nb_servers == 1:
                debug_out("server number " + str(self.rank) + " is now leader")
                self.role = "LEADER"
                self.leader_heartbeat = time.time()
                return

            # If a second has passed send vote request to all servers that have not responded
            if tmp > self.request_vote + 1:
                self.request_vote = time.time()

                # Send a request to other servers
                for i in range(1, nb_servers + 1):
                    if self.vote[i] == -1:
                        req = comm.isend((self.term, self.log), dest=i, tag=VOTE_REQ)
                        req.wait()

                debug_out("server number " + str(self.rank) + " is sending VOTE_REQ to everyone")

        # If the server is the LEADER
        if self.role == "LEADER":
            if len(self.waiting_clients) > 0:

                # We notify the client if the log has been replicated for a majority
                if self.replicated[-len(self.waiting_clients)] >= majority:
                    self.safe = -len(self.waiting_clients)
                    self.notify_client()

            # If self.speed seconds have passed since the last heartbeat send a new heatbeat
            if tmp > self.leader_heartbeat + self.speed:
                debug_out("Server number " + str(self.rank) + " Sending HeartBeat")
                self.leader_heartbeat = time.time()

                # Send heartbeat to all FOLLOWERS
                for i in range(1, nb_servers + 1):
                    if i != self.rank:
                        req = comm.isend((self.term, self.log, self.replicated), dest=i, tag=HEARTBEAT)
                        req.wait()

    def consensus(self):
        """
        Launch the consensus process bewteen all servers
        """

        # Increment term by 1
        self.update_term(self.term + 1)

        # Get the current time
        current_time = time.time()

        # Create a timeout, if the server timeout it will launch a new consensus
        self.timeout = current_time + random.uniform(3.0, 8.0)

        # Check if the server has timed out
        while current_time <= self.timeout:
            # Update current time only if the server is not leader and has not crashed
            if self.role != "LEADER" and not self.crash:
                current_time = time.time()

            # Check if the server received message from REPL or from other servers
            self.handle_message()

            # If needed send message to other servers
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
        """
        Run consensus while the process is not killed
        """

        debug_out(str(self.rank) + " is a server")

        while True:
            self.consensus()


def REPL():
    """
    REPL, read stdin and send commands to servers
    """

    while True:
        # Catch EOF error in order to avoid error massage when sending SIGINT to process
        try:
            # Read command
            command = input("REPL : ")
        except EOFError as e:
            break

        # Split command if it contains more than 1 word
        command = command.split()
        print(command)

        # If START send message to all clients
        if command[0] == "START":
            for i in range(nb_servers + 1, nb_servers + nb_clients + 1):
                req = comm.isend("START", dest=i, tag=START)
                req.wait()

        # If CRASH send message to the server specified in the command
        elif command[0] == "CRASH":
            try:
                req = comm.isend("CRASH", dest=int(command[1]), tag=CRASH)
                req.wait()
            except:
                print("REPL: invalid argument")
                continue

        # If SPEED send message (LOW/MEDIUM/HIGH) to the server specified in the command
        elif command[0] == "SPEED":
            try:
                req = comm.isend(speed_value[command[2]], dest=int(command[1]), tag=SPEED)
                req.wait()
            except:
                print("REPL: invalid argument")
                continue
        # If RECOVERY send message to the server specified in the command
        elif command[0] == "RECOVERY":
            try:
                req = comm.isend("RECOVERY", dest=int(command[1]), tag=RECOVERY)
                req.wait()
            except:
                print("REPL: invalid argument")
                continue

        # If TIMEOUT send message to the server specified in the command
        elif command[0] == "TIMEOUT":
            try:
                req = comm.isend("TIMEOUT", dest=int(command[1]), tag=TIMEOUT)
                req.wait()
            except:
                print("REPL: invalid argument")
                continue
        
        # Invalid command
        else:
            print("Invalid command")

        # Wait for next command
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
        """
        Wait for START command from REPL
        """
        req = comm.irecv(source=0)
        req.wait()

    def read_commands(self):
        """
        Load the commands list in client attribute
        """

        # Name of the file containing the commands list
        filename = "client/" + str(self.rank) + ".command"

        # Open and read commands from file
        file = open(filename, "r+")
        self.commands = file.read().splitlines()
        file.close()

    def run(self):
        """
        Send nb_req commands to leader
        """

        # Wait for START command from REPL
        self.wait_start()

        # Load commands list from file
        self.read_commands()

        debug_out(str(self.rank) + " : I'm a client")

        # Send request to leader and wait before sending the next request
        for elt in self.commands:

            # Wait time between 2 requests
            time.sleep(random.uniform(5, 8))

            debug_out("Sending message " + str(elt))

            # Send request to every server because client don't know which one is leader
            for i in range(1, nb_servers + 1):
                req = comm.isend(elt, dest=i, tag=CLIENT_COMMAND)
                req.wait()


def main():
    """
    Launch the correct function for every process in function of their rank
    """

    rank = comm.Get_rank()

    # The rank 0 is the main thread, so it is the REPL because it needs to read stdin
    if rank == 0:
        REPL()

    # Ranks between 1 and nb_servers are for servers
    elif rank <= nb_servers:
        my_server = Server(rank)
        my_server.run()

    # Other ranks are for clients
    elif nb_servers < rank < (nb_clients + nb_servers + 1):
        my_client = Client(rank)
        my_client.run()


if __name__ == "__main__":
    main()
