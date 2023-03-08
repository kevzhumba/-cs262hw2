import socket
import queue
import time
import random

from typing import List
from multiprocessing import Process
from threading import Thread
from datetime import datetime
from _thread import *


MAX_RATE = 6
MAX_INTERNAL_ROLL = 10
LOCAL_HOST = "127.0.0.1"
TRIAL = 6
FOLDER = 'initial_params'


class Machine:
    def __init__(self, host: str, machine_id: int, port: int, other_ports: List[int]):
        self.host = host
        self.port = port
        self.machine_id = machine_id
        self.other_ports = other_ports

        self.rate = random.randint(1, MAX_RATE)

    def set_msg_queue(self, msg_queue: queue.Queue):
        self.msg_queue = msg_queue

    def init_machine(self):
        '''
        Initializes the machine by creating a server socket and listening for connections.
        Spawns new thread to handle each connection.
        '''
        print("starting server| port val:", self.port)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((self.host, self.port))
        s.listen()
        while True:
            conn, addr = s.accept()
            start_new_thread(self.consumer, (conn,))

    def consumer(self, conn):
        '''
        Continuously reads messages from the client socket and adds them to the machine's message queue.

        conn (socket.socket): client socket to recv messages from
        '''
        while True:
            self._consume_one_msg(conn)

    def _consume_one_msg(self, conn):
        '''
        Reads one message from the client socket and adds it to the machine's message queue.

        conn (socket.socket): client socket to recv messages from
        '''
        data = conn.recv(1024)
        if (int.from_bytes(data, 'big') <= 0):
            # Socket disconnected
            print("connection died")
            return
        dataVal = data.decode('ascii')
        if (dataVal[-1] == '\n'):
            dataVal = dataVal[:-1]
        for i in dataVal.split("\n"):
            self.msg_queue.put(i)

    def producer(self):
        '''
        Executes the machine. First connects to all other machines as a client, and then performs
        the spec. Rolls a random number, and depending on the number performs and action. The rate
        of instructions processed is limited by the global rate of the machine.
        '''
        other_machine_sockets = []
        try:
            # Connect to all other machines
            for port in self.other_ports:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((LOCAL_HOST, int(port)))
                other_machine_sockets.append(s)
                print("Client-side connection success to port val:" +
                      str(port) + "\n")
            clock = 0
            # Open a file to write logs to
            try:
                with open(f"logs/{FOLDER}/trial{TRIAL}_machine_{self.machine_id}.log", "w") as file:
                    print("Opened")
                    print("Writing to file")
                    file.write(
                        f"Log for machine {self.machine_id} with rate {self.rate} \n")
                    print(f"machine {self.machine_id} has rate {self.rate}")
                    while True:
                        # Now that we have all the connections, we also want to initialize a clock and a file to print to.
                        for i in range(self.rate):
                            print(
                                f"machine {self.machine_id} processing instruction")
                            start_time = time.time()
                            # Try getting a message from the queue, if empty then do an event
                            try:
                                msg = self.msg_queue.get_nowait()
                                clock = self.do_process_msg(
                                    msg, clock, file)
                            except queue.Empty:
                                op = random.randint(1, MAX_INTERNAL_ROLL)
                                clock = self.do_event(
                                    op, clock, other_machine_sockets, file)

                            # Sleep for the remainder of the time
                            time.sleep(1/self.rate-(time.time()-start_time))
            except IOError:
                print("Error opening file")
        except socket.error as e:
            print("Error connecting producer: %s" % e)

    def do_process_msg(self, msg_clock: str, local_clock: int, file) -> int:
        '''
        Processes a message receive and returns the updated clock

        msg_clock (int): the clock parsed from the received message
        local_clock (int): the current local clock
        file (file): the log to write to
        '''
        try:
            update_clock = max(local_clock+1, int(msg_clock) + 1)
            print("Writing to file")

            file.write(
                f"Received message; Queue Size: {self.msg_queue.qsize()}; System Time: {datetime.now()}; Logical clock: {update_clock} \n")
            file.flush()
            return update_clock
        except IOError:
            print("Error writing to file")

    def do_event(self, op: int, clock: int, other_machines: list, file) -> int:
        '''
        Performs an event based on the operation code, and returns the updated clock

        op (int): the operation code
        clock (int): the current local clock
        other_machines (list): the list of connected machines
        file (file): the log to write to
        '''
        match op:
            case 1:
                dest_sock = other_machines[0]
                dest_sock.send(f"{str(clock)}\n".encode('ascii'))
                clock = self.do_send(
                    clock, 1, dest_sock.getsockname()[1], file)
            case 2:
                dest_sock = other_machines[1]
                dest_sock.send(f"{str(clock)}\n".encode('ascii'))
                clock = self.do_send(
                    clock, 2, dest_sock.getsockname()[1], file)
            case 3:
                for machine in other_machines:
                    machine.send(f"{str(clock)}\n".encode('ascii'))
                clock = self.do_send(clock, 3, list(map(
                    lambda x: x.getsockname()[1], other_machines)), file)
            case _:
                clock = self.do_local_process(clock, file)
        return clock

    def do_local_process(self, local_clock: int, file) -> int:
        '''
        Processes a local tick and returns the updated clock

        local_clock (int): the current local clock
        file (file): the log to write to
        '''
        try:
            update_clock = local_clock + 1
            print("Writing to file")

            file.write(
                f"Internal event; Queue Size: {self.msg_queue.qsize()}; System Time: {datetime.now()}; Logical clock: {update_clock} \n")
            file.flush()

            return update_clock
        except IOError:
            print("Error writing to file")

    def do_send(self, local_clock: int, op_code: int, port: list, file) -> int:
        '''
        Processes a message send and returns the updated clock

        local_clock (int): the current local clock
        op_code (int): the operation for the send (1, 2, 3)
        port (list): the list of ports to send to
        file (file): the log to write to
        '''
        try:
            update_clock = local_clock + 1
            print("Writing to file")

            file.write(
                f"Send message {op_code} to port(s) {str(port)}; Queue Size: {self.msg_queue.qsize()}; System Time: {datetime.now()}; Logical clock: {update_clock} \n")
            file.flush()

            return update_clock
        except IOError:
            print("Error writing to file")


def run_machine(machine: Machine):
    '''
    Runs the machine. Initializes the machine, and then executes instructions for the machine

    machine (Machine): the machine to run
    '''
    # Set message queue
    msg_queue = queue.Queue()
    machine.set_msg_queue(msg_queue)

    # Initialize machine to handle incoming connections and messages
    init_thread = Thread(target=machine.init_machine)
    init_thread.start()  # add delay to initialize the server-side logic on all processes

    time.sleep(5)  # extensible to multiple producers
    prod_thread = Thread(target=machine.producer)
    prod_thread.start()
    print("started thread")


if __name__ == '__main__':
    port1 = 2056
    port2 = 3056
    port3 = 4056

    machine1 = Machine(LOCAL_HOST, 1, port1, [port2, port3])
    machine2 = Machine(LOCAL_HOST, 2, port2, [port1, port3])
    machine3 = Machine(LOCAL_HOST, 3, port3, [port1, port2])

    p1 = Process(target=run_machine, args=(machine1,))
    p2 = Process(target=run_machine, args=(machine2,))
    p3 = Process(target=run_machine, args=(machine3,))

    p1.start()
    p2.start()
    p3.start()

    p1.join()
    p2.join()
    p3.join()
