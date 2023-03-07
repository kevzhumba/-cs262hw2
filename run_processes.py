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
MAX_INTERNAL_ROLL = 4
LOCAL_HOST = "127.0.0.1"
TRIAL = 3
FOLDER = 'smaller_internal'


class Config:
    def __init__(self, host: str, machine_id: int, port: int, other_ports: List[int]):
        self.host = host
        self.port = port
        self.machine_id = machine_id
        self.other_ports = other_ports


def consumer(conn):
    while True:
        data = conn.recv(1024)
        if (int.from_bytes(data, 'big') <= 0):
            # Socket disconnected
            print("connection died")
            return
        dataVal = data.decode('ascii')
        if (dataVal[-1] == '\n'):
            dataVal = dataVal[:-1]
        for i in dataVal.split("\n"):
            msg_queue.put(i)


def producer(other_ports, machine_id):
    other_machine_sockets = []
    try:
        for port in other_ports:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((LOCAL_HOST, int(port)))
            other_machine_sockets.append(s)
            print("Client-side connection success to port val:" + str(port) + "\n")
        clock = 0
        # open a file
        try:
            with open(f"logs/{FOLDER}/trial{TRIAL}_machine_{machine_id}.log", "w") as file:
                print("Opened")
                print("Writing to file")
                file.write(f"Log for machine {machine_id} with rate {rate} \n")
                print(f"machine {machine_id} has rate {rate}")
                while True:
                    # Now that we have all the connections, we also want to initialize a clock and a file to print to.
                    # first check queues
                    for i in range(rate):
                        print(f"machine {machine_id} processing instruction")
                        start_time = time.time()
                        try:
                            msg = msg_queue.get_nowait()
                            clock = do_process_msg(
                                msg, clock, file, msg_queue.qsize())
                        except queue.Empty:
                            op = random.randint(1, MAX_INTERNAL_ROLL)
                            clock = do_event(
                                op, clock, other_machine_sockets, file, msg_queue.qsize())
                        time.sleep(1/rate-(time.time()-start_time))
        except IOError:
            print("Error opening file")
    except socket.error as e:
        print("Error connecting producer: %s" % e)


def do_process_msg(msg_clock: str, local_clock: int, file, qsize) -> int:
    try:
        update_clock = max(local_clock+1, int(msg_clock) + 1)
        print("Writing to file")

        file.write(
            f"Received message; Queue Size: {qsize}; System Time: {datetime.now()}; Logical clock: {update_clock} \n")
        file.flush()
        return update_clock
    except IOError:
        print("Error writing to file")


def do_event(op: int, clock: int, other_machines: list, file, qsize) -> int:
    match op:
        case 1:
            dest_sock = other_machines[0]
            dest_sock.send(f"{str(clock)}\n".encode('ascii'))
            clock = do_send(clock, 1, dest_sock.getsockname()[1], file, qsize)
        case 2:
            dest_sock = other_machines[1]
            dest_sock.send(f"{str(clock)}\n".encode('ascii'))
            clock = do_send(clock, 2, dest_sock.getsockname()[1], file, qsize)
        case 3:
            for machine in other_machines:
                machine.send(f"{str(clock)}\n".encode('ascii'))
            clock = do_send(clock, 3, list(map(
                lambda x: x.getsockname()[1], other_machines)), file, qsize)
        case _:
            clock = do_local_process(clock, file, qsize)
    return clock


def do_local_process(local_clock: int, file, qsize) -> int:
    try:
        update_clock = local_clock + 1
        print("Writing to file")

        file.write(
            f"Internal event; Queue Size: {qsize}; System Time: {datetime.now()}; Logical clock: {update_clock} \n")
        file.flush()

        return update_clock
    except IOError:
        print("Error writing to file")


def do_send(local_clock: int, op_code: int, port: list, file, qsize) -> int:
    try:
        update_clock = local_clock + 1
        print("Writing to file")

        file.write(
            f"Send message {op_code} to port(s) {str(port)}; Queue Size: {qsize}; System Time: {datetime.now()}; Logical clock: {update_clock} \n")
        file.flush()

        return update_clock
    except IOError:
        print("Error writing to file")


def init_machine(config):
    host = config.host
    port = config.port
    print("starting server| port val:", port)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((host, port))
    s.listen()
    while True:
        conn, addr = s.accept()
        start_new_thread(consumer, (conn, ))


def machine(config: Config):
    # Initialize machine globals
    global msg_queue
    msg_queue = queue.Queue()
    global rate
    rate = random.randint(1, MAX_RATE)
    init_thread = Thread(target=init_machine, args=(config,))
    init_thread.start()  # add delay to initialize the server-side logic on all processes
    time.sleep(5)  # extensible to multiple producers
    prod_thread = Thread(target=producer, args=(
        config.other_ports, config.machine_id,))
    prod_thread.start()
    print("started thread")


if __name__ == '__main__':
    port1 = 2056
    port2 = 3056
    port3 = 4056

    config1 = Config(LOCAL_HOST, 1, port1, [port2, port3])
    p1 = Process(target=machine, args=(config1,))

    config2 = Config(LOCAL_HOST, 2, port2, [port1, port3])
    p2 = Process(target=machine, args=(config2,))

    config3 = Config(LOCAL_HOST, 3, port3, [port1, port2])
    p3 = Process(target=machine, args=(config3,))

    p1.start()
    p2.start()
    p3.start()

    p1.join()
    p2.join()
    p3.join()
