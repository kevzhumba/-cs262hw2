from multiprocessing import Process 
import os 
import socket 
from _thread import * 
import threading 
import queue
from datetime import datetime
import time 
from threading import Thread 
import random
import math
import time

 
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

 
def producer(portVal, machine_id):
    host= "127.0.0.1" 
    other_machines = []
    try: 
        for port in portVal:
            s = socket.socket(socket.AF_INET,socket.SOCK_STREAM) 
            s.connect((host,int(port))) 
            other_machines.append(s)
            print("Client-side connection success to port val:" + str(port) + "\n")
        clock = 0
        #open a file 
        try: 
            with open(f"logs/machine_{machine_id}.log", "w") as file:
                print("Opened")
                print("Writing to file")
                file.write(f"Log for machine {machine_id} with rate {rate} \n")
                print(f"machine {machine_id} has rate {rate}")
                while True: 
                    #Now that we have all the connections, we also want to initialize a clock and a file to print to.
                    #first check queues
                    for i in range(rate):
                        print(f"machine {machine_id} processing instruction")
                        start_time = time.time()
                        try:
                            msg = msg_queue.get_nowait()
                            clock = do_process_msg(msg, clock, file)
                        except queue.Empty:
                            op = random.randint(1, 10)
                            clock = do_event(op, clock, other_machines, file)
                        time.sleep(1/rate-(time.time()-start_time))
        except IOError:
            print("Error opening file")
    except socket.error as e: 
        print ("Error connecting producer: %s" % e)

def do_process_msg(msg_clock: str, local_clock: int, file) -> int:
    try:
        update_clock = max(local_clock, int(msg_clock) + 1)
        print("Writing to file")

        file.write(f"Received message; System Time: {datetime.now()}; Logical clock: {update_clock} \n")
        file.flush()
        return update_clock
    except IOError:
        print("Error writing to file")

def do_event(op: int, clock: int, other_machines: list, file) -> int:
    match op:
        case 1:
            dest_sock = other_machines[0]
            dest_sock.send(f"{str(clock)}\n".encode('ascii'))
            clock = do_send(clock, 1, dest_sock.getsockname()[1], file)
        case 2: 
            dest_sock = other_machines[1]
            dest_sock.send(f"{str(clock)}\n".encode('ascii'))
            clock = do_send(clock, 2, dest_sock.getsockname()[1], file)
        case 3:
            for machine in other_machines:
                machine.send(f"{str(clock)}\n".encode('ascii'))
            clock = do_send(clock, 3, map(lambda x: x.getsockname()[1], other_machines), file)
        case _:
            clock = do_local_process(clock, file)
    return clock

def do_local_process(local_clock: int, file) -> int:
    try:
        update_clock = local_clock + 1
        print("Writing to file")

        file.write(f"Internal event; System Time: {datetime.now()}; Logical clock: {update_clock} \n")
        file.flush()

        return update_clock
    except IOError:
        print("Error writing to file")

def do_send(local_clock: int, op_code: int, port: list, file) -> int:
    try:
        update_clock = local_clock + 1
        print("Writing to file")

        file.write(f"Send message {op_code} to port(s) {str(port)}; System Time: {datetime.now()}; Logical clock: {update_clock} \n")
        file.flush()

        return update_clock
    except IOError:
        print("Error writing to file")
 

def init_machine(config): 
    HOST = str(config[0]) 
    PORT = int(config[1]) 
    print("starting server| port val:", PORT) 
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
    s.bind((HOST, PORT)) 
    s.listen() 
    while True:
        conn, addr = s.accept() 
        start_new_thread(consumer, (conn, ))
 

def machine(config): 
    #Initialize machine globals
    global msg_queue
    msg_queue = queue.Queue()
    global rate
    rate = random.randint(1, 6)
    init_thread = Thread(target=init_machine, args=(config,)) 
    init_thread.start() #add delay to initialize the server-side logic on all processes 
    time.sleep(5) # extensible to multiple producers 
    prod_thread = Thread(target=producer, args=(config[2:-1], config[-1])) 
    prod_thread.start()
    print("started thread")
    

localHost= "127.0.0.1"
 

if __name__ == '__main__': 
    port1 = 2056 
    port2 = 3056 
    port3 = 4056
 

    config1=[localHost, port1, port2, port3, 1] 
    p1 = Process(target=machine, args=(config1,)) 
    config2=[localHost, port2, port3, port1, 2] 
    p2 = Process(target=machine, args=(config2,)) 
    config3=[localHost, port3, port1, port2, 3] 
    p3 = Process(target=machine, args=(config3,))
 

    p1.start() 
    p2.start() 
    p3.start()
    

    p1.join() 
    p2.join()
    p3.join()