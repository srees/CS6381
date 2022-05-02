###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: API for the middleware layer for the publisher functionality
#
# Created: Spring 2022
#
###############################################
import copy
import threading
import zmq
import time
import publicip
import random
import queue


# A concrete class that disseminates info via the broker
class Publisher:

    # I struggled with trying to reuse the socket/port between the REP/REQ portion communicating
    # with the registry, and then switching over to PUB/SUB for the broker. I ended up testing
    # whether this was an issue with reusing the port by incrementing the bind port for the second
    # part - this worked and is why there are +/- 1 in the code for binds and publish. I plan to go
    # through and rework this into an argument at some point.
    def __init__(self, args):
        self.context = zmq.Context()
        self.args = args
        self.REP_socket = self.context.socket(zmq.REP)
        self.ip = publicip.get_ip_address()
        self.REP_url = 'tcp://' + self.ip + ':' + self.args.bind
        print("Binding REP to " + self.REP_url)
        self.REP_socket.bind(self.REP_url)
        self.PUB_socket = self.context.socket(zmq.PUB)
        self.PUB_url = 'tcp://' + publicip.get_ip_address() + ':' + str(int(self.args.bind) + 1)
        print("Binding PUB to " + self.PUB_url)
        self.PUB_socket.bind(self.PUB_url)
        self.topics_history = {}

    # to be invoked by the publisher's application logic
    # to publish a value of a topic. 
    def publish(self, topic, value):
        data = {'Topic': topic, 'Value': value, 'Sender': self.ip, 'Sent': time.time(), 'Broker': 'None', 'Brokered': 0}
        self.queue_history(topic, data)
        print("Publish: ")
        print(data)
        self.PUB_socket.send_json(data)

    def start(self):
        self.REP_socket.recv_json()
        # TODO validation, ie while data != 'start', socket.recv_json()
        # We don't need to send anything back, but ZMQ requires us to reply
        self.REP_socket.send_json('ACK')
        # start thread to listen for history requests
        history_thread = threading.Thread(target=self.history_listen)
        history_thread.start()
        # Allow time for things to settle before we start pumping data out.
        # Might be unnecessary since reworking the sockets...
        time.sleep(2)

    def set_topics(self, topics):
        log_list = []
        for topic in topics:
            history_length = random.randint(1, 10) * 5
            history = queue.Queue(maxsize=history_length)
            self.topics_history[topic] = history
            log_list.append([topic, history_length])
        print("Publisher initialized with the following topics and history lengths:")
        print(log_list)

    def queue_history(self, topic, value):
        self.topics_history[topic].put(value)

    # this function will return items from the queue that it has, up to a limit of count
    def fetch_queue(self, topic, count):
        history = []
        # get copy of current state of queue, as it is constantly in flux
        duplicate = copy.deepcopy(self.topics_history[topic])
        while not duplicate.empty():
            item = duplicate.get()
            if (duplicate.qsize() + 1) <= count:  # discard records older than requested
                history.append(item)
        return history

    def history_listen(self):
        die = False
        while True and not die:
            try:
                data = self.REP_socket.recv_json()
                if data["message"] == "history":
                    self.REP_socket.send_json(self.fetch_queue(data["topic"], int(data["quantity"])))
                else:
                    self.REP_socket.send_json([])
            except KeyboardInterrupt:
                die = True
                break
        print("Exiting history.")
