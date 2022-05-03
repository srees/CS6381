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
import collections

import zmq
import time
import publicip
import copy
import threading


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
        self.ip = publicip.get_ip_address()
        self.REP_socket = self.context.socket(zmq.REP)
        self.REP_url = 'tcp://' + self.ip + ':' + self.args.bind
        print("Binding REP to " + self.REP_url)
        self.REP_socket.bind(self.REP_url)
        self.HIST_socket = self.context.socket(zmq.REP)
        self.HIST_url = 'tcp://' + self.ip + ':' + str(int(self.args.bind) + 2)
        print("Binding HIST to " + self.HIST_url)
        self.HIST_socket.bind(self.HIST_url)
        self.PUB_socket = self.context.socket(zmq.PUB)
        self.PUB_url = 'tcp://' + publicip.get_ip_address() + ':' + str(int(self.args.bind) + 1)
        print("Binding PUB to " + self.PUB_url)
        self.PUB_socket.bind(self.PUB_url)
        self.qos = 0
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

    def setQoS(self, qos):
        self. qos = qos

    def queue_history(self, topic, value):
        if topic not in self.topics_history:
            # self.topics_history[topic] = queue.Queue(maxsize=self.qos)
            self.topics_history[topic] = collections.deque([], self.qos)
        self.topics_history[topic].append(value)

    def fetch_queue(self, topic):
        history = []
        # get copy of current state of queue, as it is constantly in flux
        if topic in self.topics_history.keys():
            duplicate = copy.deepcopy(self.topics_history[topic])
            while duplicate:
                history.append(duplicate.popleft())
        return history

    def history_listen(self):
        print("History request listener started at " + self.HIST_url)
        while True:
            try:
                data = self.HIST_socket.recv_json()
                if data["message"] == "history":
                    print("Received history request for " + data["topic"])
                    to_send = self.fetch_queue(data["topic"])
                    print("Sending:")
                    print(to_send)
                    self.HIST_socket.send_json(to_send)
                else:
                    print("Received data message that was not 'history':")
                    print(data["message"])
                    self.HIST_socket.send_json([])
            except KeyboardInterrupt:
                break
        print("Exiting history.")
