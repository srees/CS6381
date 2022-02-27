###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: API for the broker functionality in the middleware layer
#
# Created: Spring 2022
#
###############################################

# See the cs6381_publisher.py file for how an abstract Publisher class is
# defined and then two specialized classes. We may need similar things here.
# I am also assuming that discovery and dissemination are lumped into the
# broker. Otherwise keep them in separate files.
import zmq
import publicip
import time


class Broker:
    def __init__(self, args):
        self.args = args
        self.pubs = []
        self.SUB_sockets = []
        self.poller = zmq.Poller()
        self.context = zmq.Context()
        self.REP_socket = self.context.socket(zmq.REP)
        self.REP_url = 'tcp://' + publicip.get_ip_address() + ':' + self.args.bind
        print("Binding REP to " + self.REP_url)
        self.REP_socket.bind(self.REP_url)
        self.PUB_socket = self.context.socket(zmq.PUB)
        self.PUB_url = 'tcp://' + publicip.get_ip_address() + ':' + str(int(self.args.bind) + 1)
        print("Binding PUB to " + self.PUB_url)
        self.PUB_socket.bind(self.PUB_url)
        self.REQ_socket = self.context.socket(zmq.REQ)
        self.REQ_url = 'tcp://' + self.args.registry + ':' + str(int(self.args.port))
        print("Binding REQ to " + self.REQ_url)
        self.REQ_socket.bind(self.REQ_url)

    def start(self):
        self.wait()  # wait for registry to give us the go
        # first subscribe to publishers
        for pub in self.pubs:
            connect_str = 'tcp://' + pub['ip'] + ':' + pub['port']
            print("Broker subscribing to " + connect_str)
            temp_sock = self.context.socket(zmq.SUB)
            temp_sock.connect(connect_str)
            temp_sock.setsockopt_string(zmq.SUBSCRIBE, '')
            self.SUB_sockets.append(temp_sock)
        for i in range(0, len(self.SUB_sockets)):
            self.poller.register(self.SUB_sockets[i], zmq.POLLIN)
        print("Starting broker listen loop...")
        while True:
            try:
                events = dict(self.poller.poll())
                for SUB_sock in self.SUB_sockets:
                    if SUB_sock in events:
                        data = SUB_sock.recv_json()
                        print("Broker received: ")
                        print(data)
                        data["Brokered"] = time.time()
                        self.republish(data)
            except KeyboardInterrupt:
                break

    # Wait for registry to give us the start signal
    def wait(self):
        self.pubs = self.REP_socket.recv_json()  # load our list of pubs with the data from registry
        # TODO validation, ie while data != 'start', socket.recv_json()
        # We don't need to send anything back, but ZMQ requires us to reply
        self.REP_socket.send_json('ACK')
        # Allow time for things to settle before we start pumping data out.
        # Might be unnecessary since reworking the sockets...
        time.sleep(2)

    def republish(self, data):
        self.PUB_socket.send_json(data)

    def get_update(self, topic):
        data = None
        self.REQ_socket.send_json(data)
