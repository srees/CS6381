###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: API for the subscriber functionality in the middleware layer
#
# Created: Spring 2022
#
###############################################

# Please see the corresponding hints in the cs6381_publisher.py file
# to see how an abstract class is defined and then two specialized classes
# are defined based on the dissemination approach. Something similar
# may have to be done here. If dissemination is direct, then each subscriber
# will have to connect to each separate publisher with whom we match.
# For the ViaBroker approach, the broker is our only publisher for everything.
import zmq
import publicip
import json


class Subscriber:

    # constructor. Add whatever class members you need
    # for the assignment
    def __init__(self, args):
        self.args = args
        self.pubs = []
        self.pub_sockets = []
        self.poller = zmq.Poller()
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.bind_url = 'tcp://' + publicip.get_ip_address() + ':' + self.args.bind
        print("Binding REP to " + self.bind_url)
        self.socket.bind(self.bind_url)

    def start(self, topics):
        self.wait()  # registry will give us the go by sending us the list of publishers
        for pub in self.pubs:
            connect_str = 'tcp://' + pub['ip'] + ':' + pub['port']
            print("Subscribing to " + connect_str)
            temp_sock = self.context.socket(zmq.SUB)
            temp_sock.connect(connect_str)
            for topic in topics:
                temp_sock.setsockopt_string(zmq.SUBSCRIBE, topic)
            self.pub_sockets.append(temp_sock)
        for i in range(0, len(self.pub_sockets)):
            self.poller.register(self.pub_sockets[i], zmq.POLLIN)
        print("Starting subscriber listen loop...")
        while True:
            try:
                events = dict(self.poller.poll())
                for sock in self.pub_sockets:
                    if sock in events:
                        data = sock.recv_json()
                        print("Subscriber received: ")
                        print(data)
            except KeyboardInterrupt:
                break

    # Wait for registry to give us the start signal
    def wait(self):
        self.pubs = self.socket.recv_json()  # load our list of pubs with the data from registry
        # TODO validation, ie while data != 'start', socket.recv_json()
        self.socket.send_json('ACK')
        # switch socket to publisher model
        self.socket.close(0)
