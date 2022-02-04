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


class Broker:
    def __init__(self, args):
        self.args = args
        self.pubs = []
        self.pub_sockets = []
        self.poller = zmq.Poller()
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.bind_url = 'tcp://' + publicip.get_ip_address() + ':' + self.args.bind
        self.bind_url2 = 'tcp://' + publicip.get_ip_address() + ':' + str(int(self.args.bind) + 1)
        print("Binding REP to " + self.bind_url)
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.bind(self.bind_url)

    def start(self):
        self.wait()  # wait for registry to give us the go
        # first subscribe to publishers
        for pub in self.pubs:
            connect_str = 'tcp://' + pub['ip'] + ':' + pub['port']
            print("Broker subscribing to " + connect_str)
            temp_sock = self.context.socket(zmq.SUB)
            temp_sock.connect(connect_str)
            temp_sock.setsockopt_string(zmq.SUBSCRIBE, '')
            self.pub_sockets.append(temp_sock)
        for i in range(0, len(self.pub_sockets)):
            self.poller.register(self.pub_sockets[i], zmq.POLLIN)
        print("Starting broker listen loop...")
        while True:
            try:
                events = dict(self.poller.poll())
                for sock in self.pub_sockets:
                    if sock in events:
                        data = sock.recv_json()
                        print("Broker received: ")
                        print(data)
                        self.republish(data)
            except KeyboardInterrupt:
                break

    # Wait for registry to give us the start signal
    def wait(self):
        self.pubs = self.socket.recv_json()  # load our list of pubs with the data from registry
        # TODO validation, ie while data != 'start', socket.recv_json()
        self.socket.send_json('ACK')
        # switch socket to publisher model
        self.socket.close(0)
        self.socket = self.context.socket(zmq.PUB)
        print("Binding PUB to " + self.bind_url2)
        self.socket.bind(self.bind_url2)

    def republish(self, data):
        self.socket.send_json(data)
