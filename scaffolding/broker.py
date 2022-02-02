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
import json


class Broker:
    def __init__(self, args):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.connect_str = "tcp://" + args.registry + ":" + str(args.port)

    def register(self, role, ip, port, topics):
        self.socket.connect(self.connect_str)
        data = {'role': role, 'ip': ip, 'port': port, 'topics': topics}
        self.socket.send(json.dumps(data))
        self.socket.disconnect(self, self.connect_str)

    def wait(self):
        self.socket.connect(self.connect_str)
        self.socket.send(b"Waiting for start...")
        message = self.socket.recv()  #does this need to be in a loop?
        self.socket.disconnect(self, self.connect_str)
