###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: API for the broker proxy at the middleware layer
#
# Created: Spring 2022
#
###############################################

# If you decide to do the RPC approach, you might need a proxy for the
# real broker.
#
# This is the proxy object for the broker which is held by both the
# publisher and subscriber-side middleware to store info about the
# whereabouts of the actual broker. The application level logic does not
# know that it is talking to a proxy object. It will simply invoke methods
# on the proxy, which then get translated under the hood into the appropriate
# serialization logic and sending to the real broker

import zmq
import json


class BrokerProxy:
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
