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

import zmq
import time
import publicip


# A concrete class that disseminates info via the broker
class Publisher:

    # I struggled with trying to reuse the socket/port between the REP/REQ portion communicating
    # with the registry, and then switching over to PUB/SUB for the broker. I ended up testing
    # whether this was an issue with reusing the port by incrementing the bind port for the second
    # part - this worked and is why there are +/- 1 in the code for binds and publish. I plan to go
    # through and rework this into an argument at some point.
    def __init__(self, args):
        print("Utilizing broker send publish method")
        self.context = zmq.Context()
        self.args = args
        self.socket = self.context.socket(zmq.REP)
        self.bind_url = 'tcp://' + publicip.get_ip_address() + ':' + self.args.bind
        self.bind_url2 = 'tcp://' + publicip.get_ip_address() + ':' + str(int(self.args.bind) + 1)
        print("Binding REP to " + self.bind_url)
        self.socket.bind(self.bind_url)

    # to be invoked by the publisher's application logic
    # to publish a value of a topic. 
    def publish(self, topic, value):
        data = {'Topic': topic, 'Value': value, 'TS': str(time.time())}
        print("Publish: ")
        print(data)
        self.socket.send_json(data)

    def start(self):
        self.socket.recv_json()
        # TODO validation, ie while data != 'start', socket.recv_json()
        self.socket.send_json('ACK')
        # switch socket to publisher model
        self.socket.close(0)
        self.socket = self.context.socket(zmq.PUB)
        print("Binding PUB to " + self.bind_url2)
        self.socket.bind(self.bind_url2)
        time.sleep(2)
