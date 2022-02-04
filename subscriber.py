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


class Subscriber:

    # constructor. Add whatever class members you need
    # for the assignment
    def __init__(self, args):
        self.context = zmq.Context()
        self.args = args
        self.socket = self.context.socket(zmq.REP)
        self.bind_url = 'tcp://' + publicip.get_ip_address() + ':' + self.args.bind
        self.bind_url2 = 'tcp://' + publicip.get_ip_address() + ':' + str(int(self.args.bind) + 1)
        print("Binding REP to " + self.bind_url)
        self.socket.bind(self.bind_url)

    def process(self, data):
        print(data)

    def start(self):
        self.socket.recv_json()
        # TODO validation, ie while data != 'start', socket.recv_json()
        self.socket.send_json('ACK')
        # switch socket to publisher model
        self.socket.close(0)
        self.socket = self.context.socket(zmq.SUB)

