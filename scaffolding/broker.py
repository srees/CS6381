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
        self.registry_connect_str = "tcp://" + args.registry + ":" + str(args.port)
        self.pub_quantity = args.pubs
        self.sub_quantity = args.subs
        self.pubs = []
        self.subs = []
        self.broker = {}
        self.bind = args.bind

    def register(self, role, ip, port):
        socket = self.context.socket(zmq.REQ)
        socket.connect(self.registry_connect_str)
        data = {'role': role, 'ip': ip, 'port': port}
        socket.send(json.dumps(data))
        socket.disconnect(self, self.registry_connect_str)

    def wait(self):
        socket = self.context.socket(zmq.REP)
        socket.bind("tcp://*:" + str(self.bind))
        while self.pubs.count() < self.pub_quantity and self.subs.count() < self.sub_quantity and not self.broker:
            message = json.loads(socket.recv())
            if message['role'] == 'broker':
                self.broker = {'ip': message['ip'], 'port': message['port']}
            if message['role'] == 'publisher':
                self.pubs.append({'ip': message['ip'], 'port': message['port'], 'topics': message['topics']})
            if message['role'] == 'subscriber':
                self.subs.append({'ip': message['ip'], 'port': message['port'], 'topics': message['topics']})
                # TODO it may be better to setup a dictionary of topics with a list of subscribers to each topic?
