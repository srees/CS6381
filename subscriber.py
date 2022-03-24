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
import time
import threading


class Subscriber:

    # constructor. Add whatever class members you need
    # for the assignment
    def __init__(self, args):
        self.args = args
        self.pubs = []
        self.SUB_sockets = {}
        self.ip = publicip.get_ip_address()
        self.poller = zmq.Poller()
        self.context = zmq.Context()

        self.REP_socket = self.context.socket(zmq.REP)
        self.REP_url = 'tcp://' + self.ip + ':' + self.args.bind
        print("Binding REP to " + self.REP_url)
        self.REP_socket.bind(self.REP_url)

        self.topics = []
        self.die = False
        self.registry = None

    def start(self, topics, function):
        self.wait()  # registry will give us the go by sending us the list of publishers
        self.topics = topics
        for pub in self.pubs:
            connect_str = 'tcp://' + pub['ip'] + ':' + pub['port']
            print("Subscribing to " + connect_str)
            temp_sock = self.context.socket(zmq.SUB)
            temp_sock.connect(connect_str)
            for topic in topics:
                temp_sock.setsockopt_string(zmq.SUBSCRIBE, '{"Topic": "' + topic)
            self.SUB_sockets[connect_str] = temp_sock
        for sock in self.SUB_sockets.values():
            self.poller.register(sock, zmq.POLLIN)
        print("Starting update loop thread")
        updates = threading.Thread(target=self.get_updates)
        updates.start()
        print("Starting subscriber listen loop...")
        while True and not self.die:
            try:
                if self.poller and self.SUB_sockets:
                    events = dict(self.poller.poll())
                    for sock in self.SUB_sockets.values():
                        if sock in events:
                            data = sock.recv_json()
                            data["Subscriber"] = self.ip
                            data["Received"] = time.time()
                            function(data)
            except zmq.error.ZMQError:
                pass  # this is needed because unregistering from the poller during an update often results in a socket error
            except KeyboardInterrupt:
                self.die = True
                break
        print("Exiting polling.")

    # Wait for registry to give us the start signal
    def wait(self):
        self.pubs = self.REP_socket.recv_json()  # load our list of pubs with the data from registry
        # TODO validation, ie while data != 'start', socket.recv_json()
        # We don't need to send anything back, but ZMQ requires us to reply
        self.REP_socket.send_json('ACK')

    def get_updates(self):
        while not self.die:
            try:
                updates = self.registry.get_updates()
                # add in any new publishers
                update_strings = []
                for pub in updates:
                    connect_str = 'tcp://' + pub['ip'] + ':' + pub['port']
                    update_strings.append(connect_str)
                    if connect_str not in self.SUB_sockets:
                        # It isn't yet in our list of pubs, we need to add it!
                        print("Broker subscribing to " + connect_str)
                        temp_sock = self.context.socket(zmq.SUB)
                        temp_sock.connect(connect_str)
                        for topic in self.topics:
                            temp_sock.setsockopt_string(zmq.SUBSCRIBE, '{"Topic": "' + topic)
                        self.SUB_sockets[connect_str] = temp_sock
                        self.poller.register(self.SUB_sockets[connect_str], zmq.POLLIN)
                        self.pubs.append(pub)
                for pub in self.pubs:
                    connect_str = 'tcp://' + pub['ip'] + ':' + pub['port']
                    if connect_str not in update_strings:
                        # This publisher has been dropped from ones we should listen to, remove it!
                        self.poller.unregister(self.SUB_sockets[connect_str])
                        self.SUB_sockets[connect_str].disconnect(connect_str)
                        self.SUB_sockets[connect_str].close()
                        del self.SUB_sockets[connect_str]
                        self.pubs.remove(pub)
                time.sleep(10)
            except KeyboardInterrupt:
                self.die = True
                break
        print("Exiting update loop.")

    def set_registry(self, registry):
        self.registry = registry
