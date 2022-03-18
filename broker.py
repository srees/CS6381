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
from zmq.utils.monitor import recv_monitor_message
import threading


class Broker:
    def __init__(self, args):
        self.args = args
        self.pubs = []
        self.SUB_sockets = {}
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
        self.monitor = self.REQ_socket.get_monitor_socket()
        self.REQ_url = 'tcp://' + self.args.registry + ':' + self.args.port
        self.REQ_socket.connect(self.REQ_url)
        self.current_registry = int(self.args.registry[-1])

    def start(self):
        self.wait()  # wait for registry to give us the go
        # first subscribe to publishers
        for pub in self.pubs:
            connect_str = 'tcp://' + pub['ip'] + ':' + pub['port']
            print("Broker subscribing to " + connect_str)
            temp_sock = self.context.socket(zmq.SUB)
            temp_sock.connect(connect_str)
            temp_sock.setsockopt_string(zmq.SUBSCRIBE, '')
            self.SUB_sockets[connect_str] = temp_sock
        for sock in self.SUB_sockets.values():
            self.poller.register(sock, zmq.POLLIN)
        print("Starting update loop thread")
        updates = threading.Thread(target=self.get_updates)
        updates.start()
        print("Starting monitor loop thread")
        monitoring = threading.Thread(target=self.do_monitor)
        monitoring.start()
        print("Starting broker listen loop...")
        while True:
            try:
                if self.poller and self.SUB_sockets:
                    events = dict(self.poller.poll())
                    for sock in self.SUB_sockets.values():
                        if sock in events:
                            data = sock.recv_json()
                            print("Broker received: ")
                            print(data)
                            data["Brokered"] = time.time()
                            self.republish(data)
            except zmq.error.ZMQError:
                pass  # this is needed because unregistering from the poller during an update often results in a socket error
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

    def get_updates(self):
        while True:
            print("Fetching updates from registry...")
            data = {'role': 'update', 'topics': []}
            self.REQ_socket.send_json(data)
            print("Request sent")
            updates = self.REQ_socket.recv_json()
            print("Reply received")
            print(updates)
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
                    temp_sock.setsockopt_string(zmq.SUBSCRIBE, '')
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

    def do_monitor(self):
        EVENT_MAP = {}
        # print("Event names:")
        for name in dir(zmq):
            if name.startswith('EVENT_'):
                value = getattr(zmq, name)
                # print("%21s : %4i" % (name, value))
                EVENT_MAP[value] = name
        while self.monitor.poll():
            evt = recv_monitor_message(self.monitor)
            evt.update({'description': EVENT_MAP[evt['event']]})
            print("Event: {}".format(evt))
            if evt['event'] == zmq.EVENT_DISCONNECTED:
                connected = False
                self.REQ_socket.disconnect(self.REQ_url)
                self.REQ_socket.close(0)
                self.REQ_socket = self.context.socket(zmq.REQ)
                tries = 1
                print("Registry failure! Checking other registries...")
                while connected is False:
                    tries += 1
                    if self.current_registry < int(self.args.registries):
                        self.current_registry += 1
                    else:
                        self.current_registry = 1
                    self.args.registry = self.args.registry[:-1] + str(self.current_registry)
                    self.REQ_url = 'tcp://' + self.args.registry + ':' + self.args.port
                    print("Trying " + self.REQ_url)
                    if self.REQ_socket.connect(self.REQ_url):
                        self.monitor = self.REQ_socket.get_monitor_socket()
                        connected = True
                    else:
                        if tries == int(self.args.registries):
                            print("All registries exhausted!")
                            self.REQ_socket.close(0)
                            self.monitor.close()
                            break
