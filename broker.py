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
import threading
from zkdriver import ZKDriver
import types


class Broker:
    def __init__(self, args):
        self.args = args
        self.pubs = []
        self.SUB_sockets = {}
        self.ip = publicip.get_ip_address()
        self.poller = zmq.Poller()
        self.context = zmq.Context()
        self.role = 'standby'

        self.REP_socket = self.context.socket(zmq.REP)
        self.REP_url = 'tcp://' + self.ip + ':' + self.args.bind
        print("Binding REP to " + self.REP_url)
        self.REP_socket.bind(self.REP_url)

        self.PUB_socket = self.context.socket(zmq.PUB)
        self.PUB_url = 'tcp://' + self.ip + ':' + str(int(self.args.bind) + 1)
        print("Binding PUB to " + self.PUB_url)
        self.PUB_socket.bind(self.PUB_url)

        # add self to zookeeper for broker election
        print("Initializing Zookeeper connection")
        zkargs = types.SimpleNamespace()
        zkargs.zkIPAddr = args.zookeeper
        zkargs.zkPort = args.zookeeper_port
        segments = self.ip.split('.')
        publish_ip_port = self.ip + ":" + str(int(self.args.bind) + 1)
        self.zk = ZKDriver(zkargs)
        self.zk.init_driver()
        self.zk.start_session()
        self.election = self.zk.zk.Election('brokers/broker', publish_ip_port)
        self.election_backup = self.zk.zk.Election('brokers/backup', publish_ip_port)

        self.die = False
        self.registry = None
        self.poll_lock = False
        self.request_lock = False

    def start(self):
        self.wait()
        primary_contenders = self.election.contenders()
        backup_contenders = self.election_backup.contenders()
        if len(primary_contenders) <= len(backup_contenders):
            self.set_role('primary')
            self.election.run(self.leader_start)
        else:
            self.set_role('backup')
            self.election_backup.run(self.leader_start)

    def set_role(self, role):
        self.role = role
        self.registry.args.broker_role = role

    def leader_start(self):
        if self.role == 'backup':
            self.pubs = []  # we only want to start polling if requested during an update
        # first subscribe to publishers
        for pub in self.pubs:
            connect_str = 'tcp://' + pub['ip'] + ':' + pub['port']
            print("Broker subscribing to " + connect_str)
            temp_sock = self.context.socket(zmq.SUB)
            temp_sock.connect(connect_str)
            for topic in pub['topics']:
                temp_sock.setsockopt_string(zmq.SUBSCRIBE, '{"Topic": "' + topic)
            self.SUB_sockets[connect_str] = temp_sock
        for sock in self.SUB_sockets.values():
            self.poller.register(sock, zmq.POLLIN)
        print("Starting update loop thread")
        updates = threading.Thread(target=self.get_updates)
        updates.start()
        print("Starting broker listen loop...")
        while True and not self.die:
            try:
                if self.request_lock:
                    self.poll_lock = True
                if self.poller and self.SUB_sockets and not self.poll_lock:
                    events = dict(self.poller.poll())
                    for sock in self.SUB_sockets.values():
                        if sock in events:
                            data = sock.recv_json()
                            print("Broker received: ")
                            print(data)
                            data["Broker"] = self.ip
                            data["Brokered"] = time.time()
                            self.republish(data)
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
        # Allow time for things to settle before we start pumping data out.
        # Might be unnecessary since reworking the sockets...
        time.sleep(2)

    def republish(self, data):
        self.PUB_socket.send_json(data)

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
                        lock = self.request_poll_lock()
                        if lock:
                            # It isn't yet in our list of pubs, we need to add it!
                            print("Broker subscribing to " + connect_str)
                            temp_sock = self.context.socket(zmq.SUB)
                            temp_sock.connect(connect_str)
                            for topic in pub['topics']:
                                temp_sock.setsockopt_string(zmq.SUBSCRIBE, '{"Topic": "' + topic)
                            self.SUB_sockets[connect_str] = temp_sock
                            self.poller.register(self.SUB_sockets[connect_str], zmq.POLLIN)
                            self.pubs.append(pub)
                        else:
                            print("Error: unable to lock polling for updates")
                    else:  # double-check our existing subscriptions haven't increased
                        for s_pub in self.pubs:
                            if s_pub['ip'] == pub['ip'] and s_pub['port'] == pub['port']:
                                for update_topic in pub['topics']:
                                    if update_topic not in s_pub['topics']:
                                        # add a subscription to the topic -- these should never be reduced however
                                        self.SUB_sockets[connect_str].setsockopt_string(zmq.SUBSCRIBE, '{"Topic": "' + update_topic)
                for pub in self.pubs:
                    connect_str = 'tcp://' + pub['ip'] + ':' + pub['port']
                    if connect_str not in update_strings:
                        lock = self.request_poll_lock()
                        if lock:
                            # This publisher has been dropped from ones we should listen to, remove it!
                            print("Broker unsubscribing from " + connect_str)
                            self.poller.unregister(self.SUB_sockets[connect_str])
                            self.SUB_sockets[connect_str].disconnect(connect_str)
                            self.SUB_sockets[connect_str].close()
                            del self.SUB_sockets[connect_str]
                            self.pubs.remove(pub)
                        else:
                            print("Error: unable to lock polling for updates")
                self.release_poll_lock()
                time.sleep(10)
            except KeyboardInterrupt:
                self.die = True
                break
        print("Exiting update loop.")

    def set_registry(self, registry):
        self.registry = registry

    def request_poll_lock(self):
        if self.poll_lock:
            return True
        abort_time = 2.0
        start_time = time.time()
        abort = False
        self.request_lock = True
        while not self.poll_lock and not abort:
            if time.time()-start_time > abort_time:
                abort = True
            time.sleep(0.01)
        if self.poll_lock:
            return True
        else:
            return False

    def release_poll_lock(self):
        self.request_lock = False
        self.poll_lock = False
