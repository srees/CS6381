import zmq
import publicip
from zkdriver import ZKDriver
import types
import threading
from zmq.utils.monitor import recv_monitor_message
import time


class RegistryProxy:
    def __init__(self, args):
        self.args = args
        self.topics = []
        self.connected = False
        self.pubs = []

        # Get registry from zookeeper
        print("Initializing Zookeeper connection")
        zkargs = types.SimpleNamespace()
        zkargs.zkIPAddr = args.zookeeper
        zkargs.zkPort = args.zookeeper_port
        self.zk = ZKDriver(zkargs)
        self.zk.init_driver()
        self.zk.start_session()
        registries = self.zk.get_children('registries')
        self.current_registry = self.zk.get_value('registries/'+registries[0])

        # Connect to registry
        print("Connecting to " + self.current_registry)
        self.context = zmq.Context()
        self.REQ_socket = self.context.socket(zmq.REQ)
        self.REQ_url = 'tcp://' + self.current_registry
        if self.REQ_socket.connect(self.REQ_url):
            self.connected = True
        self.monitor = self.REQ_socket.get_monitor_socket()

        self.die = False
        print("Starting registry monitor loop thread")
        monitoring = threading.Thread(target=self.do_monitor)
        monitoring.start()

    def register(self, topics):
        self.topics = topics
        print("Registering with " + self.connection_string + '...')
        data = {'role': self.args.role, 'ip': publicip.get_ip_address(), 'port': str(int(self.args.bind) + 1), 'topics': topics}
        self.REQ_socket.send_json(data)
        print("Registration sent")
        self.REQ_socket.recv_json()
        print("Registration received")

    def stop(self, topics):
        data = {'role': 'stop' + self.args.role, 'ip': publicip.get_ip_address(), 'port': str(int(self.args.bind) + 1), 'topics': topics}
        self.REQ_socket.send_json(data)
        print("Stop requested")
        self.REQ_socket.recv_json()
        print("Stop acknowledged")
        self.die = True
        self.zk.stop_session()

    def get_updates(self):
        if self.connected:
            print("Fetching updates from registry...")
            if self.args.role is 'broker':
                role = 'updatebroker'
            else:
                role = 'updatesub'
            data = {'role': role, 'topics': self.topics}
            self.REQ_socket.send_json(data)
            print("Request sent")
            self.pubs = self.REQ_socket.recv_json()
            print("Reply received")
            print(self.pubs)
        return self.pubs

    def do_monitor(self):
        EVENT_MAP = {}
        # print("Event names:")
        for name in dir(zmq):
            if name.startswith('EVENT_'):
                value = getattr(zmq, name)
                # print("%21s : %4i" % (name, value))
                EVENT_MAP[value] = name
        while not self.die and self.monitor.poll():
            evt = recv_monitor_message(self.monitor)
            evt.update({'description': EVENT_MAP[evt['event']]})
            print("Event: {}".format(evt))
            if evt['event'] == zmq.EVENT_DISCONNECTED:
                self.connected = False
                self.REQ_socket.disconnect(self.REQ_url)
                self.REQ_socket.close(0)
                self.REQ_socket = self.context.socket(zmq.REQ)
                print("Registry failure! Checking other registries...")
                while self.connected is False:
                    registries = self.zk.get_children('registries')
                    for registry in registries:
                        option = self.zk.get_value('registries/'+registry)
                        if option not in self.current_registry:
                            self.REQ_url = 'tcp://' + option
                            print("Trying " + option)
                            if self.REQ_socket.connect(self.REQ_url):
                                self.monitor = self.REQ_socket.get_monitor_socket()
                                self.current_registry = option
                                self.connected = True
                                break
                    if not self.connected:
                        print("No new registry available, continuing to search...")
                        time.sleep(5)
