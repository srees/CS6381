import types

import zmq
import json
from topiclist import TopicList
import publicip
from kademlia_client import KademliaClient
import time
import logging
import copy
from zkdriver import ZKDriver

print("Current libzmq version is %s" % zmq.zmq_version())
print("Current  pyzmq version is %s" % zmq.__version__)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
log = logging.getLogger('kademlia')
log.addHandler(handler)
log.setLevel(logging.DEBUG)


class KademliaReg:
    def __init__(self, args):
        print("Initializing registry object")
        self.args = args
        self.context = zmq.Context()
        self.REP_socket = self.context.socket(zmq.REP)
        ip = publicip.get_ip_address()
        reg_ip_port = ip + ":" + str(self.args.registry_port)
        self.REP_url = "tcp://" + reg_ip_port
        print("Binding REP to " + self.REP_url)
        self.REP_socket.bind(self.REP_url)
        self.REQ_socket = self.context.socket(zmq.REQ)
        self.die = False

        print("Initializing Zookeeper connection")
        zkargs = types.SimpleNamespace()
        zkargs.zkIPAddr = args.zookeeper
        zkargs.zkPort = args.zookeeper_port
        zk = ZKDriver(zkargs)
        zk.init_driver()
        zk.start_session()
        # store our information with zookeeper
        segments = ip.split('.')
        zk.create_znode('registries/registry'+segments[3], reg_ip_port)

        # retrieve from zookeeper list of other registries for DHT init
        nodes = []
        registries = zk.get_children('registries')
        print(registries)
        for registry in registries:
            print(registry)
            data = zk.get_value('registries/'+registry)
            print(data)
            parts = data.split(':')
            if ip not in parts[0]:
                nodes.append((parts[0], parts[1]))

        print("Initializing Kademlia connection")
        self.kad_client = KademliaClient(self.args.dht_port, nodes)

    def start(self):
        print("Registry starting")
        try:
            print("Registry listening...")
            while not self.die:
                message = self.REP_socket.recv_json()
                print("Received message: ")
                print(message)
                if message['role'] == 'broker': # assuming only one broker at this point in the code
                    # register with DHT
                    self.store_info(['broker'], {'ip': message['ip'], 'port': message['port']}, True)
                    # 10-4 then inform of publishers
                    self.REP_socket.send_json("Registered")
                    broker = {'ip': message['ip'], 'port': message['port']}
                    self.start_broker(broker)
                if message['role'] == 'publisher':
                    # register with DHT
                    self.store_info(message['topics'], {'ip': message['ip'], 'port': message['port']})
                    # 10-4 then inform of publishers
                    self.REP_socket.send_json("Registered")
                    pub = {'ip': message['ip'], 'port': message['port'], 'topics': message['topics']}
                    self.start_publisher(pub)
                    # self.print_registry()
                if message['role'] == 'stoppublisher':
                    # unregister with DHT
                    self.remove_info(message['topics'], {'ip': message['ip'], 'port': message['port']})
                    # reply
                    self.REP_socket.send_json("Unregistered")
                if message['role'] == 'subscriber':
                    # DHT doesn't care about registering subscribers with my model...
                    self.REP_socket.send_json("Registered")
                    sub = {'ip': message['ip'], 'port': message['port'], 'topics': message['topics']}
                    self.start_subscriber(sub)
                if message['role'] == 'updatebroker':
                    pubs = self.get_unique_publishers(message['topics'])
                    self.REP_socket.send_json(pubs)
                if message['role'] == 'updatesub':
                    data = self.fetch_for_sub(message['topics'])
                    self.REP_socket.send_json(data)

        except KeyboardInterrupt:
            print("Exiting listen loop.")
            self.die = True
            self.kad_client.kad_stop()

    # store_info: topics [s], data {} or [{}], replace bool
    def store_info(self, topics, data, replace=False):
        retry_delay = 2
        for topic in topics:
            print("Topic: " + topic)
            print("Storing:")
            print(data)
            attempt = 1
            if replace:
                dht_value = []
                if type(data) is list:
                    to_save = data
                else:
                    to_save = [data]
            else:
                dht_value = self.kad_client.get(topic)
                print("Raw pre-existing DHT value:")
                print(dht_value)
                if not dht_value:
                    dht_value = '[]'
                dht_value = json.loads(dht_value)
                to_save = copy.deepcopy(dht_value)
                if type(data) is list:
                    to_save.extend(data)
                else:
                    to_save.append(data)
            success = False
            while not success:
                print("Attempting to write:")
                print(to_save)
                self.kad_client.set(topic, json.dumps(to_save))
                if attempt > 1:
                    time.sleep(retry_delay)
                dht_value = self.kad_client.get(topic)
                if not dht_value:
                    dht_value = '[]'
                dht_value = json.loads(dht_value)
                print("Post-write DHT value:")
                print(dht_value)
                attempt += 1
                if data in dht_value or data == dht_value:
                    success = True

    def remove_info(self, topics, data):
        for topic in topics:
            dht_value = self.kad_client.get(topic)
            if dht_value:  # caveat: if the value was never set, this will never throw an error
                dht_value = json.loads(dht_value)
                if data in dht_value:
                    dht_value.remove(data)
                    self.store_info([topic], dht_value, True)

    def get_unique_publishers(self, topics=None):
        print("Getting unique publishers...")
        pubs = []
        unique_strings = []
        if topics is None or not topics:
            topics = TopicList.topiclist
            print("Using full topic list")
        for topic in topics:
            print("Getting data for " + topic)
            data = self.kad_client.get(topic)
            if data:
                print("Received data...")
                topic_pubs = json.loads(data)
                if topic_pubs:
                    for pub in topic_pubs:
                        print(pub)
                        tmp_string = pub['ip'] + ':' + pub['port']
                        if tmp_string not in unique_strings:
                            unique_strings.append(tmp_string)
                            pubs.append(pub)
            else:
                print("No data found")
        return pubs

    def start_broker(self, broker):  # We'll start the broker by sending it the list of publishers to subscribe to
        connection_string = 'tcp://' + broker.get('ip') + ':' + str(int(broker.get('port')) - 1)
        self.REQ_socket.connect(connection_string)
        print("Registry sending start to broker: " + connection_string)
        pubs = self.get_unique_publishers()
        print("Sending pubs to broker...")
        self.REQ_socket.send_json(pubs)
        self.REQ_socket.recv_json()
        self.REQ_socket.disconnect(connection_string)

    def start_subscriber(self, sub):
        data = self.fetch_for_sub(sub.get('topics'))
        connection_string = 'tcp://' + sub.get('ip') + ':' + str(int(sub.get('port')) - 1)
        self.REQ_socket.connect(connection_string)
        print("Registry sending info to subscriber: " + connection_string)
        self.REQ_socket.send_json(data)
        self.REQ_socket.recv_json()
        self.REQ_socket.disconnect(connection_string)

    def start_publisher(self, pub):
        connection_string = 'tcp://' + pub.get('ip') + ':' + str(int(pub.get('port')) - 1)
        self.REQ_socket.connect(connection_string)
        print("Sending start to: " + connection_string)
        self.REQ_socket.send_json("start")
        self.REQ_socket.recv_json()
        self.REQ_socket.disconnect(connection_string)

    def print_registry(self):
        for topic in TopicList.topiclist:
            data = self.kad_client.get(topic)
            print(topic)
            print(data)

    def fetch_for_sub(self, topics):
        if self.args.disseminate == 'broker':
            print("Registry passing broker information to subscribers:")
            result = self.kad_client.get("broker")
            while not result:
                time.sleep(1)
                result = self.kad_client.get("broker")
            broker = json.loads(self.kad_client.get("broker"))
            print("Found broker: ")
            print(broker)
            return broker
        else:
            print("Registry passing publisher information to subscribers:")
            temp_list = self.get_unique_publishers(topics)
            print("Found publishers: ")
            print(temp_list)
            return temp_list
