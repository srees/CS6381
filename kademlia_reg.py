import zmq
import json
from topiclist import TopicList
import publicip
from kademlia_client import KademliaClient
import time

print("Current libzmq version is %s" % zmq.zmq_version())
print("Current  pyzmq version is %s" % zmq.__version__)


class KademliaReg:
    def __init__(self, args):
        self.args = args
        self.context = zmq.Context()
        self.REP_socket = self.context.socket(zmq.REP)
        self.REP_url = "tcp://" + publicip.get_ip_address() + ":" + str(self.args.port)
        print("Binding REP to " + self.REP_url)
        self.REP_socket.bind(self.REP_url)
        self.REQ_socket = self.context.socket(zmq.REQ)
        if args.disseminate == "broker":
            self.expected_brokers = 1
        else:
            self.expected_brokers = 0
        self.broker = []
        self.pubs = []
        self.subs = []
        self.topics = {}

        print("Initializing Kademlia registry object")

        nodes = []
        count = 1
        port = int(self.args.bootstrap_port)
        node_count = int(self.args.num_nodes)
        while count <= node_count:
            nodes.append(("10.0.0." + str(count), port))
            count += 1
        self.kad_client = KademliaClient(port, nodes)

    def start(self):
        print("Registry starting")
        try:
            print("Registry listening...")
            while True:
                message = self.REP_socket.recv_json()
                print("Received message: ")
                print(message)
                if message['role'] == 'broker':
                    # register with DHT
                    msg = [{'ip': message['ip'], 'port': message['port']}]
                    while msg not in json.loads(self.kad_client.get("broker")):
                        self.kad_client.set("broker", json.dumps(msg))
                        # time.sleep(1)
                    # 10-4 then inform of publishers
                    self.REP_socket.send_json("Registered")
                    broker = {'ip': message['ip'], 'port': message['port']}
                    self.start_broker(broker)
                if message['role'] == 'publisher':
                    # register with DHT
                    for topic in message['topics']:
                        result = self.kad_client.get(topic)
                        if result:
                            publishers = json.loads(result)
                        else:
                            publishers = []
                        pub = {'ip': message['ip'], 'port': message['port']}
                        publishers.append(pub)
                        print("Adding " + message['ip'] + " to " + topic)
                        msg = json.dumps(publishers)
                        check = self.kad_client.get(topic)
                        if not check:
                            check = '[]'
                        retry = 1
                        while pub not in json.loads(check):
                            print("Attempting to set value with Kademlia..." + str(retry))
                            self.kad_client.set(topic, msg)
                            time.sleep(1)
                            check = self.kad_client.get(topic)
                            if not check:
                                check = '[]'
                            retry += 1
                    # 10-4 then inform of publishers
                    self.REP_socket.send_json("Registered")
                    pub = {'ip': message['ip'], 'port': message['port'], 'topics': message['topics']}
                    self.start_publisher(pub)
                    self.print_registry()
                if message['role'] == 'stoppublisher':
                    # unregister with DHT
                    for topic in message['topics']:
                        result = self.kad_client.get(topic)
                        if result:
                            publishers = json.loads(result)
                            print("Removing " + message['ip'] + " from topic " + topic + ":")
                            print(publishers)
                            pub = {'ip': message['ip'], 'port': message['port']}
                            publishers.remove(pub)
                            resp = self.kad_client.get(topic)
                            if not resp:
                                resp = '[]'
                            while pub in json.loads(resp):
                                self.kad_client.set(topic, json.dumps(publishers))
                                time.sleep(1)
                                resp = self.kad_client.get(topic)
                                if not resp:
                                    resp = '[]'
                    self.REP_socket.send_json("Unregistered")
                if message['role'] == 'subscriber':
                    # DHT doesn't care about registering subscribers with my model...
                    self.REP_socket.send_json("Registered")
                    sub = {'ip': message['ip'], 'port': message['port'], 'topics': message['topics']}
                    self.start_subscriber(sub)
                if message['role'] == 'update':
                    pubs = self.get_unique_publishers(message['topics'])
                    self.REP_socket.send_json(pubs)
        except KeyboardInterrupt:
            pass

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
        if self.args.disseminate == 'broker':
            print("Registry passing broker information to subscribers:")
            broker = json.loads(self.kad_client.get("broker"))
            print("Found broker ")
            print(broker)
            # send each subscriber the broker IP:Port
            connection_string = 'tcp://' + sub.get('ip') + ':' + str(int(sub.get('port')) - 1)
            self.REQ_socket.connect(connection_string)
            print("Registry sending broker to subscriber: " + connection_string)
            self.REQ_socket.send_json(broker)
            self.REQ_socket.recv_json()
            self.REQ_socket.disconnect(connection_string)
        else:
            print("Registry passing publisher information to subscribers:")
            temp_list = self.get_unique_publishers(sub.get('topics'))
            connection_string = 'tcp://' + sub.get('ip') + ':' + str(int(sub.get('port')) - 1)
            self.REQ_socket.connect(connection_string)
            print("Sending: ")
            print(temp_list)
            print("To subscriber at: " + connection_string)
            self.REQ_socket.send_json(temp_list)
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
