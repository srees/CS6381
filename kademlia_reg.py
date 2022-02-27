import zmq
from kademlia_dht import Kademlia_DHT
import json
from topiclist import TopicList
import publicip
import threading

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

        # print("Instantiate Kademlia DHT object")
        # if args.create:
        #     self.kdht = Kademlia_DHT()
        # else:
        #     self.kdht = Kademlia_DHT(True)
        # print("Initialize Kademlia DHT object")
        # args.ipaddr = args.bootstrap
        # args.port = args.bootstrap_port
        # if not self.kdht.initialize(args):
        #     print("Main: Initialization of Kademlia DHT failed")
        #     return

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
                    broker = {'ip': message['ip'], 'port': message['port']}
                    self.DHT_set("*", json.dumps([broker]))
                    # 10-4 then inform of publishers
                    self.REP_socket.send_json("Registered")
                    self.start_broker(broker)
                if message['role'] == 'publisher':
                    # register with DHT
                    for topic in message['topics']:
                        # here is where we could/should lock the DHT for changes
                        result = self.DHT_get(topic)
                        if result:
                            publishers = json.loads(result)
                        else:
                            publishers = []
                        publishers.append({'ip': message['ip'], 'port': message['port']})
                        self.DHT_set(topic, json.dumps(publishers))
                    # 10-4 then inform of publishers
                    self.REP_socket.send_json("Registered")
                    pub = {'ip': message['ip'], 'port': message['port'], 'topics': message['topics']}
                    self.start_publisher(pub)
                if message['role'] == 'subscriber':
                    # DHT doesn't care about registering subscribers with my model...
                    self.REP_socket.send_json("Registered")
                    sub = {'ip': message['ip'], 'port': message['port'], 'topics': message['topics']}
                    self.start_subscriber(sub)
        except KeyboardInterrupt:
            pass

    def get_unique_publishers(self, topics=None):
        pubs = []
        unique_strings = []
        if topics is None:
            topics = TopicList.topiclist
        for topic in topics:
            data = self.DHT_get(topic)
            if data:
                topic_pubs = json.loads(self.DHT_get(topic))
                if topic_pubs:
                    for pub in topic_pubs:
                        tmp_string = pub['ip'] + ':' + pub['port']
                        if tmp_string not in unique_strings:
                            unique_strings.append(tmp_string)
                            pubs.append(pub)
        return pubs

    def start_broker(self, broker):  # We'll start the broker by sending it the list of publishers to subscribe to
        connection_string = 'tcp://' + broker.get('ip') + ':' + str(int(broker.get('port')) - 1)
        pubs = self.get_unique_publishers()
        self.REQ_socket.connect(connection_string)
        print("Registry sending start to broker: " + connection_string)
        self.REQ_socket.send_json(pubs)
        self.REQ_socket.recv_json()
        self.REQ_socket.disconnect(connection_string)

    def start_subscriber(self, sub):
        if self.args.disseminate == 'broker':
            print("Registry passing broker information to subscribers:")
            broker = json.loads(self.DHT_get("*"))
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

    async def DHT_set(self, args, topic, content):
        print("Instantiate Kademlia DHT object")
        kdht = Kademlia_DHT()
        print("Initialize Kademlia DHT object")
        args.ipaddr = args.bootstrap
        args.port = args.bootstrap_port
        if not kdht.initialize(args):
            print("Main: Initialization of Kademlia DHT failed")
            return
        await kdht.set_value(topic, content)

    async def DHT_get(self, args, topic):
        print("Instantiate Kademlia DHT object")
        kdht = Kademlia_DHT()
        print("Initialize Kademlia DHT object")
        args.ipaddr = args.bootstrap
        args.port = args.bootstrap_port
        if not kdht.initialize(args):
            print("Main: Initialization of Kademlia DHT failed")
            return
        await kdht.get_value(topic)
