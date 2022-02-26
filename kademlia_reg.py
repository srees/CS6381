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
        self.expected_pubs = args.pubs
        self.expected_subs = args.subs
        if args.disseminate == "broker":
            self.expected_brokers = 1
        else:
            self.expected_brokers = 0
        self.broker = []
        self.pubs = []
        self.subs = []
        self.topics = {}

        print("Initializing Kademlia registry object")

        if self.args.create:
            self.kdht = Kademlia_DHT(True)
        else:
            self.kdht = Kademlia_DHT()
        args.ipaddr = args.registry
        args.port = args.override_port
        if not self.kdht.initialize(args):
            print("Main: Initialization of Kademlia DHT failed")
            return

        # check if this is the first node of the ring or others joining
        # an existing one
        self.ringThread = None
        if self.args.create:
            print("Main: create the first DHT node")
            self.ringThread = threading.Thread(self.kdht.create_bootstrap_node())
            # self.kdht.create_bootstrap_node()
        else:
            print("Main: join some DHT node")
            self.ringThread = threading.Thread(self.kdht.connect_to_bootstrap_node())
            # self.kdht.connect_to_bootstrap_node()
        self.ringThread.start()

    async def start(self):
        print("Registry starting")
        try:
            print("Registry listening...")
            while True:
                message = self.REP_socket.recv_json()
                print("Received message: ")
                print(message)
                if message['role'] == 'broker':
                    # register with DHT
                    await self.kdht.set_value("*", json.dumps([{'ip': message['ip'], 'port': message['port']}]))
                    # 10-4 then inform of publishers
                    self.REP_socket.send_json("Registered")
                    self.start_broker(self.get_unique_publishers())
                if message['role'] == 'publisher':
                    # register with DHT
                    for topic in message['topics']:
                        # here is where we could/should lock the DHT for changes
                        result = await self.kdht.get_value(topic)
                        if result:
                            publishers = json.loads(result)
                        else:
                            publishers = []
                        publishers.append({'ip': message['ip'], 'port': message['port']})
                        await self.kdht.set_value(topic, json.dumps(publishers))
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
            self.ringThread.join()

    async def get_unique_publishers(self, topics=None):
        pubs = []
        unique_strings = []
        if topics is None:
            topics = TopicList.topiclist
        for topic in topics:
            topic_pubs = json.loads(await self.kdht.get_value(topic))
            if topic_pubs:
                for pub in topic_pubs:
                    tmp_string = pub['ip'] + ':' + pub['port']
                    if tmp_string not in unique_strings:
                        unique_strings.append(tmp_string)
                        pubs.append(pub)
        return pubs

    def start_broker(self, pubs):  # We'll start the broker by sending it the list of publishers to subscribe to
        connection_string = 'tcp://' + self.broker[0].get('ip') + ':' + str(int(self.broker[0].get('port')) - 1)
        self.REQ_socket.connect(connection_string)
        print("Registry sending start to broker: " + connection_string)
        self.REQ_socket.send_json(pubs)
        self.REQ_socket.recv_json()
        self.REQ_socket.disconnect(connection_string)

    async def start_subscriber(self, sub):
        if self.args.disseminate == 'broker':
            print("Registry passing broker information to subscribers:")
            broker = json.loads(await self.kdht.get_value("*"))
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