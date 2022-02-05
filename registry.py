import zmq

print("Current libzmq version is %s" % zmq.zmq_version())
print("Current  pyzmq version is %s" % zmq.__version__)

class Registry:
    def __init__(self, args):
        print("Initializing registry object")
        self.args = args
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.bind_url = "tcp://" + self.args.registry + ":" + str(self.args.port)
        print("Binding REP to " + self.bind_url)
        self.socket.bind(self.bind_url)
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

    def start(self):
        print("Registry starting")
        try:
            self.collect_registrations()
            if self.args.disseminate == "broker":
                self.start_broker()
            self.start_subscribers()
            self.start_publishers()
            # since this is not dynamic, per se, we don't need to keep running to answer
            # updates or requests from late joiners
        except KeyboardInterrupt:
            pass

    def collect_registrations(self):
        print("Registry waiting for " + str(self.expected_pubs) + " pubs, " + str(self.expected_subs) + " subs, and " + str(self.expected_brokers) + " brokers.")
        while len(self.pubs) < self.expected_pubs or len(self.subs) < self.expected_subs or len(self.broker) < self.expected_brokers:
            message = self.socket.recv_json()
            print("Received message: ")
            print(message)
            if message['role'] == 'broker':
                self.broker.append({'ip': message['ip'], 'port': message['port']})
                print("Registered a broker")
            if message['role'] == 'publisher':
                self.pubs.append({'ip': message['ip'], 'port': message['port'], 'topics': message['topics']})
                print("Registered a publisher")
                for topic in message['topics']:
                    if topic in self.topics and isinstance(self.topics[topic], list):
                        # add the IP address to it
                        self.topics[topic].append(message['ip'] + ':' + message['port'])
                    else:
                        # create the topic and add the IP:port
                        self.topics[topic] = [message['ip'] + ':' + message['port']]
            if message['role'] == 'subscriber':
                self.subs.append({'ip': message['ip'], 'port': message['port'], 'topics': message['topics']})
                print("Registered a subscriber")
            self.socket.send_json("Registered")
        print("All expected parties registered")

    def start_broker(self):  # We'll start the broker by sending it the list of publishers to subscribe to
        socket = self.context.socket(zmq.REQ)
        connection_string = 'tcp://' + self.broker[0].get('ip') + ':' + str(int(self.broker[0].get('port')) - 1)
        socket.connect(connection_string)
        print("Registry sending start to broker: " + connection_string)
        socket.send_json(self.pubs)
        socket.recv_json()
        socket.disconnect(connection_string)

    def start_subscribers(self):
        if self.args.disseminate == 'broker':
            print("Registry passing broker information to subscribers:")
            print(self.broker)
            # send each subscriber the broker IP:Port
            socket = self.context.socket(zmq.REQ)
            for sub in self.subs:
                connection_string = 'tcp://' + sub.get('ip') + ':' + str(int(sub.get('port')) - 1)
                socket.connect(connection_string)
                print("Registry sending broker to subscriber: " + connection_string)
                socket.send_json(self.broker)
                socket.recv_json()
                socket.disconnect(connection_string)
        else:
            print("Registry passing publisher information to subscribers:")
            pub_ips = []
            socket = self.context.socket(zmq.REQ)
            for sub in self.subs:
                for topic in sub['topics']:
                    if topic in self.topics:
                        pub_ips.extend(self.topics[topic])
                pub_ips = set(pub_ips)
                temp_list = []
                for pub in self.pubs:
                    if (pub['ip'] + ':' + pub['port']) in pub_ips:
                        temp_list.append(pub)
                connection_string = 'tcp://' + sub.get('ip') + ':' + str(int(sub.get('port')) - 1)
                socket.connect(connection_string)
                print("Sending: ")
                print(temp_list)
                print("To subscriber at: " + connection_string)
                socket.send_json(temp_list)
                socket.recv_json()
                socket.disconnect(connection_string)

    def start_publishers(self):
        print("Registry notifying publishers to start")
        # Iterate through list of publishers and issue a start
        socket = self.context.socket(zmq.REQ)
        for pub in self.pubs:
            connection_string = 'tcp://' + pub.get('ip') + ':' + str(int(pub.get('port')) - 1)
            socket.connect(connection_string)
            print("Sending start to: " + connection_string)
            socket.send_json("start")
            socket.recv_json()
            socket.disconnect(connection_string)
