import zmq
import publicip

print("Current libzmq version is %s" % zmq.zmq_version())
print("Current  pyzmq version is %s" % zmq.__version__)

class Registry:
    def __init__(self, args):
        print("Initializing registry object")
        self.args = args
        self.context = zmq.Context()
        self.expected_pubs = args.pubs
        self.expected_subs = args.subs
        if args.disseminate == "broker":
            self.expected_brokers = 1
        else:
            self.expected_brokers = 0
        self.broker = {}
        self.pubs = []
        self.subs = []

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
        socket = self.context.socket(zmq.REP)
        print("Binding REP to tcp://" + self.args.listen + ":" + str(self.args.port))
        socket.bind("tcp://" + self.args.listen + ":" + str(self.args.port))
        while len(self.pubs) < self.expected_pubs or len(self.subs) < self.expected_subs or len(self.broker) < self.expected_brokers:
            message = socket.recv_json()
            print("Received message: ")
            print(message)
            if message['role'] == 'broker':
                self.broker = {'ip': message['ip'], 'port': message['port']}
                print("Registered a broker")
            if message['role'] == 'publisher':
                self.pubs.append({'ip': message['ip'], 'port': message['port'], 'topics': message['topics']})
                print("Registered a publisher")
            if message['role'] == 'subscriber':
                self.subs.append({'ip': message['ip'], 'port': message['port'], 'topics': message['topics']})
                print("Registered a subscriber")
            socket.send_json("Registered")
        print("All expected parties registered")

    def start_broker(self):  # We'll start the broker by sending it the list of publishers to subscribe to
        print("Registry notifying broker to start")
        socket = self.context.socket(zmq.REQ)
        socket.connect('tcp://' + self.broker.get('ip') + ':' + str(int(self.broker.get('port')) - 1))
        socket.send_json(self.pubs)
        socket.recv_json()
        socket.disconnect('tcp://' + self.broker.get('ip') + ':' + str(int(self.broker.get('port')) - 1))

    def start_subscribers(self):
        print("Registry notifying subscribers to start")
        if self.args.disseminate == 'broker':
            print("Registry passing broker information to subscribers")
            # send each subscriber the broker IP:Port
            socket = self.context.socket(zmq.REQ)
            for sub in self.subs:
                socket.connect('tcp://' + sub.get('ip') + ':' + str(int(sub.get('port')) - 1))
                socket.send_json(self.broker)
                socket.recv_json()
                socket.disconnect('tcp://' + sub.get('ip') + ':' + str(int(sub.get('port')) - 1))
        else:
            print("Registry passing publisher information to subscribers")
            # TODO: send each subscriber a list of publishers with their topics
            pass

    def start_publishers(self):
        print("Registry notifying publishers to start")
        # Iterate through list of publishers and issue a start
        socket = self.context.socket(zmq.REQ)
        for pub in self.pubs:
            socket.connect('tcp://' + pub.get('ip') + ':' + str(int(pub.get('port')) -1))
            socket.send_json("start")
            socket.recv_json()
            socket.disconnect('tcp://' + pub.get('ip') + ':' + str(int(pub.get('port')) -1))
