import zmq
import json


class Registry:
    def __init__(self, args):
        self.port = args.port
        self.context = zmq.Context()
        self.disseminate = args.disseminate
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
        try:
            self.collect_registrations()
            if self.disseminate == "broker":
                self.start_broker()
            self.start_subscribers()
            self.start_publishers()
            # since this is not dynamic, per se, we don't need to keep running to answer
            # updates or requests from late joiners
        except KeyboardInterrupt:
            pass

    def collect_registrations(self):
        socket = self.context.socket(zmq.REP)
        socket.bind("tcp://*:" + str(self.port))
        while len(self.pubs) < self.expected_pubs and len(self.subs) < self.expected_subs and len(self.broker) < self.expected_brokers:
            message = json.loads(socket.recv_str())
            if message['role'] == 'broker':
                self.broker = {'ip': message['ip'], 'port': message['port']}
            if message['role'] == 'publisher':
                self.pubs.append({'ip': message['ip'], 'port': message['port'], 'topics': message['topics']})
            if message['role'] == 'subscriber':
                self.subs.append({'ip': message['ip'], 'port': message['port'], 'topics': message['topics']})

    def start_broker(self):  # We'll start the broker by sending it the list of publishers to subscribe to
        socket = self.context.socket(zmq.REQ)
        socket.connect('tcp://' + self.broker.get('ip') + ':' + self.broker.get('port'))
        socket.send(json.dumps(self.pubs))

    def start_subscribers(self):
        if self.disseminate == 'broker':
            # send each subscriber the broker IP:Port
            socket = self.context.socket(zmq.REQ)
            for sub in self.subs:
                socket.connect('tcp://' + sub.get('ip') + ':' + sub.get('port'))
                socket.send(json.dumps(self.broker))
                socket.disconnect('tcp://' + sub.get('ip') + ':' + sub.get('port'))
        else:
            # TODO: send each subscriber a list of publishers with their topics
            pass

    def start_publishers(self):
        # Iterate through list of publishers and issue a start
        socket = self.context.socket(zmq.REQ)
        for pub in self.pubs:
            socket.connect('tcp://' + pub.get('ip') + ':' + pub.get('port'))
            socket.send(json.dumps("start"))
            socket.disconnect('tcp://' + pub.get('ip') + ':' + pub.get('port'))
