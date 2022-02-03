import zmq
import json


class RegistryProxy:
    def __init__(self, args):
        self.args = args
        self.context = zmq.Context()

    def register(self, topics):
        socket = self.context.socket(zmq.REQ)
        socket.connect('tcp://' + self.args.registry + ":" + self.args.port)
        data = {'role': self.args.role, 'ip': socket.gethostbyname(socket.gethostname()), 'port': self.args.bind, 'topics': topics}
        socket.send_str(json.dumps(data))

    def wait(self):
        socket = self.context.socket(zmq.REP)
        socket.bind('tcp://*:' + self.args.bind)
        data = socket.recv()
        return json.loads(data)
