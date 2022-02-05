import zmq
import publicip


class RegistryProxy:
    def __init__(self, args):
        self.args = args
        self.context = zmq.Context()
        self.REQ_socket = self.context.socket(zmq.REQ)
        self.connection_string = 'tcp://' + self.args.registry + ":" + self.args.port
        self.REQ_socket.connect(self.connection_string)

    def register(self, topics):
        print("Registering with " + self.connection_string + '...')
        data = {'role': self.args.role, 'ip': publicip.get_ip_address(), 'port': str(int(self.args.bind) + 1), 'topics': topics}
        self.REQ_socket.send_json(data)
        print("Registration sent")
        self.REQ_socket.recv_json()
        print("Registration received")
