import zmq
import publicip


class RegistryProxy:
    def __init__(self, args):
        self.args = args
        self.context = zmq.Context()

    def register(self, topics):
        reg_socket = self.context.socket(zmq.REQ)
        connection_string = 'tcp://' + self.args.registry + ":" + self.args.port
        reg_socket.connect(connection_string)
        print("Registering with " + connection_string + '...')
        data = {'role': self.args.role, 'ip': publicip.get_ip_address(), 'port': str(int(self.args.bind) + 1), 'topics': topics}
        reg_socket.send_json(data)
        print("Registration sent")
        reg_socket.recv_json()
        print("Registration received")
