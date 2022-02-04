###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: API for the broker functionality in the middleware layer
#
# Created: Spring 2022
#
###############################################

# See the cs6381_publisher.py file for how an abstract Publisher class is
# defined and then two specialized classes. We may need similar things here.
# I am also assuming that discovery and dissemination are lumped into the
# broker. Otherwise keep them in separate files.
import zmq
import publicip


class Broker:
    def __init__(self, args):
        self.args = args
        self.pubs = []
        self.pub_sockets = []
        self.poller = zmq.Poller()
        self.topic_publishers = {}
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.bind_url = 'tcp://' + publicip.get_ip_address() + ':' + self.args.bind
        self.bind_url2 = 'tcp://' + publicip.get_ip_address() + ':' + str(int(self.args.bind) + 1)
        print("Binding REP to " + self.bind_url)
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.bind(self.bind_url)

    # def wait(self):
    #     socket = self.context.socket(zmq.REP)
    #     socket.bind("tcp://" + publicip.get_ip_address() + ":" + str(self.bind))
    #     while self.pubs.count() < self.pub_quantity and self.subs.count() < self.sub_quantity and not self.broker:
    #         message = json.loads(socket.recv_string())
    #         if message['role'] == 'broker':
    #             self.broker = {'ip': message['ip'], 'port': message['port']}
    #         if message['role'] == 'publisher':
    #             self.pubs.append({'ip': message['ip'], 'port': message['port'], 'topics': message['topics']})
    #             # Let's save the topics into a lookup dictionary
    #             for topic in message['topics']:
    #                 if topic in self.topic_publishers and isinstance(self.topic_publishers[topic], list):
    #                     # add the IP address to it
    #                     self.topic_publishers[topic].append(message['ip'])
    #                 else:
    #                     # create the topic and add the IP:port
    #                     self.topic_publishers[topic] = [message['ip'] + ':' + message['port']]
    #         if message['role'] == 'subscriber':
    #             self.subs.append({'ip': message['ip'], 'port': message['port'], 'topics': message['topics']})

    def start(self):
        self.wait()  # wait for registry to give us the go
        # first subscribe to publishers
        # for pub in self.pubs:
        #     connect_str = 'tcp://' + pub['ip'] + ':' + pub['port']
        #     print("Broker subscribing to " + connect_str)
        #     temp_sock = self.context.socket(zmq.SUB)
        #     temp_sock.connect(connect_str)
        #     temp_sock.setsockopt_string(zmq.SUBSCRIBE, '')
        #     self.pub_sockets.append(temp_sock)
        # for i in range(0, len(self.pub_sockets)):
        #     self.poller.register(self.pub_sockets[i], zmq.POLLIN)
        connect_str = 'tcp://' + self.pubs[0]['ip'] + ':' + self.pubs[0]['port']
        print("Broker subscribing to " + connect_str)
        temp_sock = self.context.socket(zmq.SUB)
        temp_sock.connect(connect_str)
        temp_sock.setsockopt_string(zmq.SUBSCRIBE, '')
        self.poller.register(temp_sock, zmq.POLLIN)
        print("Starting broker listen loop...")
        while True:
            try:
                print('getting events')
                events = dict(self.poller.poll())
                if temp_sock in events:
                    print('match')
                    data = temp_sock.recv_json()
                    print("Broker received: ")
                    print(data)
                    self.republish(data)
                # print('starting for')
                # for sock in self.pub_sockets:
                #     print('check if')
                #     if sock in events:
                #         print('match')
                #         data = sock.recv_json()
                #         print("Broker received: ")
                #         print(data)
                #         self.republish(data)
            except KeyboardInterrupt:
                break

    # Wait for registry to give us the start signal
    def wait(self):
        self.pubs = self.socket.recv_json()  # load our list of pubs with the data from registry
        # TODO validation, ie while data != 'start', socket.recv_json()
        self.socket.send_json('ACK')
        # switch socket to publisher model
        self.socket.close(0)
        self.socket = self.context.socket(zmq.PUB)
        print("Binding PUB to " + self.bind_url2)
        self.socket.bind(self.bind_url2)

    def republish(self, data):
        self.socket.send_json(data)
