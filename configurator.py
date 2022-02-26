###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: API for the Configurator in our middleware layer 
#
# Created: Spring 2022
#
###############################################

# The goal of the configurator is to maintain all the configuration of the
# experiment in one place and then serve as a factory to produce specialized
# objects that correspond to these supplied parameters.  These configuration
# parameters are expected to be supplied as a command line or in a file
# per experiment. 

# I am assuming there will be all these individual elements that this configurator
# is able to produce when asked by the caller
from topiclist import TopicList
from publisher import Publisher
from subscriber import Subscriber
from broker import Broker
from registry import Registry
from kademlia_reg import KademliaReg
from registryproxy import RegistryProxy


# define the system configurator class that will be used as a factory object
# to supply the right objects to the caller based on supplied command line
# arguments
class Configurator:
    arguments = []

    # constructor
    def __init__(self, args):
        self.arguments = args

    # retrieve the right type of publisher depending on the cmd line argument
    def get_publisher(self):
        # check what our role is. If we are the publisher app, we get the concrete
        # publisher object else get a proxy. The publisher itself may be specialized
        # depending on the dissemination strategy
        return Publisher(self.arguments)

    # retrieve the right type of subscriber depending on the cmd line argument
    def get_subscriber(self):
        # check what our role is. If we are the subscriber app, we get the concrete
        # subscriber object else a proxy.  The subscriber itself may be specialized
        # depending on the dissemination strategy
        return Subscriber(self.arguments)

    # retrieve the right type of broker depending on the cmd line argument
    def get_broker(self):
        # check what our role is. If we are the broker, we get the concrete
        # broker object else a proxy. The broker itself may be specialized
        # depending on the dissemination strategy
        return Broker(self.arguments)

    # A publisher and subscriber app may decide to publish or subscribe to,
    # respectively, a random set of topics. We provide such a helper in the
    # cs6381_topiclist.py file
    def get_interest(self):
        # as the topic list object t
        topics = TopicList()
        return topics.interest()

    def get_iterations(self):
        return int(self.arguments.count)

    def get_registry(self):
        if self.arguments.role == "registry":
            # if self.arguments.kademlia:
            return KademliaReg(self.arguments)
            # else:
                # return Registry(self.arguments)
        else:
            return RegistryProxy(self.arguments)
