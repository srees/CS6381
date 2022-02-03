###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: API for the middleware layer for the publisher functionality
#
# Created: Spring 2022
#
###############################################

# ABC stands for abstract base class and this is how Python library
# defines the underlying abstract base class
from abc import ABC, abstractmethod
import zmq
import datetime
import json
import time


# define an abstract base class for the publisher
class Publisher(ABC):

    # to be invoked by the publisher's application logic
    # to publish a value of a topic. 
    @abstractmethod
    def publish(self, topic, value):
        pass

    # to be invoked by a broker to kickstart the publisher
    # so it can start publishing.  This method is for Assignment #1
    # where we want all publishers and subscribers deployed
    # before the publishers can start publishing. 
    @abstractmethod
    def start(self):
        pass


# a concrete class that disseminates info directly
class DirectPublisher(Publisher):

    # constructor. Add whatever class members you need
    # for the assignment
    def __init__(self, args):
        # print("Current libzmq version is %s" % zmq.zmq_version())
        print("Utilizing direct send publish method")
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUB)
        self.socket.bind("tcp://*:" + str(args.bind))  # 5556
        self.start = False

    # to be invoked by the publisher's application logic
    # to publish a value of a topic. 
    def publish(self, topic, value):
        # print("I am the direct send publisher's publish method")
        if self.start:
            data = {'TS': datetime.datetime.now(), 'Topic': topic, 'Value': value}
            self.socket.send_string(json.dumps(data))

    # to be invoked by a broker to kickstart the publisher
    # so it can start publishing.  This method is for Assignment #1
    # where we want all publishers and subscribers deployed
    # before the publishers can start publishing. 
    def start(self):
        print("I am the direct send publisher's start method")


# A concrete class that disseminates info via the broker
class ViaBrokerPublisher(Publisher):

    # constructor. Add whatever class members you need
    # for the assignment
    def __init__(self, args):
        # print("Current libzmq version is %s" % zmq.zmq_version())
        print("Utilizing broker send publish method")
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUB)
        self.socket.bind("tcp://*:" + str(args.bind))
        self.start = False

    # to be invoked by the publisher's application logic
    # to publish a value of a topic. 
    def publish(self, topic, value):
        if self.start:
            data = {'TS': datetime.datetime.now(), 'Topic': topic, 'Value': value}
            self.socket.send_string(json.dumps(data))

    # to be invoked by a broker to kickstart the publisher
    # so it can start publishing.  This method is for Assignment #1
    # where we want all publishers and subscribers deployed
    # before the publishers can start publishing. 
    def start(self):
        print("Enabling publish")
        self.start = True
