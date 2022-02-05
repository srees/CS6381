###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton logic for the subscriber application
#
# Created: Spring 2022
#
###############################################


# The basic logic of the subscriber application will be as follows (see pubapp.py
# for additional details.
#
# (1) The subscriber app decides which all topics it is going to subscriber to.
# (see pubapp.py file for how the publisher gets its interest randomly chosen
# for it.
#
# (2) the application obtains a handle to the broker (which under the
# hood will be a proxy object but the application doesn't know it is a
# proxy).
#
# (3) Register with the broker letting it know all the topics it is interested in
# and any other details needed for the underlying middleware to make the
# communication happen via ZMQ
#
# (4) Obtain a handle to the subscriber object (which maybe a specialized
# object depending on whether it is using the direct dissemination or
# via broker)
#
# (5) Wait for the broker to let us know who our publishers are for the
# topics we are interested in. In the via broker approach, the broker is the
# only publisher for us.
#
#
# (5) Have a receiving loop. See scaffolding code for polling where
# we show how different ZMQ sockets can be polled whenever there is
# an incoming event.
#
#    In each iteration, handle all the events that have been enabled by
#    receiving the data. Get the timestamp and obtain the latency for each
#    received topic from each publisher and store this info possibly in a
#    time series database like InfluxDB
#
# (6) if you have logic that allows this forever loop to terminate, then
# go ahead and clean up everything.

import argparse  # for argument parsing
from configurator import Configurator  # factory class
import time


# import any other packages you need.

###################################
#
# Parse command line arguments
#
###################################
def parseCmdLineArgs():
    # instantiate a ArgumentParser object
    parser = argparse.ArgumentParser(description="Subscriber Application")

    # Now specify all the optional arguments we support
    # At a minimum, you will need a way to specify the IP and port of the lookup
    # service, the role we are playing, what dissemination approach are we
    # using, what is our endpoint (i.e., port where we are going to bind at the
    # ZMQ level)

    # Here I am showing one example of adding a command line
    # arg for the dissemination strategy. Feel free to modify. Add more
    # options for all the things you need.
    parser.add_argument("-r", "--registry", default="127.0.0.1", help="IP Address of the registry")
    parser.add_argument("-p", "--port", default="5550", help="Port of the registry")
    parser.add_argument("-b", "--bind", default="5580", help="Port to bind for registry start")

    return parser.parse_args()


###################################
#
# Main program
#
###################################
def main():
    # first parse the arguments
    print("Main: parse command line arguments")
    args = parseCmdLineArgs()
    args.role = "subscriber"

    # get hold of the configurator, which is the factory that produces
    # many kinds of artifacts for us
    config = Configurator(args)

    # Ask the configurator to give us a random subset of topics that we can publish
    my_topics = config.get_interest()
    print("Subscriber interested in listening for these topics: {}".format(my_topics))

    # get a handle to our publisher object
    print("Getting subscriber object")
    sub = config.get_subscriber()

    # get a handle to our registry object (will be a proxy)
    print("Getting registry proxy")
    registry = config.get_registry()

    # register with lookup
    registry.register(my_topics)

    # wait for kickstart event from registry
    print("Waiting for start from registry")
    sub.start(my_topics, lambda data: data_callback(data))
    # consider adding a check for if message exists (not a timeout) before proceeding

    # now do the publication for as many iterations that we plan to do
    print("Start received.")


def data_callback(data):
    print('Received at ' + str(time.time()))
    print(data)

###################################
#
# Main entry point
#
###################################
if __name__ == "__main__":
    main()
